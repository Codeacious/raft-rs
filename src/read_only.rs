// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::OnceLock;
use std::time::Instant;

use slog::Logger;

use crate::eraftpb::Message;
use crate::quorum::{AckedIndexer, Index};
use crate::JointConfig;
use crate::{HashMap, HashSet};

/// Allowed clock drift for read leases.
/// This amount is subtracted from the read lease duration at the follower.
pub(crate) const SAFEGUARD_CLOCK_DRIFT_MICROS: u64 = 1_000; // 1ms
/// Minimum interval between a follower's `MsgAskReadLease` sends.
pub(crate) const LEASE_ASK_INTERVAL_MICROS: u64 = 10_000; // 10ms
/// Minimum interval between a leader's `MsgAskAckIndex` pings to the same lease
/// holder.
pub(crate) const ACK_INDEX_ASK_INTERVAL_MICROS: u64 = 5_000; // 5ms
/// A lease (a follower's, or the leader's own) is renewed once it's within this
/// many microseconds of expiry.
pub(crate) const LEASE_RENEWAL_THRESHOLD_MICROS: u64 = 200_000; // 200ms
/// A lease round that has not reached a majority after this long is presumed
/// lost, and the next lease event may send another. Pacing only; never part of
/// a safety decision.
pub(crate) const LEASE_ROUND_RETRY_MICROS: u64 = 10_000; // 10ms
/// Log index threshold for a read-lease follower deciding it's too far behind to
/// serve a read locally, forwarding the read to the leader instead.
pub(crate) const READ_INDEX_LOCAL_HOLD_LOG_THRESHOLD: u64 = 100;
/// How long (micros) a follower holds a not-yet-servable read locally
/// before giving up and forwarding to the leader.
pub(crate) const READ_INDEX_LOCAL_HOLD_DURATION_MICROS: u64 = 5_000; // 5ms

/// Microseconds elapsed since a fixed, lazily-captured process-start epoch.
/// Backed by `Instant`, so lease timing is monotonic and immune to wall-clock jumps.
/// Never mix this with `SystemTime`.
pub(crate) fn now_micros() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_micros() as u64
}

/// Determines the relative safety of and consistency of read only requests.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    #[default]
    Safe,
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
    /// GrantLeases extends LeaseBased read consistency to followers: the leader
    /// hands out time-bounded read leases (see `ReadLease`) so that followers can
    /// serve linearizable reads locally. Like `LeaseBased`, it requires
    /// `check_quorum = true`.
    GrantLeases,
}

/// ReadState provides state for read only query.
///
/// It's caller's responsibility to send MsgReadIndex first before getting
/// this state from ready. It's also caller's duty to differentiate if this
/// state is what it requests through request_ctx, e.g. given a unique id as
/// request_ctx.
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct ReadState {
    /// The index of the read state.
    pub index: u64,
    /// A datagram consisting of context about the request.
    pub request_ctx: Vec<u8>,
    /// Whether this index came from **this follower's own read lease**
    /// (`ReadOnlyOption::GrantLeases`) rather than from the leader's
    /// `MsgReadIndexResp`. `false` for every non-`GrantLeases` path and for a
    /// leader's own reads.
    pub served_by_follower_lease: bool,
    /// Whether this index came from **this leader's own self-lease** (either
    /// leased option) rather than from a heartbeat quorum round. A lease-served
    /// index is linearizable, but it contacted nobody after the read was
    /// proposed, so it is not evidence that a quorum followed this leader at
    /// that moment. A wrapper that renews a lease of its own from a read's
    /// proposal time (TiKV's raftstore leader lease) must only do so when this
    /// is `false`. Only ever `true` on a leader's local reads.
    pub served_by_leader_lease: bool,
}

#[derive(Default, Debug, Clone)]
pub struct ReadIndexStatus {
    pub req: Message,
    pub index: u64,
    pub acks: HashSet<u64>,
}

/// A time-bounded read lease granted by the leader to a follower so the follower
/// can serve linearizable reads locally (`ReadOnlyOption::GrantLeases`).
/// The first six fields form a fixed 48-byte big-endian wire format;
/// `last_ack_asked` is an in-memory-only field and is never marshalled.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ReadLease {
    /// Follower-minted lease id (high 32 bits = the holder's node id).
    pub lease_id: u64,
    /// The node holding the lease.
    pub node_id: u64,
    /// Leader commit index at grant time; the holder may not serve until its own
    /// commit index reaches this (the catch-up margin).
    pub log_index: u64,
    /// Lease start, in the `now_micros()` clock of whichever node holds it (the
    /// follower substitutes its own ask time in `process_granted_lease`).
    pub start_time: u64,
    /// Lease validity in micros.
    pub duration: u64,
    /// The holder's promised ack index: it will not serve below this until its
    /// commit index reaches it. What the leader's commit clamp rests on.
    pub acked_index: u64,
    /// Leader-side rate limit for `MsgAskAckIndex`; in-memory only.
    pub(crate) last_ack_asked: u64,
}

impl ReadLease {
    /// Encodes the six wire fields as 48 big-endian bytes.
    pub fn marshal(&self) -> [u8; 48] {
        let mut buf = [0u8; 48];
        buf[0..8].copy_from_slice(&self.lease_id.to_be_bytes());
        buf[8..16].copy_from_slice(&self.node_id.to_be_bytes());
        buf[16..24].copy_from_slice(&self.log_index.to_be_bytes());
        buf[24..32].copy_from_slice(&self.start_time.to_be_bytes());
        buf[32..40].copy_from_slice(&self.duration.to_be_bytes());
        buf[40..48].copy_from_slice(&self.acked_index.to_be_bytes());
        buf
    }

    /// Decodes the 48-byte wire form. `last_ack_asked` is not carried and is
    /// reset to 0.
    pub fn unmarshal(bytes: &[u8; 48]) -> ReadLease {
        let field = |off: usize| u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap());
        ReadLease {
            lease_id: field(0),
            node_id: field(8),
            log_index: field(16),
            start_time: field(24),
            duration: field(32),
            acked_index: field(40),
            last_ack_asked: 0,
        }
    }
}

/// The leases a **leader** has granted to other nodes, keyed by holder. Empty on
/// a follower: the lease a node holds itself is `ReadOnly::held_lease`, whatever
/// its role, so nothing here is ever this node's own promise.
#[derive(Default, Debug, Clone)]
pub struct ReadLeaseMap {
    /// Granted leases keyed by node_id.
    pub(crate) leases: HashMap<u64, ReadLease>,
    /// Min-heap on acked index. Entries are (acked_index, node_id).
    acked_index_heap: BinaryHeap<Reverse<(u64, u64)>>,
    /// Soonest lease-expiry time, in `now_micros()` microseconds. Private cache
    /// maintained by `update_soonest_expiry`/`cleanup_expired_leases`.
    soonest_expiry: u64,
}

impl ReadLeaseMap {
    fn put(&mut self, node_id: u64, lease: ReadLease) {
        let expiry = lease.start_time + lease.duration;
        let acked = lease.acked_index;
        self.update_soonest_expiry(expiry);
        self.leases.insert(node_id, lease);
        self.push_acked_index(node_id, acked);
    }

    /// The sole choke point for growing `acked_index_heap`. Rust's `BinaryHeap`
    /// has no decrease-key, so raising a node's acked index pushes a fresh
    /// `(acked, node)` entry rather than mutating the old one; the superseded
    /// entry is reaped immediately by the following prune.
    fn push_acked_index(&mut self, node_id: u64, acked: u64) {
        self.acked_index_heap.push(Reverse((acked, node_id)));
        self.prune_stale_acked_roots();
    }

    /// Pops superseded entries off the min-heap root: an entry is stale when its
    /// node holds no lease, or the live lease's acked index has moved past the
    /// entry's key. Stops at the first still-current root, which is therefore
    /// the minimum acked index across live leases.
    fn prune_stale_acked_roots(&mut self) {
        while let Some(&Reverse((acked, node_id))) = self.acked_index_heap.peek() {
            match self.leases.get(&node_id) {
                Some(lease) if lease.acked_index == acked => break,
                _ => {
                    self.acked_index_heap.pop();
                }
            }
        }
    }

    fn update_soonest_expiry(&mut self, exp: u64) {
        if self.soonest_expiry == 0 || exp < self.soonest_expiry {
            self.soonest_expiry = exp;
        }
    }

    /// Relative micros until the soonest-held lease is due to expire, for the
    /// wrapper to arm a precise lease-expiry cleanup timer. `None` if no leases
    /// are held; `Some(0)` = already due. O(1): reads the `soonest_expiry`
    /// cache, which only ever ratchets down until `cleanup_expired_leases`
    /// recomputes it, so the timer can fire early but never late.
    pub fn micros_until_soonest_expiry(&self) -> Option<u64> {
        if self.soonest_expiry == 0 {
            return None;
        }
        Some(self.soonest_expiry.saturating_sub(now_micros()))
    }

    /// Creates a new active lease for `node_id` and stores it. Returns a clone of
    /// the stored lease for the leader to marshal and send to a follower.
    pub fn grant_new_lease(
        &mut self,
        node_id: u64,
        lease_id: u64,
        acked_index: u64,
        log_index: u64,
        duration: u64,
    ) -> ReadLease {
        let lease = ReadLease {
            lease_id,
            node_id,
            log_index,
            start_time: now_micros(),
            duration,
            acked_index,
            last_ack_asked: 0,
        };
        self.put(node_id, lease.clone());
        lease
    }

    /// Looks up the active lease held by `node_id`, if any.
    pub fn get(&self, node_id: u64) -> Option<&ReadLease> {
        self.leases.get(&node_id)
    }

    /// Raises node_id's acked index to `index` (never lowers it) and refreshes
    /// its heap entry via `push_acked_index` (which prunes the superseded one).
    pub fn update_acked_index(&mut self, node_id: u64, index: u64) {
        match self.leases.get_mut(&node_id) {
            Some(lease) if index > lease.acked_index => lease.acked_index = index,
            // No lease, or not an increase: nothing to record.
            _ => return,
        }
        self.push_acked_index(node_id, index);
    }

    /// Minimum acked index across all active leases, or `u64::MAX` when there
    /// are none. Cleans up expired leases first, then prunes the heap and reads
    /// its root.
    pub fn min_acked_index(&mut self) -> u64 {
        self.cleanup_expired_leases();
        self.prune_stale_acked_roots();
        match self.acked_index_heap.peek() {
            Some(&Reverse((acked, _))) => acked,
            None => u64::MAX,
        }
    }

    /// Lease holders the leader should ping (`MsgAskAckIndex`) for a fresher
    /// acked index: those whose `acked_index` is below `cutoff` and that have not
    /// been asked within `ACK_INDEX_ASK_INTERVAL_MICROS`. Stamps `last_ack_asked`
    /// on each returned holder so a flurry of responses can't re-trigger an
    /// immediate re-ask.
    pub fn nodes_to_ask_for_ack(&mut self, cutoff: u64) -> Vec<u64> {
        self.cleanup_expired_leases();
        let now = now_micros();
        let mut ids = Vec::new();
        for (id, lease) in self.leases.iter_mut() {
            if lease.acked_index < cutoff
                && now.saturating_sub(lease.last_ack_asked) >= ACK_INDEX_ASK_INTERVAL_MICROS
            {
                lease.last_ack_asked = now;
                ids.push(*id);
            }
        }
        ids
    }

    pub fn cleanup_expired_leases(&mut self) {
        let now = now_micros();
        if self.soonest_expiry != 0 && now < self.soonest_expiry {
            return;
        }
        let mut soonest = 0u64;
        let mut expired = Vec::new();
        for (id, lease) in self.leases.iter() {
            let exp = lease.start_time + lease.duration;
            if exp <= now {
                expired.push(*id);
            } else if soonest == 0 || exp < soonest {
                soonest = exp;
            }
        }
        for id in expired {
            self.leases.remove(&id);
        }
        self.soonest_expiry = soonest;
    }

    pub fn has_active_lease(&mut self, node_id: u64) -> bool {
        let now = now_micros();
        let active = match self.leases.get(&node_id) {
            Some(lease) => lease.start_time + lease.duration > now,
            None => false,
        };
        if !active {
            self.cleanup_expired_leases();
        }
        active
    }

    pub fn num_active_leases(&mut self) -> usize {
        self.cleanup_expired_leases();
        self.leases.len()
    }

    /// Removes `node_id`'s lease, if any. Its heap entry goes stale and is
    /// pruned lazily.
    pub(crate) fn remove(&mut self, node_id: u64) {
        self.leases.remove(&node_id);
    }
}

/// Counters surfaced to the wrapping DB (TiKV) for observability.
#[derive(Default, Debug, Clone, Copy)]
pub struct ReadLeaseStats {
    pub times_read_lease_used: u64,
    pub times_got_read_query: u64,
}

/// A `MsgReadIndex` a follower is holding locally instead of forwarding,
/// waiting for its commit index to catch up to `required_index`.
#[derive(Debug, Clone)]
pub(crate) struct DelayedReadIndex {
    /// The original read-index message, to be replayed via `step` once it can be
    /// resolved (served locally) or given up on (forwarded to the leader).
    pub msg: Message,
    /// Deadline after which the read is forwarded to the leader.
    pub forward_at_micros: u64,
    /// Commit index this follower must reach before it can serve the read locally.
    pub required_index: u64,
}

/// The lease-related knobs `Config` carries, grouped so a `ReadOnly` can be
/// (re)built from a single argument. `Raft` does not retain its `Config`, so
/// these must live on `ReadOnly` to survive a `reset()` rebuild. Defaults
/// (0/false) leave `GrantLeases` inert; a real Raft passes the configured set
/// via `Config::read_lease_config`.
#[derive(Default, Debug, Clone)]
pub struct ReadLeaseConfig {
    /// Lease validity duration in micros (`Config::read_lease_duration_micros`).
    pub duration_micros: u64,
    /// Max number of leases the leader grants at once (`Config::max_num_read_leases`).
    pub max_leases: usize,
    /// Whether this node asks the leader for read leases (`Config::ask_for_read_lease`).
    pub ask_for_lease: bool,
    /// Catch-up margin in log indices (`Config::read_lease_catchup_margin`).
    pub catchup_margin: u64,
}

/// A follower's lease request that a leader is holding until a majority returns
/// a lease round minted after it arrived. See `ReadOnly::park_grant`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PendingGrant {
    /// The follower-minted lease id, echoed back in the answer.
    pub lease_id: u64,
    /// `now_micros()` when the request arrived. Only used to tell whether a
    /// lease dated from the request would already be over; whether the request
    /// is covered is decided by `need_round`, never by this.
    pub proposed_at: u64,
    /// The follower's commit index when it asked.
    pub asked_index: u64,
    /// Grantable once a majority has returned this lease round or a later one.
    pub need_round: u64,
}

/// A leader's pending renewal of its own lease.
#[derive(Debug, Clone, Copy)]
struct PendingSelfLease {
    /// `now_micros()` when the renewal was proposed; the new lease starts here.
    start: u64,
    /// The lease round a majority must return before the renewal installs.
    need_round: u64,
}

/// Lease-round echoes as an `AckedIndexer`, so the highest round a majority of
/// voters has returned is computed by the same code as the commit index: joint
/// configurations, learners (not counted) and silent voters (counted as 0) are
/// handled identically. The leader counts as having returned its latest round.
struct LeaseEchoIndexer<'a> {
    echoes: &'a HashMap<u64, u64>,
    self_id: u64,
    round: u64,
}

impl AckedIndexer for LeaseEchoIndexer<'_> {
    fn acked_index(&self, voter_id: u64) -> Option<Index> {
        let round = if voter_id == self.self_id {
            Some(self.round)
        } else {
            self.echoes.get(&voter_id).copied()
        };
        round.map(|index| Index { index, group_id: 0 })
    }
}

/// The lease bookkeeping only a leader uses: its lease rounds, the pending
/// renewal of its own lease, and the leases it grants to followers. Empty on a
/// follower. The lease this node holds is `ReadOnly::held_lease`, not here.
#[derive(Default, Debug, Clone)]
pub struct LeaderLeaseState {
    /// Numbers this leader's heartbeat broadcasts in a leased mode. Each one
    /// mints the next round and carries it in `MsgHeartbeat.index`; followers
    /// echo it in `MsgHeartbeatResponse.index`. A majority echoing round N
    /// shows they still followed this leader when N was minted, so a proposal
    /// recorded before N was minted may take effect. Restarts at 0 each term.
    lease_round: u64,
    /// When the latest round was minted. Retry pacing only.
    lease_round_sent_at: u64,
    /// The highest round each follower has echoed.
    follower_echoed_rounds: HashMap<u64, u64>,
    /// The highest round a majority has echoed. Everything waiting on it or an
    /// earlier round has been answered.
    lease_quorum_round: u64,
    /// This leader's pending self-lease renewal, if any.
    pending_self_lease: Option<PendingSelfLease>,
    /// Whether this leader has served a read from its self-lease since that
    /// lease installed. Tick- and timer-driven renewal only run while it is
    /// set, so an unused self-lease lapses instead of costing a broadcast per
    /// renewal; a read always proposes one.
    self_lease_used: bool,
    /// Bars this leader from renewing its own lease for the rest of the term.
    /// See `ReadOnly::ban_self_lease`.
    self_lease_banned: bool,
    /// Leases this leader has granted to other nodes.
    pub granted_leases: ReadLeaseMap,
    /// Follower lease requests this leader is holding for a lease round, keyed
    /// by asker.
    pending_grants: HashMap<u64, PendingGrant>,
    /// Nodes barred from being granted a lease for the rest of this term. See
    /// `ban_grant`.
    grant_banned: HashSet<u64>,
}

impl LeaderLeaseState {
    /// Mints the next lease round. Called for every heartbeat broadcast in a
    /// leased option, before the heartbeats are built.
    pub(crate) fn mint_lease_round(&mut self) -> u64 {
        self.lease_round += 1;
        self.lease_round_sent_at = now_micros();
        self.lease_round
    }

    /// The latest minted lease round; 0 outside leased options.
    #[inline]
    pub(crate) fn lease_round(&self) -> u64 {
        self.lease_round
    }

    /// Records that `node_id` echoed `round`. Returns `false`, ignoring the
    /// echo, if this leader never minted that round or `node_id` has already
    /// echoed a later one.
    pub(crate) fn record_lease_echo(&mut self, node_id: u64, round: u64) -> bool {
        if round > self.lease_round {
            return false;
        }
        let seen = self.follower_echoed_rounds.entry(node_id).or_insert(0);
        if round <= *seen {
            return false;
        }
        *seen = round;
        true
    }

    /// Recomputes the highest round a majority of `voters` has echoed, and
    /// returns it if it rose. Everything waiting on that round or an earlier one
    /// is then covered.
    pub(crate) fn advance_lease_quorum(
        &mut self,
        voters: &JointConfig,
        self_id: u64,
    ) -> Option<u64> {
        let (q, _) = voters.committed_index(
            false,
            &LeaseEchoIndexer {
                echoes: &self.follower_echoed_rounds,
                self_id,
                round: self.lease_round,
            },
        );
        // An empty configuration reports u64::MAX; no round beyond the latest
        // minted one can have been echoed.
        let q = cmp::min(q, self.lease_round);
        if q <= self.lease_quorum_round {
            return None;
        }
        self.lease_quorum_round = q;
        Some(q)
    }

    /// Whether a majority has yet to echo the latest minted round.
    pub(crate) fn lease_round_in_flight(&self) -> bool {
        self.lease_quorum_round < self.lease_round
    }

    /// Whether the latest round has waited long enough to be presumed lost.
    pub(crate) fn lease_round_retry_due(&self) -> bool {
        now_micros().saturating_sub(self.lease_round_sent_at) >= LEASE_ROUND_RETRY_MICROS
    }

    /// Whether a self-lease renewal or a follower request is waiting on a lease
    /// round.
    pub(crate) fn has_pending_proposals(&self) -> bool {
        self.pending_self_lease.is_some() || !self.pending_grants.is_empty()
    }

    /// Proposes a renewal of this leader's self-lease, dated now. It installs
    /// once a majority returns a lease round minted after this moment.
    pub(crate) fn arm_self_lease(&mut self) {
        self.pending_self_lease = Some(PendingSelfLease {
            start: now_micros(),
            need_round: self.lease_round + 1,
        });
    }

    /// When the pending self-lease renewal was proposed, if one is pending.
    pub(crate) fn self_lease_armed_at(&self) -> Option<u64> {
        self.pending_self_lease.map(|p| p.start)
    }

    /// Records that this leader served a read from its self-lease.
    pub(crate) fn mark_self_lease_used(&mut self) {
        self.self_lease_used = true;
    }

    pub(crate) fn self_lease_used(&self) -> bool {
        self.self_lease_used
    }

    /// Whether this leader is barred from renewing its own lease. See
    /// `ReadOnly::ban_self_lease`.
    pub(crate) fn is_self_lease_banned(&self) -> bool {
        self.self_lease_banned
    }

    /// Bars `node_id` from being granted a read lease for the rest of this
    /// term, because a configuration change removing it is in flight, and
    /// drops any request of theirs still waiting on a lease round.
    pub(crate) fn ban_grant(&mut self, node_id: u64) {
        self.grant_banned.insert(node_id);
        self.drop_pending_grant(node_id);
    }

    pub(crate) fn is_grant_banned(&self, node_id: u64) -> bool {
        self.grant_banned.contains(&node_id)
    }

    /// Ids of followers with a held request, sorted so answers go out in a
    /// deterministic order.
    pub(crate) fn parked_grant_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.pending_grants.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub(crate) fn parked_grant(&self, node_id: u64) -> Option<PendingGrant> {
        self.pending_grants.get(&node_id).copied()
    }

    pub(crate) fn take_parked_grant(&mut self, node_id: u64) -> Option<PendingGrant> {
        self.pending_grants.remove(&node_id)
    }

    /// Discards `node_id`'s held request without answering it. Used when the
    /// follower has left the configuration.
    pub(crate) fn drop_pending_grant(&mut self, node_id: u64) {
        self.pending_grants.remove(&node_id);
    }
}

/// Keeps a newly elected leader under a leased option from committing while a
/// lease from an earlier term may still be live. See
/// `Raft::new_leader_safeguard_passed`.
#[derive(Default, Debug, Clone)]
pub(crate) struct NewLeaderSafeguard {
    /// Whether the safeguard has lifted. Latches for the term.
    passed: bool,
    /// `now_micros()` when this node became leader, which is when the
    /// safeguard starts.
    start_micros: u64,
    /// How long the safeguard lasts. See `start`.
    window_micros: u64,
    /// Peers this node has heard from at its *current* term. Such a peer has
    /// necessarily run `reset()`, so it holds no lease from any earlier term.
    confirmed_at_term: HashSet<u64>,
    /// Node ids named by conf-change entries in the unapplied log tail at
    /// election time. The shortcut waits for these as well as every node in the
    /// current configuration: an unapplied addition is absent from `prs` but
    /// may still hold a prior-term lease.
    unapplied_conf_nodes: HashSet<u64>,
    /// The election-time tail scan failed, so `unapplied_conf_nodes` may be
    /// incomplete. Disables the shortcut entirely.
    scan_failed: bool,
}

impl NewLeaderSafeguard {
    /// Starts the safeguard: for the next `window_micros` this leader commits
    /// nothing. Called from `become_leader` with one lease duration plus the
    /// drift allowance.
    pub(crate) fn start(&mut self, window_micros: u64) {
        self.window_micros = window_micros;
        self.start_micros = now_micros();
        self.passed = window_micros == 0;
    }

    /// Whether the safeguard has lifted, latching it once its window has
    /// elapsed. Does not consult the confirmation shortcut.
    pub(crate) fn has_passed(&mut self) -> bool {
        if !self.passed && now_micros().saturating_sub(self.start_micros) > self.window_micros {
            self.passed = true;
        }
        self.passed
    }

    /// Latches the safeguard as passed. Used by the confirmation shortcut, so a
    /// later configuration change cannot un-pass a window already lifted.
    pub(crate) fn mark_passed(&mut self) {
        self.passed = true;
    }

    /// Records that `node_id` has been heard from at this node's current term.
    /// Ignored once the safeguard has lifted.
    pub(crate) fn confirm_at_term(&mut self, node_id: u64) {
        if !self.passed {
            self.confirmed_at_term.insert(node_id);
        }
    }

    pub(crate) fn is_confirmed_at_term(&self, node_id: u64) -> bool {
        self.confirmed_at_term.contains(&node_id)
    }

    /// Hands over the confirmations recorded so far, so `become_leader` can
    /// carry a candidate's vote responses across its `reset()`. The term does
    /// not change there, so they remain proof that each voter has reset.
    pub(crate) fn take_confirmations(&mut self) -> HashSet<u64> {
        std::mem::take(&mut self.confirmed_at_term)
    }

    /// Records a node named by an unapplied conf change at election time.
    pub(crate) fn add_unapplied_conf_node(&mut self, node_id: u64) {
        self.unapplied_conf_nodes.insert(node_id);
    }

    pub(crate) fn unapplied_conf_nodes(&self) -> &HashSet<u64> {
        &self.unapplied_conf_nodes
    }

    /// Records that the election-time tail scan failed, disabling the shortcut.
    pub(crate) fn mark_scan_failed(&mut self) {
        self.scan_failed = true;
    }

    pub(crate) fn scan_failed(&self) -> bool {
        self.scan_failed
    }
}

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: VecDeque<Vec<u8>>,
    pub(crate) delayed_read_index_reqs: VecDeque<DelayedReadIndex>,
    pub read_lease_stats: ReadLeaseStats,
    pub lease_config: ReadLeaseConfig,
    /// Last time (micros) this node asked the leader for a lease.
    pub last_lease_asked: u64,
    /// Time (micros) this `ReadOnly` was created. Observability only.
    pub creation_time: u64,
    /// The lease *this* node holds, whatever its role: a follower's is granted
    /// by the leader, a leader's is its self-lease. Reads are served locally
    /// only while it is live, so one gate covers both roles.
    held_lease: Option<ReadLease>,
    /// Lease ids this follower has asked for, mapped to when it asked. A
    /// granted lease is dated from the ask, on this node's clock.
    pending_leases: HashMap<u64, u64>,
    /// Feeds the low 32 bits of minted lease ids. Seeded with a random nonce, so
    /// ids minted after a restart within the same term do not repeat ones a
    /// grant still in flight (or parked at the leader) could match.
    pending_lease_ctr: u64,
    /// This node's *own* read-lease ack index: the highest log index it has
    /// promised the leader it will not serve below. Node-global and **monotone
    /// within a term**, deliberately not stored on the `ReadLease` — a lease's
    /// copy is only as fresh as the grant that installed it and dies with the
    /// lease. Never persisted; `reset()` rebuilds it, and a crash discharges the
    /// promise along with the lease.
    read_lease_acked_index: u64,
    /// Lease state only a leader uses.
    pub leader: LeaderLeaseState,
    /// The new-leader safeguard.
    pub(crate) safeguard: NewLeaderSafeguard,
}

impl ReadOnly {
    /// Builds a `ReadOnly` for the given option and lease config. Pass
    /// `ReadLeaseConfig::default()` when leases are irrelevant (the `Safe`
    /// option, or tests); a real Raft passes `Config::read_lease_config`.
    pub fn new(option: ReadOnlyOption, lease_config: ReadLeaseConfig) -> ReadOnly {
        ReadOnly {
            option,
            creation_time: now_micros(),
            lease_config,
            pending_lease_ctr: u64::from(rand::random::<u32>() >> 1),
            ..Default::default()
        }
    }

    /// Adds a read only request into readonly struct.
    ///
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    ///
    /// `m` is the original read only request message from the local or remote node.
    pub fn add_request(&mut self, index: u64, req: Message, self_id: u64) {
        let ctx = {
            let key: &[u8] = req.entries[0].data.as_ref();
            if self.pending_read_index.contains_key(key) {
                return;
            }
            key.to_vec()
        };
        let mut acks = HashSet::<u64>::default();
        acks.insert(self_id);
        let status = ReadIndexStatus { req, index, acks };
        self.pending_read_index.insert(ctx.clone(), status);
        self.read_index_queue.push_back(ctx);
    }

    /// Notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    pub fn recv_ack(&mut self, id: u64, ctx: &[u8]) -> Option<&HashSet<u64>> {
        self.pending_read_index.get_mut(ctx).map(|rs| {
            rs.acks.insert(id);
            &rs.acks
        })
    }

    /// Advances the read only request queue kept by the ReadOnly struct.
    /// It dequeues the requests until it finds the read only request that has
    /// the same context as the given `ctx`.
    pub fn advance(&mut self, ctx: &[u8], logger: &Logger) -> Vec<ReadIndexStatus> {
        let mut rss = vec![];
        if let Some(i) = self.read_index_queue.iter().position(|x| {
            if !self.pending_read_index.contains_key(x) {
                fatal!(logger, "cannot find correspond read state from pending map");
            }
            *x == ctx
        }) {
            for _ in 0..=i {
                let rs = self.read_index_queue.pop_front().unwrap();
                let status = self.pending_read_index.remove(&rs).unwrap();
                rss.push(status);
            }
        }
        rss
    }

    /// Returns the context of the last pending read only request in ReadOnly struct.
    pub fn last_pending_request_ctx(&self) -> Option<Vec<u8>> {
        self.read_index_queue.back().cloned()
    }

    #[inline]
    pub fn pending_read_count(&self) -> usize {
        self.read_index_queue.len()
    }

    /// Whether reads are served under a time-limited lease: the leader's
    /// self-lease under both `LeaseBased` and `GrantLeases`, plus follower leases
    /// under `GrantLeases`. A newly elected leader under these options also
    /// waits out the new-leader safeguard, since a prior leader may still hold
    /// a self-lease.
    #[inline]
    pub fn uses_read_leases(&self) -> bool {
        matches!(
            self.option,
            ReadOnlyOption::LeaseBased | ReadOnlyOption::GrantLeases
        )
    }

    /// Whether the new-leader safeguard window has elapsed on the *timer*, i.e.
    /// every lease a previous leader granted or held has provably expired.
    /// Always true outside the leased options.
    /// `Raft::new_leader_safeguard_passed` also consults the confirmation
    /// shortcut, and is what callers should use.
    pub fn safeguard_has_passed(&mut self) -> bool {
        !self.uses_read_leases() || self.safeguard.has_passed()
    }

    /// Records that `node_id` has been heard from at this node's current term,
    /// and so has run `reset()` and dropped any prior-term lease. Ignored
    /// outside the leased options.
    pub(crate) fn confirm_at_term(&mut self, node_id: u64) {
        if self.uses_read_leases() {
            self.safeguard.confirm_at_term(node_id);
        }
    }

    /// Restores confirmations taken by `NewLeaderSafeguard::take_confirmations`.
    pub(crate) fn restore_confirmations(&mut self, confirmed: HashSet<u64>) {
        for id in confirmed {
            self.confirm_at_term(id);
        }
    }

    /// Bans every node in `ids` from being granted a lease (see
    /// `LeaderLeaseState::ban_grant`), and returns the first of them that
    /// currently holds an active lease, if any.
    pub(crate) fn ban_grants(&mut self, ids: &[u64]) -> Option<u64> {
        let mut holder = None;
        for &id in ids {
            self.leader.ban_grant(id);
            if holder.is_none() && self.has_active_granted_lease(id) {
                holder = Some(id);
            }
        }
        holder
    }

    /// Bars this leader from renewing its own lease for the rest of the term,
    /// and drops the lease it holds and any pending renewal, so from here on
    /// its reads take a heartbeat quorum round. Used when a configuration
    /// change removing this leader is in flight.
    pub(crate) fn ban_self_lease(&mut self) {
        self.leader.self_lease_banned = true;
        self.leader.pending_self_lease = None;
        self.held_lease = None;
    }

    /// Micros until the lease this node holds expires, or 0 if it holds none.
    /// Forgets the lease once it has expired.
    pub fn micros_until_held_lease_expired(&mut self) -> u64 {
        if !self.uses_read_leases() {
            return 0;
        }
        let expiry = match &self.held_lease {
            Some(l) => l.start_time + l.duration,
            None => return 0,
        };
        let now = now_micros();
        if expiry > now {
            expiry - now
        } else {
            self.held_lease = None;
            0
        }
    }

    /// Whether this node holds a live lease: a granted one on a follower, its
    /// self-lease on a leader.
    pub fn has_active_held_lease(&mut self) -> bool {
        self.micros_until_held_lease_expired() > 0
    }

    /// The lease this node holds, if it is still live.
    pub fn held_lease(&mut self) -> Option<&ReadLease> {
        if self.micros_until_held_lease_expired() == 0 {
            return None;
        }
        self.held_lease.as_ref()
    }

    /// The lease this leader granted `node_id`, if any (possibly expired).
    pub fn granted_lease(&self, node_id: u64) -> Option<&ReadLease> {
        if self.option != ReadOnlyOption::GrantLeases {
            return None;
        }
        self.leader.granted_leases.get(node_id)
    }

    /// Whether this leader has granted `node_id` a lease that is still live.
    pub fn has_active_granted_lease(&mut self, node_id: u64) -> bool {
        self.option == ReadOnlyOption::GrantLeases
            && self.leader.granted_leases.has_active_lease(node_id)
    }

    /// Whether `node_id` may take a lease under `max_leases`: a slot is free,
    /// or it already holds one.
    pub(crate) fn can_grant_lease_slot(&mut self, node_id: u64) -> bool {
        self.leader.granted_leases.num_active_leases() < self.lease_config.max_leases
            || self.leader.granted_leases.has_active_lease(node_id)
    }

    /// Whether the rate-limiter permits sending another `MsgAskReadLease`.
    pub fn can_ask_for_lease(&self) -> bool {
        if self.option != ReadOnlyOption::GrantLeases || !self.lease_config.ask_for_lease {
            return false;
        }
        now_micros() - self.last_lease_asked > LEASE_ASK_INTERVAL_MICROS
    }

    pub fn mark_asked_for_lease(&mut self) {
        self.last_lease_asked = now_micros();
    }

    /// Micros until this follower should next (re)ask the leader for a read
    /// lease, for the wrapper to arm a one-shot renewal timer on the poller
    /// thread. `0` = due now. This combines the renew-within-threshold rule
    /// (`maybe_ask_for_read_lease` asks once the lease is within
    /// `LEASE_RENEWAL_THRESHOLD_MICROS` of expiry) with the ask rate-limit, so a
    /// just-asked-but-not-yet-granted lease re-arms at the rate-limit interval
    /// rather than spinning.
    pub(crate) fn micros_until_lease_renewal(&mut self) -> u64 {
        let until_expired = self.micros_until_held_lease_expired();
        let until_threshold = until_expired.saturating_sub(LEASE_RENEWAL_THRESHOLD_MICROS);
        if until_threshold > 0 {
            // Lease still comfortably valid; next check is when it nears expiry.
            return until_threshold;
        }
        // Within the renewal window (or no lease held): ask as soon as the
        // rate-limiter next permits.
        let since_asked = now_micros().saturating_sub(self.last_lease_asked);
        LEASE_ASK_INTERVAL_MICROS.saturating_sub(since_asked)
    }

    /// Micros until this leader should next try to renew its self-lease, for
    /// the wrapper's renewal timer. `None` when there is nothing to schedule:
    /// not a leased option, self-banned, or a self-lease that has served no
    /// read since it installed (a read proposes a renewal itself). While a
    /// renewal is pending, the timer is due when that renewal goes stale and
    /// may be proposed again; a lost lease round is otherwise retried by the
    /// next lease event, not by the timer, so a partitioned leader costs at
    /// most one broadcast per lease duration.
    pub(crate) fn micros_until_self_lease_renewal(&mut self) -> Option<u64> {
        if !self.uses_read_leases() || self.leader.self_lease_banned {
            return None;
        }
        let now = now_micros();
        if let Some(p) = self.leader.pending_self_lease {
            let stale_at = p.start + self.lease_config.duration_micros;
            if now < stale_at {
                return Some(stale_at - now);
            }
        }
        if !self.leader.self_lease_used {
            return None;
        }
        Some(
            self.micros_until_held_lease_expired()
                .saturating_sub(LEASE_RENEWAL_THRESHOLD_MICROS),
        )
    }

    /// Mints a fresh lease id (namespaced by `node_id`, the caller's own id) and
    /// records its local start time in `pending_leases`.
    pub fn get_marked_lease_id(&mut self, node_id: u64) -> u64 {
        self.pending_lease_ctr += 1;
        let id = ((node_id & 0xFFFF_FFFF) << 32) | (self.pending_lease_ctr & 0xFFFF_FFFF);
        self.pending_leases.insert(id, now_micros());
        id
    }

    /// Forgets a lease id this follower asked for, after the leader refused it.
    pub(crate) fn clear_pending_lease(&mut self, lease_id: u64) {
        self.pending_leases.remove(&lease_id);
    }

    /// Processes a leader-granted lease: matches it to a pending ask by id,
    /// substitutes the locally-recorded start time, subtracts the drift
    /// safeguard, and installs it as this node's held lease. Returns the
    /// installed lease, or `None` if there is no matching pending ask or it has
    /// already expired.
    pub fn process_granted_lease(&mut self, lease: ReadLease) -> Option<ReadLease> {
        if self.option != ReadOnlyOption::GrantLeases {
            return None;
        }
        // Match the grant to a pending ask by its follower-minted id.
        let saved_start_time = match self.pending_leases.get(&lease.lease_id) {
            Some(t) => *t,
            None => return None, // This can happen due to a reset
        };

        if lease.duration == 0 || saved_start_time + lease.duration <= now_micros() {
            // Already expired; do not install.
            return None;
        }

        // Followers expire conservatively (earlier) by the drift safeguard.
        let follower_duration = lease.duration.saturating_sub(SAFEGUARD_CLOCK_DRIFT_MICROS);
        if follower_duration == 0 {
            return None;
        }
        // The granted ack index may only *raise* our own: the leader's value can
        // lag acks already in flight, and must never lower the serve threshold.
        self.read_lease_acked_index = cmp::max(self.read_lease_acked_index, lease.acked_index);
        let new_lease = ReadLease {
            lease_id: lease.lease_id,
            node_id: lease.node_id,
            log_index: lease.log_index,
            start_time: saved_start_time,
            duration: follower_duration,
            acked_index: self.read_lease_acked_index,
            last_ack_asked: 0,
        };
        self.held_lease = Some(new_lease.clone());
        // One lease at a time: clear every pending ask once one is granted.
        self.pending_leases.clear();
        Some(new_lease)
    }

    /// Installs the pending self-lease renewal if `quorum_round` covers it.
    pub(crate) fn maybe_install_self_lease(
        &mut self,
        quorum_round: u64,
        node_id: u64,
        log_index: u64,
    ) {
        let p = match self.leader.pending_self_lease {
            Some(p) if p.need_round <= quorum_round => p,
            _ => return,
        };
        self.leader.pending_self_lease = None;
        self.held_lease = Some(ReadLease {
            lease_id: 0,
            node_id,
            log_index,
            start_time: p.start,
            duration: self.lease_config.duration_micros,
            acked_index: 0,
            last_ack_asked: 0,
        });
        self.leader.self_lease_used = false;
    }

    /// Holds a follower's lease request until a majority echoes a round minted
    /// after it. If that follower already has a request held, the older one is
    /// kept and this one dropped, unless the older one is stale: followers
    /// re-ask every `LEASE_ASK_INTERVAL_MICROS`, and replacing the request each
    /// time would keep pushing back the round it waits for. The kept request
    /// must keep *its own* lease id: pairing a newer ask's id with the older
    /// request's round would let the follower date its lease from an ask the
    /// covering round does not postdate. Returns whether the request was held.
    pub(crate) fn park_grant(&mut self, node_id: u64, lease_id: u64, asked_index: u64) -> bool {
        if self.option != ReadOnlyOption::GrantLeases {
            return false;
        }
        if let Some(old) = self.leader.pending_grants.get(&node_id) {
            if !self.parked_grant_stale(old) {
                return false;
            }
        }
        self.leader.pending_grants.insert(
            node_id,
            PendingGrant {
                lease_id,
                proposed_at: now_micros(),
                asked_index,
                need_round: self.leader.lease_round + 1,
            },
        );
        true
    }

    /// Whether `pg` has been held for a full lease duration, so a lease dated
    /// from its request would already have expired.
    pub(crate) fn parked_grant_stale(&self, pg: &PendingGrant) -> bool {
        now_micros().saturating_sub(pg.proposed_at) >= self.lease_config.duration_micros
    }

    /// Records that this node has the log up to `index`, raising its own
    /// `read_lease_acked_index` (which makes the mark stick even with no lease
    /// installed) and the copy on its held lease.
    ///
    /// The only writer of this node's own ack promise. Leader-side updates to a
    /// *peer's* acked index go through `granted_leases.update_acked_index`.
    pub fn mark_self_acked_index(&mut self, index: u64) {
        if self.option != ReadOnlyOption::GrantLeases {
            return;
        }
        self.read_lease_acked_index = cmp::max(self.read_lease_acked_index, index);
        if let Some(l) = self.held_lease.as_mut() {
            l.acked_index = cmp::max(l.acked_index, index);
        }
    }

    /// This node's own read-lease ack index — the single authority for the
    /// follower serve gate and for what `MsgAskAckIndexResp` reports. Never
    /// decreases within a term.
    pub fn read_lease_acked_index(&self) -> u64 {
        self.read_lease_acked_index
    }

    pub fn mark_read_index_query_stat(&mut self, used: bool) {
        if self.option != ReadOnlyOption::GrantLeases {
            return;
        }
        self.read_lease_stats.times_got_read_query += 1;
        if used {
            self.read_lease_stats.times_read_lease_used += 1;
        }
    }

    /// Holds a not-yet-servable read locally, to be re-evaluated as commit
    /// advances. `required_index` is the commit index the follower must reach to
    /// serve it.
    pub(crate) fn add_delayed_read_index_req(&mut self, msg: Message, required_index: u64) {
        self.delayed_read_index_reqs.push_back(DelayedReadIndex {
            msg,
            forward_at_micros: now_micros() + READ_INDEX_LOCAL_HOLD_DURATION_MICROS,
            required_index,
        });
    }

    /// The earliest `forward_at_micros` among held reads, or
    /// `None` if the delayer is empty.
    pub(crate) fn next_read_index_delay_micros(&self) -> Option<u64> {
        self.delayed_read_index_reqs
            .front()
            .map(|d| d.forward_at_micros)
    }

    /// Micros from now until the earliest held read must be forwarded to the
    /// leader, or `None` if the delayer is empty. 0 is for a deadline
    /// in the past. Keeps the `now_micros()` clock internal to the crate
    /// so a wrapper can arm a timer with a relative duration.
    pub(crate) fn next_read_index_delay_timeout_micros(&self) -> Option<u64> {
        self.next_read_index_delay_micros()
            .map(|t| t.saturating_sub(now_micros()))
    }

    /// Pumps the delayer against the current `commit_index`, returning the
    /// `MsgReadIndex` messages that are now ready to replay via `step`:
    ///
    /// * `required_index <= commit_index` — commit caught up; the read is
    ///   served locally on replay (`index` set to `required_index`).
    /// * past `forward_at_micros` — gave up waiting; the read is forwarded to
    ///   the leader on replay (`reject = true`).
    pub(crate) fn fire_delayed_read_index_requests(&mut self, commit_index: u64) -> Vec<Message> {
        if self.delayed_read_index_reqs.is_empty() {
            return Vec::new();
        }
        let now = now_micros();
        let mut ready = Vec::new();
        while let Some(next) = self.delayed_read_index_reqs.front() {
            if next.required_index <= commit_index {
                let mut d = self.delayed_read_index_reqs.pop_front().unwrap();
                d.msg.index = d.required_index;
                ready.push(d.msg);
            } else if next.forward_at_micros <= now {
                let mut d = self.delayed_read_index_reqs.pop_front().unwrap();
                d.msg.index = d.required_index;
                d.msg.reject = true;
                ready.push(d.msg);
            } else {
                break;
            }
        }
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_index_msg(from: u64) -> Message {
        let mut m = Message::default();
        m.set_msg_type(crate::eraftpb::MessageType::MsgReadIndex);
        m.from = from;
        m
    }

    // A held read whose required index the commit has reached is released for
    // local serve: `index` set to `required_index`, `reject` left false.
    #[test]
    fn test_delayer_serves_on_commit_catchup() {
        let mut ro = ReadOnly::new(ReadOnlyOption::GrantLeases, ReadLeaseConfig::default());
        ro.add_delayed_read_index_req(read_index_msg(1), 10);
        // Commit still behind the required index: nothing released, still held.
        assert!(ro.fire_delayed_read_index_requests(9).is_empty());
        assert_eq!(ro.delayed_read_index_reqs.len(), 1);
        // Commit catches up: the read is released for local serve.
        let fired = ro.fire_delayed_read_index_requests(10);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].index, 10);
        assert!(!fired[0].reject);
        assert!(ro.delayed_read_index_reqs.is_empty());
    }

    // A held read past its forward deadline (commit never caught up) is released
    // for forwarding to the leader: `reject` set so the replay forwards.
    #[test]
    fn test_delayer_forwards_on_timeout() {
        let mut ro = ReadOnly::new(ReadOnlyOption::GrantLeases, ReadLeaseConfig::default());
        // Construct directly with an already-elapsed deadline to avoid sleeping.
        ro.delayed_read_index_reqs.push_back(DelayedReadIndex {
            msg: read_index_msg(1),
            forward_at_micros: 0,
            required_index: 10,
        });
        let fired = ro.fire_delayed_read_index_requests(9);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].index, 10);
        assert!(fired[0].reject);
        assert!(ro.delayed_read_index_reqs.is_empty());
    }

    // FIFO scan stops at the first request that is neither caught up nor timed
    // out, even if a later request would be servable (matches the Go fork).
    #[test]
    fn test_delayer_fifo_stops_at_first_unready() {
        let mut ro = ReadOnly::new(ReadOnlyOption::GrantLeases, ReadLeaseConfig::default());
        // Head: required 20, far in the future deadline — neither ready (commit
        // is 15) nor timed out.
        ro.delayed_read_index_reqs.push_back(DelayedReadIndex {
            msg: read_index_msg(1),
            forward_at_micros: u64::MAX,
            required_index: 20,
        });
        // Tail: would be servable at commit 15, but is blocked behind the head.
        ro.delayed_read_index_reqs.push_back(DelayedReadIndex {
            msg: read_index_msg(2),
            forward_at_micros: u64::MAX,
            required_index: 10,
        });
        assert!(ro.fire_delayed_read_index_requests(15).is_empty());
        assert_eq!(ro.delayed_read_index_reqs.len(), 2);
    }

    // A follower drives `update_acked_index` on every commit advance but never
    // calls `min_acked_index` (the leader-only commit clamp). The heap must stay
    // bounded regardless: each push prunes the entry it supersedes, so a single
    // lease collapses the heap back to one entry every time.
    #[test]
    fn test_follower_acked_heap_stays_bounded_without_min_acked_index() {
        let mut map = ReadLeaseMap::default();
        // The one lease a follower holds: keyed by its own node id.
        let node = 7;
        map.put(
            node,
            ReadLease {
                node_id: node,
                start_time: now_micros(),
                duration: 60_000_000, // far in the future; never expires mid-test
                acked_index: 0,
                ..Default::default()
            },
        );
        // Simulate a long run of commit advances. No `min_acked_index` call.
        for i in 1..=10_000 {
            map.update_acked_index(node, i);
            assert_eq!(map.acked_index_heap.len(), 1, "heap grew at i={i}");
        }
        // The live min still tracks the latest acked index.
        assert_eq!(map.min_acked_index(), 10_000);
    }

    // The delayer is empty after a reset-style rebuild of ReadOnly (mirrors the
    // Go fork clearing the delayer in `reset`).
    #[test]
    fn test_delayer_cleared_on_rebuild() {
        let mut ro = ReadOnly::new(ReadOnlyOption::GrantLeases, ReadLeaseConfig::default());
        ro.add_delayed_read_index_req(read_index_msg(1), 10);
        assert_eq!(ro.delayed_read_index_reqs.len(), 1);
        let rebuilt = ReadOnly::new(ro.option, ro.lease_config.clone());
        assert!(rebuilt.delayed_read_index_reqs.is_empty());
    }

    // Builds the ReadOnly of a follower with node id `node` holding a lease that
    // will not expire mid-test, whose recorded acked index is `acked`.
    fn follower_holding_lease(node: u64, acked: u64) -> ReadOnly {
        let mut ro = ReadOnly::new(
            ReadOnlyOption::GrantLeases,
            ReadLeaseConfig {
                duration_micros: 60_000_000,
                max_leases: 5,
                ask_for_lease: true,
                catchup_margin: 10,
            },
        );
        ro.held_lease = Some(ReadLease {
            node_id: node,
            start_time: now_micros(),
            duration: 60_000_000,
            acked_index: acked,
            ..Default::default()
        });
        ro.read_lease_acked_index = acked;
        ro
    }

    // Regression (review F1, path A): a renewal grant carrying a leader-side
    // acked index staler than what this follower has already self-marked must
    // not lower its serve threshold.
    #[test]
    fn test_granted_lease_cannot_lower_follower_acked_index() {
        let node = 7;
        let mut ro = follower_holding_lease(node, 100);
        // Our log advanced well past what the leader has observed.
        ro.mark_self_acked_index(250);
        assert_eq!(ro.read_lease_acked_index(), 250);

        // A renewal lands carrying the leader's stale view.
        let lease_id = ro.get_marked_lease_id(node);
        let granted = ro.process_granted_lease(ReadLease {
            lease_id,
            node_id: node,
            log_index: 90,
            start_time: 0, // discarded; the locally recorded ask time is used
            duration: 60_000_000,
            acked_index: 100,
            last_ack_asked: 0,
        });
        assert!(granted.is_some(), "grant should have been installed");
        assert_eq!(
            ro.read_lease_acked_index(),
            250,
            "stale grant lowered the follower's ack index"
        );
        assert_eq!(
            ro.held_lease().unwrap().acked_index,
            250,
            "installed lease should carry the node-global ack index"
        );
    }

    // Regression (review F1, path B): marks made while no lease is installed —
    // first acquisition, or a gap between leases — must still stick.
    #[test]
    fn test_acked_index_survives_a_lease_gap() {
        let node = 7;
        let mut ro = follower_holding_lease(node, 100);
        // The lease lapses and is reaped.
        ro.held_lease = None;
        assert!(ro.held_lease().is_none());

        // Appends keep arriving and we keep self-marking with no lease held.
        ro.mark_self_acked_index(300);
        assert_eq!(
            ro.read_lease_acked_index(),
            300,
            "mark made during a lease gap was dropped"
        );

        // The next grant must not walk it back to the leader's view.
        let lease_id = ro.get_marked_lease_id(node);
        ro.process_granted_lease(ReadLease {
            lease_id,
            node_id: node,
            log_index: 120,
            start_time: 0,
            duration: 60_000_000,
            acked_index: 120,
            last_ack_asked: 0,
        })
        .expect("grant should have been installed");
        assert_eq!(ro.read_lease_acked_index(), 300);
    }

    // The node-global ack index is monotone within a term regardless of the
    // order marks arrive in, and a fresher grant still raises it.
    #[test]
    fn test_acked_index_is_monotone_within_a_term() {
        let node = 7;
        let mut ro = follower_holding_lease(node, 100);
        ro.mark_self_acked_index(400);
        ro.mark_self_acked_index(200); // out-of-order / stale mark
        assert_eq!(ro.read_lease_acked_index(), 400);

        // A grant ahead of us does raise it.
        let lease_id = ro.get_marked_lease_id(node);
        ro.process_granted_lease(ReadLease {
            lease_id,
            node_id: node,
            log_index: 400,
            start_time: 0,
            duration: 60_000_000,
            acked_index: 500,
            last_ack_asked: 0,
        })
        .expect("grant should have been installed");
        assert_eq!(ro.read_lease_acked_index(), 500);

        // A term change discharges the promise along with the lease state.
        let rebuilt = ReadOnly::new(ro.option, ro.lease_config.clone());
        assert_eq!(rebuilt.read_lease_acked_index(), 0);
    }
}
