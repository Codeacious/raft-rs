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
use crate::{HashMap, HashSet};

/// Allowed clock drift for read leases.
/// This amount is subtracted from the read lease duration at the follower.
pub(crate) const SAFEGUARD_CLOCK_DRIFT_MICROS: u64 = 1_000; // 1ms
/// Minimum interval between a follower's `MsgAskReadLease` sends.
pub(crate) const LEASE_ASK_INTERVAL_MICROS: u64 = 10_000; // 10ms
/// Minimum interval between a leader's `MsgAskAckIndex` pings to the same lease
/// holder.
pub(crate) const ACK_INDEX_ASK_INTERVAL_MICROS: u64 = 5_000; // 5ms
/// Followers renew their read lease once it's within this many microseconds of expiry.
pub(crate) const LEASE_RENEWAL_THRESHOLD_MICROS: u64 = 150_000; // 150ms
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

#[derive(Default, Debug, Clone)]
pub struct ReadLeaseMap {
    /// Granted leases keyed by node_id.
    pub(crate) leases: HashMap<u64, ReadLease>,
    /// Min-heap on acked index. Entries are (acked_index, node_id).
    acked_index_heap: BinaryHeap<Reverse<(u64, u64)>>,
    /// Soonest lease-expiry time, in `now_micros()` microseconds. Private cache
    /// maintained by `update_soonest_expiry`/`cleanup_expired_leases`.
    soonest_expiry: u64,
    /// Pending (asked-for, not-yet-granted) leases: `lease_id -> start_time_micros`.
    /// Private: only the follower ask/grant handlers in this module touch it.
    pending_leases: HashMap<u64, u64>,
    /// Monotonic counter feeding the low 32 bits of a minted lease id. Private.
    pending_lease_ctr: u64,
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

    /// Mints a fresh lease id for a follower's `MsgAskReadLease`, namespaced by
    /// `node_id`: the low 32 bits are a per-node monotonic counter, the high 32
    /// bits are the node id. This makes lease ids globally unique across
    /// followers.
    pub fn get_new_lease_id(&mut self, node_id: u64) -> u64 {
        self.pending_lease_ctr += 1;
        ((node_id & 0xFFFF_FFFF) << 32) | (self.pending_lease_ctr & 0xFFFF_FFFF)
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
    /// Time (micros) this `ReadOnly` was created. Observability only — the
    /// safeguard clock is `safeguard_micros_start`, which starts later.
    pub creation_time: u64,
    /// Whether the new-leader safeguard window has elapsed. Latches.
    pub safeguard_passed: bool,
    /// Phase 1 of the new-leader safeguard: leader ticks still to elapse before
    /// the wall-clock phase may begin. See `start_new_leader_safeguard`.
    safeguard_ticks_remaining: usize,
    /// `now_micros()` at which phase 2 began, or 0 while phase 1 is incomplete.
    safeguard_micros_start: u64,
    /// Nodes barred from being granted a read lease for the rest of this term,
    /// because a configuration change removing them is in flight.
    lease_banned: HashSet<u64>,
    /// Peers this node has heard from at its *current* term. Such a peer has
    /// necessarily run `reset()`, so it holds no lease from any earlier term.
    confirmed_at_term: HashSet<u64>,
    /// Node ids named by conf-change entries in the unapplied log tail at
    /// election time: an unapplied addition is absent from `prs` but may still
    /// hold a prior-term lease, so the shortcut must hear from it too.
    safeguard_required: HashSet<u64>,
    /// The election-time tail scan failed, so `safeguard_required` and
    /// `lease_banned` may be incomplete. Disables the shortcut entirely.
    safeguard_scan_failed: bool,
    pub lease_map: ReadLeaseMap,
    /// This node's *own* read-lease ack index: the highest log index it has
    /// promised the leader it will not serve below. Node-global and **monotone
    /// within a term**, deliberately not stored on the `ReadLease` — a lease's
    /// copy is only as fresh as the grant that installed it and dies with the
    /// lease. Never persisted; `reset()` rebuilds it, and a crash discharges the
    /// promise along with the lease.
    read_lease_acked_index: u64,
}

impl ReadOnly {
    /// Builds a `ReadOnly` for the given option and lease config. Pass
    /// `ReadLeaseConfig::default()` when leases are irrelevant (non-`GrantLeases`
    /// options, or tests); a real Raft passes `Config::read_lease_config`.
    pub fn new(option: ReadOnlyOption, lease_config: ReadLeaseConfig) -> ReadOnly {
        ReadOnly {
            option,
            creation_time: now_micros(),
            lease_config,
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

    /// Arms the two-phase new-leader safeguard: `election_ticks` ticks
    /// (`tick_safeguard`), then `lease_duration + SAFEGUARD_CLOCK_DRIFT_MICROS`
    /// micros. The phases are sequential and the wait is their **sum**, not
    /// their maximum. Called from `become_leader`.
    pub(crate) fn start_new_leader_safeguard(&mut self, election_ticks: usize) {
        self.safeguard_passed = false;
        self.safeguard_ticks_remaining = election_ticks;
        // Degenerate config: no tick phase, so phase 2 starts immediately.
        self.safeguard_micros_start = if election_ticks == 0 { now_micros() } else { 0 };
    }

    /// Advances phase 1 by one tick, starting phase 2 when it completes.
    /// Driven from the leader branch of `tick_heartbeat`.
    pub(crate) fn tick_safeguard(&mut self) {
        if self.safeguard_ticks_remaining == 0 {
            return;
        }
        self.safeguard_ticks_remaining -= 1;
        if self.safeguard_ticks_remaining == 0 {
            self.safeguard_micros_start = now_micros();
        }
    }

    /// Whether the new-leader safeguard window has elapsed on the *timer*, i.e.
    /// every lease a previous leader granted has provably expired.
    /// `Raft::new_leader_safeguard_passed` also consults the confirmation
    /// shortcut, and is what callers should use.
    pub fn safeguard_has_passed(&mut self) -> bool {
        if self.option != ReadOnlyOption::GrantLeases {
            return true;
        }
        if self.safeguard_passed {
            return true;
        }
        // Phase 1 (ticks) not finished yet.
        if self.safeguard_micros_start == 0 {
            return false;
        }
        if now_micros().saturating_sub(self.safeguard_micros_start)
            > self.lease_config.duration_micros + SAFEGUARD_CLOCK_DRIFT_MICROS
        {
            self.safeguard_passed = true;
        }
        self.safeguard_passed
    }

    /// Latches the safeguard as passed. Used by the confirmation shortcut, so a
    /// later configuration change cannot un-pass a window already lifted.
    pub(crate) fn mark_safeguard_passed(&mut self) {
        self.safeguard_passed = true;
    }

    /// Bars `node_id` from being granted a read lease for the rest of this
    /// term, because a configuration change removing it is in flight.
    pub(crate) fn ban_lease(&mut self, node_id: u64) {
        self.lease_banned.insert(node_id);
    }

    pub(crate) fn is_lease_banned(&self, node_id: u64) -> bool {
        self.lease_banned.contains(&node_id)
    }

    /// Records that `node_id` has been heard from at this node's current term,
    /// and so has run `reset()` and dropped any prior-term lease.
    pub(crate) fn confirm_at_term(&mut self, node_id: u64) {
        if self.option == ReadOnlyOption::GrantLeases && !self.safeguard_passed {
            self.confirmed_at_term.insert(node_id);
        }
    }

    pub(crate) fn is_confirmed_at_term(&self, node_id: u64) -> bool {
        self.confirmed_at_term.contains(&node_id)
    }

    /// Adds a node the safeguard shortcut must hear from beyond the current
    /// configuration (named by an unapplied conf change at election time).
    pub(crate) fn require_confirmation(&mut self, node_id: u64) {
        self.safeguard_required.insert(node_id);
    }

    pub(crate) fn extra_required_confirmations(&self) -> &HashSet<u64> {
        &self.safeguard_required
    }

    /// Records that the election-time tail scan failed, disabling the shortcut.
    pub(crate) fn mark_safeguard_scan_failed(&mut self) {
        self.safeguard_scan_failed = true;
    }

    pub(crate) fn safeguard_scan_failed(&self) -> bool {
        self.safeguard_scan_failed
    }

    pub fn get_read_lease(&self, node_id: u64) -> Option<&ReadLease> {
        if self.option != ReadOnlyOption::GrantLeases {
            return None;
        }
        self.lease_map.leases.get(&node_id)
    }

    pub fn micros_until_lease_expired(&mut self, node_id: u64) -> u64 {
        if self.option != ReadOnlyOption::GrantLeases {
            return 0;
        }
        // Copy the expiry out so the immutable borrow ends before the cleanup.
        let expiry = match self.get_read_lease(node_id) {
            Some(l) => l.start_time + l.duration,
            None => return 0,
        };
        let now = now_micros();
        if expiry > now {
            expiry - now
        } else {
            self.lease_map.cleanup_expired_leases();
            0
        }
    }

    pub fn has_active_read_lease(&mut self, node_id: u64) -> bool {
        self.micros_until_lease_expired(node_id) > 0
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

    /// Micros until this node should next (re)ask the leader for a read lease,
    /// for the wrapper to arm a one-shot renewal timer on the poller thread.
    /// `0` = due now. This combines the renew-within-threshold rule
    /// (`maybe_ask_for_read_lease` asks once the lease is within
    /// `LEASE_RENEWAL_THRESHOLD_MICROS` of expiry) with the ask rate-limit, so a
    /// just-asked-but-not-yet-granted lease re-arms at the rate-limit interval
    /// rather than spinning.
    pub(crate) fn micros_until_lease_renewal(&mut self, node_id: u64) -> u64 {
        let until_expired = self.micros_until_lease_expired(node_id);
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

    /// Mints a fresh lease id (namespaced by `node_id`, the caller's own id) and
    /// records its local start time in `pending_leases`.
    pub fn get_marked_lease_id(&mut self, node_id: u64) -> u64 {
        let id = self.lease_map.get_new_lease_id(node_id);
        self.lease_map.pending_leases.insert(id, now_micros());
        id
    }

    /// Processes a leader-granted lease: matches it to a pending ask by id,
    /// substitutes the locally-recorded start time, subtracts the drift
    /// safeguard, and installs it. Returns the installed lease, or `None` if
    /// there is no matching pending ask or it has already expired.
    pub fn process_granted_lease(&mut self, lease: ReadLease) -> Option<ReadLease> {
        if self.option != ReadOnlyOption::GrantLeases {
            return None;
        }
        // Match the grant to a pending ask by its follower-minted id.
        let saved_start_time = match self.lease_map.pending_leases.get(&lease.lease_id) {
            Some(t) => *t,
            None => return None,  // This can happen due to a reset
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
        let node_id = new_lease.node_id;
        let installed = new_lease.clone();
        self.lease_map.put(node_id, new_lease);
        // One lease at a time: clear every pending ask once one is granted.
        self.lease_map.pending_leases.clear();
        Some(installed)
    }

    /// Records that this node has the log up to `index`, raising both its own
    /// `read_lease_acked_index` (which makes the mark stick even with no lease
    /// installed) and the acked index of the lease keyed by `node_id`.
    ///
    /// The follower self-mark entry point, only ever called with this node's own
    /// id. Leader-side updates to a *peer's* acked index go through
    /// `lease_map.update_acked_index` directly.
    pub fn mark_node_acked_index(&mut self, node_id: u64, index: u64) {
        if self.option != ReadOnlyOption::GrantLeases {
            return;
        }
        self.read_lease_acked_index = cmp::max(self.read_lease_acked_index, index);
        self.lease_map.update_acked_index(node_id, index);
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
        ro.lease_map.put(
            node,
            ReadLease {
                node_id: node,
                start_time: now_micros(),
                duration: 60_000_000,
                acked_index: acked,
                ..Default::default()
            },
        );
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
        ro.mark_node_acked_index(node, 250);
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
            ro.get_read_lease(node).unwrap().acked_index,
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
        ro.lease_map.leases.clear();
        ro.lease_map.soonest_expiry = 0;
        assert!(ro.get_read_lease(node).is_none());

        // Appends keep arriving and we keep self-marking with no lease held.
        ro.mark_node_acked_index(node, 300);
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
        ro.mark_node_acked_index(node, 400);
        ro.mark_node_acked_index(node, 200); // out-of-order / stale mark
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
