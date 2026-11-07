// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
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
use std::ops::{Deref, DerefMut};

use crate::eraftpb::{
    ConfChange, ConfChangeType, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
    MessageType, Snapshot,
};
use protobuf::Message as _;
use raft_proto::ConfChangeI;
use rand::Rng;
use slog::Logger;

#[cfg(feature = "failpoints")]
use fail::fail_point;
use getset::Getters;
use slog::{debug, error, info, o, trace, warn};

use super::errors::{Error, Result, StorageError};
use super::raft_log::RaftLog;
use super::read_only::{
    now_micros, ReadLease, ReadOnly, ReadOnlyOption, ReadState, LEASE_RENEWAL_THRESHOLD_MICROS,
    READ_INDEX_LOCAL_HOLD_LOG_THRESHOLD, SAFEGUARD_CLOCK_DRIFT_MICROS,
};
use super::storage::{GetEntriesContext, GetEntriesFor, Storage};
use super::Config;
use crate::confchange::Changer;
use crate::quorum::VoteResult;
use crate::util;
use crate::util::NO_LIMIT;
use crate::{confchange, Progress, ProgressState, ProgressTracker};

// CAMPAIGN_PRE_ELECTION represents the first phase of a normal election when
// Config.pre_vote is true.
#[doc(hidden)]
pub const CAMPAIGN_PRE_ELECTION: &[u8] = b"CampaignPreElection";
#[doc(hidden)]
// CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase
// of the election when Config.pre_vote is true).
#[doc(hidden)]
pub const CAMPAIGN_ELECTION: &[u8] = b"CampaignElection";
#[doc(hidden)]
// CAMPAIGN_TRANSFER represents the type of leader transfer.
#[doc(hidden)]
pub const CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";

/// The role of the node.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum StateRole {
    /// The node is a follower of the leader.
    #[default]
    Follower,
    /// The node could become a leader.
    Candidate,
    /// The node is a leader.
    Leader,
    /// The node could become a candidate, if `prevote` is enabled.
    PreCandidate,
}

/// How a read index was obtained; recorded on the resulting `ReadState`.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ReadProvenance {
    /// Confirmed by a heartbeat quorum round (or a single-voter group).
    Quorum,
    /// Served from this leader's own self-lease.
    LeaderLease,
    /// Served from this follower's granted lease.
    FollowerLease,
}

/// A constant represents invalid id of raft.
pub const INVALID_ID: u64 = 0;
/// A constant represents invalid index of raft log.
pub const INVALID_INDEX: u64 = 0;

/// SoftState provides state that is useful for logging and debugging.
/// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq, Eq, Debug)]
pub struct SoftState {
    /// The potential leader of the cluster.
    pub leader_id: u64,
    /// The soft role this node may take.
    pub raft_state: StateRole,
}

/// UncommittedState is used to keep track of information of uncommitted
/// log entries on 'leader' node
struct UncommittedState {
    /// Specify maximum of uncommitted entry size.
    /// When this limit is reached, all proposals to append new log will be dropped
    max_uncommitted_size: usize,

    /// Record current uncommitted entries size.
    uncommitted_size: usize,

    /// Record index of last log entry when node becomes leader from candidate.
    /// See https://github.com/tikv/raft-rs/pull/398#discussion_r502417531 for more detail
    last_log_tail_index: u64,
}

impl UncommittedState {
    #[inline]
    pub fn is_no_limit(&self) -> bool {
        self.max_uncommitted_size == NO_LIMIT as usize
    }

    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        // fast path
        if self.is_no_limit() {
            return true;
        }

        let size: usize = ents.iter().map(|ent| ent.get_data().len()).sum();

        // 1. we should never drop an entry without any data(eg. leader election)
        // 2. we should allow at least one uncommitted entry
        // 3. add these entries will not cause size overlimit
        if size == 0
            || self.uncommitted_size == 0
            || size + self.uncommitted_size <= self.max_uncommitted_size
        {
            self.uncommitted_size += size;
            true
        } else {
            false
        }
    }

    pub fn maybe_reduce_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        // fast path
        if self.is_no_limit() || ents.is_empty() {
            return true;
        }

        // user may advance a 'Ready' which is generated before this node becomes leader
        let size: usize = ents
            .iter()
            .skip_while(|ent| ent.index <= self.last_log_tail_index)
            .map(|ent| ent.get_data().len())
            .sum();

        if size > self.uncommitted_size {
            self.uncommitted_size = 0;
            false
        } else {
            self.uncommitted_size -= size;
            true
        }
    }
}

/// The core struct of raft consensus.
///
/// It's a helper struct to get around rust borrow checks.
#[derive(Getters)]
pub struct RaftCore<T: Storage> {
    /// The current election term.
    pub term: u64,

    /// Which peer this raft is voting for.
    pub vote: u64,

    /// The ID of this node.
    pub id: u64,

    /// The current read states.
    pub read_states: Vec<ReadState>,

    /// `MsgReadIndex` messages received while this leader had not yet
    /// committed an entry in its own term. Answered by
    /// `release_pending_read_index_messages` once the first commit of the term
    /// lands. Not cleared by `reset()`.
    pending_read_index_messages: Vec<Message>,

    /// The persistent log.
    pub raft_log: RaftLog<T>,

    /// The maximum number of messages that can be inflight.
    pub max_inflight: usize,

    /// The maximum length (in bytes) of all the entries.
    pub max_msg_size: u64,

    /// The peer is requesting snapshot, it is the index that the follower
    /// needs it to be included in a snapshot.
    pub pending_request_snapshot: u64,

    /// The current role of this node.
    pub state: StateRole,

    /// Indicates whether state machine can be promoted to leader,
    /// which is true when it's a voter and its own id is in progress list.
    promotable: bool,

    /// The leader id.
    pub leader_id: u64,

    /// ID of the leader transfer target when its value is not None.
    ///
    /// If this is Some(id), we follow the procedure defined in raft thesis 3.10.
    pub lead_transferee: Option<u64>,

    /// Only one conf change may be pending (in the log, but not yet
    /// applied) at a time. This is enforced via `pending_conf_index`, which
    /// is set to a value >= the log index of the latest pending
    /// configuration change (if any). Config changes are only allowed to
    /// be proposed if the leader's applied index is greater than this
    /// value.
    ///
    /// This value is conservatively set in cases where there may be a configuration change pending,
    /// but scanning the log is possibly expensive. This implies that the index stated here may not
    /// necessarily be a config change entry, and it may not be a `BeginMembershipChange` entry, even if
    /// we set this to one.
    pub pending_conf_index: u64,

    /// The queue of read-only requests.
    pub read_only: ReadOnly,

    /// Ticks since it reached last electionTimeout when it is leader or candidate.
    /// Number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    pub election_elapsed: usize,

    /// Number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,

    /// Whether to check the quorum
    pub check_quorum: bool,

    /// Enable the prevote algorithm.
    ///
    /// This enables a pre-election vote round on Candidates prior to disrupting the cluster.
    ///
    /// Enable this if greater cluster stability is preferred over faster elections.
    pub pre_vote: bool,

    skip_bcast_commit: bool,
    batch_append: bool,
    disable_proposal_forwarding: bool,

    heartbeat_timeout: usize,
    election_timeout: usize,

    // randomized_election_timeout is a random number between
    // [min_election_timeout, max_election_timeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    randomized_election_timeout: usize,
    min_election_timeout: usize,
    max_election_timeout: usize,

    /// The logger for the raft structure.
    pub(crate) logger: slog::Logger,

    /// The election priority of this node.
    pub priority: i64,

    /// Track uncommitted log entry on this node.
    uncommitted_state: UncommittedState,

    /// Max size per committed entries in a `Read`.
    pub(crate) max_committed_size_per_ready: u64,
}

/// A struct that represents the raft consensus itself. Stores details concerning the current
/// and possible state the system can take.
pub struct Raft<T: Storage> {
    prs: ProgressTracker,

    /// The list of messages.
    pub msgs: Vec<Message>,
    /// Internal raftCore.
    pub r: RaftCore<T>,
}

impl<T: Storage> Deref for Raft<T> {
    type Target = RaftCore<T>;

    #[inline]
    fn deref(&self) -> &RaftCore<T> {
        &self.r
    }
}

impl<T: Storage> DerefMut for Raft<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.r
    }
}

#[allow(dead_code)] // ensure Raft<T> is always Send
trait AssertSend: Send {}
impl<T: Storage + Send> AssertSend for Raft<T> {}

fn new_message(to: u64, field_type: MessageType, from: Option<u64>) -> Message {
    let mut m = Message::default();
    m.to = to;
    if let Some(id) = from {
        m.from = id;
    }
    m.set_msg_type(field_type);
    m
}

fn get_priority(m: &Message) -> i64 {
    if m.priority != 0 {
        m.priority
    } else {
        i64::try_from(m.deprecated_priority).unwrap_or(i64::MAX)
    }
}

/// Maps vote and pre_vote message types to their correspond responses.
pub fn vote_resp_msg_type(t: MessageType) -> MessageType {
    match t {
        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
        _ => panic!("Not a vote message: {:?}", t),
    }
}

impl<T: Storage> Raft<T> {
    /// Creates a new raft for use on the node.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(c: &Config, store: T, logger: &Logger) -> Result<Self> {
        c.validate()?;
        let logger = logger.new(o!("raft_id" => c.id));
        let raft_state = store.initial_state()?;
        let conf_state = &raft_state.conf_state;
        let voters = &conf_state.voters;
        let learners = &conf_state.learners;

        let mut r = Raft {
            prs: ProgressTracker::with_capacity(voters.len(), learners.len(), c.max_inflight_msgs),
            msgs: Default::default(),
            r: RaftCore {
                id: c.id,
                read_states: Default::default(),
                pending_read_index_messages: Default::default(),
                raft_log: RaftLog::new(store, logger.clone(), c),
                max_inflight: c.max_inflight_msgs,
                max_msg_size: c.max_size_per_msg,
                pending_request_snapshot: INVALID_INDEX,
                state: StateRole::Follower,
                promotable: false,
                check_quorum: c.check_quorum,
                pre_vote: c.pre_vote,
                read_only: ReadOnly::new(c.read_only_option, c.read_lease_config()),
                heartbeat_timeout: c.heartbeat_tick,
                election_timeout: c.election_tick,
                leader_id: Default::default(),
                lead_transferee: None,
                term: Default::default(),
                election_elapsed: Default::default(),
                pending_conf_index: Default::default(),
                vote: Default::default(),
                heartbeat_elapsed: Default::default(),
                randomized_election_timeout: Default::default(),
                min_election_timeout: c.min_election_tick(),
                max_election_timeout: c.max_election_tick(),
                skip_bcast_commit: c.skip_bcast_commit,
                batch_append: c.batch_append,
                logger,
                priority: c.priority,
                uncommitted_state: UncommittedState {
                    max_uncommitted_size: c.max_uncommitted_size as usize,
                    uncommitted_size: 0,
                    last_log_tail_index: 0,
                },
                max_committed_size_per_ready: c.max_committed_size_per_ready,
                disable_proposal_forwarding: c.disable_proposal_forwarding,
            },
        };
        confchange::restore(&mut r.prs, r.r.raft_log.last_index(), conf_state)?;
        let new_cs = r.post_conf_change();
        if !raft_proto::conf_state_eq(&new_cs, conf_state) {
            fatal!(
                r.logger,
                "invalid restore: {:?} != {:?}",
                conf_state,
                new_cs
            );
        }

        if raft_state.hard_state != HardState::default() {
            r.load_state(&raft_state.hard_state);
        }
        if c.applied > 0 {
            // at initialize, it is possible that applied_index > committed_index,
            // so we should skip the check at `commit_apply`.
            r.commit_apply_internal(c.applied, true);
        }
        r.become_follower(r.term, INVALID_ID);

        info!(
            r.logger,
            "newRaft";
            "term" => r.term,
            "commit" => r.raft_log.committed,
            "applied" => r.raft_log.applied,
            "last index" => r.raft_log.last_index(),
            "last term" => r.raft_log.last_term(),
            "peers" => ?r.prs.conf().voters,
        );
        Ok(r)
    }

    /// Sets priority of node.
    pub fn set_priority(&mut self, priority: i64) {
        self.priority = priority;
    }

    /// Creates a new raft for use on the node with the default logger.
    ///
    /// The default logger is an `slog` to `log` adapter.
    #[allow(clippy::new_ret_no_self)]
    #[cfg(feature = "default-logger")]
    pub fn with_default_logger(c: &Config, store: T) -> Result<Self> {
        Self::new(c, store, &crate::default_logger())
    }

    /// Grabs an immutable reference to the store.
    #[inline]
    pub fn store(&self) -> &T {
        &self.raft_log.store
    }

    /// Grabs a mutable reference to the store.
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        &mut self.raft_log.store
    }

    /// Grabs a reference to the snapshot
    #[inline]
    pub fn snap(&self) -> Option<&Snapshot> {
        self.raft_log.unstable.snapshot.as_ref()
    }

    /// Returns the number of pending read-only messages.
    #[inline]
    pub fn pending_read_count(&self) -> usize {
        self.read_only.pending_read_count()
    }

    /// Returns how many read states exist.
    #[inline]
    pub fn ready_read_count(&self) -> usize {
        self.read_states.len()
    }

    /// Returns a value representing the softstate at the time of calling.
    pub fn soft_state(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.state,
        }
    }

    /// Returns a value representing the hardstate at the time of calling.
    pub fn hard_state(&self) -> HardState {
        let mut hs = HardState::default();
        hs.term = self.term;
        hs.vote = self.vote;
        hs.commit = self.raft_log.committed;
        hs
    }

    /// Returns whether the current raft is in lease.
    pub fn in_lease(&self) -> bool {
        self.state == StateRole::Leader && self.check_quorum
    }

    /// For testing leader lease
    #[doc(hidden)]
    pub fn set_randomized_election_timeout(&mut self, t: usize) {
        assert!(self.min_election_timeout <= t && t < self.max_election_timeout);
        self.randomized_election_timeout = t;
    }

    /// Fetch the length of the election timeout.
    pub fn election_timeout(&self) -> usize {
        self.election_timeout
    }

    /// Fetch the length of the heartbeat timeout
    pub fn heartbeat_timeout(&self) -> usize {
        self.heartbeat_timeout
    }

    /// Fetch the number of ticks elapsed since last heartbeat.
    pub fn heartbeat_elapsed(&self) -> usize {
        self.heartbeat_elapsed
    }

    /// Return the length of the current randomized election timeout.
    pub fn randomized_election_timeout(&self) -> usize {
        self.randomized_election_timeout
    }

    /// Set whether skip broadcast empty commit messages at runtime.
    #[inline]
    pub fn skip_bcast_commit(&mut self, skip: bool) {
        self.skip_bcast_commit = skip;
    }

    /// Set whether batch append msg at runtime.
    #[inline]
    pub fn set_batch_append(&mut self, batch_append: bool) {
        self.batch_append = batch_append;
    }

    /// Configures group commit.
    ///
    /// If group commit is enabled, only logs replicated to at least two
    /// different groups are committed.
    ///
    /// You should use `assign_commit_groups` to configure peer groups.
    pub fn enable_group_commit(&mut self, enable: bool) {
        self.mut_prs().enable_group_commit(enable);
        if StateRole::Leader == self.state && !enable && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// Whether enable group commit.
    pub fn group_commit(&self) -> bool {
        self.prs().group_commit()
    }

    /// Assigns groups to peers.
    ///
    /// The tuple is (`peer_id`, `group_id`). `group_id` should be larger than 0.
    ///
    /// The group information is only stored in memory. So you need to configure
    /// it every time a raft state machine is initialized or a snapshot is applied.
    pub fn assign_commit_groups(&mut self, ids: &[(u64, u64)]) {
        let prs = self.mut_prs();
        for (peer_id, group_id) in ids {
            assert!(*group_id > 0);
            if let Some(pr) = prs.get_mut(*peer_id) {
                pr.commit_group_id = *group_id;
            } else {
                continue;
            }
        }
        if StateRole::Leader == self.state && self.group_commit() && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// Removes all commit group configurations.
    pub fn clear_commit_group(&mut self) {
        for (_, pr) in self.mut_prs().iter_mut() {
            pr.commit_group_id = 0;
        }
    }

    /// Checks whether the raft group is using group commit and consistent
    /// over group.
    ///
    /// If it can't get a correct answer, `None` is returned.
    pub fn check_group_commit_consistent(&mut self) -> Option<bool> {
        if self.state != StateRole::Leader {
            return None;
        }
        // Previous leader may have reach consistency already.
        //
        // check applied_index instead of committed_index to avoid pending conf change.
        if !self.apply_to_current_term() {
            return None;
        }
        let (index, use_group_commit) = self.mut_prs().maximal_committed_index();
        debug!(
            self.logger,
            "check group commit consistent";
            "index" => index,
            "use_group_commit" => use_group_commit,
            "committed" => self.raft_log.committed
        );
        Some(use_group_commit && index == self.raft_log.committed)
    }

    /// Checks if logs are committed to its term.
    ///
    /// The check is useful usually when raft is leader.
    pub fn commit_to_current_term(&self) -> bool {
        self.raft_log
            .term(self.raft_log.committed)
            .is_ok_and(|t| t == self.term)
    }

    /// Checks if logs are applied to current term.
    pub fn apply_to_current_term(&self) -> bool {
        self.raft_log
            .term(self.raft_log.applied)
            .is_ok_and(|t| t == self.term)
    }

    /// Set `max_committed_size_per_ready` to `size`.
    pub fn set_max_committed_size_per_ready(&mut self, size: u64) {
        self.max_committed_size_per_ready = size;
    }

    /// Set whether or not `check_quorum`.
    pub fn set_check_quorum(&mut self, check_quorum: bool) {
        self.check_quorum = check_quorum;
    }

    /// Set the maximum limit that applied index can be ahead of persisted index.
    pub fn set_max_apply_unpersisted_log_limit(&mut self, limit: u64) {
        self.raft_log.max_apply_unpersisted_log_limit = limit;
    }
}

impl<T: Storage> RaftCore<T> {
    // send persists state to stable storage and then sends to its mailbox.
    fn send(&mut self, mut m: Message, msgs: &mut Vec<Message>) {
        debug!(
            self.logger,
            "Sending from {from} to {to}",
            from = self.id,
            to = m.to;
            "msg" => ?m,
        );
        if m.from == INVALID_ID {
            m.from = self.id;
        }
        if m.get_msg_type() == MessageType::MsgRequestVote
            || m.get_msg_type() == MessageType::MsgRequestPreVote
            || m.get_msg_type() == MessageType::MsgRequestVoteResponse
            || m.get_msg_type() == MessageType::MsgRequestPreVoteResponse
        {
            if m.term == 0 {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.Term is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign,
                //   non-zero as we use m.Term to indicate the next term we'll be
                //   campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                fatal!(
                    self.logger,
                    "term should be set when sending {:?}",
                    m.get_msg_type()
                );
            }
        } else {
            if m.term != 0 {
                fatal!(
                    self.logger,
                    "term should not be set when sending {:?} (was {})",
                    m.get_msg_type(),
                    m.term
                );
            }
            // do not attach term to MsgPropose, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if m.get_msg_type() != MessageType::MsgPropose
                && m.get_msg_type() != MessageType::MsgReadIndex
            {
                m.term = self.term;
            }
        }
        if m.get_msg_type() == MessageType::MsgRequestVote
            || m.get_msg_type() == MessageType::MsgRequestPreVote
        {
            if self.priority > 0 {
                m.deprecated_priority = self.priority as u64;
            }
            m.priority = self.priority;
        }
        msgs.push(m);
    }

    fn prepare_send_snapshot(&mut self, m: &mut Message, pr: &mut Progress, to: u64) -> bool {
        if !pr.recent_active {
            debug!(
                self.logger,
                "ignore sending snapshot to {} since it is not recently active",
                to;
            );
            return false;
        }

        m.set_msg_type(MessageType::MsgSnapshot);
        let snapshot_r = self.raft_log.snapshot(pr.pending_request_snapshot, to);
        if let Err(e) = snapshot_r {
            if e == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!(
                    self.logger,
                    "failed to send snapshot to {} because snapshot is temporarily \
                     unavailable",
                    to;
                );
                return false;
            }
            fatal!(self.logger, "unexpected error: {:?}", e);
        }
        let snapshot = snapshot_r.unwrap();
        if snapshot.get_metadata().index == 0 {
            fatal!(self.logger, "need non-empty snapshot");
        }
        let (sindex, sterm) = (snapshot.get_metadata().index, snapshot.get_metadata().term);
        m.set_snapshot(snapshot);
        debug!(
            self.logger,
            "[firstindex: {first_index}, commit: {committed}] sent snapshot[index: {snapshot_index}, term: {snapshot_term}] to {to}",
            first_index = self.raft_log.first_index(),
            committed = self.raft_log.committed,
            snapshot_index = sindex,
            snapshot_term = sterm,
            to = to;
            "progress" => ?pr,
        );
        pr.become_snapshot(sindex);
        debug!(
            self.logger,
            "paused sending replication messages to {}",
            to;
            "progress" => ?pr,
        );
        true
    }

    fn prepare_send_entries(
        &mut self,
        m: &mut Message,
        pr: &mut Progress,
        term: u64,
        ents: Vec<Entry>,
    ) {
        m.set_msg_type(MessageType::MsgAppend);
        m.index = pr.next_idx - 1;
        m.log_term = term;
        m.set_entries(ents.into());
        m.commit = self.raft_log.committed;
        if !m.entries.is_empty() {
            let last = m.entries.last().unwrap().index;
            pr.update_state(last);
        }
    }

    fn try_batching(
        &mut self,
        to: u64,
        msgs: &mut [Message],
        pr: &mut Progress,
        ents: &mut Vec<Entry>,
    ) -> bool {
        // if MsgAppend for the receiver already exists, try_batching
        // will append the entries to the existing MsgAppend
        let mut is_batched = false;
        for msg in msgs {
            if msg.get_msg_type() == MessageType::MsgAppend && msg.to == to {
                if !ents.is_empty() {
                    if !util::is_continuous_ents(msg, ents) {
                        return is_batched;
                    }
                    let mut batched_entries: Vec<_> = msg.take_entries().into();
                    batched_entries.append(ents);
                    msg.set_entries(batched_entries.into());
                    let last_idx = msg.entries.last().unwrap().index;
                    pr.update_state(last_idx);
                }
                msg.commit = self.raft_log.committed;
                is_batched = true;
                break;
            }
        }
        is_batched
    }

    /// Sends an append RPC with new entries (if any) and the current commit index to the given
    /// peer.
    fn send_append(&mut self, to: u64, pr: &mut Progress, msgs: &mut Vec<Message>) {
        self.maybe_send_append(to, pr, true, msgs);
    }

    fn send_append_aggressively(&mut self, to: u64, pr: &mut Progress, msgs: &mut Vec<Message>) {
        // If we have more entries to send, send as many messages as we
        // can (without sending empty messages for the commit index)
        while self.maybe_send_append(to, pr, false, msgs) {}
    }

    /// Sends an append RPC with new entries to the given peer,
    /// if necessary. Returns true if a message was sent. The allow_empty
    /// argument controls whether messages with no entries will be sent
    /// ("empty" messages are useful to convey updated Commit indexes, but
    /// are undesirable when we're sending multiple messages in a batch).
    fn maybe_send_append(
        &mut self,
        to: u64,
        pr: &mut Progress,
        allow_empty: bool,
        msgs: &mut Vec<Message>,
    ) -> bool {
        if pr.is_paused() {
            trace!(
                self.logger,
                "Skipping sending to {to}, it's paused",
                to = to;
                "progress" => ?pr,
            );
            return false;
        }
        let mut m = Message::default();
        m.to = to;
        if pr.pending_request_snapshot != INVALID_INDEX {
            // Check pending request snapshot first to avoid unnecessary loading entries.
            if !self.prepare_send_snapshot(&mut m, pr, to) {
                return false;
            }
        } else {
            let ents = self.raft_log.entries(
                pr.next_idx,
                self.max_msg_size,
                GetEntriesContext(GetEntriesFor::SendAppend {
                    to,
                    term: self.term,
                    aggressively: !allow_empty,
                }),
            );
            if !allow_empty && ents.as_ref().map_or(true, |e| e.is_empty()) {
                return false;
            }
            let term = self.raft_log.term(pr.next_idx - 1);
            match (term, ents) {
                (Ok(term), Ok(mut ents)) => {
                    if self.batch_append && self.try_batching(to, msgs, pr, &mut ents) {
                        return true;
                    }
                    self.prepare_send_entries(&mut m, pr, term, ents)
                }
                (_, Err(Error::Store(StorageError::LogTemporarilyUnavailable))) => {
                    // wait for storage to fetch entries asynchronously
                    return false;
                }
                _ => {
                    // send snapshot if we failed to get term or entries.
                    if !self.prepare_send_snapshot(&mut m, pr, to) {
                        return false;
                    }
                }
            }
        }
        self.send(m, msgs);
        true
    }

    // send_heartbeat sends an empty MsgAppend
    fn send_heartbeat(
        &mut self,
        to: u64,
        pr: &Progress,
        ctx: Option<Vec<u8>>,
        msgs: &mut Vec<Message>,
    ) {
        // Attach the commit as min(to.matched, self.raft_log.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        let mut m = Message::default();
        m.to = to;
        m.set_msg_type(MessageType::MsgHeartbeat);
        let commit = cmp::min(pr.matched, self.raft_log.committed);
        m.commit = commit;
        if let Some(context) = ctx {
            m.context = context.into();
        }
        // The current lease round, echoed back in `MsgHeartbeatResponse.index`.
        // Always 0 outside the leased options, where proto3 omits it.
        m.index = self.read_only.leader.lease_round();
        self.send(m, msgs);
    }
}

impl<T: Storage> Raft<T> {
    /// Get the inflight buffer size.
    pub fn inflight_buffers_size(&self) -> usize {
        let mut total_size = 0;
        for (_, pr) in self.prs().iter() {
            total_size += pr.ins.buffer_capacity() * std::mem::size_of::<u64>();
        }
        total_size
    }

    /// Sends an append RPC with new entries (if any) and the current commit index to the given
    /// peer.
    pub fn send_append(&mut self, to: u64) {
        let pr = self.prs.get_mut(to).unwrap();
        self.r.send_append(to, pr, &mut self.msgs)
    }

    pub(super) fn send_append_aggressively(&mut self, to: u64) {
        let pr = self.prs.get_mut(to).unwrap();
        self.r.send_append_aggressively(to, pr, &mut self.msgs)
    }

    /// Sends RPC, with entries to all peers that are not up-to-date
    /// according to the progress recorded in r.prs().
    pub fn bcast_append(&mut self) {
        let self_id = self.id;
        let core = &mut self.r;
        let msgs = &mut self.msgs;
        self.prs
            .iter_mut()
            .filter(|&(id, _)| *id != self_id)
            .for_each(|(id, pr)| core.send_append(*id, pr, msgs));
    }

    /// Broadcasts heartbeats to all the followers if it's leader.
    pub fn ping(&mut self) {
        if self.state == StateRole::Leader {
            self.bcast_heartbeat();
        }
    }

    /// Sends RPC, without entries to all the peers.
    pub fn bcast_heartbeat(&mut self) {
        let ctx = self.read_only.last_pending_request_ctx();
        self.bcast_heartbeat_with_ctx(ctx)
    }

    fn bcast_heartbeat_with_ctx(&mut self, ctx: Option<Vec<u8>>) {
        // In the leased options every broadcast is a lease round, whatever else
        // it is for: the scheduled beat, a read-index round, or one sent for a
        // pending lease proposal.
        let leased = self.read_only.uses_read_leases();
        if leased {
            self.read_only.leader.mint_lease_round();
        }
        let self_id = self.id;
        let core = &mut self.r;
        let msgs = &mut self.msgs;
        self.prs
            .iter_mut()
            .filter(|&(id, _)| *id != self_id)
            .for_each(|(id, pr)| core.send_heartbeat(*id, pr, ctx.clone(), msgs));
        if leased {
            // A lone voter is its own majority.
            self.advance_lease_quorum();
        }
    }

    /// Whether the new-leader safeguard has lifted, by timer or by the
    /// confirmation shortcut (every node that could hold a prior-term lease has
    /// been heard from at this term). Callers use this;
    /// `ReadOnly::safeguard_has_passed` is only the timer half. The required
    /// set is the current configuration plus any node named by an unapplied
    /// conf change (see `scan_unapplied_conf_changes`).
    pub(crate) fn new_leader_safeguard_passed(&mut self) -> bool {
        if !self.read_only.uses_read_leases() {
            return true;
        }
        if self.read_only.safeguard_has_passed() {
            return true;
        }
        if self.read_only.safeguard.scan_failed() {
            return false;
        }
        let self_id = self.id;
        let confirmed =
            |id: &u64| *id == self_id || self.read_only.safeguard.is_confirmed_at_term(*id);
        if !self.prs.progress().keys().all(confirmed)
            || !self
                .read_only
                .safeguard
                .unapplied_conf_nodes()
                .iter()
                .all(confirmed)
        {
            return false;
        }
        info!(
            self.logger,
            "new-leader read-lease safeguard lifted early: every node that could \
             hold a prior-term lease has been heard from at term {term}",
            term = self.term;
        );
        // Latch, so a later configuration change cannot re-close the window.
        self.read_only.safeguard.mark_passed();
        true
    }

    /// Attempts to advance the commit index. Returns true if the commit index
    /// changed (in which case the caller should call `r.bcast_append`).
    pub fn maybe_commit(&mut self) -> bool {
        let mut mci = self.mut_prs().maximal_committed_index().0;
        // Leased options: a freshly-elected leader commits nothing until the
        // safeguard window has elapsed — including the election no-op, which
        // would commit the whole prior-term tail with it. Under GrantLeases a
        // prior leader's follower leases may still be live; under both, the
        // prior leader's own self-lease may be, and committing nothing means
        // there is no newer state for its reads to be stale against.
        if self.read_only.uses_read_leases() && !self.new_leader_safeguard_passed() {
            return false;
        }
        // GrantLeases: the leader must not advance commit past the minimum
        // acked index across the read leases it has handed out.
        if self.read_only.option == ReadOnlyOption::GrantLeases
            && !self.read_only.leader.granted_leases.leases.is_empty()
        {
            let min_acked = self.read_only.leader.granted_leases.min_acked_index();
            let clamped = mci.min(min_acked);
            if clamped < mci {
                self.ask_lease_holders_for_ack_index(mci);
            }
            mci = clamped;
        }
        if self.r.raft_log.maybe_commit(mci, self.r.term) {
            let (self_id, committed) = (self.id, self.raft_log.committed);
            self.mut_prs()
                .get_mut(self_id)
                .unwrap()
                .update_committed(committed);
            // A read postponed for want of a current-term commit may now be
            // answerable.
            self.release_pending_read_index_messages();
            return true;
        }
        false
    }

    /// Answers `MsgReadIndex` messages that were postponed because this leader
    /// had not yet committed an entry in its own term. No-op until that first
    /// commit lands, and cheap when the queue is empty.
    fn release_pending_read_index_messages(&mut self) {
        if self.r.pending_read_index_messages.is_empty() {
            return;
        }
        if !self.commit_to_current_term() {
            return;
        }
        let msgs = std::mem::take(&mut self.r.pending_read_index_messages);
        for m in msgs {
            // The same gate as a read arriving now: served from the self-lease
            // if it is live, otherwise by a heartbeat quorum round.
            self.leader_read_index(m);
        }
    }

    /// Sends `MsgAskAckIndex` to the lease holders that are holding commit back
    /// (acked index below `cutoff`) and have not been pinged within
    /// `ACK_INDEX_ASK_INTERVAL_MICROS`. Called when the commit clamp bites.
    fn ask_lease_holders_for_ack_index(&mut self, cutoff: u64) {
        if self.read_only.option != ReadOnlyOption::GrantLeases {
            return;
        }
        let ids = self
            .read_only
            .leader
            .granted_leases
            .nodes_to_ask_for_ack(cutoff);
        for id in ids {
            let mut m = Message::default();
            m.set_msg_type(MessageType::MsgAskAckIndex);
            m.to = id;
            self.r.send(m, &mut self.msgs);
        }
    }

    /// Leader handler for `MsgAskReadLease`.
    ///
    /// Six static requirements are checked here, once; a request failing any
    /// of them is refused at once. A request meeting them all is **parked**, not
    /// granted: the seventh requirement is that a majority echo a lease round
    /// minted *after* the request arrived, which is what dates the follower's
    /// lease before any successor's election. `grant_proposals_covered_by`
    /// answers it once that holds, and re-checks only the slot count.
    fn handle_ask_read_lease(&mut self, m: &Message) {
        // Check 5 reads `committed`: bring it up to date with expired grants
        // first.
        self.maybe_commit_clamped();
        let committed = self.raft_log.committed;
        let quorum_index = self.mut_prs().maximal_committed_index().0;
        let admissible = self.read_only.option == ReadOnlyOption::GrantLeases
            // 1. request is for the current term
            && self.term == m.term
            // 2. this leader has committed in its own term, so `committed` is
            //    the real commit index rather than a lower bound
            && self.commit_to_current_term()
            // 3. the asker is still a member (`prs()` includes learners)
            && self.prs().get(m.from).is_some()
            // 4. the asker is not being drained for removal
            && !self.read_only.leader.is_grant_banned(m.from)
            // 5. the follower's log is up to date (within the catch-up margin)
            && committed <= m.index.saturating_add(self.read_only.lease_config.catchup_margin)
            // 5b. a holder's renewal: its grant is not pinning commit beyond
            //     the margin. 5 cannot catch this, as the holder clamps
            //     `committed` itself (LIVE-2)
            && !self
                .read_only
                .renewal_pins_commit(m.from, m.log_term, quorum_index)
            // 6. a lease slot is free, or this follower already holds one
            && self.read_only.can_grant_lease_slot(m.from);
        if !admissible {
            self.send_read_lease_resp(m.from, m.log_term, None);
            return;
        }
        // 7. Park until a majority echoes a round minted after this request. A
        // re-ask while one is parked keeps the first (see `park_grant`).
        if self.read_only.park_grant(m.from, m.log_term, m.index) {
            debug!(
                self.logger,
                "parking read lease request from {from} (leaseId: {lease_id}) pending a lease round",
                from = m.from,
                lease_id = m.log_term;
            );
        }
        // Also on a re-ask, in case the round covering the parked request was
        // lost.
        self.maybe_send_lease_round();
    }

    /// Answers a follower's lease request; `None` is a refusal. `log_term`
    /// echoes the asker's lease id on both paths, so a refused asker can drop
    /// the pending id it recorded when it asked.
    fn send_read_lease_resp(&mut self, to: u64, lease_id: u64, lease: Option<ReadLease>) {
        let mut resp = Message::default();
        resp.set_msg_type(MessageType::MsgAskReadLeaseResp);
        resp.to = to;
        resp.index = self.raft_log.committed;
        resp.log_term = lease_id;
        resp.set_entries(vec![Entry::default()].into());
        match lease {
            Some(lease) => resp.mut_entries()[0].data = lease.marshal().to_vec().into(),
            None => resp.reject = true,
        }
        self.r.send(resp, &mut self.msgs);
    }

    /// Sends a lease round now if any lease proposal (a self-lease renewal or a
    /// parked follower request) is waiting for one.
    ///
    /// **Divergence from the Go fork:** every pending proposal is treated as
    /// urgent. The Go fork lets a proposal ride the next scheduled heartbeat
    /// and brings a round forward only within 50 ms of expiry, which suits
    /// etcd's 100 ms heartbeat. TiKV's heartbeat is 2 s, longer than a whole
    /// lease, so waiting would only move the broadcast later and leave 50 ms
    /// for a round trip through raftstore. Batching still happens: at most one
    /// round is in flight, and proposals made while it is out all share the
    /// next one. A round that has not reached a majority after
    /// `LEASE_ROUND_RETRY_MICROS` is presumed lost, and this call resends.
    fn maybe_send_lease_round(&mut self) {
        if !self.read_only.uses_read_leases()
            || self.state != StateRole::Leader
            || !self.read_only.leader.has_pending_proposals()
        {
            return;
        }
        if self.read_only.leader.lease_round_in_flight()
            && !self.read_only.leader.lease_round_retry_due()
        {
            return;
        }
        // Also carries the latest pending read-index context, if any.
        self.bcast_heartbeat();
    }

    /// Records a peer's echo of a lease round and answers whatever a majority
    /// now covers; sends the next round if proposals are still waiting.
    fn step_lease_echo(&mut self, from: u64, round: u64) {
        if !self.read_only.leader.record_lease_echo(from, round) {
            return;
        }
        self.advance_lease_quorum();
        self.maybe_send_lease_round();
    }

    /// Recomputes the highest round a majority has echoed and, if it rose,
    /// answers every proposal waiting on it or an earlier round.
    fn advance_lease_quorum(&mut self) {
        let self_id = self.id;
        let q = match self
            .r
            .read_only
            .leader
            .advance_lease_quorum(&self.prs.conf().voters, self_id)
        {
            Some(q) => q,
            None => return,
        };
        self.grant_proposals_covered_by(q);
    }

    /// Installs the pending self-lease renewal and grants every parked follower
    /// request whose `need_round` is at or below `round`, the highest round a
    /// majority has echoed. A proposal recorded after that round was minted
    /// needs a later one, whatever the clock says.
    fn grant_proposals_covered_by(&mut self, round: u64) {
        let (self_id, committed) = (self.id, self.raft_log.committed);
        self.read_only
            .maybe_install_self_lease(round, self_id, committed);
        if self.read_only.option != ReadOnlyOption::GrantLeases {
            return;
        }
        for id in self.read_only.leader.parked_grant_ids() {
            match self.read_only.leader.parked_grant(id) {
                Some(pg) if pg.need_round <= round => {}
                // Arrived after `round` was minted; waits for a later one.
                _ => continue,
            }
            let pg = self.read_only.leader.take_parked_grant(id).unwrap();
            // Held a full lease duration: a lease dated from the request would
            // already be over. Refuse rather than grant, or this leader would
            // clamp commit on its behalf for a whole duration anyway.
            if self.read_only.parked_grant_stale(&pg) {
                self.send_read_lease_resp(id, pg.lease_id, None);
                continue;
            }
            // Only the slot count can have changed while parked. Term changes
            // rebuild `ReadOnly`; `commit_to_current_term` only improves;
            // removal and bans drop parked requests directly. The catch-up
            // margin is deliberately NOT re-checked: it is an admission
            // heuristic, the grant carries this leader's commit index, and the
            // follower cannot serve below it anyway. Re-checking it would
            // refuse leases whenever writes are flowing.
            if self.prs().get(id).is_none() || !self.read_only.can_grant_lease_slot(id) {
                self.send_read_lease_resp(id, pg.lease_id, None);
                continue;
            }
            let acked = self
                .read_only
                .grant_acked_index(id, pg.lease_id, pg.asked_index);
            let duration = self.read_only.lease_config.duration_micros;
            let lease = self.read_only.leader.granted_leases.grant_new_lease(
                id,
                pg.lease_id,
                acked,
                committed,
                duration,
            );
            self.send_read_lease_resp(id, pg.lease_id, Some(lease));
        }
    }

    /// Proposes a renewal of this leader's self-lease once it has less than
    /// `LEASE_RENEWAL_THRESHOLD_MICROS` left, unless one is already pending and
    /// not yet stale. Sends nothing itself; see `maybe_send_lease_round`.
    ///
    /// `proactive` is true for the tick and the wrapper's timer, which renew
    /// only a self-lease that has actually served a read; a read always
    /// proposes one. Refused outright while this leader's own removal is in
    /// flight (`ban_self_lease`): that is the only path that proposes one.
    fn maybe_renew_self_lease(&mut self, proactive: bool) {
        if !self.read_only.uses_read_leases()
            || self.state != StateRole::Leader
            || self.read_only.leader.is_self_lease_banned()
        {
            return;
        }
        if proactive && !self.read_only.leader.self_lease_used() {
            return;
        }
        if self.read_only.micros_until_held_lease_expired() > LEASE_RENEWAL_THRESHOLD_MICROS {
            return; // still comfortably valid
        }
        if let Some(armed) = self.read_only.leader.self_lease_armed_at() {
            // First proposal wins until it is stale. Re-proposing on every call
            // would keep pushing back the round it needs, and it would never
            // install.
            if now_micros().saturating_sub(armed) < self.read_only.lease_config.duration_micros {
                return;
            }
        }
        self.read_only.leader.arm_self_lease();
    }

    /// Timer-driven self-lease renewal for the wrapper: proposes one if due and
    /// sends a round for it. Returns `true` if a message was produced.
    pub(crate) fn maybe_renew_self_lease_proactively(&mut self) -> bool {
        let before = self.msgs.len();
        self.maybe_renew_self_lease(true);
        self.maybe_send_lease_round();
        self.msgs.len() > before
    }

    /// Answers a `MsgReadIndex` at a leader that has committed in its term.
    ///
    /// Under the leased options the read is served from the self-lease while it
    /// is live; otherwise, and always under `Safe`, it takes a heartbeat quorum
    /// round. Upstream `LeaseBased` answered unconditionally from `committed`,
    /// whose only brake was CheckQuorum eventually deposing a partitioned
    /// leader (etcd-io/raft#166).
    fn leader_read_index(&mut self, m: Message) {
        if self.prs().is_singleton() {
            let read_index = self.raft_log.committed;
            if let Some(m) = self.handle_ready_read_index(m, read_index, ReadProvenance::Quorum) {
                self.r.send(m, &mut self.msgs);
            }
            return;
        }
        if self.read_only.uses_read_leases() {
            self.maybe_renew_self_lease(false);
            if self.read_only.has_active_held_lease() {
                self.read_only.leader.mark_self_lease_used();
                let read_index = self.raft_log.committed;
                if let Some(m) =
                    self.handle_ready_read_index(m, read_index, ReadProvenance::LeaderLease)
                {
                    self.r.send(m, &mut self.msgs);
                }
                self.maybe_send_lease_round();
                return;
            }
            // No live self-lease: we cannot currently show a majority follows
            // us. Confirm it directly with a quorum round rather than refusing.
            // The broadcast below mints a lease round after the renewal just
            // proposed, so it covers that too.
            debug!(
                self.logger,
                "no active self-lease; serving read index via a quorum round"
            );
        }
        self.leader_read_index_quorum_round(m);
    }

    /// Answers a `MsgReadIndex` at a leader with a heartbeat quorum round: the
    /// `Safe` path, and the fallback for the leased options.
    fn leader_read_index_quorum_round(&mut self, m: Message) {
        let ctx = m.entries[0].data.to_vec();
        self.r
            .read_only
            .add_request(self.r.raft_log.committed, m, self.r.id);
        self.bcast_heartbeat_with_ctx(Some(ctx));
    }

    /// A read index that must be confirmed by a heartbeat quorum round even
    /// under the leased options, for a caller that treats the answer as
    /// evidence that a majority followed this leader when the read was
    /// proposed (TiKV renews its raftstore leader lease from it).
    ///
    /// Leader-only guarantee. On a leader that has not yet committed in its
    /// term, or in a single-voter group, or on a follower, this is an ordinary
    /// read index: nothing on the wire can ask a remote leader for a quorum
    /// round, and the resulting `ReadState` says how it was served.
    pub fn read_index_quorum(&mut self, rctx: Vec<u8>) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut e = Entry::default();
        e.data = rctx.into();
        m.set_entries(vec![e].into());
        if self.state == StateRole::Leader
            && self.read_only.uses_read_leases()
            && self.commit_to_current_term()
            && !self.prs().is_singleton()
        {
            self.leader_read_index_quorum_round(m);
            return;
        }
        let _ = self.step(m);
    }

    /// Leader handler for `MsgAskAckIndexResp`: updates the asking follower's
    /// acked index and, if that follower was the commit bottleneck, tries to
    /// advance the commit index.
    fn handle_ask_ack_index_resp(&mut self, m: &Message) {
        if self.read_only.option != ReadOnlyOption::GrantLeases {
            return;
        }
        let old_index = match self.read_only.granted_lease(m.from) {
            Some(rl) => rl.acked_index,
            None => return,
        };
        // Term 0 is a switch credit injected by the application, which vouches
        // for the holder's reads whatever its incarnation, but only in the term
        // the switch register answered in (`log_term`, 32 bits as the switch
        // carries it). The register's index rises only within a term; a newer
        // term's write may lower it, so a credit from another term's register
        // promises nothing about the stamps this term's reads will use. A
        // holder's own answer counts only from the incarnation that asked for
        // this grant.
        if m.term == 0 && m.log_term as u32 != self.term as u32 {
            info!(
                self.logger,
                "ignoring switch credit for {from}: register term {register_term} is not term {term}",
                from = m.from,
                register_term = m.log_term,
                term = self.term;
            );
            return;
        }
        if m.term != 0 && !self.read_only.ack_matches_grant(m.from, m.log_term) {
            info!(
                self.logger,
                "ignoring MsgAskAckIndexResp from {from}: incarnation {incarnation} did not ask for its grant",
                from = m.from,
                incarnation = m.log_term;
            );
            return;
        }
        let min_before = self.read_only.leader.granted_leases.min_acked_index();
        self.read_only
            .leader
            .granted_leases
            .update_acked_index(m.from, m.index);
        if m.term == 0 {
            self.read_only.read_lease_stats.times_credit_accepted += 1;
        }

        if old_index == min_before && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// Follower path: ask the leader for a (renewed) read lease when the current
    /// lease is near expiry (or absent) and the rate-limiter permits.
    /// The lease id is minted locally and carried in `MsgAskReadLease.log_term`.
    /// Returns `true` if a `MsgAskReadLease` was produced (so the wrapper knows a
    /// `Ready` is worth collecting when driving this from a renewal timer).
    pub(crate) fn maybe_ask_for_read_lease(&mut self) -> bool {
        // Not on a leader: after winning an election `leader_id` is this node's
        // own id, so a still-armed renewal timer would ask itself for a lease.
        if self.read_only.option != ReadOnlyOption::GrantLeases
            || self.state == StateRole::Leader
            || self.leader_id == INVALID_ID
        {
            return false;
        }
        // An idle follower (no read of its own and no append with entries for
        // the idle timeout) lets its lease lapse instead of renewing it, so an
        // idle group goes quiet (EXP-12). A read marks use first, so a
        // read-driven ask always passes.
        if self.read_only.lease_renewal_idle() {
            return false;
        }
        let self_id = self.id;
        let until_expired = self.read_only.micros_until_held_lease_expired();
        if until_expired >= LEASE_RENEWAL_THRESHOLD_MICROS || !self.read_only.can_ask_for_lease() {
            return false;
        }
        let lease_id = self.read_only.get_marked_lease_id();
        let committed = self.raft_log.committed;
        let leader = self.leader_id;
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgAskReadLease);
        m.to = leader;
        m.from = self_id;
        m.index = committed;
        m.log_term = lease_id;
        self.r.send(m, &mut self.msgs);
        info!(
            self.logger,
            "{id} sending MsgAskReadLease to leader {leader} (leaseId: {lease_id})",
            id = self_id,
            leader = leader,
            lease_id = lease_id;
        );
        self.read_only.mark_asked_for_lease();
        true
    }

    /// Follower handling of a `MsgReadIndex` under `GrantLeases`: serve the read
    /// locally from our lease when commit has caught up, hold it in the delayer
    /// when commit is close enough to catch up shortly, or forward it to the
    /// leader otherwise.
    fn serve_read_index_with_lease(&mut self, mut m: Message) {
        // Only this node's own reads are served from its lease. A read another
        // node forwarded here (its `leader_id` is stale) goes on to our leader,
        // `from` intact, which answers that node directly. Served here, a
        // give-up replay's `m.index` would be one computed against the
        // requester's lease, not ours (SAFE-5).
        if m.from != INVALID_ID && m.from != self.id {
            return self.forward_read_index(m);
        }

        // A hint covers this term's credits only if this term's leader wrote
        // the switch register it came from: after a switch reboot, a deposed
        // leader's lower write can land first (SAFE-1). The switch carries the
        // term in 32 bits. Any other nonzero hint is made unreachable, so it is
        // forwarded and never held. A held read cannot outlive a term change:
        // `reset` rebuilds `read_only`, delayer included.
        if m.commit != 0 && m.log_term as u32 != self.term as u32 {
            m.commit = u64::MAX;
        }

        let committed = self.raft_log.committed;

        // No usable lease, so forward to the leader.
        let log_index = match self.read_only.held_lease() {
            Some(rl) => rl.log_index,
            None => return self.forward_read_index(m),
        };
        // The node-global ack index, not the lease's copy: the lease's is only
        // as fresh as the grant that installed it.
        let acked_index = self.read_only.read_lease_acked_index();
        if log_index > committed {
            return self.forward_read_index(m);
        }

        // `m.commit` is an extra index this node must have committed before it
        // may serve the read from its own lease; 0 is the sole reserved value
        // and means no extra requirement. An unreachable index (including a
        // hint from another term, rewritten above) needs no case of its own:
        // it fails the serve test and exceeds the hold threshold, so the read
        // forwards. See `RawNode::read_index_switch_hint`.
        let hint = m.commit;
        // m.index != 0 is the delayed-read replay hook.
        let can_serve = (acked_index <= committed && (hint == 0 || hint <= committed))
            || (m.index != 0 && m.index <= committed);
        if can_serve {
            // The only site that serves a read from this node's own lease.
            if let Some(resp) =
                self.handle_ready_read_index(m, committed, ReadProvenance::FollowerLease)
            {
                self.msgs.push(resp);
            }
            self.read_only.mark_read_index_query_stat(true);
            return;
        }

        // Not caught up yet. Hold the read locally and let the delayer
        // re-evaluate it as commit advances, but only when
        // the required index is within the hold log-distance threshold and this
        // is not a timed-out replay (`reject`); otherwise forward to the leader.
        //
        // `checked_sub`, not `-`: an unreachable required index must forward,
        // never be held — a held read is replayed with `m.index` set, which
        // serves it.
        let required = cmp::max(hint, acked_index);
        let within_threshold = required
            .checked_sub(committed)
            .is_some_and(|d| d <= READ_INDEX_LOCAL_HOLD_LOG_THRESHOLD);
        if !m.reject && within_threshold {
            self.read_only.add_delayed_read_index_req(m, required);
            self.read_only.mark_read_held_stat();
        } else {
            self.forward_read_index(m);
        }
    }

    /// Forwards a `MsgReadIndex` to the leader and records that it was not served locally.
    fn forward_read_index(&mut self, mut m: Message) {
        m.to = self.leader_id;
        self.r.send(m, &mut self.msgs);
        self.read_only.mark_read_index_query_stat(false);
    }

    /// Pumps the read-index delayer and replays any reads it releases by
    /// re-`step`ping them: a read whose required commit index has been reached
    /// is served locally, and one held past its deadline is forwarded to the
    /// leader. Returns `true` if any read was replayed.
    ///
    /// raft-rs has no timer thread, so this is driven from the tail of `step`,
    /// from `tick()`, and — for full give-up resolution — by the wrapper via
    /// `RawNode::fire_read_index_delays()`.
    pub(crate) fn fire_delayed_read_index_requests(&mut self) -> bool {
        if self.read_only.option != ReadOnlyOption::GrantLeases
            || self.read_only.delayed_read_index_reqs.is_empty()
        {
            return false;
        }
        let committed = self.raft_log.committed;
        let ready = self.read_only.fire_delayed_read_index_requests(committed);
        let fired = !ready.is_empty();
        for m in ready {
            let _ = self.step(m);
        }
        fired
    }

    /// Commit that the Raft peer has applied up to the given index.
    ///
    /// Registers the new applied index to the Raft log.
    ///
    /// # Hooks
    ///
    /// * Post: Checks to see if it's time to finalize a Joint Consensus state.
    #[inline]
    pub fn commit_apply(&mut self, applied: u64) {
        self.commit_apply_internal(applied, false)
    }

    /// Commit that the Raft peer has applied up to the given index.
    ///
    /// Registers the new applied index to the Raft log.
    /// if `skip_check` is true, will skip the applied_index check, this is only
    /// used at initialization.
    ///
    /// # Hooks
    ///
    /// * Post: Checks to see if it's time to finalize a Joint Consensus state.
    fn commit_apply_internal(&mut self, applied: u64, skip_check: bool) {
        let old_applied = self.raft_log.applied;
        if !skip_check {
            #[allow(deprecated)]
            self.raft_log.applied_to(applied);
        } else {
            // skip applied_index check at initialization.
            assert!(applied > 0);
            self.raft_log.applied_to_unchecked(applied);
        }

        // TODO: it may never auto_leave if leader steps down before enter joint is applied.
        if self.prs.conf().auto_leave
            && old_applied <= self.pending_conf_index
            && applied >= self.pending_conf_index
            && self.state == StateRole::Leader
        {
            // If the current (and most recent, at least for this leader's term)
            // configuration should be auto-left, initiate that now. We use a
            // nil Data which unmarshals into an empty ConfChangeV2 and has the
            // benefit that appendEntry can never refuse it based on its size
            // (which registers as zero).
            let mut entry = Entry::default();
            entry.set_entry_type(EntryType::EntryConfChangeV2);

            // append_entry will never refuse an empty
            if !self.append_entry(&mut [entry]) {
                panic!("appending an empty EntryConfChangeV2 should never be dropped")
            }
            self.pending_conf_index = self.raft_log.last_index();
            info!(self.logger, "initiating automatic transition out of joint configuration"; "config" => ?self.prs.conf());
        }
    }

    /// Resets the current node to a given term.
    pub fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = INVALID_ID;
        }
        self.leader_id = INVALID_ID;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        self.abort_leader_transfer();

        self.prs.reset_votes();

        self.pending_conf_index = 0;
        self.read_only = ReadOnly::new(self.read_only.option, self.read_only.lease_config.clone());
        self.pending_request_snapshot = INVALID_INDEX;

        let last_index = self.raft_log.last_index();
        let committed = self.raft_log.committed;
        let persisted = self.raft_log.persisted;
        let self_id = self.id;
        for (&id, pr) in self.mut_prs().iter_mut() {
            pr.reset(last_index + 1);
            if id == self_id {
                pr.matched = persisted;
                pr.committed_index = committed;
            }
        }
    }

    /// Appends a slice of entries to the log.
    /// The entries are updated to match the current index and term.
    /// Only called by leader currently
    #[must_use]
    pub fn append_entry(&mut self, es: &mut [Entry]) -> bool {
        if !self.maybe_increase_uncommitted_size(es) {
            return false;
        }

        let li = self.raft_log.last_index();
        for (i, e) in es.iter_mut().enumerate() {
            e.term = self.term;
            e.index = li + 1 + i as u64;
        }
        self.raft_log.append(es);

        // Not update self's pr.matched until on_persist_entries
        true
    }

    /// Notifies that these raft logs have been persisted.
    pub fn on_persist_entries(&mut self, index: u64, term: u64) {
        let update = self.raft_log.maybe_persist(index, term);
        if update && self.state == StateRole::Leader {
            // Actually, if it is a leader and persisted index is updated, this term
            // must be equal to self.term because the persisted index must be equal to
            // the last index of entries from previous leader when it becomes leader
            // (see the comments in become_leader), namely, the new persisted entries
            // must come from this leader.
            if term != self.term {
                error!(
                    self.logger,
                    "leader's persisted index changed but the term {} is not the same as {}",
                    term,
                    self.term
                );
            }
            let self_id = self.id;
            let pr = self.mut_prs().get_mut(self_id).unwrap();
            if pr.maybe_update(index) && self.maybe_commit() && self.should_bcast_commit() {
                self.bcast_append();
            }
        }
    }

    /// Notifies that the snapshot have been persisted.
    pub fn on_persist_snap(&mut self, index: u64) {
        self.raft_log.maybe_persist_snap(index);
    }

    /// Returns true to indicate that there will probably be some readiness need to be handled.
    pub fn tick(&mut self) -> bool {
        let mut has_ready = match self.state {
            StateRole::Follower | StateRole::PreCandidate | StateRole::Candidate => {
                self.tick_election()
            }
            StateRole::Leader => self.tick_heartbeat(),
        };
        // Backstop the read-index delayer's give-up deadline. raft-rs has no
        // timer thread, so without this a held read whose commit never advances
        // would not be forwarded until the next incoming append/heartbeat.
        if self.fire_delayed_read_index_requests() {
            has_ready = true;
        }
        has_ready
    }

    // TODO: revoke pub when there is a better way to test.
    /// Run by followers and candidates after self.election_timeout.
    ///
    /// Returns true to indicate that there will probably be some readiness need to be handled.
    pub fn tick_election(&mut self) -> bool {
        self.election_elapsed += 1;
        if !self.pass_election_timeout() || !self.promotable {
            return false;
        }

        self.election_elapsed = 0;
        let m = new_message(INVALID_ID, MessageType::MsgHup, Some(self.id));
        let _ = self.step(m);
        true
    }

    // tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout.
    // Returns true to indicate that there will probably be some readiness need to be handled.
    fn tick_heartbeat(&mut self) -> bool {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        let mut has_ready = false;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            if self.check_quorum {
                let m = new_message(INVALID_ID, MessageType::MsgCheckQuorum, Some(self.id));
                has_ready = true;
                let _ = self.step(m);
            }
            if self.state == StateRole::Leader && self.lead_transferee.is_some() {
                self.abort_leader_transfer()
            }
        }

        if self.state != StateRole::Leader {
            return has_ready;
        }

        // Leased options: propose renewing a self-lease that is in use, before
        // the scheduled beat below so that beat's round can cover it.
        let msgs_before = self.msgs.len();
        self.maybe_renew_self_lease(true);

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            has_ready = true;
            let m = new_message(INVALID_ID, MessageType::MsgBeat, Some(self.id));
            let _ = self.step(m);
        }
        // Send a round for anything still waiting (a lost round included).
        self.maybe_send_lease_round();

        // Leased options: retry a commit the safeguard suppressed. Commit
        // otherwise only advances on an append response, and those are consumed
        // inside the window; with no write traffic afterwards the election no-op
        // would never commit. `commit_to_current_term()` goes true the moment it
        // does, so this is unreachable outside the window.
        if self.read_only.uses_read_leases()
            && !self.commit_to_current_term()
            && self.new_leader_safeguard_passed()
            && self.maybe_commit()
        {
            has_ready = true;
            self.bcast_append();
        }
        if self.maybe_commit_clamped() {
            has_ready = true;
        }
        // Checked last: a binding clamp that does not advance commit still
        // emits `MsgAskAckIndex`, and TiKV collects a Ready after a tick only
        // when this returns true.
        if self.msgs.len() > msgs_before {
            has_ready = true;
        }
        has_ready
    }

    /// Re-runs `maybe_commit` while the read-lease clamp holds commit below the
    /// quorum index (GrantLeases, after the term's first commit), and
    /// broadcasts if commit advanced. A grant that stops pinning commit when it
    /// expires drives no commit by itself: `maybe_commit` runs on acks, and
    /// admission and lease lookups reap expired grants silently. Called from
    /// `tick_heartbeat`, so an idle group's write commits within a tick of the
    /// expiry, and before lease admission, so check 5 judges an asker against
    /// the commit index the remaining grants allow rather than a stale clamped
    /// one (LIVE-4; the LIVE-2 residual). TiKV's ReadLeaseExpiry timer
    /// (`RawNode::expire_read_leases_and_maybe_commit`) narrows the first case
    /// to the timer's latency but cannot close the second.
    fn maybe_commit_clamped(&mut self) -> bool {
        if self.read_only.option == ReadOnlyOption::GrantLeases
            && self.commit_to_current_term()
            && self.raft_log.committed < self.mut_prs().maximal_committed_index().0
            && self.maybe_commit()
        {
            self.bcast_append();
            return true;
        }
        false
    }

    /// Converts this node to a follower.
    pub fn become_follower(&mut self, term: u64, leader_id: u64) {
        let pending_request_snapshot = self.pending_request_snapshot;
        self.reset(term);
        self.leader_id = leader_id;
        let from_role = self.state;
        self.state = StateRole::Follower;
        self.pending_request_snapshot = pending_request_snapshot;
        // TODO: In theory, it's better to let the user control this after
        // leadership changes, but because the user only know this info after
        // calling `ready()` but the committed entries is fetched within `ready()`,
        // so we hard code this logic here currently. We may need to remove
        // this hard code when we want to also support apply unpersisted log
        // on follower.
        self.raft_log.max_apply_unpersisted_log_limit = 0;
        info!(
            self.logger,
            "became follower at term {term}",
            term = self.term;
            "from_role" => ?from_role,
        );
    }

    // TODO: revoke pub when there is a better way to test.
    /// Converts this node to a candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    pub fn become_candidate(&mut self) {
        assert_ne!(
            self.state,
            StateRole::Leader,
            "invalid transition [leader -> candidate]"
        );
        let term = self.term + 1;
        self.reset(term);
        let id = self.id;
        self.vote = id;
        self.state = StateRole::Candidate;
        info!(
            self.logger,
            "became candidate at term {term}",
            term = self.term;
        );
    }

    /// Converts this node to a pre-candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    pub fn become_pre_candidate(&mut self) {
        assert_ne!(
            self.state,
            StateRole::Leader,
            "invalid transition [leader -> pre-candidate]"
        );
        // Becoming a pre-candidate changes our state.
        // but doesn't change anything else. In particular it does not increase
        // self.term or change self.vote.
        self.state = StateRole::PreCandidate;
        self.prs.reset_votes();
        // If a network partition happens, and leader is in minority partition,
        // it will step down, and become follower without notifying others.
        self.leader_id = INVALID_ID;
        info!(
            self.logger,
            "became pre-candidate at term {term}",
            term = self.term;
        );
    }

    // TODO: revoke pub when there is a better way to test.
    /// Makes this raft the leader.
    ///
    /// # Panics
    ///
    /// Panics if this is a follower node.
    pub fn become_leader(&mut self) {
        trace!(self.logger, "ENTER become_leader");
        assert_ne!(
            self.state,
            StateRole::Follower,
            "invalid transition [follower -> leader]"
        );
        let term = self.term;
        // Vote responses recorded while campaigning prove each voter already
        // stepped to this term. `reset` rebuilds `ReadOnly`, but the term does
        // not change here, so carry them across.
        let confirmed = self.read_only.safeguard.take_confirmations();
        self.reset(term);
        self.read_only.restore_confirmations(confirmed);
        self.leader_id = self.id;
        self.state = StateRole::Leader;

        let last_index = self.raft_log.last_index();
        // If there is only one peer, it becomes leader after campaigning
        // so all logs must be persisted.
        // If not, it becomes leader after sending RequestVote msg.
        // Since all logs must be persisted before sending RequestVote
        // msg and logs can not be changed when it's (pre)candidate, the
        // last index is equal to persisted index when it becomes leader.
        assert_eq!(last_index, self.raft_log.persisted);

        // Update uncommitted state
        self.uncommitted_state.uncommitted_size = 0;
        self.uncommitted_state.last_log_tail_index = last_index;

        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        let id = self.id;
        self.mut_prs().get_mut(id).unwrap().become_replicate();

        // Conservatively set the pending_conf_index to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        self.pending_conf_index = last_index;

        // Leased options: arm the safeguard and scan for unapplied conf changes,
        // both before the no-op below is appended, since the safeguard is what
        // withholds it. One window, one lease duration: every lease (a
        // follower's, or a prior leader's own) is dated from a proposal that
        // provably precedes this election.
        if self.read_only.uses_read_leases() {
            let window = self.read_only.lease_config.duration_micros + SAFEGUARD_CLOCK_DRIFT_MICROS;
            self.read_only.safeguard.start(window);
            self.scan_unapplied_conf_changes();
        }

        // No need to check result because append_entry never refuse entries
        // which size is zero
        if !self.append_entry(&mut [Entry::default()]) {
            panic!("appending an empty entry should never be dropped")
        }

        info!(
            self.logger,
            "became leader at term {term}",
            term = self.term;
        );
        trace!(self.logger, "EXIT become_leader");
    }

    // Campaign to attempt to become a leader.
    //
    // If prevote is enabled, this is handled as well.
    #[doc(hidden)]
    pub fn campaign(&mut self, campaign_type: &'static [u8]) {
        let (vote_msg, term) = if campaign_type == CAMPAIGN_PRE_ELECTION {
            self.become_pre_candidate();
            // Pre-vote RPCs are sent for next term before we've incremented self.term.
            (MessageType::MsgRequestPreVote, self.term + 1)
        } else {
            self.become_candidate();
            (MessageType::MsgRequestVote, self.term)
        };
        let self_id = self.id;
        if VoteResult::Won == self.poll(self_id, vote_msg, true) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster).
            return;
        }

        let (commit, commit_term) = self.raft_log.commit_info();
        let mut voters = [0; 7];
        let mut voter_cnt = 0;

        // Only send vote request to voters.
        for id in self.prs.conf().voters().ids().iter() {
            if id == self_id {
                continue;
            }

            if voter_cnt == voters.len() {
                self.log_broadcast_vote(vote_msg, &voters);
                voter_cnt = 0;
            }
            voters[voter_cnt] = id;
            voter_cnt += 1;
            let mut m = new_message(id, vote_msg, None);
            m.term = term;
            m.index = self.raft_log.last_index();
            m.log_term = self.raft_log.last_term();
            m.commit = commit;
            m.commit_term = commit_term;
            if campaign_type == CAMPAIGN_TRANSFER {
                m.context = campaign_type.into();
            }
            self.r.send(m, &mut self.msgs);
        }
        if voter_cnt > 0 {
            self.log_broadcast_vote(vote_msg, &voters[..voter_cnt]);
        }
    }

    #[inline]
    fn log_broadcast_vote(&self, t: MessageType, ids: &[u64]) {
        info!(
            self.logger,
            "broadcasting vote request";
            "type" => ?t,
            "term" => self.term,
            "log_term" => self.raft_log.last_term(),
            "log_index" => self.raft_log.last_index(),
            "to" => ?ids,
        );
    }

    /// Steps the raft along via a message. This should be called everytime your raft receives a
    /// message from a peer.
    pub fn step(&mut self, m: Message) -> Result<()> {
        // Handle the message term, which may result in our stepping down to a follower.
        if m.term == 0 {
            // local message
        } else if m.term > self.term {
            if m.get_msg_type() == MessageType::MsgRequestVote
                || m.get_msg_type() == MessageType::MsgRequestPreVote
            {
                let force = m.context == CAMPAIGN_TRANSFER;
                let in_lease = self.check_quorum
                    && self.leader_id != INVALID_ID
                    && self.election_elapsed < self.election_timeout;
                if !force && in_lease {
                    // if a server receives RequestVote request within the minimum election
                    // timeout of hearing from a current leader, it does not update its term
                    // or grant its vote
                    //
                    // This is included in the 3rd concern for Joint Consensus, where if another
                    // peer is removed from the cluster it may try to hold elections and disrupt
                    // stability.
                    info!(
                        self.logger,
                        "[logterm: {log_term}, index: {log_index}, vote: {vote}] ignored vote from \
                         {from} [logterm: {msg_term}, index: {msg_index}]: lease is not expired",
                        log_term = self.raft_log.last_term(),
                        log_index = self.raft_log.last_index(),
                        vote = self.vote,
                        from = m.from,
                        msg_term = m.log_term,
                        msg_index = m.index;
                        "term" => self.term,
                        "remaining ticks" => self.election_timeout - self.election_elapsed,
                        "msg type" => ?m.get_msg_type(),
                        "leader_id" => self.leader_id,
                    );

                    return Ok(());
                }
            }

            if m.get_msg_type() == MessageType::MsgRequestPreVote
                || (m.get_msg_type() == MessageType::MsgRequestPreVoteResponse && !m.reject)
            {
                // For a pre-vote request:
                // Never change our term in response to a pre-vote request.
                //
                // For a pre-vote response with pre-vote granted:
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                info!(
                    self.logger,
                    "received a message with higher term from {from}",
                    from = m.from;
                    "term" => self.term,
                    "message_term" => m.term,
                    "msg type" => ?m.get_msg_type(),
                );
                if m.get_msg_type() == MessageType::MsgAppend
                    || m.get_msg_type() == MessageType::MsgHeartbeat
                    || m.get_msg_type() == MessageType::MsgSnapshot
                {
                    self.become_follower(m.term, m.from);
                } else {
                    self.become_follower(m.term, INVALID_ID);
                }
            }
        } else if m.term < self.term {
            if (self.check_quorum || self.pre_vote)
                && (m.get_msg_type() == MessageType::MsgHeartbeat
                    || m.get_msg_type() == MessageType::MsgAppend)
            {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a higher
                // term, but if checkQuorum is true we may not advance the term on MsgVote and
                // must generate other messages to advance the term. The net result of these
                // two features is to minimize the disruption caused by nodes that have been
                // removed from the cluster's configuration: a removed node will send MsgVotes
                // which will be ignored, but it will not receive MsgApp or MsgHeartbeat, so it
                // will not create disruptive term increases, by notifying leader of this node's
                // activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "pb.MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                let to_send = new_message(m.from, MessageType::MsgAppendResponse, None);
                self.r.send(to_send, &mut self.msgs);
            } else if m.get_msg_type() == MessageType::MsgRequestPreVote {
                // Before pre_vote enable, there may be a receiving candidate with higher term,
                // but less log. After update to pre_vote, the cluster may deadlock if
                // we drop messages with a lower term.
                info!(
                    self.logger,
                    "{} [log_term: {}, index: {}, vote: {}] rejected {:?} from {} [log_term: {}, index: {}] at term {}",
                    self.id,
                    self.raft_log.last_term(),
                    self.raft_log.last_index(),
                    self.vote,
                    m.get_msg_type(),
                    m.from,
                    m.log_term,
                    m.index,
                    self.term,
                );

                let mut to_send = new_message(m.from, MessageType::MsgRequestPreVoteResponse, None);
                to_send.term = self.term;
                to_send.reject = true;
                self.r.send(to_send, &mut self.msgs);
            } else {
                // ignore other cases
                info!(
                    self.logger,
                    "ignored a message with lower term from {from}",
                    from = m.from;
                    "term" => self.term,
                    "msg type" => ?m.get_msg_type(),
                    "msg term" => m.term
                );
            }
            return Ok(());
        }

        #[cfg(feature = "failpoints")]
        fail_point!("before_step");

        // Leased options: a peer message at our current term proves the peer
        // stepped to this term, which destroyed any lease it held. Allowlisted
        // types only — local and synthetic messages carry no evidence about a
        // remote node's term, and pre-vote responses carry the candidate's
        // prospective term.
        if self.read_only.uses_read_leases()
            && m.term == self.term
            && m.from != INVALID_ID
            && m.from != self.id
            && matches!(
                m.get_msg_type(),
                MessageType::MsgAppendResponse
                    | MessageType::MsgHeartbeatResponse
                    | MessageType::MsgRequestVoteResponse
                    | MessageType::MsgAskAckIndexResp
                    | MessageType::MsgAskReadLease
            )
        {
            self.read_only.confirm_at_term(m.from);
        }

        match m.get_msg_type() {
            MessageType::MsgHup => self.hup(false),
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                // We can vote if this is a repeat of a vote we've already cast...
                let can_vote = (self.vote == m.from) ||
                    // ...we haven't voted and we don't think there's a leader yet in this term...
                    (self.vote == INVALID_ID && self.leader_id == INVALID_ID) ||
                    // ...or this is a PreVote for a future term...
                    (m.get_msg_type() == MessageType::MsgRequestPreVote && m.term > self.term);
                // ...and we believe the candidate is up to date.
                if can_vote
                    && self.raft_log.is_up_to_date(m.index, m.log_term)
                    && (m.index > self.raft_log.last_index() || self.priority <= get_priority(&m))
                {
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    self.log_vote_approve(&m);
                    let mut to_send =
                        new_message(m.from, vote_resp_msg_type(m.get_msg_type()), None);
                    to_send.reject = false;
                    to_send.term = m.term;
                    self.r.send(to_send, &mut self.msgs);
                    if m.get_msg_type() == MessageType::MsgRequestVote {
                        // Only record real votes.
                        self.election_elapsed = 0;
                        self.vote = m.from;
                    }
                } else {
                    self.log_vote_reject(&m);
                    let mut to_send =
                        new_message(m.from, vote_resp_msg_type(m.get_msg_type()), None);
                    to_send.reject = true;
                    to_send.term = self.term;
                    let (commit, commit_term) = self.raft_log.commit_info();
                    to_send.commit = commit;
                    to_send.commit_term = commit_term;
                    self.r.send(to_send, &mut self.msgs);
                    self.maybe_commit_by_vote(&m);
                }
            }
            _ => match self.state {
                StateRole::PreCandidate | StateRole::Candidate => self.step_candidate(m)?,
                StateRole::Follower => self.step_follower(m)?,
                StateRole::Leader => self.step_leader(m)?,
            },
        }
        // Central read-index delayer pump. In raft-rs the follower's commit index
        // only advances as a synchronous side-effect of stepping a message.
        // The recursive `step()` called from re-enqueuing these requests will get an empty set here.
        self.fire_delayed_read_index_requests();
        Ok(())
    }

    fn hup(&mut self, transfer_leader: bool) {
        if self.state == StateRole::Leader {
            debug!(
                self.logger,
                "ignoring MsgHup because already leader";
            );
            return;
        }

        // Scan all unapplied committed entries to find a config change.
        // Paginate the scan, to avoid a potentially unlimited memory spike.
        //
        // If there is a pending snapshot, its index will be returned by
        // `maybe_first_index`. Note that snapshot updates configuration
        // already, so as long as pending entries don't contain conf change
        // it's safe to start campaign.
        let low = match self.raft_log.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.raft_log.applied + 1,
        };
        let high = self.raft_log.committed + 1;
        let ctx = GetEntriesContext(GetEntriesFor::TransferLeader);
        if self.has_unapplied_conf_changes(low, high, ctx) {
            warn!(
                self.logger,
                "cannot campaign at term {} since there are still pending configuration changes to apply", self.term
            );
            return;
        }

        info!(
            self.logger,
            "starting a new election";
            "term" => self.term,
        );
        if transfer_leader {
            self.campaign(CAMPAIGN_TRANSFER);
        } else if self.pre_vote {
            self.campaign(CAMPAIGN_PRE_ELECTION);
        } else {
            self.campaign(CAMPAIGN_ELECTION);
        }
    }

    /// Every `RemoveNode` target named by a conf change in `entries`. A
    /// demotion to learner is not a removal: it keeps the node's `Progress`
    /// and its lease.
    fn removal_targets(entries: &[Entry]) -> Vec<u64> {
        let mut targets: Vec<u64> = Vec::new();
        for e in entries {
            let cc = match e.get_entry_type() {
                EntryType::EntryConfChange => {
                    let mut v1 = ConfChange::default();
                    if v1.merge_from_bytes(e.get_data()).is_err() {
                        continue;
                    }
                    v1.into_v2()
                }
                EntryType::EntryConfChangeV2 => {
                    let mut v2 = ConfChangeV2::default();
                    if v2.merge_from_bytes(e.get_data()).is_err() {
                        continue;
                    }
                    v2
                }
                _ => continue,
            };
            for change in cc.get_changes() {
                if change.get_change_type() == ConfChangeType::RemoveNode {
                    targets.push(change.node_id);
                }
            }
        }
        targets
    }

    /// Scans the unapplied log tail for configuration changes, seeding the two
    /// sets the new-leader safeguard depends on: every node id a conf change
    /// names becomes an extra confirmation the shortcut must collect (an
    /// unapplied addition is absent from `prs` but may hold a prior-term
    /// lease), and every `RemoveNode` target is barred from being granted a
    /// lease, or from renewing its self-lease if it is this node. Called once,
    /// from `become_leader`.
    fn scan_unapplied_conf_changes(&mut self) {
        let lo = self.raft_log.applied + 1;
        let hi = self.raft_log.last_index() + 1;
        if lo >= hi {
            return;
        }
        let page_size = self.max_committed_size_per_ready;
        let ctx = GetEntriesContext(GetEntriesFor::TransferLeader);
        let mut required: Vec<u64> = Vec::new();
        let mut banned: Vec<u64> = Vec::new();
        let scan = self.raft_log.scan(lo, hi, page_size, ctx, |ents| {
            for e in ents {
                let cc = match e.get_entry_type() {
                    EntryType::EntryConfChange => {
                        let mut v1 = ConfChange::default();
                        if v1.merge_from_bytes(e.get_data()).is_err() {
                            continue;
                        }
                        v1.into_v2()
                    }
                    EntryType::EntryConfChangeV2 => {
                        let mut v2 = ConfChangeV2::default();
                        if v2.merge_from_bytes(e.get_data()).is_err() {
                            continue;
                        }
                        v2
                    }
                    _ => continue,
                };
                for change in cc.get_changes() {
                    required.push(change.node_id);
                    if change.get_change_type() == ConfChangeType::RemoveNode {
                        banned.push(change.node_id);
                    }
                }
            }
            true
        });
        if scan.is_err() {
            // Entries above `applied` are never compacted, so this is a safety
            // net; fall back to the full timer rather than a partial set.
            error!(
                self.logger,
                "failed scanning unapplied entries [{lo}, {hi}) for conf changes; \
                 new-leader safeguard shortcut disabled this term",
                lo = lo,
                hi = hi;
                "error" => ?scan.unwrap_err(),
            );
            self.read_only.safeguard.mark_scan_failed();
            // The tail may hold this node's own removal; assume it does.
            self.read_only.ban_self_lease();
            return;
        }
        for id in required {
            self.read_only.safeguard.add_unapplied_conf_node(id);
        }
        for id in banned {
            if id == self.id {
                self.read_only.ban_self_lease();
            } else {
                self.read_only.leader.ban_grant(id);
            }
        }
    }

    fn has_unapplied_conf_changes(&self, lo: u64, hi: u64, context: GetEntriesContext) -> bool {
        if self.raft_log.applied >= self.raft_log.committed {
            // in fact applied == committed
            return false;
        }
        let mut found = false;
        // Reuse the max_committed_size_per_ready limit because it is used for
        // similar purposes (limiting the read of unapplied committed entries)
        // when raft sends entries via the Ready struct for application.
        // TODO(pavelkalinnikov): find a way to budget memory/bandwidth for this scan
        // outside the raft package.
        let page_size = self.max_committed_size_per_ready;
        if let Err(err) = self.raft_log.scan(lo, hi, page_size, context, |ents| {
            for e in ents {
                if e.get_entry_type() == EntryType::EntryConfChange
                    || e.get_entry_type() == EntryType::EntryConfChangeV2
                {
                    found = true;
                    return false;
                }
            }
            true
        }) {
            fatal!(
                self.logger,
                "error scanning unapplied entries [{}, {}): {:?}",
                lo,
                hi,
                err
            );
        }
        found
    }

    fn log_vote_approve(&self, m: &Message) {
        info!(
            self.logger,
            "[logterm: {log_term}, index: {log_index}, vote: {vote}] cast vote for {from} [logterm: {msg_term}, index: {msg_index}] \
             at term {term}",
            log_term = self.raft_log.last_term(),
            log_index = self.raft_log.last_index(),
            vote = self.vote,
            from = m.from,
            msg_term = m.log_term,
            msg_index = m.index,
            term = self.term;
            "msg type" => ?m.get_msg_type(),
        );
    }

    fn log_vote_reject(&self, m: &Message) {
        info!(
            self.logger,
            "[logterm: {log_term}, index: {log_index}, vote: {vote}] rejected vote from {from} [logterm: {msg_term}, index: \
             {msg_index}] at term {term}",
            log_term = self.raft_log.last_term(),
            log_index = self.raft_log.last_index(),
            vote = self.vote,
            from = m.from,
            msg_term = m.log_term,
            msg_index = m.index,
            term = self.term;
            "msg type" => ?m.get_msg_type(),
        );
    }

    fn handle_append_response(&mut self, m: &Message) {
        let mut next_probe_index: u64 = m.reject_hint;
        // pull out find_conflict_by_term for immutable borrow
        if m.reject && m.log_term > 0 {
            // If the follower has an uncommitted log tail, we would end up
            // probing one by one until we hit the common prefix.
            //
            // For example, if the leader has:
            //
            //   idx        1 2 3 4 5 6 7 8 9
            //              -----------------
            //   term (L)   1 3 3 3 5 5 5 5 5
            //   term (F)   1 1 1 1 2 2
            //
            // Then, after sending an append anchored at (idx=9,term=5) we
            // would receive a RejectHint of 6 and LogTerm of 2. Without the
            // code below, we would try an append at index 6, which would
            // fail again.
            //
            // However, looking only at what the leader knows about its own
            // log and the rejection hint, it is clear that a probe at index
            // 6, 5, 4, 3, and 2 must fail as well:
            //
            // For all of these indexes, the leader's log term is larger than
            // the rejection's log term. If a probe at one of these indexes
            // succeeded, its log term at that index would match the leader's,
            // i.e. 3 or 5 in this example. But the follower already told the
            // leader that it is still at term 2 at index 6, and since the
            // log term only ever goes up (within a log), this is a contradiction.
            //
            // At index 1, however, the leader can draw no such conclusion,
            // as its term 1 is not larger than the term 2 from the
            // follower's rejection. We thus probe at 1, which will succeed
            // in this example. In general, with this approach we probe at
            // most once per term found in the leader's log.
            //
            // There is a similar mechanism on the follower (implemented in
            // handleAppendEntries via a call to findConflictByTerm) that is
            // useful if the follower has a large divergent uncommitted log
            // tail[1], as in this example:
            //
            //   idx        1 2 3 4 5 6 7 8 9
            //              -----------------
            //   term (L)   1 3 3 3 3 3 3 3 7
            //   term (F)   1 3 3 4 4 5 5 5 6
            //
            // Naively, the leader would probe at idx=9, receive a rejection
            // revealing the log term of 6 at the follower. Since the leader's
            // term at the previous index is already smaller than 6, the leader-
            // side optimization discussed above is ineffective. The leader thus
            // probes at index 8 and, naively, receives a rejection for the same
            // index and log term 5. Again, the leader optimization does not improve
            // over linear probing as term 5 is above the leader's term 3 for that
            // and many preceding indexes; the leader would have to probe linearly
            // until it would finally hit index 3, where the probe would succeed.
            //
            // Instead, we apply a similar optimization on the follower. When the
            // follower receives the probe at index 8 (log term 3), it concludes
            // that all of the leader's log preceding that index has log terms of
            // 3 or below. The largest index in the follower's log with a log term
            // of 3 or below is index 3. The follower will thus return a rejection
            // for index=3, log term=3 instead. The leader's next probe will then
            // succeed at that index.
            //
            // [1]: more precisely, if the log terms in the large uncommitted
            // tail on the follower are larger than the leader's. At first,
            // it may seem unintuitive that a follower could even have such
            // a large tail, but it can happen:
            //
            // 1. Leader appends (but does not commit) entries 2 and 3, crashes.
            //   idx        1 2 3 4 5 6 7 8 9
            //              -----------------
            //   term (L)   1 2 2     [crashes]
            //   term (F)   1
            //   term (F)   1
            //
            // 2. a follower becomes leader and appends entries at term 3.
            //              -----------------
            //   term (x)   1 2 2     [down]
            //   term (F)   1 3 3 3 3
            //   term (F)   1
            //
            // 3. term 3 leader goes down, term 2 leader returns as term 4
            //    leader. It commits the log & entries at term 4.
            //
            //              -----------------
            //   term (L)   1 2 2 2
            //   term (x)   1 3 3 3 3 [down]
            //   term (F)   1
            //              -----------------
            //   term (L)   1 2 2 2 4 4 4
            //   term (F)   1 3 3 3 3 [gets probed]
            //   term (F)   1 2 2 2 4 4 4
            //
            // 4. the leader will now probe the returning follower at index
            //    7, the rejection points it at the end of the follower's log
            //    which is at a higher log term than the actually committed
            //    log.
            next_probe_index = self
                .raft_log
                .find_conflict_by_term(m.reject_hint, m.log_term)
                .0;
        }

        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        pr.recent_active = true;

        // update followers committed index via append response
        pr.update_committed(m.commit);

        if m.reject {
            // RejectHint is the suggested next base entry for appending (i.e.
            // we try to append entry RejectHint+1 next), and LogTerm is the
            // term that the follower has at index RejectHint. Older versions
            // of this library did not populate LogTerm for rejections and it
            // is zero for followers with an empty log.
            //
            // Under normal circumstances, the leader's log is longer than the
            // follower's and the follower's log is a prefix of the leader's
            // (i.e. there is no divergent uncommitted suffix of the log on the
            // follower). In that case, the first probe reveals where the
            // follower's log ends (RejectHint=follower's last index) and the
            // subsequent probe succeeds.
            //
            // However, when networks are partitioned or systems overloaded,
            // large divergent log tails can occur. The naive attempt, probing
            // entry by entry in decreasing order, will be the product of the
            // length of the diverging tails and the network round-trip latency,
            // which can easily result in hours of time spent probing and can
            // even cause outright outages. The probes are thus optimized as
            // described below.
            debug!(
                self.r.logger,
                "received msgAppend rejection";
                "reject_hint_index" => m.reject_hint,
                "reject_hint_term" => m.log_term,
                "from" => m.from,
                "index" => m.index,
            );

            if pr.maybe_decr_to(m.index, next_probe_index, m.request_snapshot) {
                debug!(
                    self.r.logger,
                    "decreased progress of {}",
                    m.from;
                    "progress" => ?pr,
                );
                if pr.state == ProgressState::Replicate {
                    pr.become_probe();
                }
                self.send_append(m.from);
            }
            return;
        }

        let old_paused = pr.is_paused();
        if !pr.maybe_update(m.index) {
            return;
        }

        // GrantLeases: the append response is a fresher acked index for this
        // holder than the commit clamp currently has. `matched` advances from
        // any ack; the grant only from the incarnation that asked for it
        // (`log_term`, see `mark_append_ack`).
        if self.read_only.option == ReadOnlyOption::GrantLeases
            && self.read_only.ack_matches_grant(m.from, m.log_term)
        {
            self.read_only
                .leader
                .granted_leases
                .update_acked_index(m.from, m.index);
        }

        let pr = self.prs.get_mut(m.from).unwrap();
        match pr.state {
            ProgressState::Probe => pr.become_replicate(),
            ProgressState::Snapshot => {
                if pr.is_snapshot_caught_up() {
                    debug!(
                        self.r.logger,
                        "snapshot caught up, resumed sending replication messages to {from}",
                        from = m.from;
                        "progress" => ?pr,
                    );
                    pr.become_probe();
                }
            }
            ProgressState::Replicate => pr.ins.free_to(m.get_index()),
        }

        if self.maybe_commit() {
            if self.should_bcast_commit() {
                self.bcast_append()
            }
        } else if old_paused {
            self.send_append(m.from);
        }

        // We've updated flow control information above, which may
        // allow us to send multiple (size-limited) in-flight messages
        // at once (such as when transitioning from probe to
        // replicate, or when freeTo() covers multiple messages). If
        // we have more entries to send, send as many messages as we
        // can (without sending empty messages for the commit index)
        self.send_append_aggressively(m.from);

        // Transfer leadership is in progress.
        if Some(m.from) == self.r.lead_transferee {
            let last_index = self.r.raft_log.last_index();
            let pr = self.prs.get_mut(m.from).unwrap();
            if pr.matched == last_index {
                info!(
                    self.logger,
                    "sent MsgTimeoutNow to {from} after received MsgAppResp",
                    from = m.from;
                );
                self.send_timeout_now(m.from);
            }
        }
    }

    fn handle_heartbeat_response(&mut self, m: &Message) {
        // Update the node. Drop the value explicitly since we'll check the qourum after.
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        // update followers committed index via heartbeat response
        pr.update_committed(m.commit);
        pr.recent_active = true;
        pr.resume();

        // free one slot for the full inflights window to allow progress.
        if pr.state == ProgressState::Replicate && pr.ins.full() {
            pr.ins.free_first_one();
        }
        // Does it request snapshot?
        if pr.matched < self.r.raft_log.last_index() || pr.pending_request_snapshot != INVALID_INDEX
        {
            self.r.send_append(m.from, pr, &mut self.msgs);
        }

        // `index` carries the lease round the follower is echoing.
        if self.read_only.uses_read_leases() && m.index != 0 {
            self.step_lease_echo(m.from, m.index);
        }

        // A context means a read-index round in any option, not only `Safe`:
        // the leased options fall back to one when the leader has no live
        // self-lease. Identical to the old `option != Safe || ...` test for
        // `Safe`.
        if m.context.is_empty() {
            return;
        }

        match self.r.read_only.recv_ack(m.from, &m.context) {
            Some(acks) if self.prs.has_quorum(acks) => {}
            _ => return,
        }

        for rs in self.r.read_only.advance(&m.context, &self.r.logger) {
            if let Some(m) = self.handle_ready_read_index(rs.req, rs.index, ReadProvenance::Quorum)
            {
                self.r.send(m, &mut self.msgs);
            }
        }
    }

    fn handle_transfer_leader(&mut self, m: &Message) {
        if self.prs().get(m.from).is_none() {
            debug!(
                self.logger,
                "no progress available for {}",
                m.from;
            );
            return;
        }

        let from = m.from;
        if self.prs.conf().learners.contains(&from) {
            debug!(
                self.logger,
                "ignored transferring leadership";
                "to" => from,
            );
            return;
        }
        let lead_transferee = from;
        if let Some(last_lead_transferee) = self.lead_transferee {
            if last_lead_transferee == lead_transferee {
                info!(
                    self.logger,
                    "[term {term}] transfer leadership to {lead_transferee} is in progress, ignores request \
                     to same node {lead_transferee}",
                    term = self.term,
                    lead_transferee = lead_transferee;
                );
                return;
            }
            self.abort_leader_transfer();
            info!(
                self.logger,
                "[term {term}] abort previous transferring leadership to {last_lead_transferee}",
                term = self.term,
                last_lead_transferee = last_lead_transferee;
            );
        }
        if lead_transferee == self.id {
            debug!(
                self.logger,
                "already leader; ignored transferring leadership to self";
            );
            return;
        }
        // Transfer leadership to third party.
        info!(
            self.logger,
            "[term {term}] starts to transfer leadership to {lead_transferee}",
            term = self.term,
            lead_transferee = lead_transferee;
        );
        // Transfer leadership should be finished in one electionTimeout
        // so reset r.electionElapsed.
        //
        // CheckQuorum's state is this cadence counter *and* the per-peer
        // `recent_active` flags; the `MsgCheckQuorum` boundary always advances
        // both together. Resetting only the counter is a torn update: flags set
        // before the transfer survive the next boundary and stretch step-down
        // to `2 * election_timeout`. It matters here because `CAMPAIGN_TRANSFER`
        // bypasses the `in_lease` vote filter, so the transferee wins
        // immediately. See etcd-io/raft#99. Clearing is safe on a healthy
        // cluster for the same reason the ordinary boundary clear is: heartbeats
        // refill the flags long before the next boundary.
        self.election_elapsed = 0;
        let self_id = self.id;
        for (id, pr) in self.mut_prs().iter_mut() {
            if *id != self_id {
                pr.recent_active = false;
            }
        }
        self.lead_transferee = Some(lead_transferee);
        let pr = self.prs.get_mut(from).unwrap();
        if pr.matched == self.r.raft_log.last_index() {
            self.send_timeout_now(lead_transferee);
            info!(
                self.logger,
                "sends MsgTimeoutNow to {lead_transferee} immediately as {lead_transferee} already has up-to-date log",
                lead_transferee = lead_transferee;
            );
        } else {
            self.r.send_append(lead_transferee, pr, &mut self.msgs);
        }
    }

    fn handle_snapshot_status(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        if pr.state != ProgressState::Snapshot {
            return;
        }
        if m.reject {
            pr.snapshot_failure();
            pr.become_probe();
            debug!(
                self.r.logger,
                "snapshot failed, resumed sending replication messages to {from}",
                from = m.from;
                "progress" => ?pr,
            );
        } else {
            pr.become_probe();
            debug!(
                self.r.logger,
                "snapshot succeeded, resumed sending replication messages to {from}",
                from = m.from;
                "progress" => ?pr,
            );
        }
        // If snapshot finish, wait for the msgAppResp from the remote node before sending
        // out the next msgAppend.
        // If snapshot failure, wait for a heartbeat interval before next try
        pr.pause();
        pr.pending_request_snapshot = INVALID_INDEX;
    }

    fn handle_unreachable(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        // During optimistic replication, if the remote becomes unreachable,
        // there is huge probability that a MsgAppend is lost.
        if pr.state == ProgressState::Replicate {
            pr.become_probe();
        }
        debug!(
            self.r.logger,
            "failed to send message to {from} because it is unreachable",
            from = m.from;
            "progress" => ?pr,
        );
    }

    fn step_leader(&mut self, mut m: Message) -> Result<()> {
        // These message types do not require any progress for m.From.
        match m.get_msg_type() {
            MessageType::MsgBeat => {
                self.bcast_heartbeat();
                return Ok(());
            }
            MessageType::MsgCheckQuorum => {
                if !self.check_quorum_active() {
                    warn!(
                        self.logger,
                        "stepped down to follower since quorum is not active";
                    );
                    let term = self.term;
                    self.become_follower(term, INVALID_ID);
                }
                return Ok(());
            }
            MessageType::MsgPropose => {
                if m.entries.is_empty() {
                    fatal!(self.logger, "stepped empty MsgProp");
                }
                if !self.prs.progress().contains_key(&self.id) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return Err(Error::ProposalDropped);
                }
                if self.lead_transferee.is_some() {
                    debug!(
                        self.logger,
                        "[term {term}] transfer leadership to {lead_transferee} is in progress; dropping \
                         proposal",
                        term = self.term,
                        lead_transferee = self.lead_transferee.unwrap();
                    );
                    return Err(Error::ProposalDropped);
                }
                // A freshly-elected leader under a leased option must wait out a
                // full lease window before accepting writes, so any lease a prior
                // leader granted or held has provably expired.
                if self.read_only.uses_read_leases() && !self.new_leader_safeguard_passed() {
                    debug!(
                        self.logger,
                        "[term {term}] read-only lease safeguard has not passed; dropping proposal",
                        term = self.term;
                    );
                    return Err(Error::ProposalDropped);
                }

                // Leased options: a leader proposing its own removal gives up its
                // self-lease now, before the entry is appended. A successor that
                // has applied the removal will not wait for this node. The
                // proposal itself goes ahead.
                if self.read_only.uses_read_leases()
                    && Self::removal_targets(m.get_entries()).contains(&self.id)
                {
                    self.read_only.ban_self_lease();
                    info!(
                        self.logger,
                        "proposing own removal; self-lease banned for the rest of the term";
                    );
                }

                // GrantLeases: a removed node keeps serving reads locally from
                // a lease the leader can no longer reach, so bar every removal
                // target from acquiring one and refuse the change while a
                // target still holds one.
                if self.read_only.option == ReadOnlyOption::GrantLeases {
                    let targets = Self::removal_targets(m.get_entries());
                    if let Some(holder) = self.read_only.ban_grants(&targets) {
                        info!(
                            self.logger,
                            "refusing conf change: {holder} still holds a read lease; \
                             draining it first",
                            holder = holder;
                        );
                        return Err(Error::ProposalDropped);
                    }
                }

                for (i, e) in m.mut_entries().iter_mut().enumerate() {
                    let mut cc;
                    if e.get_entry_type() == EntryType::EntryConfChange {
                        let mut cc_v1 = ConfChange::default();
                        if let Err(e) = cc_v1.merge_from_bytes(e.get_data()) {
                            error!(self.logger, "invalid confchange"; "error" => ?e);
                            return Err(Error::ProposalDropped);
                        }
                        cc = cc_v1.into_v2();
                    } else if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                        cc = ConfChangeV2::default();
                        if let Err(e) = cc.merge_from_bytes(e.get_data()) {
                            error!(self.logger, "invalid confchangev2"; "error" => ?e);
                            return Err(Error::ProposalDropped);
                        }
                    } else {
                        continue;
                    }

                    let reason = if self.has_pending_conf() {
                        "possible unapplied conf change"
                    } else {
                        let already_joint = confchange::joint(self.prs.conf());
                        let want_leave = cc.changes.is_empty();
                        if already_joint && !want_leave {
                            "must transition out of joint config first"
                        } else if !already_joint && want_leave {
                            "not in joint state; refusing empty conf change"
                        } else {
                            ""
                        }
                    };

                    if reason.is_empty() {
                        self.pending_conf_index = self.raft_log.last_index() + i as u64 + 1;
                    } else {
                        info!(
                            self.logger,
                            "ignoring conf change";
                            "conf change" => ?cc,
                            "reason" => reason,
                            "config" => ?self.prs.conf(),
                            "index" => self.pending_conf_index,
                            "applied" => self.raft_log.applied,
                        );
                        *e = Entry::default();
                        e.set_entry_type(EntryType::EntryNormal);
                    }
                }
                if !self.append_entry(m.mut_entries()) {
                    // return ProposalDropped when uncommitted size limit is reached
                    debug!(
                        self.logger,
                        "entries are dropped due to overlimit of max uncommitted size, uncommitted_size: {}",
                        self.uncommitted_size()
                    );
                    return Err(Error::ProposalDropped);
                }
                self.bcast_append();
                return Ok(());
            }
            MessageType::MsgReadIndex => {
                if !self.commit_to_current_term() {
                    // `raft_log.committed` is only a lower bound until this
                    // leader commits in its own term. Under the leased options
                    // the safeguard makes that gap a full lease window, so
                    // postpone the read instead of dropping it; `Safe` keeps the
                    // existing drop.
                    if self.read_only.uses_read_leases() {
                        info!(
                            self.logger,
                            "leader has not yet committed in its term; postponing read index msg",
                        );
                        self.r.pending_read_index_messages.push(m);
                    } else {
                        info!(
                            self.logger,
                            "leader has not yet committed in its term; dropping read index msg",
                        );
                    }
                    return Ok(());
                }

                // thinking: use an internally defined context instead of the user given context.
                // We can express this in terms of the term and index instead of
                // a user-supplied value.
                // This would allow multiple reads to piggyback on the same message.
                self.leader_read_index(m);
                return Ok(());
            }
            _ => {}
        }

        match m.get_msg_type() {
            MessageType::MsgAppendResponse => {
                self.handle_append_response(&m);
            }
            MessageType::MsgHeartbeatResponse => {
                self.handle_heartbeat_response(&m);
            }
            MessageType::MsgSnapStatus => {
                self.handle_snapshot_status(&m);
            }
            MessageType::MsgUnreachable => {
                self.handle_unreachable(&m);
            }
            MessageType::MsgTransferLeader => {
                self.handle_transfer_leader(&m);
            }
            MessageType::MsgAskReadLease => {
                self.handle_ask_read_lease(&m);
            }
            MessageType::MsgAskAckIndexResp => {
                self.handle_ask_ack_index_resp(&m);
            }
            _ => {
                if self.prs().get(m.from).is_none() {
                    debug!(
                        self.logger,
                        "no progress available for {}",
                        m.from;
                    );
                }
            }
        }

        Ok(())
    }

    /// Commits the logs using commit info in vote message.
    fn maybe_commit_by_vote(&mut self, m: &Message) {
        if m.commit == 0 || m.commit_term == 0 {
            return;
        }
        let last_commit = self.raft_log.committed;
        if m.commit <= last_commit || self.state == StateRole::Leader {
            return;
        }
        if !self.raft_log.maybe_commit(m.commit, m.commit_term) {
            return;
        }

        let log = &mut self.r.raft_log;
        info!(self.r.logger, "[commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to vote request [index: {}, term: {}]",
                log.committed, log.last_index(), log.last_term(), m.commit, m.commit_term);

        if self.state != StateRole::Candidate && self.state != StateRole::PreCandidate {
            return;
        }

        // Scan all unapplied committed entries to find a config change.
        // Paginate the scan, to avoid a potentially unlimited memory spike.
        let low = last_commit + 1;
        let high = self.raft_log.committed + 1;
        let ctx = GetEntriesContext(GetEntriesFor::CommitByVote);
        if self.has_unapplied_conf_changes(low, high, ctx) {
            // The candidate doesn't have to step down in theory, here just for best
            // safety as we assume quorum won't change during election.
            let term = self.term;
            self.become_follower(term, INVALID_ID);
        }
    }

    fn poll(&mut self, from: u64, t: MessageType, vote: bool) -> VoteResult {
        self.prs.record_vote(from, vote);
        let (gr, rj, res) = self.prs.tally_votes();
        // Unlike etcd, we log when necessary.
        if from != self.id {
            info!(
                self.logger,
                "received votes response";
                "vote" => vote,
                "from" => from,
                "rejections" => rj,
                "approvals" => gr,
                "type" => ?t,
                "term" => self.term,
            );
        }

        match res {
            VoteResult::Won => {
                if self.state == StateRole::PreCandidate {
                    self.campaign(CAMPAIGN_ELECTION);
                } else {
                    self.become_leader();
                    self.bcast_append();
                }
            }
            VoteResult::Lost => {
                // pb.MsgPreVoteResp contains future term of pre-candidate
                // m.term > self.term; reuse self.term
                let term = self.term;
                self.become_follower(term, INVALID_ID);
            }
            VoteResult::Pending => (),
        }
        res
    }

    // step_candidate is shared by state Candidate and PreCandidate; the difference is
    // whether they respond to MsgRequestVote or MsgRequestPreVote.
    fn step_candidate(&mut self, m: Message) -> Result<()> {
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                info!(
                    self.logger,
                    "no leader at term {term}; dropping proposal",
                    term = self.term;
                );
                return Err(Error::ProposalDropped);
            }
            MessageType::MsgAppend => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_append_entries(&m);
            }
            MessageType::MsgHeartbeat => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m);
            }
            MessageType::MsgRequestPreVoteResponse | MessageType::MsgRequestVoteResponse => {
                // Only handle vote responses corresponding to our candidacy (while in
                // state Candidate, we may get stale MsgPreVoteResp messages in this term from
                // our pre-candidate state).
                if (self.state == StateRole::PreCandidate
                    && m.get_msg_type() != MessageType::MsgRequestPreVoteResponse)
                    || (self.state == StateRole::Candidate
                        && m.get_msg_type() != MessageType::MsgRequestVoteResponse)
                {
                    return Ok(());
                }

                self.poll(m.from, m.get_msg_type(), !m.reject);
                self.maybe_commit_by_vote(&m);
            }
            MessageType::MsgTimeoutNow => debug!(
                self.logger,
                "{term} ignored MsgTimeoutNow from {from}",
                term = self.term,
                from = m.from;
                "state" => ?self.state,
            ),
            MessageType::MsgReadIndex => info!(
                self.logger,
                "no leader at term {term}; dropping read index msg",
                term = self.term;
            ),
            _ => {}
        }
        Ok(())
    }

    fn step_follower(&mut self, mut m: Message) -> Result<()> {
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                if self.leader_id == INVALID_ID {
                    info!(
                        self.logger,
                        "no leader at term {term}; dropping proposal",
                        term = self.term;
                    );
                    return Err(Error::ProposalDropped);
                } else if self.disable_proposal_forwarding {
                    info!(
                        self.logger,
                        "{from} not forwarding to leader {to} at term {term}; dropping proposal",
                        from = self.id,
                        to = self.leader_id,
                        term = self.term;
                    );
                    return Err(Error::ProposalDropped);
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_append_entries(&m);
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_snapshot(m);
            }
            MessageType::MsgTransferLeader => {
                if self.leader_id == INVALID_ID {
                    info!(
                        self.logger,
                        "no leader at term {term}; dropping leader transfer msg",
                        term = self.term;
                    );
                    return Ok(());
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            }
            MessageType::MsgTimeoutNow => {
                if self.promotable {
                    info!(
                        self.logger,
                        "[term {term}] received MsgTimeoutNow from {from} and starts an election to \
                         get leadership.",
                        term = self.term,
                        from = m.from;
                    );
                    // Leadership transfers never use pre-vote even if self.pre_vote is true; we
                    // know we are not recovering from a partition so there is no need for the
                    // extra round trip.
                    self.hup(true);
                } else {
                    info!(
                        self.logger,
                        "received MsgTimeoutNow from {} but is not promotable",
                        m.from;
                    );
                }
            }
            MessageType::MsgReadIndex => {
                if self.leader_id == INVALID_ID {
                    info!(
                        self.logger,
                        "no leader at term {term}; dropping read index msg",
                        term = self.term;
                    );
                    return Ok(());
                }
                if self.read_only.option == ReadOnlyOption::GrantLeases {
                    // Only this node's own reads keep its lease renewing; a read
                    // another node forwarded through here is not ours (EXP-12).
                    if m.from == INVALID_ID || m.from == self.id {
                        self.read_only.mark_lease_use();
                    }
                    self.serve_read_index_with_lease(m);
                    // Fire on every read in GrantLeases mode, served or not.
                    // TODO: Make this toggleable.
                    self.maybe_ask_for_read_lease();
                } else {
                    m.to = self.leader_id;
                    self.r.send(m, &mut self.msgs);
                }
            }
            MessageType::MsgReadIndexResp => {
                if m.entries.len() != 1 {
                    error!(
                        self.logger,
                        "invalid format of MsgReadIndexResp from {}",
                        m.from;
                        "entries count" => m.entries.len(),
                    );
                    return Ok(());
                }
                let rs = ReadState {
                    index: m.index,
                    request_ctx: m.take_entries()[0].take_data().to_vec(),
                    // Answered by the leader, not a follower lease. How the
                    // leader answered it is its own business.
                    served_by_follower_lease: false,
                    served_by_leader_lease: false,
                    // `step` drops a lower-term response and adopts a higher
                    // term, so this is the answering leader's term.
                    term: self.term,
                };
                self.read_states.push(rs);
                // `index` and `term` in MsgReadIndexResp is the leader's commit index and its current term,
                // the log entry in the leader's commit index will always have the leader's current term,
                // because the leader only handle MsgReadIndex after it has committed log entry in its term.
                self.raft_log.maybe_commit(m.index, m.term);
            }
            MessageType::MsgAskReadLeaseResp => {
                if m.entries.len() != 1 {
                    error!(
                        self.logger,
                        "invalid format of MsgAskReadLeaseResp from {}",
                        m.from;
                        "entries count" => m.entries.len(),
                    );
                    return Ok(());
                }
                if m.reject {
                    // `log_term` echoes the lease id we asked with, so the
                    // pending entry can be dropped rather than leak until the
                    // next grant or term change.
                    self.read_only.clear_pending_lease(m.log_term);
                    info!(
                        self.logger,
                        "request for read lease was rejected by leader {} (leaseId: {})",
                        m.from,
                        m.log_term;
                    );
                    return Ok(());
                }
                let bytes: [u8; 48] = match (&m.entries[0].data[..]).try_into() {
                    Ok(b) => b,
                    Err(_) => {
                        warn!(
                            self.logger,
                            "failed to unmarshal read lease from {}",
                            m.from;
                            "data len" => m.entries[0].data.len(),
                        );
                        return Ok(());
                    }
                };
                let read_lease = ReadLease::unmarshal(&bytes);
                if read_lease.node_id != self.id {
                    warn!(
                        self.logger,
                        "received read lease for different node {node} from {from}",
                        node = read_lease.node_id,
                        from = m.from;
                    );
                    return Ok(());
                }
                let (lease_id, leader) = (read_lease.lease_id, m.from);
                match self.read_only.process_granted_lease(read_lease) {
                    Some(rl) => info!(
                        self.logger,
                        "granted read lease from leader {leader} (leaseId: {lease_id})",
                        leader = leader,
                        lease_id = lease_id;
                        "lease" => ?rl,
                    ),
                    None => info!(
                        self.logger,
                        "got read lease from leader {leader}, but dropped it \
                         (no matching pending leaseId {lease_id})",
                        leader = leader,
                        lease_id = lease_id;
                    ),
                }
            }
            MessageType::MsgAskAckIndex => {
                if self.read_only.option != ReadOnlyOption::GrantLeases {
                    warn!(
                        self.logger,
                        "received unexpected MsgAskAckIndex from {}",
                        m.from;
                    );
                    return Ok(());
                }
                let self_id = self.id;
                let mut resp = Message::default();
                resp.set_msg_type(MessageType::MsgAskAckIndexResp);
                resp.to = m.from;
                resp.from = self_id;
                resp.index = 0;
                resp.log_term = self.read_only.incarnation_id();
                if self.read_only.has_active_held_lease() {
                    // Mark before reporting: the mark is the promise, so no
                    // later grant can install an ack index below what the leader
                    // now clamps commit on.
                    let last_index = self.raft_log.last_index();
                    self.read_only.mark_self_acked_index(last_index);
                    resp.index = self.read_only.read_lease_acked_index();
                }
                let ack_index = resp.index;
                self.r.send(resp, &mut self.msgs);
                info!(
                    self.logger,
                    "sending MsgAskAckIndexResp to {to} for ack index {ack_index}",
                    to = m.from,
                    ack_index = ack_index;
                );
            }
            _ => {}
        }
        Ok(())
    }

    /// Request a snapshot from a leader.
    pub fn request_snapshot(&mut self) -> Result<()> {
        if self.state == StateRole::Leader {
            info!(
                self.logger,
                "can not request snapshot on leader; dropping request snapshot";
            );
        } else if self.leader_id == INVALID_ID {
            info!(
                self.logger,
                "no leader; dropping request snapshot";
                "term" => self.term,
            );
        } else if self.snap().is_some() {
            info!(
                self.logger,
                "there is a pending snapshot; dropping request snapshot";
            );
        } else if self.pending_request_snapshot != INVALID_INDEX {
            info!(
                self.logger,
                "there is a pending snapshot; dropping request snapshot";
            );
        } else {
            let request_index = self.raft_log.last_index();
            let request_index_term = self.raft_log.term(request_index).unwrap();
            if self.term == request_index_term {
                self.pending_request_snapshot = request_index;
                self.send_request_snapshot();
                return Ok(());
            }
            info! {
                self.logger,
                "mismatched term; dropping request snapshot";
                "term" => self.term,
                "last_term" => request_index_term,
            };
        }
        Err(Error::RequestSnapshotDropped)
    }

    /// GrantLeases: records `index` as this node's acked index (its read-lease
    /// promise) and stamps `resp`, a successful `MsgAppendResponse` acking
    /// `index`, with the incarnation holding that promise in `log_term`, which
    /// a successful response otherwise leaves unused. The leader credits the
    /// ack to a grant only from that incarnation (`ack_matches_grant`).
    /// Rejections keep `log_term` as the hint term. A no-op in other options,
    /// so their responses are unchanged.
    fn mark_append_ack(&mut self, resp: &mut Message, index: u64) {
        if self.read_only.option != ReadOnlyOption::GrantLeases {
            return;
        }
        self.read_only.mark_self_acked_index(index);
        resp.log_term = self.read_only.incarnation_id();
    }

    // TODO: revoke pub when there is a better way to test.
    /// For a given message, append the entries to the log.
    pub fn handle_append_entries(&mut self, m: &Message) {
        // A replicated write is lease use (EXP-12, review tack-08 F1): a
        // write-only region keeps its holders, whose leases the commit clamp
        // and switch credits act on. Heartbeats and commit-only appends don't
        // count, so a region with neither reads nor writes still goes quiet.
        if self.read_only.option == ReadOnlyOption::GrantLeases && !m.entries.is_empty() {
            self.read_only.mark_lease_use();
        }
        if self.pending_request_snapshot != INVALID_INDEX {
            self.send_request_snapshot();
            return;
        }
        if m.index < self.raft_log.committed {
            debug!(
                self.logger,
                "got message with lower index than committed.";
            );
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.committed;
            to_send.commit = self.raft_log.committed;
            // Our (already-committed) index is this node's acked index.
            let committed = self.raft_log.committed;
            self.mark_append_ack(&mut to_send, committed);
            self.r.send(to_send, &mut self.msgs);
            return;
        }

        let mut to_send = Message::default();
        to_send.to = m.from;
        to_send.set_msg_type(MessageType::MsgAppendResponse);

        if let Some((_, last_idx)) = self
            .raft_log
            .maybe_append(m.index, m.log_term, m.commit, &m.entries)
        {
            to_send.set_index(last_idx);
            // Our log advanced; record it as this node's acked index.
            self.mark_append_ack(&mut to_send, last_idx);
        } else {
            debug!(
                self.logger,
                "rejected msgApp [logterm: {msg_log_term}, index: {msg_index}] \
                from {from}",
                msg_log_term = m.log_term,
                msg_index = m.index,
                from = m.from;
                "index" => m.index,
                "logterm" => ?self.raft_log.term(m.index),
            );

            let hint_index = cmp::min(m.index, self.raft_log.last_index());
            let (hint_index, hint_term) =
                self.raft_log.find_conflict_by_term(hint_index, m.log_term);

            if hint_term.is_none() {
                fatal!(
                    self.logger,
                    "term({index}) must be valid",
                    index = hint_index
                )
            }

            to_send.index = m.index;
            to_send.reject = true;
            to_send.reject_hint = hint_index;
            to_send.log_term = hint_term.unwrap();
        }
        to_send.set_commit(self.raft_log.committed);
        self.r.send(to_send, &mut self.msgs);
    }

    // TODO: revoke pub when there is a better way to test.
    /// For a message, commit and send out heartbeat.
    pub fn handle_heartbeat(&mut self, mut m: Message) {
        self.raft_log.commit_to(m.commit);
        // GrantLeases: a heartbeat advances our commit, so record it as our
        // acked index and renew the lease if it's near expiry.
        if self.read_only.option == ReadOnlyOption::GrantLeases {
            let committed = self.raft_log.committed;
            self.read_only.mark_self_acked_index(committed);
            self.maybe_ask_for_read_lease();
        }
        if self.pending_request_snapshot != INVALID_INDEX {
            self.send_request_snapshot();
            return;
        }
        let mut to_send = Message::default();
        to_send.set_msg_type(MessageType::MsgHeartbeatResponse);
        to_send.to = m.from;
        to_send.context = m.take_context();
        to_send.commit = self.raft_log.committed;
        // Echo the leader's lease round (0 outside the leased options).
        to_send.index = m.index;
        self.r.send(to_send, &mut self.msgs);
    }

    fn handle_snapshot(&mut self, mut m: Message) {
        let metadata = m.get_snapshot().get_metadata();
        let (sindex, sterm) = (metadata.index, metadata.term);
        if self.restore(m.take_snapshot()) {
            info!(
                self.logger,
                "[commit: {commit}, term: {term}] restored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                term = self.term,
                commit = self.raft_log.committed,
                snapshot_index = sindex,
                snapshot_term = sterm;
            );
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.last_index();
            // The snapshot advanced our commit; record it as this node's acked index.
            let last_index = self.raft_log.last_index();
            self.mark_append_ack(&mut to_send, last_index);
            self.r.send(to_send, &mut self.msgs);
        } else {
            info!(
                self.logger,
                "[commit: {commit}] ignored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                commit = self.raft_log.committed,
                snapshot_index = sindex,
                snapshot_term = sterm;
            );
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.committed;
            // Record our (unchanged) commit as this node's acked index.
            let committed = self.raft_log.committed;
            self.mark_append_ack(&mut to_send, committed);
            self.r.send(to_send, &mut self.msgs);
        }
    }

    /// Recovers the state machine from a snapshot. It restores the log and the
    /// configuration of state machine.
    pub fn restore(&mut self, snap: Snapshot) -> bool {
        if snap.get_metadata().index < self.raft_log.committed {
            return false;
        }
        if self.state != StateRole::Follower {
            // This is defense-in-depth: if the leader somehow ended up applying a
            // snapshot, it could move into a new term without moving into a
            // follower state. This should never fire, but if it did, we'd have
            // prevented damage by returning early, so log only a loud warning.
            //
            // At the time of writing, the instance is guaranteed to be in follower
            // state when this method is called.
            warn!(self.logger, "non-follower attempted to restore snapshot"; "state" => ?self.state);
            self.become_follower(self.term + 1, INVALID_INDEX);
            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient is not in the
        // config. This shouldn't ever happen (at the time of writing) but lots of
        // code here and there assumes that r.id is in the progress tracker.
        let meta = snap.get_metadata();
        let (snap_index, snap_term) = (meta.index, meta.term);
        let cs = meta.get_conf_state();
        if cs
            .get_voters()
            .iter()
            .chain(cs.get_learners())
            .chain(cs.get_voters_outgoing())
            // `learners_next` doesn't need to be checked. According to the rules, if a peer in
            // `learners_next`, it has to be in `voters_outgoing`.
            .all(|id| *id != self.id)
        {
            warn!(self.logger, "attempted to restore snapshot but it is not in the ConfState"; "conf_state" => ?cs);
            return false;
        }

        // Now go ahead and actually restore.

        if self.pending_request_snapshot == INVALID_INDEX
            && self.raft_log.match_term(meta.index, meta.term)
        {
            info!(
                self.logger,
                "fast-forwarded commit to snapshot";
                "commit" => self.raft_log.committed,
                "last_index" => self.raft_log.last_index(),
                "last_term" => self.raft_log.last_term(),
                "snapshot_index" => snap_index,
                "snapshot_term" => snap_term
            );
            self.raft_log.commit_to(meta.index);
            return false;
        }

        self.raft_log.restore(snap);
        let cs = self
            .r
            .raft_log
            .pending_snapshot()
            .unwrap()
            .get_metadata()
            .get_conf_state();

        self.prs.clear();
        let last_index = self.raft_log.last_index();
        if let Err(e) = confchange::restore(&mut self.prs, last_index, cs) {
            // This should never happen. Either there's a bug in our config change
            // handling or the client corrupted the conf change.
            fatal!(self.logger, "unable to restore config {:?}: {}", cs, e);
        }
        let new_cs = self.post_conf_change();
        let cs = self
            .r
            .raft_log
            .pending_snapshot()
            .unwrap()
            .get_metadata()
            .get_conf_state();
        if !raft_proto::conf_state_eq(cs, &new_cs) {
            fatal!(self.logger, "invalid restore: {:?} != {:?}", cs, new_cs);
        }

        // TODO: this is untested and likely unneeded
        let pr = self.prs.get_mut(self.id).unwrap();
        pr.maybe_update(pr.next_idx - 1);

        self.pending_request_snapshot = INVALID_INDEX;

        info!(
            self.logger,
            "restored snapshot";
            "commit" => self.raft_log.committed,
            "last_index" => self.raft_log.last_index(),
            "last_term" => self.raft_log.last_term(),
            "snapshot_index" => snap_index,
            "snapshot_term" => snap_term,
        );

        true
    }

    /// Updates the in-memory state and, when necessary, carries out additional actions
    /// such as reacting to the removal of nodes or changed quorum requirements.
    pub fn post_conf_change(&mut self) -> ConfState {
        info!(self.logger, "switched to configuration"; "config" => ?self.prs.conf());
        // Evict read leases held by nodes no longer in the configuration.
        // `self.prs` already reflects the new config here,
        // so drop any lease whose holder is absent from it.
        // Also drop any request of theirs still parked for a lease round:
        // `ban_grants` normally does when the removal is proposed, but a conf
        // change can reach this leader without passing its own pre-pass.
        if self.read_only.option == ReadOnlyOption::GrantLeases {
            let mut departed: Vec<u64> = self
                .read_only
                .leader
                .granted_leases
                .leases
                .keys()
                .copied()
                .chain(self.read_only.leader.parked_grant_ids())
                .filter(|id| !self.prs.progress().contains_key(id))
                .collect();
            departed.sort_unstable();
            departed.dedup();
            for id in departed {
                self.read_only.leader.granted_leases.remove(id);
                self.read_only.leader.drop_pending_grant(id);
            }
        }
        // TODO: instead of creating a conf state, validating conf state inside
        // progress tracker is better.
        let cs = self.prs.conf().to_conf_state();
        let is_voter = self.prs.conf().voters.contains(self.id);
        self.promotable = is_voter;
        if !is_voter && self.state == StateRole::Leader {
            // This node is leader and was removed or demoted. We prevent demotions
            // at the time writing but hypothetically we handle them the same way as
            // removing the leader: stepping down into the next Term.
            //
            // TODO(tbg): step down (for sanity) and ask follower with largest Match
            // to TimeoutNow (to avoid interruption). This might still drop some
            // proposals but it's better than nothing.
            //
            // TODO(tbg): test this branch. It is untested at the time of writing.
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and there
        // are other nodes.
        if self.state != StateRole::Leader || cs.voters.is_empty() {
            return cs;
        }

        if self.maybe_commit() {
            // If the configuration change means that more entries are committed now,
            // broadcast/append to everyone in the updated config.
            self.bcast_append();
        } else {
            // Otherwise, still probe the newly added replicas; there's no reason to
            // let them wait out a heartbeat interval (or the next incoming proposal).
            let self_id = self.id;
            let core = &mut self.r;
            let msgs = &mut self.msgs;
            self.prs
                .iter_mut()
                .filter(|&(id, _)| *id != self_id)
                .for_each(|(id, pr)| {
                    core.maybe_send_append(*id, pr, false, msgs);
                });
        }

        // The quorum size is now smaller, consider to response some read requests.
        // If there is only one peer, all pending read requests must be responded.
        if let Some(ctx) = self.read_only.last_pending_request_ctx() {
            let prs = &self.prs;
            if self
                .r
                .read_only
                .recv_ack(self.id, &ctx)
                .is_some_and(|acks| prs.has_quorum(acks))
            {
                for rs in self.r.read_only.advance(&ctx, &self.r.logger) {
                    if let Some(m) =
                        self.handle_ready_read_index(rs.req, rs.index, ReadProvenance::Quorum)
                    {
                        self.r.send(m, &mut self.msgs);
                    }
                }
            }
        }

        if self
            .lead_transferee
            .is_some_and(|e| !self.prs.conf().voters.contains(e))
        {
            self.abort_leader_transfer();
        }
        cs
    }

    /// Check if there is any pending confchange.
    ///
    /// This method can be false positive.
    #[inline]
    pub fn has_pending_conf(&self) -> bool {
        self.pending_conf_index > self.raft_log.applied
    }

    /// Specifies if the commit should be broadcast.
    ///
    /// Under `GrantLeases` it is, whatever `skip_bcast_commit` says, while any
    /// follower holds a grant or has one parked (EXP-3). A holder serves a read
    /// only once its own commit index reaches the read's bar (its acked index,
    /// its grant's `log_index`, a switch hint), so a commit it learns only from
    /// the next append forces its reads to wait or forward. Notifying commit is
    /// always safe; it changes no lease rule.
    pub fn should_bcast_commit(&self) -> bool {
        !self.skip_bcast_commit
            || self.has_pending_conf()
            || (self.read_only.option == ReadOnlyOption::GrantLeases
                && self.read_only.leader.has_lease_holders())
    }

    /// Indicates whether state machine can be promoted to leader,
    /// which is true when it's a voter and its own id is in progress list.
    pub fn promotable(&self) -> bool {
        self.promotable
    }

    #[doc(hidden)]
    pub fn apply_conf_change(&mut self, cc: &ConfChangeV2) -> Result<ConfState> {
        let mut changer = Changer::new(&self.prs);
        let (cfg, changes) = if cc.leave_joint() {
            changer.leave_joint()?
        } else if let Some(auto_leave) = cc.enter_joint() {
            changer.enter_joint(auto_leave, &cc.changes)?
        } else {
            changer.simple(&cc.changes)?
        };
        self.prs
            .apply_conf(cfg, changes, self.raft_log.last_index());
        Ok(self.post_conf_change())
    }

    /// Returns a read-only reference to the progress set.
    pub fn prs(&self) -> &ProgressTracker {
        &self.prs
    }

    /// Returns a mutable reference to the progress set.
    pub fn mut_prs(&mut self) -> &mut ProgressTracker {
        &mut self.prs
    }

    // TODO: revoke pub when there is a better way to test.
    /// For a given hardstate, load the state into self.
    pub fn load_state(&mut self, hs: &HardState) {
        if hs.commit < self.raft_log.committed || hs.commit > self.raft_log.last_index() {
            fatal!(
                self.logger,
                "hs.commit {} is out of range [{}, {}]",
                hs.commit,
                self.raft_log.committed,
                self.raft_log.last_index()
            )
        }
        self.raft_log.committed = hs.commit;
        self.term = hs.term;
        self.vote = hs.vote;
    }

    /// `pass_election_timeout` returns true iff `election_elapsed` is greater
    /// than or equal to the randomized election timeout in
    /// [`election_timeout`, 2 * `election_timeout` - 1].
    pub fn pass_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    /// Regenerates and stores the election timeout.
    pub fn reset_randomized_election_timeout(&mut self) {
        let prev_timeout = self.randomized_election_timeout;
        let timeout =
            rand::thread_rng().gen_range(self.min_election_timeout..self.max_election_timeout);
        debug!(
            self.logger,
            "reset election timeout {prev_timeout} -> {timeout} at {election_elapsed}",
            prev_timeout = prev_timeout,
            timeout = timeout,
            election_elapsed = self.election_elapsed;
        );
        self.randomized_election_timeout = timeout;
    }

    // check_quorum_active returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // check_quorum_active also resets all recent_active to false.
    // check_quorum_active can only called by leader.
    fn check_quorum_active(&mut self) -> bool {
        let self_id = self.id;
        self.mut_prs().quorum_recently_active(self_id)
    }

    /// Issues a message to timeout immediately.
    pub fn send_timeout_now(&mut self, to: u64) {
        let msg = new_message(to, MessageType::MsgTimeoutNow, None);
        self.r.send(msg, &mut self.msgs);
    }

    /// Stops the transfer of a leader.
    pub fn abort_leader_transfer(&mut self) {
        self.lead_transferee = None;
    }

    fn send_request_snapshot(&mut self) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgAppendResponse);
        m.index = self.raft_log.committed;
        m.reject = true;
        m.reject_hint = self.raft_log.last_index();
        m.to = self.leader_id;
        m.request_snapshot = self.pending_request_snapshot;
        m.log_term = self.raft_log.term(m.reject_hint).unwrap();
        self.r.send(m, &mut self.msgs);
    }

    /// Records a resolved read index locally, or builds the `MsgReadIndexResp`
    /// for a remote requester. `provenance` says how the index was obtained and
    /// is recorded on a local `ReadState`.
    fn handle_ready_read_index(
        &mut self,
        mut req: Message,
        index: u64,
        provenance: ReadProvenance,
    ) -> Option<Message> {
        if req.from == INVALID_ID || req.from == self.id {
            let rs = ReadState {
                index,
                request_ctx: req.take_entries()[0].take_data().to_vec(),
                served_by_follower_lease: provenance == ReadProvenance::FollowerLease,
                served_by_leader_lease: provenance == ReadProvenance::LeaderLease,
                term: self.term,
            };
            self.read_states.push(rs);
            return None;
        }
        let mut to_send = Message::default();
        to_send.set_msg_type(MessageType::MsgReadIndexResp);
        to_send.to = req.from;
        to_send.index = index;
        to_send.set_entries(req.take_entries());
        Some(to_send)
    }

    /// Reduce size of 'ents' from uncommitted size.
    pub fn reduce_uncommitted_size(&mut self, ents: &[Entry]) {
        // fast path for non-leader endpoint
        if self.state != StateRole::Leader {
            return;
        }

        if !self.uncommitted_state.maybe_reduce_uncommitted_size(ents) {
            // this will make self.uncommitted size not accurate.
            // but in most situation, this behaviour will not cause big problem
            warn!(
                self.r.logger,
                "try to reduce uncommitted size less than 0, first index of pending ents is {}",
                ents[0].get_index()
            );
        }
    }

    /// Increase size of 'ents' to uncommitted size. Return true when size limit
    /// is satisfied. Otherwise return false and uncommitted size remains unchanged.
    /// For raft with no limit(or non-leader raft), it always return true.
    #[inline]
    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        self.uncommitted_state.maybe_increase_uncommitted_size(ents)
    }

    /// Return current uncommitted size recorded by uncommitted_state.
    #[inline]
    pub fn uncommitted_size(&self) -> usize {
        self.uncommitted_state.uncommitted_size
    }

    /// A Raft leader allocates a vector with capacity `max_inflight_msgs` for every peer.
    /// It takes a lot of memory if there are too many Raft groups. `maybe_free_inflight_buffers`
    /// is used to free memory if necessary.
    pub fn maybe_free_inflight_buffers(&mut self) {
        for (_, pr) in self.mut_prs().iter_mut() {
            pr.ins.maybe_free_buffer();
        }
    }

    /// To adjust `max_inflight_msgs` for the specified peer.
    /// Set to `0` will disable the progress.
    pub fn adjust_max_inflight_msgs(&mut self, target: u64, cap: usize) {
        if let Some(pr) = self.mut_prs().get_mut(target) {
            pr.ins.set_cap(cap);
        }
    }
}
