// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Integration coverage for `ReadOnlyOption::GrantLeases` (follower read
//! leases). These target the two lease-grant safety rules that the unit tests
//! in `read_only.rs` cannot reach, because both live in `raft.rs` and depend on
//! node state (role, configuration) rather than on `ReadOnly` alone.

use harness::*;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use protobuf::Message as _;

use std::time::Duration;

use crate::test_util::*;

// Deliberately far below the 500 ms production value: this doubles as the
// new-leader safeguard window, and several tests below must sleep past it.
const LEASE_DURATION_MICROS: u64 = 30_000;
/// Comfortably longer than `LEASE_DURATION_MICROS + SAFEGUARD_CLOCK_DRIFT_MICROS`.
const PAST_SAFEGUARD: Duration = Duration::from_millis(45);
const CATCHUP_MARGIN: u64 = 10;

/// A `Config` with follower read leases enabled. `GrantLeases` requires
/// `check_quorum`, and `ask_for_read_lease` requires `GrantLeases` — both are
/// enforced by `Config::validate`.
fn lease_config(id: u64) -> Config {
    Config {
        check_quorum: true,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: LEASE_DURATION_MICROS,
        max_num_read_leases: 5,
        ask_for_read_lease: true,
        read_lease_catchup_margin: CATCHUP_MARGIN,
        ..new_test_config(id, 10, 1)
    }
}

fn lease_raft(id: u64, peers: Vec<u64>, l: &slog::Logger) -> Interface {
    let storage = new_storage();
    storage.initialize_with_conf_state((peers, vec![]));
    new_test_raft_with_config(&lease_config(id), storage, l)
}

fn ask_read_lease(from: u64, to: u64, term: u64, index: u64, lease_id: u64) -> Message {
    let mut m = new_message(from, to, MessageType::MsgAskReadLease, 0);
    m.term = term;
    m.index = index;
    m.log_term = lease_id;
    m
}

/// A node that has won an election must not ask *itself* for a read lease.
///
/// After winning, `leader_id` is this node's own id, so a renewal timer still
/// armed from its time as a follower would fire a `MsgAskReadLease` addressed
/// to itself. TiKV's loopback delivers it, the new leader grants itself a lease
/// it never renews, and the commit clamp then pins commit at its own frozen
/// acked index for up to a full lease duration — once per follower->leader
/// transition. Review finding F11(a).
///
/// Driven through `RawNode::maybe_renew_read_lease`, which is exactly how TiKV's
/// poller-thread renewal timer reaches this code.
#[test]
fn test_leader_does_not_ask_itself_for_read_lease() {
    let l = default_logger();
    // Single voter, so campaigning alone is enough to win the election.
    let storage = new_storage();
    storage.initialize_with_conf_state((vec![1], vec![]));
    let mut node = RawNode::new(&lease_config(1), storage, &l).expect("build RawNode");

    // `now_micros()` counts from a lazily-captured process epoch, which the
    // line above may just have initialized. Let it pass the ask rate-limit
    // interval so the follower asks below are not throttled by an artifact of
    // being early in the process's life.
    std::thread::sleep(std::time::Duration::from_millis(15));

    // Become a follower of node 2. `handle_heartbeat` asks for a lease itself,
    // which establishes that every *other* gate in `maybe_ask_for_read_lease`
    // passes here: no lease held, and the rate-limiter permits. Without this
    // the leader-side assertion below could pass for the wrong reason.
    become_follower_of_2(&mut node, 2);
    assert_eq!(
        ask_read_lease_count(&node),
        1,
        "a follower with a known leader and no lease should ask for one"
    );
    node.raft.msgs.clear();

    // Now win an election. `reset()` rebuilds `ReadOnly`, so the ask
    // rate-limiter is clear again and only the role check can stop the ask.
    node.campaign().expect("campaign");
    assert_eq!(node.raft.state, StateRole::Leader);
    assert_eq!(node.raft.leader_id, 1, "leader_id is now our own id");
    node.raft.msgs.clear();

    // The renewal timer, armed while this node was a follower, now fires.
    assert!(
        !node.maybe_renew_read_lease(),
        "leader asked for a read lease instead of suppressing the stale renewal"
    );
    let self_asks: Vec<u64> = node
        .raft
        .msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAskReadLease)
        .map(|m| m.to)
        .collect();
    assert!(
        self_asks.is_empty(),
        "leader emitted MsgAskReadLease to {:?}",
        self_asks
    );

    // Step back down to follower. The ask resumes, proving the suppression
    // above was the role check and not a rate-limiter or lease-state artifact
    // — this node reaches the same `ReadOnly` state either way, since both
    // transitions run `reset()`.
    become_follower_of_2(&mut node, 4);
    assert_eq!(
        ask_read_lease_count(&node),
        1,
        "a node demoted back to follower should resume asking for a lease"
    );
}

/// Steps a `MsgHeartbeat` from node 2 at `term`, making `node` a follower of 2.
fn become_follower_of_2(node: &mut RawNode<MemStorage>, term: u64) {
    let mut heartbeat = new_message(2, 1, MessageType::MsgHeartbeat, 0);
    heartbeat.term = term;
    node.step(heartbeat).expect("step heartbeat");
    assert_eq!(node.raft.state, StateRole::Follower);
    assert_eq!(node.raft.leader_id, 2);
}

fn ask_read_lease_count(node: &RawNode<MemStorage>) -> usize {
    node.raft
        .msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAskReadLease)
        .count()
}

/// Drives a fresh 3-node lease-config raft to leader, waits out the new-leader
/// safeguard window, and lands its election no-op — i.e. the state in which
/// grants and reads are permitted again. Returns the leader.
fn leader_past_safeguard(l: &slog::Logger) -> Interface {
    let mut raft = elect_leader(l);
    // The no-op cannot commit until the safeguard window has elapsed.
    advance_past_safeguard(&mut raft);
    ack_append_from(&mut raft, 2);
    assert!(
        raft.commit_to_current_term(),
        "no-op should have committed once the safeguard window elapsed"
    );
    raft.read_messages();
    raft
}

/// Campaigns node 1 to leader in a 3-node lease-config group and persists its
/// election no-op. The no-op is appended but not yet committed.
fn elect_leader(l: &slog::Logger) -> Interface {
    let mut raft = lease_raft(1, vec![1, 2, 3], l);
    raft.step(new_message(1, 1, MessageType::MsgHup, 0))
        .expect("campaign");
    raft.step(new_message(2, 1, MessageType::MsgRequestVoteResponse, 0))
        .expect("step vote response");
    assert_eq!(raft.state, StateRole::Leader);
    raft.persist();
    raft.read_messages();
    raft
}

/// Runs the new-leader safeguard's timer to completion: `election_timeout`
/// ticks for phase 1, then past `lease_duration + drift` for phase 2.
///
/// Node 3 is deliberately never heard from in these tests, so the confirmation
/// shortcut stays blocked and it really is the timer being exercised.
fn advance_past_safeguard(raft: &mut Interface) {
    for _ in 0..raft.election_timeout() {
        raft.tick();
        // Keep node 2 recently-active, or CheckQuorum (mandatory under
        // GrantLeases) steps this leader down exactly when phase 1 completes.
        // Node 3 stays silent, so the confirmation shortcut remains blocked.
        let mut hb = new_message(2, 1, MessageType::MsgHeartbeatResponse, 0);
        hb.term = raft.term;
        raft.step(hb).expect("step heartbeat response");
    }
    std::thread::sleep(PAST_SAFEGUARD);
    raft.read_messages();
    assert_eq!(
        raft.state,
        StateRole::Leader,
        "leader stepped down while waiting out the safeguard"
    );
}

/// Steps a `MsgAppendResponse` from `from` acknowledging the leader's whole log,
/// giving the leader a quorum for everything it has appended.
fn ack_append_from(raft: &mut Interface, from: u64) {
    let last = raft.raft_log.last_index();
    let mut resp = new_message(from, 1, MessageType::MsgAppendResponse, 0);
    resp.index = last;
    // Stamped with the leader's term, so it also counts as a confirmation that
    // `from` has stepped to this term (and therefore dropped any older lease).
    resp.term = raft.term;
    raft.step(resp).expect("step append response");
}

/// The leader must not grant a read lease to a node outside the current
/// configuration.
///
/// `MsgAskReadLease` is not a response type, so `RawNode::step`'s unknown-peer
/// filter never applies to it. Without an explicit membership check a
/// removed-but-alive peer can keep renewing a lease indefinitely, and the
/// commit clamp holds the leader's commit index at that dead peer's acked
/// index forever. Review finding F11(b).
#[test]
fn test_leader_refuses_lease_to_non_member() {
    let l = default_logger();
    let mut raft = leader_past_safeguard(&l);

    let term = raft.term;
    let committed = raft.raft_log.committed;

    // A member in good standing is granted a lease...
    raft.step(ask_read_lease(2, 1, term, committed, 0xAA))
        .expect("step member ask");
    let granted = lease_resp(&mut raft, 2);
    assert!(
        !granted.reject,
        "a configuration member should have been granted a lease"
    );
    assert_eq!(
        raft.read_only.lease_map.num_active_leases(),
        1,
        "the member's lease should be recorded on the leader"
    );

    // ...but a node that is not in the configuration is not, and leaves no
    // lease behind to clamp commit with.
    raft.step(ask_read_lease(9, 1, term, committed, 0xBB))
        .expect("step non-member ask");
    let refused = lease_resp(&mut raft, 9);
    assert!(
        refused.reject,
        "a non-member must not be granted a read lease"
    );
    assert!(
        refused.entries.first().is_none_or(|e| e.data.is_empty()),
        "a rejected grant must not carry a marshalled lease"
    );
    assert!(
        raft.read_only.lease_map.get(9).is_none(),
        "the non-member's ask must not have created a lease"
    );
    assert!(
        raft.read_only.lease_map.get(2).is_some(),
        "the member's lease should be the one that survived"
    );
    assert_eq!(
        raft.read_only.lease_map.num_active_leases(),
        1,
        "exactly one lease should be outstanding"
    );
}

/// Drains messages and returns the single `MsgAskReadLeaseResp` addressed to `to`.
fn lease_resp(raft: &mut Interface, to: u64) -> Message {
    let mut resps: Vec<Message> = raft
        .read_messages()
        .into_iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAskReadLeaseResp && m.to == to)
        .collect();
    assert_eq!(
        resps.len(),
        1,
        "expected exactly one MsgAskReadLeaseResp to {to}"
    );
    resps.pop().unwrap()
}

/// A freshly-elected leader must commit nothing — not even its own election
/// no-op — until the safeguard window has elapsed.
///
/// It cannot know which leases a prior leader granted, so it has no clamp
/// protecting their holders. Committing the no-op is what commits the whole
/// prior-term tail with it, making state visible that a still-live lease holder
/// would then fail to serve. Review finding F7.
#[test]
fn test_new_leader_withholds_commit_during_safeguard() {
    let l = default_logger();
    let mut raft = elect_leader(&l);
    let committed_at_election = raft.raft_log.committed;

    // A quorum acknowledges the no-op. Without the safeguard this commits it.
    // Only node 2 acks: node 3 stays unheard-from so the confirmation shortcut
    // cannot lift the window, leaving the timer as the only way through.
    ack_append_from(&mut raft, 2);
    assert_eq!(
        raft.raft_log.committed, committed_at_election,
        "commit advanced inside the safeguard window"
    );
    assert!(
        !raft.commit_to_current_term(),
        "no-op must not be committed inside the safeguard window"
    );

    // Once the window elapses, the same quorum state is allowed to commit.
    advance_past_safeguard(&mut raft);
    assert!(raft.maybe_commit(), "commit should resume after the window");
    assert!(raft.commit_to_current_term());
}

/// A leader whose no-op has not committed must refuse to grant leases, even to
/// a member in good standing.
///
/// Its `raft_log.committed` is only a lower bound on the true commit index, so
/// a lease granted against it can pin a holder below writes a previous leader
/// already committed and acked — and the holder's own ack-index promise was
/// discharged by the term change.
#[test]
fn test_leader_refuses_lease_before_committing_in_term() {
    let l = default_logger();
    let mut raft = elect_leader(&l);
    assert!(!raft.commit_to_current_term());

    let (term, committed) = (raft.term, raft.raft_log.committed);
    raft.step(ask_read_lease(2, 1, term, committed, 0xAA))
        .expect("step member ask");
    let resp = lease_resp(&mut raft, 2);
    assert!(
        resp.reject,
        "lease granted before the leader committed in its own term"
    );
    assert_eq!(
        raft.read_only.lease_map.num_active_leases(),
        0,
        "no lease should have been recorded"
    );

    // The same ask succeeds once the leader has committed in its term, which
    // confirms the refusal above was the commit gate and nothing else.
    advance_past_safeguard(&mut raft);
    ack_append_from(&mut raft, 2);
    assert!(raft.commit_to_current_term());
    raft.read_messages();
    let committed = raft.raft_log.committed;
    raft.step(ask_read_lease(2, 1, term, committed, 0xBB))
        .expect("step member ask");
    assert!(!lease_resp(&mut raft, 2).reject);
}

/// `tick()` must retry a commit the safeguard suppressed.
///
/// Commit normally only advances in response to an append response, and raft
/// never otherwise declines a commit the quorum has earned — so there is always
/// a pending event to drive it. The safeguard breaks that invariant: once the
/// acknowledging append responses have been consumed inside the window, nothing
/// re-drives them, and in a group with no write traffic the no-op would never
/// commit at all, leaving the leader permanently unable to serve reads or grant
/// leases.
#[test]
fn test_tick_commits_no_op_after_safeguard_window() {
    let l = default_logger();
    let mut raft = elect_leader(&l);

    // Consume the quorum's acknowledgement inside the window, leaving no
    // outstanding event that could later drive the commit.
    ack_append_from(&mut raft, 2);
    assert!(!raft.commit_to_current_term());
    raft.read_messages();

    advance_past_safeguard(&mut raft);

    // No further messages arrive — only the clock.
    raft.tick();
    assert!(
        raft.commit_to_current_term(),
        "tick did not retry the commit the safeguard suppressed; \
         an idle group would stay wedged with reads and grants disabled"
    );
}

/// A read arriving at a leader inside the safeguard window must be postponed
/// and answered once the window closes — not dropped.
///
/// raft-rs previously dropped such reads. That was survivable when the gap was
/// the few milliseconds between election and no-op commit, but the safeguard
/// makes it a full lease window, and TiKV has no leader-side recovery for it:
/// `retry_pending_reads` returns early for leaders, so a dropped read sits in
/// `pending_reads` until the client's own timeout. Matches the Go fork's
/// `pendingReadIndexMessages`.
#[test]
fn test_read_during_safeguard_is_postponed_then_answered() {
    let l = default_logger();
    let mut raft = elect_leader(&l);
    assert!(!raft.commit_to_current_term());

    // A local read arrives while the safeguard is still holding commit.
    let ctx = "during-window";
    raft.step(new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(ctx))],
    ))
    .expect("step read index");
    assert!(
        raft.read_states.is_empty(),
        "read must not be answered against an unvalidated commit index"
    );

    // It is held, not discarded: closing the window answers it.
    advance_past_safeguard(&mut raft);
    ack_append_from(&mut raft, 2);
    assert!(raft.commit_to_current_term());

    assert_eq!(
        raft.read_states.len(),
        1,
        "postponed read was dropped instead of being answered"
    );
    assert_eq!(raft.read_states[0].request_ctx, ctx.as_bytes().to_vec());
    assert_eq!(raft.read_states[0].index, raft.raft_log.committed);
}

/// The safeguard lifts immediately once every node that could hold a
/// prior-term lease has been heard from at the new term — no ticks, no waiting.
///
/// A message at the leader's current term proves the sender already stepped to
/// that term, and stepping a term runs `reset()`, which rebuilds `ReadOnly` and
/// destroys the sender's lease. When that holds for the whole configuration
/// there is nothing left for the timer to wait out.
#[test]
fn test_safeguard_lifts_early_once_all_members_confirm() {
    let l = default_logger();
    let mut raft = elect_leader(&l);
    assert!(
        !raft.commit_to_current_term(),
        "no-op must not commit before anything is confirmed"
    );

    // Node 2 confirms; node 3 has still not been heard from at this term, so it
    // could be sitting on a lease from the previous leader.
    ack_append_from(&mut raft, 2);
    assert!(
        !raft.commit_to_current_term(),
        "safeguard lifted while node 3 was still unaccounted for"
    );

    // Node 3 confirms. Nothing can be holding a prior-term lease now.
    ack_append_from(&mut raft, 3);
    assert!(
        raft.commit_to_current_term(),
        "safeguard did not lift once every member had confirmed"
    );
}

/// A node must not leave the configuration while holding a read lease: once
/// removed it is unreachable to the leader but keeps serving reads locally
/// until its own clock expires the lease.
///
/// The leader refuses the removal and simultaneously bars the target from
/// renewing, so the lease drains and the caller's retry succeeds. Refusing
/// without banning would never converge — the next renewal would be granted as
/// normal and the lease would never lapse.
#[test]
fn test_removal_of_lease_holder_is_refused_and_drains() {
    let l = default_logger();
    let mut raft = leader_past_safeguard(&l);
    let (term, committed) = (raft.term, raft.raft_log.committed);

    // Node 2 takes a lease.
    raft.step(ask_read_lease(2, 1, term, committed, 0xAA))
        .expect("step lease ask");
    assert!(!lease_resp(&mut raft, 2).reject);
    assert!(raft.read_only.lease_map.get(2).is_some());

    // Removing it is refused while that lease is live.
    let mut propose = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    e.data = conf_change(ConfChangeType::RemoveNode, 2)
        .write_to_bytes()
        .unwrap()
        .into();
    propose.mut_entries().push(e);
    assert_eq!(
        raft.step(propose).unwrap_err(),
        Error::ProposalDropped,
        "removal of a live lease holder should be refused"
    );

    // The drain: node 2 is now barred, so its renewal is rejected and the lease
    // will lapse rather than being extended indefinitely.
    raft.read_messages();
    raft.step(ask_read_lease(2, 1, term, committed, 0xBB))
        .expect("step renewal");
    assert!(
        lease_resp(&mut raft, 2).reject,
        "a node being removed must not be able to renew its lease"
    );

    // The ban is targeted: an unrelated member is unaffected.
    raft.step(ask_read_lease(3, 1, term, committed, 0xCC))
        .expect("step lease ask");
    assert!(
        !lease_resp(&mut raft, 3).reject,
        "the ban should apply only to the removal target"
    );
}
