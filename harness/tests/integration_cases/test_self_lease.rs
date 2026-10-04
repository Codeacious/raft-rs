// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Self-leases and fresh-evidence lease grants.
//!
//! A leader under either leased option serves reads from a self-lease, and
//! grants follower leases under `GrantLeases`. Every lease is dated from its
//! proposal and takes effect only once a majority echoes a lease round minted
//! *after* that proposal (`MsgHeartbeat.index`, echoed in
//! `MsgHeartbeatResponse.index`). Rust twins of the Go fork's
//! `self_lease_test.go` where one exists.

use harness::*;
use protobuf::Message as PbMessage;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;

use crate::test_util::*;

const ET: usize = 10;
/// Comfortably above the 200 ms renewal threshold, so a fresh lease is not
/// immediately up for renewal and does not send rounds of its own.
const LONG_LEASE: u64 = 2_000_000;

fn config(id: u64, option: ReadOnlyOption, lease_micros: u64) -> Config {
    Config {
        check_quorum: true,
        read_only_option: option,
        read_lease_duration_micros: lease_micros,
        max_num_read_leases: 5,
        read_lease_catchup_margin: 10,
        ..new_test_config(id, ET, 1)
    }
}

fn raft_node(id: u64, option: ReadOnlyOption, lease_micros: u64, l: &slog::Logger) -> Interface {
    let storage = new_storage();
    storage.initialize_with_conf_state((vec![1, 2, 3], vec![]));
    new_test_raft_with_config(&config(id, option, lease_micros), storage, l)
}

/// Node 1 as leader of {1, 2, 3}, past its safeguard (both peers confirm at
/// the new term) and with its no-op committed. Its outbox is drained.
fn leader(option: ReadOnlyOption, lease_micros: u64, l: &slog::Logger) -> Interface {
    let mut raft = raft_node(1, option, lease_micros, l);
    raft.step(new_message(1, 1, MessageType::MsgHup, 0))
        .expect("campaign");
    let mut vote = new_message(2, 1, MessageType::MsgRequestVoteResponse, 0);
    vote.term = raft.term;
    raft.step(vote).expect("vote");
    assert_eq!(raft.state, StateRole::Leader);
    raft.persist();
    raft.read_messages();
    for from in [2, 3] {
        ack_append(&mut raft, from);
    }
    assert!(raft.commit_to_current_term(), "no-op should have committed");
    raft.read_messages();
    raft
}

fn ack_append(raft: &mut Raft<MemStorage>, from: u64) {
    let mut resp = new_message(from, 1, MessageType::MsgAppendResponse, 0);
    resp.index = raft.raft_log.last_index();
    resp.term = raft.term;
    raft.step(resp).expect("append response");
}

/// Steps `from`'s echo of lease `round`, carrying `ctx` if it answers a
/// read-index round too.
fn echo(raft: &mut Raft<MemStorage>, from: u64, round: u64, ctx: &[u8]) {
    let mut resp = new_message(from, 1, MessageType::MsgHeartbeatResponse, 0);
    resp.term = raft.term;
    resp.index = round;
    resp.context = ctx.to_vec().into();
    raft.step(resp).expect("heartbeat response");
}

/// Takes everything in `raft`'s outbox.
fn drain(raft: &mut Raft<MemStorage>) -> Vec<Message> {
    std::mem::take(&mut raft.msgs)
}

/// The lease round and context of the (single) broadcast in `msgs`, if any.
/// Callers drain the outbox once and pass it here: draining twice would lose
/// the heartbeat before it could be echoed.
fn broadcast(msgs: &[Message]) -> Option<(u64, Vec<u8>)> {
    let hbs: Vec<&Message> = msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgHeartbeat)
        .collect();
    let first = hbs.first()?;
    assert!(
        hbs.iter().all(|m| m.index == first.index),
        "expected one broadcast, saw rounds {:?}",
        hbs.iter().map(|m| m.index).collect::<Vec<_>>()
    );
    Some((first.index, first.context.to_vec()))
}

fn ask(from: u64, term: u64, index: u64, lease_id: u64) -> Message {
    let mut m = new_message(from, 1, MessageType::MsgAskReadLease, 0);
    m.term = term;
    m.index = index;
    m.log_term = lease_id;
    m
}

fn lease_resps(msgs: &[Message], to: u64) -> Vec<&Message> {
    msgs.iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAskReadLeaseResp && m.to == to)
        .collect()
}

fn local_read(ctx: &str) -> Message {
    new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(ctx))],
    )
}

/// Gives `raft` (a leader from `leader()`) a live self-lease through the
/// fallback path: a read with no lease takes a quorum round, which is also a
/// lease round covering the renewal the read proposed.
fn prime_self_lease(raft: &mut Raft<MemStorage>) {
    raft.step(local_read("prime")).expect("read");
    let msgs = drain(raft);
    let (round, ctx) = broadcast(&msgs).expect("a read with no lease must take a round");
    echo(raft, 2, round, &ctx);
    drain(raft);
    assert_eq!(raft.read_states.len(), 1, "priming read should be answered");
    raft.read_states.clear();
    assert!(
        raft.read_only.has_active_held_lease(),
        "self-lease should be live"
    );
}

/// The ordering rule, and the test that catches a break in it: a round minted
/// *before* a proposal must not cover it. The other tests always have the
/// proposal precede the round, so they pass with the check deleted.
#[test]
fn test_lease_round_does_not_cover_later_proposals() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::GrantLeases, LONG_LEASE, &l);
    let (term, committed) = (raft.term, raft.raft_log.committed);

    // Node 2 asks: parked, not answered, and a round goes out for it.
    raft.step(ask(2, term, committed, 0xA2)).expect("ask 2");
    let msgs = raft.read_messages();
    assert!(
        lease_resps(&msgs, 2).is_empty(),
        "the ask must be parked, not answered"
    );
    let (r1, _) = broadcast(&msgs).expect("a round for node 2's ask");

    // Node 3 asks while that round is in flight: parked, and no second
    // broadcast (one round in flight at a time).
    raft.step(ask(3, term, committed, 0xA3)).expect("ask 3");
    let msgs = raft.read_messages();
    assert!(
        broadcast(&msgs).is_none(),
        "one lease round in flight at a time"
    );

    // Round r1 completes. It was minted before node 3 asked, so it covers
    // node 2 only.
    echo(&mut raft, 2, r1, &[]);
    let msgs = raft.read_messages();
    let to2 = lease_resps(&msgs, 2);
    assert_eq!(to2.len(), 1);
    assert!(
        !to2[0].reject,
        "node 2's ask predates round {} and must be granted",
        r1
    );
    assert!(
        lease_resps(&msgs, 3).is_empty(),
        "round {} was minted before node 3 asked, so it must not cover it",
        r1
    );
    let (r2, _) = broadcast(&msgs).expect("the next round, for node 3");
    assert!(r2 > r1);

    // The next round was minted after node 3 asked.
    echo(&mut raft, 2, r2, &[]);
    let msgs = raft.read_messages();
    let to3 = lease_resps(&msgs, 3);
    assert_eq!(to3.len(), 1);
    assert!(!to3[0].reject);
    assert_eq!(
        to3[0].log_term, 0xA3,
        "the grant carries the asker's lease id"
    );
}

/// A self-lease installs only on an echo of a round minted after its
/// proposal; a leader with no live lease answers reads by a quorum round, not
/// from its commit index. A lease-served read is marked as such.
#[test]
fn test_self_lease_requires_fresh_quorum() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::LeaseBased, LONG_LEASE, &l);

    // A scheduled beat mints round r0 before anything is proposed.
    raft.step(new_message(1, 1, MessageType::MsgBeat, 0))
        .expect("beat");
    let (r0, _) = broadcast(&raft.read_messages()).expect("beat round");

    // A read with no lease: not answered locally; falls back to a quorum
    // round, which also proposes a self-lease renewal.
    raft.step(local_read("r")).expect("read");
    let msgs = raft.read_messages();
    assert!(
        raft.read_states.is_empty(),
        "no lease: must not answer from committed"
    );
    let (r1, ctx) = broadcast(&msgs).expect("fallback quorum round");
    assert!(!ctx.is_empty() && r1 > r0);

    // An echo of the round minted before the proposal installs nothing.
    echo(&mut raft, 2, r0, &[]);
    assert!(
        !raft.read_only.has_active_held_lease(),
        "a round minted before the proposal must not install the self-lease"
    );
    assert!(raft.read_states.is_empty());

    // The fallback round completes: the read is answered by the quorum, and
    // the self-lease installs.
    echo(&mut raft, 2, r1, &ctx);
    assert_eq!(raft.read_states.len(), 1);
    assert!(!raft.read_states[0].served_by_leader_lease);
    assert!(raft.read_only.has_active_held_lease());
    raft.read_states.clear();
    raft.read_messages();

    // Now reads are served from the lease, with no round at all.
    raft.step(local_read("s")).expect("read");
    let msgs = raft.read_messages();
    assert!(
        broadcast(&msgs).is_none(),
        "a live, fresh lease needs no round"
    );
    assert_eq!(raft.read_states.len(), 1);
    assert!(raft.read_states[0].served_by_leader_lease);
    assert!(!raft.read_states[0].served_by_follower_lease);
}

/// `RawNode::read_index_quorum` ignores a live self-lease and takes a quorum
/// round, for a caller that renews a lease of its own from the answer.
#[test]
fn test_read_index_quorum_bypasses_self_lease() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::LeaseBased, LONG_LEASE, &l);
    prime_self_lease(&mut raft);

    raft.read_index_quorum(b"q".to_vec());
    let msgs = raft.read_messages();
    assert!(
        raft.read_states.is_empty(),
        "must not be served from the lease"
    );
    let (round, ctx) = broadcast(&msgs).expect("a quorum round");
    assert_eq!(ctx, b"q".to_vec());
    echo(&mut raft, 2, round, &ctx);
    assert_eq!(raft.read_states.len(), 1);
    assert!(!raft.read_states[0].served_by_leader_lease);
}

/// A genuine "no" is answered at once, echoing the lease id, and sends no
/// round; only a request that merely lacks evidence is parked.
#[test]
fn test_follower_lease_refused_on_static_requirement() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::GrantLeases, LONG_LEASE, &l);
    for _ in 0..20 {
        raft.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("propose");
    }
    for from in [2, 3] {
        ack_append(&mut raft, from);
    }
    raft.read_messages();
    // Node 2 claims a commit index far outside the catch-up margin.
    let term = raft.term;
    raft.step(ask(2, term, 1, 0xB2)).expect("ask");
    let msgs = raft.read_messages();
    let to2 = lease_resps(&msgs, 2);
    assert_eq!(to2.len(), 1, "a static refusal is answered, not parked");
    assert!(to2[0].reject);
    assert_eq!(to2[0].log_term, 0xB2, "the refusal echoes the lease id");
    assert!(broadcast(&msgs).is_none());
}

/// A re-ask while a request is parked keeps the first request and its id.
#[test]
fn test_lease_re_ask_keeps_first_request() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::GrantLeases, LONG_LEASE, &l);
    let (term, committed) = (raft.term, raft.raft_log.committed);
    raft.step(ask(2, term, committed, 0xC1)).expect("ask");
    let (r1, _) = broadcast(&raft.read_messages()).expect("round");
    raft.step(ask(2, term, committed, 0xC2)).expect("re-ask");
    raft.read_messages();
    echo(&mut raft, 2, r1, &[]);
    let msgs = raft.read_messages();
    let to2 = lease_resps(&msgs, 2);
    assert_eq!(to2.len(), 1);
    assert!(!to2[0].reject);
    assert_eq!(
        to2[0].log_term, 0xC1,
        "the first ask's id, dated from the first ask"
    );
}

/// A request parked for a whole lease duration is refused when covered: a
/// lease dated from it would already be over.
#[test]
fn test_stale_parked_grant_refused() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::GrantLeases, 20_000, &l);
    let (term, committed) = (raft.term, raft.raft_log.committed);
    raft.step(ask(2, term, committed, 0xD1)).expect("ask");
    let (r1, _) = broadcast(&raft.read_messages()).expect("round");
    std::thread::sleep(std::time::Duration::from_millis(30));
    echo(&mut raft, 2, r1, &[]);
    let msgs = raft.read_messages();
    let to2 = lease_resps(&msgs, 2);
    assert_eq!(to2.len(), 1);
    assert!(to2[0].reject, "a stale parked request must be refused");
    assert_eq!(to2[0].log_term, 0xD1);
    assert_eq!(raft.read_only.leader.granted_leases.num_active_leases(), 0);
}

/// Proposing a node's removal drops its parked request; the covering round
/// then grants it nothing.
#[test]
fn test_ban_drops_parked_grant() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::GrantLeases, LONG_LEASE, &l);
    let (term, committed) = (raft.term, raft.raft_log.committed);
    raft.step(ask(2, term, committed, 0xE1)).expect("ask");
    let (r1, _) = broadcast(&raft.read_messages()).expect("round");
    raft.step(remove_node(2)).expect("removal proposal");
    raft.read_messages();
    echo(&mut raft, 3, r1, &[]);
    let msgs = raft.read_messages();
    assert!(
        lease_resps(&msgs, 2).is_empty(),
        "a banned node's parked request is dropped"
    );
    assert!(raft.read_only.leader.granted_leases.get(2).is_none());
}

fn remove_node(id: u64) -> Message {
    let mut cc = ConfChange::default();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.node_id = id;
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    e.data = cc.write_to_bytes().unwrap().into();
    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    m.mut_entries().push(e);
    m
}

/// A late echo of a round that still covers a proposal counts; an echo of a
/// round never minted is ignored.
#[test]
fn test_late_and_bogus_lease_echoes() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::GrantLeases, LONG_LEASE, &l);
    let (term, committed) = (raft.term, raft.raft_log.committed);
    raft.step(ask(2, term, committed, 0xF1)).expect("ask");
    let (r1, _) = broadcast(&raft.read_messages()).expect("round");
    // An echo of a round from the future is ignored.
    echo(&mut raft, 3, r1 + 100, &[]);
    assert!(lease_resps(&raft.read_messages(), 2).is_empty());
    // A scheduled beat supersedes r1; r1's echo arrives late and still counts.
    raft.step(new_message(1, 1, MessageType::MsgBeat, 0))
        .expect("beat");
    raft.read_messages();
    echo(&mut raft, 3, r1, &[]);
    let msgs = raft.read_messages();
    assert_eq!(lease_resps(&msgs, 2).len(), 1);
    assert!(!lease_resps(&msgs, 2)[0].reject);
}

/// Followers echo the leader's round; a `Safe` leader's heartbeats carry none,
/// so `Safe` is unchanged on the wire.
#[test]
fn test_lease_round_echo_and_safe_unchanged() {
    let l = default_logger();
    let mut f = raft_node(2, ReadOnlyOption::LeaseBased, LONG_LEASE, &l);
    let mut hb = new_message(1, 2, MessageType::MsgHeartbeat, 0);
    hb.term = 1;
    hb.index = 7;
    f.step(hb).expect("heartbeat");
    let resp = f
        .read_messages()
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgHeartbeatResponse)
        .expect("heartbeat response");
    assert_eq!(resp.index, 7);

    let mut safe = raft_node(1, ReadOnlyOption::Safe, LONG_LEASE, &l);
    safe.step(new_message(1, 1, MessageType::MsgHup, 0))
        .unwrap();
    let mut vote = new_message(2, 1, MessageType::MsgRequestVoteResponse, 0);
    vote.term = safe.term;
    safe.step(vote).unwrap();
    safe.read_messages();
    safe.step(new_message(1, 1, MessageType::MsgBeat, 0))
        .unwrap();
    let hbs = safe.read_messages();
    assert!(!hbs.is_empty());
    assert!(
        hbs.iter().all(|m| m.index == 0),
        "Safe heartbeats must carry no round"
    );
}

/// A follower refused a lease forgets the lease id it asked with, so a stray
/// grant for that id later cannot install.
#[test]
fn test_rejected_ask_clears_pending_lease() {
    let l = default_logger();
    let mut f = raft_node(2, ReadOnlyOption::GrantLeases, LONG_LEASE, &l);
    std::thread::sleep(std::time::Duration::from_millis(15)); // clear the ask rate-limit
    let mut hb = new_message(1, 2, MessageType::MsgHeartbeat, 0);
    hb.term = 1;
    f.step(hb).expect("heartbeat");
    let asked = f
        .read_messages()
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAskReadLease);
    let lease_id = match asked {
        Some(m) => m.log_term,
        None => f.read_only.get_marked_lease_id(2),
    };
    let mut reject = new_message(1, 2, MessageType::MsgAskReadLeaseResp, 0);
    reject.term = 1;
    reject.reject = true;
    reject.log_term = lease_id;
    reject.set_entries(vec![Entry::default()].into());
    f.step(reject).expect("reject");

    let mut grant = new_message(1, 2, MessageType::MsgAskReadLeaseResp, 0);
    grant.term = 1;
    let lease = [lease_id, 2, 0, 0, LONG_LEASE, 0]
        .iter()
        .flat_map(|v: &u64| v.to_be_bytes())
        .collect::<Vec<u8>>();
    let mut e = Entry::default();
    e.data = lease.into();
    grant.set_entries(vec![e].into());
    f.step(grant).expect("stray grant");
    assert!(
        !f.read_only.has_active_held_lease(),
        "a grant for a refused lease id must not install"
    );
}

/// A new leader keeps the confirmations its vote responses carried: with
/// both peers having voted, the safeguard shortcut lifts at once, before any
/// append response arrives.
#[test]
fn test_new_leader_keeps_candidate_confirmations() {
    let l = default_logger();
    let mut raft = raft_node(1, ReadOnlyOption::GrantLeases, 60_000_000, &l);
    raft.step(new_message(1, 1, MessageType::MsgHup, 0))
        .unwrap();
    // Node 3's vote arrives first but does not win; node 2's does.
    for from in [3, 2] {
        let mut vote = new_message(from, 1, MessageType::MsgRequestVoteResponse, 0);
        vote.term = raft.term;
        let _ = raft.step(vote);
    }
    assert_eq!(raft.state, StateRole::Leader);
    raft.read_messages();
    // No append response has arrived: only the carried votes can lift it.
    assert!(
        raft.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .is_ok(),
        "vote-response confirmations were lost across become_leader"
    );
}

/// A leader proposing its own removal gives up its self-lease at once: a
/// successor that has applied the removal will not wait for it.
#[test]
fn test_self_removal_bans_self_lease() {
    let l = default_logger();
    let mut raft = leader(ReadOnlyOption::LeaseBased, LONG_LEASE, &l);
    prime_self_lease(&mut raft);
    raft.step(remove_node(1))
        .expect("the proposal is still accepted");
    assert!(
        !raft.read_only.has_active_held_lease(),
        "the held self-lease must be dropped"
    );
    raft.read_messages();

    // Reads now take quorum rounds, and those rounds install no new lease.
    raft.step(local_read("after")).expect("read");
    let (round, ctx) = broadcast(&raft.read_messages()).expect("a quorum round");
    echo(&mut raft, 2, round, &ctx);
    assert_eq!(raft.read_states.len(), 1);
    assert!(!raft.read_states[0].served_by_leader_lease);
    assert!(
        !raft.read_only.has_active_held_lease(),
        "no self-lease while banned"
    );
}

/// The same when the self-removal was proposed by an earlier leader and sits
/// unapplied in the log this node inherits on becoming leader.
#[test]
fn test_inherited_self_removal_bans_self_lease() {
    let l = default_logger();
    let storage = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
    let mut cc = ConfChange::default();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.node_id = 1;
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    e.data = cc.write_to_bytes().unwrap().into();
    e.term = 1;
    e.index = 1;
    storage.wl().append(&[e]).unwrap();
    let mut hs = HardState::default();
    hs.term = 1;
    storage.wl().set_hardstate(hs);
    let mut raft = new_test_raft_with_config(
        &config(1, ReadOnlyOption::LeaseBased, LONG_LEASE),
        storage,
        &l,
    );
    raft.step(new_message(1, 1, MessageType::MsgHup, 0))
        .unwrap();
    let mut vote = new_message(2, 1, MessageType::MsgRequestVoteResponse, 0);
    vote.term = raft.term;
    raft.step(vote).unwrap();
    assert_eq!(raft.state, StateRole::Leader);
    raft.persist();
    raft.read_messages();
    for from in [2, 3] {
        ack_append(&mut raft, from);
    }
    assert!(raft.commit_to_current_term());
    raft.read_messages();

    raft.step(local_read("r")).expect("read");
    let (round, ctx) = broadcast(&raft.read_messages()).expect("a quorum round");
    echo(&mut raft, 2, round, &ctx);
    assert_eq!(raft.read_states.len(), 1);
    assert!(
        !raft.read_only.has_active_held_lease(),
        "an inherited self-removal bans the self-lease"
    );
}

/// The leader's renewal timer: nothing to schedule for an unused lease; due at
/// the renewal threshold once the lease has served a read; firing it proposes
/// a renewal and sends a round.
#[test]
fn test_leader_self_lease_renewal_timer() {
    let l = default_logger();
    let storage = new_storage();
    storage.initialize_with_conf_state((vec![1, 2, 3], vec![]));
    // Shorter than the 200 ms renewal threshold: always due once used.
    let mut node = RawNode::new(&config(1, ReadOnlyOption::LeaseBased, 150_000), storage, &l)
        .expect("RawNode");
    node.campaign().unwrap();
    let mut vote = new_message(2, 1, MessageType::MsgRequestVoteResponse, 0);
    vote.term = node.raft.term;
    node.step(vote).unwrap();
    // Persist the no-op, as the harness `Interface::persist` does.
    let unstable = node.raft.raft_log.unstable_entries().to_vec();
    let last = unstable.last().unwrap().clone();
    node.raft.raft_log.stable_entries(last.index, last.term);
    node.raft.mut_store().wl().append(&unstable).unwrap();
    node.raft.on_persist_entries(last.index, last.term);
    node.raft.msgs.clear();
    for from in [2, 3] {
        ack_append(&mut node.raft, from);
    }
    node.raft.msgs.clear();
    prime_self_lease(&mut node.raft);
    assert_eq!(
        node.micros_until_lease_renewal_timeout_micros(),
        None,
        "an unused self-lease is left to lapse"
    );

    node.raft.step(local_read("used")).unwrap();
    assert!(node.raft.read_states[0].served_by_leader_lease);
    node.raft.read_states.clear();
    // Serving the read proposed a renewal and sent its round; the timer waits
    // for that renewal to go stale.
    let msgs = drain(&mut node.raft);
    let (round, _) = broadcast(&msgs).expect("renewal round");
    assert!(node.micros_until_lease_renewal_timeout_micros().unwrap() > 0);
    echo(&mut node.raft, 2, round, &[]);
    drain(&mut node.raft);
    assert_eq!(
        node.micros_until_lease_renewal_timeout_micros(),
        None,
        "the renewed lease has served nothing yet"
    );
    node.raft.step(local_read("used again")).unwrap();
    drain(&mut node.raft);
    std::thread::sleep(std::time::Duration::from_millis(160));
    // The renewal proposed by that read is now stale; the timer re-proposes.
    assert_eq!(node.micros_until_lease_renewal_timeout_micros(), Some(0));
    assert!(node.maybe_renew_read_lease(), "a due renewal sends a round");
}

/// Both leased options need a lease that outlives the drift allowance.
#[test]
fn test_leased_options_require_a_lease_duration() {
    for option in [ReadOnlyOption::LeaseBased, ReadOnlyOption::GrantLeases] {
        let mut cfg = config(1, option, 0);
        cfg.ask_for_read_lease = false;
        assert!(
            cfg.validate().is_err(),
            "{:?} accepted a zero lease duration",
            option
        );
        cfg.read_lease_duration_micros = 1_000;
        assert!(
            cfg.validate().is_err(),
            "{:?} accepted a lease no longer than the drift",
            option
        );
        cfg.read_lease_duration_micros = 1_001;
        assert!(cfg.validate().is_ok());
    }
    assert!(config(1, ReadOnlyOption::Safe, 0).validate().is_ok());
}
