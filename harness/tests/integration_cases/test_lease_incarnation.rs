// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! SAFE-2: a holder's promise (`read_lease_acked_index`) lives in its
//! `ReadOnly` instance and is lost on every `reset()`, same-term ones included,
//! and on a restart. These tests check that the leader credits a grant only
//! with acks from the incarnation that asked for it.

use harness::*;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;

use std::time::Duration;

use crate::test_util::*;

/// Long enough that every scenario finishes inside one lease.
const LEASE_DURATION_MICROS: u64 = 60_000_000;

/// Only node 2 asks for a lease, so it is the only holder.
fn incarnation_config(id: u64, pre_vote: bool) -> Config {
    Config {
        check_quorum: true,
        pre_vote,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: LEASE_DURATION_MICROS,
        max_num_read_leases: 5,
        ask_for_read_lease: id == 2,
        read_lease_catchup_margin: 10,
        ..new_test_config(id, 10, 1)
    }
}

/// Delivers `msgs` breadth-first until quiet, dropping every message `drop`
/// selects, and persists after every step like `Network::send`.
fn pump(nt: &mut Network, msgs: Vec<Message>, drop: &dyn Fn(&Message) -> bool) {
    let mut q = msgs;
    let mut guard = 0;
    while !q.is_empty() {
        guard += 1;
        assert!(guard < 1_000, "messages never quiesced");
        let mut next = vec![];
        for m in q.drain(..) {
            if drop(&m) {
                continue;
            }
            let p = nt.peers.get_mut(&m.to).unwrap();
            let _ = p.step(m);
            p.persist();
            next.extend(p.read_messages());
        }
        q = next;
    }
}

fn no_drop(_: &Message) -> bool {
    false
}

/// Appends and ack-index asks to node 2 are lost; everything else arrives.
fn lossy_to_2(m: &Message) -> bool {
    m.to == 2
        && matches!(
            m.get_msg_type(),
            MessageType::MsgAppend | MessageType::MsgAskAckIndex
        )
}

/// Steps a local message at `id` and delivers everything it causes.
fn step_and_pump(nt: &mut Network, id: u64, m: Message, drop: &dyn Fn(&Message) -> bool) {
    let msgs = {
        let p = nt.peers.get_mut(&id).unwrap();
        let _ = p.step(m);
        p.persist();
        p.read_messages()
    };
    pump(nt, msgs, drop);
}

fn propose(nt: &mut Network, drop: &dyn Fn(&Message) -> bool) {
    step_and_pump(nt, 1, new_message(1, 1, MessageType::MsgPropose, 1), drop);
}

fn beat(nt: &mut Network, drop: &dyn Fn(&Message) -> bool) {
    step_and_pump(nt, 1, new_message(1, 1, MessageType::MsgBeat, 0), drop);
}

fn local_read(nt: &mut Network, id: u64, ctx: &str) -> Option<ReadState> {
    let p = nt.peers.get_mut(&id).unwrap();
    p.read_states.clear();
    let _ = p.step(new_message_with_entries(
        id,
        id,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(ctx))],
    ));
    let _ = p.read_messages();
    p.read_states.pop()
}

fn granted_acked(nt: &Network, id: u64) -> u64 {
    nt.peers[&1]
        .read_only
        .granted_lease(id)
        .unwrap()
        .acked_index
}

/// A 3-node group led by 1 in which 2 holds a lease.
fn incarnation_group(pre_vote: bool) -> Network {
    let l = default_logger();
    let peers: Vec<Option<Interface>> = (1..=3)
        .map(|id| {
            let storage = new_storage();
            storage.initialize_with_conf_state((vec![1, 2, 3], vec![]));
            Some(new_test_raft_with_config(
                &incarnation_config(id, pre_vote),
                storage,
                &l,
            ))
        })
        .collect();
    let mut nt = Network::new_with_config(peers, &incarnation_config(1, pre_vote), &l);
    // `now_micros()` counts from a lazily captured epoch; let the ask rate
    // limiter open.
    std::thread::sleep(Duration::from_millis(15));
    step_and_pump(
        &mut nt,
        1,
        new_message(1, 1, MessageType::MsgHup, 0),
        &no_drop,
    );
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert!(nt.peers[&1].commit_to_current_term());
    beat(&mut nt, &no_drop);
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());
    nt
}

/// Proposes an entry whose ack from 2 is held back and returned, with
/// ack-index asks to 2 lost, so the clamp holds the entry uncommitted.
fn propose_holding_ack_from_2(nt: &mut Network) -> (u64, Message) {
    let before = nt.peers[&1].raft_log.committed;
    let msgs = {
        let n1 = nt.peers.get_mut(&1).unwrap();
        n1.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .unwrap();
        n1.persist();
        n1.read_messages()
    };
    let idx = nt.peers[&1].raft_log.last_index();
    let mut held = vec![];
    let mut q = msgs;
    while !q.is_empty() {
        let mut next = vec![];
        for m in q.drain(..) {
            if m.to == 2 && m.get_msg_type() == MessageType::MsgAskAckIndex {
                continue;
            }
            if m.from == 2 && m.get_msg_type() == MessageType::MsgAppendResponse && m.index == idx {
                held.push(m);
                continue;
            }
            let p = nt.peers.get_mut(&m.to).unwrap();
            let _ = p.step(m);
            p.persist();
            next.extend(p.read_messages());
        }
        q = next;
    }
    assert_eq!(held.len(), 1);
    assert_eq!(
        nt.peers[&1].raft_log.committed, before,
        "the clamp holds the entry back"
    );
    (idx, held.pop().unwrap())
}

/// Delivers an ack from 2's previous incarnation after 2 re-acquired a lease,
/// and checks it advances `matched` but not the grant.
fn assert_stale_ack_not_credited(nt: &mut Network, stale: Message, w0: u64, w1: u64) {
    let (incarnation, promise) = {
        let ro = &nt.peers[&2].read_only;
        (ro.incarnation_id(), ro.read_lease_acked_index())
    };
    assert_ne!(
        stale.log_term, incarnation,
        "the ack is from the previous incarnation"
    );
    let grant_before = granted_acked(nt, 2);
    assert!(grant_before <= promise);
    assert!(promise < w1, "the new incarnation never promised w1");

    pump(nt, vec![stale], &lossy_to_2);
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().matched,
        w1,
        "replication still counts the ack"
    );
    assert_eq!(granted_acked(nt, 2), grant_before, "the grant does not");
    assert_eq!(
        nt.peers[&1].raft_log.committed, w0,
        "the clamp still holds w1"
    );

    let committed = nt.peers[&1].raft_log.committed;
    let rs = local_read(nt, 2, "rd").expect("2 serves locally");
    assert!(rs.served_by_follower_lease);
    assert!(
        rs.index >= committed,
        "read served at {} below committed {}",
        rs.index,
        committed
    );

    // The current incarnation's own acks lift the clamp.
    propose(nt, &no_drop);
    let last = nt.peers[&1].raft_log.last_index();
    assert_eq!(nt.peers[&1].raft_log.committed, last);
}

/// An ack sent before a restart, delivered after the restarted holder's new
/// grant, must not lift the clamp.
#[test]
fn test_stale_ack_from_previous_incarnation() {
    let l = default_logger();
    let mut nt = incarnation_group(false);
    propose(&mut nt, &no_drop);
    let w0 = nt.peers[&1].raft_log.last_index();
    assert_eq!(nt.peers[&1].raft_log.committed, w0);
    let (w1, stale) = propose_holding_ack_from_2(&mut nt);

    // 2 crashes and restarts in the same term with its log and HardState.
    let store: MemStorage = nt.peers[&2].raft_log.store.clone();
    store.wl().set_hardstate(nt.peers[&2].hard_state());
    nt.peers.insert(
        2,
        Interface::new(Raft::new(&incarnation_config(2, false), store, &l).unwrap()),
    );
    assert_eq!(nt.peers[&2].term, nt.peers[&1].term);
    assert_eq!(
        nt.peers[&2].read_only.read_lease_acked_index(),
        0,
        "promise lost on restart"
    );

    // It re-acquires a lease while appends and ack-index asks to it are lost.
    beat(&mut nt, &lossy_to_2);
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());

    assert_stale_ack_not_credited(&mut nt, stale, w0, w1);
}

/// As above, but the promise is lost by a same-term reset: 2 times out into
/// pre-candidacy (its pre-votes are lost), then hears the leader again.
#[test]
fn test_stale_ack_after_same_term_reset() {
    let mut nt = incarnation_group(true);
    propose(&mut nt, &no_drop);
    let w0 = nt.peers[&1].raft_log.last_index();
    let (w1, stale) = propose_holding_ack_from_2(&mut nt);

    let (term, old) = (nt.peers[&2].term, nt.peers[&2].read_only.incarnation_id());
    {
        let n2 = nt.peers.get_mut(&2).unwrap();
        n2.step(new_message(2, 2, MessageType::MsgHup, 0)).unwrap();
        assert_eq!(n2.state, StateRole::PreCandidate);
        let _ = n2.read_messages();
    }
    beat(&mut nt, &lossy_to_2);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].term, term, "same-term reset");
    assert_ne!(
        nt.peers[&2].read_only.incarnation_id(),
        old,
        "reset() starts a new incarnation"
    );
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());

    assert_stale_ack_not_credited(&mut nt, stale, w0, w1);
}

/// As above, but through a lost pre-vote: the rejections make 2 a follower
/// again at its own term (`VoteResult::Lost`), with no leader known until the
/// next heartbeat.
#[test]
fn test_stale_ack_after_lost_pre_vote() {
    let mut nt = incarnation_group(true);
    propose(&mut nt, &no_drop);
    let w0 = nt.peers[&1].raft_log.last_index();
    let (w1, stale) = propose_holding_ack_from_2(&mut nt);

    let (term, old) = (nt.peers[&2].term, nt.peers[&2].read_only.incarnation_id());
    {
        let n2 = nt.peers.get_mut(&2).unwrap();
        n2.step(new_message(2, 2, MessageType::MsgHup, 0)).unwrap();
        assert_eq!(n2.state, StateRole::PreCandidate);
        let _ = n2.read_messages();
        for from in [1, 3] {
            let mut rej = new_message(from, 2, MessageType::MsgRequestPreVoteResponse, 0);
            rej.term = term;
            rej.reject = true;
            n2.step(rej).unwrap();
        }
        assert_eq!(n2.state, StateRole::Follower);
        assert_eq!(n2.term, term, "same-term reset");
        assert_eq!(n2.leader_id, INVALID_ID);
        assert_ne!(
            n2.read_only.incarnation_id(),
            old,
            "reset() starts a new incarnation"
        );
        let _ = n2.read_messages();
    }
    beat(&mut nt, &lossy_to_2);
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());

    assert_stale_ack_not_credited(&mut nt, stale, w0, w1);
}

/// The leader replaces a grant when it issues the renewal, but the holder keeps
/// the old lease id until the renewal arrives. Acks sent in that gap come from
/// the same incarnation and must still count, or every renewal would stall
/// commits.
#[test]
fn test_ack_during_renewal_credits_new_grant() {
    let mut nt = incarnation_group(false);
    let old_id = nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .held_lease()
        .unwrap()
        .lease_id;

    // 2 asks for a renewal; the leader grants it but the grant is held back.
    let (renew_id, committed) = {
        let n2 = nt.peers.get_mut(&2).unwrap();
        (n2.read_only.get_marked_lease_id(), n2.raft_log.committed)
    };
    let mut ask = new_message(2, 1, MessageType::MsgAskReadLease, 0);
    ask.term = nt.peers[&1].term;
    ask.index = committed;
    ask.log_term = renew_id;
    let no_grant_to_2 =
        |m: &Message| m.to == 2 && m.get_msg_type() == MessageType::MsgAskReadLeaseResp;
    pump(&mut nt, vec![ask], &no_grant_to_2);
    assert_eq!(
        nt.peers[&1].read_only.granted_lease(2).unwrap().lease_id,
        renew_id,
        "the leader replaced the grant"
    );
    assert_eq!(
        nt.peers
            .get_mut(&2)
            .unwrap()
            .read_only
            .held_lease()
            .unwrap()
            .lease_id,
        old_id,
        "the holder still holds the old lease"
    );
    assert_eq!(
        granted_acked(&nt, 2),
        nt.peers[&1].prs().get(2).unwrap().matched,
        "a normal renewal starts where matched is"
    );

    propose(&mut nt, &no_grant_to_2);
    let w = nt.peers[&1].raft_log.last_index();
    assert!(granted_acked(&nt, 2) >= w);
    assert_eq!(nt.peers[&1].raft_log.committed, w);
}

/// A stale ack from the previous incarnation still advances `matched`. A
/// renewal must not start from that `matched`: the leader's clamp switches to
/// the renewal before the holder installs it, while the holder keeps serving
/// under its current grant and promise.
#[test]
fn test_renewal_ignores_stale_ack_match() {
    let l = default_logger();
    let mut nt = incarnation_group(false);
    propose(&mut nt, &no_drop);
    let w0 = nt.peers[&1].raft_log.last_index();
    let (w1, stale) = propose_holding_ack_from_2(&mut nt);

    // 2 restarts and re-acquires a lease; then the stale ack arrives.
    let store: MemStorage = nt.peers[&2].raft_log.store.clone();
    store.wl().set_hardstate(nt.peers[&2].hard_state());
    nt.peers.insert(
        2,
        Interface::new(Raft::new(&incarnation_config(2, false), store, &l).unwrap()),
    );
    beat(&mut nt, &lossy_to_2);
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());
    pump(&mut nt, vec![stale], &lossy_to_2);
    assert_eq!(nt.peers[&1].prs().get(2).unwrap().matched, w1);
    assert_eq!(nt.peers[&1].raft_log.committed, w0);

    // 2 renews; the leader issues the renewal but it does not arrive.
    let (renew_id, committed) = {
        let n2 = nt.peers.get_mut(&2).unwrap();
        (n2.read_only.get_marked_lease_id(), n2.raft_log.committed)
    };
    let mut ask = new_message(2, 1, MessageType::MsgAskReadLease, 0);
    ask.term = nt.peers[&1].term;
    ask.index = committed;
    ask.log_term = renew_id;
    let no_grant = |m: &Message| {
        lossy_to_2(m) || (m.to == 2 && m.get_msg_type() == MessageType::MsgAskReadLeaseResp)
    };
    pump(&mut nt, vec![ask], &no_grant);
    assert_eq!(
        nt.peers[&1].read_only.granted_lease(2).unwrap().lease_id,
        renew_id
    );
    assert!(
        granted_acked(&nt, 2) <= nt.peers[&2].read_only.read_lease_acked_index(),
        "the renewal must not exceed the promise 2 is still serving under"
    );

    let no_hb =
        |m: &Message| no_grant(m) || (m.to == 2 && m.get_msg_type() == MessageType::MsgHeartbeat);
    propose(&mut nt, &no_hb);
    let committed = nt.peers[&1].raft_log.committed;
    assert_eq!(committed, w0, "the clamp still holds w1");
    let rs = local_read(&mut nt, 2, "rd").expect("2 serves locally");
    assert!(
        rs.index >= committed,
        "read served at {} below committed {}",
        rs.index,
        committed
    );
}

/// A holder's `MsgAskAckIndexResp` carries its incarnation and counts only
/// from the incarnation that asked for the grant; a Term-0 switch credit
/// injected by the application counts regardless.
#[test]
fn test_ask_ack_index_resp_incarnation() {
    let mut nt = incarnation_group(false);
    let term = nt.peers[&1].term;
    let inc = nt.peers[&2].read_only.incarnation_id();

    let mut ask = new_message(1, 2, MessageType::MsgAskAckIndex, 0);
    ask.term = term;
    let n2 = nt.peers.get_mut(&2).unwrap();
    n2.step(ask).unwrap();
    let resp = n2
        .read_messages()
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAskAckIndexResp)
        .unwrap();
    assert_eq!(resp.log_term, inc, "the answer carries the incarnation");

    let base = granted_acked(&nt, 2);
    let ack_resp = |index: u64, term: u64, log_term: u64| {
        let mut m = new_message(2, 1, MessageType::MsgAskAckIndexResp, 0);
        m.term = term;
        m.index = index;
        m.log_term = log_term;
        m
    };
    let n1 = nt.peers.get_mut(&1).unwrap();
    n1.step(ack_resp(base + 1, term, inc + 1)).unwrap();
    assert_eq!(
        granted_acked(&nt, 2),
        base,
        "another incarnation's answer is ignored"
    );

    let n1 = nt.peers.get_mut(&1).unwrap();
    n1.step(ack_resp(base + 1, term, inc)).unwrap();
    assert_eq!(
        granted_acked(&nt, 2),
        base + 1,
        "the asking incarnation's answer counts"
    );

    let n1 = nt.peers.get_mut(&1).unwrap();
    n1.step(ack_resp(base + 2, 0, 0)).unwrap();
    assert_eq!(
        granted_acked(&nt, 2),
        base + 2,
        "a Term-0 switch credit counts"
    );
}
