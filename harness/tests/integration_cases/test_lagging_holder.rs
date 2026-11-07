// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! LIVE-2: a holder whose appends are lost, while its heartbeats, asks and
//! grants still get through, must not be renewed forever. Its grant pins the
//! commit clamp, so the catch-up check against `committed` always passed and
//! commit stayed wedged although a quorum held every entry.

use harness::*;
use raft::eraftpb::*;
use raft::*;

use std::time::Duration;

use crate::test_util::*;

/// Only node 2 asks for a lease, so it is the only holder.
fn lagging_config(id: u64, lease_micros: u64, pre_vote: bool) -> Config {
    Config {
        check_quorum: true,
        pre_vote,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: lease_micros,
        max_num_read_leases: 5,
        ask_for_read_lease: id == 2,
        read_lease_catchup_margin: 10,
        ..new_test_config(id, 10, 1)
    }
}

/// Delivers `msgs` breadth-first until quiet, dropping every message `drop`
/// selects, and persists after every step like `Network::send`.
pub(crate) fn pump(nt: &mut Network, msgs: Vec<Message>, drop: &dyn Fn(&Message) -> bool) {
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

#[test]
fn test_lagging_holder_renewal_refused() {
    let l = default_logger();
    let peers = [1, 2, 3];
    let mk = |id: u64| {
        let storage = new_storage();
        storage.initialize_with_conf_state((peers.to_vec(), vec![]));
        Some(new_test_raft_with_config(
            &lagging_config(id, 50_000, false),
            storage,
            &l,
        ))
    };
    let mut nt = Network::new_with_config(
        vec![mk(1), mk(2), mk(3)],
        &lagging_config(1, 50_000, false),
        &l,
    );
    // Clear the lease-ask rate limit, measured from process start.
    std::thread::sleep(Duration::from_millis(15));
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());
    let stuck = nt.peers[&1].raft_log.committed;

    let drop_app_to_2 = |m: &Message| {
        m.to == 2
            && (m.get_msg_type() == MessageType::MsgAppend
                || m.get_msg_type() == MessageType::MsgSnapshot)
    };
    // About 12 lease durations (10 ms per round), with a write every round.
    for _ in 0..60 {
        let mut out = vec![];
        let n1 = nt.peers.get_mut(&1).unwrap();
        let _ = n1.step(new_message(1, 1, MessageType::MsgPropose, 1));
        n1.persist();
        out.extend(n1.read_messages());
        for id in 1..=3u64 {
            let p = nt.peers.get_mut(&id).unwrap();
            p.tick();
            p.persist();
            out.extend(p.read_messages());
        }
        pump(&mut nt, out, &drop_app_to_2);
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        nt.peers[&2].raft_log.last_index(),
        stuck,
        "2 never received an append"
    );
    let n1 = nt.peers.get_mut(&1).unwrap();
    assert_eq!(n1.state, StateRole::Leader);
    let m3 = n1.prs().get(3).unwrap().matched;
    assert!(m3 > stuck + 10, "the quorum (1, 3) moved on: {}", m3);
    assert!(
        !n1.read_only.has_active_granted_lease(2),
        "2's renewal was refused"
    );
    assert!(
        n1.raft_log.committed > stuck + 10,
        "commit wedged at {} behind 2 while the quorum holds {}",
        n1.raft_log.committed,
        m3
    );
}

/// 5b refuses only renewals. A holder that resets while its grant pins commit
/// asks from a new incarnation, whose acks can never raise the old grant. That
/// ask must replace the grant, as it did before 5b, instead of waiting out the
/// old grant (up to D).
#[test]
fn test_new_incarnation_not_treated_as_renewal() {
    let l = default_logger();
    let mk = |id: u64| {
        let storage = new_storage();
        storage.initialize_with_conf_state((vec![1, 2, 3], vec![]));
        Some(new_test_raft_with_config(
            &lagging_config(id, 60_000_000, true),
            storage,
            &l,
        ))
    };
    let mut nt = Network::new_with_config(
        vec![mk(1), mk(2), mk(3)],
        &lagging_config(1, 60_000_000, true),
        &l,
    );
    std::thread::sleep(Duration::from_millis(15));
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
    assert!(nt
        .peers
        .get_mut(&2)
        .unwrap()
        .read_only
        .has_active_held_lease());
    let w0 = nt.peers[&1].raft_log.committed;
    let no_drop = |_: &Message| false;
    let step_and_pump = |nt: &mut Network, m: Message, drop: &dyn Fn(&Message) -> bool| {
        let msgs = {
            let p = nt.peers.get_mut(&m.to).unwrap();
            let _ = p.step(m);
            p.persist();
            p.read_messages()
        };
        pump(nt, msgs, drop);
    };

    // 2 receives the writes but its acks are lost, so its grant pins commit
    // more than the margin below the quorum (1, 3).
    let ackless = |m: &Message| {
        (m.from == 2
            && matches!(
                m.get_msg_type(),
                MessageType::MsgAppendResponse | MessageType::MsgAskAckIndexResp
            ))
            || (m.to == 2 && m.get_msg_type() == MessageType::MsgAskAckIndex)
    };
    for _ in 0..12 {
        step_and_pump(
            &mut nt,
            new_message(1, 1, MessageType::MsgPropose, 1),
            &ackless,
        );
    }
    let quorum = nt
        .peers
        .get_mut(&1)
        .unwrap()
        .mut_prs()
        .maximal_committed_index()
        .0;
    let old_grant = nt.peers[&1].read_only.granted_lease(2).unwrap().clone();
    assert_eq!(
        nt.peers[&1].raft_log.committed, w0,
        "2's grant clamps commit"
    );
    assert!(quorum > old_grant.acked_index + 10);

    // 2 times out into pre-candidacy (its pre-votes are lost), then hears the
    // leader again and asks from its new incarnation.
    let old = nt.peers[&2].read_only.incarnation_id();
    {
        let n2 = nt.peers.get_mut(&2).unwrap();
        n2.step(new_message(2, 2, MessageType::MsgHup, 0)).unwrap();
        assert_eq!(n2.state, StateRole::PreCandidate);
        let _ = n2.read_messages();
    }
    step_and_pump(
        &mut nt,
        new_message(1, 1, MessageType::MsgBeat, 0),
        &no_drop,
    );
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    let new = nt.peers[&2].read_only.incarnation_id();
    assert_ne!(new, old, "reset() starts a new incarnation");

    assert!(
        nt.peers
            .get_mut(&2)
            .unwrap()
            .read_only
            .has_active_held_lease(),
        "the new incarnation was granted"
    );
    assert_eq!(
        nt.peers[&1].read_only.granted_lease(2).unwrap().lease_id >> 32,
        new,
        "the new grant replaced the old incarnation's"
    );
    // The grant starts at the asked index; the next append's acks, now from
    // the grant's incarnation, lift the clamp.
    step_and_pump(
        &mut nt,
        new_message(1, 1, MessageType::MsgPropose, 1),
        &no_drop,
    );
    assert_eq!(
        nt.peers[&1].raft_log.committed,
        nt.peers[&1].raft_log.last_index(),
        "the new incarnation's acks lift the clamp"
    );
}
