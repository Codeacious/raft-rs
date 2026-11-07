// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! LIVE-4: a grant that stops pinning commit when it expires drives no commit
//! by itself. The library re-runs the clamp from `tick_heartbeat` and before
//! lease admission (`maybe_commit_clamped`), without relying on the wrapper's
//! ReadLeaseExpiry timer.

use harness::*;
use raft::eraftpb::*;
use raft::*;

use std::time::Duration;

use super::test_lagging_holder::pump;
use crate::test_util::*;

/// Only node 2 asks for a lease, so it is the only holder, and no other ask
/// re-runs the clamp at admission.
fn expiry_config(id: u64, lease_micros: u64, heartbeat_tick: usize) -> Config {
    Config {
        check_quorum: true,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: lease_micros,
        max_num_read_leases: 5,
        ask_for_read_lease: id == 2,
        read_lease_catchup_margin: 10,
        ..new_test_config(id, 20, heartbeat_tick)
    }
}

fn new_network(lease_micros: u64, heartbeat_tick: usize) -> Network {
    let l = default_logger();
    let peers = [1, 2, 3];
    let mk = |id: u64| {
        let storage = new_storage();
        storage.initialize_with_conf_state((peers.to_vec(), vec![]));
        Some(new_test_raft_with_config(
            &expiry_config(id, lease_micros, heartbeat_tick),
            storage,
            &l,
        ))
    };
    let mut nt = Network::new_with_config(
        vec![mk(1), mk(2), mk(3)],
        &expiry_config(1, lease_micros, heartbeat_tick),
        &l,
    );
    // Clear the lease-ask rate limit, measured from process start.
    std::thread::sleep(Duration::from_millis(15));
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
    assert!(nt
        .peers
        .get_mut(&1)
        .unwrap()
        .read_only
        .has_active_granted_lease(2));
    nt
}

/// Once an unreachable holder's grant expires in the leader's view, the write
/// its grant held back commits on an idle group with only ticks to drive it.
#[test]
fn test_expired_holder_write_commits() {
    let mut nt = new_network(30_000, 1);
    let cut2 = |m: &Message| m.to == 2 || m.from == 2;
    let msgs = {
        let n1 = nt.peers.get_mut(&1).unwrap();
        let _ = n1.step(new_message(1, 1, MessageType::MsgPropose, 1));
        n1.persist();
        n1.read_messages()
    };
    pump(&mut nt, msgs, &cut2);
    let w = nt.peers[&1].raft_log.last_index();
    assert!(
        nt.peers[&1].raft_log.committed < w,
        "the clamp holds w while 2 is unreachable"
    );

    std::thread::sleep(Duration::from_millis(45)); // past the 30 ms lease
    for _ in 0..20 {
        let msgs = {
            let n1 = nt.peers.get_mut(&1).unwrap();
            n1.tick();
            n1.persist();
            n1.read_messages()
        };
        pump(&mut nt, msgs, &cut2);
    }
    let n1 = &nt.peers[&1];
    assert_eq!(n1.state, StateRole::Leader);
    assert_eq!(
        n1.raft_log.committed, w,
        "w commits after 2's grant expired"
    );
}

/// LIVE-2 residual: once a lagging holder's grant expires, its next ask is
/// judged as a new ask by check 5 against `committed`. If `committed` is still
/// the value the expired grant clamped it to, the holder is re-granted at its
/// old index and pins commit for another lease. The ask arrives here with no
/// leader tick in between, so only admission can re-run the clamp; TiKV's
/// expiry timer can lose the same race.
#[test]
fn test_lagging_holder_not_regranted_after_expiry() {
    let lease_micros = 50_000;
    let mut nt = new_network(lease_micros, 1);
    let stuck = nt.peers[&1].raft_log.committed;

    // More writes than the catch-up margin, none of them reaching 2.
    let drop_app_to_2 = |m: &Message| {
        m.to == 2
            && (m.get_msg_type() == MessageType::MsgAppend
                || m.get_msg_type() == MessageType::MsgSnapshot)
    };
    for _ in 0..20 {
        let msgs = {
            let n1 = nt.peers.get_mut(&1).unwrap();
            let _ = n1.step(new_message(1, 1, MessageType::MsgPropose, 1));
            n1.persist();
            n1.read_messages()
        };
        pump(&mut nt, msgs, &drop_app_to_2);
    }
    {
        let n1 = nt.peers.get_mut(&1).unwrap();
        assert_eq!(n1.raft_log.committed, stuck, "2's grant clamps commit");
        let m3 = n1.prs().get(3).unwrap().matched;
        assert!(m3 > stuck + 10, "the quorum (1, 3) moved past the margin");
    }

    // The group goes idle and 2's grant expires on both sides. The heartbeat
    // makes 2 ask at once.
    std::thread::sleep(Duration::from_micros(lease_micros) + Duration::from_millis(20));
    assert!(!nt
        .peers
        .get_mut(&1)
        .unwrap()
        .read_only
        .has_active_granted_lease(2));
    let msgs = {
        let n1 = nt.peers.get_mut(&1).unwrap();
        let _ = n1.step(new_message(1, 1, MessageType::MsgBeat, 0));
        n1.read_messages()
    };
    pump(&mut nt, msgs, &drop_app_to_2);

    let n1 = nt.peers.get_mut(&1).unwrap();
    let m3 = n1.prs().get(3).unwrap().matched;
    assert_eq!(
        n1.raft_log.committed, m3,
        "commit caught up with the quorum"
    );
    assert!(
        !n1.read_only.has_active_granted_lease(2),
        "2 was not re-granted at its old index"
    );
}

/// A tick that re-runs a binding clamp without advancing commit still emits
/// `MsgAskAckIndex` to the live holder below the quorum, and must report a
/// Ready: TiKV collects one after a tick only when `tick()` returns true. Here
/// the first ask was lost, so the tick's ask is the retry.
#[test]
fn test_clamped_tick_ack_ask_reports_ready() {
    // A 10-tick heartbeat, so the tick under test sends no heartbeat.
    let mut nt = new_network(500_000, 10);
    let stuck = nt.peers[&1].raft_log.committed;
    let lose_to_2 = |m: &Message| {
        m.to == 2
            && matches!(
                m.get_msg_type(),
                MessageType::MsgAppend | MessageType::MsgSnapshot | MessageType::MsgAskAckIndex
            )
    };
    let msgs = {
        let n1 = nt.peers.get_mut(&1).unwrap();
        let _ = n1.step(new_message(1, 1, MessageType::MsgPropose, 1));
        n1.persist();
        n1.read_messages()
    };
    pump(&mut nt, msgs, &lose_to_2);
    assert_eq!(
        nt.peers[&1].raft_log.committed, stuck,
        "2's grant clamps commit"
    );

    // Past the 5 ms per-holder ask interval.
    std::thread::sleep(Duration::from_millis(10));
    let n1 = nt.peers.get_mut(&1).unwrap();
    assert!(n1.read_only.has_active_granted_lease(2));
    let ready = n1.tick();
    let msgs = n1.read_messages();
    assert_eq!(n1.raft_log.committed, stuck, "commit did not advance");
    assert!(
        msgs.iter()
            .any(|m| m.to == 2 && m.get_msg_type() == MessageType::MsgAskAckIndex),
        "the tick re-asked 2: {:?}",
        msgs
    );
    assert!(
        !msgs
            .iter()
            .any(|m| m.get_msg_type() == MessageType::MsgHeartbeat),
        "not a heartbeat tick"
    );
    assert!(ready, "tick() reports the queued ack ask as a Ready");
}
