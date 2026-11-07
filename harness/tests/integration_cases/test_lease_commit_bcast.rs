// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! EXP-3: a lease holder serves a read only once its own commit index reaches
//! the read's bar, so a commit it learns only from the next append makes its
//! reads wait or forward. Under `GrantLeases` the leader therefore broadcasts
//! every commit advance while any follower holds or has parked a grant, even
//! with `skip_bcast_commit` set (TiKV's default). Without holders, commit
//! notification stays as configured.

use harness::*;
use raft::eraftpb::*;
use raft::*;

use std::time::Duration;

use crate::test_util::*;

/// `skip_bcast_commit` on every node, as TiKV runs it. Followers ask for
/// leases iff `ask`.
fn bcast_config(id: u64, ask: bool, lease_micros: u64) -> Config {
    Config {
        check_quorum: true,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: lease_micros,
        max_num_read_leases: 5,
        ask_for_read_lease: ask && id != 1,
        read_lease_catchup_margin: 10,
        skip_bcast_commit: true,
        ..new_test_config(id, 20, 1)
    }
}

/// Elects 1, then sends one heartbeat round, which carries the no-op's commit
/// to the followers and, when they ask, gets them their leases.
fn new_network(ask: bool, lease_micros: u64) -> Network {
    let l = default_logger();
    let peers = [1, 2, 3];
    let mk = |id: u64| {
        let storage = new_storage();
        storage.initialize_with_conf_state((peers.to_vec(), vec![]));
        Some(new_test_raft_with_config(
            &bcast_config(id, ask, lease_micros),
            storage,
            &l,
        ))
    };
    let mut nt = Network::new_with_config(
        vec![mk(1), mk(2), mk(3)],
        &bcast_config(1, ask, lease_micros),
        &l,
    );
    // Clear the lease-ask rate limit, measured from process start.
    std::thread::sleep(Duration::from_millis(15));
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
    let committed = nt.peers[&1].raft_log.committed;
    for id in [2, 3] {
        assert_eq!(
            nt.peers[&id].raft_log.committed, committed,
            "setup: the heartbeat carries the no-op's commit to {id}"
        );
        assert_eq!(
            nt.peers
                .get_mut(&1)
                .unwrap()
                .read_only
                .has_active_granted_lease(id),
            ask,
            "setup: {id} holds a grant iff it asks"
        );
    }
    nt
}

fn propose(nt: &mut Network) -> u64 {
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    let n1 = &nt.peers[&1];
    assert_eq!(
        n1.raft_log.committed,
        n1.raft_log.last_index(),
        "the write commits"
    );
    n1.raft_log.committed
}

/// With both followers holding grants, each write's commit reaches them as it
/// happens, not with the next write.
#[test]
fn test_commit_reaches_lease_holders_at_once() {
    let mut nt = new_network(true, 10_000_000);
    for _ in 0..3 {
        let c = propose(&mut nt);
        for id in [2, 3] {
            assert_eq!(
                nt.peers[&id].raft_log.committed, c,
                "holder {id} learns commit {c} without waiting for another append"
            );
        }
    }
}

/// With no grants, `skip_bcast_commit` holds: followers learn a write's commit
/// only from a later message.
#[test]
fn test_commit_stays_lazy_without_holders() {
    let mut nt = new_network(false, 10_000_000);
    let c = propose(&mut nt);
    for id in [2, 3] {
        assert!(
            nt.peers[&id].raft_log.committed < c,
            "follower {id} without a grant is not told about commit {c} eagerly"
        );
    }
}

/// Once the grants have lapsed (nothing renews them here: no heartbeat, read
/// or timer reaches the followers), commit notification is lazy again.
#[test]
fn test_commit_lazy_again_after_grants_lapse() {
    let mut nt = new_network(true, 30_000);
    std::thread::sleep(Duration::from_millis(45)); // past the 30 ms leases
    let c = propose(&mut nt);
    for id in [2, 3] {
        assert!(
            nt.peers[&id].raft_log.committed < c,
            "follower {id} whose grant lapsed is not told about commit {c} eagerly"
        );
    }
}
