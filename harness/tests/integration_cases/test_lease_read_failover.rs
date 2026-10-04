// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Regression coverage for the two upstream etcd-raft leased-read bugs, ported
//! to this fork: etcd-io/raft#99 (`MsgTransferLeader` is a torn update of
//! CheckQuorum state) and etcd-io/raft#166 (the leader's `LeaseBased` "lease" is
//! a tumbling window, so a deposed leader keeps answering reads).
//!
//! Twins of `/home/codeacious/tack/deps/raft/raft_transfer_checkquorum_test.go`
//! and `.../read_only_lease_failover_test.go`.

use harness::*;
use raft::eraftpb::*;
use raft::*;

use crate::test_util::*;

const ET: usize = 10;

/// Advance every node by one tick and deliver whatever it emits, honouring the
/// network's `cut()` rules. The analogue of the Go tests' `tick` closure.
fn tick_all(nt: &mut Network) {
    for id in 1..=3u64 {
        let msgs = {
            let p = nt.peers.get_mut(&id).unwrap();
            p.tick();
            p.persist();
            p.read_messages()
        };
        let msgs = nt.filter(msgs);
        nt.send(msgs);
    }
}

/// The self-lease duration for the failover tests. The new-leader safeguard is
/// one lease duration of wall clock, so it must be short enough to lift within
/// the test, and long enough to outlast the few ticks between priming a lease
/// and the election.
const LEASE_MICROS: u64 = 50_000;
/// Wall clock per tick in the overlap loops. The safeguard is pure wall clock,
/// so a tick loop that passes no time would never lift it. And it must be long
/// enough that the deposed leader's CheckQuorum step-down (ticks) lands well
/// after the new leader's safeguard lifts (one lease duration): without that
/// overlap, a deposed leader answering from `committed` with no lease at all
/// would go undetected.
const TICK_SLEEP: std::time::Duration = std::time::Duration::from_millis(10);

fn leased_raft(id: u64, option: ReadOnlyOption, l: &slog::Logger) -> Interface {
    let mut cfg = new_test_config(id, ET, 1);
    cfg.check_quorum = true;
    cfg.pre_vote = false;
    cfg.read_only_option = option;
    cfg.read_lease_duration_micros = LEASE_MICROS;
    cfg.max_num_read_leases = 5;
    cfg.read_lease_catchup_margin = 10;
    let storage = new_storage();
    storage.initialize_with_conf_state((vec![1, 2, 3], vec![]));
    new_test_raft_with_config(&cfg, storage, l)
}

fn leased_network(option: ReadOnlyOption, l: &slog::Logger) -> Network {
    let mut cfg = new_test_config(1, ET, 1);
    cfg.check_quorum = true;
    cfg.pre_vote = false;
    cfg.read_only_option = option;
    Network::new_with_config(
        vec![
            Some(leased_raft(1, option, l)),
            Some(leased_raft(2, option, l)),
            Some(leased_raft(3, option, l)),
        ],
        &cfg,
        l,
    )
}

/// Serves one read at `id` through the whole network, so its heartbeat round
/// (which is also a lease round) completes, and asserts the leader came out
/// holding a live self-lease. Without the assertion a lease that never
/// installed would leave the overlap checks below vacuously true.
fn prime_self_lease(nt: &mut Network, id: u64) {
    nt.send(vec![new_message_with_entries(
        id,
        id,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some("prime"))],
    )]);
    let p = nt.peers.get_mut(&id).unwrap();
    p.read_states.clear();
    assert!(
        p.read_only.has_active_held_lease(),
        "precondition: node {} should hold a self-lease",
        id
    );
}

/// Runs the overlap after a new leader (node 2) has been elected while the
/// old one (node 1) is cut off. At every tick, the deposed leader must never
/// answer a read below what the new term has made visible to clients: that is
/// the safety property, and it holds regardless of when node 1 steps down.
///
/// Also asserts liveness (the new leader commits: a safeguard that never lifts
/// would satisfy the invariant trivially) and non-vacuity (node 1 really did
/// answer reads from its self-lease after the election).
fn assert_no_stale_read_in_overlap(nt: &mut Network, stale_committed: u64) {
    let (mut committed_at, mut answered_after_election) = (None, 0);
    for i in 0..=6 * ET {
        let new_committed = nt.peers[&2].raft_log.committed;
        if nt.peers[&1].state == StateRole::Leader {
            if let Some(idx) = read_index_at(nt, 1) {
                answered_after_election += 1;
                assert!(
                    idx >= new_committed,
                    "tick {}: deposed leader answered a read at index {} while the new leader \
                     had committed {} -- stale read across failover",
                    i,
                    idx,
                    new_committed
                );
            }
        }
        if new_committed > stale_committed && committed_at.is_none() {
            committed_at = Some(i);
        }
        if committed_at.is_some() && nt.peers[&1].state != StateRole::Leader {
            break;
        }
        tick_all(nt);
        std::thread::sleep(TICK_SLEEP);
    }
    assert!(
        committed_at.is_some(),
        "new leader never committed; safeguard did not lift"
    );
    assert!(
        answered_after_election > 0,
        "the deposed leader never answered from its self-lease; the check was vacuous"
    );
}

/// The index this node would answer a read with right now, or `None` if it
/// declined to answer.
fn read_index_at(nt: &mut Network, id: u64) -> Option<u64> {
    let p = nt.peers.get_mut(&id).unwrap();
    let before = p.read_states.len();
    let m = new_message_with_entries(
        id,
        id,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some("probe"))],
    );
    let _ = p.step(m);
    p.persist();
    let _ = p.read_messages();
    if p.read_states.len() == before {
        None
    } else {
        Some(p.read_states.last().unwrap().index)
    }
}

/// Regression test for etcd-io/raft#99.
///
/// `handle_transfer_leader` resets `election_elapsed` to give the transfer its
/// own deadline. That counter is also CheckQuorum's cadence, whose state
/// includes the per-peer `recent_active` flags; resetting the counter without
/// clearing the flags is a torn update, so activity recorded *before* the
/// transfer carries the leader through the next boundary and step-down takes
/// `2 * election_timeout` instead of one.
///
/// That window is a linearizability hazard for both leased-read modes:
/// `CAMPAIGN_TRANSFER` sets `force`, bypassing the `in_lease` vote filter, so
/// the transferee wins immediately while the old leader keeps serving reads
/// from a commit index the new term has already moved past.
///
/// The bound must hold wherever in the CheckQuorum cycle the transfer lands --
/// the pre-fix behaviour is worst when the transfer arrives early in a cycle.
#[test]
fn test_leader_transfer_clears_quorum_activity() {
    let l = default_logger();

    for transfer_at in [1usize, ET / 2, ET - 1] {
        let mut cfg = new_test_config(1, ET, 1);
        cfg.check_quorum = true;
        cfg.pre_vote = false;
        let mut nt = Network::new_with_config(vec![None, None, None], &cfg, &l);
        nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        assert_eq!(nt.peers[&1].state, StateRole::Leader);

        // Advance to the requested phase of the CheckQuorum cycle.
        while nt.peers[&1].election_elapsed != transfer_at {
            tick_all(&mut nt);
        }

        // Transfer to 2, then cut 1 off before any vote can reach it. The one
        // MsgTimeoutNow that already left is still delivered.
        let in_flight = {
            let n1 = nt.peers.get_mut(&1).unwrap();
            n1.step(new_message(2, 1, MessageType::MsgTransferLeader, 0))
                .expect("step transfer");
            n1.read_messages()
        };
        nt.cut(1, 2);
        nt.cut(1, 3);
        for m in in_flight {
            if m.get_msg_type() == MessageType::MsgTimeoutNow {
                nt.peers
                    .get_mut(&m.to)
                    .unwrap()
                    .step(m)
                    .expect("timeout now");
            }
        }
        let votes = nt.peers.get_mut(&2).unwrap().read_messages();
        let votes = nt.filter(votes);
        nt.send(votes);

        assert_eq!(
            nt.peers[&2].state,
            StateRole::Leader,
            "transfer_at={}: transferee should win immediately",
            transfer_at
        );
        assert!(nt.peers[&2].term > nt.peers[&1].term);

        let mut stepped_down = None;
        for i in 1..=4 * ET {
            tick_all(&mut nt);
            if nt.peers[&1].state != StateRole::Leader {
                stepped_down = Some(i);
                break;
            }
        }

        let stepped_down = stepped_down.unwrap_or_else(|| {
            panic!(
                "transfer_at={}: deposed leader never stepped down",
                transfer_at
            )
        });
        assert!(
            stepped_down <= ET,
            "transfer_at={}: deposed leader served for {} ticks after the transferee won; \
             CheckQuorum must bound this at one election timeout ({})",
            transfer_at,
            stepped_down,
            ET
        );
    }
}

/// Regression test for etcd-io/raft#166.
///
/// Under `ReadOnlyOption::LeaseBased` a leader answers `MsgReadIndex` straight
/// from `raft_log.committed` with no timing check at all; its only brake is
/// CheckQuorum, which is driven by the sticky per-peer `recent_active` flag.
/// That flag carries no age, so a partitioned leader survives one boundary on
/// evidence up to a full election timeout old and only steps down at the next
/// -- while the followers, whose `election_elapsed` is a true sliding window,
/// call an election after one. The deposed leader therefore keeps answering
/// reads from a commit index the new term has already moved past.
///
/// The property is asserted at every tick of the overlap, plus liveness: a
/// safeguard that never lifts would satisfy it trivially.
#[test]
fn test_lease_based_read_not_stale_after_failover() {
    let l = default_logger();
    let mut nt = leased_network(ReadOnlyOption::LeaseBased, &l);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    // Settle past a CheckQuorum boundary, so recent_active is freshly set and
    // the old leader gets the longest run it can.
    for _ in 0..=ET {
        tick_all(&mut nt);
    }
    let stale_committed = nt.peers[&1].raft_log.committed;
    assert!(stale_committed > 0);
    prime_self_lease(&mut nt, 1);

    nt.cut(1, 2);
    nt.cut(1, 3);
    // Drive the election by hand at exactly one election timeout of silence --
    // the earliest a new leader can legitimately appear. `in_lease` keys off
    // the *base* election_timeout, so raising only the randomized timeout stops
    // 2 and 3 campaigning on their own without affecting when they may vote.
    for id in [2u64, 3] {
        nt.peers
            .get_mut(&id)
            .unwrap()
            .set_randomized_election_timeout(2 * ET - 1);
    }
    for _ in 0..ET {
        tick_all(&mut nt);
    }
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    assert_eq!(
        nt.peers[&2].state,
        StateRole::Leader,
        "new leader should win after one election timeout of silence"
    );
    assert_eq!(
        nt.peers[&1].state,
        StateRole::Leader,
        "precondition: deposed leader has not noticed yet"
    );

    // A client writes to the new leader. Under upstream #166 this commits at
    // once and the deposed leader answers reads below it.
    nt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
    assert_no_stale_read_in_overlap(&mut nt, stale_committed);
}

/// A leadership transfer must not let a deposed leader serve stale reads.
///
/// `CAMPAIGN_TRANSFER` bypasses the `in_lease` vote filter, so the transferee
/// wins at once: nobody sat out an election timeout of silence first, and the
/// old leader's last lease may have started just before the election. What
/// keeps this safe is that the lease started before any voter for the new
/// leader echoed it (happens-before, no timing), and the new leader's safeguard
/// outwaits one lease duration. This is the path where the old tick-based
/// budget failed, and where a self-lease that installed on stale evidence, or a
/// leader that answered from `committed` without one, would fail now.
///
/// The confirmation shortcut cannot mask it: node 1 is partitioned and never
/// confirms at the new term.
#[test]
fn test_grant_leases_read_not_stale_after_leadership_transfer() {
    let l = default_logger();
    let mut nt = leased_network(ReadOnlyOption::GrantLeases, &l);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    // Let node 1's own safeguard lift so it can commit and serve normally.
    for _ in 0..2 * ET {
        tick_all(&mut nt);
        std::thread::sleep(TICK_SLEEP);
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    let stale_committed = nt.peers[&1].raft_log.committed;
    assert!(
        stale_committed > 1,
        "precondition: the original leader must be past its own safeguard"
    );
    prime_self_lease(&mut nt, 1);

    // Transfer to 2 and cut 1 off before any vote can reach it.
    let in_flight = {
        let n1 = nt.peers.get_mut(&1).unwrap();
        n1.step(new_message(2, 1, MessageType::MsgTransferLeader, 0))
            .expect("step transfer");
        n1.read_messages()
    };
    nt.cut(1, 2);
    nt.cut(1, 3);
    for m in in_flight {
        if m.get_msg_type() == MessageType::MsgTimeoutNow {
            nt.peers
                .get_mut(&m.to)
                .unwrap()
                .step(m)
                .expect("timeout now");
        }
    }
    let votes = nt.peers.get_mut(&2).unwrap().read_messages();
    let votes = nt.filter(votes);
    nt.send(votes);
    assert_eq!(nt.peers[&2].state, StateRole::Leader);
    assert_eq!(
        nt.peers[&1].state,
        StateRole::Leader,
        "precondition: deposed leader has not noticed yet"
    );

    nt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
    assert_no_stale_read_in_overlap(&mut nt, stale_committed);
}

/// A leader whose own removal a successor has applied must not keep serving
/// from its self-lease.
///
/// The successor's safeguard shortcut waits for every node in *its*
/// configuration, and the removed leader is not in it, so the shortcut lifts
/// as soon as the remaining voters confirm (at once, from their votes). The
/// removed leader applies its own removal later, and until it does it still
/// leads in its own eyes. What prevents a stale read is that it gave up its
/// self-lease when it proposed the removal (`ban_self_lease`).
///
/// Driven through a transfer, so no voter sat out an election timeout first.
#[test]
fn test_removed_leader_read_not_stale() {
    let l = default_logger();
    let mut nt = leased_network(ReadOnlyOption::LeaseBased, &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    prime_self_lease(&mut nt, 1);

    // Node 1 proposes its own removal; it commits everywhere.
    let mut cc = ConfChange::default();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.node_id = 1;
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    e.data = protobuf::Message::write_to_bytes(&cc).unwrap().into();
    let mut prop = new_message(1, 1, MessageType::MsgPropose, 0);
    prop.mut_entries().push(e);
    nt.send(vec![prop]);
    let cc_index = nt.peers[&1].raft_log.last_index();
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
    let stale_committed = nt.peers[&1].raft_log.committed;
    assert!(
        stale_committed >= cc_index,
        "the removal should have committed"
    );

    // Node 2 applies it; node 1 has not yet.
    {
        let n2 = nt.peers.get_mut(&2).unwrap();
        assert!(n2.raft_log.committed >= cc_index);
        n2.apply_conf_change(&raft_proto::ConfChangeI::into_v2(cc))
            .expect("apply removal");
        n2.commit_apply(cc_index);
        assert!(!n2.prs().conf().to_conf_state().voters.contains(&1));
    }

    // Transfer to node 2 and cut node 1 off before any vote can reach it.
    let in_flight = {
        let n1 = nt.peers.get_mut(&1).unwrap();
        n1.step(new_message(2, 1, MessageType::MsgTransferLeader, 0))
            .expect("step transfer");
        n1.read_messages()
    };
    nt.cut(1, 2);
    nt.cut(1, 3);
    for m in in_flight {
        if m.get_msg_type() == MessageType::MsgTimeoutNow {
            nt.peers
                .get_mut(&m.to)
                .unwrap()
                .step(m)
                .expect("timeout now");
        }
    }
    let votes = nt.peers.get_mut(&2).unwrap().read_messages();
    let votes = nt.filter(votes);
    nt.send(votes);
    assert_eq!(nt.peers[&2].state, StateRole::Leader);
    assert_eq!(nt.peers[&1].state, StateRole::Leader, "precondition");

    // Node 1 is not in node 2's configuration, so node 2 commits at once.
    nt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
    let new_committed = nt.peers[&2].raft_log.committed;
    assert!(
        new_committed > stale_committed,
        "precondition: the new leader should commit without waiting for node 1"
    );
    if let Some(idx) = read_index_at(&mut nt, 1) {
        assert!(
            idx >= new_committed,
            "removed leader answered a read at index {} while the new leader had committed {}",
            idx,
            new_committed
        );
    }
}
