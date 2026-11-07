// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! EXP-12: a follower keeps renewing its read lease only while it has seen
//! lease use within `read_lease_idle_timeout_micros` (or was built within it):
//! a read of its own, or an append carrying entries (review tack-08 F1).
//! Past that, heartbeats no longer make it ask, its lease lapses and the
//! leader holds no grant for it, so a group with neither reads nor writes can
//! go quiet. The next local read asks again.

use harness::*;
use raft::eraftpb::*;
use raft::*;

use std::time::{Duration, Instant};

use crate::test_util::*;

const LEASE_MICROS: u64 = 50_000;
const IDLE_MICROS: u64 = 250_000;

fn idle_config(id: u64) -> Config {
    Config {
        check_quorum: true,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: LEASE_MICROS,
        max_num_read_leases: 5,
        ask_for_read_lease: id != 1,
        read_lease_catchup_margin: 10,
        read_lease_idle_timeout_micros: IDLE_MICROS,
        ..new_test_config(id, 20, 1)
    }
}

/// Elects 1 and hands out the first grants. Returns the network and the time
/// the followers' lease state was built (their election-time reset).
fn new_network() -> (Network, Instant) {
    let l = default_logger();
    let peers = [1, 2, 3];
    let mk = |id: u64| {
        let storage = new_storage();
        storage.initialize_with_conf_state((peers.to_vec(), vec![]));
        Some(new_test_raft_with_config(&idle_config(id), storage, &l))
    };
    let mut nt = Network::new_with_config(vec![mk(1), mk(2), mk(3)], &idle_config(1), &l);
    // Clear the lease-ask rate limit, measured from process start.
    std::thread::sleep(Duration::from_millis(15));
    let built = Instant::now();
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
    for id in [2, 3] {
        assert!(holds(&mut nt, id), "setup: {id} holds a grant");
    }
    (nt, built)
}

fn holds(nt: &mut Network, id: u64) -> bool {
    nt.peers
        .get_mut(&1)
        .unwrap()
        .read_only
        .has_active_granted_lease(id)
}

fn beat(nt: &mut Network) {
    nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
}

fn sleep_until(t: Instant) {
    let now = Instant::now();
    if t > now {
        std::thread::sleep(t - now);
    }
}

fn read_at(nt: &mut Network, from: u64, at: u64, ctx: &str) {
    nt.send(vec![new_message_with_entries(
        from,
        at,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(ctx))],
    )]);
}

/// A new term's followers renew on heartbeats with no reads at all, until
/// the idle timeout; after it a heartbeat no longer renews, and the lapsed
/// grants are gone. A local read then gets its follower a grant again, and
/// the read itself is answered.
#[test]
fn test_idle_follower_lets_lease_lapse_until_a_read() {
    let (mut nt, built) = new_network();
    let idle = Duration::from_micros(IDLE_MICROS);
    let lease = Duration::from_micros(LEASE_MICROS);

    // Inside the window: an expired lease is renewed by the next heartbeat.
    std::thread::sleep(lease + Duration::from_millis(10));
    for id in [2, 3] {
        assert!(!holds(&mut nt, id), "{id}'s first grant has expired");
    }
    beat(&mut nt);
    for id in [2, 3] {
        assert!(holds(&mut nt, id), "{id} renews on a heartbeat while warm");
    }

    // Past the window, with no read: heartbeats renew nothing.
    sleep_until(built + idle + Duration::from_millis(20));
    beat(&mut nt);
    sleep_until(Instant::now() + lease + Duration::from_millis(10));
    beat(&mut nt);
    for id in [2, 3] {
        assert!(!holds(&mut nt, id), "idle {id} lets its lease lapse");
    }

    // A local read at 2 asks again and is answered; 3 stays idle.
    read_at(&mut nt, 2, 2, "wake");
    assert!(holds(&mut nt, 2), "a read gets 2 a grant again");
    assert!(
        !holds(&mut nt, 3),
        "3 saw no read and stays without a grant"
    );
    let rs = &nt.peers[&2].read_states;
    assert!(
        rs.iter().any(|s| s.request_ctx == b"wake"),
        "the read that woke 2 is answered"
    );

    // That read restarted 2's window: after its lease lapses, a heartbeat
    // renews it.
    std::thread::sleep(lease + Duration::from_millis(10));
    beat(&mut nt);
    assert!(holds(&mut nt, 2), "2 renews on a heartbeat after its read");
    assert!(!holds(&mut nt, 3), "3 still does not");
}

/// A read another node forwards through an idle follower is not that
/// follower's own, so it neither asks for nor renews a lease.
#[test]
fn test_forwarded_read_does_not_wake_idle_follower() {
    let (mut nt, built) = new_network();
    sleep_until(
        built
            + Duration::from_micros(IDLE_MICROS)
            + Duration::from_micros(LEASE_MICROS)
            + Duration::from_millis(20),
    );
    beat(&mut nt);
    assert!(!holds(&mut nt, 2), "setup: 2 is idle without a grant");

    // 3's read, forwarded through 2 (3's leader id points at 2).
    read_at(&mut nt, 3, 2, "via-2");
    assert!(!holds(&mut nt, 2), "a forwarded read does not wake 2");
}

/// A write-only group keeps its holders: appends carrying entries count as
/// lease use, so the followers still renew past the idle timeout with no read
/// at all (review tack-08 F1). Their leases are what the commit clamp and the
/// switch credits act on in a write-only cell.
#[test]
fn test_writes_keep_followers_renewing() {
    let (mut nt, built) = new_network();
    let past_idle = built
        + Duration::from_micros(IDLE_MICROS)
        + Duration::from_micros(LEASE_MICROS)
        + Duration::from_millis(30);
    while Instant::now() < past_idle {
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        beat(&mut nt);
        std::thread::sleep(Duration::from_millis(20));
    }
    beat(&mut nt);
    for id in [2, 3] {
        assert!(
            holds(&mut nt, id),
            "{id} saw writes but no reads, and should still renew"
        );
    }
}
