// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Coverage for the extra index requirement a `MsgReadIndex` can carry in its
//! `commit` field, with its term in `log_term` (`RawNode::read_index_switch_hint`),
//! and the follower serve gate's handling of it. Rust twin of the Go fork's
//! `read_only_switch_hint_test.go`.
//!
//! The contract is deliberately index-shaped and knows nothing about switches:
//! `commit` is an extra index this node must have committed before it may serve
//! the read from its own lease, and 0 is the only reserved value, meaning "no
//! extra requirement". A caller that must not be served locally at all says so
//! in the same vocabulary, by naming an index this node cannot reach. A nonzero
//! `commit` counts only in the term (mod 2^32) given in `log_term`; in any other
//! term it is treated as unreachable (SAFE-1).
//!
//! These live in the harness rather than in `read_only.rs` because the decision
//! is made in `Raft::serve_read_index_with_lease`, which needs a whole node
//! (role, leader id, log, lease) rather than a bare `ReadOnly`.

use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;

use crate::test_util::*;

/// Every entry up to here is present and committed on the test follower.
const COMMITTED: u64 = 10;
/// Comfortably above `LEASE_RENEWAL_THRESHOLD_MICROS` (150 ms), so stepping a
/// read does not also emit a `MsgAskReadLease` and disturb the message counts.
const LEASE_DURATION_MICROS: u64 = 500_000;

/// A follower of leader 2 at term 1 whose log is fully committed at index
/// `COMMITTED`, holding a live read lease granted at log index 5 and having
/// acked up to `acked_index`.
fn switch_hint_node(acked_index: u64, l: &slog::Logger) -> RawNode<MemStorage> {
    switch_hint_node_at(acked_index, 1, l)
}

/// `switch_hint_node`, at `term`.
fn switch_hint_node_at(acked_index: u64, term: u64, l: &slog::Logger) -> RawNode<MemStorage> {
    let mut node = leaseless_follower_at(term, l);
    install_lease(&mut node, acked_index);
    node
}

/// The same follower, before any lease is granted. `ReadLeaseMap::leases` is
/// `pub(crate)`, so the no-lease case has to be built rather than torn down.
fn leaseless_follower(l: &slog::Logger) -> RawNode<MemStorage> {
    leaseless_follower_at(1, l)
}

/// `leaseless_follower`, at `term`.
fn leaseless_follower_at(term: u64, l: &slog::Logger) -> RawNode<MemStorage> {
    let storage = new_storage();
    storage.initialize_with_conf_state((vec![1, 2, 3], vec![]));
    let ents: Vec<Entry> = (1..=COMMITTED)
        .map(|i| {
            let mut e = Entry::default();
            e.term = 1;
            e.index = i;
            e
        })
        .collect();
    storage.wl().append(&ents).expect("append to storage");

    let cfg = Config {
        check_quorum: true,
        read_only_option: ReadOnlyOption::GrantLeases,
        read_lease_duration_micros: LEASE_DURATION_MICROS,
        max_num_read_leases: 5,
        ask_for_read_lease: true,
        read_lease_catchup_margin: 10,
        ..new_test_config(1, 10, 1)
    };
    let mut node = RawNode::new(&cfg, storage, l).expect("build RawNode");
    node.raft.become_follower(term, 2);
    node.raft.raft_log.commit_to(COMMITTED);
    node
}

/// Installs a live lease through the real grant path: mint the pending ask
/// (whose local start time `process_granted_lease` substitutes for the
/// leader's), then step the leader's `MsgAskReadLeaseResp` carrying the
/// 48-byte lease. The granted `acked_index` is maxed into this node's own
/// `read_lease_acked_index`, which is the value the serve gate consults.
fn install_lease(node: &mut RawNode<MemStorage>, acked_index: u64) {
    let lease_id = node.raft.read_only.get_marked_lease_id();
    let mut resp = new_message(2, 1, MessageType::MsgAskReadLeaseResp, 0);
    let mut e = Entry::default();
    e.data = marshal_lease(lease_id, 1, 5, LEASE_DURATION_MICROS, acked_index).into();
    resp.set_entries(vec![e].into());
    node.step(resp).expect("step MsgAskReadLeaseResp");
    assert_eq!(
        node.raft.read_only.read_lease_acked_index(),
        acked_index,
        "lease should have installed"
    );
}

/// The 48-byte `ReadLease` wire form: six big-endian u64s,
/// `[lease_id, node_id, log_index, start_time, duration, acked_index]`.
/// Hand-rolled because `ReadLease` is crate-private; keep in sync with
/// `read_only.rs::ReadLease::marshal`.
fn marshal_lease(
    lease_id: u64,
    node_id: u64,
    log_index: u64,
    duration: u64,
    acked_index: u64,
) -> Vec<u8> {
    // `start_time` is ignored on the follower — `process_granted_lease`
    // substitutes the ask time it recorded in `get_marked_lease_id`.
    [lease_id, node_id, log_index, 0, duration, acked_index]
        .iter()
        .flat_map(|v| v.to_be_bytes())
        .collect()
}

/// Steps a read hinted in this node's own term.
fn step_hinted_read(node: &mut RawNode<MemStorage>, hint: u64) {
    let term = node.raft.term;
    step_hinted_read_at_term(node, hint, term);
}

/// Steps a locally issued read (`from` unset, as
/// `RawNode::read_index_switch_hint` makes one): only those are served from
/// the lease.
fn step_hinted_read_at_term(node: &mut RawNode<MemStorage>, hint: u64, hint_term: u64) {
    let mut m = new_message(0, 1, MessageType::MsgReadIndex, 0);
    m.commit = hint;
    m.log_term = hint_term;
    let mut e = Entry::default();
    e.data = b"ctx".to_vec().into();
    m.set_entries(vec![e].into());
    node.step(m).expect("step MsgReadIndex");
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Served,
    Held,
    Forwarded,
}

fn outcome(node: &mut RawNode<MemStorage>) -> Outcome {
    let msgs: Vec<Message> = node.raft.msgs.drain(..).collect();
    let held = node.next_read_index_delay_micros().is_some();
    let served = node.raft.read_states.drain(..).count();
    match (msgs.len(), held) {
        (0, true) if served == 0 => Outcome::Held,
        (0, false) if served == 1 => Outcome::Served,
        (1, false)
            if served == 0
                && msgs[0].get_msg_type() == MessageType::MsgReadIndex
                && msgs[0].to == node.raft.leader_id =>
        {
            Outcome::Forwarded
        }
        _ => panic!(
            "unexpected outcome: {} msgs ({:?}), held={}, served={}",
            msgs.len(),
            msgs,
            held,
            served
        ),
    }
}

/// The serve/hold/forward decision of the follower read-index gate across the
/// range of `MsgReadIndex.commit` values.
///
/// The case that matters for safety is the unreachable hint. TiKV's assist mode
/// supplies one when it holds no switch index for the region: a switch that
/// reboots puts its register back to 0 while the leader's commit clamp is still
/// lifted by the high value that register previously reflected, so serving on
/// the follower's own acked index alone can return a read below a committed
/// write. raft is told none of that — only that the read must not be served
/// here — and its ordinary rules do the rest.
#[test]
fn test_switch_hint_gate() {
    let l = default_logger();
    // (name, hint, acked_index, expected)
    let cases: &[(&str, u64, u64, Outcome)] = &[
        // No extra requirement. This is the ordinary case outside assist
        // (noassist / grant-leases have no switch to hint with, so `commit` is
        // legitimately always 0), so it must stay servable; only this node's own
        // acked index gates.
        ("no hint", 0, 5, Outcome::Served),
        ("no hint, acked ahead of committed", 0, 20, Outcome::Held),
        // A real index, as assist supplies from the switch's saved value.
        ("hint below committed", 5, 5, Outcome::Served),
        ("hint equal to committed", COMMITTED, 5, Outcome::Served),
        ("hint just ahead of committed", 20, 5, Outcome::Held),
        ("hint far ahead of committed", 5000, 5, Outcome::Forwarded),
        // The unreachable index a caller uses to mean "do not serve locally".
        // It must forward in both directions of the acked-index comparison, and
        // must never be held (see `test_switch_hint_unreachable_is_not_held`).
        ("unreachable hint", u64::MAX, 5, Outcome::Forwarded),
        (
            "unreachable hint, acked ahead of committed",
            u64::MAX,
            20,
            Outcome::Forwarded,
        ),
    ];

    for (name, hint, acked_index, want) in cases {
        let mut node = switch_hint_node(*acked_index, &l);
        step_hinted_read(&mut node, *hint);
        assert_eq!(&outcome(&mut node), want, "case {}", name);
    }
}

/// A hint counts only in the term whose leader wrote the switch register it
/// came from (SAFE-1). A hint from any other term is treated exactly like
/// `u64::MAX`: forwarded, even one that would otherwise be served, and never
/// held, even within the hold gap. The term is compared modulo 2^32, the width
/// the switch carries. Twin of Go's `TestSwitchHintTermGate`.
#[test]
fn test_switch_hint_term_gate() {
    const WRAP: u64 = 1 << 32;
    let l = default_logger();
    // Go's term 1 (so "earlier" is 0), a later one, and one past 2^32, where
    // the switch's stamp is only the low half.
    for term in [1u64, 7, WRAP + 7] {
        // (name, hint, hint_term, acked_index, expected)
        let cases: &[(&str, u64, u64, u64, Outcome)] = &[
            ("hint at this term", 5, term, 5, Outcome::Served),
            (
                "hint at this term mod 2^32",
                5,
                term + WRAP,
                5,
                Outcome::Served,
            ),
            (
                "hint at this term's low 32 bits",
                5,
                term as u32 as u64,
                5,
                Outcome::Served,
            ),
            (
                "hint from an earlier term",
                5,
                term - 1,
                5,
                Outcome::Forwarded,
            ),
            ("hint from a later term", 5, term + 1, 5, Outcome::Forwarded),
            ("no hint ignores the term", 0, term + 1, 5, Outcome::Served),
            // Never held: neither for its own gap nor for the acked index's.
            (
                "hint in the hold gap at this term",
                20,
                term,
                5,
                Outcome::Held,
            ),
            (
                "hint in the hold gap from a later term",
                20,
                term + 1,
                5,
                Outcome::Forwarded,
            ),
            (
                "acked ahead, hint from an earlier term",
                5,
                term - 1,
                20,
                Outcome::Forwarded,
            ),
            ("acked ahead, no hint", 0, term + 1, 20, Outcome::Held),
        ];
        for (name, hint, hint_term, acked_index, want) in cases {
            let mut node = switch_hint_node_at(*acked_index, term, &l);
            step_hinted_read_at_term(&mut node, *hint, *hint_term);
            assert_eq!(&outcome(&mut node), want, "term {}: case {}", term, name);
        }
    }

    // Forwarded as unreachable, like Go: a node with a stale leader id that
    // receives it cannot use the hint in its own term either.
    let mut node = switch_hint_node_at(5, 7, &l);
    step_hinted_read_at_term(&mut node, 5, 6);
    let msgs: Vec<Message> = node.raft.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 1, "expected one forward, got {:?}", msgs);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgReadIndex);
    assert_eq!(msgs[0].commit, u64::MAX);
}

/// Pins the hole an unreachable hint closes, and the reason it may not be held.
/// Holding it would be worse than useless: the delayer waits for this node's
/// commit index to reach `max(hint, acked_index)`, which no commit can satisfy,
/// so the read would sit out its hold and then be re-stepped with `m.index`
/// set — and the `m.index` disjunct of the serve gate would serve it, having
/// never met the requirement at all.
///
/// The gap arithmetic is what keeps it out of the delayer, so this also pins
/// that `max(hint, acked_index) - committed` does not wrap or panic: raft-rs
/// uses `checked_sub`, and a debug build would panic on a bare `-`.
#[test]
fn test_switch_hint_unreachable_is_not_held() {
    let l = default_logger();
    for acked_index in [5u64, 20] {
        // below and above committed
        let mut node = switch_hint_node(acked_index, &l);
        step_hinted_read(&mut node, u64::MAX);

        assert!(
            node.next_read_index_delay_micros().is_none(),
            "acked {}: unreachable hint was held by the delayer; it must be forwarded",
            acked_index
        );
        assert!(
            node.raft.read_states.is_empty(),
            "acked {}: unreachable hint was served locally; it must be forwarded",
            acked_index
        );

        let msgs: Vec<Message> = node.raft.msgs.drain(..).collect();
        assert_eq!(
            msgs.len(),
            1,
            "acked {}: expected one message, got {:?}",
            acked_index,
            msgs
        );
        assert_eq!(msgs[0].get_msg_type(), MessageType::MsgReadIndex);
        assert_eq!(msgs[0].to, 2);
        // The hint rides along to the leader untouched. That is fine, and is why
        // no reserved value has to be understood outside this gate: the leader's
        // `MsgReadIndex` handler never reads `commit`.
        assert_eq!(
            msgs[0].commit,
            u64::MAX,
            "acked {}: forwarded commit must be preserved",
            acked_index
        );
    }
}

/// The other forwarding path: with no live lease the gate never looks at
/// `commit` at all, so an unreachable hint changes nothing. (A
/// `MsgAskReadLease` rides along here, since a follower with no lease asks for
/// one on every read it cannot serve.)
#[test]
fn test_switch_hint_unreachable_without_lease() {
    let l = default_logger();
    let mut node = leaseless_follower(&l);
    step_hinted_read(&mut node, u64::MAX);

    let msgs: Vec<Message> = node.raft.msgs.drain(..).collect();
    let forwarded = msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgReadIndex && m.to == 2)
        .count();
    assert_eq!(
        forwarded, 1,
        "expected one forward to the leader, got {:?}",
        msgs
    );
    assert!(
        node.next_read_index_delay_micros().is_none(),
        "unreachable hint was held by the delayer"
    );
}

/// `ReadState::served_by_follower_lease` distinguishes the two ways a follower
/// read acquires an index, which are otherwise indistinguishable to the wrapping
/// DB: served from this node's own lease, or answered by the leader. TiKV keys
/// its assist serve gate on it — only a lease-served index needs the switch's
/// answer as a floor; a leader-answered one is already linearizable.
///
/// Both halves are driven through the real paths: a locally-issued read (as
/// `RawNode::read_index_switch_hint` makes one, `from` unset) served under the
/// lease, and the same read forwarded (by an unreachable hint) and then answered
/// by the leader.
#[test]
fn test_read_state_records_whether_the_lease_served_it() {
    let l = default_logger();

    // Served locally: a hint at or below committed, lease live.
    let mut node = switch_hint_node(5, &l);
    let mut m = new_message(0, 1, MessageType::MsgReadIndex, 0);
    m.commit = 5;
    m.log_term = node.raft.term;
    let mut e = Entry::default();
    e.data = b"local".to_vec().into();
    m.set_entries(vec![e].into());
    node.step(m).expect("step local MsgReadIndex");
    assert_eq!(node.raft.read_states.len(), 1, "read should be served here");
    assert!(
        node.raft.read_states[0].served_by_follower_lease,
        "a lease-served index must be marked as such"
    );

    // Forwarded (unreachable hint), then answered by the leader's
    // MsgReadIndexResp.
    let mut node = switch_hint_node(5, &l);
    let mut m = new_message(0, 1, MessageType::MsgReadIndex, 0);
    m.commit = u64::MAX;
    let mut e = Entry::default();
    e.data = b"forwarded".to_vec().into();
    m.set_entries(vec![e].into());
    node.step(m).expect("step local MsgReadIndex");
    assert!(
        node.raft.read_states.is_empty(),
        "an unreachable hint must not be served locally"
    );
    let mut resp = new_message(2, 1, MessageType::MsgReadIndexResp, 0);
    resp.index = COMMITTED;
    let mut e = Entry::default();
    e.data = b"forwarded".to_vec().into();
    resp.set_entries(vec![e].into());
    node.step(resp).expect("step MsgReadIndexResp");
    assert_eq!(node.raft.read_states.len(), 1);
    assert!(
        !node.raft.read_states[0].served_by_follower_lease,
        "a leader-answered index must not be marked lease-served"
    );
}

/// Steps a locally issued read (`from` unset, as
/// `RawNode::read_index_switch_hint` makes one).
fn step_local_read(node: &mut RawNode<MemStorage>, ctx: &[u8], hint: u64, hint_term: u64) {
    let mut m = new_message(0, 1, MessageType::MsgReadIndex, 0);
    m.commit = hint;
    m.log_term = hint_term;
    let mut e = Entry::default();
    e.data = ctx.to_vec().into();
    m.set_entries(vec![e].into());
    node.step(m).expect("step local MsgReadIndex");
}

/// A follower serves only its own reads from its lease (SAFE-5). A read another
/// node forwarded here (that node's `leader_id` is stale: this node led the
/// term it was sent in, and now follows a later leader) is forwarded on to
/// this node's leader, `from` intact, so the leader answers the requester
/// directly. Served here, the answer would reach the requester as a plain
/// `MsgReadIndexResp`, recorded with `served_by_follower_lease = false`, and
/// TiKV would skip its switch floor for it.
///
/// The replay case is the one the SAFE-1 hint-term gate does not stop: a read
/// the requester held and then gave up on arrives with `index` set and
/// `reject`, and the `m.index` disjunct of the serve gate would admit it
/// without consulting this node's acked index or the hint.
#[test]
fn test_remote_read_is_not_served_from_follower_lease() {
    const TERM: u64 = 3;
    const REQUESTER: u64 = 3;
    let l = default_logger();
    // (name, acked_index, commit, log_term, index, reject)
    let cases: &[(&str, u64, u64, u64, u64, bool)] = &[
        ("no hint", 5, 0, 0, 0, false),
        ("no hint, acked ahead of committed", 20, 0, 0, 0, false),
        ("hint at this term", 5, 5, TERM, 0, false),
        (
            "give-up replay from an earlier term",
            5,
            5,
            TERM - 1,
            5,
            true,
        ),
        // Served before the fix only by the `m.index` disjunct.
        (
            "give-up replay, acked ahead of committed",
            20,
            0,
            0,
            5,
            true,
        ),
    ];
    let mut failed = Vec::new();
    for (name, acked_index, commit, log_term, index, reject) in cases {
        let mut node = switch_hint_node_at(*acked_index, TERM, &l);
        let mut m = new_message(REQUESTER, 1, MessageType::MsgReadIndex, 0);
        m.commit = *commit;
        m.log_term = *log_term;
        m.index = *index;
        m.reject = *reject;
        let mut e = Entry::default();
        e.data = b"remote".to_vec().into();
        m.set_entries(vec![e].into());
        node.step(m).expect("step remote MsgReadIndex");

        let msgs: Vec<Message> = node.raft.msgs.drain(..).collect();
        let forwarded = node.raft.read_states.is_empty()
            && node.next_read_index_delay_micros().is_none()
            && msgs.len() == 1
            && msgs[0].get_msg_type() == MessageType::MsgReadIndex
            && msgs[0].to == 2
            // So the leader answers the requester.
            && msgs[0].from == REQUESTER;
        if !forwarded {
            failed.push(format!(
                "{}: held={} read_states={} msgs={:?}",
                name,
                node.next_read_index_delay_micros().is_some(),
                node.raft.read_states.len(),
                msgs
            ));
        }
    }
    assert!(
        failed.is_empty(),
        "remote reads not forwarded to the leader:\n{}",
        failed.join("\n")
    );
}

/// The other side of SAFE-5: a read issued through the `RawNode` entry points
/// TiKV uses (`read_index_switch_hint`, `read_index`, and `read_index_quorum`,
/// which is an ordinary read off a leader) arrives with `from` unset, so the
/// lease-holding follower serves it from its own lease rather than forwarding
/// it as another node's read.
#[test]
fn test_raw_node_reads_are_served_from_follower_lease() {
    let l = default_logger();
    let reads: &[(&str, fn(&mut RawNode<MemStorage>))] = &[
        ("read_index_switch_hint", |node| {
            let term = node.raft.term;
            node.read_index_switch_hint(b"api".to_vec(), 5, term)
        }),
        ("read_index", |node| node.read_index(b"api".to_vec())),
        ("read_index_quorum", |node| {
            node.read_index_quorum(b"api".to_vec())
        }),
    ];
    for (name, read) in reads {
        let mut node = switch_hint_node(5, &l);
        read(&mut node);
        let msgs: Vec<Message> = node.raft.msgs.drain(..).collect();
        assert!(
            msgs.is_empty(),
            "{}: a local read sent messages: {:?}",
            name,
            msgs
        );
        assert_eq!(node.raft.read_states.len(), 1, "{}: not served here", name);
        assert!(
            node.raft.read_states[0].served_by_follower_lease,
            "{}: not served from the follower lease",
            name
        );
        assert_eq!(node.raft.read_states[0].index, COMMITTED, "{}", name);
    }
}

/// `ReadState::term` is the term a follower read's index was produced in, on
/// both paths: served from this node's lease, and answered by the leader. TiKV
/// applies a read's switch stamp as a floor only if the stamp's term equals it
/// (SAFE-1).
#[test]
fn test_read_state_records_its_term() {
    const TERM: u64 = 3;
    let l = default_logger();

    // Served from the lease.
    let mut node = switch_hint_node_at(5, TERM, &l);
    step_local_read(&mut node, b"local", 5, TERM);
    assert_eq!(node.raft.read_states.len(), 1, "read should be served here");
    assert!(node.raft.read_states[0].served_by_follower_lease);
    assert_eq!(node.raft.read_states[0].term, TERM);

    // A later term's lease serves in that term.
    node.raft.read_states.clear();
    node.raft.become_follower(TERM + 1, 2);
    install_lease(&mut node, 5);
    step_local_read(&mut node, b"next", 5, TERM + 1);
    assert_eq!(node.raft.read_states.len(), 1, "read should be served here");
    assert!(node.raft.read_states[0].served_by_follower_lease);
    assert_eq!(node.raft.read_states[0].term, TERM + 1);

    // Forwarded (a hint from the previous term), then answered by the
    // leader's MsgReadIndexResp at its term.
    let mut node = switch_hint_node_at(5, TERM, &l);
    step_local_read(&mut node, b"forwarded", 5, TERM - 1);
    assert!(
        node.raft.read_states.is_empty(),
        "a hint from another term must not be served locally"
    );
    let mut resp = new_message(2, 1, MessageType::MsgReadIndexResp, 0);
    resp.term = TERM;
    resp.index = COMMITTED;
    let mut e = Entry::default();
    e.data = b"forwarded".to_vec().into();
    resp.set_entries(vec![e].into());
    node.step(resp).expect("step MsgReadIndexResp");
    assert_eq!(node.raft.read_states.len(), 1);
    assert!(!node.raft.read_states[0].served_by_follower_lease);
    assert_eq!(node.raft.read_states[0].term, TERM);
}
