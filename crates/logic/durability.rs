// Modified by Ryuz Corp (Cello): proof-deadline unit tests (issue #298).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! When a write's durability proof gives up, sans-IO.
//!
//! A write's response waits until the node proves the write durable. The
//! wait has a budget, but the budget measures a stall, not a queue: a node
//! that commits more writes than it can upload at once keeps the excess in a
//! queue behind its upload slots, and a write at the back of that queue is
//! not failing -- it is waiting its turn while the node proves other writes.
//! Measured on one c3-standard-4 (2026-09-01), 4,000 cells that committed in
//! the same second needed about nine seconds to drain through 64 upload
//! slots; a fixed ten-second budget counted from each write's commit failed
//! the last third of them, reset those cells, and re-fired their alarms 30 to
//! 110 seconds late, with the store healthy throughout.
//!
//! The rule here therefore has two regimes. While the upload that covers the
//! write has not started, the wait extends as long as the node lands proofs,
//! up to [`QUEUED_BUDGETS`] budgets in total. Once that upload is in flight,
//! the fixed budget runs from the moment it began, exactly as before, so a
//! stuck upload still fails in one budget. The two together mean a stalled
//! store fails every waiter one budget after its last proof, a stuck cell
//! fails one budget after its capture, and a healthy but busy node fails
//! nobody. Raising the budget instead would move the same false failure to
//! a bigger burst.

/// One write's wait for its proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofWait {
    /// Monotonic ms when the wait began, right after the write took its
    /// ticket.
    pub started_ms: u64,
    /// The durability ticket the write took. A capture whose `capture_seq`
    /// reaches it covers this write.
    pub ticket: u64,
    /// The budget for one proof, in ms (`CELLO_LTX_DURABILITY_TIMEOUT_SECS`).
    pub budget_ms: u64,
}

/// What the node has done since, as the executor observes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofProgress {
    /// Monotonic ms of the last proof any cell on the node landed; 0 when
    /// none has yet.
    pub node_proof_ms: u64,
    /// The highest ticket a capture of this cell has begun for.
    pub capture_seq: u64,
    /// Monotonic ms when the capture that reached `capture_seq` began.
    pub capture_started_ms: u64,
}

/// How many budgets a queued write may wait in total while the node keeps
/// proving other writes. The cap bounds a client's wait under sustained
/// overload, where the queue never empties; six budgets is a minute at the
/// default, and the operation deadline usually ends the wait first.
pub const QUEUED_BUDGETS: u64 = 6;

/// The monotonic ms at which this wait gives up, given what the node has done
/// so far. Recomputed on every wake, so a proof landing elsewhere on the node
/// or the cell's own capture beginning moves the deadline forward.
pub fn proof_deadline(wait: &ProofWait, progress: &ProofProgress) -> u64 {
    if progress.capture_seq >= wait.ticket {
        // In flight: the fixed budget, from the capture that covers this
        // write. A capture cannot cover a ticket taken after it, so the
        // `max` only guards a clock read that raced the ticket.
        return progress
            .capture_started_ms
            .max(wait.started_ms)
            .saturating_add(wait.budget_ms);
    }
    // Queued: one budget after the node's last proof, bounded in total.
    let anchor = progress.node_proof_ms.max(wait.started_ms);
    let cap = wait
        .started_ms
        .saturating_add(wait.budget_ms.saturating_mul(QUEUED_BUDGETS));
    anchor.saturating_add(wait.budget_ms).min(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait(ticket: u64, started_ms: u64, budget_ms: u64) -> ProofWait {
        ProofWait {
            started_ms,
            ticket,
            budget_ms,
        }
    }

    /// While the covering capture has not begun, the deadline rides the
    /// node's last proof: one budget past it, so a healthy but busy node
    /// (proving other cells) never fails a queued write.
    #[test]
    fn queued_deadline_rides_the_node_proof() {
        let wait = wait(7, 1_000, 10_000);
        let progress = ProofProgress {
            node_proof_ms: 5_000,
            capture_seq: 3,
            capture_started_ms: 0,
        };
        assert_eq!(proof_deadline(&wait, &progress), 15_000);
    }

    /// A proof landing anywhere on the node re-anchors a queued wait, even
    /// past its original start budget.
    #[test]
    fn queued_deadline_advances_with_later_proofs() {
        let wait = wait(7, 1_000, 10_000);
        let progress = ProofProgress {
            node_proof_ms: 12_000,
            capture_seq: 3,
            capture_started_ms: 0,
        };
        assert_eq!(proof_deadline(&wait, &progress), 22_000);
    }

    /// The queue is bounded: under sustained overload the wait ends at most
    /// `QUEUED_BUDGETS` past its start, so a stuck queue cannot park a
    /// client past the operation deadline.
    #[test]
    fn queued_wait_is_capped_in_total() {
        let wait = wait(7, 1_000, 10_000);
        let progress = ProofProgress {
            node_proof_ms: 500_000,
            capture_seq: 3,
            capture_started_ms: 0,
        };
        assert_eq!(
            proof_deadline(&wait, &progress),
            1_000 + 10_000 * QUEUED_BUDGETS
        );
    }

    /// Once this cell's capture covering the ticket has begun, the deadline
    /// is fixed from the capture start: a stuck upload still fails in one
    /// budget, and later node proofs no longer extend it.
    #[test]
    fn in_flight_deadline_runs_from_the_covering_capture() {
        let wait = wait(7, 1_000, 10_000);
        let progress = ProofProgress {
            node_proof_ms: 900_000,
            capture_seq: 7,
            capture_started_ms: 4_000,
        };
        assert_eq!(proof_deadline(&wait, &progress), 14_000);
    }

    /// A capture that does not reach the ticket leaves the wait queued: an
    /// older capture is never this write's proof.
    #[test]
    fn stale_capture_keeps_the_wait_queued() {
        let wait = wait(7, 1_000, 10_000);
        let progress = ProofProgress {
            node_proof_ms: 1_000,
            capture_seq: 6,
            capture_started_ms: 9_000,
        };
        assert_eq!(proof_deadline(&wait, &progress), 11_000);
    }
}
