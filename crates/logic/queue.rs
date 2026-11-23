// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Cloudflare Queue policy, reified sans I/O.
//!
//! A queue is one reserved cell. The cell owns SQL and dispatch, while this
//! module owns the values and transitions that another caller must reproduce:
//! the stable address, the public bounds, the alarm deadline, concurrency
//! admission, retry timing, the release of a batch a consumer isolate refused,
//! lease-generation advancement, settlement fencing, purge classification,
//! and deploy-time configuration validation.
//!
//! The lease generation travels in [`PlannedLease`]. A caller cannot select a
//! row and then forget to advance its generation, because this module never
//! returns a bare sequence number for a deliverable row. That prevents a late
//! settlement from an expired lease from acknowledging a newer delivery.

use crate::Ms;

/// The runtime-supplied Durable Object class that stores every queue.
pub const RESERVED_CLASS: &str = "__Queue";

/// Cloudflare measures a Queue kilobyte as 1000 bytes.
pub const MAX_MESSAGE_BYTES: usize = 128_000;
pub const MAX_SEND_BATCH_BYTES: usize = 256_000;
pub const MAX_BATCH_MESSAGES: usize = 100;
pub const MAX_BATCH_TIMEOUT_SECONDS: u16 = 60;
pub const MAX_RETRIES: u16 = 100;
pub const MAX_CONCURRENCY: u16 = 250;
pub const MAX_DELAY_SECONDS: u32 = 86_400;

pub const DEFAULT_MAX_BATCH_SIZE: u16 = 10;
pub const DEFAULT_MAX_BATCH_TIMEOUT_SECONDS: u16 = 5;
pub const DEFAULT_MAX_RETRIES: u16 = 3;

/// Queue retention is deployment-independent in Cello v1. Wrangler has no
/// configuration key for it, so accepting one would make a Cello project
/// invalid on Cloudflare.
pub const RETENTION_MS: Ms = 4 * 24 * 60 * 60 * 1000;

/// The Durable Object name that `getByName` hashes for a queue.
///
/// The identity function is intentional. The queue name is already the
/// fleet-wide resource identity, and prefixing it here would make a project
/// address a different queue from Cloudflare. Both the binding and the
/// operator CLI call this function before the runtime applies the namespace
/// HMAC, so even an identity address has one source.
pub fn cell_name(queue_name: &str) -> &str {
    queue_name
}

/// Choose the one durable alarm from every Queue deadline.
///
/// A deadline already past becomes `now`. SQLite can retain an overdue row
/// across a crash, and installing its old timestamp as a new alarm can leave
/// the wake index behind the clock instead of scheduling immediate work.
pub fn rearm(
    now: Ms,
    batch_deadline: Option<Ms>,
    earliest_visible: Option<Ms>,
    earliest_lease_expiry: Option<Ms>,
    next_sweep: Option<Ms>,
) -> Option<Ms> {
    [
        batch_deadline,
        earliest_visible,
        earliest_lease_expiry,
        next_sweep,
    ]
    .into_iter()
    .flatten()
    .min()
    .map(|deadline| deadline.max(now))
}

/// Resolve a retry deadline from Cloudflare's precedence rule.
///
/// A per-message or per-batch delay wins, including an explicit zero. The
/// configured consumer delay is a fixed default; it does not compound with
/// the attempt number. Cloudflare documents exponential backoff as code an
/// application can implement with `message.attempts`, not as broker behavior.
pub fn retry_at(now: Ms, explicit_seconds: Option<u32>, configured_seconds: Option<u32>) -> Ms {
    let seconds = explicit_seconds.or(configured_seconds).unwrap_or(0);
    now.saturating_add(Ms::from(seconds).saturating_mul(1000))
}

/// Whether a failed current delivery has spent the configured retry count.
///
/// `attempt` is one for the first delivery. `max_retries = 3` therefore allows
/// attempts one through four, and the fourth failure exhausts the message.
pub fn exhausted(attempt: u16, max_retries: u16) -> bool {
    attempt > max_retries
}

/// The durable transition owed when one persisted lease expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiredLease {
    /// The delivery that just failed. The first delivery is attempt one.
    pub attempt: u16,
    pub action: ExpiredLeaseAction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiredLeaseAction {
    RetryAt(Ms),
    Exhausted,
    DeletePurged,
}

/// Spend one failed delivery and decide what replaces its expired lease.
///
/// Purge wins over retry because a purge is irreversible. Otherwise the same
/// fixed consumer delay and retry ceiling used by an explicit settlement
/// apply to a consumer that did not settle before its lease deadline.
pub fn expire_lease(
    now: Ms,
    prior_failures: u16,
    max_retries: u16,
    configured_delay_seconds: Option<u32>,
    purge_on_settle: bool,
) -> ExpiredLease {
    let attempt = prior_failures.saturating_add(1);
    let action = if purge_on_settle {
        ExpiredLeaseAction::DeletePurged
    } else if exhausted(attempt, max_retries) {
        ExpiredLeaseAction::Exhausted
    } else {
        ExpiredLeaseAction::RetryAt(retry_at(now, None, configured_delay_seconds))
    };
    ExpiredLease { attempt, action }
}

/// The delay a batch refused because its isolate was at its `ArrayBuffer`
/// budget waits before its rows are visible again. It doubles per consecutive
/// refusal up to [`REFUSAL_MAX_DELAY_MS`].
pub const REFUSAL_BASE_DELAY_MS: Ms = 1_000;

/// The same ladder for a refusal that means the *node* had no slot, rather
/// than one isolate being full.
///
/// A node refusal is the node shedding load: `place_without_growing` keeps
/// admitting into the isolates it already has, which are the ones that just
/// refused, so retrying soon only adds encode, decode and a durable commit to
/// the condition the node is shedding for. Waiting longer costs nothing a
/// queue cannot afford.
pub const REFUSAL_PRESSURE_BASE_DELAY_MS: Ms = 5_000;

/// The longest delay, for either kind. It is on the order of the old fixed
/// lease (admission + handler + settlement, minutes): a batch parked for that
/// long is no worse off than one waiting for its lease to expire, and a long
/// squeeze stops costing a batch encode per few seconds.
pub const REFUSAL_MAX_DELAY_MS: Ms = 300_000;

/// A refusal gap this long means the pressure is gone: the next refusal starts
/// a fresh streak instead of resuming at the ceiling.
///
/// Longer than one maximum backoff, because the ladder's own last rungs are
/// minutes apart: a smaller gap would restart the streak between two rungs and
/// a sustained squeeze would never reach the charge bound.
pub const REFUSAL_RESET_MS: Ms = REFUSAL_MAX_DELAY_MS + 60_000;

/// Free releases allowed per streak before each further refusal spends a
/// delivery attempt.
///
/// Six is enough for a batch to ride out a collection, a burst that fills one
/// isolate, or a caller that frees its buffers between two alarms. Past that
/// the batch is not being delayed by a moment of pressure, and continuing to
/// release it would let a batch that never fits cycle until retention deletes
/// it: with the ladder below, six releases span 1 + 2 + 4 + 8 + 16 + 32
/// seconds.
pub const REFUSAL_FREE_LIMIT: u16 = 6;

/// ...and the other bound: a streak this old stops being free however few
/// releases it took.
///
/// Sparse refusals (one every 20 seconds, say) would otherwise never reach
/// [`REFUSAL_FREE_LIMIT`] while still never delivering. A minute of continuous
/// refusal is well under the old dead-letter horizon for a default consumer
/// (`max_retries = 3` behind a 316-second lease, ~21 minutes), so a message
/// reaches `max_retries` and its dead-letter queue rather than retention.
pub const REFUSAL_FREE_WINDOW_MS: Ms = 60_000;

/// Which capacity refused the batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalKind {
    /// One isolate's backing-store budget (`CELLO_V8_EXTERNAL_LIMIT_MB`).
    Budget,
    /// The node had no stateless slot, or was shedding.
    Pressure,
}

/// The persisted refusal streak: consecutive refusals of one kind, when the
/// last one happened, and when the streak began.
///
/// It is broker state, not per-message state. One queue has one consumer
/// script, so a refusal says something about that script's isolates rather
/// than about the batch, and a shared streak means one signal of pressure
/// instead of one row update per message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefusalStreak {
    pub count: u16,
    pub kind: RefusalKind,
    pub started_at: Ms,
    pub at: Ms,
}

/// What a refusal owes the refused rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalAction {
    /// Clear the lease and make the rows visible after `delay_ms`. No attempt.
    Release,
    /// Spend a delivery attempt now: the ordinary lease-expiry transition
    /// (attempt, retry delay, dead-letter move) applies.
    ChargeFailure,
}

/// One refusal's outcome: the streak to persist, the backoff it planned, and
/// whether the rows go back to the queue or count as a failed delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub streak: RefusalStreak,
    pub delay_ms: Ms,
    pub action: RefusalAction,
}

/// Plan what a batch its consumer isolate refused owes.
///
/// A refusal is not a delivery: the isolate could not materialize the batch,
/// so no line of the consumer ran and the attempt is not the consumer's to
/// spend. The rows return to ordinary queue state after a delay that grows
/// with consecutive refusals, which keeps a full isolate from being dispatched
/// to in a loop.
///
/// The free release is bounded, because a release with no attempt is only
/// safe while it is *temporary*:
///
/// - a batch whose bodies cannot fit an empty isolate (`unfittable`) never
///   becomes deliverable, so it is charged at once;
/// - after [`REFUSAL_FREE_LIMIT`] consecutive free releases, or
///   [`REFUSAL_FREE_WINDOW_MS`] after the streak began, each further refusal
///   is charged. A message then spends `max_retries` and reaches its
///   dead-letter queue, which is what a queue is for, instead of cycling
///   until retention deletes it;
/// - a success resets the streak (the cell clears it on settlement), so
///   intermittent refusals never ratchet a healthy queue into charging.
///
/// A gap of [`REFUSAL_RESET_MS`] between refusals starts the streak over, and
/// a change of kind does too: a node squeeze that outlasts one isolate's
/// budget is a different episode with a longer ladder.
pub fn refuse_batch(
    now: Ms,
    kind: RefusalKind,
    unfittable: bool,
    prior: Option<RefusalStreak>,
) -> Refusal {
    refuse_batch_with(
        now,
        kind,
        unfittable,
        prior,
        REFUSAL_FREE_LIMIT,
        REFUSAL_FREE_WINDOW_MS,
    )
}

/// [`refuse_batch`] with the free-release bounds given.
fn refuse_batch_with(
    now: Ms,
    kind: RefusalKind,
    unfittable: bool,
    prior: Option<RefusalStreak>,
    free_limit: u16,
    free_window_ms: Ms,
) -> Refusal {
    // A streak younger than the free window continues however slowly the
    // ladder climbs: the delay between two refusals grows to minutes, so a gap
    // test alone would restart the ladder before it ever reaches the bound and
    // the batch would stay free forever. Past the window the streak has
    // already charged, and a quiet gap then starts a fresh episode.
    let continued = matches!(
        prior,
        Some(prior)
            if prior.kind == kind
                && prior.count > 0
                && (now.saturating_sub(prior.started_at) < free_window_ms.max(1)
                    || now.saturating_sub(prior.at) < REFUSAL_RESET_MS)
    );
    let (count, started_at) = match prior {
        Some(prior) if continued => (prior.count.saturating_add(1), prior.started_at),
        _ => (1, now),
    };
    let base = match kind {
        RefusalKind::Budget => REFUSAL_BASE_DELAY_MS,
        RefusalKind::Pressure => REFUSAL_PRESSURE_BASE_DELAY_MS,
    };
    let doublings = u32::from(count.saturating_sub(1)).min(16);
    let delay_ms = base
        .saturating_mul(1i64 << doublings)
        .min(REFUSAL_MAX_DELAY_MS);
    let action =
        if unfittable || count > free_limit || now.saturating_sub(started_at) >= free_window_ms {
            RefusalAction::ChargeFailure
        } else {
            RefusalAction::Release
        };
    Refusal {
        streak: RefusalStreak {
            count,
            kind,
            started_at,
            at: now,
        },
        delay_ms,
        action,
    }
}

/// The fixed lifetime of one persisted Queue lease.
///
/// Admission, handler execution, and settlement each have a runtime-enforced
/// budget. A lease must cover all three or a live consumer can be reclaimed.
/// The broker stores the resulting absolute deadline once, so a restart or a
/// configuration change cannot lengthen an existing lease.
pub fn lease_duration_ms(admission_wait_ms: Ms, handler_budget_ms: Ms, settle_budget_ms: Ms) -> Ms {
    admission_wait_ms
        .saturating_add(handler_budget_ms)
        .saturating_add(settle_budget_ms)
}

/// The row facts needed to plan one lease transaction.
///
/// The SQL query supplies rows in its chosen best-effort delivery order. This
/// function preserves that order and never upgrades it into a FIFO promise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchRow {
    pub seq: i64,
    pub visible_at: Ms,
    pub lease_generation: u64,
    pub leased_until: Option<Ms>,
    /// Purge marks a live lease and lets its consumer finish. The row is
    /// deleted when the lease settles or expires, and is never redelivered.
    pub purge_on_settle: bool,
}

/// A row and the generation the new consumer must settle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlannedLease {
    pub seq: i64,
    pub generation: u64,
    /// True when the previous lease expired. The cell increments the failed
    /// attempt in the same transaction that installs this generation.
    pub reclaimed: bool,
}

/// One message identity captured by a Queue lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseMember<'a> {
    pub seq: i64,
    pub message_id: &'a str,
    pub generation: u64,
}

/// Decide whether a settlement names the complete current lease batch.
///
/// A consumer can retain an old settlement after the broker has re-leased a
/// message. The lease ID selects the candidate rows, and this comparison then
/// fences every member by its message identity, sequence, and generation. A
/// complete one-to-one match also rejects a truncated or duplicated batch.
pub fn settlement_matches(current: &[LeaseMember<'_>], submitted: &[LeaseMember<'_>]) -> bool {
    if current.is_empty() || current.len() != submitted.len() {
        return false;
    }
    if current.iter().enumerate().any(|(index, member)| {
        current[..index]
            .iter()
            .any(|prior| prior.message_id == member.message_id)
    }) {
        return false;
    }

    let mut matched = vec![false; current.len()];
    for submitted in submitted {
        let Some((index, _)) = current.iter().enumerate().find(|(index, current)| {
            !matched[*index]
                && current.message_id == submitted.message_id
                && current.seq == submitted.seq
                && current.generation == submitted.generation
        }) else {
            return false;
        };
        matched[index] = true;
    }
    true
}

/// Decide whether another batch lease fits the configured concurrency bound.
///
/// `active_batches` counts persisted lease IDs, not completed settlements. A
/// consumer batch occupies its slot from lease installation until settlement
/// or expiry removes the lease.
pub fn can_install_lease(active_batches: usize, max_concurrency: u16) -> bool {
    active_batches < usize::from(max_concurrency)
}

/// The lease fact needed to classify one row during a Queue purge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PurgeRow {
    pub seq: i64,
    pub lease_id_present: bool,
    pub leased_until: Option<Ms>,
}

/// All mutations one purge transaction owes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PurgePlan {
    /// Rows with no live lease can be deleted immediately.
    pub delete: Vec<i64>,
    /// Rows with a live lease remain until settlement or expiry.
    pub mark_for_settle: Vec<i64>,
}

/// Partition Queue rows without deleting a row under a live consumer.
pub fn purge_plan(now: Ms, rows: &[PurgeRow]) -> PurgePlan {
    let mut plan = PurgePlan::default();
    for row in rows {
        if row.lease_id_present && row.leased_until.is_some_and(|until| until > now) {
            plan.mark_for_settle.push(row.seq);
        } else {
            plan.delete.push(row.seq);
        }
    }
    plan
}

/// All mutations one batch-planning transaction owes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchPlan {
    pub leases: Vec<PlannedLease>,
    /// Purged rows remain while a live consumer can still settle them. Once
    /// that lease expires, deletion replaces reclamation and redelivery.
    pub delete_purged: Vec<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    GenerationExhausted { seq: i64 },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GenerationExhausted { seq } => {
                write!(f, "queue message {seq} exhausted its lease generation")
            }
        }
    }
}

impl std::error::Error for PlanError {}

/// Select visible, unleased rows and reclaim expired leases under a new
/// generation. A live lease is never returned.
pub fn batch_plan(
    now: Ms,
    rows: &[BatchRow],
    max_batch_size: usize,
) -> Result<BatchPlan, PlanError> {
    let mut plan = BatchPlan::default();
    for row in rows {
        let live_lease = row.leased_until.is_some_and(|until| until > now);
        if row.purge_on_settle {
            if !live_lease {
                plan.delete_purged.push(row.seq);
            }
            continue;
        }
        if plan.leases.len() >= max_batch_size || row.visible_at > now || live_lease {
            continue;
        }
        let generation = row
            .lease_generation
            .checked_add(1)
            .ok_or(PlanError::GenerationExhausted { seq: row.seq })?;
        plan.leases.push(PlannedLease {
            seq: row.seq,
            generation,
            reclaimed: row.leased_until.is_some(),
        });
    }
    Ok(plan)
}

/// The Wrangler Queue values whose bounds cello enforces at deploy time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    pub max_batch_size: u16,
    pub max_batch_timeout_seconds: u16,
    pub max_retries: u16,
    pub max_concurrency: Option<u16>,
    pub delivery_delay_seconds: u32,
    pub retry_delay_seconds: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigError {
    pub field: &'static str,
    pub message: &'static str,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.field, self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Validate the Queue bounds that a bad deployment must report up front.
pub fn validate_config(config: &QueueConfig) -> Result<(), ConfigError> {
    let fail = |field, message| ConfigError { field, message };
    if !(1..=MAX_BATCH_MESSAGES as u16).contains(&config.max_batch_size) {
        return Err(fail("max_batch_size", "must be between 1 and 100"));
    }
    if config.max_batch_timeout_seconds > MAX_BATCH_TIMEOUT_SECONDS {
        return Err(fail(
            "max_batch_timeout",
            "must be between 0 and 60 seconds",
        ));
    }
    if config.max_retries > MAX_RETRIES {
        return Err(fail("max_retries", "must be between 0 and 100"));
    }
    if config
        .max_concurrency
        .is_some_and(|value| !(1..=MAX_CONCURRENCY).contains(&value))
    {
        return Err(fail("max_concurrency", "must be between 1 and 250"));
    }
    if config.delivery_delay_seconds > MAX_DELAY_SECONDS {
        return Err(fail(
            "delivery_delay",
            "must be between 0 and 86400 seconds",
        ));
    }
    if config
        .retry_delay_seconds
        .is_some_and(|value| value > MAX_DELAY_SECONDS)
    {
        return Err(fail("retry_delay", "must be between 0 and 86400 seconds"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: Ms = 1_800_000_000_000;

    fn budget(count: u16, started_at: Ms, at: Ms) -> RefusalStreak {
        RefusalStreak {
            count,
            kind: RefusalKind::Budget,
            started_at,
            at,
        }
    }

    /// The first refusal waits the base delay and releases the batch, so a
    /// released batch is never redelivered in the same instant.
    #[test]
    fn first_refusal_waits_the_base_delay() {
        let refusal = refuse_batch(NOW, RefusalKind::Budget, false, None);
        assert_eq!(refusal.delay_ms, REFUSAL_BASE_DELAY_MS);
        assert_eq!(refusal.action, RefusalAction::Release);
        assert_eq!(refusal.streak.count, 1);
        assert_eq!(refusal.streak.started_at, NOW);
    }

    /// Consecutive refusals double the wait and keep growing past the old
    /// five-second ceiling, so a long squeeze costs a batch encode per
    /// minutes rather than per seconds.
    #[test]
    fn consecutive_refusals_double_to_the_ceiling() {
        let mut streak = None;
        let mut delays = Vec::new();
        for step in 0..12 {
            let refusal = refuse_batch(NOW + step, RefusalKind::Budget, false, streak);
            delays.push(refusal.delay_ms);
            streak = Some(refusal.streak);
        }
        assert_eq!(&delays[..6], &[1_000, 2_000, 4_000, 8_000, 16_000, 32_000]);
        assert!(delays.contains(&REFUSAL_MAX_DELAY_MS), "{delays:?}");
        assert_eq!(*delays.last().expect("delays"), REFUSAL_MAX_DELAY_MS);
        assert!(delays.iter().all(|delay| *delay <= REFUSAL_MAX_DELAY_MS));
    }

    /// A node refusal gets the longer ladder: retrying into a shedding node
    /// only adds work to the pressure.
    #[test]
    fn pressure_waits_longer_than_a_budget_squeeze() {
        let pressure = refuse_batch(NOW, RefusalKind::Pressure, false, None);
        assert_eq!(pressure.delay_ms, REFUSAL_PRESSURE_BASE_DELAY_MS);
        assert!(
            pressure.delay_ms > REFUSAL_BASE_DELAY_MS,
            "a node refusal must not reuse the per-isolate ladder"
        );
        assert_eq!(pressure.action, RefusalAction::Release);
    }

    /// A different kind is a different episode: the per-isolate ladder must not
    /// carry over into a node squeeze.
    #[test]
    fn a_kind_change_starts_the_streak_over() {
        let prior = budget(4, NOW, NOW);
        let refusal = refuse_batch(NOW + 1, RefusalKind::Pressure, false, Some(prior));
        assert_eq!(refusal.streak.count, 1);
        assert_eq!(refusal.streak.kind, RefusalKind::Pressure);
        assert_eq!(refusal.delay_ms, REFUSAL_PRESSURE_BASE_DELAY_MS);
    }

    /// The free releases are bounded by the count: past the limit every
    /// refusal spends an attempt, so a batch that never fits reaches its
    /// dead-letter queue instead of retention.
    #[test]
    fn a_long_streak_charges_the_attempt() {
        let at_limit = refuse_batch(
            NOW + 100,
            RefusalKind::Budget,
            false,
            Some(budget(REFUSAL_FREE_LIMIT, NOW, NOW)),
        );
        assert_eq!(at_limit.streak.count, REFUSAL_FREE_LIMIT + 1);
        assert_eq!(at_limit.action, RefusalAction::ChargeFailure);
    }

    /// ...and by elapsed time, so sparse refusals cannot stay free for a day.
    #[test]
    fn an_old_streak_charges_the_attempt() {
        let refusal = refuse_batch(
            NOW + REFUSAL_FREE_WINDOW_MS,
            RefusalKind::Budget,
            false,
            Some(budget(2, NOW, NOW + REFUSAL_FREE_WINDOW_MS - 1_000)),
        );
        assert_eq!(refusal.streak.count, 3);
        assert_eq!(refusal.action, RefusalAction::ChargeFailure);
    }

    /// A batch that cannot fit an empty isolate is charged at once: no
    /// collection will ever make room.
    #[test]
    fn an_unfittable_batch_is_charged_at_once() {
        let refusal = refuse_batch(NOW, RefusalKind::Budget, true, None);
        assert_eq!(refusal.action, RefusalAction::ChargeFailure);
        assert_eq!(refusal.streak.count, 1);
    }

    /// A success clears the streak (the cell drops it on settlement), so the
    /// next refusal is free again.
    #[test]
    fn a_cleared_streak_starts_free() {
        assert_eq!(
            refuse_batch(NOW, RefusalKind::Budget, false, None).action,
            RefusalAction::Release
        );
        let refusal = refuse_batch(NOW + 1, RefusalKind::Budget, false, None);
        assert_eq!(refusal.streak.count, 1);
        assert_eq!(refusal.delay_ms, REFUSAL_BASE_DELAY_MS);
        assert_eq!(refusal.action, RefusalAction::Release);
    }

    /// A quiet spell after the free window starts the streak over: a stale
    /// ceiling must not charge a fresh, isolated refusal.
    #[test]
    fn a_gap_after_the_window_starts_the_streak_over() {
        let started = NOW;
        let refusal = refuse_batch(
            NOW + REFUSAL_FREE_WINDOW_MS + REFUSAL_RESET_MS,
            RefusalKind::Budget,
            false,
            Some(budget(40, started, NOW)),
        );
        assert_eq!(refusal.streak.count, 1);
        assert_eq!(
            refusal.streak.started_at,
            NOW + REFUSAL_FREE_WINDOW_MS + REFUSAL_RESET_MS
        );
        assert_eq!(refusal.delay_ms, REFUSAL_BASE_DELAY_MS);
        assert_eq!(refusal.action, RefusalAction::Release);
    }

    /// The ladder's long rungs do not restart the streak: a 32-second gap is
    /// still the same squeeze, and the window bound fires on schedule.
    #[test]
    fn a_long_ladder_rung_does_not_restart_the_streak() {
        let prior = budget(5, NOW, NOW + 31_000);
        let refusal = refuse_batch(NOW + 63_000, RefusalKind::Budget, false, Some(prior));
        assert_eq!(refusal.streak.count, 6);
        assert_eq!(refusal.action, RefusalAction::ChargeFailure);
    }

    /// A zero-count streak is not a streak: it must not start at the second
    /// rung of the ladder.
    #[test]
    fn a_zero_streak_is_a_first_refusal() {
        let refusal = refuse_batch(NOW, RefusalKind::Budget, false, Some(budget(0, NOW, NOW)));
        assert_eq!(refusal.streak.count, 1);
        assert_eq!(refusal.delay_ms, REFUSAL_BASE_DELAY_MS);
    }

    /// A saturated counter keeps the ceiling and the charge: it cannot wrap
    /// back into free releases.
    #[test]
    fn a_saturated_streak_charges() {
        let refusal = refuse_batch(
            NOW + 1,
            RefusalKind::Budget,
            false,
            Some(budget(u16::MAX, NOW, NOW)),
        );
        assert_eq!(refusal.streak.count, u16::MAX);
        assert_eq!(refusal.delay_ms, REFUSAL_MAX_DELAY_MS);
        assert_eq!(refusal.action, RefusalAction::ChargeFailure);
    }
}
