// Modified by Ryuz Corp (Cello): focused regressions for the cron/wake bound issues (upstream #292, #293, #294).
//! Bound regressions for `cron::next_after` and the wake entry keys.
//!
//! Each test fails on the pre-fix code: `next_after` wrapped past
//! `i64::MAX`, `parse_entry_key` rejected the year-10000 bucket
//! `entry_key` writes, and `L-2W` matched twice in August 2026.

use cello_logic::cron::{matches, next_after, parse};
use cello_logic::wake::{entry_key, migration_seed_key, parse_entry_key, PublicationId};
use cello_logic::Ms;

const ID: PublicationId = PublicationId {
    epoch: 1,
    sequence: 1,
};

/// `cron::next_after` must return `None` at the end of the `i64` range
/// instead of wrapping to a negative timestamp (upstream #292).
#[test]
fn cron_next_after_at_i64_max_is_none() {
    for expression in ["* * * * *", "0 0 29 2 *", "0 0 L-2W * *", "0 0 * * 1"] {
        let cron = parse(expression).unwrap();
        assert_eq!(next_after(&cron, Ms::MAX), None, "{expression}");
    }
}

/// The last representable minute still fires; the minute after it does not.
#[test]
fn cron_next_after_last_representable_minute() {
    let every = parse("* * * * *").unwrap();
    let last_minute = Ms::MAX.div_euclid(60_000);
    let last_ms = last_minute * 60_000;
    assert_eq!(next_after(&every, last_ms - 1), Some(last_ms));
    assert_eq!(next_after(&every, last_ms), None);
    assert_eq!(next_after(&every, Ms::MAX), None);
}

/// Ordinary and boundary schedules keep their behavior.
#[test]
fn cron_ordinary_schedules_unchanged() {
    let every = parse("* * * * *").unwrap();
    assert_eq!(next_after(&every, 0), Some(60_000));
    // 14:30 on 1970-01-01.
    assert_eq!(
        next_after(&parse("30 14 * * *").unwrap(), 0),
        Some(52_200_000)
    );
    // Midnight daily: 1970-01-02.
    let daily = parse("0 0 * * *").unwrap();
    assert_eq!(next_after(&daily, 0), Some(86_400_000));
    assert!(matches(&daily, 86_400_000));
    // Cloudflare numbers Sunday as 1: the first Sunday is 1970-01-04.
    let sunday = parse("0 0 * * 1").unwrap();
    assert_eq!(next_after(&sunday, 0), Some(259_200_000));
    assert!(!matches(&sunday, 86_400_000));
    // The 1st of the month: 1970-02-01.
    assert_eq!(
        next_after(&parse("0 0 1 * *").unwrap(), 0),
        Some(2_678_400_000)
    );
    // Leap day: 1972-02-29.
    assert_eq!(
        next_after(&parse("0 0 29 2 *").unwrap(), 0),
        Some(68_169_600_000)
    );
}

/// `entry_key` for year 10000 must survive `parse_entry_key` (upstream #293).
#[test]
fn wake_entry_key_year_10000_round_trips() {
    let due_ms = 253_402_300_800_000;
    let key = entry_key(due_ms, "scope", ID);
    assert_eq!(key, format!("wake/entries/10000-01-01T00:00/scope/{ID}"));
    let parsed = parse_entry_key(&key).expect("year-10000 key must parse");
    assert_eq!(parsed.minute_ms, due_ms);
    assert_eq!(parsed.cell, "scope");
    assert_eq!(parsed.id, ID);
}

/// Keys round-trip across eras, including floored minutes and the largest
/// representable bucket (a nine-digit year, exercising the wide path).
#[test]
fn wake_entry_key_round_trips_across_eras() {
    let last_ms = Ms::MAX.div_euclid(60_000) * 60_000;
    for due_ms in [
        0,
        60_000,
        1_000_000_000_000,
        253_402_300_799_999,
        253_402_300_800_000,
        -60_000,
        -1_000_000_000_000,
        last_ms,
    ] {
        let floored = due_ms.div_euclid(60_000) * 60_000;
        let key = entry_key(due_ms, "scope", ID);
        let parsed = parse_entry_key(&key).expect("entry_key output must parse");
        assert_eq!(parsed.minute_ms, floored, "{key}");
        assert_eq!(parsed.cell, "scope", "{key}");
        assert_eq!(parsed.id, ID, "{key}");
    }
}

/// Malformed identities and scopes stay rejected.
#[test]
fn wake_invalid_keys_stay_rejected() {
    let id = ID.to_string();
    let bad = [
        String::new(),
        "wake/entries/".to_string(),
        format!("1970-01-01T00:00/scope/{id}"),
        format!("wake/entries/1970-13-01T00:00/scope/{id}"),
        format!("wake/entries/1970-00-10T00:00/scope/{id}"),
        format!("wake/entries/1970-02-30T00:00/scope/{id}"),
        format!("wake/entries/1970-01-01T24:00/scope/{id}"),
        format!("wake/entries/1970-01-01T00:60/scope/{id}"),
        format!("wake/entries/970-01-01T00:00/scope/{id}"),
        format!("wake/entries/+970-01-01T00:00/scope/{id}"),
        format!("wake/entries/1970-1-01T00:00/scope/{id}"),
        format!("wake/entries/1970-01-1T00:00/scope/{id}"),
        format!("wake/entries/1970-01-01 00:00/scope/{id}"),
        format!("wake/entries/1970-01-01T00:00//{id}"),
        format!("wake/entries/1970-01-01T00:00/bad scope/{id}"),
        "wake/entries/1970-01-01T00:00/scope/not-an-id".to_string(),
        "wake/entries/1970-01-01T00:00/scope/0000000000000000-0000000000000001".to_string(),
        format!("wake/entries/1970-01-01T00:00/scope/{id}/extra"),
        format!("wake/entries/10000-01-01T00:00/scope/{id}/extra"),
        format!("wake/entries/10000-13-01T00:00/scope/{id}"),
    ];
    for key in &bad {
        assert_eq!(parse_entry_key(key), None, "{key}");
    }
}

/// The offline migration seed keeps its zero-epoch identity.
#[test]
fn wake_migration_seed_still_parses() {
    let parsed = parse_entry_key(&migration_seed_key("scope")).expect("migration seed must parse");
    assert_eq!(parsed.minute_ms, 0);
    assert_eq!(parsed.cell, "scope");
    assert_eq!(
        parsed.id,
        PublicationId {
            epoch: 0,
            sequence: 0
        }
    );
}

/// `0 0 L-2W * *` names the weekday nearest the 29th — a Saturday in
/// August 2026 — which is Friday the 28th only (upstream #294).
#[test]
fn cron_l2w_matches_once_in_august_2026() {
    let cron = parse("0 0 L-2W * *").unwrap();
    assert!(matches(&cron, 1_787_875_200_000));
    assert!(!matches(&cron, 1_788_134_400_000));
    // Re-arming from inside the August firing skips straight to September:
    // September 2026 ends on the 30th with a Monday 28th nominal date.
    assert_eq!(
        next_after(&cron, 1_787_875_200_000 - 86_400_000),
        Some(1_787_875_200_000)
    );
    assert_eq!(
        next_after(&cron, 1_787_875_200_000),
        Some(1_790_553_600_000)
    );
    // The last-weekday form is unaffected: August 2026 ends on a Monday.
    let last_weekday = parse("0 0 LW * *").unwrap();
    assert!(matches(&last_weekday, 1_788_134_400_000));
    assert!(!matches(&last_weekday, 1_787_875_200_000));
}

/// Every `L-xW` month over a full 400-year Gregorian cycle fires exactly
/// once per month on the weekday nearest the nominal date (offsets 1-7),
/// and never twice for wider offsets; `LW` always names the last weekday.
#[test]
fn cron_last_offset_weekday_single_match_all_layouts() {
    let by_offset: Vec<_> = (1..=30)
        .map(|offset| parse(&format!("0 0 L-{offset}W * *")).unwrap())
        .collect();
    let last_weekday = parse("0 0 LW * *").unwrap();
    // 1970-01-01 was a Thursday; Sunday is weekday 0.
    let mut day: i64 = 0;
    let mut examined = 0;
    for year in 1970..2370 {
        for month in 1..=12u32 {
            let last = month_len(year, month);
            let at_midnight = |dom: u32| (day + i64::from(dom) - 1) * 86_400_000;
            let weekday_of = |dom: u32| (day + i64::from(dom) - 1 + 4).rem_euclid(7) as u32;
            for (index, cron) in by_offset.iter().enumerate() {
                let offset = index as u32 + 1;
                let matched: Vec<u32> = (1..=last)
                    .filter(|dom| matches(cron, at_midnight(*dom)))
                    .collect();
                if offset <= 7 {
                    let nominal = last - offset;
                    let expected = match weekday_of(nominal) {
                        0 => nominal + 1,
                        6 => nominal - 1,
                        _ => nominal,
                    };
                    assert_eq!(matched, vec![expected], "L-{offset}W {year}-{month:02}");
                    examined += 1;
                } else {
                    assert!(
                        matched.len() <= 1,
                        "L-{offset}W {year}-{month:02} fired twice: {matched:?}"
                    );
                }
            }
            let matched: Vec<u32> = (1..=last)
                .filter(|dom| matches(&last_weekday, at_midnight(*dom)))
                .collect();
            let expected = match weekday_of(last) {
                0 => last - 2,
                6 => last - 1,
                _ => last,
            };
            assert_eq!(matched, vec![expected], "LW {year}-{month:02}");
            day += i64::from(last);
        }
    }
    assert!(examined > 30_000);
}

fn month_len(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}
