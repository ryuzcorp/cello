// Modified by Ryuz Corp (Cello): cross-isolate RPC stubs and streams; operator token for the internal listener; per-node Workers Cache API; tenant egress policy; the full build version for --version; staged invoke modules; the hardening module; nodejs process.env population flag (upstream #296); shared TLS roots module (upstream #230).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

//! Effect adapters for the clean-sheet core.
//!
//! The executable owns one serial actor which is the only caller of
//! `cello_logic::on_event`. Adapter futures never borrow core state; they send
//! versioned completion events back through its mailbox.

mod tokio_select;
#[doc(hidden)]
pub use tokio_select::__asyncrt_select_support;

pub mod actor;
#[cfg(all(test, cello_internal_tests))]
mod conformance_core_loop_tests {
    include!(env!("CELLO_CONFORMANCE_CORE_LOOP_TESTS"));
}
#[cfg(all(test, cello_internal_tests))]
mod conformance_facet_failure_tests {
    include!(env!("CELLO_CONFORMANCE_FACET_FAILURE_TESTS"));
}
pub mod assets;
#[cfg(not(cello_internal_tests))]
pub mod asyncrt;
// The internal test flag alone selects the simulated asyncrt, so an external
// test harness built with the flag sees the same simulated world the
// in-crate suites see. `test` must not be part of the gate:
// a dependency never has it, and the harness binary builds unflagged,
// so the shipped and harness builds keep the real asyncrt.
#[cfg(cello_internal_tests)]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
pub mod asyncrt {
    include!(env!("CELLO_INTERNAL_ASYNCRT"));
}
#[cfg(all(test, cello_internal_tests))]
mod asyncrt_contract_tests {
    include!(env!("CELLO_INTERNAL_ASYNCRT_TESTS"));
}
pub mod bucket;
pub mod cell_cli;
pub mod cell_dispatch;
mod cell_host;
pub mod cell_runtime;
pub mod clean_reload;
pub mod cli_options;
pub mod cli_output;
pub mod container;
pub mod control_plane;
pub mod d1_cli;
pub mod dead_node_gc;
pub mod deploy;
pub mod dev;
pub mod docker;
pub mod drain_token;
pub mod egress;
pub mod engine_api;
pub mod env_vars;
#[cfg(cello_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod fault {
    include!(env!("CELLO_INTERNAL_SQLITE_FAULT"));
}
pub(crate) mod facet_streams;
pub mod fleet;
pub mod generation;
pub mod hardening;
pub mod host_channels;
pub mod host_services;
pub mod http_streams;
pub mod invoke_cli;
pub mod isolate_pipe;
pub mod js;
pub mod kv_blob;
pub mod kv_cli;
pub(crate) mod local_store;
pub mod ltx_repl;
pub mod ltx_replication;
pub mod machine;
pub mod memory;
pub mod node_log;
pub(crate) mod operator_cell;
pub mod operator_token;
#[doc(hidden)]
pub mod otlp;
pub mod ownership_store;
pub mod peer_auth;
pub mod peer_failure;
pub mod peer_probe;
pub mod pool;
pub mod protocol;
pub(crate) mod queue_batching;
pub mod queue_cli;
pub mod queue_policy;
pub mod r2_cli;
pub(crate) mod r2_store;
pub mod replication;
pub mod runtime;
pub mod staged_invoke;
pub mod startup;
pub mod storage;
pub mod stub_table;
pub mod telemetry;
pub mod tls_roots;
pub mod upload_table;
pub mod version;
pub mod wake;
pub mod wake_entry;
pub mod wake_format;
pub mod worker_cache;
pub mod ws_client;
pub mod ws_message;
pub mod ws_registry;

#[cfg(all(test, cello_internal_tests))]
mod composed_simulation {
    include!(env!("CELLO_INTERNAL_COMPOSED_SIMULATION"));
}

#[cfg(all(test, cello_internal_tests))]
mod simulation_reproduction {
    include!(env!("CELLO_INTERNAL_SIMULATION_REPRODUCTION"));
}

#[cfg(all(test, cello_internal_tests))]
mod token_lifecycle_tests {
    include!(env!("CELLO_INTERNAL_TOKEN_PROBE"));
}

#[cfg(all(test, cello_internal_tests))]
mod conformance_world_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s2_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_S2_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_o3_oracle {
    include!(env!("CELLO_CONFORMANCE_O3_ORACLE_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5a_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_S5A_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5c_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_S5C_TESTS"));
}

#[cfg(cello_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod conformance_sim_store {
    include!(env!("CELLO_CONFORMANCE_SIM_STORE_TESTS"));
}

#[cfg(cello_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod conformance_sim_cell_host {
    include!(env!("CELLO_CONFORMANCE_SIM_CELL_HOST_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_s1_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_S1_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_coverage {
    include!(env!("CELLO_CONFORMANCE_WORLD_COVERAGE_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s3_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_S3_TESTS"));
}

#[cfg(all(test, cello_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5b_tests {
    include!(env!("CELLO_CONFORMANCE_WORLD_S5B_TESTS"));
}

/// Completion token for a resident-isolate reservation made by the decision
/// core. Dropping the queued/running job reports that the cell is idle again;
/// the token contains no selection or lifecycle policy of its own.
pub struct CellActivityGuard {
    finish: Option<Box<dyn FnOnce() + Send>>,
}

impl CellActivityGuard {
    pub fn new(finish: impl FnOnce() + Send + 'static) -> Self {
        Self {
            finish: Some(Box::new(finish)),
        }
    }
}

impl Drop for CellActivityGuard {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish();
        }
    }
}

/// A stateless entrypoint selected for one fetch request.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerFetchEntrypoint {
    pub name: String,
    /// The caller's `ctx.props` as V8 structured-clone bytes. An empty vector
    /// represents an omitted or `undefined` props value.
    pub props: Vec<u8>,
}

/// Resource limits selected by a Worker Loader stub for one invocation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInvocationLimits {
    pub cpu_ms: Option<u32>,
    pub sub_requests: Option<u32>,
}

/// One operation on a Worker entrypoint. A property read has no arguments,
/// while a call owns its complete receiver path and structured-clone payload.
pub enum WorkerRpcOperation {
    Get { path: Vec<String> },
    Call { path: Vec<String>, args: Vec<u8> },
}

pub enum WorkerJob {
    Fetch {
        queued_at: std::time::Instant,
        /// An entrypoint dispatcher target, or the direct default export when
        /// absent.
        entrypoint: Option<WorkerFetchEntrypoint>,
        invocation_limits: Option<WorkerInvocationLimits>,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        request_id: Option<js::RequestId>,
        /// Receives one completed invocation report for Dynamic Worker tails.
        /// Ordinary fetches leave this empty.
        tail_report: Option<tokio::sync::oneshot::Sender<String>>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::HttpResponse>>,
    },
    Rpc {
        entrypoint: String,
        operation: WorkerRpcOperation,
        /// The caller's `ctx.props` as V8 structured-clone bytes, the same
        /// encoding as `args`. It is empty when the caller sent none, which no
        /// encoded value can be. A Worker Loader entrypoint or a transferred
        /// Service Binding can set it.
        props: Vec<u8>,
        invocation_limits: Option<WorkerInvocationLimits>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
    Queue {
        queued_at: std::time::Instant,
        batch: js::QueueBatch,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::QueueDispatchResult>>,
    },
}

/// Temporary host seam required by the verbatim JS adapter. The runtime
/// adapter will construct the real shared Worker queue; lifecycle policy does
/// not move into this type.
/// Compatibility switches copied with the JS adapter. These are runtime
/// semantics, not lifecycle decisions.
pub fn worker_compat(metadata: &serde_json::Value) -> js::Compat {
    let flags = metadata
        .get("compatibility_flags")
        .and_then(serde_json::Value::as_array);
    let has_flag = |name: &str| {
        flags.is_some_and(|flags| {
            flags
                .iter()
                .any(|flag| flag.as_str().is_some_and(|flag| flag == name))
        })
    };
    let date = metadata
        .get("compatibility_date")
        .and_then(serde_json::Value::as_str);
    let switch = |enable: &str, disable: &str, since: &str| {
        if has_flag(enable) {
            return true;
        }
        if has_flag(disable) {
            return false;
        }
        date.is_some_and(|date| date >= since)
    };
    js::Compat {
        delete_all_deletes_alarm: switch(
            "delete_all_deletes_alarm",
            "delete_all_preserves_alarm",
            "2026-02-24",
        ),
        js_rpc: has_flag("js_rpc"),
        fetcher_get_put_delete: !switch(
            "fetcher_no_get_put_delete",
            "fetcher_has_get_put_delete",
            "2024-03-26",
        ),
        sqlite_vec: has_flag("sqlite_vec"),
        // A deployment with no compatibility_date behaves as workerd's
        // default (the newest date), so it gets the standard "blob" default;
        // only a date older than the implied one keeps ArrayBuffer. The
        // explicit opt-out wins either way.
        websocket_standard_binary_type: if has_flag("no_websocket_standard_binary_type") {
            false
        } else if has_flag("websocket_standard_binary_type") {
            true
        } else {
            date.is_none_or(|date| date >= "2026-03-17")
        },
        queue_json_messages: switch("queue_json_messages", "queue_v8_messages", "2024-03-18"),
        // The release date of 0.2.10, the first version that can store the
        // text type. Workerd has no flag or date for this — it always binds
        // text — so Cello picks the date it starts matching workerd rather
        // than inventing an upstream one. A deployment with no
        // `compatibility_date` keeps INTEGER: unlike `binaryType`'s WHATWG
        // default, this type is data already in the database, so an
        // unversioned deployment must not start rewriting it.
        sql_boolean_text: switch("sql_boolean_text", "no_sql_boolean_text", "2026-11-24"),
        // The same date and reasoning as `sql_boolean_text`: workerd's D1
        // binding stores a JavaScript number as REAL at every compatibility
        // date, so Cello starts matching it at the release that can store the
        // new type. A deployment with no date keeps INTEGER, because a stored
        // type is data already in the database.
        d1_number_real: switch("d1_number_real", "no_d1_number_real", "2026-11-24"),
        // The opt-out wins over both the implicit `nodejs_compat`
        // population and the explicit populate flag, matching Cloudflare's
        // `nodejs_compat_do_not_populate_process_env` (denoland/celld#296).
        nodejs_process_env: (has_flag("nodejs_compat")
            || has_flag("nodejs_compat_populate_process_env"))
            && !has_flag("nodejs_compat_do_not_populate_process_env"),
    }
}

#[cfg(test)]
mod compat_tests {
    use super::*;

    fn metadata(date: Option<&str>, flags: &[&str]) -> serde_json::Value {
        let mut metadata = serde_json::Map::new();
        if let Some(date) = date {
            metadata.insert("compatibility_date".into(), serde_json::json!(date));
        }
        if !flags.is_empty() {
            metadata.insert("compatibility_flags".into(), serde_json::json!(flags));
        }
        serde_json::Value::Object(metadata)
    }

    #[test]
    fn binary_type_default_follows_date_flags_and_absence() {
        let binary_type = |date: Option<&str>, flags: &[&str]| {
            worker_compat(&metadata(date, flags)).websocket_standard_binary_type
        };
        // The implied date is exclusive: the day before keeps ArrayBuffer.
        assert!(!binary_type(Some("2026-03-16"), &[]));
        assert!(binary_type(Some("2026-03-17"), &[]));
        // No compatibility_date behaves as workerd's default (the newest).
        assert!(binary_type(None, &[]));
        // Explicit flags win in both directions, and the opt-out wins even
        // when the date would imply the flag.
        assert!(binary_type(
            Some("2026-01-01"),
            &["websocket_standard_binary_type"]
        ));
        assert!(!binary_type(
            Some("2026-11-01"),
            &["no_websocket_standard_binary_type"]
        ));
        assert!(!binary_type(None, &["no_websocket_standard_binary_type"]));
    }

    #[test]
    fn sql_boolean_text_follows_date_flags_and_absence() {
        let boolean_text = |date: Option<&str>, flags: &[&str]| {
            worker_compat(&metadata(date, flags)).sql_boolean_text
        };
        // The gate's date is inclusive: the day before keeps INTEGER.
        assert!(!boolean_text(Some("2026-11-23"), &[]));
        assert!(boolean_text(Some("2026-11-24"), &[]));
        assert!(boolean_text(Some("2027-01-01"), &[]));
        // No compatibility_date keeps INTEGER: existing rows hold 1/0, so an
        // unversioned deployment must not change the type it writes.
        assert!(!boolean_text(None, &[]));
        // Either explicit flag wins over the date, in both directions.
        assert!(boolean_text(Some("2026-11-01"), &["sql_boolean_text"]));
        assert!(!boolean_text(Some("2027-01-01"), &["no_sql_boolean_text"]));
        assert!(!boolean_text(None, &["no_sql_boolean_text"]));
    }

    #[test]
    fn d1_number_real_follows_date_flags_and_absence() {
        let number_real = |date: Option<&str>, flags: &[&str]| {
            worker_compat(&metadata(date, flags)).d1_number_real
        };
        // The gate's date is inclusive: the day before keeps INTEGER.
        assert!(!number_real(Some("2026-11-23"), &[]));
        assert!(number_real(Some("2026-11-24"), &[]));
        assert!(number_real(Some("2027-01-01"), &[]));
        // No compatibility_date keeps INTEGER, as for `sql_boolean_text`.
        assert!(!number_real(None, &[]));
        // Either explicit flag wins over the date, in both directions.
        assert!(number_real(Some("2026-11-01"), &["d1_number_real"]));
        assert!(!number_real(Some("2027-01-01"), &["no_d1_number_real"]));
        assert!(!number_real(None, &["no_d1_number_real"]));
    }
}
