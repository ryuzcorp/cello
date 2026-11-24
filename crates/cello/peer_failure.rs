// Modified by Ryuz Corp (Cello): versioned handler-failure body across nodes
// (#226). Apache-2.0.

//! The peer wire form of a Durable Object handler failure.
//!
//! When a forwarded fetch's handler throws on the owner, the owner answers
//! with the [`HANDLER_FAILURE_HEADER`] marker. The header value is the version
//! of the body that follows:
//!
//! - `1` (the original form): `cell Worker failed: <message>`, plain text.
//!   Everything that is not a handler failure keeps this form, so a new caller
//!   still learns the owner's reason for a transport or runtime error.
//! - `2`: a JSON object carrying the [`CellHandlerError`] parts, so the caller
//!   can rebuild the value the handler threw exactly as a local owner does:
//!   the class (Error subclass, `DOMException`, plain object), workerd's
//!   `remote` marker, and a first-line-only stack.
//!
//! ```json
//! {"kind":"dom","name":"AbortError","message":"aborted!","value":"AAECAw=="}
//! ```
//!
//! `value` is the base64 storage-clone bytes of a thrown non-Error object and
//! is present only for `kind:"object"`. The bytes reach the caller isolate
//! through `__sc_decode`, the same storage/clone codec a stored value uses, so
//! a peer cannot smuggle a host object or a wasm module across: the storage
//! delegate declines both.
//!
//! **Rolling upgrades.** The header and its meaning ("the owner ran the
//! handler; the reply is that failure, not an application response") do not
//! change, and [`PROTOCOL_VERSION`](crate::peer_auth::PROTOCOL_VERSION) does
//! not move: a mixed fleet keeps forwarding. A new caller reads `2` as
//! structured and anything else as version `1` text, so an old owner's answer
//! still fails the call. An old caller ignores the value and reads the whole
//! body as text, so it still sees an error from a new owner — the versioned
//! JSON as its message, rather than a clean one. Both directions keep
//! at-most-once: the failure marker still stops the retry policy.
//!
//! **Untrusted input.** The body is bounded ([`MAX_BODY_BYTES`]) before it is
//! parsed; the parse refuses unknown fields, an unknown `kind`, an oversized
//! name/message, an `object` without clone bytes, and more clone bytes than
//! [`MAX_VALUE_BYTES`]. Nothing a peer sends reaches the isolate as anything
//! but a `CellHandlerError` the caller would have produced itself.

use crate::engine_api::{CellHandlerError, CellHandlerKind};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

/// The header that marks a peer reply as a handler failure. Its value is the
/// body version ([`VERSION_STRUCTURED`] or the legacy text form).
pub const HANDLER_FAILURE_HEADER: &str = "x-cells-handler-failure";

/// The legacy text body's prefix (version `1`). A caller strips it so an
/// error's message is the handler's own words, not the host's wrapper.
pub const LEGACY_PREFIX: &str = "cell Worker failed: ";

/// The body version that carries the structured handler failure.
pub const VERSION_STRUCTURED: &str = "2";

/// The largest handler-failure body either side accepts or sends. A peer
/// control body is already capped at this size; the structured form fits
/// under the same cap by shrinking the fields that are not a class name.
pub const MAX_BODY_BYTES: usize = 64 << 10;

/// Longest class name carried. A class name is an identifier, so this is
/// generous; a longer one is truncated at a character boundary.
pub const MAX_NAME_BYTES: usize = 1 << 10;

/// Longest message carried before the clone budget takes precedence. A
/// longer one is truncated at a character boundary and marked with `…`.
pub const MAX_MESSAGE_BYTES: usize = 16 << 10;

/// Most clone bytes carried for a thrown object. The local path has no such
/// cap; this one only keeps the peer body inside [`MAX_BODY_BYTES`]. An
/// object whose clone is larger crosses as the primitive shape instead — a
/// plain `Error` carrying the message — which is the shape the local path
/// uses when its serializer refuses a value outright.
pub const MAX_VALUE_BYTES: usize = 32 << 10;

/// The message a caller reports when the body itself could not be read (for
/// example a peer that streamed more than [`MAX_BODY_BYTES`]). It names the
/// owner's failure so the shape stays "the handler failed", never a 500
/// handed to the application.
pub const UNREADABLE_BODY_MESSAGE: &str =
    "the Durable Object handler failed on the owner node; its failure report could not be read";

/// A handler failure a peer owner reported. It is a distinct type because the
/// caller's retry policy must not re-send a call the owner already ran, and
/// because `cello::js` reads the structured failure through
/// [`cell_handler_error`](crate::engine_api::cell_handler_error).
#[derive(Clone, Debug)]
pub struct RemoteHandlerFailure(pub CellHandlerError);

impl std::fmt::Display for RemoteHandlerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl std::error::Error for RemoteHandlerFailure {}

/// The body an owner sends for a handler failure the local path could shape.
/// The body always fits [`MAX_BODY_BYTES`].
pub fn structured_body(handler: &CellHandlerError) -> String {
    let full_message = truncate(&handler.message, MAX_MESSAGE_BYTES);
    let mut kind = handler.kind;
    let mut name = truncate(&handler.name, MAX_NAME_BYTES);
    let mut message = full_message.clone();
    // A clone that does not fit the peer budget cannot cross. Report the
    // failure in the shape the local path uses when the serializer refuses a
    // value: a primitive `Error` with the message.
    let mut value = match kind {
        CellHandlerKind::Object => match &handler.value {
            Some(bytes) if !bytes.is_empty() && bytes.len() <= MAX_VALUE_BYTES => {
                Some(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
            _ => {
                kind = CellHandlerKind::Primitive;
                name = "Error".to_string();
                None
            }
        },
        _ => None,
    };
    loop {
        if render(kind, &name, &message, value.as_deref()).len() <= MAX_BODY_BYTES {
            break;
        }
        if let Some(encoded) = value.as_deref() {
            // The clone may fit with a shorter message; dropping it first
            // would lose the object for no reason.
            if render(kind, &name, "", Some(encoded)).len() <= MAX_BODY_BYTES {
                message = halve(&message);
                continue;
            }
            // The clone alone is over the envelope, so it can never cross.
            // The message is the one part a caller can still show, so it
            // keeps its full (already capped) text.
            value = None;
            kind = CellHandlerKind::Primitive;
            name = "Error".to_string();
            message = full_message.clone();
            continue;
        }
        // No clone to drop: JSON escaping can multiply a string several
        // times over, so a message under its own cap can still push the body
        // past the envelope. Halve it; the class name and the kind are tiny
        // and never the cause.
        if message.is_empty() {
            // Kind, class name and an empty message always fit.
            break;
        }
        message = halve(&message);
    }
    render(kind, &name, &message, value.as_deref())
}

/// The smallest proper prefix of `message` (on a character boundary) that
/// still differs, so a fitting loop always makes progress.
fn halve(message: &str) -> String {
    let halved = truncate(message, message.len() / 2);
    if halved.len() < message.len() {
        halved
    } else {
        String::new()
    }
}

fn render(kind: CellHandlerKind, name: &str, message: &str, value: Option<&str>) -> String {
    serde_json::to_string(&HandlerFailureBody {
        kind: kind.as_str().to_string(),
        name: name.to_string(),
        message: message.to_string(),
        value: value.map(str::to_string),
    })
    .expect("a handler failure body serializes")
}

/// The version `1` body: the text every owner sent before the structured form.
/// A new owner keeps it for every failure that is not a handler exception.
pub fn legacy_body(message: &str) -> String {
    let mut body = String::with_capacity(LEGACY_PREFIX.len() + message.len());
    body.push_str(LEGACY_PREFIX);
    body.push_str(message);
    truncate(&body, MAX_BODY_BYTES)
}

/// Read a peer's handler-failure body. `version` is the header value.
///
/// `None` means the body is not the structured form the caller can trust, so
/// the caller reports it as version `1` text instead. The structured form is
/// accepted only when every field is one the local path could have produced.
pub fn decode_body(version: &str, body: &[u8]) -> Option<CellHandlerError> {
    if version != VERSION_STRUCTURED || body.len() > MAX_BODY_BYTES {
        return None;
    }
    let parsed: HandlerFailureBody = serde_json::from_slice(body).ok()?;
    let kind = match parsed.kind.as_str() {
        "error" => CellHandlerKind::Error,
        "dom" => CellHandlerKind::Dom,
        "object" => CellHandlerKind::Object,
        "primitive" => CellHandlerKind::Primitive,
        _ => return None,
    };
    if parsed.name.len() > MAX_NAME_BYTES || parsed.message.len() > MAX_MESSAGE_BYTES {
        return None;
    }
    let value = match kind {
        CellHandlerKind::Object => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(parsed.value?)
                .ok()?;
            if bytes.is_empty() || bytes.len() > MAX_VALUE_BYTES {
                return None;
            }
            Some(bytes)
        }
        // Only an object crosses by value; any bytes on another kind would be
        // a field the local path never produces.
        _ if parsed.value.is_some() => return None,
        _ => None,
    };
    Some(CellHandlerError {
        kind,
        name: parsed.name,
        message: parsed.message,
        value,
    })
}

/// The failure a peer reply reports: structured when the body is the form a
/// new owner sends, the legacy text otherwise, and a bounded placeholder when
/// the transport could not read the body at all (`body` is `None`, which the
/// caller passes when the peer streamed past [`MAX_BODY_BYTES`]).
pub fn remote_handler_failure(version: &str, body: Option<&[u8]>) -> CellHandlerError {
    let Some(body) = body else {
        return unreadable_handler_failure();
    };
    match decode_body(version, body) {
        Some(handler) => handler,
        None => {
            let text = String::from_utf8_lossy(body);
            let message = text.strip_prefix(LEGACY_PREFIX).unwrap_or(&text);
            legacy_handler_failure(message)
        }
    }
}

/// The version `1` reading of a body: the owner's text becomes the message of
/// a primitive `Error`, which is the shape workerd gives a thrown primitive.
pub fn legacy_handler_failure(message: &str) -> CellHandlerError {
    CellHandlerError {
        kind: CellHandlerKind::Primitive,
        name: "Error".to_string(),
        message: message.to_string(),
        value: None,
    }
}

/// The failure a caller reports when the body could not be read at all.
pub fn unreadable_handler_failure() -> CellHandlerError {
    legacy_handler_failure(UNREADABLE_BODY_MESSAGE)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HandlerFailureBody {
    kind: String,
    name: String,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

/// The longest prefix of `text` that fits `limit` bytes on a character
/// boundary. A truncated string ends in `…`, so a caller can tell.
fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_string();
    }
    // The marker is part of the result, so the cut leaves room for it: a
    // truncated field never exceeds the limit its reader validates.
    let marker = '…';
    let room = limit.saturating_sub(marker.len_utf8());
    let mut end = room.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = text[..end].to_string();
    truncated.push(marker);
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(handler: CellHandlerError) {
        let body = structured_body(&handler);
        assert!(body.len() <= MAX_BODY_BYTES, "{body}");
        let decoded = decode_body(VERSION_STRUCTURED, body.as_bytes())
            .unwrap_or_else(|| panic!("structured body must decode: {body}"));
        assert_eq!(decoded.kind, handler.kind);
        assert_eq!(decoded.name, handler.name);
        assert_eq!(decoded.message, handler.message);
        assert_eq!(decoded.value, handler.value);
    }

    #[test]
    fn every_kind_round_trips() {
        round_trip(CellHandlerError {
            kind: CellHandlerKind::Error,
            name: "TypeError".to_string(),
            message: "bad type".to_string(),
            value: None,
        });
        round_trip(CellHandlerError {
            kind: CellHandlerKind::Dom,
            name: "AbortError".to_string(),
            message: "aborted!".to_string(),
            value: None,
        });
        round_trip(CellHandlerError {
            kind: CellHandlerKind::Object,
            name: "ObjError".to_string(),
            message: "obj msg".to_string(),
            value: Some(vec![1, 2, 3, 4]),
        });
        round_trip(CellHandlerError {
            kind: CellHandlerKind::Primitive,
            name: "Error".to_string(),
            message: "plain string".to_string(),
            value: None,
        });
    }

    #[test]
    fn the_legacy_version_is_never_structured() {
        let handler = legacy_handler_failure("boom");
        let body = structured_body(&handler);
        assert!(decode_body("1", body.as_bytes()).is_none());
        assert!(decode_body("", body.as_bytes()).is_none());
        assert!(decode_body("3", body.as_bytes()).is_none());
    }

    #[test]
    fn the_legacy_body_keeps_its_prefix_and_stays_bounded() {
        let body = legacy_body("boom");
        assert_eq!(body, format!("{LEGACY_PREFIX}boom"));
        let long = legacy_body(&"x".repeat(MAX_BODY_BYTES * 2));
        assert!(long.len() <= MAX_BODY_BYTES);
        assert!(long.starts_with(LEGACY_PREFIX));
    }

    #[test]
    fn an_oversize_body_or_field_is_refused() {
        let handler = CellHandlerError {
            kind: CellHandlerKind::Object,
            name: "ObjError".to_string(),
            message: "obj msg".to_string(),
            value: Some(vec![7u8; MAX_VALUE_BYTES + 1]),
        };
        let body = structured_body(&handler);
        let decoded = decode_body(VERSION_STRUCTURED, body.as_bytes()).expect("degrades");
        assert_eq!(decoded.kind, CellHandlerKind::Primitive);
        assert_eq!(decoded.name, "Error");

        let huge = format!(
            r#"{{"kind":"primitive","name":"Error","message":"{}"}}"#,
            "x".repeat(MAX_BODY_BYTES)
        );
        assert!(decode_body(VERSION_STRUCTURED, huge.as_bytes()).is_none());
    }

    #[test]
    fn a_hostile_field_is_refused() {
        for body in [
            r#"{"kind":"dom","name":"AbortError","message":"m","value":"AAAA"}"#,
            r#"{"kind":"object","name":"E","message":"m"}"#,
            r#"{"kind":"object","name":"E","message":"m","value":""}"#,
            r#"{"kind":"object","name":"E","message":"m","value":"%%%"}"#,
            r#"{"kind":"error","name":"E","message":"m","extra":1}"#,
            r#"{"kind":"weird","name":"E","message":"m"}"#,
            r#"{"kind":"error","name":"E"}"#,
            "not json",
        ] {
            assert!(
                decode_body(VERSION_STRUCTURED, body.as_bytes()).is_none(),
                "{body} must be refused"
            );
        }
    }

    #[test]
    fn an_unreadable_body_is_still_a_handler_failure() {
        let handler = remote_handler_failure(VERSION_STRUCTURED, None);
        assert_eq!(handler.kind, CellHandlerKind::Primitive);
        assert_eq!(handler.name, "Error");
        assert_eq!(handler.message, UNREADABLE_BODY_MESSAGE);
        // A structured body the header does not vouch for is text, and the
        // legacy prefix still comes off.
        let body = format!("{LEGACY_PREFIX}owner exploded");
        let handler = remote_handler_failure("1", Some(body.as_bytes()));
        assert_eq!(handler.message, "owner exploded");
        // A garbage structured body falls back to its text, bounded by the
        // body the transport already capped.
        let handler = remote_handler_failure(VERSION_STRUCTURED, Some(b"{\"kind\":"));
        assert_eq!(handler.message, "{\"kind\":");
    }

    #[test]
    fn a_clone_that_fits_keeps_the_object_when_only_the_message_is_trimmed() {
        let handler = CellHandlerError {
            kind: CellHandlerKind::Object,
            name: "BigError".to_string(),
            message: "\u{1}".repeat(MAX_MESSAGE_BYTES),
            value: Some(vec![1, 2, 3]),
        };
        let body = structured_body(&handler);
        assert!(body.len() <= MAX_BODY_BYTES, "{} bytes", body.len());
        let decoded = decode_body(VERSION_STRUCTURED, body.as_bytes()).expect("fits");
        assert_eq!(decoded.kind, CellHandlerKind::Object);
        assert_eq!(decoded.value, Some(vec![1, 2, 3]));
        assert!(
            decoded.message.len() < MAX_MESSAGE_BYTES,
            "the message is what was trimmed: {}",
            decoded.message.len()
        );
    }

    #[test]
    fn a_message_of_control_characters_still_fits_the_envelope() {
        let handler = CellHandlerError {
            kind: CellHandlerKind::Error,
            name: "Error".to_string(),
            message: "\u{0}".repeat(MAX_MESSAGE_BYTES),
            value: None,
        };
        let body = structured_body(&handler);
        assert!(body.len() <= MAX_BODY_BYTES, "{} bytes", body.len());
        assert!(decode_body(VERSION_STRUCTURED, body.as_bytes()).is_some());
    }
}
