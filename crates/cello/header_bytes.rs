// Modified by Ryuz Corp (Cello): UTF-8-with-replacement header decoding (issue #295). Apache-2.0.

//! Decoding of inbound HTTP/1 header values into Worker `Headers` strings.
//!
//! The ingress used to collect headers with `HeaderValue::to_str`, so any
//! value that was not valid UTF-8 silently dropped its whole header —
//! notably a `Cookie` with a non-ASCII byte, which vanished along with every
//! other cookie. Every HTTP-and-tunnel-bytes to string conversion goes
//! through this module: values decode as UTF-8 with replacement, as workerd
//! decodes a received value, so a valid non-ASCII `Cookie` stays visible
//! and an undecodable byte becomes U+FFFD instead of dropping the header
//! (#295).

/// Inbound HTTP/1 value bytes to the Worker's `Headers` string: UTF-8 with
/// U+FFFD replacement, matching workerd's decoding of a received value.
/// Infallible; ASCII values cross exactly as before.
pub fn header_value_to_js(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
