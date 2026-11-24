// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! zlib: the one-shot `__zlib` op and the streaming compression backend.
//!
//! `CompressionStream` and `DecompressionStream` need output chunk by chunk,
//! so a stream keeps a live flate2 writer whose `Vec` sink is drained after
//! every push. The one-shot op has no such state and runs to completion.
//!
//! A live writer is native memory V8 neither sees nor bounds: a gzip
//! encoder holds roughly 300 KiB, a decoder roughly 75 KiB. Three rules
//! bound it. The table is an isolate slot, so retiring an isolate frees
//! every stream it still holds. JS releases a stream on its last flush,
//! on cancel or abort, and through a `FinalizationRegistry` when a stream
//! is abandoned mid-flight. And an isolate holds at most
//! [`MAX_LIVE_ZLIB_STREAMS`] at once, so a tenant that keeps streams
//! reachable, or outruns the collector, is refused instead of growing the
//! process.
use super::*;

pub(super) fn op_zlib(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use std::io::{Read, Write};
    let mode = args.get(0).to_rust_string_lossy(scope);
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let result = (|| -> Result<Vec<u8>> {
        match mode.as_str() {
            "gzip" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "gunzip" => {
                let mut decoder = flate2::read::GzDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            "deflate" => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "inflate" => {
                let mut decoder = flate2::read::ZlibDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            "deflateRaw" => {
                let mut encoder =
                    flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "inflateRaw" => {
                let mut decoder = flate2::read::DeflateDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            _ => Err(anyhow!("unsupported zlib operation")),
        }
    })();
    match result {
        Ok(bytes) => webcrypto_return_bytes(scope, rv, &bytes),
        Err(error) => {
            let message = v8::String::new(scope, &format!("zlib: {error}")).unwrap();
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// One live `CompressionStream`/`DecompressionStream`: a stateful flate2
/// writer whose `Vec` sink is drained after every push, so output streams
/// chunk by chunk instead of arriving once at close.
enum ZlibStream {
    GzipEncode(flate2::write::GzEncoder<Vec<u8>>),
    GzipDecode(Box<GzipDecode>),
    ZlibEncode(flate2::write::ZlibEncoder<Vec<u8>>),
    ZlibDecode(flate2::write::ZlibDecoder<Vec<u8>>),
    RawEncode(flate2::write::DeflateEncoder<Vec<u8>>),
    RawDecode(flate2::write::DeflateDecoder<Vec<u8>>),
    // Workerd's brotli stream: quality 5 with a 19-bit window reproduces its
    // bytes exactly (measured against workerd 1.20261121.1 for several
    // inputs; qualities 5 and 6 are byte-identical on them).
    // Boxed: a brotli encoder is ~5.6 KiB of state, and the enum would
    // otherwise carry that size in every variant (clippy::large_enum_variant).
    BrotliEncode(Box<brotli::CompressorWriter<Vec<u8>>>),
    BrotliDecode(Box<brotli::DecompressorWriter<Vec<u8>>>),
}

/// Workerd's gzip close() message for a member whose trailer never arrived,
/// shared by every decompressor that can report it.
const INCOMPLETE: &str = "Called close() on a decompression stream with incomplete data";

/// A gzip stream framed here rather than by flate2's `write::GzDecoder`.
///
/// That decoder validates the trailer only in `finish()`, so a missing
/// trailer and a wrong checksum are the same `InvalidInput` error, a bad
/// checksum surfaces on close() instead of on the push that carried it,
/// and bytes after the member reach the caller as a generic failure.
/// Workerd validates as the bytes arrive, and its three outcomes are
/// distinct (#230, measured on 1.20261121.1): a failed push is
/// "Decompression failed.", a header or trailer that never completes is
/// the incomplete-data error at close, and input past the trailer is
/// "Trailing bytes after end of compressed data".
struct GzipDecode {
    phase: GzipPhase,
    /// Raw inflate: the gzip header and trailer are this type's job.
    inflate: flate2::Decompress,
    /// CRC-32 of the uncompressed bytes, and their count for ISIZE.
    crc: flate2::Crc,
    /// CRC-32 of every header byte, for FHCRC's low 16 bits.
    header_crc: flate2::Crc,
    /// The header's FLG byte, with each optional field cleared as it is
    /// consumed, so the next one is always the lowest remaining bit.
    flags: u8,
}

enum GzipPhase {
    /// Collecting the fixed ten-byte header.
    Header {
        buf: [u8; 10],
        len: usize,
    },
    /// Collecting FEXTRA's two-byte XLEN.
    ExtraLen {
        buf: [u8; 2],
        len: usize,
    },
    /// Skipping FEXTRA's XLEN payload bytes.
    Extra {
        left: usize,
    },
    /// Skipping a NUL-terminated FNAME or FCOMMENT.
    Name,
    Comment,
    /// Collecting FHCRC, checked against the header's CRC-16 once both
    /// bytes have arrived.
    HeaderCrc {
        buf: [u8; 2],
        len: usize,
    },
    /// Inflating the deflate body.
    Body,
    /// Collecting the trailer's CRC-32, checked as soon as its four bytes
    /// arrive (workerd fails that push, not the one carrying ISIZE).
    TrailerCrc {
        buf: [u8; 4],
        len: usize,
    },
    /// Collecting the trailer's ISIZE.
    TrailerSize {
        buf: [u8; 4],
        len: usize,
    },
    /// The member ended and its trailer checked out.
    Done,
}

const GZIP_FAILURE: &str = "Decompression failed.";
const GZIP_TRAILING: &str = "Trailing bytes after end of compressed data";

/// The bytes after a NUL-terminated header field, or `None` when this
/// chunk ends inside it and the next push has to carry the terminator.
fn after_nul(input: &[u8]) -> Option<&[u8]> {
    input
        .iter()
        .position(|&byte| byte == 0)
        .map(|end| &input[end + 1..])
}

impl GzipDecode {
    fn new() -> Self {
        GzipDecode {
            phase: GzipPhase::Header {
                buf: [0; 10],
                len: 0,
            },
            inflate: flate2::Decompress::new(false),
            crc: flate2::Crc::new(),
            header_crc: flate2::Crc::new(),
            flags: 0,
        }
    }

    /// Consumes what it can of the next optional header field, falling
    /// through to the one after it.
    fn next_header_field(&mut self) {
        self.phase = if self.flags & 0x04 != 0 {
            self.flags &= !0x04;
            GzipPhase::ExtraLen {
                buf: [0; 2],
                len: 0,
            }
        } else if self.flags & 0x08 != 0 {
            self.flags &= !0x08;
            GzipPhase::Name
        } else if self.flags & 0x10 != 0 {
            self.flags &= !0x10;
            GzipPhase::Comment
        } else if self.flags & 0x02 != 0 {
            self.flags &= !0x02;
            GzipPhase::HeaderCrc {
                buf: [0; 2],
                len: 0,
            }
        } else {
            GzipPhase::Body
        };
    }

    /// One push: advances the frame and inflates the body bytes the frame
    /// leaves, returning whatever output it produced.
    fn push(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            // Taken by value so an arm can set `self.phase` without
            // borrowing it; every path below installs the next phase.
            match std::mem::replace(&mut self.phase, GzipPhase::Done) {
                GzipPhase::Header { mut buf, mut len } => {
                    let take = (10 - len).min(input.len());
                    buf[len..len + take].copy_from_slice(&input[..take]);
                    self.header_crc.update(&input[..take]);
                    len += take;
                    input = &input[take..];
                    // Workerd rejects a bad magic, method or flag as soon
                    // as those bytes arrive, not once the header is whole
                    // (measured: an eight-byte "not gzip" chunk fails the
                    // push rather than the close).
                    if (len >= 2 && (buf[0] != 0x1f || buf[1] != 0x8b))
                        || (len >= 3 && buf[2] != 8)
                        || (len >= 4 && buf[3] & 0xe0 != 0)
                    {
                        return Err(anyhow!(GZIP_FAILURE));
                    }
                    if len < 10 {
                        self.phase = GzipPhase::Header { buf, len };
                        return Ok(out);
                    }
                    self.flags = buf[3];
                    self.next_header_field();
                }
                GzipPhase::ExtraLen { mut buf, mut len } => {
                    let take = (2 - len).min(input.len());
                    buf[len..len + take].copy_from_slice(&input[..take]);
                    self.header_crc.update(&input[..take]);
                    len += take;
                    input = &input[take..];
                    if len < 2 {
                        self.phase = GzipPhase::ExtraLen { buf, len };
                        return Ok(out);
                    }
                    self.phase = GzipPhase::Extra {
                        left: u16::from_le_bytes(buf) as usize,
                    };
                }
                GzipPhase::Extra { left } => {
                    let take = left.min(input.len());
                    self.header_crc.update(&input[..take]);
                    input = &input[take..];
                    let left = left - take;
                    if left > 0 {
                        self.phase = GzipPhase::Extra { left };
                        return Ok(out);
                    }
                    self.next_header_field();
                }
                GzipPhase::Name => {
                    let Some(rest) = after_nul(input) else {
                        self.header_crc.update(input);
                        self.phase = GzipPhase::Name;
                        return Ok(out);
                    };
                    self.header_crc.update(&input[..input.len() - rest.len()]);
                    input = rest;
                    self.next_header_field();
                }
                GzipPhase::Comment => {
                    let Some(rest) = after_nul(input) else {
                        self.header_crc.update(input);
                        self.phase = GzipPhase::Comment;
                        return Ok(out);
                    };
                    self.header_crc.update(&input[..input.len() - rest.len()]);
                    input = rest;
                    self.next_header_field();
                }
                GzipPhase::HeaderCrc { mut buf, mut len } => {
                    let take = (2 - len).min(input.len());
                    buf[len..len + take].copy_from_slice(&input[..take]);
                    len += take;
                    input = &input[take..];
                    if len < 2 {
                        self.phase = GzipPhase::HeaderCrc { buf, len };
                        return Ok(out);
                    }
                    // RFC 1952 FHCRC: the low 16 bits of the CRC-32 of every
                    // header byte before these two.
                    if u16::from_le_bytes(buf) != self.header_crc.sum() as u16 {
                        return Err(anyhow!(GZIP_FAILURE));
                    }
                    self.next_header_field();
                }
                GzipPhase::Body => {
                    let mut status = flate2::Status::Ok;
                    while status != flate2::Status::StreamEnd {
                        let produced = out.len();
                        let before = self.inflate.total_in();
                        out.reserve(32 * 1024);
                        status = self
                            .inflate
                            .decompress_vec(input, &mut out, flate2::FlushDecompress::None)
                            .map_err(|_| anyhow!(GZIP_FAILURE))?;
                        let used = (self.inflate.total_in() - before) as usize;
                        input = &input[used..];
                        self.crc.update(&out[produced..]);
                        // Neither buffer moved: the next push or close
                        // supplies the rest of the member.
                        if status != flate2::Status::StreamEnd && used == 0 && out.len() == produced
                        {
                            self.phase = GzipPhase::Body;
                            return Ok(out);
                        }
                    }
                    self.phase = GzipPhase::TrailerCrc {
                        buf: [0; 4],
                        len: 0,
                    };
                }
                GzipPhase::TrailerCrc { mut buf, mut len } => {
                    let take = (4 - len).min(input.len());
                    buf[len..len + take].copy_from_slice(&input[..take]);
                    len += take;
                    input = &input[take..];
                    if len < 4 {
                        self.phase = GzipPhase::TrailerCrc { buf, len };
                        return Ok(out);
                    }
                    if u32::from_le_bytes(buf) != self.crc.sum() {
                        return Err(anyhow!(GZIP_FAILURE));
                    }
                    self.phase = GzipPhase::TrailerSize {
                        buf: [0; 4],
                        len: 0,
                    };
                }
                GzipPhase::TrailerSize { mut buf, mut len } => {
                    let take = (4 - len).min(input.len());
                    buf[len..len + take].copy_from_slice(&input[..take]);
                    len += take;
                    input = &input[take..];
                    if len < 4 {
                        self.phase = GzipPhase::TrailerSize { buf, len };
                        return Ok(out);
                    }
                    if u32::from_le_bytes(buf) != self.crc.amount() {
                        return Err(anyhow!(GZIP_FAILURE));
                    }
                    self.phase = GzipPhase::Done;
                }
                GzipPhase::Done => {
                    if !input.is_empty() {
                        return Err(anyhow!(GZIP_TRAILING));
                    }
                    self.phase = GzipPhase::Done;
                    return Ok(out);
                }
            }
        }
    }

    /// Close: only a member whose trailer checked out is complete.
    fn finish(self) -> Result<Vec<u8>> {
        match self.phase {
            GzipPhase::Done => Ok(Vec::new()),
            _ => Err(anyhow!(INCOMPLETE)),
        }
    }
}

/// The writer types share `write`/`get_ref`/`get_mut` but not `finish`,
/// so dispatch once here; the brotli arms are the ones without `finish`.
macro_rules! zlib_each {
    ($stream:expr, $w:ident => $body:expr) => {
        match $stream {
            ZlibStream::GzipEncode($w) => $body,
            ZlibStream::GzipDecode(_) => {
                unreachable!("the gzip decoder is framed in Rust")
            }
            ZlibStream::ZlibEncode($w) => $body,
            ZlibStream::ZlibDecode($w) => $body,
            ZlibStream::RawEncode($w) => $body,
            ZlibStream::RawDecode($w) => $body,
            ZlibStream::BrotliEncode($w) => $body,
            ZlibStream::BrotliDecode($w) => $body,
        }
    };
}

/// The flate2 writers also have `finish`; the brotli ones do not, so the
/// `finish` path dispatches only over these.
macro_rules! zlib_each_flate {
    ($stream:expr, $w:ident => $body:expr) => {
        match $stream {
            ZlibStream::GzipEncode($w) => $body,
            ZlibStream::GzipDecode(_) => {
                unreachable!("the gzip decoder is framed in Rust")
            }
            ZlibStream::ZlibEncode($w) => $body,
            ZlibStream::ZlibDecode($w) => $body,
            ZlibStream::RawEncode($w) => $body,
            ZlibStream::RawDecode($w) => $body,
            ZlibStream::BrotliEncode(_) | ZlibStream::BrotliDecode(_) => {
                unreachable!("brotli arms are finished by into_inner")
            }
        }
    };
}

impl ZlibStream {
    fn new(format: &str, decompress: bool) -> Option<ZlibStream> {
        use flate2::write as z;
        let level = flate2::Compression::default();
        let sink = Vec::new;
        Some(match (format, decompress) {
            ("gzip", false) => Self::GzipEncode(z::GzEncoder::new(sink(), level)),
            ("gzip", true) => Self::GzipDecode(Box::new(GzipDecode::new())),
            ("deflate", false) => Self::ZlibEncode(z::ZlibEncoder::new(sink(), level)),
            ("deflate", true) => Self::ZlibDecode(z::ZlibDecoder::new(sink())),
            ("deflate-raw", false) => Self::RawEncode(z::DeflateEncoder::new(sink(), level)),
            ("deflate-raw", true) => Self::RawDecode(z::DeflateDecoder::new(sink())),
            ("brotli", false) => {
                Self::BrotliEncode(Box::new(brotli::CompressorWriter::new(sink(), 4096, 5, 19)))
            }
            ("brotli", true) => {
                Self::BrotliDecode(Box::new(brotli::DecompressorWriter::new(sink(), 4096)))
            }
            _ => return None,
        })
    }

    fn decompress(&self) -> bool {
        matches!(
            self,
            Self::GzipDecode(_) | Self::ZlibDecode(_) | Self::RawDecode(_) | Self::BrotliDecode(_)
        )
    }

    /// Workerd's canonical message, matched by its conformance suite.
    fn failure(&self) -> &'static str {
        if self.decompress() {
            "Decompression failed."
        } else {
            "Compression failed."
        }
    }

    /// Write one chunk and return whatever output it produced. Decoded
    /// output flows per chunk; encoders hold theirs until a full buffer
    /// accumulates (matching Workerd, whose suite reads a small message
    /// back as one chunk), with the rest arriving from `finish`. A
    /// decoder that stops consuming input has hit the end of the
    /// compressed stream, so leftover bytes are trailing garbage —
    /// Workerd's strict_compression_checks. Gzip frames and validates its
    /// own bytes (see [`GzipDecode`]).
    fn push(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;
        if let Self::GzipDecode(decoder) = self {
            return decoder.push(input);
        }
        const ENCODER_CHUNK: usize = 32 * 1024;
        let failure = self.failure();
        let held = if self.decompress() { 0 } else { ENCODER_CHUNK };
        zlib_each!(self, w => {
            while !input.is_empty() {
                let n = w.write(input).map_err(|_| anyhow!(failure))?;
                if n == 0 { return Err(anyhow!(failure)); }
                input = &input[n..];
            }
            if w.get_ref().len() < held { return Ok(Vec::new()); }
            Ok(std::mem::take(w.get_mut()))
        })
    }

    /// Terminate the stream and return the final output. A decoder reports
    /// the incomplete-data error when its member never ended; gzip's
    /// trailer is validated as its bytes arrive (see [`GzipDecode`]).
    fn finish(self) -> Result<Vec<u8>> {
        // Workerd tells a truncated stream apart from corrupt input on
        // close(): "incomplete data" for the former, "Decompression failed."
        // for the latter. flate2 reports the former as UnexpectedEof, and its
        // brotli decoder reports both as a failed close (measured: workerd
        // answers the incomplete message for either brotli case).
        let failure = self.failure();
        match self {
            Self::GzipDecode(decoder) => decoder.finish(),
            Self::BrotliEncode(writer) => Ok((*writer).into_inner()),
            Self::BrotliDecode(writer) => (*writer).into_inner().map_err(|_| anyhow!(INCOMPLETE)),
            other => zlib_each_flate!(other, w => w.finish().map_err(|error| {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    anyhow!(INCOMPLETE)
                } else {
                    anyhow!(failure)
                }
            })),
        }
    }
}

/// The most native compression streams one isolate may hold at once.
///
/// Sized against the default 128 MiB V8 heap: 128 gzip encoders hold about
/// 38 MiB, decoders about a quarter of that. A stateless isolate serves a
/// couple of requests before the pool grows another, and a cell isolate
/// hosts up to 32 cells, so a live stream per request or per cell is far
/// below it. Reaching it means streams were left open faster than the
/// collector frees them; the constructor then throws a `RangeError`.
const MAX_LIVE_ZLIB_STREAMS: usize = 128;

/// Live compression streams for one isolate, keyed by an id JS holds across
/// awaits.
///
/// An isolate slot, for the reason `TextDecoders` in `js.rs` is one: a
/// `CompressionStream` is a JS object, so it never leaves the isolate that
/// made it, and every op that touches it runs under that isolate's
/// `v8::Locker` — whichever tokio worker drives the turn. It used to be a
/// process-wide table, which nothing but JS ever emptied: a stream left
/// open stayed for the life of the process, after the isolate that made it
/// was gone. Dropping the isolate now drops the table.
///
/// Ids count up per isolate and are never reused, so a finalizer's late
/// release of a stream that already ended is a no-op.
#[derive(Default)]
struct ZlibStreams {
    live: HashMap<u64, ZlibStream>,
    next: u64,
    /// Dropped with the table, so a test can see the isolate's teardown
    /// release it. Never read: its drop is the observation.
    #[cfg(test)]
    #[allow(dead_code)]
    teardown_probe: Option<Arc<()>>,
}

/// The calling isolate's stream table, created on first use.
fn zlib_streams<'a>(scope: &'a mut v8::PinScope) -> &'a mut ZlibStreams {
    if scope.get_slot::<ZlibStreams>().is_none() {
        scope.set_slot(ZlibStreams::default());
    }
    scope.get_slot_mut::<ZlibStreams>().expect("just set")
}

fn zlib_stream_return(
    scope: &mut v8::PinScope,
    rv: v8::ReturnValue<v8::Value>,
    result: Result<Vec<u8>>,
) {
    match result {
        Ok(bytes) => webcrypto_return_bytes(scope, rv, &bytes),
        Err(error) => {
            let message = v8::String::new(scope, &error.to_string()).unwrap();
            let exception = v8::Exception::type_error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// `__zlib_stream_new(format, decompress)` -> id. JS validates the format
/// and throws the spec TypeError, so an unknown one here is a bug. An
/// isolate already holding [`MAX_LIVE_ZLIB_STREAMS`] gets a `RangeError`
/// and no native state.
pub(super) fn op_zlib_stream_new(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let format = args.get(0).to_rust_string_lossy(scope);
    let decompress = args.get(1).boolean_value(scope);
    let streams = zlib_streams(scope);
    if streams.live.len() >= MAX_LIVE_ZLIB_STREAMS {
        let message = v8::String::new(
            scope,
            &format!(
                "Too many open compression streams in this isolate (limit \
                 {MAX_LIVE_ZLIB_STREAMS}); close, cancel or abort the ones \
                 no longer in use."
            ),
        )
        .unwrap();
        let exception = v8::Exception::range_error(scope, message);
        scope.throw_exception(exception);
        return;
    }
    let stream = ZlibStream::new(&format, decompress).expect("JS validated the compression format");
    streams.next += 1;
    let id = streams.next;
    streams.live.insert(id, stream);
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `__zlib_stream_push(id, view)` -> Uint8Array (possibly empty). An
/// error poisons the stream: its native state is dropped and the
/// TypeError errors both sides of the transform.
pub(super) fn op_zlib_stream_push(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let streams = &mut zlib_streams(scope).live;
    let result = match streams.get_mut(&id) {
        Some(stream) => {
            let out = stream.push(&bytes);
            if out.is_err() {
                streams.remove(&id);
            }
            out
        }
        None => Err(anyhow!("zlib stream already closed")),
    };
    zlib_stream_return(scope, rv, result);
}

/// `__zlib_stream_end(id)` -> the final Uint8Array. Consumes the stream.
pub(super) fn op_zlib_stream_end(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let result = zlib_streams(scope)
        .live
        .remove(&id)
        .ok_or_else(|| anyhow!("zlib stream already closed"));
    zlib_stream_return(scope, rv, result.and_then(ZlibStream::finish));
}

/// `__zlib_stream_drop(id)` — cancelled/aborted transform cleanup, and the
/// `FinalizationRegistry` release of an abandoned stream. An id that
/// already ended is a no-op.
pub(super) fn op_zlib_stream_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    zlib_streams(scope).live.remove(&id);
}

/// Native stream lifetimes, observed in the isolate's own table: every way
/// a stream ends releases its entry, the cap refuses the stream past it,
/// collection releases an abandoned stream, and dropping the isolate drops
/// the table. Every stream here is a small deflate-raw decoder or a short
/// round trip, so a full table stays near 10 MiB.
#[cfg(test)]
mod tests {
    use super::{ZlibStreams, MAX_LIVE_ZLIB_STREAMS};
    use crate::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
    use std::sync::Arc;
    use std::time::Instant;

    const SCRIPT: &str = r#"
const held = [];
async function drain(readable) {
  const reader = readable.getReader();
  const parts = [];
  let length = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    parts.push(value);
    length += value.byteLength;
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) { out.set(part, offset); offset += part.byteLength; }
  return out;
}
async function through(stream, input) {
  const writer = stream.writable.getWriter();
  const output = drain(stream.readable);
  await writer.write(input);
  await writer.close();
  return output;
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const n = Number(url.searchParams.get("n") ?? 0);
    switch (url.pathname) {
      case "/roundtrip": {
        const input = new TextEncoder().encode("cello ".repeat(4096));
        for (const format of ["gzip", "deflate", "deflate-raw"]) {
          const packed = await through(new CompressionStream(format), input);
          const back = await through(new DecompressionStream(format), packed);
          if (packed.byteLength >= input.byteLength ||
              back.byteLength !== input.byteLength ||
              !back.every((byte, i) => byte === input[i]))
            return Response.json({ ok: false, format });
        }
        return Response.json({ ok: true });
      }
      case "/gzip-frames": {
        // The three outcomes workerd distinguishes on a decompression
        // stream: an unfinished member, a checksum that does not check out,
        // and bytes past the member (#230).
        const payload = new TextEncoder().encode("hello world hello world");
        const packed = await through(new CompressionStream("gzip"), payload);
        const flipped = packed.slice();
        flipped[flipped.length - 1] ^= 0xff;
        const trailing = new Uint8Array(packed.byteLength + 3);
        trailing.set(packed);
        trailing.set([1, 2, 3], packed.byteLength);
        const attempt = async (bytes) => {
          const stream = new DecompressionStream("gzip");
          const writer = stream.writable.getWriter();
          const read = drain(stream.readable).then(() => "resolved", (e) => e.message);
          const write = await writer.write(bytes).then(() => "resolved", (e) => e.message);
          const close = await writer.close().then(() => "resolved", (e) => e.message);
          return { write, close, read: await read };
        };
        return Response.json({
          normal: await attempt(packed),
          missingTrailer: await attempt(packed.slice(0, packed.byteLength - 8)),
          badChecksum: await attempt(flipped),
          trailing: await attempt(trailing),
        });
      }
      case "/release": {
        // Held, so only the stream's own ending can release its entry.
        const cancelled = new CompressionStream("gzip");
        held.push(cancelled);
        await cancelled.readable.cancel("done");
        const aborted = new DecompressionStream("deflate");
        held.push(aborted);
        await aborted.writable.abort("done");
        const poisoned = new DecompressionStream("gzip");
        held.push(poisoned);
        const writer = poisoned.writable.getWriter();
        const read = drain(poisoned.readable).then(() => "resolved", (e) => e.name);
        const wrote = await writer.write(new Uint8Array(16).fill(7))
          .then(() => "resolved", (e) => e.name);
        // A non-BufferSource chunk fails before any push, so the transform
        // must still release its native entry.
        const invalid = new CompressionStream("gzip");
        held.push(invalid);
        const iwriter = invalid.writable.getWriter();
        const iread = drain(invalid.readable).then(() => "resolved", (e) => e.name);
        const iwrote = await iwriter.write(123)
          .then(() => "resolved", (e) => e.name);
        return Response.json({ wrote, read: await read, iwrote, iread: await iread });
      }
      case "/fill":
        for (let i = 0; i < n; i++) held.push(new DecompressionStream("deflate-raw"));
        return Response.json({ ok: true });
      case "/one":
        try {
          held.push(new DecompressionStream("deflate-raw"));
          return Response.json({ ok: true });
        } catch (e) {
          return Response.json({ ok: false, name: e.name, message: e.message });
        }
      case "/cancel-one":
        await held.pop().readable.cancel("done");
        return Response.json({ ok: true });
      case "/abandon":
        for (let i = 0; i < n; i++) new DecompressionStream("deflate-raw");
        return Response.json({ ok: true });
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

    /// Runs `test` on a thread created after V8 initialised: the PKRU
    /// access an isolate needs is inherited at thread creation. The test-only
    /// constructor in `runtime` has already initialised V8 on the process's
    /// initial thread (see `init_v8`), so no libtest thread can be the one
    /// that wins the one-time grant; this call only states the dependency.
    fn on_engine_thread(test: impl FnOnce() + Send + 'static) {
        crate::runtime::init_v8();
        crate::asyncrt::shared_test_runtime();
        std::thread::spawn(test).join().expect("engine thread");
    }

    fn load() -> Worker {
        let config = WorkerConfig::new(WorkerConfigOptions {
            src: SCRIPT.to_string(),
            script_name: "compression-lifecycle".to_string(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            r2_bindings: Vec::new(),
            d1_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        });
        Worker::load_config(Arc::new(config)).expect("load worker")
    }

    /// One request whose handler settles on promises alone, so its first
    /// turn is the whole request. Answers the JSON body.
    fn fetch(worker: &mut Worker, path: &str) -> serde_json::Value {
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: format!("http://test{path}"),
            method: "GET".to_string(),
            body: RequestBody::Bytes(bytes::Bytes::new()),
            headers: Vec::new(),
            request_id: None,
            tail_report: None,
            reply,
        };
        let (entry, started) = worker.turn_begin(job, None);
        drop(started);
        let mut entry = entry.expect("request starts");
        entry.abandon();
        drop(entry);
        let response = receive
            .try_recv()
            .expect("answered in one turn")
            .expect("ok answer");
        assert_eq!(response.status, 200, "{path} answered {}", response.status);
        serde_json::from_slice(&response.body).expect("json body")
    }

    fn live(worker: &Worker) -> usize {
        let inner = worker.inner.as_ref().expect("live worker");
        let (locker, _cells) = inner.lock();
        locker
            .get_slot::<ZlibStreams>()
            .map_or(0, |streams| streams.live.len())
    }

    /// A full collection, then the foreground tasks it posted — V8 runs
    /// `FinalizationRegistry` callbacks from one — until none is runnable.
    fn collect(worker: &mut Worker) {
        worker.settle_heap();
        let inner = worker.inner.as_ref().expect("live worker");
        let (locker, _cells) = inner.lock();
        let platform = v8::V8::get_current_platform();
        for _ in 0..1024 {
            if !v8::Platform::pump_message_loop(&platform, &locker, false) {
                return;
            }
        }
        panic!("foreground tasks kept coming after 1024 runs");
    }

    /// A gzip member is framed natively: an unfinished member is the
    /// incomplete-data error on close, a bad checksum fails the push that
    /// carries the trailer, and bytes past the member are their own error.
    #[test]
    fn gzip_frames_report_workerd_outcomes() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/gzip-frames");
            assert_eq!(report["normal"]["write"], "resolved", "{report}");
            assert_eq!(report["normal"]["close"], "resolved", "{report}");
            assert_eq!(report["missingTrailer"]["write"], "resolved", "{report}");
            assert_eq!(
                report["missingTrailer"]["close"],
                "Called close() on a decompression stream with incomplete data",
                "{report}"
            );
            assert_eq!(
                report["missingTrailer"]["read"],
                report["missingTrailer"]["close"]
            );
            assert_eq!(
                report["badChecksum"]["write"], "Decompression failed.",
                "{report}"
            );
            assert_eq!(
                report["badChecksum"]["close"], "This WritableStream has been closed.",
                "{report}"
            );
            assert_eq!(
                report["trailing"]["write"], "Trailing bytes after end of compressed data",
                "{report}"
            );
            assert_eq!(live(&worker), 0);
        });
    }

    /// Every format round-trips chunk by chunk, and a stream that flushes
    /// leaves nothing behind.
    #[test]
    fn round_trip_releases_on_flush() {
        on_engine_thread(|| {
            let mut worker = load();
            assert_eq!(fetch(&mut worker, "/roundtrip")["ok"], true);
            assert_eq!(live(&worker), 0);
        });
    }

    /// Cancel, abort, a poisoning push and an invalid chunk each release
    /// the native stream while JS still holds the stream object.
    #[test]
    fn cancel_abort_and_push_error_release_while_held() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/release");
            assert_eq!(report["wrote"], "TypeError", "{report}");
            assert_eq!(report["read"], "TypeError", "{report}");
            assert_eq!(report["iwrote"], "TypeError", "{report}");
            assert_eq!(report["iread"], "TypeError", "{report}");
            assert_eq!(live(&worker), 0);
        });
    }

    /// The stream past the cap is refused with a `RangeError` and no
    /// native state; releasing one admits the next.
    #[test]
    fn cap_refuses_the_stream_past_it() {
        on_engine_thread(|| {
            let mut worker = load();
            fetch(&mut worker, &format!("/fill?n={MAX_LIVE_ZLIB_STREAMS}"));
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
            let refused = fetch(&mut worker, "/one");
            assert_eq!(refused["ok"], false, "{refused}");
            assert_eq!(refused["name"], "RangeError", "{refused}");
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
            fetch(&mut worker, "/cancel-one");
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS - 1);
            assert_eq!(fetch(&mut worker, "/one")["ok"], true);
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
        });
    }

    /// Streams nobody closed and nobody references are released once the
    /// collector finds them, which frees the cap for new streams.
    #[test]
    fn abandoned_streams_release_after_collection() {
        on_engine_thread(|| {
            let mut worker = load();
            fetch(&mut worker, &format!("/abandon?n={MAX_LIVE_ZLIB_STREAMS}"));
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
            collect(&mut worker);
            assert_eq!(live(&worker), 0);
            assert_eq!(fetch(&mut worker, "/one")["ok"], true);
        });
    }

    /// The cap and the table belong to one isolate: a full isolate does not
    /// refuse another's stream, and dropping it drops every stream it still
    /// held, reachable from JS or not.
    #[test]
    fn isolate_teardown_drops_its_streams() {
        on_engine_thread(|| {
            let mut full = load();
            let mut other = load();
            fetch(&mut full, &format!("/fill?n={MAX_LIVE_ZLIB_STREAMS}"));
            assert_eq!(fetch(&mut other, "/one")["ok"], true);
            let probe = Arc::new(());
            let released = Arc::downgrade(&probe);
            {
                let inner = full.inner.as_ref().expect("live worker");
                let (mut locker, _cells) = inner.lock();
                let streams = locker
                    .get_slot_mut::<ZlibStreams>()
                    .expect("the isolate made streams");
                assert_eq!(streams.live.len(), MAX_LIVE_ZLIB_STREAMS);
                streams.teardown_probe = Some(probe);
            }
            drop(full);
            assert!(
                released.upgrade().is_none(),
                "the retired isolate's stream table outlived it"
            );
            assert_eq!(live(&other), 1);
        });
    }
}
