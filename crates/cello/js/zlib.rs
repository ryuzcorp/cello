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
                let mut encoder = flate2::GzBuilder::new()
                    .operating_system(3)
                    .write(Vec::new(), ZlibStream::LEVEL);
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
                let mut encoder = ZlibFramed::new(ZlibStream::LEVEL);
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
    ZlibEncode(ZlibFramed),
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

/// A raw deflate encoder wrapped in the zlib framing workerd writes: the
/// header is always `78 9c` (FLEVEL 2, what zlib emits for level 6) even
/// though the body is compressed at workerd's level 7, and the trailer is
/// the Adler-32 of the input in big-endian order. flate2's own
/// `ZlibEncoder` derives FLEVEL from the level (`78 da`), which is the one
/// observable difference left in the `deflate` stream's bytes (#245).
struct ZlibFramed {
    inner: flate2::write::DeflateEncoder<Vec<u8>>,
    adler: adler2::Adler32,
    header: bool,
}

impl ZlibFramed {
    /// The zlib FCHECK-valid header workerd writes: CMF 0x78 (deflate, 32K
    /// window), FLG 0x9c (FLEVEL 2, no dictionary, check bits).
    const HEADER: [u8; 2] = [0x78, 0x9c];

    fn new(level: flate2::Compression) -> Self {
        ZlibFramed {
            inner: flate2::write::DeflateEncoder::new(Vec::new(), level),
            adler: adler2::Adler32::new(),
            header: false,
        }
    }

    fn write_header(&mut self) {
        if self.header {
            return;
        }
        self.header = true;
        self.inner.get_mut().extend_from_slice(&Self::HEADER);
    }

    fn get_mut(&mut self) -> &mut Vec<u8> {
        self.inner.get_mut()
    }

    fn finish(mut self) -> std::io::Result<Vec<u8>> {
        self.write_header();
        let mut out = self.inner.finish()?;
        out.extend_from_slice(&self.adler.checksum().to_be_bytes());
        Ok(out)
    }
}

impl std::io::Write for ZlibFramed {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_header();
        // flate2's codec hands back a partial count when its output buffer
        // fills; the caller re-writes the remainder, so only the consumed
        // bytes may count towards the trailer (flate2's own `GzEncoder`
        // does the same two lines away).
        let n = self.inner.write(buf)?;
        self.adler.write_slice(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.write_header();
        self.inner.flush()
    }
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
const TRAILING: &str = "Trailing bytes after end of compressed data";

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
                        return Err(anyhow!(TRAILING));
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
    /// Workerd's encoder settings: zlib at level 7 (measured: its gzip bytes
    /// equal zlib's level 7 for every probe payload, and differ from level 6
    /// on inputs where the levels diverge), mtime 0, XFL 0 and the header's
    /// OS byte 3 (Unix), with the zlib backend behind it (#245).
    const LEVEL: flate2::Compression = flate2::Compression::new(7);

    fn new(format: &str, decompress: bool) -> Option<ZlibStream> {
        use flate2::write as z;
        let level = Self::LEVEL;
        let sink = Vec::new;
        Some(match (format, decompress) {
            ("gzip", false) => Self::GzipEncode(
                flate2::GzBuilder::new()
                    .operating_system(3)
                    .write(sink(), level),
            ),
            ("gzip", true) => Self::GzipDecode(Box::new(GzipDecode::new())),
            ("deflate", false) => Self::ZlibEncode(ZlibFramed::new(level)),
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
    /// output flows per chunk; encoders return whatever the compressor
    /// emitted for this push -- the gzip/zlib header on the first one, and
    /// nothing more until deflate has a full block or the stream ends.
    /// Workerd enqueues a chunk per push the same way (#257), while the
    /// concatenation of every push and `finish` is the same byte stream. A
    /// decoder that stops consuming input has hit the end of the
    /// compressed stream, so leftover bytes are trailing garbage --
    /// Workerd's strict_compression_checks. Gzip frames and validates its
    /// own bytes (see [`GzipDecode`]).
    fn push(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;
        if let Self::GzipDecode(decoder) = self {
            return decoder.push(input);
        }
        let failure = self.failure();
        // A decoder that stops consuming has reached the end of its stream,
        // so the rest of the push is trailing garbage; workerd names that
        // case separately from a corrupt stream (#245).
        let stop = if self.decompress() { TRAILING } else { failure };
        zlib_each!(self, w => {
            while !input.is_empty() {
                let n = w.write(input).map_err(|_| anyhow!(failure))?;
                if n == 0 { return Err(anyhow!(stop)); }
                input = &input[n..];
            }
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
                 {MAX_LIVE_ZLIB_STREAMS}); close or abort the ones \
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
      case "/encoded-bytes": {
        // Workerd's encoder settings: zlib level 7 with mtime 0, OS 3 and the
        // deflate header 78 9c (#245). The expected hex is workerd's own
        // output for "hello".
        const hex = (bytes) =>
          [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
        const payload = new TextEncoder().encode("hello");
        const out = {};
        for (const format of ["gzip", "deflate", "deflate-raw"]) {
          out[format] = hex(await through(new CompressionStream(format), payload));
        }
        return Response.json(out);
      }
      case "/deflate-chunking": {
        // The Adler-32 trailer must cover exactly the bytes the encoder
        // consumed, so a write that flate2 splits at its 32 KiB output
        // buffer still produces a decodable stream that equals the same
        // input written in one piece (#245 review).
        const size = 1024 * 1024;
        const data = new Uint8Array(size);
        let seed = 7;
        for (let i = 0; i < size; i += 1) {
          seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
          data[i] = (seed >>> 16) & 0xff;
        }
        const digest = async (bytes) => {
          const hash = await crypto.subtle.digest("SHA-256", bytes);
          return [...new Uint8Array(hash)]
            .map((b) => b.toString(16).padStart(2, "0")).join("");
        };
        const encode = async (chunks) => {
          const stream = new CompressionStream("deflate");
          const writer = stream.writable.getWriter();
          const read = drain(stream.readable);
          for (const chunk of chunks) await writer.write(chunk);
          await writer.close();
          return await read;
        };
        const decode = async (bytes) => {
          const stream = new DecompressionStream("deflate");
          const writer = stream.writable.getWriter();
          const read = drain(stream.readable);
          await writer.write(bytes);
          await writer.close();
          return await read;
        };
        const pieces = (step) => {
          const out = [];
          for (let at = 0; at < size; at += step)
            out.push(data.subarray(at, Math.min(size, at + step)));
          return out;
        };
        const single = await encode([data]);
        const chunked = await encode(pieces(65536));
        const odd = await encode(pieces(70000));
        const back = await decode(single);
        return Response.json({
          source: await digest(data),
          single: await digest(single),
          chunked: await digest(chunked),
          odd: await digest(odd),
          decoded: await digest(back),
          decodedLength: back.byteLength,
        });
      }
      case "/release": {
        // Held, so only the stream's own ending can release its entry. A
        // readable cancel ends the compressor once nothing accepted is
        // still outstanding (#270); the abort that follows drops an
        // already-ended stream as a no-op, as does the abort on a stream
        // that was never cancelled.
        const cancelled = new CompressionStream("gzip");
        held.push(cancelled);
        await cancelled.readable.cancel("done");
        await cancelled.writable.abort("done");
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
      case "/close-cancel-loop": {
        // writer.close() around a same-turn readable cancel, both orderings
        // and with no write. Every round must release its native stream on
        // its own: the wrappers stay held, so nothing here waits for the
        // collector, and the 128th leaked stream would already be a
        // RangeError (#262).
        const errors = [];
        const settle = async (promise, label) => {
          try {
            await promise;
          } catch (e) {
            errors.push(`${label}: ${e.message}`);
          }
        };
        for (let round = 0; round < 300; round += 1) {
          for (const mode of ["queued", "inflight", "noWrite"]) {
            const stream = new CompressionStream("gzip");
            held.push(stream);
            const writer = stream.writable.getWriter();
            const reader = stream.readable.getReader();
            const pull = reader.read().catch(() => {});
            if (mode !== "queued") await null;
            const write = mode === "noWrite"
              ? null
              : writer.write(new Uint8Array(64).fill(0x61));
            const close = writer.close();
            const cancel = reader.cancel("done");
            if (write !== null) await settle(write, `${mode} write ${round}`);
            await settle(close, `${mode} close ${round}`);
            await settle(cancel, `${mode} cancel ${round}`);
            await pull;
          }
        }
        return Response.json({
          ok: errors.length === 0,
          errors: errors.slice(0, 4),
          held: held.length,
        });
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
        // A readable cancel ends the native stream once nothing accepted
        // is outstanding (#270): the filled stream never took a write, so
        // cancelling frees its slot with no close. Closing would free it
        // too -- the end consumes the entry before running it, even when
        // the flush fails.
        await held.pop().readable.cancel("done");
        return Response.json({ ok: true });
      case "/cancel-with-write": {
        // The write is accepted before the cancel, so it still runs; the
        // cancel records itself and the drain that follows ends the
        // compressor, stashing the flush outcome, and the close that
        // follows replays it and frees nothing further (#270).
        const stream = new CompressionStream("gzip");
        held.push(stream);
        const writer = stream.writable.getWriter();
        const write = writer.write(new Uint8Array(64).fill(0x61))
          .then(() => "ok", (e) => e.name);
        const cancel = stream.readable.cancel("done").then(() => "ok", (e) => e.name);
        const writeResult = await write;
        const cancelResult = await cancel;
        const close = await writer.close().then(() => "ok", (e) => e.name);
        return Response.json({ write: writeResult, cancel: cancelResult, close });
      }
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

    /// A deflate write larger than flate2's 32 KiB output buffer is split
    /// inside the encoder; the Adler-32 must cover only the bytes it
    /// consumed, so chunked and single-shot output are identical and the
    /// result decodes (through flate2's `ZlibDecoder`, which the
    /// `DecompressionStream` path uses). Regression for #245's review.
    #[test]
    fn deflate_chunking_stays_lossless() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/deflate-chunking");
            let source = report["source"].as_str().expect("digest").to_string();
            assert_eq!(report["single"], report["chunked"], "{report}");
            assert_eq!(report["single"], report["odd"], "{report}");
            assert_eq!(report["decoded"], source, "{report}");
            assert_eq!(report["decodedLength"], 1024 * 1024, "{report}");
            assert_eq!(live(&worker), 0);
        });
    }

    /// The encoders write workerd's bytes: zlib level 7, gzip mtime 0 with
    /// OS 3, and the deflate header 78 9c. The expected hex is workerd
    /// 1.20261121.1's own output for "hello" (#245).
    #[test]
    fn encoders_write_workerd_bytes() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/encoded-bytes");
            assert_eq!(
                report["gzip"], "1f8b0800000000000003cb48cdc9c9070086a6103605000000",
                "{report}"
            );
            assert_eq!(report["deflate"], "789ccb48cdc9c90700062c0215", "{report}");
            assert_eq!(report["deflate-raw"], "cb48cdc9c90700", "{report}");
            assert_eq!(live(&worker), 0);
        });
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

    /// A readable cancel ends its compressor once nothing accepted is
    /// still outstanding, an abort drops its stream, and a poisoning push
    /// or an invalid chunk each release the native stream too -- all while
    /// JS still holds the stream object.
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
    /// native state; releasing one admits the next. A readable cancel
    /// ends the compressor once nothing accepted is outstanding, so
    /// cancelling -- with no close -- is what admits the next stream
    /// here (#270).
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

    /// A readable cancel that arrives behind an accepted write lets that
    /// write run; the drain that follows ends the compressor and stashes
    /// the flush outcome, so a close that follows replays it and the slot
    /// count stays at zero (#270).
    #[test]
    fn close_after_cancel_releases_the_slot() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/cancel-with-write");
            assert_eq!(report["write"], "ok", "{report}");
            assert_eq!(report["cancel"], "ok", "{report}");
            assert_eq!(report["close"], "ok", "{report}");
            assert_eq!(live(&worker), 0);
        });
    }

    /// 300 rounds of writer.close() across a same-turn readable cancel, in
    /// both orderings and with no write, release every native stream on its
    /// own: the wrappers stay held, so a leak shows up as the cap's
    /// `RangeError` well before the last round rather than as a later
    /// collection (#262).
    #[test]
    fn close_across_cancel_releases_every_round() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/close-cancel-loop");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["held"], 900, "{report}");
            assert_eq!(live(&worker), 0, "{report}");
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
