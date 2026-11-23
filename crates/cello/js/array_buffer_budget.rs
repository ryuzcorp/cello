// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): one isolate's ArrayBuffer backing stores are
// charged to a per-isolate budget and refused past it.
// Apache-2.0.

//! A per-isolate ceiling on `ArrayBuffer` backing-store memory.
//!
//! V8's default ArrayBuffer allocator is `malloc`, so `new ArrayBuffer(n)` in
//! a guest was an unbounded allocation inside the shared process:
//! `near_heap_limit` extends the V8 heap and never fails the allocation, and
//! backing stores are external memory, not heap. A loop of
//! `new Uint8Array(8 << 20)` therefore grew the node without limit while the
//! isolate's heap stayed near empty.
//!
//! Each isolate now gets an allocator of its own
//! (`CreateParams::array_buffer_allocator`) that charges every backing store to
//! [`BackingStoreBudget`] and returns null past the cap. V8 turns that null
//! into `RangeError: Array buffer allocation failed` at the allocation site,
//! so the guest sees a catchable failure rather than the process growing, and
//! the refusal condemns the isolate the way the heap guard does. Freeing a
//! store (garbage collection) releases its bytes, so an isolate that churns
//! buffers is never condemned for memory it no longer holds.
//!
//! Two facts about V8 152 shape the coverage:
//!
//! - `v8::ArrayBuffer::New` on the **embedder** API aborts the process when
//!   the allocator returns null (`FatalProcessOutOfMemory:
//!   v8::ArrayBuffer::New`), so the host may not allocate backing stores that
//!   way once a cap exists. [`host_store`] hands V8 a store built from the
//!   host's own `Vec`, charged to the same budget and refused in Rust with a
//!   `RangeError` instead.
//! - Resizable `ArrayBuffer`s, growable `SharedArrayBuffer`s and
//!   `WebAssembly.Memory` do **not** reach the embedder allocator: V8 backs
//!   them with the platform page allocator
//!   (`BackingStore::TryAllocateAndPartiallyCommitMemory`), by design. No
//!   allocator hook can refuse a `resize`/`grow`, so this budget does not
//!   cover them. V8 counts a resized `ArrayBuffer` in `external_memory` (a
//!   growable `SharedArrayBuffer` and a wasm memory are not counted at all),
//!   so its own heap heuristics still see that one, but the budget cannot.
//!   They are the documented gap: `--wasm-max-mem-pages` bounds one wasm
//!   memory's growth in pages (not the number of memories), and
//!   `docs/reference/limits.md` states the rest. A page-allocator hook in
//!   rusty_v8 is the change that would cover them.
//!
//! The invariants this module keeps:
//!
//! - one isolate's accounting never charges another's: the budget travels
//!   with the allocator V8 holds for that isolate, and every store is freed
//!   by the deleter that charged it;
//! - an allocation past the cap fails closed: the reservation is refused
//!   before memory is taken, and a refusal that reaches JavaScript (a host
//!   store's, or the turn-boundary reading of retained use) sets the
//!   isolate's condemned flag. A null return from the allocator does
//!   not: V8 may collect and retry it, and a retry that succeeds never fails
//!   the guest. The one allocator refusal that cannot be retried — a request
//!   larger than the whole cap — latches, because no collection can make room
//!   for it;
//! - the process never aborts: every host-created store is charged in Rust
//!   before it exists, and the only null return V8 can see is from a JS
//!   allocation, which V8 handles by throwing.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Backing-store ceiling as a multiple of the isolate's V8 heap limit, used
/// when `CELLO_V8_EXTERNAL_LIMIT_MB` is unset.
///
/// Four times the heap leaves room for everything one request legitimately
/// holds at once — a 64 MiB request body or `/tmp` read, a 32 MiB WebSocket
/// message or tee skew, a 32 MiB decompression output, and a copy of any of
/// them — and still bounds one isolate to five heap limits in total. The
/// Cloudflare Workers isolate limit is 128 MB for heap and buffers together;
/// see `docs/reference/cloudflare-compat.md`.
pub(super) const EXTERNAL_LIMIT_HEAP_MULTIPLE: usize = 4;

/// The backing-store ceiling for an isolate whose V8 heap limit is
/// `heap_limit` bytes.
///
/// `CELLO_V8_EXTERNAL_LIMIT_MB` overrides the multiple. It is validated at
/// startup like `CELLO_V8_HEAP_LIMIT_MB`, so the multiplication below cannot
/// overflow.
pub(super) fn external_limit_bytes(heap_limit: usize) -> usize {
    crate::env_vars::positive::<usize>("CELLO_V8_EXTERNAL_LIMIT_MB")
        .expect("validated CELLO_V8_EXTERNAL_LIMIT_MB")
        .map(|megabytes| {
            megabytes
                .checked_mul(1024 * 1024)
                .expect("validated CELLO_V8_EXTERNAL_LIMIT_MB range")
        })
        .unwrap_or_else(|| heap_limit.saturating_mul(EXTERNAL_LIMIT_HEAP_MULTIPLE))
}

/// One isolate's backing-store accounting: what it may hold, what it holds,
/// and whether an allocation past the ceiling was refused.
///
/// Shared by the allocator V8 calls and by the host's own stores, so both
/// draw on the same budget. Atomics rather than locks: V8 may free a store
/// from a collection on another thread, and an allocator callback may not
/// block or call back into V8.
pub(super) struct BackingStoreBudget {
    /// The ceiling, in bytes. Atomic only so a test can lower it for one
    /// isolate without a second constructor; production never writes it.
    cap: AtomicUsize,
    used: AtomicUsize,
    /// Latched by the refusal of an allocation past `cap`. It is the
    /// external-memory half of the isolate's condemnation: `recover_heap`
    /// clears it once usage falls back under the recovery share, exactly as
    /// it clears the heap-limit flag.
    exceeded: AtomicBool,
}

impl BackingStoreBudget {
    pub(super) fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap: AtomicUsize::new(cap),
            used: AtomicUsize::new(0),
            exceeded: AtomicBool::new(false),
        })
    }

    pub(super) fn cap(&self) -> usize {
        self.cap.load(Ordering::Relaxed)
    }

    /// Lower (or raise) the ceiling. Test-only: a guest test that filled four
    /// heap limits of buffers would hold 500 MiB per test in parallel.
    #[cfg(test)]
    pub(super) fn set_cap(&self, cap: usize) {
        self.cap.store(cap, Ordering::Relaxed);
    }

    pub(super) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub(super) fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Relaxed)
    }

    pub(super) fn clear_exceeded(&self) {
        self.exceeded.store(false, Ordering::Relaxed);
    }

    /// Latch the condemnation without a refusal. `recover_heap` uses it for
    /// an isolate whose retained use is still over the recovery share after a
    /// collection: a refusal V8's own retry absorbed is not evidence, but the
    /// bytes the isolate still holds are.
    pub(super) fn latch_exceeded(&self) {
        self.exceeded.store(true, Ordering::Relaxed);
    }

    /// Whether the bytes this allocator handed out are at or above `share` of
    /// the cap. `recover_heap` measures this the way it measures the heap.
    pub(super) fn used_share(&self) -> f64 {
        let cap = self.cap();
        if cap == 0 {
            return 1.0;
        }
        self.used() as f64 / cap as f64
    }

    /// Reserve `len` bytes, or refuse.
    ///
    /// A compare-exchange loop, not a fetch-add followed by a check: two
    /// concurrent allocations must not both see room for the last bytes. A
    /// refusal is *not* latched here: V8 answers a null from the ArrayBuffer
    /// allocator by collecting and retrying the same allocation
    /// (`heap-allocator.cc: RetryCustomAllocate`), and when the retry succeeds
    /// the guest never sees an error. Latching that attempt would condemn an
    /// isolate for a refusal nobody saw. [`Self::reserve_latching`] latches,
    /// because its refusals do reach the guest, and `recover_heap` latches on
    /// retained use after a collection.
    fn try_reserve(&self, len: usize) -> bool {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = used.checked_add(len) else {
                return false;
            };
            if next > self.cap() {
                return false;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(actual) => used = actual,
            }
        }
    }

    /// Reserve bytes for a refusal the guest is about to see — a host store —
    /// latching the condemnation when the reservation is refused.
    ///
    /// The refusal becomes a `RangeError` at the guest's call site, so the
    /// isolate genuinely failed to allocate: same signal as the heap guard's.
    /// The allocator keeps [`Self::try_reserve`] instead, because V8 may retry
    /// a null return after a collection.
    pub(super) fn reserve_latching(&self, len: usize) -> bool {
        if self.try_reserve(len) {
            return true;
        }
        self.exceeded.store(true, Ordering::Relaxed);
        false
    }

    pub(super) fn release(&self, len: usize) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                Some(used.saturating_sub(len))
            });
    }
}

/// A host-owned store this isolate's budget refused.
///
/// Deliberately a value, not a scheduled V8 exception: V8 forbids running
/// JavaScript operations once an exception is pending, and a conversion may
/// have to abandon a half-built row, batch or argument list. The caller
/// decides how the guest learns about it — a rejected promise, a throw at the
/// op boundary, or an error on an internal reply channel — and stops touching
/// V8 until it has.
#[derive(Clone, Copy, Debug)]
pub(super) struct HostStoreRefusal {
    /// The ceiling the bytes would have crossed, for the guest-facing message.
    pub(super) cap: usize,
    /// The bytes the caller asked for. Past the whole cap no collection can
    /// make room, so the caller can tell a squeeze from a batch that can
    /// never fit and stop asking.
    pub(super) requested: usize,
}

impl std::fmt::Display for HostStoreRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Array buffer allocation failed: the isolate's ArrayBuffer memory \
             budget is exhausted ({} MiB, see CELLO_V8_EXTERNAL_LIMIT_MB); free \
             buffers before allocating more",
            self.cap / (1024 * 1024)
        )
    }
}

impl std::error::Error for HostStoreRefusal {}

/// Backing-store alignment. V8 asks for at least 8-byte alignment (a
/// `Float64Array` view over the store); the system allocator provides more.
const ALIGN: usize = 8;

fn layout(len: usize) -> Option<std::alloc::Layout> {
    std::alloc::Layout::from_size_align(len, ALIGN).ok()
}

/// What a zero-length store's `free` sees. V8 does not call the allocator for
/// an empty `ArrayBuffer` in 152, but the vtable must still answer one, and
/// the pointer must be non-null and freeable: `free` ignores it and `dealloc`
/// is never reached with a zero-size layout.
fn empty_pointer() -> *mut c_void {
    std::ptr::NonNull::<u8>::dangling().as_ptr().cast()
}

fn allocate(budget: &BackingStoreBudget, len: usize, zeroed: bool) -> *mut c_void {
    if len == 0 {
        return empty_pointer();
    }
    if !budget.try_reserve(len) {
        // A request for more than the whole cap can never be satisfied — no
        // collection returns bytes the cap never had — so this refusal is
        // V8's last and the guest sees the `RangeError`. Latch it. A smaller
        // refusal stays unlatched: V8 collects and retries that one, and a
        // retry that succeeds never fails the guest.
        if len > budget.cap() {
            budget.latch_exceeded();
        }
        return std::ptr::null_mut();
    }
    let Some(layout) = layout(len) else {
        // Unreachable for a valid `usize` length the budget admitted, but
        // must not abort: a null return is the allocator's only failure.
        budget.release(len);
        return std::ptr::null_mut();
    };
    // SAFETY: `len` is non-zero, so the layout has a non-zero size.
    let pointer = unsafe {
        if zeroed {
            std::alloc::alloc_zeroed(layout)
        } else {
            std::alloc::alloc(layout)
        }
    };
    if pointer.is_null() {
        budget.release(len);
        return std::ptr::null_mut();
    }
    pointer.cast()
}

unsafe extern "C" fn allocate_zeroed(budget: &BackingStoreBudget, len: usize) -> *mut c_void {
    allocate(budget, len, true)
}

unsafe extern "C" fn allocate_uninitialized(
    budget: &BackingStoreBudget,
    len: usize,
) -> *mut c_void {
    allocate(budget, len, false)
}

unsafe extern "C" fn free(budget: &BackingStoreBudget, data: *mut c_void, len: usize) {
    if data.is_null() || len == 0 {
        return;
    }
    // SAFETY: `data` and `len` are exactly what `allocate` returned and
    // charged; `layout` succeeds for anything `allocate` accepted.
    if let Some(layout) = layout(len) {
        unsafe { std::alloc::dealloc(data.cast(), layout) };
        budget.release(len);
    }
}

unsafe extern "C" fn drop_budget(handle: *const BackingStoreBudget) {
    if handle.is_null() {
        return;
    }
    // SAFETY: `allocator` leaked exactly one `Arc::into_raw` of this budget,
    // and V8 calls the vtable's `drop` once, after the isolate is gone.
    unsafe { drop(Arc::from_raw(handle)) };
}

static VTABLE: v8::RustAllocatorVtable<BackingStoreBudget> = v8::RustAllocatorVtable {
    allocate: allocate_zeroed,
    allocate_uninitialized,
    free,
    drop: drop_budget,
};

/// The isolate's allocator: V8 holds a `shared_ptr` to it for the isolate's
/// lifetime, and it holds one `Arc` reference to `budget`.
///
/// # Panics
///
/// Never. V8 requires the allocator at `CreateParams` time; a budget is
/// created for every isolate.
pub(super) fn allocator(budget: &Arc<BackingStoreBudget>) -> v8::UniqueRef<v8::Allocator> {
    // SAFETY: the handle is an `Arc<BackingStoreBudget>` kept alive by the
    // allocator, and `VTABLE` matches that type for all four entries.
    unsafe { v8::new_rust_allocator(Arc::into_raw(budget.clone()), &VTABLE) }
}

/// What the deleter of a host-owned store releases when V8 frees the store.
struct HostCharge {
    budget: Arc<BackingStoreBudget>,
    /// The bytes the store's allocation was charged for: its `capacity`, not
    /// its length.
    charged: usize,
    /// The store's memory, kept only for its destructor: dropping this `Vec`
    /// after the `Box` is reconstructed frees the buffer V8 pointed at, and
    /// moving the `Vec` here never reallocates it.
    _bytes: Vec<u8>,
}

unsafe extern "C" fn release_host_store(
    _data: *mut c_void,
    _byte_length: usize,
    deleter_data: *mut c_void,
) {
    if deleter_data.is_null() {
        return;
    }
    // SAFETY: `host_store` boxed exactly one `HostCharge` per store and gave
    // V8 its raw pointer as the deleter data; V8 calls this once.
    let charge = unsafe { Box::from_raw(deleter_data.cast::<HostCharge>()) };
    charge.budget.release(charge.charged);
    // `charge` drops here, freeing the `Vec` the store presented.
}

/// A backing store for bytes the host already owns, charged to `budget`.
///
/// Answers [`HostStoreRefusal`] when the bytes would take the isolate past
/// its cap; the caller throws or rejects with the `RangeError` the guest
/// would have seen from V8. The refusal is latched, because the guest does
/// see it: unlike a null from the allocator, there is no V8 retry that could
/// absorb it.
///
/// What is charged is the `Vec`'s **allocation** (`capacity()`), not its
/// length: a `Vec` of 32 MiB length and 96 MiB capacity retains 96 MiB until
/// V8 frees the store, and a cap that charged 32 would undercount by 64. The
/// no-copy property is unchanged — V8 is handed the same buffer the `Vec`
/// owns, and the `Box<HostCharge>` keeps the `Vec` alive so its destructor
/// frees it.
///
/// An empty `Vec` retains nothing worth accounting: any spare capacity is
/// released here rather than handed to V8's zero-length store, so nothing is
/// charged and no deleter bookkeeping is needed.
pub(super) fn host_store(
    budget: &Arc<BackingStoreBudget>,
    mut bytes: Vec<u8>,
) -> Result<v8::UniqueRef<v8::BackingStore>, HostStoreRefusal> {
    if bytes.is_empty() {
        return Ok(v8::ArrayBuffer::new_backing_store_from_vec(Vec::new()));
    }
    let len = bytes.len();
    let retained = bytes.capacity();
    if !budget.reserve_latching(retained) {
        return Err(HostStoreRefusal {
            cap: budget.cap(),
            requested: len,
        });
    }
    let pointer = bytes.as_mut_ptr();
    let charge = Box::new(HostCharge {
        budget: budget.clone(),
        charged: retained,
        _bytes: bytes,
    });
    // SAFETY: the pointer is `charge.bytes`'s buffer, which the `Box` keeps
    // alive until V8 calls the deleter, and `release_host_store` takes back
    // exactly the `HostCharge` pointer handed over here.
    Ok(unsafe {
        v8::ArrayBuffer::new_backing_store_from_ptr(
            pointer.cast(),
            len,
            release_host_store,
            Box::into_raw(charge).cast(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cap is the hard line: a reservation that would cross it is
    /// refused before any memory is taken, and `used` does not move.
    #[test]
    fn reservation_past_the_cap_is_refused_without_charging() {
        let budget = BackingStoreBudget::new(1024);
        assert!(budget.try_reserve(768));
        assert_eq!(budget.used(), 768);
        assert!(!budget.exceeded());
        // The allocator path refuses without latching: V8 may retry it.
        assert!(!budget.try_reserve(512));
        assert_eq!(budget.used(), 768, "a refused reservation charged nothing");
        assert!(
            !budget.exceeded(),
            "an allocator refusal is not evidence until V8's retry fails"
        );
        // A wrapper refusal does reach the guest, so it latches.
        assert!(!budget.reserve_latching(512));
        assert!(budget.exceeded(), "the refusal condemns the isolate");
        budget.clear_exceeded();
        budget.latch_exceeded();
        assert!(budget.exceeded(), "retained use latches the same flag");
        // The room that was there is still there.
        assert!(budget.try_reserve(256));
        assert_eq!(budget.used(), 1024);
        budget.clear_exceeded();
        assert!(!budget.exceeded());
    }

    /// Releasing hands the bytes back, so churn never walks into the cap.
    #[test]
    fn release_returns_bytes_to_the_budget() {
        let budget = BackingStoreBudget::new(4096);
        for _ in 0..100 {
            assert!(budget.try_reserve(4096));
            budget.release(4096);
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.exceeded());
        // A release of bytes never reserved cannot underflow the counter.
        budget.release(1 << 20);
        assert_eq!(budget.used(), 0);
    }

    /// Two budgets are two ceilings: filling one leaves the other untouched.
    #[test]
    fn budgets_do_not_share_accounting() {
        let first = BackingStoreBudget::new(1024);
        let second = BackingStoreBudget::new(1024);
        assert!(first.try_reserve(1024));
        assert!(!first.try_reserve(1));
        assert!(
            second.try_reserve(1024),
            "the second isolate is not charged"
        );
        assert!(!second.exceeded());
    }

    /// A real allocation through the vtable is zeroed, charged, and freed
    /// back to the budget.
    #[test]
    fn allocator_round_trip_charges_and_releases() {
        let budget = BackingStoreBudget::new(1 << 20);
        let pointer = allocate(&budget, 4096, true);
        assert!(!pointer.is_null());
        assert_eq!(budget.used(), 4096);
        // SAFETY: `allocate` returned 4096 initialized bytes.
        let bytes = unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), 4096) };
        assert!(bytes.iter().all(|byte| *byte == 0), "the store is zeroed");
        // SAFETY: the pointer and length are the ones just allocated.
        unsafe { free(&budget, pointer, 4096) };
        assert_eq!(budget.used(), 0);

        let empty = allocate(&budget, 0, false);
        assert!(
            !empty.is_null(),
            "a zero-length store still answers a pointer"
        );
        // SAFETY: a zero-length allocation is not deallocated.
        unsafe { free(&budget, empty, 0) };
        assert_eq!(budget.used(), 0);
    }

    /// A store past the cap answers null, which is the only failure V8 can
    /// handle by throwing.
    #[test]
    fn allocator_refuses_past_the_cap() {
        let budget = BackingStoreBudget::new(4096);
        assert!(!allocate(&budget, 4096, false).is_null());
        assert!(allocate(&budget, 1, false).is_null(), "the cap is exact");
        assert!(
            !budget.exceeded(),
            "the allocator does not latch a refusal V8 can retry"
        );
        assert_eq!(budget.used(), 4096);

        // More than the whole cap can never be satisfied: V8's retry after a
        // collection cannot find room the cap never had, so the refusal is
        // final and latches.
        assert!(allocate(&budget, 8192, false).is_null());
        assert!(
            budget.exceeded(),
            "a request larger than the cap records its final refusal"
        );
    }

    /// A host store is charged for what it retains — the `Vec`'s allocation —
    /// not for how many of those bytes the guest can see.
    ///
    /// A `Vec` built with more capacity than length hands V8 a short view over
    /// a large allocation, and the allocation is what the isolate holds until
    /// the store is freed. Charging the length would let a response body of
    /// 32 MiB with 96 MiB of capacity sit under a 64 MiB cap while holding
    /// 96 MiB of the process.
    #[test]
    fn host_store_charges_the_retained_capacity() {
        let budget = BackingStoreBudget::new(4096);

        let mut bytes = Vec::with_capacity(4096);
        bytes.extend_from_slice(&[7_u8; 16]);
        assert_eq!((bytes.len(), bytes.capacity()), (16, 4096));
        let store = host_store(&budget, bytes).expect("4096 bytes fit the cap");
        assert_eq!(
            budget.used(),
            4096,
            "the retained allocation is charged, not the 16 visible bytes"
        );
        assert_eq!(store.byte_length(), 16, "the guest still sees 16 bytes");
        drop(store);
        assert_eq!(budget.used(), 0, "freeing the store returns the charge");

        // The same bytes with the same length are refused when the
        // allocation, not the length, does not fit.
        let mut bytes = Vec::with_capacity(8192);
        bytes.push(0);
        assert!(
            host_store(&budget, bytes).is_err(),
            "8192 retained bytes do not fit a 4096-byte cap"
        );
        assert_eq!(budget.used(), 0, "a refused store charges nothing");

        // An empty `Vec` retains nothing: its spare capacity is released
        // rather than charged, and the store carries no bytes.
        let bytes = Vec::<u8>::with_capacity(4096);
        let store = host_store(&budget, bytes).expect("an empty store is free");
        assert_eq!(store.byte_length(), 0);
        assert_eq!(budget.used(), 0);
    }
}
/// The budget as a guest sees it: buffers past the ceiling are refused with a
/// `RangeError` at the call site, the isolate is condemned the way the heap
/// guard condemns it, freeing buffers lifts the condemnation, and the process
/// stops growing. Resizable `ArrayBuffer`s, growable `SharedArrayBuffer`s and
/// `WebAssembly.Memory` are outside the budget (V8 allocates them from
/// platform pages), and one test records that behaviour.
///
/// Each test that fills the budget lowers it to 64 MiB through
/// `set_external_limit_for_test` after loading the worker: `cargo test` runs
/// these in parallel, and four heap limits per test would hold half a gigabyte
/// each. The default ceiling is pinned once, by
/// `growth_past_the_budget_is_refused_and_bounded`.
#[cfg(test)]
mod guest_tests {
    use crate::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
    use std::sync::Arc;
    use std::time::Instant;

    const MIB: usize = 1024 * 1024;
    /// What these tests run with, except where they pin the default.
    const TEST_CAP: usize = 64 * MIB;

    const SCRIPT: &str = r#"
const held = [];
globalThis.__held = held;
function n(url, name, fallback) {
  return Number(url.searchParams.get(name) ?? fallback);
}
function report(error) {
  return { ok: false, name: error.name, message: error.message };
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      // Allocate `step` bytes at a time, filling each view so the pages are
      // really committed, and retain them. The caller keeps `target` below
      // the ceiling: an isolate at its ceiling cannot encode a response body
      // either, which is what `/alloc` proves on its own.
      case "/grow": {
        const step = n(url, "step", 0);
        const target = n(url, "target", 0);
        let grown = 0;
        while (grown + step <= target) {
          const view = new Uint8Array(step);
          view.fill(1);
          held.push(view);
          grown += step;
        }
        return Response.json({ grown });
      }
      // One allocation of `mib` MiB, retained when it succeeds.
      case "/alloc": {
        const bytes = n(url, "mib", 0) * 1024 * 1024;
        try {
          const view = new Uint8Array(bytes);
          view.fill(1);
          held.push(view);
          return Response.json({ ok: true, bytes: view.byteLength });
        } catch (error) {
          return Response.json(report(error));
        }
      }
      // Allocate and drop in a loop: nothing is retained, so the budget sees
      // churn, not growth.
      case "/churn": {
        const step = n(url, "step", 0);
        const count = n(url, "count", 0);
        let allocated = 0;
        let refused = null;
        for (let index = 0; index < count; index++) {
          try {
            const view = new Uint8Array(step);
            view.fill(1);
            allocated += step;
          } catch (error) {
            refused = `${error.name}: ${error.message}`;
            break;
          }
        }
        return Response.json({ allocated, refused });
      }
      // Every legitimate per-request payload at once, which the ceiling must
      // leave room for.
      case "/legit": {
        const body = new Uint8Array(64 * 1024 * 1024).fill(1);
        const message = new Uint8Array(32 * 1024 * 1024).fill(2);
        const tee = new Uint8Array(32 * 1024 * 1024).fill(3);
        const cloned = structuredClone(tee);
        held.push(body, message, tee, cloned);
        return Response.json({
          bytes: body.byteLength + message.byteLength + tee.byteLength +
            cloned.byteLength,
        });
      }
      // `structuredClone` copies the buffer through V8's deserializer, which
      // must draw on the same budget.
      case "/clone": {
        const size = n(url, "mib", 0) * 1024 * 1024;
        try {
          const source = new Uint8Array(size);
          const copy = structuredClone(source);
          held.push(source, copy);
          return Response.json({ ok: true, bytes: copy.byteLength });
        } catch (error) {
          return Response.json(report(error));
        }
      }
      // `transfer` allocates the target buffer.
      case "/transfer": {
        const from = n(url, "from", 0) * 1024 * 1024;
        const to = n(url, "to", 0) * 1024 * 1024;
        try {
          const moved = new ArrayBuffer(from).transfer(to);
          held.push(moved);
          return Response.json({ ok: true, bytes: moved.byteLength });
        } catch (error) {
          return Response.json(report(error));
        }
      }
      // Construct a resizable `ArrayBuffer` and resize it in the same turn.
      // V8 backs it with the platform page allocator, so the isolate budget
      // neither charges nor refuses it: this route proves the documented gap.
      case "/rab": {
        const bytes = n(url, "bytes", 8);
        const max = n(url, "max", 0) * 1024 * 1024;
        const resize = n(url, "resize", 0) * 1024 * 1024;
        try {
          const buffer = new ArrayBuffer(bytes, { maxByteLength: max });
          buffer.resize(resize);
          held.push(buffer);
          return Response.json({ ok: true, bytes: buffer.byteLength });
        } catch (error) {
          return Response.json(report(error));
        }
      }
      // A growable `SharedArrayBuffer`, grown in the same turn.
      case "/gsab": {
        const max = n(url, "max", 0) * 1024 * 1024;
        const grow = n(url, "grow", 0) * 1024 * 1024;
        try {
          const shared = new SharedArrayBuffer(8, { maxByteLength: max });
          shared.grow(grow);
          held.push(shared);
          const words = new Int32Array(shared);
          Atomics.store(words, 0, 7);
          Atomics.add(words, 0, 1);
          return Response.json({
            ok: true,
            bytes: shared.byteLength,
            growable: shared.growable === true,
            atomics: Atomics.load(words, 0),
          });
        } catch (error) {
          return Response.json(report(error));
        }
      }
      // `WebAssembly.Memory`: `initial` and `grow` are both in 64 KiB pages.
      // Like the resizable paths it never reaches the isolate allocator, so
      // `--wasm-max-mem-pages` is its only bound, and that bound is per
      // memory.
      case "/wasm": {
        const initial = n(url, "initial", 0);
        const maximum = n(url, "maximum", -1);
        const grow = n(url, "grow", 0);
        try {
          const descriptor = { initial };
          if (maximum >= 0) descriptor.maximum = maximum;
          const memory = new WebAssembly.Memory(descriptor);
          if (grow > 0) memory.grow(grow);
          held.push(memory);
          return Response.json({
            ok: true,
            bytes: memory.buffer.byteLength,
          });
        } catch (error) {
          return Response.json(report(error));
        }
      }
      case "/free": {
        const freed = held.length;
        held.length = 0;
        return Response.json({ freed });
      }
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

    /// Runs `test` on a thread created after V8 initialised; see
    /// `runtime::init_v8`.
    fn on_engine_thread(test: impl FnOnce() + Send + 'static) {
        crate::runtime::init_v8();
        crate::asyncrt::shared_test_runtime();
        std::thread::spawn(test).join().expect("engine thread");
    }

    fn load() -> Worker {
        load_script(SCRIPT)
    }

    fn load_script(src: &str) -> Worker {
        let config = WorkerConfig::new(WorkerConfigOptions {
            src: src.to_string(),
            script_name: "array-buffer-budget".to_string(),
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

    /// Load a worker whose backing-store ceiling is [`TEST_CAP`]. Answers the
    /// worker and the bytes it already charges (the script itself and the
    /// bootstrap's stores), which every expectation is relative to.
    fn load_with_test_cap() -> (Worker, usize) {
        let worker = load();
        worker.set_external_limit_for_test(TEST_CAP);
        let (used, cap) = budget(&worker);
        assert_eq!(cap, TEST_CAP);
        assert!(
            used < MIB,
            "loading the worker charged {used} bytes of buffers"
        );
        (worker, used)
    }

    /// One request whose handler settles on promises alone, so its first turn
    /// is the whole request. Answers the JSON body.
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
            .unwrap_or_else(|error| panic!("{path} did not answer in one turn: {error:?}"))
            .unwrap_or_else(|error| panic!("{path} was rejected: {error}"));
        assert_eq!(response.status, 200, "{path} answered {}", response.status);
        serde_json::from_slice(&response.body).expect("json body")
    }

    /// Bytes the budget currently charges, and the ceiling it enforces.
    fn budget(worker: &Worker) -> (usize, usize) {
        worker
            .backing_store_budget_for_test()
            .expect("every loaded isolate has a budget")
    }

    /// A full collection and the foreground tasks it posted. This is where a
    /// dropped backing store gives its bytes back to the budget: V8 frees the
    /// store through the allocator's `free` (or a host store's deleter), and
    /// the collection must finish before the accounting drops. Runs until no
    /// task is left runnable.
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

    /// The step-1 failure, now bounded: after most of the budget is retained,
    /// a further allocation is refused with a `RangeError`, the allocator
    /// stops charging at the ceiling, the isolate is condemned, and a second
    /// isolate pays nothing for the first one's fill.
    ///
    /// The fixture stops a step under the ceiling on purpose: a response body
    /// is itself a charged host store, and an isolate at the exact ceiling
    /// cannot encode its own answer. That is the guest-visible contract (free
    /// buffers before allocating more).
    #[test]
    fn growth_past_the_budget_is_refused_and_bounded() {
        on_engine_thread(|| {
            let mut worker = load();
            let (_, cap) = budget(&worker);
            assert_eq!(cap, 4 * 128 * MIB, "the default ceiling is four heaps");

            let report = fetch(&mut worker, "/grow?step=8388608&target=520093696");
            assert_eq!(report["grown"], 496 * MIB, "{report}");
            let (used_after_grow, _) = budget(&worker);
            assert!(used_after_grow <= cap, "{used_after_grow} of {cap}");

            let report = fetch(&mut worker, "/alloc?mib=32");
            assert_eq!(report["ok"], false, "the growth was not refused: {report}");
            assert_eq!(
                report["name"], "RangeError",
                "the refusal is the RangeError V8 raises: {report}"
            );
            assert!(
                report["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("Array buffer allocation failed")),
                "the message names the failed allocation: {report}"
            );

            let (used, cap_after) = budget(&worker);
            assert_eq!(cap_after, cap);
            assert!(
                used <= cap,
                "the allocator never charges past the ceiling: {used} of {cap}"
            );
            let heap = worker.heap_bytes().expect("live isolate");
            assert!(
                heap.external as usize <= cap + 64 * MIB,
                "V8 external memory stays near the ceiling: {} bytes",
                heap.external
            );
            // The retained bytes condemned the isolate at the turn boundary.
            // This refusal itself did not latch: 32 MiB fits under the whole
            // cap, so V8's collect-and-retry could have succeeded. Both guards
            // answer through the same accessor.
            assert_eq!(
                worker.condemned_for_test(),
                Some(true),
                "retained use condemns the isolate, as the heap guard does"
            );

            // The condemnation is the refused isolate's. A second isolate
            // starts with an empty budget and serves the same allocation.
            let mut other = load();
            let report = fetch(&mut other, "/alloc?mib=64");
            assert_eq!(
                report["ok"], true,
                "another isolate was charged for the first one's fill: {report}"
            );
            assert_eq!(other.condemned_for_test(), Some(false));
            let (used_other, _) = budget(&other);
            assert!(used_other >= 64 * MIB);
        });
    }

    /// Allocate and free well past the ceiling in total: accounting falls when
    /// the collector frees a store, so churn is never refused and never
    /// condemns the isolate.
    #[test]
    fn churn_past_the_budget_is_never_refused() {
        on_engine_thread(|| {
            let (mut worker, base) = load_with_test_cap();
            // 24 requests x 4 chunks x 4 MiB = 384 MiB, six ceilings over.
            for round in 0..24 {
                let report = fetch(&mut worker, "/churn?step=4194304&count=4");
                assert!(
                    report["refused"].is_null(),
                    "churn round {round} was refused: {report}"
                );
                assert_eq!(report["allocated"], 16 * MIB);
                collect(&mut worker);
                let (used, _) = budget(&worker);
                assert!(
                    used < base + TEST_CAP / 2,
                    "collected churn left {used} charged after round {round}"
                );
                assert_eq!(
                    worker.condemned_for_test(),
                    Some(false),
                    "churn condemned the isolate after round {round}"
                );
            }
        });
    }

    /// Every legitimate payload at once fits: a 64 MiB body or `/tmp` read, a
    /// 32 MiB WebSocket message, a 32 MiB tee budget and a clone of it.
    #[test]
    fn legitimate_payloads_fit_under_the_budget() {
        on_engine_thread(|| {
            // The default ceiling: the payload set is what the four-heap
            // multiple is sized for.
            let mut worker = load();
            let (base, _) = budget(&worker);
            let report = fetch(&mut worker, "/legit");
            assert_eq!(report["bytes"], 160 * MIB, "{report}");
            assert_eq!(worker.condemned_for_test(), Some(false));
            let (used, cap) = budget(&worker);
            // The payloads plus the response body's own store, which is
            // charged like any other. 160 MiB of 512 is well under the share
            // that would condemn the isolate.
            assert!(
                used >= base + 160 * MIB && used <= base + 160 * MIB + MIB,
                "charged {used} bytes for 160 MiB of payloads"
            );
            assert!(used < cap);

            // And the isolate still takes more afterwards: the ceiling is not
            // a per-request budget.
            let report = fetch(&mut worker, "/churn?step=4194304&count=2");
            assert!(report["refused"].is_null(), "{report}");
            assert_eq!(worker.condemned_for_test(), Some(false));
        });
    }

    /// `structuredClone` and `transfer` allocate through the same budget as
    /// `new ArrayBuffer`.
    ///
    /// Each probe gets its own isolate. The clone probe depends only on the
    /// bytes the guard charged, not on V8 retaining a store past a collection:
    /// a fresh isolate grows to half the ceiling, then a clone that needs two
    /// copies of 24 MiB is refused before it can cross it.
    #[test]
    fn clone_and_transfer_use_the_same_budget() {
        on_engine_thread(|| {
            {
                let (mut worker, base) = load_with_test_cap();
                let report = fetch(&mut worker, "/grow?step=4194304&target=33554432");
                assert_eq!(report["grown"], 32 * MIB, "{report}");
                assert_eq!(worker.condemned_for_test(), Some(false));
                // 32 MiB charged, 32 MiB free: a 24 MiB clone needs 48.
                let report = fetch(&mut worker, "/clone?mib=24");
                assert_eq!(report["ok"], false, "the clone was not refused: {report}");
                // A clone of a plain buffer is allocated by V8's deserializer
                // through the isolate allocator, so the allocator's refusal
                // surfaces as V8's deserialize error, not as the guard's own
                // `RangeError`. That difference is documented.
                assert!(
                    report["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("deserialize")),
                    "the clone failed in V8's deserializer: {report}"
                );
                let (used, cap) = budget(&worker);
                assert!(
                    used <= cap,
                    "the clone charged past the ceiling: {used}/{cap}"
                );
                assert!(used >= base + 32 * MIB, "the fill is still charged: {used}");
            }

            {
                let (mut worker, _) = load_with_test_cap();
                let report = fetch(&mut worker, "/grow?step=4194304&target=33554432");
                assert_eq!(report["grown"], 32 * MIB, "{report}");
                // The transfer target is an allocation too: 8 MiB in, 64 MiB
                // out crosses the ceiling.
                let report = fetch(&mut worker, "/transfer?from=8&to=64");
                assert_eq!(
                    report["ok"], false,
                    "the transfer was not refused: {report}"
                );
                assert_eq!(report["name"], "RangeError", "{report}");
                let (used, cap) = budget(&worker);
                assert!(
                    used <= cap,
                    "the transfer charged past the ceiling: {used}/{cap}"
                );
            }
        });
    }

    /// The documented gap: V8 backs a resizable `ArrayBuffer`, a growable
    /// `SharedArrayBuffer` and `WebAssembly.Memory` with the platform page
    /// allocator, which no isolate allocator hook can see. None of the three
    /// is charged or refused, crossing the cap with them condemns nothing, and
    /// a wasm memory is bounded only per memory by `--wasm-max-mem-pages`.
    ///
    /// This test is the record of that behaviour: a page-allocator hook in
    /// rusty_v8 is the change that would let the budget cover these paths, and
    /// it must update this test and `docs/reference/limits.md` together.
    #[test]
    fn page_allocator_buffers_are_not_capped_by_the_budget() {
        on_engine_thread(|| {
            let (mut worker, base) = load_with_test_cap();
            // 40 MiB of the 64 MiB cap through the allocator: 62.5 percent,
            // under the recovery share, so the turn boundary latches nothing.
            let report = fetch(&mut worker, "/grow?step=4194304&target=41943040");
            assert_eq!(report["grown"], 40 * MIB, "{report}");
            let (used, cap) = budget(&worker);
            assert!(used >= base + 40 * MIB && used < cap, "{used} of {cap}");

            // A 32 MiB resizable buffer takes the isolate to 72 MiB of buffers
            // against a 64 MiB cap, and the budget neither sees nor refuses it.
            let report = fetch(&mut worker, "/rab?bytes=8&max=32&resize=32");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["bytes"], 32 * MIB, "{report}");

            // The same for a growable `SharedArrayBuffer` and a wasm memory.
            let report = fetch(&mut worker, "/gsab?max=24&grow=24");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["bytes"], 24 * MIB, "{report}");
            assert_eq!(report["growable"], true, "{report}");
            assert_eq!(report["atomics"], 8, "{report}");
            let report = fetch(&mut worker, "/wasm?initial=128&maximum=512&grow=256");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["bytes"], 384 * 65536, "{report}");

            let (after_page_stores, cap_after) = budget(&worker);
            assert_eq!(cap_after, cap);
            assert!(
                after_page_stores < used + 4096,
                "a page-allocator buffer was charged: {used} -> {after_page_stores}"
            );
            assert_eq!(
                worker.external_exceeded_for_test(),
                Some(false),
                "a page-allocator buffer latched the budget's condemnation"
            );

            // The allocator still enforces the cap with them present: 40 MiB
            // held plus 32 MiB asked is over 64.
            //
            // Only the budget is asserted here. V8 does count a resized
            // `ArrayBuffer` in `external_memory` (measured: +32 MiB; a
            // growable `SharedArrayBuffer` and a wasm memory are not counted
            // at all), so its own heap heuristics can react to one—including
            // the heap guard's condemnation, which is a different mechanism
            // from this budget's latch.
            let report = fetch(&mut worker, "/alloc?mib=32");
            assert_eq!(
                report["ok"], false,
                "the allocator counted page-allocator buffers: {report}"
            );
            assert_eq!(report["name"], "RangeError", "{report}");
            assert_eq!(
                worker.external_exceeded_for_test(),
                Some(false),
                "a refusal under the whole cap latched the budget"
            );

            // `--wasm-max-mem-pages` is the only bound on a wasm memory, and
            // it is per memory: the default 512 MiB budget gives 8192 pages of
            // 64 KiB. Measured on V8 152, the flag bounds *growth*: a declared
            // maximum above the page count still constructs, a grow to exactly
            // 8192 pages succeeds, and the next page fails — so wasm code
            // cannot take one memory past the budget either.
            let report = fetch(&mut worker, "/wasm?initial=1&maximum=16384&grow=8191");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["bytes"], 8192 * 65536, "{report}");
            let report = fetch(&mut worker, "/wasm?initial=1&grow=8192");
            assert_eq!(
                report["ok"], false,
                "a wasm memory grew past the per-memory bound: {report}"
            );
            assert_eq!(report["name"], "RangeError", "{report}");
            assert!(
                report["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("Unable to grow")),
                "the growth failed in V8: {report}"
            );
        });
    }

    /// Freeing what is charged lifts the condemnation: the collector returns
    /// the bytes to the budget, so an isolate that lets go serves again and is
    /// never condemned for memory it no longer holds.
    #[test]
    fn freed_buffers_lift_the_condemnation() {
        on_engine_thread(|| {
            let (mut worker, base) = load_with_test_cap();
            let report = fetch(&mut worker, "/grow?step=4194304&target=58720256");
            assert_eq!(report["grown"], 56 * MIB, "{report}");

            // One allocation past the ceiling, in a turn that begins over the
            // recovery share: the isolate is condemned.
            let report = fetch(&mut worker, "/alloc?mib=16");
            assert_eq!(report["ok"], false, "{report}");
            assert_eq!(report["name"], "RangeError", "{report}");
            assert_eq!(worker.condemned_for_test(), Some(true));

            fetch(&mut worker, "/free");
            collect(&mut worker);
            let (used, _) = budget(&worker);
            assert_eq!(used, base, "the freed stores are released");
            let report = fetch(&mut worker, "/alloc?mib=8");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(
                worker.condemned_for_test(),
                Some(false),
                "a drained isolate serves again"
            );
            assert_eq!(worker.external_exceeded_for_test(), Some(false));
            let (used, cap) = budget(&worker);
            assert!(used < cap);
        });
    }

    /// A body stream whose one chunk is bigger than the whole cap. The read
    /// promise is what a stream chunk reaches the guest through, so a refused
    /// chunk must reject it — never resolve it with a value the guest has to
    /// recognise as missing.
    const STREAM_SCRIPT: &str = r#"
export default {
  async fetch(request) {
    const reader = request.body.getReader();
    try {
      const { value } = await reader.read();
      return Response.json({ ok: true, bytes: value ? value.byteLength : -1 });
    } catch (error) {
      return Response.json({
        ok: false,
        name: error && error.name,
        message: String(error && error.message),
      });
    }
  },
};
"#;

    /// A refused stream chunk rejects the read promise with the budget's
    /// `RangeError`, and the request answers instead of dying: the host store
    /// that carries a chunk is charged like any other, and the reader is the
    /// thing it must report to.
    #[test]
    fn refused_stream_chunk_rejects_the_read_promise() {
        on_engine_thread(|| {
            // The test delivers the chunk itself, so the stream's own bytes
            // never need to exist twice.
            let stream: crate::js::HttpChunkStream =
                Box::pin(futures_util::stream::iter(vec![Ok::<Vec<u8>, String>(
                    vec![7; 8 << 20],
                )]));
            let stream_id = crate::js::register_body_stream(stream).expect("register body stream");
            let mut worker = load_script(STREAM_SCRIPT);
            worker.set_external_limit_for_test(TEST_CAP);
            let (base, cap) = budget(&worker);
            assert_eq!(cap, TEST_CAP);

            let (reply, mut receive) = tokio::sync::oneshot::channel();
            let job = crate::WorkerJob::Fetch {
                queued_at: Instant::now(),
                entrypoint: None,
                invocation_limits: None,
                url: "http://test/read".to_string(),
                method: "GET".to_string(),
                body: RequestBody::Stream(stream_id),
                headers: Vec::new(),
                // A held body crosses as its bytes; a streamed body crosses
                // as its id, and only an incoming request (one with an id)
                // carries a stream at all.
                request_id: Some(crate::js::next_request_id()),
                tail_report: None,
                reply,
            };
            let (entry, started) = worker.turn_begin(job, None);
            let mut entry = entry.expect("request starts");
            assert_eq!(started.len(), 1, "the handler awaits its first chunk");
            let op = started[0].0;
            drop(started);

            // The chunk body is larger than the whole ceiling, so no
            // collection can make room for it: this is the final refusal.
            let started = worker.turn_deliver(
                &mut entry,
                op,
                Ok(crate::asyncrt::OpOut::Bytes(vec![7; 3 * TEST_CAP])),
            );
            drop(started);
            if !entry.finished() {
                drop(worker.turn_poll(&mut entry));
            }
            let response = receive
                .try_recv()
                .expect("the handler answered the refused read")
                .unwrap_or_else(|error| panic!("the request failed instead: {error:#}"));
            let report: serde_json::Value =
                serde_json::from_slice(&response.body).expect("json body");
            assert_eq!(
                report["ok"], false,
                "the refused chunk resolved the read promise: {report}"
            );
            assert_eq!(
                report["name"], "RangeError",
                "the reader sees the budget's RangeError: {report}"
            );
            assert!(
                report["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("budget")),
                "the rejection names the budget: {report}"
            );
            let (used, cap) = budget(&worker);
            assert!(
                used < base + MIB,
                "the refused chunk left {used} bytes charged, base {base}"
            );
            assert!(used < cap);
            entry.abandon();
        });
    }

    /// A scratch cell database in the worker's own storage, as the D1 tests
    /// build one: `open_with_compat` needs the isolate's cells installed, and
    /// holding the worker's lock is what installs them.
    fn open_cell(worker: &mut Worker, scope: &str, dir: &tempfile::TempDir) {
        let path = dir.path().join(format!("{scope}.sqlite3"));
        let inner = worker.inner.as_mut().expect("live worker");
        let (_locker, _cells) = inner.lock();
        crate::storage::open_with_compat(scope, path.to_str().expect("utf-8 path"), false)
            .expect("open cell");
    }

    /// One statement on the cell, with the cursor it starts closed again.
    fn run_sql(worker: &mut Worker, scope: &str, sql: &str) {
        let inner = worker.inner.as_mut().expect("live worker");
        let (_locker, _cells) = inner.lock();
        let (cursor, _columns, _row, _written, _reused) =
            crate::storage::sql_cursor_start_values(scope, sql, &[])
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        crate::storage::sql_cursor_close(cursor);
    }

    /// A cell scope whose cap is [`SMALL_CAP`] and one table with BLOBs big
    /// enough to be unpayable. A cell connection clamps
    /// `SQLITE_LIMIT_LENGTH` to 2.2 MB, so one blob is a MiB and a two-blob
    /// row stays inside a legal row: one fits beside nothing at
    /// [`SMALL_CAP`], two cannot both fit.
    const SMALL_CAP: usize = MIB + MIB / 2;
    const BLOB: usize = MIB;

    fn cell_with_blobs(name: &str) -> (Worker, tempfile::TempDir, String) {
        let mut worker = load();
        worker.set_external_limit_for_test(SMALL_CAP);
        let dir = tempfile::tempdir().expect("temp dir");
        let scope = format!("array-buffer-blob-{name}");
        open_cell(&mut worker, &scope, &dir);
        run_sql(
            &mut worker,
            &scope,
            "CREATE TABLE b(id INTEGER PRIMARY KEY, one BLOB, two BLOB)",
        );
        run_sql(
            &mut worker,
            &scope,
            &format!("INSERT INTO b VALUES (1, zeroblob({BLOB}), zeroblob({BLOB}))"),
        );
        (worker, dir, scope)
    }

    /// A row carrying two BLOBs that cannot both be presented throws one
    /// `RangeError`, presents nothing, and closes the native cursor. Without
    /// the close every refusal would retain a SQLite statement until the cell
    /// closes — row 1 because the exception prevented the `SqlCursor` (and its
    /// finalizer) from existing, a later row because `_advanceNative` →
    /// `_finishNative` only unregisters the finalizer.
    ///
    /// The converter is driven through [`cursor_row`], which is exactly what
    /// both cursor ops call; it is the function that closes and throws.
    #[test]
    fn refused_blob_row_closes_the_cursor_and_throws_once() {
        on_engine_thread(|| {
            let (mut worker, _dir, scope) = cell_with_blobs("close");
            let inner = worker.inner.as_mut().expect("live worker");
            let (mut locker, _cells) = inner.lock();
            v8::scope!(let hs, &mut *locker);
            let realm = inner.realm(hs);
            let context = realm.context;
            let cs = &mut v8::ContextScope::new(hs, context);
            let tc = std::pin::pin!(v8::TryCatch::new(cs));
            let tc = &mut tc.init();

            // Row 1 is the two-BLOB row: the first column fits, the second
            // cannot, which is the D1 shape the finding describes.
            let (cursor, _columns, row, _written, _reused) =
                crate::storage::sql_cursor_start_values(&scope, "SELECT one, two FROM b", &[])
                    .expect("cursor starts");
            assert!(
                crate::storage::sql_cursor_scope(cursor).is_some(),
                "the native cursor exists before the conversion"
            );
            let row = row.expect("the first row is stepped");
            let refused = super::super::storage_ops::cursor_row(tc, cursor, row);
            assert!(
                refused.is_none(),
                "a row with an unpayable BLOB column is not presented"
            );
            let exception = tc.exception().expect("the refusal was thrown");
            let text = exception
                .to_string(tc)
                .expect("exception text")
                .to_rust_string_lossy(tc);
            assert!(
                text.starts_with("RangeError:") && text.contains("budget"),
                "the guest sees the budget's RangeError, got {text:?}"
            );
            assert!(
                crate::storage::sql_cursor_scope(cursor).is_none(),
                "the native cursor is closed, so its statement is not retained"
            );

            // The connection is not wedged: a later query starts and finishes
            // on the same cell.
            let (after, _columns, _row, _written, _reused) =
                crate::storage::sql_cursor_start_values(&scope, "SELECT count(*) FROM b", &[])
                    .expect("a later query still runs");
            crate::storage::sql_cursor_close(after);
        });
    }

    /// A refused `RETURNING` row reports that its write already happened, and
    /// the write is really there.
    ///
    /// SQLite applies an autocommit `INSERT ... RETURNING`'s write before the
    /// first row exists, and closing the cursor finalizes the statement, which
    /// keeps the write. A guest that saw only the plain budget message would
    /// free buffers and retry, applying the insert twice; the message therefore
    /// says the effects stand.
    #[test]
    fn refused_returning_blob_reports_the_write_was_applied() {
        on_engine_thread(|| {
            let (mut worker, _dir, scope) = cell_with_blobs("applied");
            worker.set_external_limit_for_test(BLOB / 2);
            run_sql(
                &mut worker,
                &scope,
                "CREATE TABLE w(id INTEGER PRIMARY KEY, blob BLOB)",
            );
            {
                let inner = worker.inner.as_mut().expect("live worker");
                let (mut locker, _cells) = inner.lock();
                v8::scope!(let hs, &mut *locker);
                let realm = inner.realm(hs);
                let context = realm.context;
                let cs = &mut v8::ContextScope::new(hs, context);
                let tc = std::pin::pin!(v8::TryCatch::new(cs));
                let tc = &mut tc.init();
                let (cursor, _columns, row, _written, _reused) =
                    crate::storage::sql_cursor_start_values(
                        &scope,
                        &format!("INSERT INTO w(blob) VALUES (zeroblob({BLOB})) RETURNING blob"),
                        &[],
                    )
                    .expect("cursor starts");
                assert!(
                    row.is_some(),
                    "the write ran and produced its RETURNING row"
                );
                assert!(
                    crate::storage::sql_cursor_is_write(cursor),
                    "the cursor knows its statement writes"
                );
                let refused = super::super::storage_ops::cursor_row(tc, cursor, row.expect("row"));
                assert!(refused.is_none(), "the 1 MiB BLOB does not fit");
                let text = tc
                    .exception()
                    .expect("the refusal was thrown")
                    .to_string(tc)
                    .expect("exception text")
                    .to_rust_string_lossy(tc);
                assert!(
                    text.starts_with("RangeError:") && text.contains("budget"),
                    "the guest sees the budget's RangeError, got {text:?}"
                );
                assert!(
                    text.contains("had already executed") && text.contains("not rolled back"),
                    "the refusal says the write stands, got {text:?}"
                );
                // The insert really was applied, so a retry would duplicate it.
                let (after, _columns, row, _written, _reused) =
                    crate::storage::sql_cursor_start_values(&scope, "SELECT count(*) FROM w", &[])
                        .expect("count query runs");
                crate::storage::sql_cursor_close(after);
                assert!(
                    matches!(row.as_deref(), Some([crate::storage::SqlValue::Integer(1)])),
                    "the refused RETURNING row's write stands: {row:?}"
                );
            }
        });
    }

    /// A read-only statement has no write to report, and its refusal keeps the
    /// plain budget message: telling the guest "do not retry" there would be
    /// wrong, because nothing was applied.
    #[test]
    fn refused_read_only_row_reports_no_write() {
        on_engine_thread(|| {
            let (mut worker, _dir, scope) = cell_with_blobs("readonly");
            worker.set_external_limit_for_test(BLOB / 2);
            let inner = worker.inner.as_mut().expect("live worker");
            let (mut locker, _cells) = inner.lock();
            v8::scope!(let hs, &mut *locker);
            let realm = inner.realm(hs);
            let context = realm.context;
            let cs = &mut v8::ContextScope::new(hs, context);
            let tc = std::pin::pin!(v8::TryCatch::new(cs));
            let tc = &mut tc.init();
            let (cursor, _columns, row, _written, _reused) =
                crate::storage::sql_cursor_start_values(&scope, "SELECT one FROM b", &[])
                    .expect("cursor starts");
            assert!(
                !crate::storage::sql_cursor_is_write(cursor),
                "a SELECT is not a write"
            );
            let refused = super::super::storage_ops::cursor_row(tc, cursor, row.expect("row"));
            assert!(refused.is_none(), "the 1 MiB BLOB does not fit");
            let text = tc
                .exception()
                .expect("the refusal was thrown")
                .to_string(tc)
                .expect("exception text")
                .to_rust_string_lossy(tc);
            assert!(
                text.starts_with("RangeError:") && text.contains("budget"),
                "the guest sees the budget's RangeError, got {text:?}"
            );
            assert!(
                !text.contains("already executed"),
                "a read-only refusal must not tell the guest a write stands: {text:?}"
            );
        });
    }

    /// A refused host store latches the isolate's condemnation even when the
    /// retained use is nowhere near the recovery share: the guest sees the
    /// `RangeError`, so the isolate failed to allocate and must be replaced
    /// like one that hit the heap limit.
    #[test]
    fn refused_host_store_latches_condemnation() {
        on_engine_thread(|| {
            // A single BLOB larger than the whole cap, on an isolate holding
            // nothing else: only the host store's own refusal can latch.
            let (mut worker, _dir, scope) = cell_with_blobs("latch");
            worker.set_external_limit_for_test(BLOB / 2);
            let (before, cap) = budget(&worker);
            assert_eq!(cap, BLOB / 2);
            assert_eq!(
                worker.external_exceeded_for_test(),
                Some(false),
                "an idle isolate is not condemned"
            );
            {
                let inner = worker.inner.as_mut().expect("live worker");
                let (mut locker, _cells) = inner.lock();
                v8::scope!(let hs, &mut *locker);
                let realm = inner.realm(hs);
                let context = realm.context;
                let cs = &mut v8::ContextScope::new(hs, context);
                let tc = std::pin::pin!(v8::TryCatch::new(cs));
                let tc = &mut tc.init();
                let (cursor, _columns, row, _written, _reused) =
                    crate::storage::sql_cursor_start_values(&scope, "SELECT one FROM b", &[])
                        .expect("cursor starts");
                let row = row.expect("the first row is stepped");
                let refused = super::super::storage_ops::cursor_row(tc, cursor, row);
                assert!(refused.is_none(), "the BLOB does not fit in the cap");
                assert!(tc.has_caught(), "the refusal was thrown");
            }
            assert_eq!(
                worker.external_exceeded_for_test(),
                Some(true),
                "the host store's refusal condemns the isolate"
            );
            assert_eq!(worker.condemned_for_test(), Some(true));
            let (used, _) = budget(&worker);
            assert_eq!(used, before, "a refused store charges nothing");
        });
    }

    /// The one allocator refusal V8 cannot retry — a fixed buffer larger than
    /// the whole cap — condemns the isolate even though it holds nothing: no
    /// collection can free room the cap never had.
    #[test]
    fn oversize_fixed_buffer_condemns_the_isolate() {
        on_engine_thread(|| {
            let (mut worker, base) = load_with_test_cap();
            assert_eq!(
                worker.external_exceeded_for_test(),
                Some(false),
                "an empty isolate is not condemned"
            );
            // 96 MiB against a 64 MiB ceiling: V8 collects and retries, and
            // both attempts must fail.
            let report = fetch(&mut worker, "/alloc?mib=96");
            assert_eq!(
                report["ok"], false,
                "the allocation was not refused: {report}"
            );
            assert_eq!(
                report["name"], "RangeError",
                "the refusal is V8's RangeError: {report}"
            );
            assert_eq!(
                worker.external_exceeded_for_test(),
                Some(true),
                "an unpayable allocation records its final refusal"
            );
            assert_eq!(worker.condemned_for_test(), Some(true));
            let (used, cap) = budget(&worker);
            assert!(
                used < base + MIB,
                "the refusal charged {used} bytes (base {base}): only the \
                 answer's own store should be charged"
            );
            assert!(cap > base);
            // The latch is this isolate's: a second one starts clean.
            let (other, _) = load_with_test_cap();
            assert_eq!(other.external_exceeded_for_test(), Some(false));
        });
    }
}
