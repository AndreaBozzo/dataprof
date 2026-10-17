//! Count the allocations the per-cell optimizations are meant to remove.
// A global allocator is the only way to observe them, and implementing one is
// unsafe; it only forwards to `System`.
#![allow(unsafe_code)]
use dataprof_runtime::{StreamReservoirSampler, StreamingColumnCollection};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
thread_local! {
    static SIZE: Cell<usize> = const { Cell::new(0) };
    static COUNT: Cell<usize> = const { Cell::new(0) };
}
fn record(size: usize) {
    let _ = SIZE.try_with(|wanted| {
        if wanted.get() != 0 && wanted.get() == size {
            let _ = COUNT.try_with(|count| count.set(count.get() + 1));
        }
    });
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
fn count_size(size: usize, run: impl FnOnce()) -> usize {
    COUNT.with(|count| count.set(0));
    SIZE.with(|wanted| wanted.set(size));
    run();
    SIZE.with(|wanted| wanted.set(0));
    COUNT.with(Cell::get)
}

#[test]
fn existing_column_headers_do_not_allocate_per_cell() {
    let headers = vec!["h".repeat(8191)];
    let mut columns = StreamingColumnCollection::new();
    columns.process_record(&headers, vec!["value".to_owned()]);
    let allocations = count_size(8191, || {
        for _ in 0..100 {
            columns.process_record(&headers, vec!["value".to_owned()]);
        }
    });
    assert_eq!(columns.get_column_stats(&headers[0]).unwrap().count, 101);
    assert_eq!(allocations, 0, "existing header copies still allocate");
}

#[test]
fn rejected_reservoir_offers_do_not_allocate_strings() {
    let mut sampler = StreamReservoirSampler::new(8);
    let value = "x".repeat(4093);
    let allocations = count_size(value.len(), || {
        for _ in 0..10_000 {
            sampler.offer(&value);
        }
    });
    assert_eq!(sampler.samples().len(), 8);
    assert!(
        allocations < 200,
        "{allocations} string allocations for 10,000 offers"
    );
}

#[test]
fn borrowed_reservoir_keeps_the_original_seeded_membership() {
    // Recorded from the pre-change executable on the same 10,000 offers.
    let mut sampler = StreamReservoirSampler::new(3);
    for index in 0..10_000 {
        sampler.offer(&format!("{index:08}{}", "x".repeat(101)));
    }
    let retained: Vec<_> = sampler.samples().iter().map(|v| &v[..8]).collect();
    assert_eq!(retained, ["00006502", "00008040", "00000707"]);
}
