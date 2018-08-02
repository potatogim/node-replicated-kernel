#![feature(
    intrinsics,
    asm,
    lang_items,
    const_fn,
    core,
    raw,
    box_syntax,
    start,
    panic_implementation,
    panic_info_message,
    alloc,
    allocator_api,
    heap_api,
    global_asm,
    linkage
)]
#![no_std]

extern crate spin;

extern crate rlibc;

#[macro_use]
pub mod mutex;

extern crate alloc;

#[cfg(target_arch = "x86_64")]
#[macro_use]
extern crate x86;

#[cfg(target_arch = "x86_64")]
extern crate slabmalloc;

#[cfg(target_arch = "x86_64")]
#[macro_use]
extern crate klogger;

#[cfg(target_arch = "x86_64")]
extern crate elfloader;

#[cfg(target_arch = "x86_64")]
extern crate multiboot;

extern crate backtracer;

//extern crate termstyle;

pub use klogger::*;

#[macro_use]
mod prelude;
pub mod panic;

#[cfg(all(target_arch = "x86_64"))]
#[path = "arch/x86_64/mod.rs"]
pub mod arch;

mod allocator;
mod mm;

use core::alloc::{GlobalAlloc, Layout};
use mm::BespinSlabsProvider;
use slabmalloc::{PageProvider, ZoneAllocator};
use spin::Mutex;

unsafe impl Send for BespinSlabsProvider {}
unsafe impl Sync for BespinSlabsProvider {}

static PAGER: Mutex<BespinSlabsProvider> = Mutex::new(BespinSlabsProvider::new());

pub struct SafeZoneAllocator(Mutex<ZoneAllocator<'static>>);

impl SafeZoneAllocator {
    pub const fn new(provider: &'static Mutex<PageProvider>) -> SafeZoneAllocator {
        SafeZoneAllocator(Mutex::new(ZoneAllocator::new(provider)))
    }
}

unsafe impl GlobalAlloc for SafeZoneAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        assert!(layout.align().is_power_of_two());
        let ptr = self.0.lock().allocate(layout);
        slog!("allocated ptr=0x{:x} layout={:?}", ptr as usize, layout);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        slog!("dealloc ptr = 0x{:x} layout={:?}", ptr as usize, layout);
        self.0.lock().deallocate(ptr, layout);
    }
}

#[global_allocator]
static MEM_PROVIDER: SafeZoneAllocator = SafeZoneAllocator::new(&PAGER);

#[cfg(not(test))]
mod std {
    pub use core::cmp;
    pub use core::fmt;
    pub use core::iter;
    pub use core::marker;
    pub use core::ops;
    pub use core::option;
}

#[repr(u8)]
// If this type is modified, update run.sh script as well.
pub enum ExitReason {
    Ok = 0,
    ReturnFromMain = 1,
    KernelPanic = 2,
    OutOfMemory = 3,
    UnhandledInterrupt = 4,
    GeneralProtectionFault = 5,
    PageFault = 6,
}

/// Kernel entry-point
#[cfg(not(feature = "integration-tests"))]
pub fn main() {
    slog!("Reached architecture independent area");
    arch::debug::shutdown(ExitReason::Ok);
}

#[cfg(all(feature = "integration-tests", feature = "test-exit"))]
#[no_mangle]
pub fn main() {
    arch::debug::shutdown(ExitReason::Ok);
}

#[cfg(all(feature = "integration-tests", feature = "test-pfault"))]
#[no_mangle]
pub fn main() {
    unsafe {
        let ptr = 0x8000000 as *mut u8;
        let val = *ptr;
    }
}

#[cfg(all(feature = "integration-tests", feature = "test-gpfault"))]
#[no_mangle]
pub fn main() {
    // Note that int!(13) doesn't work in qemu. It doesn't push an error code properly for it.
    // So we cause a GP by loading garbage in the ss segment register.
    use x86::segmentation::{load_ss, SegmentSelector};
    unsafe {
        load_ss(SegmentSelector::new(99, x86::Ring::Ring3));
    }
}

#[cfg(all(feature = "integration-tests", feature = "test-alloc"))]
#[no_mangle]
pub fn main() {
    use alloc::vec::Vec;
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    for i in 0..1024 {
        buf.push(i);
    }

    slog!("1024 bytes allocated.");
    arch::debug::shutdown(ExitReason::Ok);
}