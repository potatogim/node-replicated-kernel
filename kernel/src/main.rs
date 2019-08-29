#![no_std]
#![feature(
    intrinsics,
    asm,
    lang_items,
    const_fn,
    raw,
    box_syntax,
    start,
    panic_info_message,
    allocator_api,
    global_asm,
    linkage,
    c_variadic,
    alloc_layout_extra,
    ptr_internals,
    compiler_builtins_lib,
    ptr_offset_from,
    box_into_raw_non_null,
    box_into_pin,
    untagged_unions
)]

#[cfg(not(target_os = "none"))]
extern crate libc;

#[macro_use]
pub mod mutex;

extern crate alloc;
extern crate custom_error;

#[macro_use]
extern crate log;

#[cfg(target_arch = "x86_64")]
extern crate x86;

#[cfg(target_arch = "x86_64")]
extern crate apic;

#[cfg(target_arch = "x86_64")]
extern crate slabmalloc;

#[cfg(target_arch = "x86_64")]
#[macro_use]
extern crate klogger;

#[cfg(target_arch = "x86_64")]
extern crate elfloader;

extern crate backtracer;
extern crate rawtime;

#[macro_use]
extern crate lazy_static;

pub use klogger::*;

#[cfg(target_os = "none")]
pub mod panic;

#[cfg(all(target_arch = "x86_64", target_os = "none"))]
#[path = "arch/x86_64/mod.rs"]
pub mod arch;

#[cfg(all(target_arch = "x86_64", target_family = "unix"))]
#[path = "arch/unix/mod.rs"]
pub mod arch;

mod kcb;
mod memory;
#[macro_use]
mod prelude;
mod error;

#[cfg(target_os = "none")]
extern crate acpica_sys;

use core::alloc::{GlobalAlloc, Layout};
use memory::{BespinSlabsProvider, PhysicalAllocator};
use slabmalloc::{PageProvider, ZoneAllocator};
use spin::Mutex;

mod std {
    pub use core::cmp;
    pub use core::fmt;
    pub use core::iter;
    pub use core::marker;
    pub use core::ops;
    pub use core::option;
}

#[allow(dead_code)]
static PAGER: Mutex<BespinSlabsProvider> = Mutex::new(BespinSlabsProvider::new());

#[allow(dead_code)]
pub struct SafeZoneAllocator(Mutex<ZoneAllocator<'static>>);

impl SafeZoneAllocator {
    pub const fn new(provider: &'static Mutex<PageProvider>) -> SafeZoneAllocator {
        SafeZoneAllocator(Mutex::new(ZoneAllocator::new(provider)))
    }
}

unsafe impl GlobalAlloc for SafeZoneAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        trace!("alloc layout={:?}", layout);
        if layout.size() <= ZoneAllocator::MAX_ALLOC_SIZE {
            let ptr = self.0.lock().allocate(layout);
            trace!("allocated ptr=0x{:x} layout={:?}", ptr as usize, layout);
            ptr
        } else {
            let kcb = crate::kcb::get_kcb();
            let mut fmanager = kcb.pmanager();

            let mut f = fmanager.allocate(layout);
            let ptr = f.map_or(core::ptr::null_mut(), |mut region| {
                region.zero();
                region.kernel_vaddr().as_mut_ptr()
            });
            trace!("allocated ptr=0x{:x} layout={:?}", ptr as usize, layout);
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        debug!("dealloc ptr = 0x{:x} layout={:?}", ptr as usize, layout);
        if layout.size() <= ZoneAllocator::MAX_ALLOC_SIZE {
            //debug!("dealloc ptr = 0x{:x} layout={:?}", ptr as usize, layout);
            self.0.lock().deallocate(ptr, layout);
        } else {
            use arch::memory::{kernel_vaddr_to_paddr, VAddr};
            let kcb = crate::kcb::get_kcb();
            let mut fmanager = kcb.pmanager();
            fmanager.deallocate(
                memory::Frame::new(
                    kernel_vaddr_to_paddr(VAddr::from_u64(ptr as u64)),
                    layout.size(),
                ),
                layout,
            );
        }
    }
}

#[global_allocator]
static MEM_PROVIDER: SafeZoneAllocator = SafeZoneAllocator::new(&PAGER);

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
    UserSpaceError = 7,
}

/// Kernel entry-point
#[no_mangle]
#[cfg(not(feature = "integration-test"))]
pub fn xmain() {
    debug!("Reached architecture independent area");
    error!("error");
    warn!("warning");
    info!("info");
    debug!("debug");
    trace!("trace");

    debug!("allocating a region of mem");
    unsafe {
        {
            let mem_mgmt = kcb::get_kcb().pmanager();
            mem_mgmt.print_info();
        }
        let new_region: *mut u8 =
            alloc::alloc::alloc(Layout::from_size_align_unchecked(8192, 4096));
        let p: *mut u8 = new_region.offset(4096);
        assert!(!p.is_null());

        {
            let mem_mgmt = kcb::get_kcb().pmanager();
            mem_mgmt.print_info();
        }
    }

    arch::debug::shutdown(ExitReason::Ok);
}

include!("integration_main.rs");
