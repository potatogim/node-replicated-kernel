use alloc::vec;
use alloc::vec::Vec;
use core::cmp::{Eq, PartialEq};
use core::ptr;

use proptest::prelude::*;

use super::*;
use crate::*;

use crate::memory::tcache::TCache;
use crate::memory::vspace::model::ModelAddressSpace;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestAction {
    Map(VAddr, Frame, MapAction),
    Adjust(VAddr, MapAction),
    Resolve(VAddr),
    Unmap(VAddr),
}

fn action() -> impl Strategy<Value = TestAction> {
    // Generate a possible action for applying on the vspace,
    // note we currently assume that a frame is either of base-page
    // or large-page size. Arbitrary frames are possible to map
    // but our (simple) vspace can only unmap one page-table
    // entry at a time.
    prop_oneof![
        (
            vaddrs(0x60_0000),
            frames(0x60_0000, 0x40_0000),
            map_rights()
        )
            .prop_map(|(a, b, c)| TestAction::Map(a, b, c)),
        (vaddrs(0x60_0000), map_rights()).prop_map(|(a, b)| TestAction::Adjust(a, b)),
        vaddrs(0x60_0000).prop_map(TestAction::Unmap),
        vaddrs(0x60_0000).prop_map(TestAction::Resolve),
    ]
}

fn actions() -> impl Strategy<Value = Vec<TestAction>> {
    prop::collection::vec(action(), 0..512)
}

fn map_rights() -> impl Strategy<Value = MapAction> {
    prop_oneof![
        Just(MapAction::ReadUser),
        Just(MapAction::ReadKernel),
        Just(MapAction::ReadWriteUser),
        Just(MapAction::ReadWriteKernel),
        Just(MapAction::ReadExecuteUser),
        Just(MapAction::ReadExecuteKernel),
        Just(MapAction::ReadWriteExecuteUser),
        Just(MapAction::ReadWriteExecuteKernel),
    ]
}

fn page_sizes() -> impl Strategy<Value = usize> {
    prop::sample::select(vec![BASE_PAGE_SIZE, LARGE_PAGE_SIZE])
}

prop_compose! {
    fn frames(max_base: u64, max_size: usize)(base in base_aligned_addr(max_base), size in page_sizes()) -> Frame {
        let paddr = if base & 0x1 > 0 {
            PAddr::from(base).align_down_to_base_page()
        } else {
            PAddr::from(base).align_down_to_large_page()
        };

        Frame::new(paddr, size, 0)
    }
}

prop_compose! {
    fn vaddrs(max: u64)(base in 0..max) -> VAddr { VAddr::from(base & !0xfff) }
}

prop_compose! {
    fn base_aligned_addr(max: u64)(base in 0..max) -> u64 { base & !0xfff }
}

prop_compose! {
    fn large_aligned_addr(max: u64)(base in 0..max) -> u64 { base & !0x1fffff }
}

proptest! {
    // Verify that our implementation behaves according to the `ModelAddressSpace`.
    #[test]
    fn model_equivalence(ops in actions()) {
        //let _r = env_logger::try_init();

        use TestAction::*;
        let mut mm = crate::arch::memory::MemoryMapper::new();
        let f = mm.allocate_frame(16 * 1024 * 1024).unwrap();
        let mut tcache = TCache::new_with_frame(0, 0, f);

        let mut totest = VSpace::new();
        let mut model: ModelAddressSpace = Default::default();

        for action in ops {
            match action {
                Map(base, frame, rights) => {
                    let rmodel = model.map_frame(base, frame, rights, &mut tcache);
                    let rtotest = totest.map_frame(base, frame, rights, &mut tcache);
                    assert_eq!(rmodel, rtotest);
                }
                Adjust(vaddr, rights) => {
                    let rmodel = model.adjust(vaddr, rights);
                    let rtotest = totest.adjust(vaddr, rights);
                    assert_eq!(rmodel, rtotest);
                }
                Resolve(vaddr) => {
                    let rmodel = model.resolve(vaddr);
                    let rtotest = totest.resolve(vaddr);
                    assert_eq!(rmodel, rtotest);
                }
                Unmap(vaddr) => {
                    let rmodel = model.unmap(vaddr);
                    let rtotest = totest.unmap(vaddr);
                    assert_eq!(rmodel, rtotest);
                }
            }
        }
    }
}