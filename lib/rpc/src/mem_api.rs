use alloc::vec::Vec;
use core::result::Result;

/// Some handle/identifier used to access remote memory. This is just a placeholder for now.
pub type MemHandle = u64;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum MemError {
    // TODO
    Unknown,
}

// Memory Operations - args & return values based on POSIX API
pub trait MemClientAPI {

    /// Remote read from space in an Remote Memory Region
    fn mem_read(&self, handle: MemHandle, count: u64) -> Result<Vec<u8>, MemError>;

    /// Remote write to space in an Remote Memory Region
    fn mem_write(&self, handle: MemHandle, src: Vec<u8>) -> Result<u64, MemError>;

    /// Allocate a Remote Memory Region
    fn mem_malloc(&self, size: u64) -> Result<MemHandle, MemError>;

    /// free a Remote Memory Region and notify others TODO: do we actually notify others?
    fn mem_free(&self, handle: MemHandle) -> Result<(), MemError>;

    /// open a Remote Memory Region with name
    fn mem_map(&self, length: u64, prot: u64, handle: MemHandle, flags: u64, offset: u64) -> Result<MemHandle, MemError>;

    /// close a Remote Memory Region with name
    fn mem_unmap(&self, handle: MemHandle, length: u64) -> Result<(), MemError>;

    /// set space in an Remote Memory Region with value
    fn mem_memset(&self, handle: MemHandle, c: u8, n: u64) -> Result<MemHandle, MemError>;

    /// copy content from Remote Memory Region to Remote Memory Region
    fn mem_memcpy(&self, dst: MemHandle, src: MemHandle, n: u64) -> Result<MemHandle, MemError>;

    /// move data from Remote Memory Region to Remote Memory Region
    fn mem_memmove(&self, dst: MemHandle, src: MemHandle, length: u64) -> Result<MemHandle, MemError>;
}