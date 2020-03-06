use bitflags::*;

/// Struct used in `file_getinfo` systemcall.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub struct FileInfo {
    pub ftype: u64,
    pub fsize: u64,
}

bitflags! {
    /// File flags to open the file
    pub struct FileFlags:u64 {
        const O_NONE = 0x0000;
        const O_RDONLY = 0x0001; /* open for reading only */
        const O_WRONLY = 0x0002; /* open for writing only */
        const O_RDWR = 0x0003; /* open for reading and writing */
        const O_CREAT = 0x0200; /* create if nonexistant */
        const O_TRUNC = 0x0400; /* truncate to zero length */
    }
}

/// Needed to implement default for memnode.
impl Default for FileFlags {
    fn default() -> FileFlags {
        FileFlags::O_NONE
    }
}

/// Convert u64 to FileFlags.
impl From<u64> for FileFlags {
    fn from(flag: u64) -> FileFlags {
        FileFlags::from_bits_truncate(flag)
    }
}

/// Convert FileFlags to u64.
impl From<FileFlags> for u64 {
    fn from(flag: FileFlags) -> u64 {
        flag.bits()
    }
}

/// Implementation for FileFlags to check if the file is opened using
/// readable, writable or create flags.
impl FileFlags {
    pub fn is_read(&self) -> bool {
        (*self & FileFlags::O_RDONLY) == FileFlags::O_RDONLY
    }

    pub fn is_write(&self) -> bool {
        (*self & FileFlags::O_WRONLY) == FileFlags::O_WRONLY
    }

    pub fn is_create(&self) -> bool {
        (*self & FileFlags::O_CREAT) == FileFlags::O_CREAT
    }

    pub fn is_truncate(&self) -> bool {
        (*self & FileFlags::O_TRUNC) == FileFlags::O_TRUNC
    }
}

bitflags! {
    /// FileModes to store the file in the memory. A file can be stored in
    /// readable, writable or executable mode.
    pub struct FileModes: u64 {
        const S_IRWXU = 0x007; /* RWX mask for user */
        const S_IRUSR = 0x004; /* R for user */
        const S_IWUSR = 0x002; /* W for user */
        const S_IXUSR = 0x001; /* X for user */
    }
}

/// Convert u64 to FileModes.
impl From<u64> for FileModes {
    fn from(mode: u64) -> FileModes {
        FileModes::from_bits_truncate(mode)
    }
}

/// Convert FileModes to u64.
impl From<FileModes> for u64 {
    fn from(mode: FileModes) -> u64 {
        mode.bits()
    }
}

/// Implementation of FileModes to check if the file is readable, writable or executable.
impl FileModes {
    pub fn is_readable(&self) -> bool {
        (*self & FileModes::S_IRUSR) == FileModes::S_IRUSR
    }

    pub fn is_writable(&self) -> bool {
        (*self & FileModes::S_IWUSR) == FileModes::S_IWUSR
    }

    pub fn is_executable(&self) -> bool {
        (*self & FileModes::S_IXUSR) == FileModes::S_IXUSR
    }
}