use abomonation::Abomonation;
use alloc::vec::Vec;

#[derive(Debug, Eq, PartialEq, PartialOrd, Clone, Copy)]
#[repr(u8)]
pub enum RPCType {
    /// Client requesting to register with RPC server
    Registration = 1,

    /// Create a file
    Create = 2,
    /// Open a file
    Open = 3,
    /// Read from a file
    Read = 4,
    /// Read from a file from the given offset
    ReadAt = 5,
    /// Write to a file
    Write = 6,
    /// Write to a file
    WriteAt = 7,
    /// Close an opened file.
    Close = 8,
    /// Get the information related to the file.
    GetInfo = 9,
    /// Delete the file
    Delete = 10,
    /// Write to a file without going into NR.
    WriteDirect = 11,
    /// Rename a file.
    FileRename = 12,
    /// Create a directory.
    MkDir = 13,
    
    Unknown,
}

pub fn is_fileio(op: RPCType) -> bool {
    return op >= RPCType::Create && op <= RPCType::MkDir
}

impl From<u8> for RPCType {
    /// Construct a RPCType enum based on a 8-bit value.
    fn from(op: u8) -> RPCType {
        match op {
            1 => RPCType::Registration,

            // TODO: Add RPC requests
            // 2 => RPCType::Create,
            3 => RPCType::Open,
            4 => RPCType::Read,
            5 => RPCType::ReadAt,
            6 => RPCType::Write,
            7 => RPCType::WriteAt,
            8 => RPCType::Close,
            9 => RPCType::GetInfo,
            10 => RPCType::Delete,
            11 => RPCType::WriteDirect,
            12 => RPCType::FileRename,
            13 => RPCType::MkDir,

            _ => RPCType::Unknown,
        }
    }
}
unsafe_abomonate!(RPCType);

#[derive(Debug)]
pub struct RPCHeader {
    pub client_id : u64,
    pub req_id : u64,
    pub msg_type : RPCType,
    pub msg_len : u64,
}
unsafe_abomonate!(RPCHeader: client_id, req_id, msg_type, msg_len);

//////// FILEIO Operations 
#[derive(Debug)]
pub struct RPCOpenReq {
    pub pathname: Vec<u8>,
}
unsafe_abomonate!(RPCOpenReq: pathname);

#[derive(Debug)]
pub struct RPCOpenRes {
    pub fd: u64,
}
unsafe_abomonate!(RPCOpenRes: fd);

#[derive(Debug)]
pub struct RPCCloseReq {
    pub fd: u64,
}
unsafe_abomonate!(RPCCloseReq: fd);

#[derive(Debug)]
pub struct RPCCloseRes {
    pub ret: Result<(), u64>,
}
unsafe_abomonate!(RPCCloseRes: ret);

#[derive(Debug)]
pub struct RPCDeleteReq {
    pub pathname: Vec<u8>,
}
unsafe_abomonate!(RPCDeleteReq: pathname);

#[derive(Debug)]
pub struct RPCDeleteRes {
    pub ret: Result<(), u64>,
}
unsafe_abomonate!(RPCDeleteRes: ret);

#[derive(Debug)]
pub struct RPCRenameReq {
    pub oldname: Vec<u8>,
    pub newname: Vec<u8>,
}
unsafe_abomonate!(RPCRenameReq: oldname, newname);

#[derive(Debug)]
pub struct RPCRenameRes {
    pub ret: Result<(), u64>,
}
unsafe_abomonate!(RPCRenameRes: ret);

#[derive(Debug)]
pub struct RPCReadReq {
    pub fd: u64,
    pub len: u64,
    pub offset: u64,
}
unsafe_abomonate!(RPCReadReq: fd, len, offset);

#[derive(Debug)]
pub struct RPCWriteReq {
    pub fd: u64,
    pub offset: u64,
}
unsafe_abomonate!(RPCWriteReq: fd, offset);

// Used for both reading and writing
#[derive(Debug)]
pub struct RPCRWRes {
    pub num_bytes: u64,
}
unsafe_abomonate!(RPCRWRes: num_bytes);

//////// End FILEIO Operations 