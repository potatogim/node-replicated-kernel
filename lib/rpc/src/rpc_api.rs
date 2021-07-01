use alloc::vec::Vec;
use core::result::Result;

use crate::rpc::{RPCType, RPCHeader};
use crate::cluster_api::{NodeId};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RPCError {
    MalformedResponse,
    MalformedRequest,
    NotImplemented,
    TransportError,
    Unknown,
}

/// RPC server operations
pub trait RPCServerAPI {

    /// register an RPC func with an ID 
    fn rpc_register(&self, rpc_id: RPCType) -> Result<(), RPCError>;

    /// receives next RPC call with RPC ID
    fn rpc_recv(&self) -> Result<(&RPCHeader, Vec<u8>), RPCError>;

    /// replies an RPC call with results
    fn rpc_reply(&self, client: NodeId, data: Vec<u8>) -> Result<(), RPCError>;

    /// Run the RPC server
    fn rpc_run_server(&mut self) -> Result<(), RPCError>;
}

/// RPC client operations
pub trait RPCClientAPI {

    /// calls a remote RPC function with ID
    fn rpc_call(&mut self, rpc_id: RPCType, data: Vec<u8>) -> Result<Vec<u8>, RPCError>;

    /// send data to a remote node
    fn msg_send(&mut self, data: Vec<u8>) -> Result<(), RPCError>;

    /// receive data from a remote node
    fn msg_recv(&mut self) -> Result<Vec<u8>, RPCError>;
}
