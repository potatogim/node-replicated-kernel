use abomonation::{encode, decode};
use alloc::borrow::ToOwned;
use alloc::{vec, vec::{Vec}};
use log::{debug, warn};

use smoltcp::iface::EthernetInterface;
use smoltcp::socket::{SocketSet, SocketHandle, TcpSocket, TcpSocketBuffer};
use smoltcp::time::Instant;
use smoltcp::wire::IpAddress;

use vmxnet3::smoltcp::DevQueuePhy;

use crate::cluster_api::{ClusterError, ClusterClientAPI, NodeId};
use crate::rpc_api::{RPCError, RPCClientAPI};
use crate::rpc::*;

const RX_BUF_LEN: usize = 4096;
const TX_BUF_LEN: usize = 4096;

pub struct TCPClient<'a> {
    iface: EthernetInterface<'a, DevQueuePhy>,
    sockets: SocketSet<'a>,
    server_handle: Option<SocketHandle>,
    server_ip: IpAddress,
    server_port: u16,
    client_port: u16,
    client_id: NodeId,
    req_id: u64,
}

impl TCPClient<'_> {
    pub fn new<'a>(server_ip: IpAddress, server_port: u16, iface: EthernetInterface<'a, DevQueuePhy>) -> TCPClient<'a> {
        TCPClient {
            iface: iface,
            sockets: SocketSet::new(vec![]),
            server_handle: None,
            server_ip: server_ip,
            server_port: server_port,
            client_port: 10110,
            client_id: 0,
            req_id: 0,
        }
    }
}

impl ClusterClientAPI for TCPClient<'_> {

    /// Register with controller, analogous to LITE join_cluster()
    /// TODO: add timeout?? with error returned if timeout occurs?
    fn join_cluster(&mut self) -> Result<NodeId, ClusterError> {
        // create client socket
        let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; RX_BUF_LEN]);
        let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; TX_BUF_LEN]);
        let tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
        self.server_handle = Some(self.sockets.add(tcp_socket));

        {
            let mut socket = self.sockets.get::<TcpSocket>(self.server_handle.unwrap());
            socket.connect((self.server_ip, self.server_port), self.client_port).unwrap();
            debug!("Attempting to connect to server {}:{}", self.server_ip, self.server_port);
        }

        // Connect to server
        loop {
            match self.iface.poll(&mut self.sockets, Instant::from_millis(0)) {
                Ok(_) => {},
                Err(e) => {
                    warn!("poll error: {}", e);
                }
            }
            let socket = self.sockets.get::<TcpSocket>(self.server_handle.unwrap());
            // Waiting for send/recv forces the TCP handshake to fully complete
            if socket.is_active() && (socket.may_send() || socket.may_recv()) {
                debug!("Connected to server, ready to send/recv data");
                break;
            }
        }

        self.rpc_call(RPCType::Registration, Vec::new()).unwrap();
        Ok(self.client_id)
    }

}

/// RPC client operations
impl RPCClientAPI for TCPClient<'_> {

    /// calls a remote RPC function with ID
    fn rpc_call(&mut self, rpc_id: RPCType, data: Vec<u8>) -> Result<Vec<u8>, RPCError> {

        // Create request header
        let req_hdr = RPCHeader {
            client_id: self.client_id,
            req_id: self.req_id,
            msg_type: rpc_id,
            msg_len: data.len() as u64,
        };
        
        // Serialize request header then request body
        let mut req_data = Vec::new();
        unsafe { encode(&req_hdr, &mut req_data) }.unwrap();
        if data.len() > 0 {
            //unsafe { encode(&data, &mut req_data) }.unwrap();
            req_data.extend(data);
        }

        // Send request
        self.msg_send(req_data).unwrap();
        
        // Receive response and parse header
        let mut res = self.msg_recv().unwrap();
        let (res_hdr, res_body) = unsafe { decode::<RPCHeader>(&mut res) }.unwrap();

        // Check request & client IDs, and also length of received data
        if ((res_hdr.client_id != self.client_id) && rpc_id != RPCType::Registration) || res_hdr.req_id != self.req_id {
            warn!("Mismatched client id ({}, {}) or request id ({}, {})", res_hdr.client_id, self.client_id, res_hdr.req_id, self.req_id);
            return Err(RPCError::MalformedResponse);
        } else if res_hdr.msg_len != (res_body.len() as u64) {
            warn!("Did not receive all RPC data!");
            return Err(RPCError::MalformedResponse);
        }

        // Increment request id
        self.req_id += 1;

        // If registration, update id
        if rpc_id == RPCType::Registration {
            self.client_id = res_hdr.client_id;
            debug!("Set client ID to: {}", self.client_id);
            return Ok(Vec::new());
        }

        Ok(res_body.to_vec())
    }

    /// send data to a remote node
    fn msg_send(&mut self, data: Vec<u8>) -> Result<(), RPCError> {
        // TODO: check TX capacity, chunk if necessary??

        let mut data_sent = false;
        loop {
            match self.iface.poll(&mut self.sockets, Instant::from_millis(0)) {
                Ok(_) => {},
                Err(e) => {
                    warn!("poll error: {}", e);
                }
            }

            let mut socket = self.sockets.get::<TcpSocket>(self.server_handle.unwrap());
            if socket.can_send() && !data_sent {
                socket.send_slice(&data[..]).unwrap();
                debug!("Client sent: {:?}", data);
                data_sent = true;
            } else if data_sent {
                return Ok(());
            }
        }
    }

    /// receive data from a remote node
    fn msg_recv(&mut self) -> Result<Vec<u8>, RPCError> {
        loop {
            match self.iface.poll(&mut self.sockets, Instant::from_millis(0)) {
                Ok(_) => {},
                Err(e) => {
                    warn!("poll error: {}", e);
                }
            }

            let mut socket = self.sockets.get::<TcpSocket>(self.server_handle.unwrap());
            if socket.can_recv() {
                // TODO: check rx capacity
                let data = socket.recv(|buffer| {
                    let recvd_len = buffer.len();
                    let data = buffer.to_owned();
                    (recvd_len, data)
                }).unwrap();
                if data.len() > 0 {
                    debug!("Client recv: {:?}", data);
                    return Ok(data);
                }
            }
        }
    }
}

impl TCPClient<'_> {
    pub fn fio_write(&mut self, fd: u64, data: Vec<u8>) -> Result<u64, RPCError> {
        self.fio_writeat(fd, 0, data)
    }

    pub fn fio_writeat(&mut self, fd: u64, offset: u64, data: Vec<u8>) -> Result<u64, RPCError> {
        let req = RPCWriteReq {
            fd: fd,
            offset: offset,
        };
        let mut req_data = Vec::new();
        unsafe { encode(&req, &mut req_data) }.unwrap();
        req_data.extend(data);

        let mut res = self.rpc_call(RPCType::WriteAt, req_data).unwrap();
        if let Some((res, remaining)) = unsafe { decode::<RPCRWRes>(&mut res) } {
            if remaining.len() > 0 {
                return Err(RPCError::MalformedResponse);
            }
            debug!("Wrote {:?} bytes", res.num_bytes);
            return Ok(res.num_bytes);
        } else {
            return Err(RPCError::MalformedResponse);
        }
    }

    pub fn fio_read(&mut self, fd: u64, len: u64) -> Result<u64, RPCError> {
        self.fio_readat(fd, len, 0)
    }

    pub fn fio_readat(&mut self, fd: u64, len: u64, offset: u64) -> Result<u64, RPCError> {
        let req = RPCReadReq {
            fd: fd,
            len: len,
            offset: offset,
        };
        let mut req_data = Vec::new();
        unsafe { encode(&req, &mut req_data) }.unwrap();

        let mut res = self.rpc_call(RPCType::ReadAt, req_data).unwrap();
        if let Some((res, remaining)) = unsafe { decode::<RPCRWRes>(&mut res) } {
            if res.num_bytes != remaining.len() as u64 {
                warn!("Unexpected amount of data: bytes_read={:?}, data.len={:?}", res.num_bytes, remaining.len());
                return Err(RPCError::MalformedRequest);
            }
            debug!("Read {:?} bytes: {:?}", res.num_bytes, remaining);
            return Ok(res.num_bytes);
        } else {
            return Err(RPCError::MalformedResponse);
        }
    }

    pub fn fio_create(&mut self, pathname: &[u8]) -> Result<u64, RPCError> {
        self.fio_open_create(pathname, RPCType::Create)
    }

    pub fn fio_open(&mut self, pathname: &[u8]) -> Result<u64, RPCError> {
        self.fio_open_create(pathname, RPCType::Open)
    }

    fn fio_open_create(&mut self, pathname: &[u8], rpc_type: RPCType) -> Result<u64, RPCError>{
        let req = RPCOpenReq {
            pathname: pathname.to_vec(),
        };
        let mut req_data = Vec::new();
        unsafe { encode(&req, &mut req_data) }.unwrap();
        let mut res = self.rpc_call(rpc_type, req_data).unwrap();
        if let Some((res, remaining)) = unsafe { decode::<RPCOpenRes>(&mut res) } {
            if remaining.len() > 0 {
                return Err(RPCError::MalformedResponse);
            }
            debug!("Open() fd = {}", res.fd);
            return Ok(res.fd);
        } else {
            return Err(RPCError::MalformedResponse);
        }
    }

    pub fn fio_close(&mut self, fd: u64) -> Result<(), RPCError> {
        let req = RPCCloseReq {
            fd: fd,
        };
        let mut req_data = Vec::new();
        unsafe { encode(&req, &mut req_data) }.unwrap();

        let mut res = self.rpc_call(RPCType::Close, req_data).unwrap();
        if let Some((res, remaining)) = unsafe { decode::<RPCCloseRes>(&mut res) } {
            if remaining.len() > 0 {
                return Err(RPCError::MalformedResponse);
            }
            debug!("Close() {:?}", res);
            return Ok(());
        } else {
            return Err(RPCError::MalformedResponse);
        }
    }

    pub fn fio_delete(&mut self, pathname: &[u8]) -> Result<(), RPCError> {
        let req = RPCDeleteReq {
            pathname: pathname.to_vec(),
        };
        let mut req_data = Vec::new();
        unsafe { encode(&req, &mut req_data) }.unwrap();
        let mut res = self.rpc_call(RPCType::Delete, req_data).unwrap();
        if let Some((res, remaining)) = unsafe { decode::<RPCDeleteRes>(&mut res) } {
            if remaining.len() > 0 {
                return Err(RPCError::MalformedResponse);
            }
            debug!("Delete() {:?}", res);
            return Ok(());
        } else {
            return Err(RPCError::MalformedResponse);
        }
    }

    pub fn fio_rename(&mut self, oldname: &[u8], newname: &[u8]) -> Result<(), RPCError> {
        let req = RPCRenameReq {
            oldname: oldname.to_vec(),
            newname: newname.to_vec(),
        };
        let mut req_data = Vec::new();
        unsafe { encode(&req, &mut req_data) }.unwrap();
        let mut res = self.rpc_call(RPCType::FileRename, req_data).unwrap();
        if let Some((res, remaining)) = unsafe { decode::<RPCRenameRes>(&mut res) } {
            if remaining.len() > 0 {
                return Err(RPCError::MalformedResponse);
            }
            debug!("Delete() {:?}", res);
            return Ok(());
        } else {
            return Err(RPCError::MalformedResponse);
        }
    }
}
