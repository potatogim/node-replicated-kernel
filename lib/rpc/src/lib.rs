#![no_std]

#[macro_use]
extern crate abomonation;
extern crate alloc;
extern crate lazy_static;
extern crate smoltcp;

extern crate vmxnet3;

pub mod cluster_api;
pub mod rpc;
pub mod rpc_api;
pub mod tcp_client;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
