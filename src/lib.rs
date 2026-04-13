#![cfg_attr(windows, feature(abi_vectorcall))]
use crate::config::Config;
use crate::conn::{QuicheServerSocket, SocketAddress};
use ext_php_rs::prelude::*;

pub mod config;
pub mod conn;
pub mod quiche;
pub mod timer;

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    let _ = env_logger::builder().is_test(false).try_init();
    let module = module
        .name("ext-quiche")
        .class::<Config>()
        .class::<SocketAddress>()
        .class::<QuicheServerSocket>();

    let module = timer::register(module);

    module
}
