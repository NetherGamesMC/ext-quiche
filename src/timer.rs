use ext_php_rs::prelude::*;

#[php_function]
pub fn network_eventfd_create() -> i64 {
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) as i64 }
}

#[php_function]
pub fn network_eventfd_signal(fd: i64) {
    let val: u64 = 1;
    unsafe { libc::write(fd as i32, &val as *const u64 as *const _, 8) };
}

#[php_function]
pub fn network_eventfd_close(fd: i64) {
    unsafe { libc::close(fd as i32) };
}

pub fn register(module: ModuleBuilder) -> ModuleBuilder {
    module
        .function(wrap_function!(network_eventfd_create))
        .function(wrap_function!(network_eventfd_signal))
        .function(wrap_function!(network_eventfd_close))
}
