use std::sync::atomic::AtomicPtr;

mod sstable;
mod thread_ctx;

struct DiskCtl {
    atomic_ptr: AtomicPtr<()>
}