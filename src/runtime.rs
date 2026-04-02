// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Global Tokio runtime for the C FFI layer.

use std::sync::{LazyLock, RwLock};
use std::time::Duration;

use lance_core::{Error, Result};

use crate::error::ffi_try;

/// Global multi-threaded Tokio runtime, shared across all FFI calls.
/// Initialized lazily on first access.
pub(crate) static RT: LazyLock<RwLock<Option<tokio::runtime::Runtime>>> =
    LazyLock::new(|| RwLock::new(None));

fn create_runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::Internal {
            message: format!("Failed to create tokio runtime: {err}"),
            location: snafu::location!(),
        })
}

fn ensure_runtime() -> Result<()> {
    {
        let guard = RT.read().map_err(|_| Error::Internal {
            message: "runtime lock poisoned".to_string(),
            location: snafu::location!(),
        })?;
        if guard.is_some() {
            return Ok(());
        }
    }

    let mut guard = RT.write().map_err(|_| Error::Internal {
        message: "runtime lock poisoned".to_string(),
        location: snafu::location!(),
    })?;
    if guard.is_none() {
        *guard = Some(create_runtime()?);
    }
    Ok(())
}

fn init_inner() -> Result<i32> {
    ensure_runtime()?;
    Ok(0)
}

fn shutdown_inner() -> Result<i32> {
    let runtime = {
        let mut guard = RT.write().map_err(|_| Error::Internal {
            message: "runtime lock poisoned".to_string(),
            location: snafu::location!(),
        })?;
        guard.take()
    };

    if let Some(runtime) = runtime {
        runtime.shutdown_timeout(Duration::from_secs(30));
    }

    Ok(0)
}

/// Lazily initialize the shared Tokio runtime used by the C API.
///
/// Returns 0 on success and -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn lance_init() -> i32 {
    ffi_try!(init_inner(), neg)
}

/// Shut down and drop the shared Tokio runtime used by the C API.
///
/// Returns 0 on success and -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn lance_shutdown() -> i32 {
    ffi_try!(shutdown_inner(), neg)
}

pub(crate) fn runtime_handle() -> tokio::runtime::Handle {
    ensure_runtime().expect("failed to initialize lance-c runtime");
    let guard = RT
        .read()
        .expect("lance-c runtime lock poisoned while acquiring runtime handle");
    guard
        .as_ref()
        .expect("lance-c runtime missing after initialization")
        .handle()
        .clone()
}

/// Block the current thread on an async future using the global runtime.
pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
    let mut future = Some(f);
    loop {
        {
            let guard = RT
                .read()
                .expect("lance-c runtime lock poisoned while acquiring runtime");
            if let Some(runtime) = guard.as_ref() {
                return runtime.block_on(future.take().expect("future already taken"));
            }
        }
        ensure_runtime().expect("failed to initialize lance-c runtime");
    }
}
