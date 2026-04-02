// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Fragment creation C API.

use std::ffi::c_char;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_schema::Schema as ArrowSchema;
use lance::dataset::WriteParams;
use lance::dataset::fragment::write::FragmentCreateBuilder;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::{Error, Result};
use lance_file::version::LanceFileVersion;
use snafu::location;

use crate::error::ffi_try;
use crate::helpers;
use crate::runtime::block_on;

fn validate_local_spool_uri(uri: &str) -> Result<()> {
    if uri.is_empty() {
        return Err(Error::InvalidInput {
            source: "spool_uri must not be empty".into(),
            location: location!(),
        });
    }

    if uri.contains("://") && !uri.starts_with("file://") {
        return Err(Error::InvalidInput {
            source: "spool_uri must be a local path or file:// URI".into(),
            location: location!(),
        });
    }

    Ok(())
}

/// Create exactly one local Lance fragment under `spool_uri/data/`.
///
/// Returns 0 on success and -1 on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_create(
    spool_uri: *const c_char,
    schema: *const FFI_ArrowSchema,
    stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    ffi_try!(
        unsafe { fragment_create_inner(spool_uri, schema, stream) },
        neg
    )
}

unsafe fn fragment_create_inner(
    spool_uri: *const c_char,
    schema: *const FFI_ArrowSchema,
    stream: *mut FFI_ArrowArrayStream,
) -> Result<i32> {
    if schema.is_null() || stream.is_null() {
        return Err(Error::InvalidInput {
            source: "spool_uri, schema, and stream must not be NULL".into(),
            location: location!(),
        });
    }

    let spool_uri =
        unsafe { helpers::parse_c_string(spool_uri)? }.ok_or_else(|| Error::InvalidInput {
            source: "spool_uri must not be NULL".into(),
            location: location!(),
        })?;
    validate_local_spool_uri(spool_uri)?;

    let arrow_schema = ArrowSchema::try_from(unsafe { &*schema })?;
    let target_schema = LanceSchema::try_from(&arrow_schema)?;
    let reader = unsafe { ArrowArrayStreamReader::from_raw(stream) }?;

    block_on(async move {
        let write_params = WriteParams::with_storage_version(LanceFileVersion::V2_2);
        FragmentCreateBuilder::new(spool_uri)
            .schema(&target_schema)
            .write_params(&write_params)
            .write(reader, Some(0))
            .await?;
        Ok(0)
    })
}
