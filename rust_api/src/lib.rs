use std::sync::Arc;

use anyhow::{anyhow, Result};

/// Rust handle for a local turbograph VFS. Wraps the Kuzu UDFs with
/// an async API; hides connection management, BLOB marshalling, and
/// NULL-to-error translation.
///
/// Parallels `SharedTurboliteVfs` in turbolite. Construction requires
/// the turbograph Kuzu extension to be loaded in the lbug database
/// (static-linked via `LBUG_STATIC_EXTENSIONS=turbograph`).
pub struct TurbographVfs {
    db: Arc<lbug::Database>,
}

impl TurbographVfs {
    pub fn new(db: Arc<lbug::Database>) -> Self {
        Self { db }
    }

    /// `turbograph_sync()` — checkpoint + upload dirty pages. Leader-only.
    /// Returns the manifest version produced by the checkpoint.
    pub async fn sync(&self) -> Result<u64> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("turbograph: failed to create connection: {e}"))?;
            let mut result = conn
                .query("RETURN turbograph_sync()")
                .map_err(|e| anyhow!("turbograph_sync() failed: {e}"))?;
            let row = result
                .next()
                .ok_or_else(|| anyhow!("turbograph_sync() returned no rows"))?;
            if row.is_empty() {
                return Err(anyhow!("turbograph_sync() returned empty row"));
            }
            match &row[0] {
                lbug::Value::Null(_) => Err(anyhow!("turbograph_sync() returned NULL")),
                lbug::Value::Int64(v) => Ok(*v as u64),
                other => Err(anyhow!(
                    "turbograph_sync() returned unexpected type: {other:?} (expected INT64)"
                )),
            }
        })
        .await
        .map_err(|e| anyhow!("turbograph_sync task panicked: {e}"))?
    }

    /// `turbograph_manifest_bytes()` — capture current VFS manifest.
    /// Returns `None` if the VFS has no manifest (UDF returned NULL).
    pub async fn manifest_bytes(&self) -> Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("turbograph: failed to create connection: {e}"))?;
            let mut result = conn
                .query("RETURN turbograph_manifest_bytes()")
                .map_err(|e| anyhow!("turbograph_manifest_bytes() failed: {e}"))?;
            let row = match result.next() {
                Some(r) => r,
                None => return Ok(None),
            };
            if row.is_empty() {
                return Ok(None);
            }
            match &row[0] {
                lbug::Value::Null(_) => Ok(None),
                lbug::Value::Blob(v) => Ok(Some(v.clone())),
                other => Err(anyhow!(
                    "turbograph_manifest_bytes() returned unexpected type: {other:?} (expected BLOB)"
                )),
            }
        })
        .await
        .map_err(|e| anyhow!("turbograph_manifest_bytes task panicked: {e}"))?
    }

    /// `turbograph_manifest_bytes_with_graphstream_delta(seq, prefix)` —
    /// capture hybrid payload (tag 0x02). Leader-only. Returns bytes.
    pub async fn manifest_bytes_with_graphstream_delta(
        &self,
        journal_seq: u64,
        segment_prefix: &str,
    ) -> Result<Vec<u8>> {
        let db = self.db.clone();
        let segment_prefix = segment_prefix.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("turbograph: failed to create connection: {e}"))?;
            let mut prepared = conn
                .prepare("RETURN turbograph_manifest_bytes_with_graphstream_delta($seq, $prefix)")
                .map_err(|e| anyhow!("prepare failed: {e}"))?;
            let mut result = conn
                .execute(
                    &mut prepared,
                    vec![
                        ("seq", lbug::Value::Int64(journal_seq as i64)),
                        ("prefix", lbug::Value::String(segment_prefix)),
                    ],
                )
                .map_err(|e| {
                    anyhow!("turbograph_manifest_bytes_with_graphstream_delta() failed: {e}")
                })?;
            let row = result.next().ok_or_else(|| {
                anyhow!("turbograph_manifest_bytes_with_graphstream_delta() returned no rows")
            })?;
            if row.is_empty() {
                return Err(anyhow!(
                    "turbograph_manifest_bytes_with_graphstream_delta() returned empty row"
                ));
            }
            match &row[0] {
                lbug::Value::Null(_) => Err(anyhow!(
                    "turbograph_manifest_bytes_with_graphstream_delta() returned NULL"
                )),
                lbug::Value::Blob(v) => Ok(v.clone()),
                other => Err(anyhow!(
                    "turbograph_manifest_bytes_with_graphstream_delta() returned unexpected type: {other:?} (expected BLOB)"
                )),
            }
        })
        .await
        .map_err(|e| {
            anyhow!("turbograph_manifest_bytes_with_graphstream_delta task panicked: {e}")
        })?
    }

    /// `turbograph_set_manifest_bytes(bytes)` — apply bytes to local VFS.
    /// Errors on NULL return (decode/apply failed).
    pub async fn set_manifest_bytes(&self, bytes: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("turbograph: failed to create connection: {e}"))?;
            let mut prepared = conn
                .prepare("RETURN turbograph_set_manifest_bytes($bytes)")
                .map_err(|e| anyhow!("prepare failed: {e}"))?;
            let mut result = conn
                .execute(&mut prepared, vec![("bytes", lbug::Value::Blob(bytes))])
                .map_err(|e| anyhow!("turbograph_set_manifest_bytes() failed: {e}"))?;
            let row = result
                .next()
                .ok_or_else(|| anyhow!("turbograph_set_manifest_bytes() returned no rows"))?;
            if row.is_empty() {
                return Err(anyhow!("turbograph_set_manifest_bytes() returned empty row"));
            }
            match &row[0] {
                lbug::Value::Null(_) => {
                    Err(anyhow!("turbograph_set_manifest_bytes() returned NULL"))
                }
                lbug::Value::Int64(_) => Ok(()),
                other => Err(anyhow!(
                    "turbograph_set_manifest_bytes() returned unexpected type: {other:?} (expected INT64)"
                )),
            }
        })
        .await
        .map_err(|e| anyhow!("turbograph_set_manifest_bytes task panicked: {e}"))?
    }

    /// `turbograph_get_manifest_version()` — cheap version poll.
    /// Returns `None` if the VFS has no manifest yet.
    pub async fn manifest_version(&self) -> Result<Option<u64>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("turbograph: failed to create connection: {e}"))?;
            let mut result = conn
                .query("RETURN turbograph_get_manifest_version()")
                .map_err(|e| anyhow!("turbograph_get_manifest_version() failed: {e}"))?;
            let row = match result.next() {
                Some(r) => r,
                None => return Ok(None),
            };
            if row.is_empty() {
                return Ok(None);
            }
            match &row[0] {
                lbug::Value::Null(_) => Ok(None),
                lbug::Value::Int64(v) => Ok(Some(*v as u64)),
                other => Err(anyhow!(
                    "turbograph_get_manifest_version() returned unexpected type: {other:?} (expected INT64)"
                )),
            }
        })
        .await
        .map_err(|e| anyhow!("turbograph_manifest_version task panicked: {e}"))?
    }
}
