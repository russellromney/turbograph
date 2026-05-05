#[cfg(feature = "extension-tests")]
mod vfs_smoke {
    use std::sync::Arc;

    #[tokio::test]
    async fn udf_calls_fail_clearly_without_extension() {
        // The Rust handle is intentionally thin: if the turbograph extension is
        // not loaded into Ladybug, every UDF-backed operation must fail clearly
        // instead of silently pretending the VFS has an empty manifest.
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Arc::new(
            lbug::Database::new(
                tmp.path().join("db").to_str().unwrap(),
                lbug::SystemConfig::default(),
            )
            .unwrap(),
        );
        let vfs = turbograph::TurbographVfs::new(db);

        let result = vfs.sync().await;
        assert!(
            result.is_err(),
            "sync should fail without turbograph extension: {:?}",
            result
        );

        let result = vfs.manifest_bytes().await;
        assert!(
            result.is_err(),
            "manifest_bytes should fail without turbograph extension: {:?}",
            result
        );

        let result = vfs.manifest_version().await;
        assert!(
            result.is_err(),
            "manifest_version should fail without turbograph extension: {:?}",
            result
        );

        let result = vfs.set_manifest_bytes(&[0x01, 0x02, 0x03]).await;
        assert!(
            result.is_err(),
            "set_manifest_bytes should fail without turbograph extension: {:?}",
            result
        );
    }
}
