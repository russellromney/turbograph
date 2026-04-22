#[cfg(feature = "extension-tests")]
mod vfs_smoke {
    use std::sync::Arc;

    #[tokio::test]
    async fn sync_manifest_bytes_roundtrip() {
        // This test requires LBUG_STATIC_EXTENSIONS=turbograph and TURBOGRAPH_DIR.
        // It is gated behind the extension-tests feature so cargo check works
        // without a ladybug source tree present.
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Arc::new(
            lbug::Database::new(
                tmp.path().join("db").to_str().unwrap(),
                lbug::SystemConfig::default(),
            )
            .unwrap(),
        );
        let vfs = turbograph::TurbographVfs::new(db);

        // Without the extension loaded, sync returns an error (UDF not found).
        let result = vfs.sync().await;
        assert!(
            result.is_err(),
            "sync should fail without turbograph extension: {:?}",
            result
        );

        // manifest_bytes returns NULL -> None when no manifest exists.
        let result = vfs.manifest_bytes().await;
        assert!(
            result.is_ok() && result.unwrap().is_none(),
            "manifest_bytes should return None without extension or manifest"
        );

        // manifest_version returns NULL -> None when no manifest exists.
        let result = vfs.manifest_version().await;
        assert!(
            result.is_ok() && result.unwrap().is_none(),
            "manifest_version should return None without extension or manifest"
        );

        // set_manifest_bytes returns an error on NULL.
        let result = vfs.set_manifest_bytes(&[0x01, 0x02, 0x03]).await;
        assert!(
            result.is_err(),
            "set_manifest_bytes should fail without turbograph extension: {:?}",
            result
        );
    }
}
