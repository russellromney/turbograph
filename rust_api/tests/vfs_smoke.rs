#[cfg(feature = "extension-tests")]
mod vfs_smoke {
    use std::sync::Arc;

    #[tokio::test]
    async fn extension_loaded_without_active_tfs_reports_empty_or_error() {
        // The extension test command statically links turbograph into Ladybug,
        // but this database is not opened through a configured Turbograph VFS.
        // Read-only probes should report "no active manifest"; mutating ops
        // should fail clearly instead of pretending an apply/sync succeeded.
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
        assert!(result.is_err(), "sync should fail without active TFS: {:?}", result);

        let result = vfs.manifest_bytes().await;
        assert_eq!(result.unwrap(), None);

        let result = vfs.manifest_version().await;
        assert_eq!(result.unwrap(), None);

        let result = vfs.set_manifest_bytes(&[0x01, 0x02, 0x03]).await;
        assert!(
            result.is_err(),
            "set_manifest_bytes should fail without active TFS: {:?}",
            result
        );
    }
}
