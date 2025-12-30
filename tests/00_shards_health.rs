mod support;

#[tokio::test]
async fn shards_are_reachable() {
    support::ensure_shards_accessible().await;
    let cfg = support::load_config().expect("load pgcrab.toml");
    assert!(
        !cfg.users.is_empty(),
        "pgcrab.toml has no [[users]] entries"
    );
    for user in cfg.users {
        assert!(!user.username.trim().is_empty(), "user.username is empty");
        assert!(
            !user.database_name.trim().is_empty(),
            "user.database_name is empty"
        );
        assert!(!user.password.is_empty(), "user.password is empty");
    }
}
