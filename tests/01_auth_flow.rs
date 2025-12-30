mod support;

use tokio_postgres::NoTls;

#[tokio::test]
async fn auth_flow_accepts_valid_user() {
    support::ensure_shards_accessible().await;
    let cfg = support::load_config().expect("load pgcrab.toml");
    let shard = cfg
        .shards
        .first()
        .cloned()
        .expect("expected at least one [[shards]] entry");
    let users = cfg.users;

    assert!(
        !users.is_empty(),
        "expected at least one [[users]] entry"
    );

    let port = support::reserve_port(&shard.host);
    let mut child = support::spawn_pgcrab(&shard.host, port);

    support::wait_for_listen(&shard.host, port).await;

    for user in users {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            shard.host, port, user.username, user.password, shard.name
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .expect("connect should succeed");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        drop(client);
    }
    let _ = child.kill();
}
