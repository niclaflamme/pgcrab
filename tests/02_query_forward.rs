mod support;

use tokio_postgres::NoTls;

#[tokio::test]
async fn query_forwarding_works() {
    support::ensure_shards_accessible().await;
    let cfg = support::load_config().expect("load pgcrab.toml");
    let shard = cfg
        .shards
        .first()
        .cloned()
        .expect("expected at least one [[shards]] entry");
    let user = cfg
        .users
        .first()
        .cloned()
        .expect("expected at least one [[users]] entry");

    let port = support::reserve_port(&shard.host);
    let mut child = support::spawn_pgcrab(&shard.host, port);
    support::wait_for_listen(&shard.host, port).await;

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

    let rows = client
        .simple_query("select 1")
        .await
        .expect("select 1 should succeed");

    let row = rows
        .iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("expected a row");

    let value = row.get(0).expect("value present");
    assert_eq!(value, "1");

    let _ = child.kill();
}
