mod support;

use std::{net::TcpListener, process::Command, time::Duration};

use tokio::time::sleep;
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

    let port = reserve_port(&shard.host);
    let mut child = spawn_pgcrab(&shard.host, port);

    wait_for_listen(&shard.host, port).await;

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

fn reserve_port(host: &str) -> u16 {
    let addr = format!("{host}:0");
    let listener = TcpListener::bind(&addr).expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

fn spawn_pgcrab(host: &str, port: u16) -> std::process::Child {
    let exe = env!("CARGO_BIN_EXE_pgcrab");
    let config_path = std::env::var("PGCRAB_CONFIG_FILE").unwrap_or_else(|_| "pgcrab.toml".into());

    Command::new(exe)
        .env("PGCRAB_HOST", host)
        .env("PGCRAB_PORT", port.to_string())
        .env("PGCRAB_CONFIG_FILE", config_path)
        .spawn()
        .expect("spawn pgcrab")
}

async fn wait_for_listen(host: &str, port: u16) {
    let addr = format!("{host}:{port}");
    for _ in 0..50 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("pgcrab did not start listening on {addr}");
}
