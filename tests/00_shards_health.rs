mod support;

#[tokio::test]
async fn shards_are_reachable() {
    support::ensure_shards_accessible().await;
}
