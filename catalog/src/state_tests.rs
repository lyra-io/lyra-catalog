use super::*;
use lyra_meta::metadata::MemoryMetadata;
use lyra_meta::utils::verifier::make_verifier;
use opentelemetry::global;
use std::time::Duration;

#[tokio::test]
async fn forced_drop_timeout_remains_terminal_without_retry() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let owner = metadata.get_user("lyrasys").await.unwrap().unwrap().id();
    let state = State::new(metadata.clone(), global::meter("test"))
        .await
        .unwrap();
    state.set_ready(true);
    metadata
        .create_database(Database::new("blocked", owner))
        .await
        .unwrap();
    let token = CancellationToken::new();
    // Deliberately retain an admitted request after cancellation to model a
    // worker that has not quiesced. No test-only lifecycle behavior is used.
    let admitted = state.admit("blocked", owner, token.clone()).await.unwrap();
    let command = Command::Drop {
        name: "blocked".into(),
        if_exists: false,
        force: true,
        timeout: Duration::from_millis(10),
    };
    let error = state.execute(command.clone(), 0, owner).await.unwrap_err();
    assert_eq!(error.code(), "57014");
    assert!(token.is_cancelled());
    assert_eq!(
        metadata
            .get_database("blocked")
            .await
            .unwrap()
            .unwrap()
            .value()
            .state(),
        DatabaseState::DropFailed
    );
    assert!(
        state
            .admit("blocked", owner, CancellationToken::new())
            .await
            .is_err()
    );
    drop(admitted);
    assert_eq!(
        state.execute(command, 0, owner).await.unwrap_err().code(),
        "55000"
    );
    let restarted = State::new(metadata, global::meter("test")).await.unwrap();
    restarted.set_ready(true);
    assert!(
        restarted
            .admit("blocked", owner, CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        restarted
            .admit("public", owner, CancellationToken::new())
            .await
            .is_ok()
    );
}
