use sooqa_persistence::TelegramRepository;

#[tokio::test]
async fn telegram_receipts_are_process_local_and_fenced() {
    let repository = TelegramRepository::default();
    let claim = match repository.claim_update(42).await.unwrap() {
        sooqa_persistence::TelegramUpdateClaimResult::Claimed(claim) => claim,
        other => panic!("unexpected claim result: {other:?}"),
    };
    assert!(matches!(
        repository.claim_update(42).await.unwrap(),
        sooqa_persistence::TelegramUpdateClaimResult::InProgress
    ));
    repository.complete_update(claim).await.unwrap();
    assert!(matches!(
        repository.claim_update(42).await.unwrap(),
        sooqa_persistence::TelegramUpdateClaimResult::Completed
    ));
}
