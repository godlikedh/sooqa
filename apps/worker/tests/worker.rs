use sooqa_jobs::{Job, JobStatus, JobType, NewJob};
use sooqa_worker::HandlerRegistry;
use time::OffsetDateTime;
use uuid::Uuid;

#[test]
fn registry_is_typed_by_job_kind() {
    let mut registry = HandlerRegistry::new();
    registry.register(JobType::CleanupWorkspace, |_job| Box::pin(async { Ok(()) }));
    assert!(registry.contains(JobType::CleanupWorkspace));
    assert_eq!(registry.job_types(), vec![JobType::CleanupWorkspace]);
}

#[test]
fn job_envelopes_have_typed_payloads_and_fenced_leases() {
    let new_job = NewJob::publish_post(Uuid::new_v4());
    let now = OffsetDateTime::now_utc();
    let job = Job {
        id: Uuid::new_v4(),
        command: new_job.command().clone(),
        status: JobStatus::Running,
        priority: 0,
        run_at: now,
        attempt_count: 1,
        max_attempts: 3,
        lease_token: Some(Uuid::new_v4()),
        lease_owner: Some("worker".to_owned()),
        lease_expires_at: Some(now),
        last_heartbeat_at: Some(now),
        last_error_class: None,
        last_error_message: None,
        dedupe_key: None,
        created_at: now,
        updated_at: now,
        completed_at: None,
    };
    assert_eq!(job.job_type(), JobType::PublishPost);
    assert_eq!(job.lease().unwrap().worker_id, "worker");
}
