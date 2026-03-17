use crate::codex::TurnContext;
use crate::protocol::EventMsg;
use crate::protocol::WarningEvent;
use crate::state::TaskKind;
use crate::tasks::SessionTask;
use crate::tasks::SessionTaskContext;
use async_trait::async_trait;
use codex_git::CreateGhostCommitOptions;
use codex_git::GhostSnapshotReport;
use codex_git::GitToolingError;
use codex_git::create_ghost_commit_with_report;
use codex_protocol::models::ResponseItem;
use codex_protocol::user_input::UserInput;
use codex_utils_readiness::Readiness;
use codex_utils_readiness::Token;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

pub(crate) struct GhostSnapshotTask {
    token: Token,
}

const SNAPSHOT_GATE_TIMEOUT: Duration = Duration::from_secs(30);

async fn mark_tool_gate_ready(gate: &codex_utils_readiness::ReadinessFlag, token: Token) {
    match gate.mark_ready(token).await {
        Ok(true) => info!("ghost snapshot gate marked ready"),
        Ok(false) => warn!("ghost snapshot gate already ready"),
        Err(err) => warn!("failed to mark ghost snapshot ready: {err}"),
    }
}

async fn release_tool_gate_then<F, Fut>(
    gate: &codex_utils_readiness::ReadinessFlag,
    token: Token,
    follow_up: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    mark_tool_gate_ready(gate, token).await;
    follow_up().await;
}

#[async_trait]
impl SessionTask for GhostSnapshotTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.ghost_snapshot"
    }

    async fn run(
        self: Arc<Self>,
        session: Arc<SessionTaskContext>,
        ctx: Arc<TurnContext>,
        _input: Vec<UserInput>,
        cancellation_token: CancellationToken,
    ) -> Option<String> {
        tokio::task::spawn(async move {
            let token = self.token;
            let warnings_enabled = !ctx.ghost_snapshot.disable_warnings;
            let snapshot_timed_out = Arc::new(AtomicBool::new(false));
            let (snapshot_finished_tx, snapshot_finished_rx) = tokio::sync::oneshot::channel();

            {
                let ctx_for_task = ctx.clone();
                let session_for_task = session.clone();
                let snapshot_timed_out = snapshot_timed_out.clone();

                tokio::task::spawn(async move {
                    let repo_path = ctx_for_task.cwd.clone();
                    let ghost_snapshot = ctx_for_task.ghost_snapshot.clone();
                    let ghost_snapshot_for_commit = ghost_snapshot.clone();

                    let result = tokio::task::spawn_blocking(move || {
                        let options = CreateGhostCommitOptions::new(&repo_path)
                            .ghost_snapshot(ghost_snapshot_for_commit);
                        create_ghost_commit_with_report(&options)
                    })
                    .await;

                    let timed_out = snapshot_timed_out.load(Ordering::Relaxed);

                    match result {
                        Ok(Ok((ghost_commit, report))) => {
                            if timed_out {
                                info!(
                                    "ghost snapshot finished after gate timeout; discarding stale snapshot"
                                );
                            } else {
                                info!("ghost snapshot blocking task finished");
                                if warnings_enabled {
                                    for message in format_snapshot_warnings(
                                        ghost_snapshot.ignore_large_untracked_files,
                                        ghost_snapshot.ignore_large_untracked_dirs,
                                        &report,
                                    ) {
                                        session_for_task
                                            .session
                                            .send_event(
                                                &ctx_for_task,
                                                EventMsg::Warning(WarningEvent { message }),
                                            )
                                            .await;
                                    }
                                }
                                session_for_task
                                    .session
                                    .record_conversation_items(
                                        &ctx_for_task,
                                        &[ResponseItem::GhostSnapshot {
                                            ghost_commit: ghost_commit.clone(),
                                        }],
                                    )
                                    .await;
                                info!("ghost commit captured: {}", ghost_commit.id());
                            }
                        }
                        Ok(Err(err)) => match err {
                            GitToolingError::NotAGitRepository { .. } => info!(
                                sub_id = ctx_for_task.sub_id.as_str(),
                                "skipping ghost snapshot because current directory is not a Git repository"
                            ),
                            _ if timed_out => {
                                info!(
                                    sub_id = ctx_for_task.sub_id.as_str(),
                                    "ghost snapshot failed after gate timeout: {err}"
                                );
                            }
                            _ => {
                                warn!(
                                    sub_id = ctx_for_task.sub_id.as_str(),
                                    "failed to capture ghost snapshot: {err}"
                                );
                            }
                        },
                        Err(err) if timed_out => {
                            warn!(
                                sub_id = ctx_for_task.sub_id.as_str(),
                                "ghost snapshot task panicked after gate timeout: {err}"
                            );
                        }
                        Err(err) => {
                            warn!(
                                sub_id = ctx_for_task.sub_id.as_str(),
                                "ghost snapshot task panicked: {err}"
                            );
                            let message =
                                format!("Snapshots disabled after ghost snapshot panic: {err}.");
                            session_for_task
                                .session
                                .notify_background_event(&ctx_for_task, message)
                                .await;
                        }
                    }

                    let _ = snapshot_finished_tx.send(());
                });
            }

            let cancelled = tokio::select! {
                _ = cancellation_token.cancelled() => {
                    snapshot_timed_out.store(true, Ordering::Relaxed);
                    true
                }
                _ = snapshot_finished_rx => false,
                _ = tokio::time::sleep(SNAPSHOT_GATE_TIMEOUT) => {
                    snapshot_timed_out.store(true, Ordering::Relaxed);
                    false
                }
            };

            if cancelled {
                info!("ghost snapshot task cancelled");
            }

            if snapshot_timed_out.load(Ordering::Relaxed) && warnings_enabled && !cancelled {
                release_tool_gate_then(&ctx.tool_call_gate, token, || async {
                    session
                        .session
                        .send_event(
                            &ctx,
                            EventMsg::Warning(WarningEvent {
                                message: "Repository snapshot is taking too long. Continuing without an undo snapshot for this turn; large untracked or ignored files can slow snapshots.".to_string(),
                            }),
                        )
                        .await;
                })
                .await;
            } else {
                mark_tool_gate_ready(&ctx.tool_call_gate, token).await;
            }
        });
        None
    }
}

impl GhostSnapshotTask {
    pub(crate) fn new(token: Token) -> Self {
        Self { token }
    }
}

fn format_snapshot_warnings(
    ignore_large_untracked_files: Option<i64>,
    ignore_large_untracked_dirs: Option<i64>,
    report: &GhostSnapshotReport,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(message) = format_large_untracked_warning(ignore_large_untracked_dirs, report) {
        warnings.push(message);
    }
    if let Some(message) =
        format_ignored_untracked_files_warning(ignore_large_untracked_files, report)
    {
        warnings.push(message);
    }
    warnings
}

fn format_large_untracked_warning(
    ignore_large_untracked_dirs: Option<i64>,
    report: &GhostSnapshotReport,
) -> Option<String> {
    if report.large_untracked_dirs.is_empty() {
        return None;
    }
    let threshold = ignore_large_untracked_dirs?;
    const MAX_DIRS: usize = 3;
    let mut parts: Vec<String> = Vec::new();
    for dir in report.large_untracked_dirs.iter().take(MAX_DIRS) {
        parts.push(format!("{} ({} files)", dir.path.display(), dir.file_count));
    }
    if report.large_untracked_dirs.len() > MAX_DIRS {
        let remaining = report.large_untracked_dirs.len() - MAX_DIRS;
        parts.push(format!("{remaining} more"));
    }
    Some(format!(
        "Repository snapshot ignored large untracked directories (>= {threshold} files): {}. These directories are excluded from snapshots and undo cleanup. Adjust `ghost_snapshot.ignore_large_untracked_dirs` to change this behavior.",
        parts.join(", ")
    ))
}

fn format_ignored_untracked_files_warning(
    ignore_large_untracked_files: Option<i64>,
    report: &GhostSnapshotReport,
) -> Option<String> {
    let threshold = ignore_large_untracked_files?;
    if report.ignored_untracked_files.is_empty() {
        return None;
    }

    const MAX_FILES: usize = 3;
    let mut parts: Vec<String> = Vec::new();
    for file in report.ignored_untracked_files.iter().take(MAX_FILES) {
        parts.push(format!(
            "{} ({})",
            file.path.display(),
            format_bytes(file.byte_size)
        ));
    }
    if report.ignored_untracked_files.len() > MAX_FILES {
        let remaining = report.ignored_untracked_files.len() - MAX_FILES;
        parts.push(format!("{remaining} more"));
    }

    Some(format!(
        "Repository snapshot ignored untracked files larger than {}: {}. These files are preserved during undo cleanup, but their contents are not captured in the snapshot. Adjust `ghost_snapshot.ignore_large_untracked_files` to change this behavior. To avoid this message in the future, update your `.gitignore`.",
        format_bytes(threshold),
        parts.join(", ")
    ))
}

fn format_bytes(bytes: i64) -> String {
    const KIB: i64 = 1024;
    const MIB: i64 = 1024 * 1024;

    if bytes >= MIB {
        return format!("{} MiB", bytes / MIB);
    }
    if bytes >= KIB {
        return format!("{} KiB", bytes / KIB);
    }
    format!("{bytes} B")
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_git::LargeUntrackedDir;
    use codex_utils_readiness::Readiness;
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    #[test]
    fn large_untracked_warning_includes_threshold() {
        let report = GhostSnapshotReport {
            large_untracked_dirs: vec![LargeUntrackedDir {
                path: PathBuf::from("models"),
                file_count: 250,
            }],
            ignored_untracked_files: Vec::new(),
        };

        let message = format_large_untracked_warning(Some(200), &report).unwrap();
        assert!(message.contains(">= 200 files"));
    }

    #[test]
    fn large_untracked_warning_disabled_when_threshold_disabled() {
        let report = GhostSnapshotReport {
            large_untracked_dirs: vec![LargeUntrackedDir {
                path: PathBuf::from("models"),
                file_count: 250,
            }],
            ignored_untracked_files: Vec::new(),
        };

        assert_eq!(format_large_untracked_warning(None, &report), None);
    }

    #[tokio::test]
    async fn release_tool_gate_then_marks_ready_before_follow_up_finishes() {
        let gate = Arc::new(codex_utils_readiness::ReadinessFlag::new());
        let token = gate.subscribe().await.expect("token");
        let (follow_up_started_tx, follow_up_started_rx) = oneshot::channel();
        let (follow_up_finish_tx, follow_up_finish_rx) = oneshot::channel::<()>();

        let waiter = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move {
                gate.wait_ready().await;
            }
        });

        let helper = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move {
                release_tool_gate_then(&gate, token, || async {
                    let _ = follow_up_started_tx.send(());
                    let _ = follow_up_finish_rx.await;
                })
                .await;
            }
        });

        follow_up_started_rx.await.expect("follow-up started");
        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("gate should be ready before follow-up completes")
            .expect("waiter task");
        let _ = follow_up_finish_tx.send(());
        helper.await.expect("helper task");
    }
}
