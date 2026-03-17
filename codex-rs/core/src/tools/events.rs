use crate::codex::Session;
use crate::codex::TurnContext;
use crate::error::CodexErr;
use crate::error::SandboxErr;
use crate::exec::ExecToolCallOutput;
use crate::function_tool::FunctionCallError;
use crate::parse_command::parse_command;
use crate::protocol::EventMsg;
use crate::protocol::ExecCommandBeginEvent;
use crate::protocol::ExecCommandEndEvent;
use crate::protocol::ExecCommandSource;
use crate::protocol::ExecCommandStatus;
use crate::protocol::FileChange;
use crate::protocol::PatchApplyBeginEvent;
use crate::protocol::PatchApplyEndEvent;
use crate::protocol::PatchApplyStatus;
use crate::protocol::TurnDiffEvent;
use crate::tools::context::SharedTurnDiffTracker;
use crate::tools::sandboxing::ToolError;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_protocol::parse_command::ParsedCommand;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use tracing::warn;

use super::format_exec_output_str;

const TURN_DIFF_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
pub(crate) struct ToolEventCtx<'a> {
    pub session: &'a Session,
    pub turn: &'a TurnContext,
    pub call_id: &'a str,
    pub turn_diff_tracker: Option<&'a SharedTurnDiffTracker>,
}

impl<'a> ToolEventCtx<'a> {
    pub fn new(
        session: &'a Session,
        turn: &'a TurnContext,
        call_id: &'a str,
        turn_diff_tracker: Option<&'a SharedTurnDiffTracker>,
    ) -> Self {
        Self {
            session,
            turn,
            call_id,
            turn_diff_tracker,
        }
    }
}

pub(crate) enum ToolEventStage {
    Begin,
    Success(ExecToolCallOutput),
    Failure(ToolEventFailure),
}

pub(crate) enum ToolEventFailure {
    Output(ExecToolCallOutput),
    Message(String),
    Rejected(String),
}

pub(crate) async fn emit_exec_command_begin(
    ctx: ToolEventCtx<'_>,
    command: &[String],
    cwd: &Path,
    parsed_cmd: &[ParsedCommand],
    source: ExecCommandSource,
    interaction_input: Option<String>,
    process_id: Option<&str>,
) {
    ctx.session
        .send_event(
            ctx.turn,
            EventMsg::ExecCommandBegin(ExecCommandBeginEvent {
                call_id: ctx.call_id.to_string(),
                process_id: process_id.map(str::to_owned),
                turn_id: ctx.turn.sub_id.clone(),
                command: command.to_vec(),
                cwd: cwd.to_path_buf(),
                parsed_cmd: parsed_cmd.to_vec(),
                source,
                interaction_input,
            }),
        )
        .await;
}

pub(crate) async fn build_turn_diff_event(
    tracker: &SharedTurnDiffTracker,
) -> Option<TurnDiffEvent> {
    build_turn_diff_event_with_timeout(tracker, TURN_DIFF_TIMEOUT, |mut tracker| {
        tracker.get_unified_diff()
    })
    .await
}

async fn build_turn_diff_event_with_timeout<F>(
    tracker: &SharedTurnDiffTracker,
    diff_timeout: Duration,
    compute_diff: F,
) -> Option<TurnDiffEvent>
where
    F: FnOnce(TurnDiffTracker) -> anyhow::Result<Option<String>> + Send + 'static,
{
    let tracker_snapshot = {
        let guard = tracker.lock().await;
        guard.clone()
    };
    let diff_task = spawn_blocking(move || compute_diff(tracker_snapshot));
    match timeout(diff_timeout, diff_task).await {
        Ok(Ok(Ok(Some(unified_diff)))) => Some(TurnDiffEvent { unified_diff }),
        Ok(Ok(Ok(None))) => None,
        Ok(Ok(Err(err))) => {
            warn!("failed to compute turn diff: {err:#}");
            None
        }
        Ok(Err(err)) => {
            warn!("turn diff task failed: {err}");
            None
        }
        Err(_) => {
            warn!(
                "turn diff computation exceeded {:?}; skipping diff emission",
                diff_timeout
            );
            None
        }
    }
}

// Concrete, allocation-free emitter: avoid trait objects and boxed futures.
pub(crate) enum ToolEmitter {
    Shell {
        command: Vec<String>,
        cwd: PathBuf,
        source: ExecCommandSource,
        parsed_cmd: Vec<ParsedCommand>,
        freeform: bool,
    },
    ApplyPatch {
        changes: HashMap<PathBuf, FileChange>,
        auto_approved: bool,
    },
    UnifiedExec {
        command: Vec<String>,
        cwd: PathBuf,
        source: ExecCommandSource,
        parsed_cmd: Vec<ParsedCommand>,
        process_id: Option<String>,
    },
}

impl ToolEmitter {
    pub fn shell(
        command: Vec<String>,
        cwd: PathBuf,
        source: ExecCommandSource,
        freeform: bool,
    ) -> Self {
        let parsed_cmd = parse_command(&command);
        Self::Shell {
            command,
            cwd,
            source,
            parsed_cmd,
            freeform,
        }
    }

    pub fn apply_patch(changes: HashMap<PathBuf, FileChange>, auto_approved: bool) -> Self {
        Self::ApplyPatch {
            changes,
            auto_approved,
        }
    }

    pub fn unified_exec(
        command: &[String],
        cwd: PathBuf,
        source: ExecCommandSource,
        process_id: Option<String>,
    ) -> Self {
        let parsed_cmd = parse_command(command);
        Self::UnifiedExec {
            command: command.to_vec(),
            cwd,
            source,
            parsed_cmd,
            process_id,
        }
    }

    pub async fn emit(&self, ctx: ToolEventCtx<'_>, stage: ToolEventStage) {
        match (self, stage) {
            (
                Self::Shell {
                    command,
                    cwd,
                    source,
                    parsed_cmd,
                    ..
                },
                stage,
            ) => {
                emit_exec_stage(
                    ctx,
                    ExecCommandInput::new(command, cwd.as_path(), parsed_cmd, *source, None, None),
                    stage,
                )
                .await;
            }

            (
                Self::ApplyPatch {
                    changes,
                    auto_approved,
                },
                ToolEventStage::Begin,
            ) => {
                if let Some(tracker) = ctx.turn_diff_tracker {
                    let mut guard = tracker.lock().await;
                    guard.on_patch_begin(changes);
                }
                ctx.session
                    .send_event(
                        ctx.turn,
                        EventMsg::PatchApplyBegin(PatchApplyBeginEvent {
                            call_id: ctx.call_id.to_string(),
                            turn_id: ctx.turn.sub_id.clone(),
                            auto_approved: *auto_approved,
                            changes: changes.clone(),
                        }),
                    )
                    .await;
            }
            (Self::ApplyPatch { changes, .. }, ToolEventStage::Success(output)) => {
                emit_patch_end(
                    ctx,
                    changes.clone(),
                    output.stdout.text.clone(),
                    output.stderr.text.clone(),
                    output.exit_code == 0,
                    if output.exit_code == 0 {
                        PatchApplyStatus::Completed
                    } else {
                        PatchApplyStatus::Failed
                    },
                )
                .await;
            }
            (
                Self::ApplyPatch { changes, .. },
                ToolEventStage::Failure(ToolEventFailure::Output(output)),
            ) => {
                emit_patch_end(
                    ctx,
                    changes.clone(),
                    output.stdout.text.clone(),
                    output.stderr.text.clone(),
                    output.exit_code == 0,
                    if output.exit_code == 0 {
                        PatchApplyStatus::Completed
                    } else {
                        PatchApplyStatus::Failed
                    },
                )
                .await;
            }
            (
                Self::ApplyPatch { changes, .. },
                ToolEventStage::Failure(ToolEventFailure::Message(message)),
            ) => {
                emit_patch_end(
                    ctx,
                    changes.clone(),
                    String::new(),
                    (*message).to_string(),
                    false,
                    PatchApplyStatus::Failed,
                )
                .await;
            }
            (
                Self::ApplyPatch { changes, .. },
                ToolEventStage::Failure(ToolEventFailure::Rejected(message)),
            ) => {
                emit_patch_end(
                    ctx,
                    changes.clone(),
                    String::new(),
                    (*message).to_string(),
                    false,
                    PatchApplyStatus::Declined,
                )
                .await;
            }
            (
                Self::UnifiedExec {
                    command,
                    cwd,
                    source,
                    parsed_cmd,
                    process_id,
                },
                stage,
            ) => {
                emit_exec_stage(
                    ctx,
                    ExecCommandInput::new(
                        command,
                        cwd.as_path(),
                        parsed_cmd,
                        *source,
                        None,
                        process_id.as_deref(),
                    ),
                    stage,
                )
                .await;
            }
        }
    }

    pub async fn begin(&self, ctx: ToolEventCtx<'_>) {
        self.emit(ctx, ToolEventStage::Begin).await;
    }

    fn format_exec_output_for_model(
        &self,
        output: &ExecToolCallOutput,
        ctx: ToolEventCtx<'_>,
    ) -> String {
        match self {
            Self::Shell { freeform: true, .. } => {
                super::format_exec_output_for_model_freeform(output, ctx.turn.truncation_policy)
            }
            _ => super::format_exec_output_for_model_structured(output, ctx.turn.truncation_policy),
        }
    }

    pub async fn finish(
        &self,
        ctx: ToolEventCtx<'_>,
        out: Result<ExecToolCallOutput, ToolError>,
    ) -> Result<String, FunctionCallError> {
        let (event, result) = match out {
            Ok(output) => {
                let content = self.format_exec_output_for_model(&output, ctx);
                let exit_code = output.exit_code;
                let event = ToolEventStage::Success(output);
                let result = if exit_code == 0 {
                    Ok(content)
                } else {
                    Err(FunctionCallError::RespondToModel(content))
                };
                (event, result)
            }
            Err(ToolError::Codex(CodexErr::Sandbox(SandboxErr::Timeout { output })))
            | Err(ToolError::Codex(CodexErr::Sandbox(SandboxErr::Denied { output, .. }))) => {
                let response = self.format_exec_output_for_model(&output, ctx);
                let event = ToolEventStage::Failure(ToolEventFailure::Output(*output));
                let result = Err(FunctionCallError::RespondToModel(response));
                (event, result)
            }
            Err(ToolError::Codex(err)) => {
                let message = format!("execution error: {err:?}");
                let event = ToolEventStage::Failure(ToolEventFailure::Message(message.clone()));
                let result = Err(FunctionCallError::RespondToModel(message));
                (event, result)
            }
            Err(ToolError::Rejected(msg)) => {
                // Normalize common rejection messages for exec tools so tests and
                // users see a clear, consistent phrase.
                //
                // NOTE: ToolError::Rejected is currently used for both user-declined approvals
                // and some operational/runtime rejection paths (for example setup failures).
                // We intentionally map all of them through the "rejected" event path for now,
                // which means a subset of non-user failures may be reported as Declined.
                //
                // TODO: We should add a new ToolError variant for user-declined approvals.
                let normalized = if msg == "rejected by user" {
                    match self {
                        Self::Shell { .. } | Self::UnifiedExec { .. } => {
                            "exec command rejected by user".to_string()
                        }
                        Self::ApplyPatch { .. } => "patch rejected by user".to_string(),
                    }
                } else {
                    msg
                };
                let event = ToolEventStage::Failure(ToolEventFailure::Rejected(normalized.clone()));
                let result = Err(FunctionCallError::RespondToModel(normalized));
                (event, result)
            }
        };
        self.emit(ctx, event).await;
        result
    }
}

struct ExecCommandInput<'a> {
    command: &'a [String],
    cwd: &'a Path,
    parsed_cmd: &'a [ParsedCommand],
    source: ExecCommandSource,
    interaction_input: Option<&'a str>,
    process_id: Option<&'a str>,
}

impl<'a> ExecCommandInput<'a> {
    fn new(
        command: &'a [String],
        cwd: &'a Path,
        parsed_cmd: &'a [ParsedCommand],
        source: ExecCommandSource,
        interaction_input: Option<&'a str>,
        process_id: Option<&'a str>,
    ) -> Self {
        Self {
            command,
            cwd,
            parsed_cmd,
            source,
            interaction_input,
            process_id,
        }
    }
}

struct ExecCommandResult {
    stdout: String,
    stderr: String,
    aggregated_output: String,
    exit_code: i32,
    duration: Duration,
    formatted_output: String,
    status: ExecCommandStatus,
}

async fn emit_exec_stage(
    ctx: ToolEventCtx<'_>,
    exec_input: ExecCommandInput<'_>,
    stage: ToolEventStage,
) {
    match stage {
        ToolEventStage::Begin => {
            emit_exec_command_begin(
                ctx,
                exec_input.command,
                exec_input.cwd,
                exec_input.parsed_cmd,
                exec_input.source,
                exec_input.interaction_input.map(str::to_owned),
                exec_input.process_id,
            )
            .await;
        }
        ToolEventStage::Success(output)
        | ToolEventStage::Failure(ToolEventFailure::Output(output)) => {
            let exec_result = ExecCommandResult {
                stdout: output.stdout.text.clone(),
                stderr: output.stderr.text.clone(),
                aggregated_output: output.aggregated_output.text.clone(),
                exit_code: output.exit_code,
                duration: output.duration,
                formatted_output: format_exec_output_str(&output, ctx.turn.truncation_policy),
                status: if output.exit_code == 0 {
                    ExecCommandStatus::Completed
                } else {
                    ExecCommandStatus::Failed
                },
            };
            emit_exec_end(ctx, exec_input, exec_result).await;
        }
        ToolEventStage::Failure(ToolEventFailure::Message(message)) => {
            let text = message.to_string();
            let exec_result = ExecCommandResult {
                stdout: String::new(),
                stderr: text.clone(),
                aggregated_output: text.clone(),
                exit_code: -1,
                duration: Duration::ZERO,
                formatted_output: text,
                status: ExecCommandStatus::Failed,
            };
            emit_exec_end(ctx, exec_input, exec_result).await;
        }
        ToolEventStage::Failure(ToolEventFailure::Rejected(message)) => {
            let text = message.to_string();
            let exec_result = ExecCommandResult {
                stdout: String::new(),
                stderr: text.clone(),
                aggregated_output: text.clone(),
                exit_code: -1,
                duration: Duration::ZERO,
                formatted_output: text,
                status: ExecCommandStatus::Declined,
            };
            emit_exec_end(ctx, exec_input, exec_result).await;
        }
    }
}

async fn emit_exec_end(
    ctx: ToolEventCtx<'_>,
    exec_input: ExecCommandInput<'_>,
    exec_result: ExecCommandResult,
) {
    ctx.session
        .send_event(
            ctx.turn,
            EventMsg::ExecCommandEnd(ExecCommandEndEvent {
                call_id: ctx.call_id.to_string(),
                process_id: exec_input.process_id.map(str::to_owned),
                turn_id: ctx.turn.sub_id.clone(),
                command: exec_input.command.to_vec(),
                cwd: exec_input.cwd.to_path_buf(),
                parsed_cmd: exec_input.parsed_cmd.to_vec(),
                source: exec_input.source,
                interaction_input: exec_input.interaction_input.map(str::to_owned),
                stdout: exec_result.stdout,
                stderr: exec_result.stderr,
                aggregated_output: exec_result.aggregated_output,
                exit_code: exec_result.exit_code,
                duration: exec_result.duration,
                formatted_output: exec_result.formatted_output,
                status: exec_result.status,
            }),
        )
        .await;
}

async fn emit_patch_end(
    ctx: ToolEventCtx<'_>,
    changes: HashMap<PathBuf, FileChange>,
    stdout: String,
    stderr: String,
    success: bool,
    status: PatchApplyStatus,
) {
    ctx.session
        .send_event(
            ctx.turn,
            EventMsg::PatchApplyEnd(PatchApplyEndEvent {
                call_id: ctx.call_id.to_string(),
                turn_id: ctx.turn.sub_id.clone(),
                stdout,
                stderr,
                success,
                changes,
                status,
            }),
        )
        .await;

    if let Some(tracker) = ctx.turn_diff_tracker
        && let Some(turn_diff_event) = build_turn_diff_event(tracker).await
    {
        ctx.session
            .send_event(ctx.turn, EventMsg::TurnDiff(turn_diff_event))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::build_turn_diff_event_with_timeout;
    use crate::tools::context::SharedTurnDiffTracker;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::sync::Mutex;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_turn_diff_event_returns_diff_before_timeout() {
        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));

        let event = build_turn_diff_event_with_timeout(&tracker, Duration::from_secs(1), |_| {
            Ok(Some("diff --git a/foo b/foo\n".to_string()))
        })
        .await;

        assert_eq!(
            event.map(|ev| ev.unified_diff),
            Some("diff --git a/foo b/foo\n".to_string())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_turn_diff_event_times_out_without_blocking_turn_completion() {
        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));
        let started_at = Instant::now();

        let event = build_turn_diff_event_with_timeout(&tracker, Duration::from_millis(50), |_| {
            std::thread::sleep(Duration::from_millis(250));
            Ok(Some("late diff".to_string()))
        })
        .await;

        assert!(event.is_none());
        assert!(
            started_at.elapsed() < Duration::from_millis(200),
            "turn diff generation should time out instead of blocking completion"
        );
    }
}
