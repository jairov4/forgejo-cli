//! Shared helpers for viewing and waiting on CI (commit status) results.

use std::io::Write;
use std::time::{Duration, Instant};

use clap::Args;
use forgejo_api::{
    Forgejo,
    structs::{CommitStatus, CommitStatusState},
};
use futures::stream::TryStreamExt;

use crate::{SpecialRender, ftl_println, h, lh, repo::RepoName};

/// Exit code when `--wait` finishes with a failing check.
pub const EXIT_CHECKS_FAILED: i32 = 3;
/// Exit code when `--wait` gives up because `--timeout` elapsed.
pub const EXIT_TIMEOUT: i32 = 4;

/// Number of consecutive failed API requests tolerated while waiting.
const MAX_CONSECUTIVE_ERRORS: u32 = 10;

#[derive(Args, Clone, Debug)]
pub struct WaitArgs {
    #[clap(help = h!("arg-ci-wait"), long_help = lh!("arg-ci-wait"))]
    #[clap(long)]
    pub wait: bool,
    #[clap(help = h!("arg-ci-timeout"))]
    #[clap(long, requires = "wait", value_parser = parse_duration, value_name = "DURATION")]
    pub timeout: Option<Duration>,
    #[clap(help = h!("arg-ci-interval"))]
    #[clap(long, requires = "wait", value_parser = parse_duration, value_name = "DURATION", default_value = "30s")]
    pub interval: Duration,
    #[clap(help = h!("arg-ci-grace"))]
    #[clap(long, requires = "wait", value_parser = parse_duration, value_name = "DURATION", default_value = "10s")]
    pub grace: Duration,
}

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration `{s}`, expected e.g. `90`, `30s`, `50m` or `2h`"))?;
    let secs = match unit {
        "" | "s" => num,
        "m" => num * 60,
        "h" => num * 60 * 60,
        _ => {
            return Err(format!(
                "invalid duration unit `{unit}`, expected `s`, `m` or `h`"
            ));
        }
    };
    Ok(Duration::from_secs(secs))
}

/// Something whose CI checks can be waited on.
pub trait CiSnapshot {
    /// The commit (or other ref) the checks belong to, and its checks. `None`
    /// means there is nothing left to wait for (e.g. the PR was merged).
    fn checks(&self) -> Option<(&str, &[CommitStatus])>;
    fn print(&self) -> eyre::Result<()>;
}

/// Fetches the latest status of each check on `reference` (a sha, branch or tag).
pub async fn fetch_commit_statuses(
    repo: &RepoName,
    api: &Forgejo,
    reference: &str,
) -> eyre::Result<Vec<CommitStatus>> {
    let mut commit_statuses = api
        .repo_get_combined_status_by_ref(repo.owner(), repo.name(), reference)
        .stream_pages()
        .map_ok(|page| {
            futures::stream::iter(
                page.statuses
                    .unwrap_or_default()
                    .into_iter()
                    .map(Result::<_, forgejo_api::ForgejoError>::Ok),
            )
        })
        .try_flatten()
        .try_collect::<Vec<_>>()
        .await
        .or_else(|e| {
            if is_empty_combined_status(&e) {
                Ok(Vec::new())
            } else {
                Err(e)
            }
        })?;
    commit_statuses.sort_by(|a, b| a.context.cmp(&b.context));
    Ok(commit_statuses)
}

/// Forgejo reports `"state": ""` for a commit without any statuses, which
/// `forgejo_api` can't deserialize.
fn is_empty_combined_status(e: &forgejo_api::ForgejoError) -> bool {
    let forgejo_api::ForgejoError::BadStructure(forgejo_api::StructureError::Serde {
        contents,
        ..
    }) = e
    else {
        return false;
    };
    serde_json::from_slice::<serde_json::Value>(contents)
        .is_ok_and(|v| v.get("total_count").and_then(|c| c.as_u64()) == Some(0))
}

pub fn print_commit_statuses(commit_statuses: &[CommitStatus]) -> eyre::Result<()> {
    let SpecialRender { bullet, .. } = *crate::special_render();
    if commit_statuses.is_empty() {
        ftl_println!("msg-ci-no_checks");
    }
    for status in commit_statuses {
        let state = status
            .status
            .as_ref()
            .ok_or_else(|| eyre::eyre!("status does not have status"))?;
        let context = status
            .context
            .as_deref()
            .ok_or_else(|| eyre::eyre!("status does not have context"))?;
        print!("{bullet} ");
        let state = commit_state_name(state);
        ftl_println!("msg-pr-status-entry", state, context);
    }
    Ok(())
}

pub fn commit_state_name(state: &CommitStatusState) -> &'static str {
    match state {
        CommitStatusState::Success => "success",
        CommitStatusState::Pending => "pending",
        CommitStatusState::Warning => "warning",
        CommitStatusState::Failure => "failure",
        CommitStatusState::Skipped => "skipped",
        CommitStatusState::Error => "error",
    }
}

enum WaitOutcome {
    Done,
    Green,
    Red,
    NoChecks,
}

/// Classifies a snapshot, or returns `None` if checks are still running.
fn wait_outcome(snapshot: &impl CiSnapshot) -> Option<WaitOutcome> {
    let Some((_, commit_statuses)) = snapshot.checks() else {
        return Some(WaitOutcome::Done);
    };
    let states = commit_statuses.iter().flat_map(|x| x.status.as_ref());
    if commit_statuses.is_empty() {
        Some(WaitOutcome::NoChecks)
    } else if states.clone().any(|s| *s == CommitStatusState::Pending) {
        None
    } else if states
        .clone()
        .any(|s| matches!(s, CommitStatusState::Failure | CommitStatusState::Error))
    {
        Some(WaitOutcome::Red)
    } else {
        Some(WaitOutcome::Green)
    }
}

fn summary(snapshot: &impl CiSnapshot) -> String {
    let Some((reference, commit_statuses)) = snapshot.checks() else {
        return "done".into();
    };
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for state in commit_statuses.iter().flat_map(|x| x.status.as_ref()) {
        *counts.entry(commit_state_name(state)).or_default() += 1;
    }
    let counts = counts
        .iter()
        .map(|(k, v)| format!("{v} {k}"))
        .collect::<Vec<_>>()
        .join(", ");
    // Abbreviate full commit hashes.
    let reference = if reference.len() == 40 && reference.bytes().all(|b| b.is_ascii_hexdigit()) {
        &reference[..10]
    } else {
        reference
    };
    if counts.is_empty() {
        format!("{reference}: no checks")
    } else {
        format!("{reference}: {counts}")
    }
}

/// Whether a failed request is worth retrying while waiting (network trouble or
/// server-side errors), as opposed to e.g. a missing PR or bad credentials.
fn is_transient_error(e: &eyre::Report) -> bool {
    use forgejo_api::{ApiErrorKind, ForgejoError};
    // Codeberg answers 410 when rate limiting.
    let retryable = |code: u16| code >= 500 || code == 429 || code == 410;
    e.chain()
        .find_map(|e| e.downcast_ref::<ForgejoError>())
        .is_some_and(|e| match e {
            ForgejoError::ReqwestError(_) => true,
            ForgejoError::UnexpectedStatusCode(code) => retryable(code.as_u16()),
            ForgejoError::ApiError(e) => match e.error_kind() {
                ApiErrorKind::Generic => true,
                ApiErrorKind::Other(code) => retryable(code.as_u16()),
                _ => false,
            },
            _ => false,
        })
}

/// Polls `fetch` until every check has finished, then exits with a status
/// code describing the result (see `EXIT_*`).
pub async fn wait_for_checks<S, F, Fut>(mut fetch: F, args: &WaitArgs) -> eyre::Result<()>
where
    S: CiSnapshot,
    F: FnMut() -> Fut,
    Fut: Future<Output = eyre::Result<S>>,
{
    let SpecialRender { fancy, .. } = *crate::special_render();
    let start = Instant::now();
    let deadline = args.timeout.map(|t| start + t);
    let mut interval = Duration::from_secs(5).min(args.interval);
    let mut prev_lines = 0;
    let mut prev_summary = String::new();
    let mut reference: Option<String> = None;
    let mut reference_seen_at = start;
    let mut errors = 0;
    let mut last: Option<S> = None;

    let outcome = loop {
        match fetch().await {
            Ok(snapshot) => {
                errors = 0;
                let new_reference = snapshot.checks().map(|(r, _)| r.to_owned());
                if new_reference != reference {
                    reference = new_reference;
                    reference_seen_at = Instant::now();
                }
                if fancy {
                    if prev_lines > 0 {
                        print!("\x1b[{prev_lines}A\x1b[J");
                    }
                    snapshot.print()?;
                    prev_lines = snapshot.checks().map_or(1, |(_, c)| c.len() + 1);
                } else {
                    let summary = summary(&snapshot);
                    if summary != prev_summary {
                        let elapsed = start.elapsed().as_secs();
                        eprintln!("[{:02}:{:02}] {summary}", elapsed / 60, elapsed % 60);
                        prev_summary = summary;
                    }
                }
                let outcome = wait_outcome(&snapshot);
                last = Some(snapshot);
                match outcome {
                    // Checks may not be registered yet right after a push.
                    Some(WaitOutcome::NoChecks) if reference_seen_at.elapsed() < args.grace => (),
                    Some(outcome) => break Some(outcome),
                    None => (),
                }
            }
            Err(e) if !is_transient_error(&e) => return Err(e),
            Err(e) => {
                errors += 1;
                if errors >= MAX_CONSECUTIVE_ERRORS {
                    return Err(e.wrap_err(format!("{errors} consecutive request failures")));
                }
                eprintln!(
                    "warning: failed to fetch status ({errors}/{MAX_CONSECUTIVE_ERRORS}): {e}"
                );
            }
        }

        let mut sleep = interval;
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break None;
            }
            sleep = sleep.min(remaining);
        }
        // Don't sleep past the end of the grace period when no checks exist yet.
        if last
            .as_ref()
            .and_then(|s| s.checks())
            .is_some_and(|(_, c)| c.is_empty())
        {
            let grace_left = args.grace.saturating_sub(reference_seen_at.elapsed());
            if !grace_left.is_zero() {
                sleep = sleep.min(grace_left);
            }
        }
        tokio::time::sleep(sleep).await;
        interval = interval.mul_f64(1.5).min(args.interval);
    };

    if !fancy && let Some(snapshot) = &last {
        snapshot.print()?;
    }
    std::io::stdout().flush()?;

    match outcome {
        Some(WaitOutcome::Done | WaitOutcome::Green) => Ok(()),
        Some(WaitOutcome::NoChecks) => {
            eprintln!("no checks reported within the grace period");
            Ok(())
        }
        Some(WaitOutcome::Red) => Err(crate::ExitCode(EXIT_CHECKS_FAILED).into()),
        None => {
            eprintln!("timed out waiting for checks");
            Err(crate::ExitCode(EXIT_TIMEOUT).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_duration;
    use std::time::Duration;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_duration("50m"), Ok(Duration::from_secs(3000)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert!(parse_duration("5x").is_err());
        assert!(parse_duration("m").is_err());
    }
}
