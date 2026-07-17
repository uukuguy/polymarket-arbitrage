from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from scripts.planning_status import _git_log_for


def test_uncommitted_plan_does_not_inherit_historical_matching_scopes() -> None:
    with patch("scripts.planning_status.subprocess.check_output", return_value="") as call:
        commits = _git_log_for("03", "01", Path("new/03-01-PLAN.md"))

    assert commits == []
    assert call.call_count >= 1
    assert "--diff-filter=A" in call.call_args_list[0].args[0]


def test_scoped_commits_are_searched_only_after_plan_creation() -> None:
    with patch(
        "scripts.planning_status.subprocess.check_output",
        side_effect=[
            "plan_creation_sha\n",
            "",
            "abc1234 feat(03-01): persist position state\n",
        ],
    ) as call:
        commits = _git_log_for("03", "01", Path("m2/03-01-PLAN.md"))

    assert commits == [("abc1234", "feat(03-01): persist position state")]
    commit_log_command = call.call_args_list[2].args[0]
    assert "plan_creation_sha..HEAD" in commit_log_command
    assert "--all" not in commit_log_command


def test_closed_plan_scope_stops_at_summary_creation() -> None:
    with patch(
        "scripts.planning_status.subprocess.check_output",
        side_effect=[
            "plan_creation_sha\n",
            "summary_creation_sha\n",
            "abc1234 feat(03-01): original workstream work\n",
        ],
    ) as call:
        commits = _git_log_for("03", "01", Path("m1/03-01-PLAN.md"))

    assert commits == [("abc1234", "feat(03-01): original workstream work")]
    commit_log_command = call.call_args_list[2].args[0]
    assert "plan_creation_sha..summary_creation_sha" in commit_log_command
    assert "HEAD" not in commit_log_command
