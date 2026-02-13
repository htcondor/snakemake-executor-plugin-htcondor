"""
Tests for the JobEventLog-based job status monitoring.

These tests verify that the executor correctly reads HTCondor job event logs
and interprets job states, replacing the previous schedd query-based approach.
"""

import pytest
import time
from unittest.mock import Mock
from htcondor2 import JobEventType
from snakemake_executor_plugin_htcondor import Executor, JobStatus, JobState


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def make_event(event_type, **kwargs):
    """Create a mock HTCondor event with the given type and attributes."""
    event = Mock()
    event.type = event_type
    event.get = Mock(side_effect=lambda k, d=None: kwargs.get(k, d))
    return event


def make_mock_executor(**extra_attrs):
    """
    Create a mock executor with the core methods bound.

    Additional attributes (e.g. _held_timeout) can be passed as kwargs.
    """
    executor = Mock(spec=Executor)
    executor.logger = Mock()
    executor._job_event_logs = {}
    executor._job_current_states = {}
    executor._log_missing_counts = {}
    executor.jobDir = "/tmp/test_htcondor"

    for method_name in (
        "_read_job_events",
        "_get_job_event_log",
        "_cleanup_job_tracking",
        "_try_read_job_log",
        "_needs_fallback",
        "_htcondor_status_to_job_status",
        "_batch_query_schedd",
        "_batch_query_history",
    ):
        method = getattr(Executor, method_name)
        setattr(executor, method_name, method.__get__(executor, Executor))

    # Sensible defaults that individual tests can override
    executor._held_timeout = 14400
    executor._log_missing_threshold = 3
    executor._workflow_start_time = int(time.time()) - 3600

    for k, v in extra_attrs.items():
        setattr(executor, k, v)

    return executor


def _setup_event_log(executor, cluster_id, events):
    """Attach a mock event log returning *events* to *executor*."""
    mock_log = Mock()
    mock_log.events = Mock(return_value=iter(events))
    executor._job_event_logs[cluster_id] = mock_log


# ---------------------------------------------------------------------------
# Event-type → status mapping
# ---------------------------------------------------------------------------
class TestEventToStatusMapping:
    """Verify that each HTCondor event type produces the correct JobStatus."""

    @pytest.fixture
    def executor(self):
        return make_mock_executor()

    # -- simple single-event tests -----------------------------------------

    def test_submit_sets_idle(self, executor):
        _setup_event_log(executor, 1, [make_event(JobEventType.SUBMIT)])
        assert executor._read_job_events(1).status == JobStatus.IDLE

    def test_execute_sets_running(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.RUNNING

    def test_evicted_sets_idle(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_EVICTED),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.IDLE

    def test_suspended_sets_suspended(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_SUSPENDED),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.SUSPENDED

    def test_unsuspended_sets_running(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_SUSPENDED),
                make_event(JobEventType.JOB_UNSUSPENDED),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.RUNNING

    def test_shadow_exception_sets_idle(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.SHADOW_EXCEPTION),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.IDLE

    def test_reconnect_failed_sets_idle(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_RECONNECT_FAILED),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.IDLE

    def test_reconnected_sets_running(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_DISCONNECTED),
                make_event(JobEventType.JOB_RECONNECTED),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.RUNNING

    def test_aborted_sets_removed(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.JOB_ABORTED),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.REMOVED

    def test_file_transfer_sets_transferring_when_not_running(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.FILE_TRANSFER),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.TRANSFERRING

    def test_file_transfer_does_not_override_running(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.FILE_TRANSFER),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.RUNNING

    def test_image_size_does_not_change_status(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.IMAGE_SIZE),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.RUNNING

    # -- terminated events with metadata -----------------------------------

    def test_terminated_normal_exit_zero(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(
                    JobEventType.JOB_TERMINATED, ReturnValue=0, TerminatedNormally=True
                ),
            ],
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.COMPLETED
        assert result.exit_code == 0
        assert result.exit_by_signal is False

    def test_terminated_nonzero_exit(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(
                    JobEventType.JOB_TERMINATED, ReturnValue=42, TerminatedNormally=True
                ),
            ],
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.COMPLETED
        assert result.exit_code == 42

    def test_terminated_by_signal(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(
                    JobEventType.JOB_TERMINATED,
                    ReturnValue=None,
                    TerminatedNormally=False,
                    TerminatedBySignal=9,
                ),
            ],
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.COMPLETED
        assert result.exit_by_signal is True
        assert result.exit_signal == 9

    # -- held / released ---------------------------------------------------

    def test_held_event_records_reason_and_timestamp(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(
                    JobEventType.JOB_HELD, HoldReason="Job exceeded memory limit"
                ),
            ],
        )
        before = time.time()
        result = executor._read_job_events(1)
        after = time.time()

        assert result.status == JobStatus.HELD
        assert result.hold_reason == "Job exceeded memory limit"
        assert before <= result.held_since <= after

    def test_released_clears_hold_state(self, executor):
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.JOB_HELD, HoldReason="test"),
                make_event(JobEventType.JOB_RELEASED),
            ],
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.IDLE
        assert result.hold_reason is None
        assert result.held_since is None


# ---------------------------------------------------------------------------
# State persistence across reads (core "Unknown status" bug fix)
# ---------------------------------------------------------------------------
class TestStatePersistence:
    """State must survive across read cycles when no new events appear."""

    def test_cached_state_returned_on_empty_read(self):
        """Core regression test for the 'Unknown status' bug: JobEventLog
        readers advance their position and return no events on subsequent
        reads, so the cached state must be returned instead of PENDING."""
        executor = make_mock_executor()

        call_count = [0]

        def events_gen(stop_after=0):
            call_count[0] += 1
            if call_count[0] == 1:
                return iter(
                    [
                        make_event(JobEventType.SUBMIT),
                        make_event(JobEventType.EXECUTE),
                    ]
                )
            return iter([])

        mock_log = Mock()
        mock_log.events = Mock(side_effect=events_gen)
        executor._job_event_logs[1] = mock_log

        assert executor._read_job_events(1).status == JobStatus.RUNNING
        assert executor._read_job_events(1).status == JobStatus.RUNNING

    def test_terminal_state_returned_without_rereading_log(self):
        """Terminal states (COMPLETED, REMOVED) short-circuit: no event log
        access is needed."""
        executor = make_mock_executor()
        executor._job_current_states[1] = JobState(
            status=JobStatus.COMPLETED,
            exit_code=0,
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.COMPLETED
        assert result.exit_code == 0

    def test_held_state_is_not_terminal(self):
        """Held jobs must keep reading events — they may be released."""
        executor = make_mock_executor()
        executor._job_current_states[1] = JobState(
            status=JobStatus.HELD,
            hold_reason="test",
            held_since=time.time() - 60,
        )
        _setup_event_log(executor, 1, [make_event(JobEventType.JOB_RELEASED)])

        result = executor._read_job_events(1)
        assert result.status == JobStatus.IDLE
        assert result.hold_reason is None


# ---------------------------------------------------------------------------
# Realistic multi-event lifecycle scenarios
# ---------------------------------------------------------------------------
class TestJobLifecycleScenarios:
    """End-to-end event sequences representing real HTCondor job lifecycles."""

    @pytest.fixture
    def executor(self):
        return make_mock_executor()

    def test_preemption_and_reschedule(self, executor):
        """SUBMIT → EXECUTE → EVICTED → EXECUTE → TERMINATED"""
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_EVICTED),
                make_event(JobEventType.EXECUTE),
                make_event(
                    JobEventType.JOB_TERMINATED, ReturnValue=0, TerminatedNormally=True
                ),
            ],
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.COMPLETED
        assert result.exit_code == 0

    def test_hold_release_and_complete(self, executor):
        """SUBMIT → EXECUTE → HELD → RELEASED → EXECUTE → TERMINATED"""
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.JOB_HELD, HoldReason="Policy violation"),
                make_event(JobEventType.JOB_RELEASED),
                make_event(JobEventType.EXECUTE),
                make_event(
                    JobEventType.JOB_TERMINATED, ReturnValue=0, TerminatedNormally=True
                ),
            ],
        )
        result = executor._read_job_events(1)
        assert result.status == JobStatus.COMPLETED
        assert result.hold_reason is None

    def test_shadow_exception_and_reschedule(self, executor):
        """SUBMIT → EXECUTE → SHADOW_EXCEPTION → EXECUTE → TERMINATED"""
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
                make_event(JobEventType.SHADOW_EXCEPTION),
                make_event(JobEventType.EXECUTE),
                make_event(
                    JobEventType.JOB_TERMINATED, ReturnValue=0, TerminatedNormally=True
                ),
            ],
        )
        assert executor._read_job_events(1).status == JobStatus.COMPLETED


# ---------------------------------------------------------------------------
# Held timeout validation
# ---------------------------------------------------------------------------
class TestHeldTimeoutValidation:
    """Verify that _validate_held_timeout handles edge cases."""

    def test_none_held_timeout_does_not_raise(self):
        """If held_timeout is None (e.g., explicit CLI override),
        __post_init__ should normalize it to the default before validation."""
        from snakemake_executor_plugin_htcondor import (
            DEFAULT_HELD_TIMEOUT_SECONDS,
            Executor,
        )

        executor = Mock(spec=Executor)
        executor.logger = Mock()

        # Simulate what __post_init__ does after our fix
        held_timeout = None  # as if user passed None
        if held_timeout is None:
            held_timeout = DEFAULT_HELD_TIMEOUT_SECONDS
        executor._held_timeout = held_timeout

        # Bind and call _validate_held_timeout — should not raise
        validate = Executor._validate_held_timeout.__get__(executor, Executor)
        validate()  # no TypeError

        assert executor._held_timeout == DEFAULT_HELD_TIMEOUT_SECONDS

    def test_none_would_cause_type_error_without_normalization(self):
        """Confirm that None._held_timeout < 0 raises TypeError,
        proving the normalization is necessary."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._held_timeout = None

        validate = Executor._validate_held_timeout.__get__(executor, Executor)
        with pytest.raises(TypeError):
            validate()


# ---------------------------------------------------------------------------
# Error handling & cleanup
# ---------------------------------------------------------------------------
class TestErrorHandlingAndCleanup:
    """Error resilience in event reading and tracking-data cleanup."""

    def test_read_error_returns_none_preserves_state(self):
        """On exception, _read_job_events returns None (triggers fallback),
        but preserves last known state for later recovery."""
        executor = make_mock_executor()
        executor._job_current_states[1] = JobState(status=JobStatus.RUNNING)

        mock_log = Mock()
        mock_log.events = Mock(side_effect=Exception("Read error"))
        executor._job_event_logs[1] = mock_log

        assert executor._read_job_events(1) is None
        assert executor._job_current_states[1].status == JobStatus.RUNNING
        assert executor.logger.warning.called

    def test_missing_log_returns_none(self):
        executor = make_mock_executor(jobDir="/nonexistent/path")
        assert executor._get_job_event_log(99999) is None

    def test_unavailable_log_returns_none_despite_cached_state(self):
        """_read_job_events must return None when the log can't be read,
        even if _job_current_states has a cached entry.  This ensures
        _try_read_job_log won't mistakenly reset the missing counter."""
        executor = make_mock_executor(jobDir="/nonexistent/path")
        executor._job_current_states[1] = JobState(status=JobStatus.RUNNING)

        result = executor._read_job_events(1)
        assert result is None
        # Cached state must still be preserved for later recovery
        assert executor._job_current_states[1].status == JobStatus.RUNNING

    def test_existing_reader_is_reused(self):
        executor = make_mock_executor()
        sentinel = Mock()
        executor._job_event_logs[1] = sentinel
        assert executor._get_job_event_log(1) is sentinel

    def test_cleanup_removes_all_tracking_data(self):
        executor = make_mock_executor()
        executor._job_event_logs[1] = Mock()
        executor._job_current_states[1] = JobState(status=JobStatus.COMPLETED)
        executor._log_missing_counts[1] = 2

        executor._cleanup_job_tracking(1)

        assert 1 not in executor._job_event_logs
        assert 1 not in executor._job_current_states
        assert 1 not in executor._log_missing_counts


# ---------------------------------------------------------------------------
# _report_and_resolve_job_state
# ---------------------------------------------------------------------------
class TestReportAndResolveJobState:
    """Verify that job states are correctly reported to Snakemake."""

    @pytest.fixture
    def executor(self):
        ex = make_mock_executor()
        ex._report_and_resolve_job_state = (
            Executor._report_and_resolve_job_state.__get__(ex, Executor)
        )
        ex.report_job_success = Mock()
        ex.report_job_error = Mock()
        return ex

    @pytest.fixture
    def job_info(self):
        ji = Mock()
        ji.external_jobid = 1
        ji.job = Mock()
        ji.job.jobid = "test_rule"
        return ji

    def test_success_on_exit_zero(self, executor, job_info):
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(status=JobStatus.COMPLETED, exit_code=0),
        )
        assert result is None
        executor.report_job_success.assert_called_once_with(job_info)
        executor.report_job_error.assert_not_called()

    def test_error_on_nonzero_exit(self, executor, job_info):
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(status=JobStatus.COMPLETED, exit_code=1),
        )
        assert result is None
        executor.report_job_error.assert_called_once()
        assert "ExitCode 1" in executor.report_job_error.call_args.kwargs["msg"]

    def test_error_on_signal_kill(self, executor, job_info):
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(status=JobStatus.COMPLETED, exit_by_signal=True, exit_signal=9),
        )
        assert result is None
        assert "signal 9" in executor.report_job_error.call_args.kwargs["msg"]

    def test_error_on_removed(self, executor, job_info):
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(status=JobStatus.REMOVED),
        )
        assert result is None
        assert "removed" in executor.report_job_error.call_args.kwargs["msg"].lower()

    def test_running_yields_job_no_report(self, executor, job_info):
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(status=JobStatus.RUNNING),
        )
        assert result is job_info
        executor.report_job_success.assert_not_called()
        executor.report_job_error.assert_not_called()

    def test_held_past_timeout_reports_error(self, executor, job_info):
        executor._held_timeout = 3600
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(
                status=JobStatus.HELD,
                hold_reason="Memory limit exceeded",
                held_since=time.time() - 7200,
            ),
        )
        assert result is None
        assert (
            "Memory limit exceeded" in executor.report_job_error.call_args.kwargs["msg"]
        )

    def test_held_within_timeout_yields_job(self, executor, job_info):
        executor._held_timeout = 7200
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(
                status=JobStatus.HELD,
                hold_reason="Memory limit exceeded",
                held_since=time.time() - 3600,
            ),
        )
        assert result is job_info
        executor.report_job_success.assert_not_called()
        executor.report_job_error.assert_not_called()

    def test_held_with_zero_timeout_fails_immediately(self, executor, job_info):
        executor._held_timeout = 0
        result = executor._report_and_resolve_job_state(
            job_info,
            JobState(status=JobStatus.HELD, hold_reason="test", held_since=time.time()),
        )
        assert result is None
        executor.report_job_error.assert_called_once()


# ---------------------------------------------------------------------------
# JobStatus enum
# ---------------------------------------------------------------------------
class TestJobStatusEnum:
    """Verify the JobStatus enum values and methods."""

    def test_all_values_exist(self):
        expected = {
            "pending",
            "idle",
            "running",
            "completed",
            "held",
            "removed",
            "transferring",
            "suspended",
        }
        assert {s.value for s in JobStatus} == expected

    def test_display_names_are_capitalized(self):
        for status in JobStatus:
            assert status.display_name == status.value.capitalize()

    def test_only_completed_and_removed_are_terminal(self):
        terminal = {s for s in JobStatus if s.is_terminal()}
        assert terminal == {JobStatus.COMPLETED, JobStatus.REMOVED}


# ---------------------------------------------------------------------------
# Schedd / history fallback
# ---------------------------------------------------------------------------
class TestFallbackMechanism:
    """Fallback mechanism when log files are unavailable."""

    @pytest.fixture
    def executor(self):
        return make_mock_executor()

    def test_htcondor_status_code_conversion(self, executor):
        expected = {
            1: JobStatus.IDLE,
            2: JobStatus.RUNNING,
            3: JobStatus.REMOVED,
            4: JobStatus.COMPLETED,
            5: JobStatus.HELD,
            6: JobStatus.TRANSFERRING,
            7: JobStatus.SUSPENDED,
            99: JobStatus.PENDING,  # unknown → PENDING
        }
        for code, status in expected.items():
            assert executor._htcondor_status_to_job_status(code) == status

    def test_log_missing_counter_increments(self, executor):
        executor._try_read_job_log(1)
        assert executor._log_missing_counts[1] == 1
        executor._try_read_job_log(1)
        assert executor._log_missing_counts[1] == 2

    def test_needs_fallback_respects_threshold(self, executor):
        executor._log_missing_counts[1] = 2
        assert executor._needs_fallback(1) is False
        executor._log_missing_counts[1] = 3
        assert executor._needs_fallback(1) is True
        executor._log_missing_counts[1] = 5
        assert executor._needs_fallback(1) is True

    def test_log_missing_counter_resets_on_success(self, executor):
        executor._log_missing_counts[1] = 2
        _setup_event_log(
            executor,
            1,
            [
                make_event(JobEventType.SUBMIT),
                make_event(JobEventType.EXECUTE),
            ],
        )
        result = executor._try_read_job_log(1)
        assert result.status == JobStatus.RUNNING
        assert 1 not in executor._log_missing_counts

    def test_log_missing_counter_not_reset_on_cached_state(self, executor):
        """If the log file is unavailable but cached state exists,
        _try_read_job_log must NOT reset the missing counter.
        This prevents fallback from being permanently suppressed."""
        executor.jobDir = "/nonexistent/path"
        executor._job_current_states[1] = JobState(status=JobStatus.RUNNING)
        executor._log_missing_counts[1] = 2

        result = executor._try_read_job_log(1)
        assert result is None
        # Counter should have incremented, not been reset
        assert executor._log_missing_counts[1] == 3

    def test_batch_queries_return_empty_for_empty_input(self, executor):
        assert executor._batch_query_schedd([]) == {}
        assert executor._batch_query_history([]) == {}
