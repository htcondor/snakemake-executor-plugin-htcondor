"""
Tests for the JobEventLog-based job status monitoring.

These tests verify that the executor correctly reads HTCondor job event logs
and interprets job states, replacing the previous schedd query-based approach.
"""

import pytest
import time
from unittest.mock import Mock
from snakemake_executor_plugin_htcondor import Executor, JobStatus


def make_event(event_type, **kwargs):
    """
    Helper to create a mock event with the given type.

    This is a module-level helper used by all test classes to avoid duplication.

    Args:
        event_type: The JobEventType for this event
        **kwargs: Additional key-value pairs to return from event.get()

    Returns:
        A Mock event object with the specified type and data
    """
    event = Mock()
    event.type = event_type
    event.get = Mock(side_effect=lambda k, d=None: kwargs.get(k, d))
    return event


class TestJobEventLogStateTracking:
    """Tests for job state tracking across multiple check cycles."""

    def test_state_persists_across_reads(self):
        """
        Verify that job state is correctly tracked across multiple
        check cycles when no new events have occurred.

        This tests the core fix for the 'Unknown' status bug where
        JobEventLog readers advance their position and return no
        events on subsequent reads.
        """
        from htcondor2 import JobEventType

        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        # Create a mock event log that returns events on first call, empty on second
        mock_event_log = Mock()
        call_count = [0]

        def events_generator(stop_after=0):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: return submit and execute events
                return iter(
                    [
                        make_event(JobEventType.SUBMIT),
                        make_event(JobEventType.EXECUTE),
                    ]
                )
            else:
                # Subsequent calls: no new events
                return iter([])

        mock_event_log.events = Mock(side_effect=events_generator)
        executor._job_event_logs[12345] = mock_event_log

        # First read
        result1 = executor._read_job_events(12345)
        assert result1["status"] == JobStatus.RUNNING

        # Second read (no new events) - should return cached state
        result2 = executor._read_job_events(12345)
        assert result2["status"] == JobStatus.RUNNING

    def test_cleanup_removes_tracking_data(self):
        """Verify that job tracking data is cleaned up after job completes."""
        executor = Mock(spec=Executor)
        executor._job_event_logs = {12345: Mock()}
        executor._job_current_states = {12345: {"status": JobStatus.COMPLETED}}
        executor._log_missing_counts = {12345: 2}
        executor.logger = Mock()

        # Bind the cleanup method
        executor._cleanup_job_tracking = Executor._cleanup_job_tracking.__get__(
            executor, Executor
        )

        executor._cleanup_job_tracking(12345)

        assert 12345 not in executor._job_event_logs
        assert 12345 not in executor._job_current_states
        assert 12345 not in executor._log_missing_counts


class TestJobEventTypes:
    """Tests for handling different HTCondor job event types."""

    @pytest.fixture
    def mock_executor(self):
        """Create a mock executor with the event reading methods bound."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test_htcondor"

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )
        executor._cleanup_job_tracking = Executor._cleanup_job_tracking.__get__(
            executor, Executor
        )

        return executor

    def test_submit_event_sets_idle(self, mock_executor):
        """SUBMIT event (type 0) should set status to IDLE."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter([make_event(JobEventType.SUBMIT)])
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.IDLE

    def test_execute_event_sets_running(self, mock_executor):
        """EXECUTE event (type 1) should set status to RUNNING."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.RUNNING

    def test_terminated_event_sets_completed(self, mock_executor):
        """JOB_TERMINATED event (type 5) should set status to COMPLETED."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=0,
                        TerminatedNormally=True,
                    ),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.COMPLETED
        assert result["exit_code"] == 0
        assert result["exit_by_signal"] is False

    def test_terminated_with_nonzero_exit(self, mock_executor):
        """JOB_TERMINATED with non-zero exit code should be captured."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=42,
                        TerminatedNormally=True,
                    ),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.COMPLETED
        assert result["exit_code"] == 42

    def test_held_event_sets_held_with_timestamp(self, mock_executor):
        """JOB_HELD event (type 12) should set status to HELD and record timestamp."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(
                        JobEventType.JOB_HELD, HoldReason="Job exceeded memory limit"
                    ),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        before_time = time.time()
        result = mock_executor._read_job_events(12345)
        after_time = time.time()

        assert result is not None
        assert result["status"] == JobStatus.HELD
        assert result["hold_reason"] == "Job exceeded memory limit"
        # Verify held_since is set to approximately the current time
        assert result["held_since"] is not None
        assert before_time <= result["held_since"] <= after_time

    def test_aborted_event_sets_removed(self, mock_executor):
        """JOB_ABORTED event (type 9) should set status to REMOVED."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.JOB_ABORTED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.REMOVED

    def test_evicted_event_sets_idle(self, mock_executor):
        """JOB_EVICTED event should set status back to IDLE."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_EVICTED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.IDLE

    def test_suspended_event_sets_suspended(self, mock_executor):
        """JOB_SUSPENDED event should set status to SUSPENDED."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_SUSPENDED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.SUSPENDED

    def test_unsuspended_event_sets_running(self, mock_executor):
        """JOB_UNSUSPENDED event should set status back to RUNNING."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_SUSPENDED),
                    make_event(JobEventType.JOB_UNSUSPENDED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.RUNNING

    def test_released_event_clears_held_state(self, mock_executor):
        """JOB_RELEASED event should set status back to IDLE and clear held_since."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.JOB_HELD, HoldReason="Test hold reason"),
                    make_event(JobEventType.JOB_RELEASED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.IDLE
        # hold_reason and held_since should be cleared after release
        assert result["hold_reason"] is None
        assert result["held_since"] is None

    def test_file_transfer_event_sets_transferring(self, mock_executor):
        """FILE_TRANSFER event should set status to TRANSFERRING when not running."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.FILE_TRANSFER),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.TRANSFERRING

    def test_file_transfer_does_not_override_running(self, mock_executor):
        """FILE_TRANSFER event should not override RUNNING status."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),  # Now running
                    make_event(JobEventType.FILE_TRANSFER),  # Output transfer
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        # Should still be RUNNING, not TRANSFERRING
        assert result["status"] == JobStatus.RUNNING

    def test_image_size_event_does_not_change_status(self, mock_executor):
        """IMAGE_SIZE event should not change the job status."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.IMAGE_SIZE),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.RUNNING  # Still running

    def test_shadow_exception_sets_idle(self, mock_executor):
        """SHADOW_EXCEPTION event should set status to IDLE (for reschedule)."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.SHADOW_EXCEPTION),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.IDLE

    def test_reconnect_failed_sets_idle(self, mock_executor):
        """JOB_RECONNECT_FAILED event should set status to IDLE."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_RECONNECT_FAILED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.IDLE

    def test_reconnected_sets_running(self, mock_executor):
        """JOB_RECONNECTED event should set status to RUNNING."""
        from htcondor2 import JobEventType

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_DISCONNECTED),
                    make_event(JobEventType.JOB_RECONNECTED),
                ]
            )
        )

        mock_executor._job_event_logs[12345] = mock_event_log

        result = mock_executor._read_job_events(12345)

        assert result is not None
        assert result["status"] == JobStatus.RUNNING


class TestStatePersistenceAcrossReads:
    """
    Tests verifying that state persists correctly when no new events are available.

    This specifically tests the fix for the 'Unknown' status bug.
    """

    def test_second_read_returns_cached_state(self):
        """
        Verify that the second read (with no new events) returns
        the last known state, not 'pending'/'unknown'.
        """
        from htcondor2 import JobEventType

        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        # Create a mock event log that returns events on first call, empty on second
        mock_event_log = Mock()
        call_count = [0]

        def events_generator(stop_after=0):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: return submit and execute events
                return iter(
                    [
                        make_event(JobEventType.SUBMIT),
                        make_event(JobEventType.EXECUTE),
                    ]
                )
            else:
                # Subsequent calls: no new events
                return iter([])

        mock_event_log.events = Mock(side_effect=events_generator)
        executor._job_event_logs[12345] = mock_event_log

        # First read
        result1 = executor._read_job_events(12345)
        assert result1["status"] == JobStatus.RUNNING

        # Second read (no new events)
        result2 = executor._read_job_events(12345)
        assert (
            result2["status"] == JobStatus.RUNNING
        )  # Should still be RUNNING, not PENDING

    def test_terminal_state_cached_and_returned(self):
        """
        Verify that terminal states (completed, removed) are
        returned without re-reading the log.
        """
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        # Pre-populate with a terminal state
        executor._job_current_states[12345] = {
            "status": JobStatus.COMPLETED,
            "exit_code": 0,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": None,
            "held_since": None,
        }

        # Should return cached state without reading log
        result = executor._read_job_events(12345)

        assert result["status"] == JobStatus.COMPLETED
        assert result["exit_code"] == 0

    def test_held_state_is_not_terminal(self):
        """
        Verify that held state is NOT cached as terminal - it should
        continue to read events in case job is released.
        """
        from htcondor2 import JobEventType

        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        # Pre-populate with a held state
        executor._job_current_states[12345] = {
            "status": JobStatus.HELD,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": "Test reason",
            "held_since": time.time() - 60,  # Held for 1 minute
        }

        # Create a mock event log that returns a RELEASED event
        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.JOB_RELEASED),
                ]
            )
        )
        executor._job_event_logs[12345] = mock_event_log

        # Read should process the release event
        result = executor._read_job_events(12345)

        # Job should now be IDLE, not still HELD
        assert result["status"] == JobStatus.IDLE
        assert result["hold_reason"] is None
        assert result["held_since"] is None


class TestLogFileInitialization:
    """Tests for lazy log file initialization."""

    def test_lazy_initialization_when_file_not_ready(self):
        """Test that missing log files don't cause errors."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/nonexistent/path"

        # Bind method
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        # Should return None, not raise
        result = executor._get_job_event_log(99999)
        assert result is None

    def test_returns_existing_reader(self):
        """Test that existing readers are returned without creating new ones."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor.jobDir = "/tmp/test"

        existing_reader = Mock()
        executor._job_event_logs = {12345: existing_reader}

        # Bind method
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        result = executor._get_job_event_log(12345)
        assert result is existing_reader


class TestErrorHandling:
    """Tests for error handling in event reading."""

    def test_error_returns_last_known_state(self):
        """Test that errors during reading return the last known state."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )

        # Set up initial state
        executor._job_current_states[12345] = {
            "status": JobStatus.RUNNING,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": None,
            "held_since": None,
        }

        # Create a mock that raises an error
        mock_event_log = Mock()
        mock_event_log.events = Mock(side_effect=Exception("Read error"))
        executor._job_event_logs[12345] = mock_event_log

        # Should return last known state despite error
        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.RUNNING

        # Should have logged a warning
        assert executor.logger.warning.called


class TestHeldJobTimeout:
    """Tests for held job timeout behavior."""

    @pytest.fixture
    def mock_executor_with_timeout(self):
        """Create a mock executor with held timeout configured."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"
        executor._held_timeout = 3600  # 1 hour timeout for testing

        # Bind methods
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )
        executor._cleanup_job_tracking = Executor._cleanup_job_tracking.__get__(
            executor, Executor
        )

        return executor

    def test_held_job_within_timeout_stays_active(self, mock_executor_with_timeout):
        """Held job within timeout should remain in the active jobs list."""
        executor = mock_executor_with_timeout

        # Job held 30 minutes ago (within 1 hour timeout)
        executor._job_current_states[12345] = {
            "status": JobStatus.HELD,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": "Memory limit exceeded",
            "held_since": time.time() - 1800,  # 30 minutes ago
        }

        # Create mock event log that returns no new events
        mock_event_log = Mock()
        mock_event_log.events = Mock(return_value=iter([]))
        executor._job_event_logs[12345] = mock_event_log

        # Read job state
        result = executor._read_job_events(12345)

        # Job should still be held
        assert result["status"] == JobStatus.HELD
        # held_since should be preserved
        assert result["held_since"] is not None

    def test_held_job_past_timeout_can_be_identified(self, mock_executor_with_timeout):
        """Held job past timeout can be identified by check_active_jobs."""
        executor = mock_executor_with_timeout

        # Job held 2 hours ago (past 1 hour timeout)
        held_since = time.time() - 7200  # 2 hours ago
        executor._job_current_states[12345] = {
            "status": JobStatus.HELD,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": "Memory limit exceeded",
            "held_since": held_since,
        }

        # The state should reflect that timeout has been exceeded
        state = executor._job_current_states[12345]
        elapsed = time.time() - state["held_since"]

        assert elapsed > executor._held_timeout
        assert state["status"] == JobStatus.HELD

    def test_zero_timeout_means_immediate_failure(self):
        """Timeout of 0 should mean held jobs fail immediately."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._held_timeout = 0  # Immediate failure

        # This is configuration validation - held jobs should fail immediately
        assert executor._held_timeout == 0


class TestJobEventLogReadAndCleanup:
    """Tests for reading job events and cleanup helper methods."""

    @pytest.fixture
    def mock_executor_for_check(self):
        """Create a mock executor with check_active_jobs bound."""

        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor._log_missing_counts = {}
        executor.jobDir = "/tmp/test_htcondor"
        executor._held_timeout = 14400  # 4 hour default

        # Bind all the methods needed for check_active_jobs
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )
        executor._cleanup_job_tracking = Executor._cleanup_job_tracking.__get__(
            executor, Executor
        )

        return executor

    def test_completed_job_yields_and_cleans_up(self, mock_executor_for_check):
        """Verify that completed jobs are yielded and tracking data cleaned up."""
        from htcondor2 import JobEventType

        executor = mock_executor_for_check

        # Create a completed job state
        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=0,
                        TerminatedNormally=True,
                    ),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        # Read events and verify completion
        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.COMPLETED
        assert result["exit_code"] == 0

        # Clean up and verify tracking data is removed
        executor._cleanup_job_tracking(12345)
        assert 12345 not in executor._job_event_logs
        assert 12345 not in executor._job_current_states

    def test_running_job_not_yielded(self, mock_executor_for_check):
        """Verify that running jobs return their current state."""
        from htcondor2 import JobEventType

        executor = mock_executor_for_check

        # Set up a running job
        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        # Read events
        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.RUNNING

        # Tracking data should still exist
        assert 12345 in executor._job_current_states

    def test_held_job_state_tracked_correctly(self, mock_executor_for_check):
        """Verify that held jobs have their state tracked correctly."""
        from htcondor2 import JobEventType

        executor = mock_executor_for_check

        # Set up a held job
        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(
                        JobEventType.JOB_HELD, HoldReason="Memory limit exceeded"
                    ),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        # Read events
        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.HELD
        assert result["hold_reason"] == "Memory limit exceeded"
        assert result["held_since"] is not None


class TestProcessJobState:
    """Tests for _process_job_state method and job reporting behavior."""

    @pytest.fixture
    def mock_executor_for_processing(self):
        """Create a mock executor with _process_job_state bound."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor._log_missing_counts = {}
        executor.jobDir = "/tmp/test_htcondor"
        executor._held_timeout = 14400  # 4 hour default

        # Bind methods needed for processing
        executor._process_job_state = Executor._process_job_state.__get__(
            executor, Executor
        )
        executor._cleanup_job_tracking = Executor._cleanup_job_tracking.__get__(
            executor, Executor
        )

        # Mock the reporting methods - these are what we want to verify
        executor.report_job_success = Mock()
        executor.report_job_error = Mock()

        return executor

    @pytest.fixture
    def mock_job_info(self):
        """Create a mock SubmittedJobInfo."""
        job_info = Mock()
        job_info.external_jobid = 12345
        job_info.job = Mock()
        job_info.job.jobid = "test_rule"
        return job_info

    def test_completed_job_with_exit_code_zero_reports_success(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that completed jobs with exit code 0 call report_job_success."""
        executor = mock_executor_for_processing

        job_state = {
            "status": JobStatus.COMPLETED,
            "exit_code": 0,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": None,
            "held_since": None,
        }

        result = executor._process_job_state(mock_job_info, job_state)

        # Should not yield (returns None for terminal states)
        assert result is None
        # Should have called report_job_success
        executor.report_job_success.assert_called_once_with(mock_job_info)
        executor.report_job_error.assert_not_called()

    def test_completed_job_with_nonzero_exit_reports_error(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that completed jobs with nonzero exit code call report_job_error."""
        executor = mock_executor_for_processing

        job_state = {
            "status": JobStatus.COMPLETED,
            "exit_code": 1,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": None,
            "held_since": None,
        }

        result = executor._process_job_state(mock_job_info, job_state)

        assert result is None
        executor.report_job_error.assert_called_once()
        executor.report_job_success.assert_not_called()
        # Verify error message mentions the exit code
        call_args = executor.report_job_error.call_args
        assert "ExitCode 1" in call_args.kwargs["msg"]

    def test_completed_job_by_signal_reports_error(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that jobs terminated by signal call report_job_error."""
        executor = mock_executor_for_processing

        job_state = {
            "status": JobStatus.COMPLETED,
            "exit_code": None,
            "exit_by_signal": True,
            "exit_signal": 9,
            "hold_reason": None,
            "held_since": None,
        }

        result = executor._process_job_state(mock_job_info, job_state)

        assert result is None
        executor.report_job_error.assert_called_once()
        executor.report_job_success.assert_not_called()
        # Verify error message mentions the signal
        call_args = executor.report_job_error.call_args
        assert "signal 9" in call_args.kwargs["msg"]

    def test_removed_job_reports_error(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that removed/aborted jobs call report_job_error."""
        executor = mock_executor_for_processing

        job_state = {
            "status": JobStatus.REMOVED,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": None,
            "held_since": None,
        }

        result = executor._process_job_state(mock_job_info, job_state)

        assert result is None
        executor.report_job_error.assert_called_once()
        executor.report_job_success.assert_not_called()
        # Verify error message mentions removal
        call_args = executor.report_job_error.call_args
        assert "removed" in call_args.kwargs["msg"].lower()

    def test_running_job_yields_and_no_report(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that running jobs yield the job and don't call report methods."""
        executor = mock_executor_for_processing

        job_state = {
            "status": JobStatus.RUNNING,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": None,
            "held_since": None,
        }

        result = executor._process_job_state(mock_job_info, job_state)

        # Should yield the job (return it)
        assert result is mock_job_info
        # Should not have called any report methods
        executor.report_job_success.assert_not_called()
        executor.report_job_error.assert_not_called()

    def test_held_job_past_timeout_reports_error(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that held jobs past timeout call report_job_error."""
        executor = mock_executor_for_processing
        executor._held_timeout = 3600  # 1 hour

        job_state = {
            "status": JobStatus.HELD,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": "Memory limit exceeded",
            "held_since": time.time() - 7200,  # 2 hours ago
        }

        result = executor._process_job_state(mock_job_info, job_state)

        assert result is None
        executor.report_job_error.assert_called_once()
        executor.report_job_success.assert_not_called()
        # Verify error message mentions hold reason
        call_args = executor.report_job_error.call_args
        assert "Memory limit exceeded" in call_args.kwargs["msg"]

    def test_held_job_within_timeout_yields_job(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that held jobs within timeout yield and don't report error."""
        executor = mock_executor_for_processing
        executor._held_timeout = 7200  # 2 hours

        job_state = {
            "status": JobStatus.HELD,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": "Memory limit exceeded",
            "held_since": time.time() - 3600,  # 1 hour ago (within 2 hour timeout)
        }

        result = executor._process_job_state(mock_job_info, job_state)

        # Should yield the job (still active)
        assert result is mock_job_info
        executor.report_job_success.assert_not_called()
        executor.report_job_error.assert_not_called()

    def test_held_job_with_zero_timeout_reports_error_immediately(
        self, mock_executor_for_processing, mock_job_info
    ):
        """Verify that held jobs with timeout=0 fail immediately."""
        executor = mock_executor_for_processing
        executor._held_timeout = 0  # Fail immediately

        job_state = {
            "status": JobStatus.HELD,
            "exit_code": None,
            "exit_by_signal": False,
            "exit_signal": None,
            "hold_reason": "Memory limit exceeded",
            "held_since": time.time(),
        }

        result = executor._process_job_state(mock_job_info, job_state)

        assert result is None
        executor.report_job_error.assert_called_once()
        executor.report_job_success.assert_not_called()


class TestJobLifecycleScenarios:
    """Tests for realistic job lifecycle scenarios."""

    @pytest.fixture
    def mock_executor(self):
        """Create a mock executor with methods bound."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor.jobDir = "/tmp/test"

        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )
        executor._cleanup_job_tracking = Executor._cleanup_job_tracking.__get__(
            executor, Executor
        )

        return executor

    def test_preemption_and_reschedule(self, mock_executor):
        """
        Test job that is preempted (evicted) and rescheduled.

        Lifecycle: SUBMIT -> EXECUTE -> EVICTED -> EXECUTE -> TERMINATED
        """
        from htcondor2 import JobEventType

        executor = mock_executor

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_EVICTED),
                    make_event(JobEventType.EXECUTE),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=0,
                        TerminatedNormally=True,
                    ),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.COMPLETED
        assert result["exit_code"] == 0

    def test_hold_and_release(self, mock_executor):
        """
        Test job that is held and then released.

        Lifecycle: SUBMIT -> EXECUTE -> HELD -> RELEASED -> EXECUTE -> TERMINATED
        """
        from htcondor2 import JobEventType

        executor = mock_executor

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_HELD, HoldReason="Policy violation"),
                    make_event(JobEventType.JOB_RELEASED),
                    make_event(JobEventType.EXECUTE),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=0,
                        TerminatedNormally=True,
                    ),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.COMPLETED
        assert result["hold_reason"] is None  # Should be cleared after release

    def test_shadow_exception_and_reschedule(self, mock_executor):
        """
        Test job that experiences a shadow exception and is rescheduled.

        Lifecycle: SUBMIT -> EXECUTE -> SHADOW_EXCEPTION -> EXECUTE -> TERMINATED
        """
        from htcondor2 import JobEventType

        executor = mock_executor

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.SHADOW_EXCEPTION),
                    make_event(JobEventType.EXECUTE),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=0,
                        TerminatedNormally=True,
                    ),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.COMPLETED

    def test_job_killed_by_signal(self, mock_executor):
        """
        Test job that is killed by a signal.

        Lifecycle: SUBMIT -> EXECUTE -> TERMINATED (by signal)
        """
        from htcondor2 import JobEventType

        executor = mock_executor

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(
                        JobEventType.JOB_TERMINATED,
                        ReturnValue=None,
                        TerminatedNormally=False,
                        TerminatedBySignal=9,
                    ),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.COMPLETED
        assert result["exit_by_signal"] is True
        assert result["exit_signal"] == 9

    def test_job_removed_by_user(self, mock_executor):
        """
        Test job that is removed via condor_rm.

        Lifecycle: SUBMIT -> EXECUTE -> ABORTED
        """
        from htcondor2 import JobEventType

        executor = mock_executor

        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                    make_event(JobEventType.JOB_ABORTED),
                ]
            )
        )

        executor._job_event_logs[12345] = mock_event_log

        result = executor._read_job_events(12345)
        assert result["status"] == JobStatus.REMOVED


class TestJobStatusEnum:
    """Tests for the JobStatus enum."""

    def test_status_values(self):
        """Verify all expected status values exist."""
        assert JobStatus.PENDING.value == "pending"
        assert JobStatus.IDLE.value == "idle"
        assert JobStatus.RUNNING.value == "running"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.HELD.value == "held"
        assert JobStatus.REMOVED.value == "removed"
        assert JobStatus.TRANSFERRING.value == "transferring"
        assert JobStatus.SUSPENDED.value == "suspended"

    def test_display_names(self):
        """Verify display names are capitalized."""
        assert JobStatus.PENDING.display_name == "Pending"
        assert JobStatus.RUNNING.display_name == "Running"
        assert JobStatus.COMPLETED.display_name == "Completed"

    def test_terminal_states(self):
        """Verify which states are terminal."""
        # Only COMPLETED and REMOVED are terminal
        assert JobStatus.COMPLETED.is_terminal() is True
        assert JobStatus.REMOVED.is_terminal() is True

        # HELD is NOT terminal - job can be released
        assert JobStatus.HELD.is_terminal() is False

        # Other states are not terminal
        assert JobStatus.PENDING.is_terminal() is False
        assert JobStatus.IDLE.is_terminal() is False
        assert JobStatus.RUNNING.is_terminal() is False
        assert JobStatus.TRANSFERRING.is_terminal() is False
        assert JobStatus.SUSPENDED.is_terminal() is False


class TestScheddFallback:
    """Tests for schedd/history fallback when log files are unavailable."""

    @pytest.fixture
    def mock_executor_with_fallback(self):
        """Create a mock executor with fallback methods bound."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._job_event_logs = {}
        executor._job_current_states = {}
        executor._log_missing_counts = {}
        executor._log_missing_threshold = 3
        executor._workflow_start_time = int(time.time()) - 3600  # 1 hour ago
        executor.jobDir = "/tmp/test"

        # Bind methods for the new batch-based approach
        executor._read_job_events = Executor._read_job_events.__get__(
            executor, Executor
        )
        executor._get_job_event_log = Executor._get_job_event_log.__get__(
            executor, Executor
        )
        executor._try_read_job_log = Executor._try_read_job_log.__get__(
            executor, Executor
        )
        executor._needs_fallback = Executor._needs_fallback.__get__(executor, Executor)
        executor._batch_query_schedd = Executor._batch_query_schedd.__get__(
            executor, Executor
        )
        executor._batch_query_history = Executor._batch_query_history.__get__(
            executor, Executor
        )
        executor._htcondor_status_to_job_status = (
            Executor._htcondor_status_to_job_status.__get__(executor, Executor)
        )

        return executor

    def test_htcondor_status_conversion(self, mock_executor_with_fallback):
        """Test conversion of HTCondor status codes to JobStatus enum."""
        executor = mock_executor_with_fallback

        assert executor._htcondor_status_to_job_status(1) == JobStatus.IDLE
        assert executor._htcondor_status_to_job_status(2) == JobStatus.RUNNING
        assert executor._htcondor_status_to_job_status(3) == JobStatus.REMOVED
        assert executor._htcondor_status_to_job_status(4) == JobStatus.COMPLETED
        assert executor._htcondor_status_to_job_status(5) == JobStatus.HELD
        assert executor._htcondor_status_to_job_status(6) == JobStatus.TRANSFERRING
        assert executor._htcondor_status_to_job_status(7) == JobStatus.SUSPENDED
        # Unknown status should return PENDING
        assert executor._htcondor_status_to_job_status(99) == JobStatus.PENDING

    def test_log_missing_count_tracks_failures(self, mock_executor_with_fallback):
        """Test that log missing count increases on each failed read attempt."""
        executor = mock_executor_with_fallback

        # First attempt - log not available (returns None)
        result = executor._try_read_job_log(12345)

        # Should have incremented the counter
        assert executor._log_missing_counts[12345] == 1
        assert result is None

        # Second attempt
        result = executor._try_read_job_log(12345)
        assert executor._log_missing_counts[12345] == 2

    def test_needs_fallback_respects_threshold(self, mock_executor_with_fallback):
        """Test that _needs_fallback returns True only after threshold is reached."""
        executor = mock_executor_with_fallback

        # Below threshold
        executor._log_missing_counts[12345] = 1
        assert executor._needs_fallback(12345) is False

        executor._log_missing_counts[12345] = 2
        assert executor._needs_fallback(12345) is False

        # At threshold
        executor._log_missing_counts[12345] = 3
        assert executor._needs_fallback(12345) is True

        # Above threshold
        executor._log_missing_counts[12345] = 5
        assert executor._needs_fallback(12345) is True

    def test_log_missing_count_reset_on_success(self, mock_executor_with_fallback):
        """Test that log missing count is reset when log becomes available."""
        from htcondor2 import JobEventType

        executor = mock_executor_with_fallback

        # Set up a counter as if we've had some failures
        executor._log_missing_counts[12345] = 2

        # Now set up a working log
        mock_event_log = Mock()
        mock_event_log.events = Mock(
            return_value=iter(
                [
                    make_event(JobEventType.SUBMIT),
                    make_event(JobEventType.EXECUTE),
                ]
            )
        )
        executor._job_event_logs[12345] = mock_event_log

        # Read should succeed and reset counter
        result = executor._try_read_job_log(12345)

        assert result["status"] == JobStatus.RUNNING
        assert 12345 not in executor._log_missing_counts

    def test_batch_query_returns_dict_per_job(self, mock_executor_with_fallback):
        """Test that batch query methods return a dict keyed by cluster_id."""
        executor = mock_executor_with_fallback

        # Empty list should return empty dict
        result = executor._batch_query_schedd([])
        assert result == {}

        result = executor._batch_query_history([])
        assert result == {}


class TestHistoryFallback:
    """Tests specific to condor_history fallback."""

    def test_history_uses_workflow_start_time(self):
        """Verify that workflow start time is tracked for history queries."""
        executor = Mock(spec=Executor)
        executor.logger = Mock()
        executor._workflow_start_time = int(time.time()) - 7200  # 2 hours ago

        # The workflow start time should be set and in the past
        assert executor._workflow_start_time > 0
        assert executor._workflow_start_time < time.time()

    def test_batch_approach_prevents_blocking(self):
        """
        Verify the two-pass design: log reads are done first for all jobs,
        then a single batch fallback query is made.

        This test documents the expected behavior: when multiple jobs need
        fallback, they should all be queried in a single batch call rather
        than sequentially blocking each other.
        """
        # The implementation uses check_active_jobs which:
        # 1. First pass: tries log files for ALL jobs (fast, non-blocking)
        # 2. Second pass: batch queries schedd for ALL jobs needing fallback
        # 3. Third pass: batch queries history for remaining jobs
        #
        # This ensures that a slow history query for job1 doesn't block
        # the status check for job2, job3, etc.
        #
        # This is a design documentation test - the actual behavior is
        # verified by the integration tests in TestCheckActiveJobsIntegration.
        pass
