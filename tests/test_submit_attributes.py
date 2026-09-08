"""
Tests for HTCondor submit attribute handling.

Raw attributes are passed via the htcondor_submit_<name> resource prefix:
    htcondor_submit_output_destination -> output_destination (string)
    htcondor_submit_MY__SendCredential -> MY.SendCredential (double underscore converts to dot)
    htcondor_submit_nice_user          -> nice_user (boolean)
    htcondor_submit_priority           -> priority (integer)

If a htcondor_submit_* name collides with a key the plugin already sets,
the executor-set key takes precedence; htcondor_submit_* will not override
built-in submit fields.

String values are wrapped in double quotes (for ClassAd string literals).
Non-string values (bool, int) are passed through as-is.

"""

import pytest
from unittest.mock import Mock

from conftest import create_mock_submit_executor, mock_htcondor_submission


class TestSubmittingAttributes:
    @pytest.fixture
    def mock_executor(self, tmp_path):
        return create_mock_submit_executor(tmp_path)

    def test_run_job_applies_htcondor_submit_raw_attributes(self, mock_executor):
        """Test that htcondor_submit_* resources are copied into the submit dict."""
        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            job = Mock()
            job.name = "rule_a"
            job.jobid = 7
            job.threads = 1
            job.resources = {
                "htcondor_submit_output_destination": "/mnt/gdrive/outputs",
                "htcondor_submit_MY__SendCredential": True,
                "htcondor_submit_priority": 5,
                "htcondor_submit_nice_user": True,
            }

            mock_executor.run_job(job)

        assert captured_submit_dict["output_destination"] == '"/mnt/gdrive/outputs"'
        assert captured_submit_dict["MY.SendCredential"] is True
        assert captured_submit_dict["priority"] == 5
        assert captured_submit_dict["nice_user"] is True
        assert captured_submit_dict["batch_name"] == "rule_a-7"

    def test_run_job_applies_classad_attributes(self, mock_executor):
        """Test that classad_* resources are copied into the submit dict."""
        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            job = Mock()
            job.name = "rule_b"
            job.jobid = 8
            job.threads = 1
            job.resources = {
                "classad_MyDataSource": "https://example.com/data",
                "classad_nice_user": True,
                "classad_priority": 5,
            }

            mock_executor.run_job(job)

        assert captured_submit_dict["+MyDataSource"] == '"https://example.com/data"'
        assert captured_submit_dict["+nice_user"] is True
        assert captured_submit_dict["+priority"] == 5
        assert captured_submit_dict["batch_name"] == "rule_b-8"

    def test_run_job_applies_classad_and_htcondor_submit_together(self, mock_executor):
        """Test that classad_* and htcondor_submit_* resources can coexist."""
        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            job = Mock()
            job.name = "rule_c"
            job.jobid = 9
            job.threads = 1
            job.resources = {
                "classad_MyDataSource": "https://example.com/data",
                "classad_priority": 5,
                "htcondor_submit_nice_user": True,
                "htcondor_submit_MY__SendCredential": False,
            }

            mock_executor.run_job(job)

        assert captured_submit_dict["+MyDataSource"] == '"https://example.com/data"'
        assert captured_submit_dict["+priority"] == 5
        assert captured_submit_dict["nice_user"] is True
        assert captured_submit_dict["MY.SendCredential"] is False
        assert captured_submit_dict["batch_name"] == "rule_c-9"

    def test_same_resources_behaviors(self, mock_executor):
        """Raw htcondor_submit_* values should NOT override plugin-generated submit keys."""
        captured_submit_dict = {}

        # Ensure the executor generates a transfer_input_files entry
        mock_executor._get_exec_args_and_transfer_files.return_value = (
            "python",
            "-m snakemake --cores 1",
            ["htcondor.txt"],
            [],
            [],
        )
        with mock_htcondor_submission(captured_submit_dict):
            job = Mock()
            job.name = "rule_d"
            job.jobid = 9
            job.threads = 1
            job.resources = {
                # In real usage, paths are not quoted; this test uses a quoted string for simplicity
                "htcondor_submit_transfer_input_files": "htcondor_submit.txt",
                "htcondor_submit_request_memory": 1024,
                "request_memory": 512,
            }
            mock_executor.run_job(job)

        assert captured_submit_dict["transfer_input_files"] == "htcondor.txt"
        assert captured_submit_dict["request_memory"] == 512

    @pytest.mark.parametrize(
        "is_group, expected",
        [
            pytest.param(
                True,
                "work/z_upstream.txt, work/a_downstream.txt",
                id="group_dependency_order",
            ),
            pytest.param(
                False,
                "work/a_downstream.txt, work/z_upstream.txt",
                id="ordinary_lexical_order",
            ),
        ],
    )
    def test_run_job_orders_transfer_outputs(self, mock_executor, is_group, expected):
        """Test grouped order preservation and ordinary lexical sorting."""
        captured_submit_dict = {}
        mock_executor._get_exec_args_and_transfer_files.return_value = (
            "python",
            "-m snakemake --cores 1",
            [],
            ["work/z_upstream.txt", "work/a_downstream.txt"],
            [],
        )
        with mock_htcondor_submission(captured_submit_dict):
            job = Mock()
            job.name = "output_order"
            job.jobid = 10
            job.threads = 1
            job.resources = {}
            job.is_group = Mock(return_value=is_group)
            mock_executor.run_job(job)

        assert captured_submit_dict["transfer_output_files"] == expected
