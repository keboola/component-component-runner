"""Tests for how Component maps Queue client failures onto exit codes.

The component exits 1 for a UserException and 2 for anything else. Exit 2 is an internal error: it
pages the team and shows the user nothing useful. These tests pin which failures reach the user as a
UserException, and - just as importantly - that an unreadable Queue V2 reply is NOT retried against
the Queue V1 API, because a retried job start request would start a second job.
"""

import unittest

import mock
from keboola.component.exceptions import UserException

from component import Component
from queue_v1_client import KeboolaClientQueueV1Exception
from queue_v2_client import KeboolaClientQueueV2Exception, KeboolaClientQueueV2ResponseError


def _component_with_mock_clients() -> Component:
    """Build a Component with both Queue clients mocked, bypassing the Keboola data dir."""
    component = Component.__new__(Component)
    component.client_v1 = mock.MagicMock()
    component.client_v2 = mock.MagicMock()
    return component


class TestRunComponentJob(unittest.TestCase):

    def test_returns_v2_payload_on_success(self):
        component = _component_with_mock_clients()
        component.client_v2.run_job.return_value = {"id": "1"}
        self.assertEqual({"id": "1"}, component.run_component_job("keboola.some-component", "2"))
        component.client_v1.run_job.assert_not_called()

    def test_unreadable_v2_reply_raises_user_exception_without_touching_v1(self):
        """The job must still fail - but as a user error, and without starting a second job."""
        component = _component_with_mock_clients()
        component.client_v2.run_job.side_effect = KeboolaClientQueueV2ResponseError("HTTP 502, unreadable")
        with self.assertRaises(UserException) as ctx:
            component.run_component_job("keboola.some-component", "2")
        component.client_v1.run_job.assert_not_called()
        self.assertIn("keboola.some-component", str(ctx.exception))
        self.assertIn("HTTP 502, unreadable", str(ctx.exception))

    def test_v2_client_exception_still_falls_back_to_v1(self):
        component = _component_with_mock_clients()
        component.client_v2.run_job.side_effect = KeboolaClientQueueV2Exception("boom")
        component.client_v1.run_job.return_value = {"id": "3"}
        self.assertEqual({"id": "3"}, component.run_component_job("keboola.some-component", "2"))
        component.client_v1.run_job.assert_called_once()

    def test_both_clients_failing_still_raises_user_exception(self):
        component = _component_with_mock_clients()
        component.client_v2.run_job.side_effect = KeboolaClientQueueV2Exception("v2 boom")
        component.client_v1.run_job.side_effect = KeboolaClientQueueV1Exception("v1 boom")
        with self.assertRaises(UserException):
            component.run_component_job("keboola.some-component", "2")


class TestWaitUntilJobFinished(unittest.TestCase):

    def test_returns_status_on_success(self):
        component = _component_with_mock_clients()
        component.client_v2.wait_until_job_finished.return_value = "success"
        self.assertEqual("success", component.wait_until_job_finished("4"))
        component.client_v1.wait_until_job_finished.assert_not_called()

    def test_unreadable_v2_reply_raises_user_exception_without_touching_v1(self):
        component = _component_with_mock_clients()
        component.client_v2.wait_until_job_finished.side_effect = KeboolaClientQueueV2ResponseError("HTTP 502")
        with self.assertRaises(UserException) as ctx:
            component.wait_until_job_finished("4")
        component.client_v1.wait_until_job_finished.assert_not_called()
        self.assertIn("job ID 4", str(ctx.exception))
        self.assertIn("HTTP 502", str(ctx.exception))

    def test_v2_client_exception_still_falls_back_to_v1(self):
        component = _component_with_mock_clients()
        component.client_v2.wait_until_job_finished.side_effect = KeboolaClientQueueV2Exception("boom")
        component.client_v1.wait_until_job_finished.return_value = "success"
        self.assertEqual("success", component.wait_until_job_finished("4"))
        component.client_v1.wait_until_job_finished.assert_called_once()

    def test_both_clients_failing_still_raises_user_exception(self):
        component = _component_with_mock_clients()
        component.client_v2.wait_until_job_finished.side_effect = KeboolaClientQueueV2Exception("v2 boom")
        component.client_v1.wait_until_job_finished.side_effect = KeboolaClientQueueV1Exception("v1 boom")
        with self.assertRaises(UserException):
            component.wait_until_job_finished("4")


if __name__ == "__main__":
    unittest.main()
