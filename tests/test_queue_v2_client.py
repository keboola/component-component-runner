"""Tests for how the Queue V2 client reads Queue API replies.

Every unreadable reply must surface as KeboolaClientQueueV2ResponseError. Before that existed, an
error page or an empty body made json.loads raise a bare json.JSONDecodeError which no caller
handled, so the run ended as an opaque internal error.
"""

import json
import unittest
from http import HTTPStatus

import mock
import requests

from queue_v2_client import (
    KeboolaClientQueueV2,
    KeboolaClientQueueV2Exception,
    KeboolaClientQueueV2ResponseError,
)

NOT_JSON_OBJECT_BODIES = {
    "html error page": "<html>\n<head><title>502 Bad Gateway</title></head>\n</html>",
    "empty body": "",
    "whitespace only": "   \n\t ",
    "plain text": "Bad Gateway",
    "json null": "null",
    "json list": "[1, 2, 3]",
    "json string": '"a string"',
    "json number": "42",
    "truncated json": '{"id": "123"',
}


def _response(status_code: int, text: str) -> requests.Response:
    """Build a minimal requests.Response with the given status code and raw body."""
    response = requests.Response()
    response.status_code = status_code
    response.reason = HTTPStatus(status_code).phrase
    response.url = "https://queue.keboola.com/jobs"
    response._content = text.encode("utf-8")
    return response


class TestHandleHttpError(unittest.TestCase):
    """_handle_http_error must never let a parse error escape.

    A JSON object error body keeps raising KeboolaClientQueueV2Exception, which is what makes the
    caller fall back to the Queue V1 API. Anything else raises KeboolaClientQueueV2ResponseError,
    which deliberately does not trigger that fallback.
    """

    def test_success_reply_does_not_raise(self):
        for status in (200, 201, 204):
            with self.subTest(status=status):
                KeboolaClientQueueV2._handle_http_error(_response(status, json.dumps({"id": "123"})))

    def test_json_object_error_body_keeps_existing_message(self):
        body = json.dumps({"error": "Configuration not found", "code": 404})
        with self.assertRaises(KeboolaClientQueueV2Exception) as ctx:
            KeboolaClientQueueV2._handle_http_error(_response(404, body))
        self.assertEqual("Configuration not found. Exception code 404", str(ctx.exception))
        self.assertNotIsInstance(ctx.exception, KeboolaClientQueueV2ResponseError)

    def test_json_object_error_body_without_expected_keys_keeps_existing_message(self):
        with self.assertRaises(KeboolaClientQueueV2Exception) as ctx:
            KeboolaClientQueueV2._handle_http_error(_response(500, json.dumps({"unexpected": 1})))
        self.assertEqual("None. Exception code None", str(ctx.exception))

    def test_unreadable_error_body_raises_response_error(self):
        """Regression for the reported failure: json.JSONDecodeError must not escape this handler."""
        for label, body in NOT_JSON_OBJECT_BODIES.items():
            with self.subTest(body=label):
                with self.assertRaises(KeboolaClientQueueV2ResponseError) as ctx:
                    KeboolaClientQueueV2._handle_http_error(_response(502, body))
                self.assertNotIsInstance(ctx.exception, json.JSONDecodeError)
                self.assertNotIsInstance(ctx.exception, KeboolaClientQueueV2Exception)
                self.assertIn("HTTP 502", str(ctx.exception))

    def test_empty_error_body_is_reported_as_empty(self):
        with self.assertRaises(KeboolaClientQueueV2ResponseError) as ctx:
            KeboolaClientQueueV2._handle_http_error(_response(503, ""))
        self.assertIn("<empty>", str(ctx.exception))

    def test_blank_error_body_is_not_reported_as_empty(self):
        with self.assertRaises(KeboolaClientQueueV2ResponseError) as ctx:
            KeboolaClientQueueV2._handle_http_error(_response(503, "   \n\t "))
        self.assertIn("<no printable content>", str(ctx.exception))

    def test_long_error_body_excerpt_is_capped(self):
        with self.assertRaises(KeboolaClientQueueV2ResponseError) as ctx:
            KeboolaClientQueueV2._handle_http_error(_response(500, "x" * 5_000_000))
        message = str(ctx.exception)
        self.assertIn("x" * 200, message)
        self.assertNotIn("x" * 201, message)


class TestParseJsonBody(unittest.TestCase):
    """A 2xx reply whose body is not a JSON object must also fail as a response error."""

    def test_json_object_is_returned_unchanged(self):
        payload = {"id": "123", "isFinished": False, "status": "processing"}
        parsed = KeboolaClientQueueV2._parse_json_body(_response(200, json.dumps(payload)), "reply")
        self.assertEqual(payload, parsed)

    def test_unreadable_body_raises_response_error(self):
        for label, body in NOT_JSON_OBJECT_BODIES.items():
            with self.subTest(body=label):
                with self.assertRaises(KeboolaClientQueueV2ResponseError):
                    KeboolaClientQueueV2._parse_json_body(_response(200, body), "reply")


class TestClientMethods(unittest.TestCase):
    """The same guarantee through the public methods, with the HTTP layer mocked out."""

    def setUp(self):
        self.client = KeboolaClientQueueV2("token", "", "")

    def test_run_job_returns_payload_on_success(self):
        reply = _response(201, json.dumps({"id": "789"}))
        with mock.patch.object(KeboolaClientQueueV2, "post_raw", return_value=reply):
            self.assertEqual({"id": "789"}, self.client.run_job("keboola.some-component", "1", None))

    def test_run_job_raises_response_error_on_empty_success_body(self):
        with mock.patch.object(KeboolaClientQueueV2, "post_raw", return_value=_response(200, "")):
            with self.assertRaises(KeboolaClientQueueV2ResponseError):
                self.client.run_job("keboola.some-component", "1", None)

    def test_run_job_raises_response_error_on_html_error_body(self):
        reply = _response(502, "<html><body>502 Bad Gateway</body></html>")
        with mock.patch.object(KeboolaClientQueueV2, "post_raw", return_value=reply):
            with self.assertRaises(KeboolaClientQueueV2ResponseError):
                self.client.run_job("keboola.some-component", "1", None)

    def test_wait_until_job_finished_returns_status_on_success(self):
        replies = [
            _response(200, json.dumps({"isFinished": True})),
            _response(200, json.dumps({"status": "success"})),
        ]
        with mock.patch.object(KeboolaClientQueueV2, "get_raw", side_effect=replies):
            with mock.patch("queue_v2_client.client.time.sleep"):
                self.assertEqual("success", self.client.wait_until_job_finished("789"))

    def test_wait_until_job_finished_raises_response_error_on_html_body(self):
        reply = _response(200, "<html><body>gateway</body></html>")
        with mock.patch.object(KeboolaClientQueueV2, "get_raw", return_value=reply):
            with mock.patch("queue_v2_client.client.time.sleep"):
                with self.assertRaises(KeboolaClientQueueV2ResponseError):
                    self.client.wait_until_job_finished("789")

    def test_invalid_stack_raises_client_exception(self):
        with self.assertRaises(KeboolaClientQueueV2Exception):
            KeboolaClientQueueV2("token", "not-a-stack.", "")


if __name__ == "__main__":
    unittest.main()
