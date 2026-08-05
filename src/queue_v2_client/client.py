import json
import time
from typing import Dict, Optional

import requests
from keboola.http_client import HttpClient
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry

QUEUE_V2_URL = "https://queue.{STACK}keboola.com"
CLOUD_URL = "https://queue.{STACK}keboola.cloud"
VALID_STACKS = ["", "eu-central-1.", "north-europe.azure.", "europe-west3.gcp.", "us-east4.gcp."]


class KeboolaClientQueueV2Exception(Exception):
    pass


class KeboolaClientQueueV2ResponseError(Exception):
    """The Queue API replied with a body that could not be read as a JSON object.

    Deliberately NOT a subclass of KeboolaClientQueueV2Exception. That exception tells the caller to
    retry the operation against the legacy Queue V1 API, which for a job start request would start a
    second job. An unreadable reply gives no way to tell whether the request already took effect, so
    it must end the run rather than be retried.
    """


def _loads_json_object(text: Optional[str]) -> Optional[Dict]:
    """Parse text as a JSON object. Returns None if it is not valid JSON, or not a JSON object.

    RecursionError and UnicodeDecodeError are included because a hostile or broken reply should not
    be able to end the run with an unhandled exception through this function either.
    """
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError, RecursionError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _summarise_body(text: Optional[str], limit: int = 200) -> str:
    """Collapse a reply body into a short single-line excerpt for an error message."""
    if not text:
        return "<empty>"
    # Slice before normalising - an error page can be megabytes and only the excerpt is reported.
    collapsed = " ".join(text[: limit * 10].split())
    return collapsed[:limit] if collapsed else "<no printable content>"


class KeboolaClientQueueV2(HttpClient):
    def __init__(self, sapi_token: str, keboola_stack: str, custom_cloud_stack: Optional[str]) -> None:
        """
        Args:
            sapi_token:
            keboola_stack: str, e.g. https://queue.{STACK}keboola.com.
                           For instance one of ["", "eu-central-1.", "north-europe.azure.", "Custom Stack"]
            custom_cloud_stack: str, name of custom stack (https://queue.{STACK}keboola.cloud),
                          required if keboola_stack == "Custom Stack"
        """
        auth_header = {"X-StorageApi-Token": sapi_token}

        if not custom_cloud_stack.endswith("."):
            custom_cloud_stack = custom_cloud_stack + "."

        if keboola_stack == "Custom Stack":
            job_url = CLOUD_URL.replace("{STACK}", custom_cloud_stack)
        else:
            job_url = QUEUE_V2_URL.replace("{STACK}", keboola_stack)
            self.validate_stack(keboola_stack)
        super().__init__(job_url, auth_header=auth_header)

    @staticmethod
    def validate_stack(stack: str) -> None:
        if stack not in VALID_STACKS:
            raise KeboolaClientQueueV2Exception(
                f"Invalid stack entered, make sure it is in the list of valid stacks {VALID_STACKS} "
            )

    def run_job(self, component_id: str, config_id: str, variables: Optional[Dict]) -> Dict:
        data = {"component": component_id, "mode": "run", "config": config_id}
        if variables:
            flat_variables = [{"name": k, "value": v} for k, v in variables.items()]
            data["variableValuesData"] = {"values": flat_variables}
        header = {"Content-Type": "application/json"}

        response = self.post_raw(endpoint_path="jobs", headers=header, data=json.dumps(data))
        self._handle_http_error(response)
        return self._parse_json_body(response, "reply to a job start request")

    def wait_until_job_finished(self, job_id: str) -> str:
        is_finished = False
        while not is_finished:
            try:
                response = self.get_raw(endpoint_path=f"jobs/{job_id}")
                self._handle_http_error(response)
                is_finished = self._parse_json_body(response, "job status reply").get("isFinished")
            except HTTPError as http_err:
                raise KeboolaClientQueueV2Exception(http_err) from http_err
            time.sleep(10)
        try:
            response = self.get_raw(endpoint_path=f"jobs/{job_id}")
            self._handle_http_error(response)
            return self._parse_json_body(response, "job status reply").get("status")
        except HTTPError as http_err:
            raise KeboolaClientQueueV2Exception(http_err) from http_err

    @staticmethod
    def _handle_http_error(response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            # An HTTP error reply is not guaranteed to carry a JSON object body: gateways and proxies
            # answer 5xx with an HTML page, and some errors come back with an empty body. Parsing
            # such a body used to raise a bare json.JSONDecodeError, which no caller handles, so the
            # run ended as an opaque internal error instead of a message the user can act on.
            response_error = _loads_json_object(e.response.text)
            if response_error is None:
                raise KeboolaClientQueueV2ResponseError(
                    f"The Keboola Queue API returned HTTP {e.response.status_code} with an "
                    f"unexpected error body: {_summarise_body(e.response.text)}"
                ) from e
            raise KeboolaClientQueueV2Exception(
                f"{response_error.get('error')}. Exception code {response_error.get('code')}"
            ) from e

    @staticmethod
    def _parse_json_body(response, description: str) -> Dict:
        """Read a reply body that is expected to be a JSON object.

        Guards the same defect as _handle_http_error: an unreadable body used to leak a
        json.JSONDecodeError (or an AttributeError, for valid JSON that is not an object) as an
        unhandled internal error.
        """
        parsed = _loads_json_object(response.text)
        if parsed is None:
            raise KeboolaClientQueueV2ResponseError(
                f"The Keboola Queue API returned HTTP {response.status_code} with an unexpected "
                f"{description}: {_summarise_body(response.text)}"
            )
        return parsed

    # override to continue on failure
    def _requests_retry_session(self, session=None):
        session = session or requests.Session()
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
