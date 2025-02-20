import json
import time
from typing import Dict, Optional

import requests
from keboola.http_client import HttpClient
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry
import logging

QUEUE_V2_URL = "https://queue.{STACK}keboola.com"
CLOUD_URL = "https://queue.{STACK}keboola.cloud"
VALID_STACKS = ["", "eu-central-1.", "north-europe.azure.", "europe-west3.gcp.", "us-east4.gcp."]


class KeboolaClientQueueV2Exception(Exception):
    pass


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
            custom_cloud_stack = custom_cloud_stack+"."

        if keboola_stack == "Custom Stack":
            job_url = CLOUD_URL.replace("{STACK}", custom_cloud_stack)
            logging.info(f"Using custom stack: {job_url}")
        else:
            job_url = QUEUE_V2_URL.replace("{STACK}", keboola_stack)
            logging.info(f"validating stack: {job_url}")
            self.validate_stack(keboola_stack)
        super().__init__(job_url, auth_header=auth_header)

    @staticmethod
    def validate_stack(stack: str) -> None:
        if stack not in VALID_STACKS:
            raise KeboolaClientQueueV2Exception(
                f"Invalid stack entered, make sure it is in the list of valid stacks {VALID_STACKS} ")

    def run_job(self, component_id: str, config_id: str, variables: Optional[Dict]) -> Dict:
        data = {"component": component_id,
                "mode": "run",
                "config": config_id}
        if variables:
            flat_variables = [{"name": k, "value": v} for k, v in variables.items()]
            data["variableValuesData"] = {"values": flat_variables}
        header = {'Content-Type': 'application/json'}

        logging.info(f"Starting job {config_id}")
        try:
            response = self.post_raw(endpoint_path="jobs", headers=header, data=json.dumps(data))
        except Exception as e:
            logging.error(f"Error starting job using v2 {e}")
        self._handle_http_error(response)
        logging.info(f"Job {config_id} started")
        return json.loads(response.text)

    def wait_until_job_finished(self, job_id: str) -> str:
        is_finished = False
        logging.info(f"Waiting for job {job_id} to finish")
        while not is_finished:
            try:
                response = self.get_raw(endpoint_path=f"jobs/{job_id}")
                self._handle_http_error(response)
                is_finished = json.loads(response.text).get("isFinished")
            except HTTPError as http_err:
                raise KeboolaClientQueueV2Exception(http_err) from http_err
            time.sleep(10)
        try:
            response = self.get_raw(endpoint_path=f"jobs/{job_id}")
            self._handle_http_error(response)
            return json.loads(response.text).get("status")
        except HTTPError as http_err:
            raise KeboolaClientQueueV2Exception(http_err) from http_err

    @staticmethod
    def _handle_http_error(response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            response_error = json.loads(e.response.text)
            raise KeboolaClientQueueV2Exception(
                f"{response_error.get('error')}. Exception code {response_error.get('code')}") from e

    # override to continue on failure
    def _requests_retry_session(self, session=None):
        session = session or requests.Session()
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
