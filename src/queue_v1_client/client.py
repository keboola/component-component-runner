import json
import time
from typing import Optional, Dict

from keboola.http_client import HttpClient
from requests.exceptions import HTTPError
import logging

BASE_URL = "https://syrup.{STACK}keboola.com/"
QUEUE_URL = "https://syrup.keboola.com/queue/jobs/"
CLOUD_URL = "https://queue.{STACK}keboola.cloud"
VALID_STACKS = ["", "eu-central-1.", "north-europe.azure.", "europe-west3.gcp.", "us-east4.gcp."]


class KeboolaClientQueueV1Exception(Exception):
    pass


class KeboolaClientQueueV1(HttpClient):
    def __init__(self, sapi_token: str, keboola_stack: str, custom_stack: str) -> None:
        if keboola_stack == "Custom Stack":
            base_url = CLOUD_URL.replace("{STACK}", custom_stack)
            logging.info(f"Using custom stack: {base_url}")

        else:
            base_url = BASE_URL.replace("{STACK}", keboola_stack)
            self.validate_stack(keboola_stack)
            logging.info(f"validating stack: {base_url}")

        self.auth_header = {"Content-Type": "application/json",
                            "X-StorageApi-Token": sapi_token}
        super().__init__(base_url, auth_header=self.auth_header)

    def run_job(self, component_id: str, config_id: str, variables: Optional[Dict]):
        endpoint = f"/docker/{component_id}/run"
        data = {"config": config_id}
        if variables:
            flat_variables = [{"name": k, "value": v} for k, v in variables.items()]
            data["variableValuesData"] = {"values": flat_variables}
        try:
            return self.post(endpoint_path=endpoint, data=json.dumps(data))
        except HTTPError as http_err:
            raise KeboolaClientQueueV1Exception(http_err) from http_err

    def wait_until_job_finished(self, job_id: str) -> str:
        is_finished = False
        param = {"include": "metrics"}
        while not is_finished:
            try:
                is_finished = self.get(endpoint_path=f"/queue/jobs/{job_id}",
                                       params=param).get("isFinished")
            except HTTPError as http_err:
                raise KeboolaClientQueueV1Exception(http_err) from http_err
            time.sleep(10)
        try:
            return self.get(endpoint_path=f"{self.base_url}{job_id}", is_absolute_path=True, params=param).get("status")
        except HTTPError as http_err:
            raise KeboolaClientQueueV1Exception(http_err) from http_err

    @staticmethod
    def validate_stack(stack: str) -> None:
        if stack not in VALID_STACKS:
            raise KeboolaClientQueueV1Exception(
                f"Invalid stack entered, make sure it is in the list of valid stacks {VALID_STACKS} ")
