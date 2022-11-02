import json
import time
from typing import Optional, Dict

from keboola.http_client import HttpClient
from requests.exceptions import HTTPError

BASE_URL = "https://syrup.keboola.com/docker/"
QUEUE_URL = "https://syrup.keboola.com/queue/jobs/"

RUN_ENDPOINT = "run"


class KeboolaClientQueueV1Exception(Exception):
    pass


class KeboolaClientQueueV1(HttpClient):
    def __init__(self, sapi_token: str) -> None:
        self.auth_header = {"Content-Type": "application/json",
                            "X-StorageApi-Token": sapi_token}
        super().__init__(BASE_URL, auth_header=self.auth_header)

    def run_job(self, component_id: str, config_id: str, variables: Optional[Dict]):
        endpoint = "/".join([component_id, RUN_ENDPOINT])
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
                is_finished = self.get(endpoint_path=f"{QUEUE_URL}{job_id}",
                                       is_absolute_path=True,
                                       params=param).get("isFinished")
            except HTTPError as http_err:
                raise KeboolaClientQueueV1Exception(http_err) from http_err
            time.sleep(10)
        try:
            return self.get(endpoint_path=f"{QUEUE_URL}{job_id}", is_absolute_path=True, params=param).get("status")
        except HTTPError as http_err:
            raise KeboolaClientQueueV1Exception(http_err) from http_err
