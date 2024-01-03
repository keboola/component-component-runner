import csv
import logging
from typing import Optional, Dict, Generator, List

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from kbcstorage.components import Components
from kbcstorage.configurations import Configurations

from queue_v1_client import KeboolaClientQueueV1, KeboolaClientQueueV1Exception
from queue_v2_client import KeboolaClientQueueV2, KeboolaClientQueueV2Exception

KEY_COMPONENT_PARAMETERS = "component_parameters"
KEY_SAPI_TOKEN = "#sapi_token"
KEY_COMPONENT_ID = "component_id"
KEY_CONFIG_ID = "config_id"
KEY_KBC_STACK = "keboola_stack"
KEY_CUSTOM_STACK = "custom_stack"

KEY_RUN_PARAMETERS = "run_parameters"
KEY_WAIT_UNTIL_FINISH = "wait_until_finish"
KEY_USE_VARIABLES = "use_variables"
KEY_VARIABLE_MODE = "variable_mode"
KEY_VARIABLES = "variables"
KEY_VARIABLE_NAME = "name"
KEY_VARIABLE_VALUE = "value"

REQUIRED_PARAMETERS = [KEY_COMPONENT_PARAMETERS, KEY_RUN_PARAMETERS]
REQUIRED_IMAGE_PARS = []
REQUIRED_COMPONENT_PARAMETERS = [KEY_SAPI_TOKEN, KEY_COMPONENT_ID, KEY_CONFIG_ID, KEY_KBC_STACK]
REQUIRED_RUN_PARAMETERS = [KEY_WAIT_UNTIL_FINISH, KEY_USE_VARIABLES]

VARIABLE_MODES = ["self_defined", "from_file_run_all", "from_file_run_first"]


class Component(ComponentBase):

    def __init__(self):
        self.client_v1 = None
        self.client_v2 = None
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        component_parameters = params.get(KEY_COMPONENT_PARAMETERS, {})
        run_parameters = params.get(KEY_RUN_PARAMETERS, {})
        self._validate_parameters(component_parameters, REQUIRED_COMPONENT_PARAMETERS, 'component config parameters')
        self._validate_parameters(run_parameters, REQUIRED_RUN_PARAMETERS, 'component run parameters')

        component_id = component_parameters.get(KEY_COMPONENT_ID)
        config_id = component_parameters.get(KEY_CONFIG_ID)
        sapi_token = component_parameters.get(KEY_SAPI_TOKEN)
        keboola_stack = component_parameters.get(KEY_KBC_STACK, "")
        custom_stack = component_parameters.get(KEY_CUSTOM_STACK, "")

        wait_until_finish = run_parameters.get(KEY_WAIT_UNTIL_FINISH)
        use_variables = run_parameters.get(KEY_USE_VARIABLES)
        variable_mode = run_parameters.get(KEY_VARIABLE_MODE)
        variables = run_parameters.get(KEY_VARIABLES)

        self._init_clients(sapi_token, keboola_stack, custom_stack)

        if use_variables:
            run_variable_groups = self.get_run_variables(variable_mode, variables)
            for run_variable_group in run_variable_groups:
                logging.info(f"Running job of component '{component_id}' with variables {run_variable_group}")
                self.run_job(component_id, config_id, wait_until_finish, variables=run_variable_group)
        else:
            logging.info(f"Running job of component '{component_id}'")
            self.run_job(component_id, config_id, wait_until_finish)

    def _init_clients(self, sapi_token: str, keboola_stack: str, custom_stack: str) -> None:
        self.client_v1 = KeboolaClientQueueV1(sapi_token, keboola_stack, custom_stack)
        self.client_v2 = KeboolaClientQueueV2(sapi_token, keboola_stack, custom_stack)

    def run_job(self, component_id: str, config_id: str, wait_until_finish: bool,
                variables: Optional[Dict] = None) -> None:
        response = self.run_component_job(component_id, config_id, variables)
        job_id = response.get("id")
        logging.info(f"Job execution started with job ID {job_id}")
        if wait_until_finish:
            logging.info("Waiting till job run is finished")
            status = self.wait_until_job_finished(job_id)
            self.process_status(status)
            logging.info(f"Job {job_id} finished with success")
        else:
            logging.info("Job is being run. if you require the trigger to wait "
                         "till the job is finished, specify this in the configuration")

    def run_component_job(self, component_id: str, config_id: str, variables: Optional[Dict] = None) -> Dict:
        try:
            return self.client_v2.run_job(component_id, config_id, variables)
        except KeboolaClientQueueV2Exception as v2_exc:
            try:
                return self.client_v1.run_job(component_id, config_id, variables)
            except KeboolaClientQueueV1Exception as v1_exc:
                raise UserException(f"Failed to run the component job, please recheck the validity of "
                                    f"your configuration \n\n{v2_exc, v1_exc}") from v1_exc

    def get_single_input_table(self) -> Optional[TableDefinition]:
        input_tables = self.get_input_tables_definitions()
        if len(input_tables) > 1:
            raise UserException("Only 1 Input table is allowed to be set")
        elif len(input_tables) == 0:
            return None
        else:
            return input_tables[0]

    def wait_until_job_finished(self, job_id):
        try:
            return self.client_v2.wait_until_job_finished(job_id)
        except KeboolaClientQueueV2Exception as v2_exc:
            try:
                return self.client_v1.wait_until_job_finished(job_id)
            except KeboolaClientQueueV1Exception as v1_exc:
                raise UserException(f"Failed to monitor Job ID {job_id}."
                                    f"\n\n{v2_exc, v1_exc}") from v1_exc

    @staticmethod
    def process_status(status: str) -> None:
        if status.lower() != "success":
            raise UserException(f"Orchestration did not end in success, ended in {status}")

    @staticmethod
    def get_variable_reader(input_table: TableDefinition) -> Generator:
        with open(input_table.full_path, 'r') as in_table:
            yield from csv.DictReader(in_table)

    def get_run_variables(self, variable_mode: str, variables: List[Dict]) -> Optional[Generator]:
        if variable_mode == "self_defined":
            yield {var[KEY_VARIABLE_NAME]: var[KEY_VARIABLE_VALUE] for var in variables}
        elif variable_mode == "from_file_run_all":
            input_table = self.get_single_input_table()
            yield from self.get_variable_reader(input_table)
        elif variable_mode == "from_file_run_first":
            input_table = self.get_single_input_table()
            yield next(self.get_variable_reader(input_table))
        else:
            raise UserException(f"Variable mode should be one of the following : {VARIABLE_MODES}")

    @sync_action('list_components')
    def list_components(self):
        CONNECTION_URL = "https://connection.{STACK}keboola.com"

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        component_parameters = params.get(KEY_COMPONENT_PARAMETERS, {})
        sapi_token = component_parameters.get(KEY_SAPI_TOKEN)

        keboola_stack = component_parameters.get(KEY_KBC_STACK, "")
        custom_stack = component_parameters.get(KEY_CUSTOM_STACK, "")

        if keboola_stack == "Custom Stack":
            root_url = CONNECTION_URL.replace("{STACK}", custom_stack)
        else:
            root_url = CONNECTION_URL.replace("{STACK}", keboola_stack)

        components = Components(root_url, sapi_token, "default")

        return [SelectElement(label=c['id'], value=c['id']) for c in components.list()]

    @sync_action('list_configurations')
    def list_configurations(self):
        CONNECTION_URL = "https://connection.{STACK}keboola.com"

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        component_parameters = params.get(KEY_COMPONENT_PARAMETERS, {})
        sapi_token = component_parameters.get(KEY_SAPI_TOKEN)
        keboola_stack = component_parameters.get(KEY_KBC_STACK, "")
        custom_stack = component_parameters.get(KEY_CUSTOM_STACK, "")

        component_id = component_parameters.get(KEY_COMPONENT_ID)

        if keboola_stack == "Custom Stack":
            root_url = CONNECTION_URL.replace("{STACK}", custom_stack)
        else:
            root_url = CONNECTION_URL.replace("{STACK}", keboola_stack)

        configuration = Configurations(root_url, sapi_token, "default")

        return [SelectElement(label=f"[{c['id']}] {c['name']}", value=c['id'])
                for c in configuration.list(component_id)]


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
