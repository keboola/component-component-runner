import csv
import logging
from typing import Optional, Dict, Generator, List

from configuration import Configuration, VariableMode

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from kbcstorage.components import Components
from kbcstorage.configurations import Configurations

from queue_v1_client import KeboolaClientQueueV1, KeboolaClientQueueV1Exception
from queue_v2_client import KeboolaClientQueueV2, KeboolaClientQueueV2Exception


class Component(ComponentBase):

    def __init__(self):
        self.client_v1 = None
        self.client_v2 = None
        super().__init__()

    def run(self):
        self._init_configuration()

        component_id = self._configuration.component_parameters.component_id
        config_id = self._configuration.component_parameters.config_id
        sapi_token = self._configuration.component_parameters.pswd_sapi_token
        keboola_stack = self._configuration.component_parameters.keboola_stack
        custom_stack = self._configuration.component_parameters.custom_stack

        wait_until_finish = self._configuration.run_parameters.wait_until_finish
        use_variables = self._configuration.run_parameters.use_variables
        variable_mode = self._configuration.run_parameters.variable_mode
        variables = self._configuration.run_parameters.variables

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

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

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
            logging.warning(f"Failed to run the component job using V2 API \n{v2_exc}")
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
            yield {var["name"]: var["value"] for var in variables}
        elif variable_mode == "from_file_run_all":
            input_table = self.get_single_input_table()
            yield from self.get_variable_reader(input_table)
        elif variable_mode == "from_file_run_first":
            input_table = self.get_single_input_table()
            yield next(self.get_variable_reader(input_table))
        else:
            raise UserException(f"Variable mode should be one of the following : "
                                f"{', '.join(mode.value for mode in VariableMode)}")

    @staticmethod
    def get_stack_url(custom_stack, keboola_stack):
        connection_url = "https://connection.{STACK}keboola.com"
        cloud_url = "https://connection.{STACK}keboola.cloud"

        if not custom_stack.endswith("."):
            custom_stack = custom_stack+"."

        if keboola_stack == "Custom Stack":
            root_url = cloud_url.replace("{STACK}", custom_stack)
        else:
            root_url = connection_url.replace("{STACK}", keboola_stack)
        return root_url

    @sync_action('list_components')
    def list_components(self):
        self._init_configuration()

        stack_url = self.get_stack_url(self._configuration.component_parameters.custom_stack,
                                       self._configuration.component_parameters.keboola_stack)

        components = Components(stack_url, self._configuration.component_parameters.pswd_sapi_token, "default")

        return [SelectElement(label=f"{c['name']} {c['type']} [{c['id']}]", value=c['id']) for c in components.list()]

    @sync_action('list_configurations')
    def list_configurations(self):
        self._init_configuration()

        stack_url = self.get_stack_url(self._configuration.component_parameters.custom_stack,
                                       self._configuration.component_parameters.keboola_stack)

        configuration = Configurations(stack_url, self._configuration.component_parameters.pswd_sapi_token, "default")

        return [SelectElement(label=f"{c['name']} [{c['id']}]", value=c['id'])
                for c in configuration.list(self._configuration.component_parameters.component_id)]


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
