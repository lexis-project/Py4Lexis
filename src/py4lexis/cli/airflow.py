from typing import Optional
from pandas import DataFrame
from requests import Response, post, get
from dateutil import parser
from datetime import datetime
from random import random

from py4lexis.exceptions import Py4LexisException
from py4lexis.session import LexisSession
from py4lexis.utils import convert_list_of_dicts_to_pandas

from py4lexis.workflows.airflow import Airflow
from tabulate import tabulate
from json import dumps

class AirflowCLI(object):
    """
        A class holds methods to manage DAGs within Lexis Airflow instance using INTERACTIVE mode.

        Attributes
        ----------
        session : class
            Class that holds LEXIS session
        print_content : bool, optional
            If True then contents of all requests will be printed.

        Methods
        -------
        get_workflows_list() -> None
            Prints the table of existing workflows (DAGs).

        get_workflow_info(workflow_id: str) -> None
            Prints info of existing workflow (DAG) selected by its workflow ID (dag_id).

        get_workflow_details(workflow_id: str) -> None
            Prints details of existing workflow (DAG) selected by its workflow ID (dag_id).
        
    """
    
    def __init__(self, session: LexisSession, 
                 print_content: Optional[bool]=False) -> None:
        self.session = session
        self.print_content = print_content
        self.airflow = Airflow(self.session, self.print_content, suppress_print=False)

    @staticmethod
    def _print_json(content: dict):
        formated_json = dumps(content, indent=2)
        print(formated_json)


    def get_workflows_list(self) -> None:
        """
            Prints the table of existing workflows (DAGs) with short info.

            Returns
            -------
            None
        """

        content, req_status = self.airflow.get_workflows_list(content_as_pandas=True)
        
        if req_status is not None:
            try:
                print(f"Formatting pandas DataFrame into ASCII table...")
                
                cols: list[str] = ["WorkflowID (dag_id)", "Description", "Is subdag", "Is active", "Is paused"]
                workflows_table: DataFrame = DataFrame(columns=cols)
                workflows_table["WorkflowID (dag_id)"] = content["dag_id"]
                workflows_table["Description"] = content["description"]
                workflows_table["Is subdag"] = content["is_subdag"]
                workflows_table["Is active"] = content["is_active"]
                workflows_table["Is paused"] = content["is_paused"]
                
                print(tabulate(workflows_table.values.tolist(), cols, tablefmt="grid"))


            except KeyError as kerr:
                self.session.logging.error(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")


    def get_workflow_info(self, workflow_id: str) -> None:
        """
            Prints info of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow.                

            Returns
            -------
            None
        """

        content, req_status = self.airflow.get_workflow_info(workflow_id)

        if req_status is not None:
            self._print_json(content)


    def get_workflow_details(self, workflow_id: str) -> None:
        """
            Prints details of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow. 

            Returns
            -------
            None
        """

        content, req_status = self.airflow.get_workflow_details(workflow_id)

        if req_status is not None:
            self._print_json(content)


    def get_workflow_params(self, workflow_id: str) -> None:
        """
            Gets and prints params of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow. 

            Returns
            -------
            dict | None
                Params of existing workflow as dictionary.  None is returned if some errors have occured.
        """
        
        wf_default_parameters, req_status = self.airflow.get_workflow_params(workflow_id)

        if req_status is not None:
            self._print_json(wf_default_parameters)

        return wf_default_parameters


    def execute_workflow(self, workflow_id: str, workflow_parameters: dict, workflow_run_id: Optional[str | None]=None) -> tuple[dict, int] | tuple[None, None]:
        """
            Execute manually an existing workflow (DAG) which is selected by its workflow ID (dag_id) and prints the result.

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow.
            workflow_parameters: dict
                Parameters of the existing workflow (DAG) as dictionary.
            workflow_run_id: str | None, optional
                Workflow run id (dag_run_id). If None, will be set automatically.

            Returns
            -------
            None
        """

        content, req_status = self.airflow.execute_workflow(workflow_id, workflow_parameters, workflow_run_id)

        if req_status is not None:
            self._print_json(content)
        

    def get_workflow_states(self, workflow_id: str, filter_by_workflow_run_id: Optional[str]="", filter_by_workflow_state: Optional[str]="") -> tuple[list[dict], int] | tuple[DataFrame, int] | tuple[None, None]:
        """
            Prints run states of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow.
            content_as_pandas: bool, optional
                If True, content will be returned as DataFrame. False by default.
            filter_by_workflow_run_id: str, optional
                Can be used to filter workflow states by workflow run ID. If this is set, the filter_by_workflow_state will be ignored.
            filter_by_workflow_state: str, optional.
                Can be used to filter workflow states by their current state. Should be one of: ['success', 'failed', 'running', 'queued'].
                If the filter_by_workflow_run_id is set, this one will be ignored.

            Returns
            -------
            dict | None
                Run states of existing workflow as list of dictionaries.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        content, req_status = self.airflow.get_workflow_states(workflow_id, content_as_pandas=True)

        if req_status is not None:
                
            cols: list[str] = ["WorkflowRunID", "ExecutionDate", "State"]
            workflows_table: DataFrame = DataFrame(columns=cols)
            workflows_table["WorkflowRunID"] = content["workflow_run_id"]
            workflows_table["ExecutionDate"] = content["execution_date"]
            workflows_table["State"] = content["state"]
            
            if filter_by_workflow_run_id == "" and filter_by_workflow_state == "":
                pass

            elif filter_by_workflow_run_id != "":
                workflows_table = workflows_table[workflows_table["WorkflowRunID"] == filter_by_workflow_run_id]
            
            elif filter_by_workflow_state != "":
                if filter_by_workflow_state.lower() in ["success", "failed", "running", "queued"]:
                    workflows_table = workflows_table[workflows_table["State"] == filter_by_workflow_run_id]
                else:
                    print(f"filter_by_workflow_state should be one of: ['success', 'failed', 'running', 'queued']")        
                    self.session.logging.error(f"GET -- WORKFLOW STATE -- filter_by_workflow_state should be one of: ['success', 'failed', 'running', 'queued'] -- FAIL")

            else: 
                print(f"WARNING: Filters can't be determined. Returning all states.")
                self.session.logging.warn(f"GET -- WORKFLOW STATE -- Filters can't be determined. Returning all states. -- WARN")

            print(tabulate(workflows_table.values.tolist(), cols, tablefmt="grid"))

            