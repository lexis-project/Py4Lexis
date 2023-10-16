from typing import Optional
from pandas import DataFrame
from requests import Response, post, get
from dateutil import parser
from datetime import datetime
from random import random
from py4lexis.exceptions import Py4LexisException
from py4lexis.session import LexisSession
from py4lexis.utils import convert_list_of_objects_to_pandas


class Airflow(object):
    """
        A class holds methods to manage DAGs within Lexis Airflow instance.

        Attributes
        ----------
        session : class
            Class that holds LEXIS session
        print_content : bool, optional
            If True then contents of all requests will be printed.
        suppress_print: bool, optional
            If True then all prints are suppressed. By default: suppress_print=True

        Methods
        -------
        create_dataset(access: str, 
                        project: str, 
                        push_method: Optional[str]="empty", 
                        zone: Optional[str]="IT4ILexisZone",
                        path: Optional[str]="", 
                        contributor: Optional[list[str]]=["UNKNOWN contributor"], 
                        creator: Optional[list[str]]=["UNKNOWN creator"],
                        owner: Optional[list[str]]=["UNKNOWN owner"], 
                        publicationYear: Optional[str]=str(date.today().year),
                        publisher: Optional[list[str]]=["UNKNOWN publisher"], 
                        resourceType: Optional[str]=str("UNKNOWN resource type"),
                        title: Optional[str]=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int] | tuple[None, None]
            Create an empty dataset with specified attributes.

        
    """
    
    def __init__(self, session: LexisSession, 
                 print_content: Optional[bool]=False,
                 suppress_print: Optional[bool]=True) -> None:
        self.session = session
        self.print_content = print_content
        self.suppress_print = suppress_print


    def get_workflows_list(self,
                           content_as_pandas: Optional[bool]=False) -> tuple[list[dict] | DataFrame | None, int] | tuple[None, None]:
        """
            Get list of existing workflows (DAGs).

            Parameters
            ----------
            content_as_pandas : bool, optional
                Convert HTTP response content from JSON to pandas DataFrame. By default: content_as_pandas=False.

            Returns
            -------
            list[dict] | DataFrame | None
                List of workflows as list of JSON dictionaries or pandas DataFrame if 'content_as_pandas=True'.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Retrieving list of existing workflows (DAGs)...")

        url: str = self.session.API_AIR + "/dags"

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        self.session.check_token()
        while not status_solved:
            response: Response = get(url, headers=self.session.API_HEADER, verify=False)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        

        if not is_error and content_as_pandas:
            content = convert_list_of_objects_to_pandas(self.session, 
                                                        content["dags"], 
                                                        supress_print=self.suppress_print)

            if content is None:
                is_error = True
                self.session.logging.error(f"GET -- {url} -- CONVERT TO DATAFRAME -- FAILED")
            else:
                self.session.logging.debug(f"GET -- {url} -- CONVERT TO DATAFRAME -- OK")    
                
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving list of workflows (DAGs). See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving list of workflows (DAGs). See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                    print("List of workflows (DAGs) successfully retrieved (and converted) -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code



    def get_workflow_info(self, 
                          workflow_id: str) -> tuple[dict | None, int] | tuple[None, None]:
        """
            Get info of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow.                

            Returns
            -------
            dict | None
                Info about existing workflow (DAG) as dictionary.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Retrieving info about existing workflow (DAG) by its ID...")

        url: str = self.session.API_AIR + "/dags" + "/" + workflow_id

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        self.session.check_token()
        while not status_solved:
            response: Response = get(url, headers=self.session.API_HEADER, verify=False)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        

        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving info about existing workflow (DAG) by its ID. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving info about existing workflow (DAG) by its ID. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                    print("Info about existing workflow (DAG) successfully retrieved -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code
        

    def get_workflow_details(self, 
                             workflow_id: str) -> tuple[dict, int] | tuple[None, None]:
        """
            Get details of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow. 

            Returns
            -------
            dict | None
                Details of existing workflow as dictionary.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Retrieving details of existing workflow (DAG) by its ID...")

        url: str = self.session.API_AIR + "/dags" + "/" + workflow_id + "/details"

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        self.session.check_token()
        while not status_solved:
            response: Response = get(url, headers=self.session.API_HEADER, verify=False)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        
                
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving details of existing workflow (DAG) by its ID. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving details of existing workflow (DAG) by its ID. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                    print("Details of existing workflow (DAG) successfully retrieved -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code


    def get_workflow_params(self, workflow_id: str) -> tuple[dict, int] | tuple[None, None]:
        """
            Get params of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow. 

            Returns
            -------
            dict | None
                Params of existing workflow as dictionary.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Retrieving params of existing workflow (DAG) by its ID...")

        url: str = self.session.API_AIR + "/dags" + "/" + workflow_id + "/details"

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        self.session.check_token()
        while not status_solved:
            response: Response = get(url, headers=self.session.API_HEADER, verify=False)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        
       
                
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving params of existing workflow (DAG) by its ID. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving params of existing workflow (DAG) by its ID. See log file, please.")
            return None, None
        else:
            params: dict = content["params"]
            wf_default_parameters: dict = dict()

            for key in params.keys():
                wf_default_parameters[key] = params[key]["value"]

            if not self.suppress_print:
                    print("Params of existing workflow (DAG) successfully retrieved -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return wf_default_parameters, response.status_code


    def execute_workflow(self, workflow_id: str, workflow_parameters: dict, workflow_run_id: Optional[str | None]=None) -> tuple[dict, int] | tuple[None, None]:
        """
            Get state of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow.
            workflow_parameters: dict
                Parameters of the existing workflow (DAG) as dictionary.
            workflow_run_id: str | None, optional
                Workflow run id (dag_run_id). 

            Returns
            -------
            dict | None
                Response as dictionary.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Executing existing workflow (DAG) by its ID...")

        url: str = self.session.API_AIR + "/dags" + "/" +  workflow_id + "/dagRuns"

        if workflow_run_id is None:
            workflow_run_id: str = "py4lexis_exec_" + datetime.now().isoformat() + "_" + str(round(random() * 100))

        workflow_parameters["access_token"] = self.session.TOKEN
        workflow_input: dict = {
            "conf": workflow_parameters,
            "dag_run_id": workflow_run_id
        }

        self.session.logging.debug(f"POST -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        self.session.check_token()
        while not status_solved:
            response: Response = post(url, headers=self.session.API_HEADER, json=workflow_input, verify=False)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"POST -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        
            
                
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while executing existing workflow (DAG) by its ID. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while executing existing workflow (DAG) by its ID. See log file, please.")
            return None, None
        else:

            content_out = {
                "status": response.status_code,
                "workflow_id": content["dag_id"],
                "workflow_run_id": content["dag_run_id"],
                "State": content["state"]
            }

            if not self.suppress_print:
                    print("Execute the existing workflow (DAG) was successfull -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return content_out, response.status_code
        

    def get_workflow_states(self, workflow_id: str) -> tuple[list[dict], int] | tuple[None, None]:
        """
            Get run states of existing workflow (DAG) selected by its workflow ID (dag_id).

            Parameters
            ----------
            workflow_id: str
                Workflow ID (dag_id) of the existing workflow. 

            Returns
            -------
            dict | None
                Run states of existing workflow as list of dictionaries.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Retrieving state of existing workflow (DAG) by its ID...")

        url: str = self.session.API_AIR + "/dags" + "/" +  workflow_id + "/dagRuns"

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        self.session.check_token()
        while not status_solved:
            response: Response = get(url, headers=self.session.API_HEADER, verify=False)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        
                
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving run states of existing workflow (DAG) by its ID. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving run states of existing workflow (DAG) by its ID. See log file, please.")
            return None, None
        else:
            
            workflow_states: list[dict] = {
                'dag_runs': []
            }
            for dag in content['dag_runs']:
                exec_time: datetime = parser.parse(dag["execution_date"])

                workflow_state: dict = {
                    "workflow_run_id": dag["dag_run_id"],
                    "execution_date": exec_time.ctime(),
                    "state": dag["state"]
                }

                workflow_states["dag_runs"].append(workflow_state)

            if not self.suppress_print:
                    print("Run states of existing workflow (DAG) successfully retrieved -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return workflow_states, response.status_code