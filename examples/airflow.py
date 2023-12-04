from py4lexis.session import LexisSession
from py4lexis.workflows.airflow import Airflow
from py4lexis.cli.airflow import AirflowCLI

"""
    Example file how to use Py4Lexis via CLI to manage Airflow workflows
"""

# Init session with username/password via LEXIS login page
session = LexisSession(login_method="browser") # Also could be used "password" method by inserting LEXIS (only!) credentials into console/terminal

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage Airflow Workflows
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Init Airflow Manager
# airflow = Airflow(session)  # Core class to manage workflows. Functions return content of requests.
airflow = AirflowCLI(session)  # Interactive class to manage datasets. Functions mostly return None because they print content to console/terminal. Only get_workflow_params returns the content.

# Get list of all existing workflows
airflow.get_workflows_list()

# Get info about existing workflow
airflow.get_workflow_info(workflow_id="WORKFLOW_(DAG)_ID")

# Get details of existing workflow
airflow.get_workflow_details(workflow_id="WORKFLOW_(DAG)_ID")

# Get workflow's parameters (it prints the content and also returns it)
wrf_params = airflow.get_workflow_params(workflow_id="WORKFLOW_(DAG)_ID")

# Execute workflow
airflow.execute_workflow(workflow_id="WORKFLOW_(DAG)_ID", 
                         workflow_parameters=wrf_params)

# Get workflows' state
airflow.get_workflow_states(workflow_id="WORKFLOW_(DAG)_ID",
                            filter_by_workflow_state="running") # Can be also filtered by Workflow Run ID