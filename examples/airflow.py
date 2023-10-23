from py4lexis.session import LexisSession
from py4lexis.workflows.airflow import Airflow

"""
    Example file how to use Py4Lexis via CLI to manage Airflow workflows
"""

# Init session with username/password as user input
session = LexisSession()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage Airflow Workflows
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Init Airflow Manager
airflow = Airflow(session)

# Get list of All Existing Workflows
wrfs, _ = airflow.get_workflows_list()

# Get info about existing Workflow
wrf_info, _ = airflow.get_workflow_info("DAG_ID_HERE")

# Get details of existing workflow
wrf_detail, _ = airflow.get_workflow_details("DAG_ID_HERE")

# Get Workflow Params
wrf_params, _ = airflow.get_workflow_params("DAG_ID_HERE")

# Execute Workflow
post_response, _ = airflow.execute_workflow("DAG_ID_HERE", wrf_params)

# Get Workflows' state
wrf_states, _ = airflow.get_workflow_states("DAG_ID_HERE")