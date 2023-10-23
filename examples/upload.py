from py4lexis.session import LexisSession
from py4lexis.cli.datasets import DatasetsCLI
from py4lexis.workflows.airflow import Airflow

"""
    Example file how to use Py4Lexis via CLI
"""

# Init session with username/password as user input
session = LexisSession()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage LEXIS datasets
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get Datasets manager
# ds = DatasetsCLI(session)

# Create new dataset
# new_ds = ds.create_dataset("project", "demoproject", "empty")

# Create new dataset and upload file into it
# ds.tus_uploader_new("project", "demoproject", "test10M.dat", title=["TUS TEST 1509"])

# Add new directory tree of files to existing dataset or rewrite existing files in such directory tree
'''
ds.tus_uploader_rewrite(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81",
                        dataset_title="UNTITLED_TUS_Dataset_10-08-2023_10:22:39",
                        project="demoproject", 
                        access="project",
                        filename="first.tar.gz",
                        zone="IT4ILexisZone")
'''

'''
ds.tus_uploader_rewrite(dataset_id="c7b4a0f6-539e-11ee-ba21-fa163e515f81",
                        dataset_title="TUS TEST 1509",
                        project="demoproject", 
                        access="project",
                        filename="test10M.dat",
                        zone="IT4ILexisZone")
'''

'''
ds.tus_uploader_rewrite(dataset_id="c7b4a0f6-539e-11ee-ba21-fa163e515f81",
                        dataset_title="TUS TEST 1509",
                        project="demoproject", 
                        access="project",
                        filename="test_text2.txt",
                        zone="IT4ILexisZone")
'''

# Get status of files being uploaded to datasets
# ds.get_dataset_status(filter_project="demoproject")

# List all datasets
# dsets = ds.get_all_datasets(filter_access="project", filter_project="demoproject")

# Download Dataset
# ds.download_dataset(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone")

# List all files in dataset as ASCII directory tree
# ds.get_list_of_files_in_dataset(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone", print_dir_tree=True)

# List all files in dataset as DataFrame table
# ds.get_list_of_files_in_dataset(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone", print_dir_tree=False)
#ds.get_list_of_files_in_dataset(dataset_id="c7b4a0f6-539e-11ee-ba21-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone", print_dir_tree=False)

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage Airflow Workflows
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Init Airflow Manager
airflow = Airflow(session)

# Get list of All Existing Workflows
wrfs, _ = airflow.get_workflows_list()
print(f"Workflows:\n{wrfs}\n\n\n")

# Get info about existing Workflow
wrf_info, _ = airflow.get_workflow_info("compbiomed_dynamic_select_v2")
print(f"Info:\n{wrf_info}\n\n\n")

# Get details of existing workflow
wrf_detail, _ = airflow.get_workflow_details("compbiomed_dynamic_select_v2")
print(f"Detail:\n{wrf_detail}\n\n\n")

# Get Workflow Params
wrf_params, _ = airflow.get_workflow_params("compbiomed_dynamic_select_v2")
print(f"Params:\n{wrf_params}\n\n\n")

# Execute Workflow
post_response, _ = airflow.execute_workflow("compbiomed_dynamic_select_v2", wrf_params)
print(f"Response:\n{post_response}\n\n\n")

# Get Workflows' state
wrf_states, _ = airflow.get_workflow_states("compbiomed_dynamic_select_v2")
print(f"States:\n{wrf_states}\n\n\n")
