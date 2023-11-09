# Py4Lexis

Py4Lexis is a Python package which provides a manager to handle datasets within the LEXIS DDI (upload, download, list existing datasets) 
and a manager to handle workflows in the LEXIS Airflow instance (list existing workflows, execute workflows, check state). 
For both, there exist classes suitable for further processing within a script, and classes that work in interactive mode by printing 
the content of requests and other messages right into the console/terminal. 

Logging in to the LEXIS session is performed via the LEXIS login page. Thus, *B2Access* and *MyAccessID* could be used to log in. Uploading of files (datasets) to the LEXIS DDI is performed
by tuspy Python package, i.e., by TUS Client.

___
## Installation
1. Download the repository.
2. Create a virtual environment within the Py4Lexis repository:
      
      ```
      python -m venv ./venv
      ```
      
   
3. Activate the virtual environment:
      
      ```
      source path/to/Py4Lexis/.venv/bin/activate
      ```      

4. Install the package from root of Py4Lexis repository:

      ```
      python -m pip install .
      ```      
    
  It will install the Py4Lexis package with all dependencies defined in *requirements.txt*.

___
## Initialize LEXIS session
To log in to the LEXIS session, use:
```
session = LexisSession()
```
You will be redirected to the LEXIS login page. *LEXIS username/password*, *B2Access* or *MyAccessID* could be used as credentials.

___
## Manage LEXIS datasets
Example of how to use Py4Lexis to manage datasets within LEXIS DDI. Further information about classes/functions can be found in comments within each class/function.

*NOTE*: It is considered that for given examples below an interactive DatasetCLI class is used. 

### Init datasets manager

Interactive class to manage datasets which functions return `None` because they print content to console/terminal is imported from:
```
from py4lexis.cli import DatasetsCLI
ds = DatasetsCLI(session)
```
*NOTE*: Filter parameters (as shown below) are available only in interactive class DatasetsCLI.

A core class to manage datasets which functions return content of requests (suitable for further processing) could be also used, and is imported from:

```
from py4lexis.ddi import Datasets
core_ds = Datasets(session)
```

### Create new dataset
To create new dataset, use:
```
ds.create_dataset(access="DATASET_ACCESS", 
                  project="PROJECT_SHORTNAME")
```

### Create new dataset and upload file into it
To create new dataset with files upload, use:
```
ds.tus_uploader_new(access="DATASET_ACCESS", 
                    project="PROJECT_SHORTNAME", 
                    filename="FILENAME", 
                    file_path="FILEPATH", 
                    title=["TITLE"])
```


### Upload new files to existing dataset or rewrite existing ones
To rewrite existing files in dataset or to upload new files to it, use:
```
ds.tus_uploader_rewrite(dataset_id="DATASET_INTERNAL_ID",
                        dataset_title="DATASET_TITLE",
                        project="PROJECT_SHORTNAME", 
                        access="ACCESS",
                        filename="FILENAME",
                        file_path="FILEPATH")
```
*NOTE*: DATASET_TITLE have to be same as it is identified by DATASET_INTERNAL_ID.

### Get datasets' upload status
To get datasets' upload status, use:
```
ds.get_dataset_status(filter_project="PROJECT_SHORTNAME")
```
You can also filter the content by filter_filename or filter_task_state (one of ["PENDING", "SUCCESS"]).

*NOTE*: Filters are available only in interactive class.

### List all datasets
To list all existing datasets, use:
```
ds.get_all_datasets(filter_access="DATASET_ACCESS", 
                    filter_project="PROJECT_SHORT_NAME")
```
Can be also filtered by: filter_title, filter_zone.

*NOTE*: Function should be used to obtain datasets' InternalID. Filters are available only
in interactive class.

### Delete dataset
To delete dataset, use:
```
ds.delete_dataset_by_id(dataset_id="DATASETS_INTERNAL_ID", 
                        access="DATASET_ACCESS", 
                        project="PROJECT_SHORT_NAME")
```

### Download Dataset
To download whole dataset, use:
```
ds.download_dataset(dataset_id="DATASET_INTERNAL_ID", 
                    access="DATASET_ACCESS", 
                    project="PROJECT_SHORT_NAME")
```


### List all files in dataset as ASCII directory tree
To list all files in dataset as ASCII directory tree, use:
```
ds.get_list_of_files_in_dataset(dataset_id="DATASET_INTERNAL_ID", 
                                access="DATASET_ACCESS", 
                                project="PROJECT_SHORT_NAME", 
                                print_dir_tree=True)
```
*NOTE*: print_dir_tree is available only in interactive class.


### List all files in dataset as DataFrame table
To list all files in dataset as DataFrame table, use:
```
ds.get_list_of_files_in_dataset(dataset_id="DATASET_INTERNAL_ID", 
                                access="DATASET_ACCESS", 
                                project="PROJECT_SHORT_NAME", 
                                print_dir_tree=False)
```
*NOTE*: print_dir_tree is available only in interactive class.

### Get a dataset path
To obtain dataset's path needed within workflow's transfers, use:
```
ds.get_dataset_path(access="DATASETS_ACCESS", 
                    project="PROJECT_SHORT_NAME", 
                    internalID="DATASET_INTERNAL_ID")
```
___
## Manage Airflow's workflows
Example of how to use Py4Lexis to Airflow's workflows within LEXIS Airflow instance. Further information about classes/functions can be found in comments within each class/function.

*NOTE*: It is considered that for given examples below an interactive AirflowCLI class is used. 

### Init Airflow Manager
Interactive class to manage datasets could be initialised by code below. Functions mostly return `None` because they print content to console/terminal. Only get_workflow_params returns the content.
```
from py4lexis.cli.airflow import AirflowCLI
airflow = AirflowCLI(session)  
```

A core class to manage workflows could be also used. Functions return content of requests which can be processed further within a script.
```
from py4lexis.workflows.airflow import Airflow
core_airflow = Airflow(session)
```

### Get list of all existing workflows
To get a table of all existing workflows, use:
```
airflow.get_workflows_list()
```


### Get info about existing workflow
To get workflow's info, use:
```
airflow.get_workflow_info(workflow_id="WORKFLOW_(DAG)_ID")
```


### Get details of existing workflow
To get more workflow's info, use:
```
airflow.get_workflow_details(workflow_id="WORKFLOW_(DAG)_ID")
```


### Get workflow's parameters
To obtain workflow's paramateres, use:
```
wrf_params = airflow.get_workflow_params(workflow_id="WORKFLOW_(DAG)_ID")
```
*NOTE*: It prints the content to the console/terminal and also returns it as the output. Such an output can be used for executing workflow as shown below.

### Execute workflow
To execute workflow, use:
```
airflow.execute_workflow(workflow_id="WORKFLOW_(DAG)_ID", 
                         workflow_parameters=wrf_params)
```

### Get workflow's states
To get all workflow's run states, use:
```
airflow.get_workflow_states(workflow_id="WORKFLOW_(DAG)_ID",
                            filter_by_workflow_state="running")
```
*NOTE*: Can be also filtered by Workflow Run ID. In that case, filter_by_workflow_state will be ignored. Filters can be only used in interactive class.
___