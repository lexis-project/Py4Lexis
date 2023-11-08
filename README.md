# Py4Lexis

Py4Lexis is a Python package which provides a manager to handle datasets within the LEXIS DDI (upload, download, list existing datasets) 
and a manager to handle workflows in the LEXIS Airflow instance (list existing workflows, execute workflows, check state). 
For both, there exist classes suitable for further processing within a script, and classes that work in interactive mode by printing 
the content of requests and other messages right into the console/terminal. 

Logging in to the LEXIS session is performed via the LEXIS login page. Thus, B2Access and MyAccessID could be used to log in. Uploading of files (datasets) to the LEXIS DDI is performed
by tuspy Python package, i.e., by TUS Client.

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

4. Install the package from root of py4lexis repository:

      ```
      python -m pip install .
      ```      
    
  It will install the Py4Lexis package with all dependencies defined in *requirements.txt*.

## Initialize LEXIS session
To log in to the LEXIS session, use:
```
session = LexisSession()
```
You will be redirected to the LEXIS login page. *LEXIS username/password*, *B2Access* or *MyAccessID* could be used as credentials.

## Manage LEXIS datasets
Example of how to use Py4Lexis to manage datasets within LEXIS DDI. Further information about classes/functions can be found in comments within each class/function.

*NOTE*: It is considered that for given examples below an interactive DatasetCLI class is used. 

### Get Datasets manager
Core class to manage datasets which functions return content of requests is imported from:
```
from py4lexis.ddi import Datasets
ds = Datasets(session)
```

Interactive class to manage datasets which functions return nothing because they print content to console/terminal is imported from:
```
from py4lexis.cli import DatasetsCLI
ds = DatasetsCLI(session)
```
*NOTE*: Filter parameters are available only in interactive class DatasetsCLI.

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
You can also filter the content by filter_filename or filter_task_state ("PENDING", "SUCCESS").

*NOTE*: Filters are available only in interactive mode.

### List all datasets
To list all existing datasets, use:
```
ds.get_all_datasets(filter_access="DATASET_ACCESS", 
                    filter_project="PROJECT_SHORT_NAME")
```
Can be also filtered by: filter_title, filter_zone.

*NOTE*: Function should be used to obtain datasets' InternalID. Filters are available only
in interactive mode.

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
*NOTE*: print_dir_tree is available only in interactive mode.


### List all files in dataset as DataFrame table
To list all files in dataset as DataFrame table, use:
```
ds.get_list_of_files_in_dataset(dataset_id="DATASET_INTERNAL_ID", 
                                access="DATASET_ACCESS", 
                                project="PROJECT_SHORT_NAME", 
                                print_dir_tree=False)
```
*NOTE*: print_dir_tree is available only in interactive mode.

### Get a dataset path
To obtain dataset's path needed within workflow's transfers, use:
```
ds.get_dataset_path(access="DATASETS_ACCESS", 
                    project="PROJECT_SHORT_NAME", 
                    internalID="DATASET_INTERNAL_ID")
```