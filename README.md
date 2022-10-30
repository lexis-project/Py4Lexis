# py4lexis

Package py4lexis provides functions to manage datasets within the LEXIS platform. 
Uploading datasets' files is performed by tuspy Python package, i.e. by TUS Client.

## Install
We recommend to use virtual environment, e.g.:
      
      python -m venv ./venv

With activated virtual environment, type:

      python -m pip install py4lexis

## Initialise the connection
To initialise the connection with the LEXIS server, use:

    from py4lexis.session import LexisSession
    p4l = LexisSession(PATH_TO_CONFIG_TOML, PATH_TO_LOG_FILE)

where **PATH_TO_CONFIG_TOML** is **path_to_your/config.toml** sent by LEXIS administrator, and **PATH_TO_LOG_FILE** is **path_to_your/log_file.toml**.
By default, **PATH_TO_LOG_FILE = './lexis_logs.log'**.

## Classes to manage datasets
There are two possible ways to manage datasets in py4lexis package. The first is to use

      from py4lexis.ddi.datasets import Datasets
      datasets = Datasets(LEXIS_SESSION)

and the second is to use

      from py4lexis.cli.datasets import DatasetsCLI
      datasets = DatasetsCLI(LEXIS_SESSION)

Both provide same functions, but the **DatasetsCLI** acts in interactive mode with prints to the console. Moreover, using **DatasetsCLI** no return values are passed from
the existing functions.

## Functions in Datasets (DatasetsCLI) class     
### Create dataset

    datasets.create_dataset(access, project, push_method=None, path=None, contributor=None, creator=None,
                            owner=None, publicationYear=None, publisher=None, resourceType=None, title=None)


   Creates an empty dataset with specified attributes.

   #### Parameters
   * access : string. One of the access types [public, project, user]
   * project: string. Project's short name.
   * push_method: string (optional). By default: push_mehtod = "empty"
   * path: string (optional). By default, root path is set, i.e. './'
   * contributor: list of strings (optional). By default: ["UNKNOWN contributor"]
   * creator: list of strings (optional). By default: ["UNKNOWN creator"]
   * owner: list of strings (optional). By default: ["UNKNOWN owner"]
   * publicationYear: string (optional). By default: current year
   * publisher: list of strings (optional). By default: ["UNKNOWN publisher"]
   * resourceType: string (optional). By default: "UNKNOWN resource type"
   * title: string (optional). By default: "UNTITLED_Dataset_" + timestamp

   #### Return
   * content : string. Content of HTTP request in JSON.
   * req_status : integer. Status code of HTTP request.
                          
### Upload a file

    datasets.tus_client_uploader(access, project, filename, file_path=None, path=None, contributor=None, creator=None,
                                 owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                                 expand=None, encryption=None)

   Upload a data by TUS client to the specified dataset.

   #### Parameters
   * access : string. One of the access types [public, project, user]
   * project: string. Project's short name.
   * filename: string. Name of a file to be uploaded
   * file_path: string. Path to a file in user's machine
   * path: string (optional). By default, root path is set, i.e. './'.
   * contributor: list of strings (optional). By default: ["UNKNOWN contributor"]
   * creator: list of strings (optional). By default: ["UNKNOWN creator"]
   * owner: list of strings (optional). By default: ["UNKNOWN owner"]
   * publicationYear: string (optional). By default: current year
   * publisher: list of strings (optional). By default: ["UNKNOWN publisher"]
   * resourceType: string (optional). By default: "UNKNOWN resource type"
   * title: string (optional). By default: "UNTITLED_Dataset_" + timestamp
   * expand: string (optional). By default: "no"
   * encryption: string (optional). By default: "no"


### Check datasets status
To check datasets status, use:

    datasets.get_dataset_status()

   #### Return
   * content : string. Content of HTTP request in JSON.
   * req_status : integer. Status code of HTTP request.

### Get existing datasets
To print basic information of all existing datasets, use:

    datasets.get_all_datasets()
    
   #### Return
   * content : string. Content of HTTP request in JSON.
   * req_status : integer. Status code of HTTP request.

### Delete a dataset
To delete an existing dataset, use:

    datasets.delete_dataset_by_id(internal_id, access, project)

   #### Parameters
   * internal_id: string. InternalID of the dataset. InternalID could be retrieved e.g. by command above.
   * access : string. One of the access types [public, project, user]
   * project: string. Project's short name.

   #### Return
   * content : string. Content of HTTP request in JSON.
   * req_status : integer. Status code of HTTP request.
