# Py4Lexis

Py4Lexis provides functions to manage datasets in the LEXIS server. Uploading datasets' files is performed by tuspy Python package, e.g. by TUS Client.

## Install
1. Download files from repository.
2. In the repository folder, open the terminal and type: 

        pip install -r requirements.txt

3. You can use functions from py4Lexis, now.

## Initialise the connection
To initialise the connection with the LEXIS server, use:

    import modules.py4Lexis as pl
    pl_api = pl.LexisSession(username, pwd, keycloak_url, realm, client_id, client_secret, ddi_endpoint_url, zonename)
    
## Available functions
Consider that we initialise connection like in the step "Initialise the connection", e.g. we have defined "pl_api" variable.
### Refresh Keycloak token
To refresh keycloak token, use:

    pl_api.refresh_token()
    
### Create dataset

    pl_api.create_dataset(access, project, push_method=None, path=None, contributor=None, creator=None,
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
   Prints a response content of the POST request.
                          
### Upload a file

    pl_api.tus_client_uploader(access, project, filename, file_path=None, path=None, contributor=None, creator=None,
                               owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                               expand=None, encryption=None)

   Creates a dataset and upload a data by TUS client.

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

   #### Return
   Prints a progress bar of the processing upload.

### Check datasets status
To check datasets status, use:

    pl_api.get_dataset_status()

### Existing datasets
To print basic information of all existing datasets, use:

    get_all_datasets()
    
### Delete a dataset
To delete an existing dataset, use:

    pl_api.delete_dataset_by_id(internal_id, access, project)

#### Parameters
* internal_id: string. InternalID of the dataset
* access : string. One of the access types [public, project, user]
* project: string. Project's short name.

#### Return
Prints a response content of the DELETE request.
