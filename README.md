# Py4Lexis

Py4Lexis provides functions to manage datasets in the LEXIS server. 
Uploading datasets' files is performed by tuspy Python package, i.e. by TUS Client.

## Install
1. Download the repository.
2. Copy retrieved **config.toml** file into the downloaded repository.
3. In the repository folder, open the terminal and type: 
        
        python3 venv -m venv
        source venv/bin/activate
        pip install -r requirements.txt
4. Initialise LEXIS API session as below. 

## Initialise the connection
To initialise the connection with the LEXIS server, use:

    from py4Lexis.init as init_api_session
    p4l_api = init_api_session()

If you have not activated the virtual environment, open the terminal and type:

        source venv/bin/activate

Then, you can initialise connection as above.
    
## Available functions
Assume that we initialise connection like above, i.e. we have defined **p4l_api** object.
### Refresh Keycloak token
To refresh keycloak token, use:

    p4l_api.refresh_token()
    
### Create dataset

    p4l_api.create_dataset(access, project, push_method=None, path=None, contributor=None, creator=None,
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
   Prints a response content of the request.
                          
### Upload a file

    p4l_api.tus_client_uploader(access, project, filename, file_path=None, path=None, contributor=None, creator=None,
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

    p4l_api.get_dataset_status()

   #### Return
   Prints a table of existing datasets with their status.

### Get existing datasets
To print basic information of all existing datasets, use:

    p4l_api.get_all_datasets()
    
   #### Return
   Prints a table of existing datasets with basic information.

### Delete a dataset
To delete an existing dataset, use:

    p4l_api.delete_dataset_by_id(internal_id, access, project)

   #### Parameters
   * internal_id: string. InternalID of the dataset. InternalID could be retrieved e.g. by command above.
   * access : string. One of the access types [public, project, user]
   * project: string. Project's short name.

   #### Return
   Prints a response content of the request.
