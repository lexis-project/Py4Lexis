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
    pl_api = pl.LexisSession(username, password)
    
## Available functions
Consider that we initialise connection like in the step "Initialise the connection", e.g. we have defined "pl_api" variable.
### Refresh Keycloak token
To refresh keycloak token, use:

    pl_api.refresh_token()
    
### Create dataset
To create an empty dataset with specified attributes, use:

    pl_api.create_dataset(access, project, push_method=None, path=None, contributor=None, creator=None,
                          owner=None, publicationYear=None, publisher=None, resourceType=None, title=None)
                          
### Upload a file
To upload a file by TUS Client, use:

    pl_api.tus_client_uploader(access, project, filename, file_path=None, path=None, contributor=None, creator=None,
                               owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                               expand=None, encryption=None)
                              
### Check datasets status
To check datasets status, use:

    pl_api.get_dataset_status()

### Existing datasets
To print basic information of all existing datasets, use:

    get_all_datasets()
    
### Delete a dataset
To delete an existing dataset, use:

    pl_api.delete_dataset_by_id(internal_id, access, project)
