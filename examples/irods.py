from py4lexis.session import LexisSession
from py4lexis.irods import iRODS

"""
    Example file how to use Py4Lexis to manage iRODS collections
"""

# Init session with username/password via LEXIS login page
session = LexisSession(login_method="browser") # Also could be used "password" method by inserting LEXIS (only!) credentials into console/terminal

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage iRODS collections
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# get iRODS session
irods = iRODS(session)

# put data object to iRODS
data_object = irods.put_object_to_dataset_collection(local_filepath="LOCAL_FILE_PATH",
                                                    irods_filepath="RELATIVE_FILE_PATH_IN_IRODS",
                                                    access="LEXIS_PROJECT_ACCESS",
                                                    project="LEXIS_PROJECT_SHORTNAME",
                                                    internal_id="DATASET_INTERNAL_ID")

# get data object from iRODS
data_object = irods.put_object_to_dataset_collection(local_filepath="LOCAL_PATH_TO_FILE",
                                                     irods_filepath="RELATIVE_FILE_PATH_IN_IRODS",
                                                     access="LEXIS_PROJECT_ACCESS",
                                                     project="LEXIS_PROJECT_SHORTNAME",
                                                     internal_id="DATASET_INTERNAL_ID")

# create new collection within an existing dataset
collection = irods.create_collection_within_dataset(collection_name="NEW_COLLECTION_NAME",
                                                    access="LEXIS_PROJECT_ACCESS",
                                                    project="LEXIS_PROJECT_SHORTNAME",
                                                    internal_id="DATASET_INTERNAL_ID")

# get LEXIS project as a collection
collection = irods.get_project_collection(access="LEXIS_PROJECT_ACCESS",
                                          project="LEXIS_PROJECT_SHORTNAME")

# get a dataset as a collection
collection = irods.get_dataset_collection(access="LEXIS_PROJECT_ACCESS",
                                          project="LEXIS_PROJECT_SHORTNAME",
                                          internal_id="DATASET_INTERNAL_ID")