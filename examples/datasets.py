from py4lexis.session import LexisSession
from py4lexis.ddi.datasets import Datasets
from py4lexis.cli.datasets import DatasetsCLI

"""
    Example file how to use Py4Lexis to manage datasets
"""

# Init session with username/password via LEXIS login page
session = LexisSession(login_method="browser") # Also could be used "password" method by inserting LEXIS (only!) credentials into console/terminal

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage LEXIS datasets
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get Datasets manager
# ds = Datasets(session)  # Core class to manage datasets. Functions return content of requests.
ds = DatasetsCLI(session) # Interactive class to manage datasets. Functions return None because they print content to console/terminal.

# Create new dataset
ds.create_dataset(access="DATASET_ACCESS", 
                  project="PROJECT_SHORT_NAME")

# Create new dataset and upload file into it
ds.tus_uploader_new(access="DATASET_ACCESS", 
                    project="PROJECT_SHORT_NAME", 
                    filename="FILENAME", 
                    file_path="FILE_PATH", 
                    title=["TITLE_HERE"])

# Upload new files to existing dataset or rewrite existing ones
ds.tus_uploader_rewrite(internal_id="DATASET_INTERNAL_ID",
                        dataset_title="DATASET_TITLE", # Dataset title have to be same as it is identified by DATASET_UUID
                        project="PROJECT_SHORT_NAME", 
                        access="ACCESS_HERE",
                        filename="FILENAME",
                        file_path="FILE_PATH")


# Get datasets' upload status
ds.get_dataset_status(filter_project="PROJECT_SHORT_NAME") # Filters are available only in interactive mode


# List all datasets
ds.get_all_datasets(filter_access="DATASET_ACCESS", 
                    filter_project="PROJECT_SHORT_NAME") # Filters are available only in interactive mode


# Delete dataset
ds.delete_dataset_by_id(internal_id="DATASETS_INTERNAL_ID", 
                        access="DATASET_ACCESS", 
                        project="PROJECT_SHORT_NAME")


# Download Dataset
ds.download_dataset(internal_id="DATASET_INTERNAL_ID", 
                    access="DATASET_ACCESS", 
                    project="PROJECT_SHORT_NAME")


# List all files in dataset as ASCII directory tree
ds.get_list_of_files_in_dataset(internal_id="DATASET_INTERNAL_ID", 
                                access="DATASET_ACCESS", 
                                project="PROJECT_SHORT_NAME", 
                                print_dir_tree=True) # print_dir_tree parameter is available only in interactive mode


# List all files in dataset as DataFrame table
ds.get_list_of_files_in_dataset(internal_id="DATASET_INTERNAL_ID", 
                                access="DATASET_ACCESS", 
                                project="PROJECT_SHORT_NAME", 
                                print_dir_tree=False) # print_dir_tree parameter is available only in interactive mode


# Get a dataset path
ds.get_dataset_path(access="DATASET_ACCESS", 
                    project="PROJECT_SHORT_NAME", 
                    internal_id="DATASETS_INTERNAL_ID")