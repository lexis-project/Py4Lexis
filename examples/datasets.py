from py4lexis.session import LexisSession
from py4lexis.cli.datasets import DatasetsCLI

"""
    Example file how to use Py4Lexis via CLI to manage datasets
"""

# Init session with username/password as user input
session = LexisSession()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Manage LEXIS datasets
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get Datasets manager
ds = DatasetsCLI(session)

# Create new dataset
new_ds = ds.create_dataset("ACCESS_HERE", "PROJECT_SHORTNAME_HERE")

# Create new dataset and upload file into it
ds.tus_uploader_new("ACCESS_HERE", "PROJECT_SHORTNAME_HERE", "FILENAME_HERE", file_path="FILEPATH_HERE", title=["TITLE_HERE"])

# Add new directory tree of files to existing dataset or rewrite existing files in such directory tree
ds.tus_uploader_rewrite(dataset_id="DATASET_UUID_HERE",
                        dataset_title="DATASET_TITLE", # Dataset title have to be same as it is identified by DATASET_UUID
                        project="PROJECT_SHORTNAME_HERE", 
                        access="ACCESS_HERE",
                        filename="FILENAME_HERE",
                        file_path="FILEPATH_HERE")


# Get status of files being uploaded to datasets
ds.get_dataset_status(filter_project="PROJECT_SHORTNAME_HERE")

# List all datasets
dsets = ds.get_all_datasets(filter_access="ACCESS_HERE", filter_project="PROJECT_SHORTNAME_HERE")

# Download Dataset
ds.download_dataset(dataset_id="DATASET_UUID_HERE", access="ACCESS_HERE", project="PROJECT_SHORTNAME_HERE")

# List all files in dataset as ASCII directory tree
ds.get_list_of_files_in_dataset(dataset_id="DATASET_UUID_HERE", access="ACCESS_HERE", project="PROJECT_SHORTNAME_HERE", print_dir_tree=True)

# List all files in dataset as DataFrame table
ds.get_list_of_files_in_dataset(dataset_id="DATASET_UUID_HERE", access="ACCESS_HERE", project="PROJECT_SHORTNAME_HERE", print_dir_tree=False)