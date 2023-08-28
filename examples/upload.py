from py4lexis.session import LexisSession
from py4lexis.cli.datasets import DatasetsCLI

"""
    Example file how to use Py4Lexis via CLI
"""

# Init session with config file
session = LexisSession(config_file="config.toml")

# Get Datasets manager
ds = DatasetsCLI(session)

# Create new dataset
#new_ds = ds.create_dataset("project", "demoproject", "empty")

# Create new dataset and upload file into it
ds.tus_uploader_new("project", "demoproject", "test10M.dat", title=["TUS TEST 2808"])

# Add new directory tree of files to existing dataset or rewrite existing files in such directory tree
'''
ds.tus_uploader_rewrite(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81",
                        dataset_title="UNTITLED_TUS_Dataset_10-08-2023_10:22:39",
                        project="demoproject", 
                        access="project",
                        filename="first.tar.gz",
                        zone="IT4ILexisZone")
'''

# Get status of files being uploaded to datasets
ds.get_dataset_status(filter_project="demoproject")

# List all datasets
# dsets = ds.get_all_datasets(filter_access="project", filter_project="demoproject")

# Download Dataset
# ds.download_dataset(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone")

# List all files in dataset as ASCII directory tree
# ds.get_list_of_files_in_dataset(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone", print_dir_tree=True)
# List all files in dataset as DataFrame table
# ds.get_list_of_files_in_dataset(dataset_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone", print_dir_tree=False)


