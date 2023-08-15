from py4lexis.session import LexisSession
from py4lexis.cli.datasets import DatasetsCLI

# Init session with config file
session = LexisSession(config_file="config.toml")

# Get Datasets manager
ds = DatasetsCLI(session)

# Create new dataset
new_ds = ds.create_dataset("project", "demoproject", "empty")

# Create new dataset and upload file into it
ds.tus_uploader("project", "demoproject", "test10M.dat")

# Get status of files being uploaded to datasets
ds.get_dataset_status(filter_project="demoproject")

# List all datasets
dsets = ds.get_all_datasets(filter_access="project", filter_project="demoproject")

# List all files in dataset
ds.get_list_of_files_in_dataset(internal_id="0e79c1f6-3757-11ee-885e-fa163e515f81", access="project", project="demoproject", zone="IT4ILexisZone")
#ds.get_list_of_files_in_dataset(internal_id="2e889ce2-2c85-11ee-b219-005056b06219", access="project", project="exa4mind_wp4", zone="IT4ILexisZone")


