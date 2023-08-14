from py4lexis.session import LexisSession
from py4lexis.cli.datasets import DatasetsCLI

# Init session with config file
session = LexisSession(config_file="./config.toml")
# session.refresh_token()

# Get Datasets manager
ds = DatasetsCLI(session)

# Create new dataset
# new_ds = ds.create_dataset("project", "demoproject", "empty")

# Create new dataset and upload file into it
# ds.tus_uploader("project", "demoproject", "test10M.dat")

# Get status of files being uploaded to datasets
# ds.get_dataset_status()

# List all datasets
dsets = ds.get_all_datasets(filter_access="project", filter_project="demoproject")
ds.get_list_of_files_in_dataset(internal_id="42d2ca92-90fe-11ed-bb11-fa163e515f81", 
                                access="project",
                                project="EVEREST_Weather", 
                                zone="IT4ILexisZone")


