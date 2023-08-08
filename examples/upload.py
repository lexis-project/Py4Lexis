from py4lexis.session import LexisSession
from py4lexis.cli.datasets import DatasetsCLI

# Init session with config file
session = LexisSession(config_file='config.toml')
session.token_refresh()

# Get Datasets manager
ds = DatasetsCLI(session)

# new_ds = ds.create_dataset("project", "demoproject", "empty")

# List all datasets
dsets = ds.get_all_datasets(filter_access="project", filter_project="demoproject")
# print(dsets[0])


# Upload dataset
'''
ds.tus_uploader('project', 'demoproject', 'test10M.dat', 
                file_path=None, path=None, contributor=None, creator=None,
                owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                expand=None, encryption=None)

ds.get_dataset_status()
'''