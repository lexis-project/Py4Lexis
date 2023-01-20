from py4lexis.session import LexisSession
from py4lexis.ddi.datasets import Datasets

# Init session with config file
session = LexisSession(config_file='config.toml')
session.token_refresh()

# Get Datasets manager
ds = Datasets(session)

# List all datasets
dsets = ds.get_all_datasets()
print(dsets[0])


# Upload dataset
ds.tus_uploader('project', 'demoproject', 'test.dat', 
                    file_path=None, path=None, contributor=None, creator=None,
                    owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                    expand=None, encryption=None)

ds.get_dataset_status()