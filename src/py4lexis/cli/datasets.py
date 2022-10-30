# datasets.py

import json
from py4lexis.ddi.datasets import Datasets

# Making ASCII table
# Source: https://stackoverflow.com/questions/5909873/how-can-i-pretty-print-ascii-tables-with-python
from tabulate import tabulate


class DatasetsCLI:
    def __init__(self, session):
        """
            A class holds methods to manage datasets within LEXIS platform using INTERACTIVE mode.

            Attributes
            ----------
            session : class, LEXIS session

            Methods
            -------
            create_dataset(access, project, push_method=None, path=None, contributor=None, creator=None,
                            owner=None, publicationYear=None, publisher=None, resourceType=None, title=None)
                Create an empty dataset with specified attributes.

            tus_uploader(access, project, filename, file_path=None, path=None, contributor=None, creator=None,
                         owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                         expand=None, encryption=None)
                Create a dataset and upload a data by TUS client.

            get_dataset_status()
                Prints a table of the datasets' staging states.

            get_all_dataset()
                Prints a table of the all existing datasets.

            delete_dataset_by_id(internal_id, access, project)
                Deletes a dataset by a specified internalID.
        """
        self.session = session
        self.datasets = Datasets(session)

    def create_dataset(self, access, project, push_method=None, path=None, contributor=None, creator=None,
                       owner=None, publicationYear=None, publisher=None, resourceType=None, title=None):
        """
            Creates an empty dataset with specified attributes

            Parameters
            ----------
            access : str
                One of the access types [public, project, user]
            project: str
                Project's short name.
            push_method: str, optional
                By default: push_mehtod = "empty"
            path: str, optional
                By default, root path is set, i.e. './'
            contributor: list (str), optional
                By default: ["UNKNOWN contributor"]
            creator: list (str), optional
                By default: ["UNKNOWN creator"]
            owner: list (str), optional
                By default: ["UNKNOWN owner"]
            publicationYear: str, optional
                By default: current year
            publisher: list (str), optional
                By default: ["UNKNOWN publisher"]
            resourceType: str, optional
                By default: "UNKNOWN resource type"
            title: str, optional
                By default: "UNTITLED_Dataset_" + timestamp
        """

        print(f"Creating dataset with title {title}\n    access:{access}\n    project:{project}\n    push_method:{push_method}")
        content, req_status = self.datasets.create_dataset(access, project, push_method, path, contributor, creator,
                                                           owner, publicationYear, publisher, resourceType, title)
        if 200 <= req_status <= 299:
            print(f"Dataset successfully created...")
        else:
            print(f"Some error occurred while creating dataset. See log file, please.")

        print(f"Printing HTTP request content:")
        print(json.dumps(content, indent=4))

    def tus_uploader(self, access, project, filename, file_path=None, path=None, contributor=None, creator=None,
                     owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                     expand=None, encryption=None):
        """
            Upload a data by TUS client to the specified dataset.

            Parameters
            ----------
            access : str
                One of the access types [public, project, user]
            project: str
                Project's short name.
            filename: str
                Name of a file to be uploaded
            file_path: str
                Path to a file in user's machine
            path: str, optional
                By default, root path is set, i.e. './'.
            contributor: list (str), optional
                By default: ["UNKNOWN contributor"]
            creator: list (str), optional
                By default: ["UNKNOWN creator"]
            owner: list (str), optional
                By default: ["UNKNOWN owner"]
            publicationYear: str, optional
                By default: current year
            publisher: list (str), optional
                By default: ["UNKNOWN publisher"]
            resourceType: str, optional
                By default: "UNKNOWN resource type"
            title: str, optional
                By default: "UNTITLED_Dataset_" + timestamp
            expand: str, optional
                By default: "no"
            encryption: str, optional
                By default: "no"
        """

        print(f"Initialising TUS upload of {filename}\n    access: {access}\n    project: {project}")
        self.datasets.tus_uploader(access, project, filename, file_path, path, contributor, creator, owner, publicationYear,
                                   publisher, resourceType, title, expand, encryption)

    def get_dataset_status(self):
        """
            Prints datasets with status into table
        """

        print(f"Retrieving data of the datasets...")
        content, req_status = self.datasets.get_dataset_status()
        if 200 <= req_status <= 299:
            try:
                print(f"Formatting response into ASCII table...")
                data_table = []
                for i in range(len(content)):
                    data_table.append([content[i]['filename'], content[i]['task_state']])
                print(tabulate(data_table, ["Filename", "State"], tablefmt="grid"))

            except json.decoder.JSONDecodeError:
                print(f"JSON response of 'get_dataset_status()' from 'py4lexis.ddi.datasets' can't be decoded!!!")

            except KeyError as kerr:
                print(f"Wrong or missing key '{kerr}' in JSON response content!!!")
                print(f"Printing HTTP request content:")
                print(json.dumps(content, indent=4))
        else:
            print(f"Some error occurred. See log file, please.")
            print(f"Printing HTTP request content:")
            print(json.dumps(content, indent=4))

    def get_all_datasets(self):
        """
            Prints a table of the all existing datasets.
        """

        print(f"Retrieving data of the datasets...")
        content, req_status = self.datasets.get_all_datasets()
        if 200 <= req_status <= 299:
            try:
                print(f"Formatting response into ASCII table...")
                data_table = []
                for i in range(len(content)):
                    if 'title' in content[i]['metadata']:
                        title = content[i]['metadata']['title']
                    else:
                        title = 'UKNOWN Title'

                    if 'CreationDate' in content[i]['metadata']:
                        date_time = content[i]['metadata']['CreationDate']
                    else:
                        date_time = 'UKNOWN Creation Date'

                    if 'access' in content[i]['location']:
                        access = content[i]['location']['access']
                    else:
                        access = 'UKNOWN Access'

                    if 'project' in content[i]['location']:
                        project = content[i]['location']['project']
                    else:
                        project = 'UKNOWN Project'

                    if 'zone' in content[i]['location']:
                        zone = content[i]['location']['zone']
                    else:
                        zone = 'UKNOWN Zone'

                    if 'internalID' in content[i]['location']:
                        internalID = content[i]['location']['internalID']
                    else:
                        internalID = 'UKNOWN InternalID'

                    data_table.append([title,
                                       date_time,
                                       access,
                                       project,
                                       zone,
                                       internalID])
                print(tabulate(data_table, ["Title", "Creation Date", "Access", "Project", "Zone", "Internal ID"],
                               tablefmt="grid"))

            except json.decoder.JSONDecodeError:
                print(f"JSON response of 'get_all_datasets()' from 'py4lexis.ddi.manage_datasets' can't be decoded!!!")

            except KeyError as kerr:
                print(f"Wrong or missing key '{kerr}' in JSON response content!!!")
                print(f"Printing HTTP request content:")
                print(json.dumps(content, indent=4))
        else:
            print(f"Some error occurred. See log file, please.")
            print(f"Printing HTTP request content:")
            print(json.dumps(content, indent=4))

    def delete_dataset_by_id(self, internal_id, access, project):

        print(f"Deleting dataset with ID: {internal_id}")
        content, req_status = self.datasets.delete_dataset_by_id(internal_id, access, project)

        if 200 <= req_status <= 299:
            print(f"Dataset has been deleted...")
        else:
            print(f"Some error occurred. See log file, please.")
            print(f"Printing HTTP request content:")
            print(json.dumps(content, indent=4))