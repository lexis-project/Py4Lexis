# datasets.py

import json
from py4lexis.session import LexisSession
from py4lexis.ddi.datasets import Datasets
from py4lexis.exceptions import Py4LexisException
from py4lexis.utils import printProgressBar
from datetime import date, datetime
from pandas import DataFrame
import time
# Making ASCII table
# Source: https://stackoverflow.com/questions/5909873/how-can-i-pretty-print-ascii-tables-with-python
from tabulate import tabulate


class DatasetsCLI:
    def __init__(self, session: LexisSession):
        """
            A class holds methods to manage datasets within LEXIS platform using INTERACTIVE mode.

            Attributes
            ----------
            session : class, LEXIS session

            Methods
            -------
            create_dataset(access: str, project: str, push_method: str | None=None, path: str | None=None,
                           contributor: list[str] | None=None, creator: list[str] | None=None, owner: list[str] | None=None,
                           publicationYear: str | None=None, publisher: list[str] | None=None, resourceType: str | None=None,
                           title: str | None=None) -> None
                Create an empty dataset with specified attributes.

            tus_uploader(access: str, project: str, filename: str, file_path: str | None=None,
                         path: str | None=None, contributor: list[str] | None=None, creator: list[str] | None=None,
                         owner: list[str] | None=None, publicationYear: str | None=None, publisher: list[str] | None=None,
                         resourceType: str | None=None, title: str | None=None,
                         expand: str | None=None, encryption: str | None=None) -> None
                Create a dataset and upload a data by TUS client.

            get_dataset_status()
                Prints a table of the datasets' staging states.

            get_all_datasets(filter_title: str=None, filter_access: str=None, 
                             filter_project: str=None, filter_zone: str=None) -> None
                Prints a table of the all existing datasets. Is possible to use filters to get filtered table by
                title, access, zone, project.

            delete_dataset_by_id(internal_id, access, project)
                Deletes a dataset by a specified internalID.

            download_dataset(acccess: str, project: str, internal_id: str, zone:str,
                             path: str = "", destination_file: str = "./download.tar.gz") -> None
                Downloads dataset by a specified informtions as access, zone, project, InternalID.
                It is possible to specify by path parameter which exact file in the dataset should be downloaded.
                It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"
        """
        self.session: LexisSession = session
        self.datasets: Datasets = Datasets(session, suppress_print=False)

    def create_dataset(self, access: str, project: str, push_method: str | None=None, zone: str | None = None, path: str | None=None,
                       contributor: list[str] | None=None, creator: list[str] | None=None, owner: list[str] | None=None,
                       publicationYear: str | None=None, publisher: list[str] | None=None, resourceType: str | None=None,
                       title: str | None=None) -> None:
        """
            Creates an empty dataset with specified attributes

            Parameters
            ----------
            access: str
                One of the access types [public, project, user]
            project: str
                Project's short name.
            push_method: str | None, optional
                By default: push_mehtod='empty', if set to None.
            zone: str | None, optional
                iRODS zone name, one of ['IT4ILexisZone', 'LRZLexisZone']. By default: 'IT4ILexisZone', if set to None.
            path: str | None, optional
                By default, root path is set, i.e. './', if set to None.
            contributor: list[str] | None, optional
                By default: ["UNKNOWN contributor"], if set to None.
            creator: list[str] | None, optional
                By default: ["UNKNOWN creator"], if set to None.
            owner: list[str] | None, optional
                By default: ["UNKNOWN owner"], if set to None.
            publicationYear: str | None, optional
                By default: CURRENT_YEAR, if set to None.
            publisher: list[str] | None, optional
                By default: ["UNKNOWN publisher"], if set to None.
            resourceType: str | None, optional
                By default: "UNKNOWN resource type", if set to None.
            title: str | None, optional
                By default: "UNTITLED_Dataset_" + TIMESTAMP, if set to None.
            
            Return
            ------
            None
        """

        print(f"Creating dataset with title {title}\n"+ 
              f" access:{access}\n"+
              f" project:{project}\n"+
              f" push_method:{push_method}\n"+
              f" zone:{zone}")
        content, req_status = self.datasets.create_dataset(access, project, push_method, zone, path, contributor, creator,
                                                           owner, publicationYear, publisher, resourceType, title)
        if 200 <= req_status <= 299:
            print(f"Dataset successfully created...")
        else:
            print(f"Some error occurred while creating dataset. See log file, please.")

        print(f"Printing HTTP request content:")
        print(json.dumps(content, indent=4))

    def tus_uploader(self, access: str, project: str, filename: str, 
                     zone: str="IT4ILexisZone", 
                     file_path: str="./",
                     path: str="", 
                     contributor: list[str]=["NONAME contributor"], 
                     creator: list[str]=["NONAME creator"],
                     owner: list[str]=["NONAME owner"], 
                     publicationYear: str=str(date.today().year), 
                     publisher: list[str]=["NONAME publisher"],
                     resourceType: str="NONAME resource type", 
                     title: str="UNTITLED_TUS_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"), 
                     expand: str="no", 
                     encryption: str="no") -> None:
        """
            Upload a data by TUS client to the specified dataset.

            Parameters
            ----------
            access : str
                One of the access types [public, project, user].
            project: str
                Project's short name.
            filename: str
                Name of a file to be uploaded.
            zone: str | None, optional
                iRODS zone name, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone".
            file_path: str | None, optional,
                Path to a file in user's machine. By default "./".
            path: str, optional
                By default, root path is set, i.e. "./".
            contributor: list[str], optional
                By default: ["UNKNOWN contributor"].
            creator: list[str], optional
                By default: ["UNKNOWN creator"].
            owner: list[str], optional
                By default: ["UNKNOWN owner"].
            publicationYear: str, optional
                By default: CURRENT_YEAR.
            publisher: list[str], optional
                By default: ["UNKNOWN publisher"].
            resourceType: str, optional
                By default: "UNKNOWN resource type".
            title: str, optional
                By default: "UNTITLED_Dataset_" + TIMESTAMP.
            expand: str, optional
                By default: "no".
            encryption: str, optional
                By default: "no".

            Returns
            -------
            None
        """

        print(f"Initialising TUS upload of {filename}\n    access: {access}\n    project: {project}")
        self.datasets.tus_uploader(access, project, filename, zone, file_path, path, contributor, creator, owner, publicationYear,
                                   publisher, resourceType, title, expand, encryption)

    def get_dataset_status(self, filter_filename: str="", filter_project: str="", filter_task_state=""):
        """
            Prints datasets with status into table. Is possible to use filters to get filtered table by
            filename, project, task_status.

            Parameters
            ----------
            filter_filename : str, optional
                To filter table of datasets by filename. By default: "".
            filter_project : str, optional
                To filter table of datasets by project. By default: "".
            filter_task_state : str, optional
                To filter table of datasets by task_state. One of ["PENDING", "SUCCESS"]. By default: "".
        """

        print(f"Retrieving data of the datasets...")
        content, req_status = self.datasets.get_dataset_status()
        if 200 <= req_status <= 299:
            try:
                print(f"Formatting response into ASCII table...")
                
                cols: list[str] = ["Filename", "Project", "TaskState", "TaskResult", "TransferType"]
                datasets_table: DataFrame = content[cols]

                if filter_filename != "":
                    datasets_table = datasets_table[datasets_table["Filename"] == filter_filename]

                if filter_project != "":
                    datasets_table = datasets_table[datasets_table["Project"] == filter_project]

                if filter_task_state != "":
                    datasets_table = datasets_table[datasets_table["TaskState"] == filter_task_state]

                print(tabulate(datasets_table.values.tolist(), cols, tablefmt="grid"))

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

    def get_all_datasets(self, filter_title: str="", filter_access: str="", 
                         filter_project: str="", filter_zone: str="") -> None:
        """
            Prints a table of the all existing datasets. Is possible to use filters to get filtered table by
            title, access, zone, project.

            Parameters
            ----------
            filter_title : str, optional
                To filter table of datasets by title. By default: "".
            filter_access : str, optional
                To filter table of datasets by access. By default: "".
            filter_project : str, optional
                To filter table of datasets by project. By default: "".
            filter_zone : str, optional
                To filter table of datasets by title. By default: "".
        """

        print(f"Retrieving data of the datasets...")
        content, req_status = self.datasets.get_all_datasets(content_as_pandas=True)
        if 200 <= req_status <= 299:
            try:
                print(f"Formatting pandas DataFrame from response into ASCII table...")
                
                cols: list[str] = ["Title", "Access", "Project", "Zone", "InternalID", "CreationDate"]
                datasets_table: DataFrame = content[cols]

                if filter_title is not None:
                    datasets_table = datasets_table[datasets_table["Title"] == filter_title]
                
                if filter_access is not None:
                    datasets_table = datasets_table[datasets_table["Access"] == filter_access]

                if filter_project is not None:
                    datasets_table = datasets_table[datasets_table["Project"] == filter_project]

                if filter_zone is not None:
                    datasets_table = datasets_table[datasets_table["Zone"] == filter_zone]

                print(tabulate(datasets_table.values.tolist(), cols, tablefmt="grid"))

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


    def download_dataset(self, acccess: str, project: str, internal_id: str, zone:str,
                         path: str = "", destination_file: str = "./download.tar.gz") -> None:
        """
            Downloads dataset by a specified information as access, zone, project, InternalID.
            It is possible to specify by path parameter which exact file in the dataset should be downloaded.
            It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"
            
            Parameters
            ----------
            access : str
                One of the access types [public, project, user]
            project: str
                Project's short name.
            internal_id: str
                InternalID of the dataset.
            zone: str
                iRODS zone name.
            path: str, optional
                Path to exact folder, by default = "".
            destination_file: str, optional
                Paht to the local destination folder, by default "./download.tar.gz"

            Return
            ------
            None
        """

        print("Submitting download request on server")
        down_request = self.datasets._ddi_submit_download(dataset_id=internal_id, 
                            zone=zone, access=acccess, project=project, path=path)
        print("Download submitted")

        # Wait until it is ready
        retries_max: int = 200
        retries: int = 0
        delay: int = 5 # secs
        print("Checking the status of download request")
        while retries < retries_max:
            status = self.datasets._ddi_get_download_status(request_id=down_request)

            if status['task_state'] == 'SUCCESS':
                # Download file
                print("Starting downloading the dataset")
                self.datasets._ddi_download_dataset(request_id=down_request, destination_file=destination_file, progress_func=printProgressBar)
                break

            if status['task_state'] == 'ERROR' or status['task_state'] == "FAILURE":
                self.session.logging.error('DOWNLOAD --  request failed: {0} -- FAILED'.format(status['task_result']))
                print("Download request ended with FAILURE status")
                raise Py4LexisException(status['task_result'])

            self.session.logging.debug(f"DOWNLOAD -- waiting for download to become ready -- remaining retries {retries} -- OK")    
            print(f'Download request not ready yet, {retries_max - retries} retries remaining')
            # Refresh
            self.session.token_refresh()
            retries = retries + 1
            time.sleep(delay)
        print("Dataset downloaded")

