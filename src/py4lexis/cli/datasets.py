# datasets.py

from typing import Optional
from py4lexis.session import LexisSession
from py4lexis.ddi.datasets import Datasets
from py4lexis.utils import printProgressBar
from datetime import date, datetime
from pandas import DataFrame
import time
# Making ASCII table
# Source: https://stackoverflow.com/questions/5909873/how-can-i-pretty-print-ascii-tables-with-python
from tabulate import tabulate


class DatasetsCLI:
    def __init__(self, 
                 session: LexisSession, 
                 print_content: Optional[bool]=False):
        """
            A class holds methods to manage datasets within LEXIS platform using INTERACTIVE mode.

            Attributes
            ----------
            session : class,
                Class which holds LEXIS session
            print_content : bool, optional
                If True then contents of all requests will be printed.

            Methods
            -------
            create_dataset(access: str, 
                           project: str, 
                           push_method: Optional[str]="empty", 
                           zone: Optional[str]="IT4ILexisZone",
                           path: Optional[str]="", 
                           contributor: Optional[list[str]]=["UNKNOWN contributor"], 
                           creator: Optional[list[str]]=["UNKNOWN creator"],
                           owner: Optional[list[str]]=["UNKNOWN owner"], 
                           publicationYear: Optional[str]=str(date.today().year),
                           publisher: Optional[list[str]]=["UNKNOWN publisher"], 
                           resourceType: Optional[str]=str("UNKNOWN resource type"),
                           title: Optional[str]=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> None
                Create an empty dataset with specified attributes.

            tus_uploader(access: str, 
                         project: str, 
                         filename: str, 
                         zone: Optional[str]="IT4ILexisZone", 
                         file_path: Optional[str]="./",
                         path: Optional[str]="", 
                         contributor: Optional[list[str]]=["NONAME contributor"], 
                         creator: Optional[list[str]]=["NONAME creator"],
                         owner: Optional[list[str]]=["NONAME owner"], 
                         publicationYear: Optional[str]=str(date.today().year), 
                         publisher: Optional[list[str]]=["NONAME publisher"],
                         resourceType: Optional[str]="NONAME resource type", 
                         title: Optional[str]="UNTITLED_TUS_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"), 
                         expand: Optional[str]="no", 
                         encryption: Optional[str]="no") -> None
                Create a dataset and upload a data by TUS client.

            get_dataset_status(filter_filename: Optional[str]="", 
                               filter_project: Optional[str]="", 
                               filter_task_state: Optional[str]="") -> None
                Prints datasets with status into table. Is possible to use filters to get filtered table by
                filename, project, task_status.

            get_all_datasets(filter_title: Optional[str]="",
                             filter_access: Optional[str]="", 
                             filter_project: Optional[str]="", 
                             filter_zone: Optional[str]="") -> None
                Prints a table of the all existing datasets. Is possible to use filters to get filtered table by
                title, access, zone, project.

            delete_dataset_by_id(internal_id: str, 
                                 access: str, 
                                 project: str) -> None
                Deletes a dataset by a specified internalID.

            download_dataset(acccess: str,
                             project: str,
                             internal_id: str,
                             zone:str,
                             path: Optional[str] = "",
                             destination_file: Optional[str] = "./download.tar.gz") -> None
                Downloads dataset by a specified informtions as access, zone, project, InternalID.
                It is possible to specify by path parameter which exact file in the dataset should be downloaded.
                It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"
        """
        self.print_content: bool = print_content
        self.session: LexisSession = session
        self.datasets: Datasets = Datasets(session, suppress_print=False)


    def create_dataset(self, 
                       access: str, 
                       project: str, 
                       push_method: Optional[str]="empty", 
                       zone: Optional[str]="IT4ILexisZone",
                       path: Optional[str]="", 
                       contributor: Optional[list[str]]=["UNKNOWN contributor"], 
                       creator: Optional[list[str]]=["UNKNOWN creator"],
                       owner: Optional[list[str]]=["UNKNOWN owner"], 
                       publicationYear: Optional[str]=str(date.today().year),
                       publisher: Optional[list[str]]=["UNKNOWN publisher"], 
                       resourceType: Optional[str]=str("UNKNOWN resource type"),
                       title: Optional[str]=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> None:
        """
            Creates an empty dataset with specified attributes

            Parameters
            ----------
            access : str, 
                One of the access types [public, project, user]
            project: str, 
                Project's short name.
            push_method: str, optional
                By default: "empty".
            zone: str, optional
                iRODS zone name in which dataset will be stored, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone".
            path: str, optional
                By default: "./"
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
            
            Return
            ------
            None
        """

        print(f"Creating dataset:"+
              f"    title: {title}\n"+ 
              f"    access:{access}\n"+
              f"    project:{project}\n"+
              f"    push_method:{push_method}\n"+
              f"    zone:{zone}")
        content, req_status = self.datasets.create_dataset(access, project, push_method, zone, path, contributor, creator,
                                                           owner, publicationYear, publisher, resourceType, title)
        if req_status is not None:
            print(f"Dataset successfully created...")
        else:
            print(f"Some error occurred while creating dataset. See log file, please.")


    def tus_uploader(self, 
                     access: str, 
                     project: str, 
                     filename: str, 
                     zone: Optional[str]="IT4ILexisZone", 
                     file_path: Optional[str]="./",
                     path: Optional[str]="", 
                     contributor: Optional[list[str]]=["NONAME contributor"], 
                     creator: Optional[list[str]]=["NONAME creator"],
                     owner: Optional[list[str]]=["NONAME owner"], 
                     publicationYear: Optional[str]=str(date.today().year), 
                     publisher: Optional[list[str]]=["NONAME publisher"],
                     resourceType: Optional[str]="NONAME resource type", 
                     title: Optional[str]="UNTITLED_TUS_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"), 
                     expand: Optional[str]="no", 
                     encryption: Optional[str]="no") -> None:
        """
            Upload a data by TUS client to a new specified dataset.

            Parameters
            ----------
            access : str
                One of the access types [public, project, user].
            project: str
                Project's short name.
            filename: str
                Name of a file to be uploaded.
            zone: str | None, optional
                iRODS zone name in which dataset will be stored, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone".
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

        print(f"Initialising TUS upload:\n"+
              f"    filename: {filename}\n"+
              f"    access: {access}\n"+
              f"    project: {project}\n"+
              f"    zone: {zone}")
        self.datasets.tus_uploader(access, project, filename, zone, file_path, path, contributor, creator, owner, publicationYear,
                                   publisher, resourceType, title, expand, encryption)


    def get_dataset_status(self, 
                           filter_filename: Optional[str]="", 
                           filter_project: Optional[str]="", 
                           filter_task_state: Optional[str]="") -> None:
        """
            Prints datasets with status into table. Is possible to use filters to get filtered table by
            filename, project, task_status.

            Parameters
            ----------
            filter_filename : str, optional
                To filter table of datasets by filename. By default: "" (i.e. filter is off).
            filter_project : str, optional
                To filter table of datasets by project. By default: "" (i.e. filter is off).
            filter_task_state : str, optional
                To filter table of datasets by task_state. One of ["PENDING", "SUCCESS"]. By default: "" (i.e. filter is off).
        """

        print(f"Retrieving data of status of the datasets...")
        content, req_status = self.datasets.get_dataset_status(content_as_pandas=True)
        if req_status is not None:
            try:
                print(f"Formatting pandas DataFrame into ASCII table...")
                
                cols: list[str] = ["Filename", "Project", "TaskState", "DatasetID", "TransferType"]
                datasets_table: DataFrame = content[cols]

                if filter_filename != "":
                    datasets_table = datasets_table[datasets_table["Filename"] == filter_filename]

                if filter_project != "":
                    datasets_table = datasets_table[datasets_table["Project"] == filter_project]

                if filter_task_state != "":
                    datasets_table = datasets_table[datasets_table["TaskState"] == filter_task_state]

                print(tabulate(datasets_table.values.tolist(), cols, tablefmt="grid"))

            except KeyError as kerr:
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
        else:
            print(f"Some error occurred while creating dataset. See log file, please.")


    def get_all_datasets(self, 
                         filter_title: Optional[str]="",
                         filter_access: Optional[str]="", 
                         filter_project: Optional[str]="", 
                         filter_zone: Optional[str]="") -> None:
        """
            Prints a table of the all existing datasets. Is possible to use filters to get filtered table by
            title, access, zone, project.

            Parameters
            ----------
            filter_title : str, optional
                To filter table of datasets by title. By default: "" (i.e. filter is off).
            filter_access : str, optional
                To filter table of datasets by access. By default: "" (i.e. filter is off).
            filter_project : str, optional
                To filter table of datasets by project. By default: "" (i.e. filter is off).
            filter_zone : str, optional
                To filter table of datasets by title. By default: "" (i.e. filter is off).
        """

        print(f"Retrieving data of the datasets...")
        content, req_status = self.datasets.get_all_datasets(content_as_pandas=True)
        if req_status is not None:
            try:
                print(f"Formatting pandas DataFrame into ASCII table...")
                
                cols: list[str] = ["Title", "Access", "Project", "Zone", "InternalID", "CreationDate"]
                datasets_table: DataFrame = content[cols]

                if filter_title != "":
                    datasets_table = datasets_table[datasets_table["Title"] == filter_title]
                
                if filter_access != "":
                    datasets_table = datasets_table[datasets_table["Access"] == filter_access]

                if filter_project != "":
                    datasets_table = datasets_table[datasets_table["Project"] == filter_project]

                if filter_zone != "":
                    datasets_table = datasets_table[datasets_table["Zone"] == filter_zone]

                print(tabulate(datasets_table.values.tolist(), cols, tablefmt="grid"))

            except KeyError as kerr:
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
        else:
            print(f"Some error occurred while creating dataset. See log file, please.")


    def delete_dataset_by_id(self, 
                             internal_id: str, 
                             access: str, 
                             project: str) -> None:
        """
            Deletes a dataset by a specified internalID.

            Parameters
            ----------
            internal_id : str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            access : str
                One of the access types [public, project, user]. Can be obtain by get_all_datasets() method.
            project: str
                Project's short name in which dataset is stored. Can be obtain by get_all_datasets() method.

            Returns
            -------
            None
        """

        print(f"Deleting dataset with ID: {internal_id}")
        content, req_status = self.datasets.delete_dataset_by_id(internal_id, access, project)

        if req_status is not None:
            print(f"Dataset has been deleted...")
        else:
            print(f"Some error occurred. See log file, please.")


    def download_dataset(self, 
                         acccess: str,
                         project: str,
                         internal_id: str,
                         zone:str,
                         path: Optional[str] = "",
                         destination_file: Optional[str] = "./download.tar.gz") -> None:
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
                iRODS zone name in which dataset is stored, one of ["IT4ILexisZone", "LRZLexisZone"]. Can be obtain by get_all_datasets() method.
            path: str, optional
                Path to exact folder, by default = "".
            destination_file: str, optional
                Paht to the local destination folder, by default "./download.tar.gz"

            Return
            ------
            None
        """

        print("Submitting download request on server...")
        down_request = self.datasets._ddi_submit_download(dataset_id=internal_id, 
                            zone=zone, access=acccess, project=project, path=path)
        print("Download submitted!")

        # Wait until it is ready
        retries_max: int = 200
        retries: int = 0
        delay: int = 5 # secs
        is_error: bool = False
        print("Checking the status of download request...")
        while retries < retries_max:
            status = self.datasets._ddi_get_download_status(request_id=down_request)

            if status["task_state"] == "SUCCESS":
                # Download file
                print("Starting downloading the dataset...")
                self.datasets._ddi_download_dataset(request_id=down_request, destination_file=destination_file, progress_func=printProgressBar)
                break

            if status["task_state"] == "ERROR" or status["task_state"] == "FAILURE":
                is_error = True
                self.session.logging.error(f"DOWNLOAD --  request failed: {status['task_result']} -- FAILED")
                print("Download request ended with FAILURE status! Dataset Download -- FAILED!")
                break

            self.session.logging.debug(f"DOWNLOAD -- waiting for download to become ready -- remaining retries {retries} -- OK")    
            print(f"Download request not ready yet, {retries_max}/{retries} retries remaining")
            # Refresh
            self.session.refresh_token()
            retries = retries + 1
            time.sleep(delay)

        if retries == retries_max and not is_error:
            is_error = True
            print(f"Reached maximum retries: {retries}/{retries_max} -- Download FAILED!")

        if not is_error:
            print("Dataset download -- SUCCESS!")


    def get_list_of_files_in_dataset(self, 
                                     internal_id: str, 
                                     access: str,
                                     project: str, 
                                     zone: str,
                                     filter_filename: Optional[str]="", 
                                     filename_compare_type: Optional[str]="",
                                     filter_size: Optional[int]=0,
                                     size_compare_type: Optional[str]="eq", 
                                     filter_type: Optional[str]="") -> None:
        """
            List all files within the dataset.

            Parameters
            ----------
            internal_id : str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            access : str
                Access of the dataset. One of ["user", "project", "public"]. Can be obtain by get_all_datasets() method.
            project : str
                Project's short name in which the dataset is stored. Can be obtain by get_all_datasets() method.
            zone : str
                iRODS zone name in which dataset is stored, one of ["IT4ILexisZone", "LRZLexisZone"]. Can be obtain by get_all_datasets() method.
            filter_filename : str, optional
                To filter table of files by name. By default: "" (i.e. filter is off).
            filename_compare_type : str, optional
                Type of comparison for filtering by filename. One of ["eq", "in"].
                "eq" => filter filename == filename in table,
                "in" => filter filename 'in' filename in table (part of filename in table).
                By default: "eq".
            filter_size : int, optional
                To filter table of files by size. By default: 0 (i.e. filter is off).
            size_compare_type : str, optional
                Type of comparison for filtering by size. One of ["eq", "l", "leq", "g", "geq"].
                "eq" => filter size == size in table,
                "l" => filter size < size in table,
                "leq" => filter size <= size in table,
                "g" => filter size > size in table,
                "geq" => filter size >= size in table.
                By default: "eq".
            filter_type : str, optional
                To filter table of files by type. By default: "" (i.e. filter is off).

            Returns
            -------
            None
        """

        print(f"Retrieving data of files in the dataset...")
        content, req_status = self.datasets.get_list_of_files_in_dataset(internal_id, 
                                                                         access,
                                                                         project, 
                                                                         zone, 
                                                                         content_as_pandas=True)
        if req_status is not None:
            try:
                print(f"Formatting pandas DataFrame into ASCII table...")
                
                cols: list[str] = ["Filename", "Size", "Type", "CreateTime", "Checksum"]
                datasets_table: DataFrame = content

                if filter_filename != "":
                    if filename_compare_type == "eq":
                        datasets_table = datasets_table[datasets_table["Filename"] == filter_filename]
                    elif filename_compare_type == "in":
                        datasets_table = datasets_table[filter_filename in datasets_table["Filename"]]
                    else:
                        print("Wrong comparison type for filename.")
                        ["Filename", "Size", "Type", "CreationTime", "Checksum"]
                if filter_size > 0:
                    if size_compare_type == "eq":
                        datasets_table = datasets_table[datasets_table["Size"] == filter_size]
                    elif size_compare_type == "l":
                        datasets_table = datasets_table[datasets_table["Size"] < filter_size]
                    elif size_compare_type == "leq":
                        datasets_table = datasets_table[datasets_table["Size"] <= filter_size]
                    elif size_compare_type == "g":
                        datasets_table = datasets_table[datasets_table["Size"] > filter_size]
                    elif size_compare_type == "geq":
                        datasets_table = datasets_table[datasets_table["Size"] >= filter_size]
                    else:
                        print("Wrong comparison type for filesize.")
                else:
                    print("Filesize cannot be negative.")

                print(tabulate(datasets_table.values.tolist(), cols, tablefmt="grid"))

            except KeyError as kerr:
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
        else:
            print(f"Some error occurred while creating dataset. See log file, please.")