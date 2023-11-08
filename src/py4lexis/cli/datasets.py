from __future__ import annotations
from typing import Generator, Optional
from py4lexis.session import LexisSession
from py4lexis.ddi.datasets import Datasets
from py4lexis.directory_tree import DirectoryTree
from py4lexis.custom_types.directory_tree import TreeDirectoryObject
from datetime import date, datetime
from pandas import DataFrame
# Making ASCII table
# Source: https://stackoverflow.com/questions/5909873/how-can-i-pretty-print-ascii-tables-with-python
from tabulate import tabulate


class DatasetsCLI(object):
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

            tus_uploader_new(access: str, 
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
                Creates a new dataset with specified metadata and upload a file or whole directory tree to it.

            tus_uploader_rewrite(dataset_id: str,
                                 dataset_title: str,
                                 access: str, 
                                 project: str, 
                                 filename: str, 
                                 zone: Optional[str]="IT4ILexisZone", 
                                 file_path: Optional[str]="./",
                                 path: Optional[str]="",                               
                                 encryption: Optional[str]="no") -> None:
                Uploads a file or whole directory tree to existing dataset. If files already exist, it will rewrite them.

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

            delete_dataset_by_id(dataset_id: str, 
                                 access: str, 
                                 project: str) -> None
                Deletes a dataset by a specified internalID.

            download_dataset(acccess: str,
                             project: str,
                             dataset_id: str,
                             Optional[str]="IT4ILexisZone",
                             path: Optional[str] = "",
                             destination_file: Optional[str] = "./download.tar.gz") -> None
                Downloads dataset by a specified informtions as access, zone, project, InternalID.
                It is possible to specify by path parameter which exact file in the dataset should be downloaded.
                It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"

            get_list_of_files_in_dataset(dataset_id: str, 
                                         access: str,
                                         project: str, 
                                         Optional[str]="IT4ILexisZone",
                                         path: Optional[str]="",
                                         print_dir_tree: Optional[bool]=False,
                                         filter_filename: Optional[str]="", 
                                         filename_compare_type: Optional[str]="",
                                         filter_size: Optional[int]=0,
                                         size_compare_type: Optional[str]="eq", 
                                         filter_type: Optional[str]="") -> None:
                List all files within the dataset.

            get_dataset_path(access: str, project: str, internalID: str, username: Optional[str]="") -> str
                Prints a path for an existing dataset as the combination of access, project, internalID and username.
        """
        self.print_content: bool = print_content
        self.session: LexisSession = session
        self.datasets: Datasets = Datasets(session, 
                                           print_content=print_content, 
                                           suppress_print=False)


    def create_dataset(self, 
                       access: str, 
                       project: str, 
                       push_method: Optional[str]="empty", 
                       zone: Optional[str]="",
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
        if zone == "":
            zone = self.session.DFLT_Z 

        _, _ = self.datasets.create_dataset(access, 
                                            project, 
                                            push_method, 
                                            zone, 
                                            path, 
                                            contributor, 
                                            creator,
                                            owner, 
                                            publicationYear, 
                                            publisher, 
                                            resourceType, 
                                            title)


    def tus_uploader_new(self, 
                         access: str, 
                         project: str, 
                         filename: str, 
                         zone: Optional[str]="", 
                         file_path: Optional[str]="./",
                         path: Optional[str]="", 
                         contributor: Optional[list[str]]=["UNKNOWN contributor"], 
                         creator: Optional[list[str]]=["UNKNOWN creator"],
                         owner: Optional[list[str]]=["UNKNOWN owner"], 
                         publicationYear: Optional[list[str]]=[str(date.today().year)], 
                         publisher: Optional[list[str]]=["UNKNOWN publisher"],
                         resourceType: Optional[list[str]]=["UNKNOWN resource type"], 
                         title: Optional[list[str]]=["UNTITLED_TUS_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S")], 
                         encryption: Optional[str]="no") -> None:
        """
            Creates a new dataset with specified metadata and upload a file or whole directory tree to it.

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
            publicationYear: list[str], optional
                By default: [CURRENT_YEAR].
            publisher: list[str], optional
                By default: ["UNKNOWN publisher"].
            resourceType: list[str], optional
                By default: ["UNKNOWN resource type"].
            title: list[str], optional
                By default: ["UNTITLED_Dataset_" + TIMESTAMP].
            encryption: str, optional
                By default: "no".

            Returns
            -------
            None
        """
        if zone == "":
            zone = self.session.DFLT_Z 

        self.datasets.tus_uploader_new(access, project, filename, zone, file_path, path, contributor, creator, owner, publicationYear,
                                       publisher, resourceType, title, encryption)
        

    def tus_uploader_rewrite(self, 
                             dataset_id: str,
                             dataset_title: str,
                             access: str, 
                             project: str, 
                             filename: str, 
                             zone: Optional[str]="", 
                             file_path: Optional[str]="./",
                             path: Optional[str]="",  
                             encryption: Optional[str]="no") -> None:
        """
            Uploads a file or whole directory tree to existing dataset. If files already exist, it will rewrite them.

            Parameters
            ----------
            dataset_id : str
                Internal ID of existing dataset.
            dataset_title : str
                Title of existing dataset.
            access : str
                One of the access types [public, project, user].
            project: str
                Project's short name in which dataset will be stored.
            filename: str
                Name of a file to be uploaded.
            zone: str | None, optional
                iRODS zone name in which dataset will be stored, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone".
            file_path: str | None, optional,
                Path to a file in user's machine. By default "./".
            path: str, optional
                By default, root path is set, i.e. "./".
            encryption: str, optional
                By default: "no".

            Returns
            -------
            None
        """
        if zone == "":
            zone = self.session.DFLT_Z 

        self.datasets.tus_uploader_rewrite(dataset_id, dataset_title, access, project, filename,
                                           zone, file_path, path, encryption)


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
                self.session.logging.error(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")


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
                self.session.logging.error(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")


    def delete_dataset_by_id(self, 
                             dataset_id: str, 
                             access: str, 
                             project: str) -> None:
        """
            Deletes a dataset by a specified internalID.

            Parameters
            ----------
            dataset_id : str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            access : str
                One of the access types [public, project, user]. Can be obtain by get_all_datasets() method.
            project: str
                Project's short name in which dataset is stored. Can be obtain by get_all_datasets() method.

            Returns
            -------
            None
        """

        _, _ = self.datasets.delete_dataset_by_id(dataset_id, access, project)


    def download_dataset(self, 
                         dataset_id: str,
                         access: str,
                         project: str,                         
                         zone: Optional[str] = "",
                         path: Optional[str] = "",
                         destination_file: Optional[str] = "./downloaded.tar.gz") -> None:
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
            dataset_id: str
                InternalID of the dataset.
            zone: str, optional
                iRODS zone name in which dataset will be stored, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone". Can be obtain by get_all_datasets() method.
            path: str, optional
                Path to exact folder, by default = "".
            destination_file: str, optional
                Paht to the local destination folder, by default "./download.tar.gz"

            Return
            ------
            None
        """
        if zone == "":
            zone = self.session.DFLT_Z 

        self.datasets.download_dataset(dataset_id, access, project, zone, path, destination_file)
        

    def get_list_of_files_in_dataset(self, 
                                     dataset_id: str, 
                                     access: str,
                                     project: str, 
                                     zone: Optional[str]="",
                                     path: Optional[str]="",
                                     print_dir_tree: Optional[bool]=False,
                                     filter_filename: Optional[str]="", 
                                     filename_compare_type: Optional[str]="",
                                     filter_size: Optional[int]=0,
                                     size_compare_type: Optional[str]="geq", 
                                     filter_type: Optional[str]="") -> None:
        """
            List all files within the dataset.

            Parameters
            ----------
            dataset_id : str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            access : str
                Access of the dataset. One of ["user", "project", "public"]. Can be obtain by get_all_datasets() method.
            project : str
                Project's short name in which the dataset is stored. Can be obtain by get_all_datasets() method.
            zone: str, optional
                iRODS zone name in which dataset will be stored, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone". Can be obtain by get_all_datasets() method.
            path : str, optional
                Path within the dataset. By default: path="".
            print_dir_tree : bool, optional
                If True, the directory tree within the dataset will be printed. Also, if True, printing the ASCII table will be suppressed.
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
                By default: "geq".
            filter_type : str, optional
                To filter table of files by type. By default: "" (i.e. filter is off).

            Returns
            -------
            None
        """
        if zone == "":
            zone = self.session.DFLT_Z 

        if not print_dir_tree:
            content, req_status = self.datasets.get_list_of_files_in_dataset(dataset_id, 
                                                                            access,
                                                                            project, 
                                                                            zone, 
                                                                            path=path,
                                                                            content_as_pandas=True)
        else:
            content, req_status = self.datasets.get_list_of_files_in_dataset(dataset_id, 
                                                                            access,
                                                                            project, 
                                                                            zone, 
                                                                            path=path,
                                                                            content_as_pandas=False)
            

        if req_status is not None and not print_dir_tree:
            try:
                print(f"Formatting pandas DataFrame into ASCII table...")
                
                cols: list[str] = ["Filename", "Path", "Size", "CreateTime", "Checksum"]
                datasets_table: DataFrame = content

                if filter_filename != "":
                    if filename_compare_type == "eq":
                        datasets_table = datasets_table[datasets_table["Filename"] == filter_filename]
                    elif filename_compare_type == "in":
                        datasets_table = datasets_table[filter_filename in datasets_table["Filename"]]
                    else:
                        print("Wrong comparison type for filename.")
                if filter_size >= 0:
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

                if filter_type != "":
                    datasets_table = datasets_table[datasets_table["Type"] == filter_type]

                print(tabulate(datasets_table.values.tolist(), cols, tablefmt="grid"))

            except KeyError as kerr:
                self.session.logging.error(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")
                print(f"Wrong or missing key '{kerr}' in response content as DataFrame!!!")

        if req_status is not None and print_dir_tree:
            tree_content: TreeDirectoryObject = TreeDirectoryObject(content)
            tree_items: Generator[DirectoryTree, None, None] = DirectoryTree.make_tree(tree_content)
            for item in tree_items:
                print(item.to_string())


    def get_dataset_path(self, access: str, project: str, internalID: str, username: Optional[str]=""):
        """
            Prints a path for an existing dataset as the combination of access, project, internalID and username.

            Parameters:
            -----------
            access : str
                Access mode of the project (user, project, public)
            project : str
                Project's short name.
            internalID : str
                Dataset's internalID as UUID.
            username : str, optional
                The iRODS username. Needed when user access is defined

            Returns:
            --------
            None

        """

        print(f"Path: {self.datasets.get_dataset_path(access, project, internalID, username)}")