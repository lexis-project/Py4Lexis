from __future__ import annotations
from typing import Literal
import requests as req
from requests import Response
from datetime import date, datetime
from py4lexis.ddi.tus_client import TusClient
from tusclient import exceptions
from pandas import DataFrame
from py4lexis.exceptions import Py4LexisException
from py4lexis.session import LexisSession
from py4lexis.utils import convert_get_all_datasets_to_pandas, \
                           convert_get_datasets_status_to_pandas, \
                           convert_dir_tree_to_pandas, printProgressBar, \
                           assemble_dataset_path, check_if_uuid
from py4lexis.ddi.uploader import Uploader
import json
import time


class Datasets(object):
    def __init__(self,
                 session: LexisSession,
                 print_content: bool=False,
                 suppress_print: bool=True) -> None:
        """
            A class holds methods to manage datasets within LEXIS platform.

            Attributes
            ----------
            session : class
                Class that holds LEXIS session
            print_content : bool, optional
                If True then contents of all requests will be printed.
            suppress_print: bool, optional
                If True then all prints are suppressed. By default: suppress_print=True

            Methods
            -------
            create_dataset(access: Literal["public", "project", "user"], 
                           project: str, 
                           push_method: str="empty", 
                           zone: str="IT4ILexisZone",
                           path: str="", 
                           contributor: list[str]=["UNKNOWN contributor"], 
                           creator: list[str]=["UNKNOWN creator"],
                           owner: list[str]=["UNKNOWN owner"], 
                           publicationYear: str=str(date.today().year),
                           publisher: list[str]=["UNKNOWN publisher"], 
                           resourceType: str=str("UNKNOWN resource type"),
                           title: str=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int] | tuple[None, None]
                Create an empty dataset with specified attributes.

            tus_uploader_new(access: Literal["public", "project", "user"], 
                             project: str, 
                             filename: str, 
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
                             encryption: str="no") -> None
                Creates a new dataset with specified metadata and upload a file or whole directory tree to it.

            tus_uploader_rewrite(internal_id: str,
                                 dataset_title: str,
                                 access: Literal["public", "project", "user"], 
                                 project: str, 
                                 filename: str, 
                                 zone: str="IT4ILexisZone", 
                                 file_path: str="./",
                                 path: str="",                               
                                 encryption: str="no") -> None:
                Uploads a file or whole directory tree to existing dataset. If files already exist, it will rewrite them.
                An archive .tar.gz could be uploaded or the "pure" files.

            get_dataset_status(content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
                Prints a table of the datasets' staging states.

            get_all_datasets(content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
                Prints a table of the all existing datasets.

            delete_dataset_by_id(internal_id: str, 
                                 access: Literal["public", "project", "user"], 
                                 project: str) -> tuple[list[dict], int] | tuple[None, None] | None
                Deletes a dataset by a specified internalID.

            download_dataset(access: Literal["public", "project", "user"], 
                             project: str, 
                             internal_id: str, 
                             zone: str="IT4ILexisZone",
                             path: str="",
                             destination_file: str="./download.tar.gz") -> None
                Downloads dataset by a specified informtions as access, zone, project, Interna_Id.
                It is possible to specify by path parameter which exact file in the dataset should be downloaded.
                It is possible to specify local desination folder. Default is set to = "./download.tar.gz"

            get_list_of_files_in_dataset(internal_id: str, 
                                         access: Literal["public", "project", "user"],
                                         project: str, 
                                         zone: str="IT4ILexisZone", 
                                         path: str="",
                                         content_as_pandas: bool=False) -> dict[str] | DataFrame | None
                List all files within the dataset.

            get_dataset_path(access: Literal["public", "project", "user"], 
                             project: str, 
                             internal_id: str, 
                             username: str="") -> str | None
                Returns a path for an existing dataset as the combination of access, project, internalID and username.
        """
        self.session = session
        self.print_content = print_content
        self.suppress_print = suppress_print


    def create_dataset(self, 
                       access: Literal["public", "project", "user"], 
                       project: str, 
                       push_method: str="empty", 
                       zone: str="",
                       path: str="", 
                       contributor: list[str]=["UNKNOWN contributor"], 
                       creator: list[str]=["UNKNOWN creator"],
                       owner: list[str]=["UNKNOWN owner"], 
                       publicationYear: str=str(date.today().year),
                       publisher: list[str]=["UNKNOWN publisher"], 
                       resourceType: str=str("UNKNOWN resource type"),
                       title: str=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int] | tuple[None, None]:
        """
            Creates an empty dataset with specified attributes

            Parameters
            ----------
            access : Literal["public", "project", "user"], 
                One of the access types [public, project, user]
            project: str, 
                Project's shortname.
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

            Returns
            -------
            dict | None
                Content of the HTTP request as JSON. None is returned if some errors have occured.
            int | None
                Status of the HTTP request. None is returned if some errors have occured.
        """
        if zone == "":
            zone = self.session.zone

        url: str = self.session.API_PATH + "/dataset"
        post_body: dict = {
            "push_method": push_method,
            "access": access,
            "project": project,
            "zone": zone,
            "path": path,
            "metadata": {
                "contributor": contributor,
                "creator": creator,
                "owner": owner,
                "publicationYear": publicationYear,
                "publisher": publisher,
                "resourceType": resourceType,
                "title": title
            }
        }

        self.session.logging.debug(f"POST -- {url} -- CREATE -- PROGRESS")
        status_solved: bool = False
        content: dict = {}
        is_error: bool = False

        if not self.suppress_print:
            print(f"Creating dataset:"+
                  f"    title: {title}\n"+ 
                  f"    access:{access}\n"+
                  f"    project:{project}\n"+
                  f"    push_method:{push_method}\n"+
                  f"    zone:{zone}")

        while not status_solved:
            response: Response = req.post(
                    self.session.API_PATH + "/dataset",
                    headers=self.session.API_HEADER,
                    json=post_body)
            
            content, status_solved, is_error = self.session.handle_request_status(response,
                                                                                  f"POST -- {url} -- CREATE", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)  

        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while creating the dataset. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while creating the dataset. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                print(f"The dataset was successfully created:")
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code
    

    def tus_uploader_new(self, 
                         access: Literal["public", "project", "user"], 
                         project: str, 
                         filename: str, 
                         zone: str="", 
                         file_path: str="./",
                         path: str="", 
                         contributor: list[str]=["UNKNOWN contributor"], 
                         creator: list[str]=["UNKNOWN creator"],
                         owner: list[str]=["UNKNOWN owner"], 
                         publicationYear: list[str]=[str(date.today().year)], 
                         publisher: list[str]=["UNKNOWN publisher"],
                         resourceType: list[str]=["NONAME resource type"], 
                         title: list[str]=["UNTITLED_TUS_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S")], 
                         encryption: str="no") -> None:
        """
            Creates a new dataset with specified metadata and upload a file or whole directory tree to it.

            Parameters
            ----------
            access : Literal["public", "project", "user"]
                One of the access types [public, project, user].
            project: str
                Project's shortname in which dataset will be stored.
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
            zone = self.session.zone

        expand: str = "no"
        if ".tar.gz" in file_path:
            expand = "yes"

        file_path = file_path + filename
        metadata: dict = {
            "path": path,
            "zone": zone,
            "filename": filename,
            "user": self.session.USERNAME,
            "project": project,
            "access": access,
            "expand": expand,
            "encryption": encryption,
            "metadata": json.dumps({
                "contributor": contributor,
                "creator": creator,
                "owner": owner,
                "publicationYear": publicationYear,
                "publisher": publisher,
                "resourceType": resourceType,
                "title": title
            })
        }

        self._tus_upload(file_path, metadata)


    def tus_uploader_rewrite(self, 
                             internal_id: str,
                             dataset_title: str,
                             access: Literal["public", "project", "user"], 
                             project: str, 
                             filename: str, 
                             zone: str="", 
                             file_path: str="./",
                             path: str="",                               
                             encryption: str="no") -> None:
        """
            Uploads a file or whole directory tree to existing dataset. If files already exist, it will rewrite them.
            An archive .tar.gz could be uploaded or the "pure" files.

            Parameters
            ----------
            internal_id : str
                Internal ID of existing dataset.
            dataset_title : str
                Title of existing dataset.
            access : Literal["public", "project", "user"]
                One of the access types [public, project, user].
            project: str
                Project's shortname in which dataset will be stored.
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
            zone = self.session.zone

        file_path = file_path + filename

        expand: str = "no"
        if ".tar.gz" in file_path:
            expand = "yes"

        metadata: dict = {
            "filename": filename,
            "project": project,
            "access": access,
            "zone": zone,
            "metadata": json.dumps({
                "title": dataset_title
            }),
            "encryption": encryption,
            "expand": expand,
            "path": path,
            "internal_id": internal_id
        }

        self._tus_upload(file_path, metadata)


    def _tus_upload(self, file_path: str, metadata: dict):
        if not self.suppress_print:
            print(f"Initialising TUS upload with the following metadata:\n"+
                  f"    {metadata}")
            
        url: str = self.session.API_PATH + "/transfer/upload/"
        status: bool = True
        try:
            # TODO: Make better check if token is alive
            self.session.refresh_token()

            if not self.suppress_print:
                print("Initialising TUS client...")

            tus_client: TusClient = TusClient(url,
                                              headers=self.session.API_HEADER)
            
            self.session.logging.debug(f"TUS UPLOAD -- TUS client initialised -- OK")
            if not self.suppress_print:
                print(f"Initialising TUS client -- OK\n"+
                      f"Initialising TUS uploader...")

            uploader: Uploader = tus_client.uploader(file_path=file_path, 
                                                     chunk_size=1048576,
                                                     metadata=metadata)
            
            self.session.logging.debug(f"TUS UPLOAD -- TUS upload initialised -- OK")
            if not self.suppress_print:
                print("Initialising TUS uploader -- OK")
            uploader.upload()

        except exceptions.TusCommunicationError as te:
            status = False
            self.session.logging.error(f"TUS UPLOAD -- Upload error: {te.response_content} -- FAILED")

        if not status:
            if not self.suppress_print:
                print(f"Some errors occurred during the TUS upload. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred during the TUS upload. See log file, please.")


    def get_dataset_status(self, 
                           content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
        """
            Get datasets' upload status information.

            Parameters
            ----------
            content_as_pandas : bool, optional
                Convert HTTP response content from JSON to pandas DataFrame. By default: content_as_pandas=False

            Returns
            -------
            list[dict] | DataFrame | None
                Content of the HTTP request as list of JSONs dictionaries or pandas DataFrame if 'content_as_pandas=True'.  None is returned if some errors have occured.
            int | None
                Status of the HTTP request.  None is returned if some errors have occured.
        """
        if not self.suppress_print:
            print(f"Retrieving upload status of the datasets...")

        url: str = self.session.API_PATH + "/transfer/status"

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        is_error: bool = True

        while not status_solved:
            response: Response = req.get(url,
                                         headers={"Authorization": "Bearer " + self.session.TOKEN})
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)        

        if not is_error and content_as_pandas:
            content = convert_get_datasets_status_to_pandas(self.session, 
                                                            content, 
                                                            supress_print=self.suppress_print)

            if content is None:
                is_error = True
                self.session.logging.error(f"GET -- {url} -- CONVERT TO DATAFRAME -- FAILED")
            else:
                self.session.logging.debug(f"GET -- {url} -- CONVERT TO DATAFRAME -- OK")    
                
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving upload status of datasets. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving upload status of datasets. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                    print("Upload status of datasets successfully retrieved (and converted) -- OK") 
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code


    def get_all_datasets(self, 
                         content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
        """
            Get all existing datasets

            Parameters
            ----------
            content_as_pandas: bool, optional
                Convert HTTP response content from JSON to pandas DataFrame. By default: content_as_pandas=False

            Returns
            -------
            list[dict] | DataFrame | None
                Content of the HTTP request as JSON or as pandas DataFrame if 'content_as_pandas=True'. None if some errors have occured.
            int | None
                Status of the HTTP request. None if some errors have occured.
        """

        if not self.suppress_print:
            print(f"Retrieving data of the datasets...")

        url: str = self.session.API_PATH + "/dataset/search/metadata"

        self.session.logging.debug(f"POST -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = None
        is_error: bool = True
        while not status_solved:
            response: Response = req.post(url,
                                          headers=self.session.API_HEADER,
                                          json={})

            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"POST -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)
        
        if not is_error and content_as_pandas:
            content = convert_get_all_datasets_to_pandas(self.session, 
                                                         content, 
                                                         supress_print=self.suppress_print)

            if content is None:
                is_error = True
                self.session.logging.error(f"POST -- {url} -- CONVERT TO DATAFRAME -- FAILED")
                
            else:
                self.session.logging.debug(f"POST -- {url} -- CONVERT TO DATAFRAME -- OK")
                    
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving data of the datasets. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving data of the datasets. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                    print(f"Data of the datasets successfully retrieved (and converted)....")
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code


    def delete_dataset_by_id(self, 
                             internal_id: str, 
                             access: Literal["public", "project", "user"], 
                             project: str) -> tuple[list[dict], int] | tuple[None, None]:
        """
            Deletes a dataset by a specified internalID.

            Parameters
            ----------
            internal_id : str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            access : Literal["public", "project", "user"]
                One of the access types [public, project, user]. Can be obtain by get_all_datasets() method.
            project: str
                Project's shortname in which dataset is stored. Can be obtain by get_all_datasets() method.

            Returns
            -------
            list[dict] | None
                Content of the HTTP request as JSON. None if some errors have occured.
            int | None
                Status of the HTTP request. None if some errors have occured.
        """
        is_uuid = check_if_uuid(self.session, internal_id)
        if not is_uuid:
            return None, None

        if not self.suppress_print:
            print(f"Deleting dataset with ID: {internal_id}")

        url: str = self.session.API_PATH + "/dataset"
        delete_body: dict = {
            "access": access,
            "project": project,
            "internalID": internal_id
        }

        self.session.logging.debug(f"DELETE -- {url} -- ID:{internal_id} -- PROGRESS")
        status_solved: bool = False
        content: dict = {}
        is_error: bool = False
        while not status_solved:
            response = req.delete(url,
                                  headers=self.session.API_HEADER,
                                  json=delete_body)

            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"DELETE -- {url} -- ID:{internal_id}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)

        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while deleting the dataset. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while deleting the dataset. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                print(f"Dataset has been deleted...")
            if self.print_content:
                print(f"content: {content}")
            return content, response.status_code
    

    def _ddi_submit_download(self, 
                             internal_id: str, 
                             zone: str, 
                             access: Literal["public", "project", "user"], 
                             project: str, 
                             path: str) -> str | None:
        """
            Private method to obtain 'requestID' for the dataset download.

            Parameters
            ----------
            internal_id : str
                InternalID of the dataset.
            zone : str
                Zone of the dataset.
            access : Literal["public", "project", "user"]
                Access of the dataset. One of the access types [public, project, user].
            project : str
                Project's shortname.

            Returns
            -------
            str | None
                ID of the request. None if some errors occur
        """
        url: str = self.session.API_PATH + "/transfer/download"
        download_body: dict = {
            "zone": zone,
            "access": access,
            "project": project,
            "internal_id": internal_id,
            "path": path
        }
        
        status_solved: bool = False
        is_error: bool = False
        content: dict = {}
        try:
            while not status_solved:
                response: Response = req.post(url,
                                              headers=self.session.API_HEADER,
                                              json=download_body)

                content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                      f"POST -- {url} -- DATA_ID:{internal_id}", 
                                                                                      to_json=True,
                                                                                      suppress_print=self.suppress_print)
            
            if not is_error:
                request_id: str = content["requestId"]

        except KeyError as kerr:
            is_error = True
            self.session.logging.error(f"POST -- {url} -- DATA_ID:{internal_id} -- Wrong or missing key '{kerr}' in response -- FAILED")
        
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while submitting download. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while submitting download. See log file, please.")
            return None
        else:
            return request_id
            

    def _ddi_get_download_status(self, 
                                 request_id: str) -> dict:
        """
            Private method to get download status for the dataset downloading.

            Parameters
            ----------
            request_id : str
                Request ID obtained by _ddi_submit_download.

            Returns
            -------
            dict
                Status of the download.
        """
        url: str = self.session.API_PATH + "/transfer/status/" + request_id

        status_solved: bool = False
        is_error: bool = False
        content: dict = {}
        while not status_solved:
            response: Response = req.get(url, headers=self.session.API_HEADER)
            
            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"GET -- {url} -- REQ_ID:{request_id}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)
        
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving download status. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving download status. See log file, please.")
            return None
        else:
            return content      


    def _ddi_download_dataset(self, 
                              request_id: str, 
                              destination_file: str, 
                              progress_func: callable = None) -> None:
        """
            Private method providing download of the dataset.

            Parameters
            ----------
            request_id : str
                Request ID obtained by _ddi_submit_download.
            destination_file: str
                Destination path for the download.
            progress_func : callable, optional
                Function providing the progress of the download.

            Returns
            -------
            None
        """
        url: str = self.session.API_PATH + "/transfer/download/" + request_id

        status_solved: bool = False
        is_error: bool = False
        content: dict = {}
        try:
            while not status_solved:
                response: Response = req.get(url,
                                             headers=self.session.API_HEADER,
                                             stream=True)
                
                content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                      f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD", 
                                                                                      to_json=False,
                                                                                      suppress_print=self.suppress_print)
            
            # File ready, start downloading
            total_length = response.headers.get("content-length")
            self.session.logging.debug(f"GET -- {url} -- REQ_ID:{request_id} -- STARTING DOWNLOAD -- OK")

            with open(destination_file, "wb") as f:
                if total_length is None: # no content length header
                    f.write(content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    current: int = int(0)
                    chunk_size: int = int(4096)
                    for data in response.iter_content(chunk_size=chunk_size):
                        if progress_func:
                            progress_func(current, total_length, prefix='Progress: ', suffix='Downloaded', length=50)
                            current += chunk_size
                        dl += len(data)
                        f.write(data)
                    if progress_func:
                        progress_func(total_length, total_length, prefix='Progress: ', suffix='Downloaded', length=50)

        except KeyError as kerr:
            is_error = True
            self.session.logging.error(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- Wrong or missing key '{kerr}' in response -- FAILED")

        except IOError as ioe:
            is_error = True
            self.session.logging.error(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- FAILED")
            self.session.logging.error(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- ERROR -- {ioe}")
        
        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while downloading dataset. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while downloading dataset. See log file, please.")


    def download_dataset(self, 
                         internal_id: str,
                         acccess: str, 
                         project: str,                           
                         zone: str="",
                         path: str="",
                         destination_file: str="./download.tar.gz") -> None:
        """
            Downloads dataset by a specified information as access, zone, project, InternalID.
            It is possible to specify by path parameter which exact file in the dataset should be downloaded.
            It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"
            
            Parameters
            ----------
            access : Literal["public", "project", "user"]
                One of the access types [public, project, user]. Can be obtain by get_all_datasets() method.
            project: str
                Project's shortname in which dataset is stored. Can be obtain by get_all_datasets() method.
            internal_id: str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            zone: str
                iRODS zone name in which dataset is stored, one of ["IT4ILexisZone", "LRZLexisZone"]. Can be obtain by get_all_datasets() method.
            path: str, optional
                Path to exact folder, by default  = "".
            destination_file: str, optional
                Path to the local destination folder, by default "./download.tar.gz".

            Returns
            -------
            None
        """
        is_uuid = check_if_uuid(self.session, internal_id)
        if not is_uuid:
            return None
        
        if zone == "":
            zone = self.session.zone
        
        if not self.suppress_print:
            print("Submitting download request on server...")

        down_request = self._ddi_submit_download(internal_id=internal_id, 
                                                 zone=zone, 
                                                 access=acccess,
                                                 project=project, 
                                                 path=path)
        if not self.suppress_print:
            print("Download submitted!")

        # Wait until it is ready
        retries_max: int = 200
        retries: int = 0
        delay: int = 5 # secs
        is_error: bool = False

        if not self.suppress_print:
            print("Checking the status of download request...")

        while retries < retries_max:
            status = self._ddi_get_download_status(request_id=down_request)

            if status["task_state"] == "SUCCESS":
                if not self.suppress_print:
                    print("Starting downloading the dataset...")
                # Download file
                if not self.suppress_print:
                    self._ddi_download_dataset(request_id=down_request, destination_file=destination_file, progress_func=printProgressBar)
                else:
                    self._ddi_download_dataset(request_id=down_request, destination_file=destination_file)
                break

            if status["task_state"] == "ERROR" or status["task_state"] == "FAILURE":
                is_error = True
                self.session.logging.error(f"DOWNLOAD -- REQ_ID:{down_request} -- request failed: {status['task_result']} -- FAILED")
                break

            self.session.logging.debug(f"DOWNLOAD -- REQ_ID:{down_request} -- waiting for download to become ready -- remaining retries {retries}/{retries_max} -- OK")    
            
            if not self.suppress_print:
                print(f"Download request not ready yet, {retries_max}/{retries} retries remaining")

            # Refresh
            self.session.refresh_token()
            retries = retries + 1
            time.sleep(delay)
        
        if retries == retries_max and not is_error:
            is_error = True
            self.session.logging.error(f"DOWNLOAD -- REQ_ID:{down_request} -- Reached maximum retries: {retries}/{retries_max} -- FAILED") 

        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while downloading dataset. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while downloading dataset. See log file, please.")
        else:
            if not self.suppress_print:
                print("Dataset successfully downloaded -- OK!")


    def get_list_of_files_in_dataset(self, 
                                     internal_id: str, 
                                     access: Literal["public", "project", "user"],
                                     project: str, 
                                     zone: str="", 
                                     path: str="",
                                     content_as_pandas: bool=False) -> dict[str] | DataFrame | None:
        """
            List all files within the dataset.

            Parameters
            ----------
            internal_id : str
                InternalID of the dataset. Can be obtain by get_all_datasets() method.
            access : Literal["public", "project", "user"]
                Access of the dataset. One of ["user", "project", "public"]. Can be obtain by get_all_datasets() method.
            project : str
                Project's shortname in which the dataset is stored. Can be obtain by get_all_datasets() method.
            zone : str
                iRODS zone name in which dataset is stored, one of ["IT4ILexisZone", "LRZLexisZone"]. Can be obtain by get_all_datasets() method.
            path : str, optional
                Path within the dataset. By default: path="".
            print_dir_tree: bool, optional
                If True, directory tree will be printed. By default: print_dir_tree=False.
            content_as_pandas: bool, optional
                Convert HTTP response content from JSON to pandas DataFrame. By default: content_as_pandas=False.

            Returns
            -------
            list[dict] | DataFrame | None
                Content of the HTTP request as JSON or as pandas DataFrame if 'content_as_pandas=True'. None if some errors have occured.
            int | None
                Status of the HTTP request. None if some errors have occured.
        """
        is_uuid = check_if_uuid(self.session, internal_id)
        if not is_uuid:
            return None
        
        if zone == "":
            zone = self.session.zone

        if not self.suppress_print:
            print(f"Retrieving data of files in the dataset...")

        url: str = self.session.API_PATH + "/dataset/listing"

        post_body: dict = {
            "internalID": internal_id,
            "access": access,
            "project": project,
            "path": path,
            "recursive": True,
            "zone": zone
        }

        self.session.logging.debug(f"POST -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = None
        is_error: bool = True
        while not status_solved:
            response: Response = req.post(url,
                                            headers=self.session.API_HEADER,
                                            json=post_body)

            content, status_solved, is_error = self.session.handle_request_status(response, 
                                                                                  f"POST -- {url}", 
                                                                                  to_json=True,
                                                                                  suppress_print=self.suppress_print)

            if not is_error and content_as_pandas:
                content = convert_dir_tree_to_pandas(self.session, 
                                                     content, 
                                                     supress_print=self.suppress_print)

                if content is None:
                    is_error = True
                    self.session.logging.error(f"POST -- {url} -- DATAFRAME -- FAILED")
                else:
                    self.session.logging.debug(f"POST -- {url} -- DATAFRAME -- OK")

        if is_error:
            if not self.suppress_print:
                print(f"Some errors occurred while retrieving list of files. See log file, please.")
            if self.session.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while retrieving list of files. See log file, please.")
            return None, None
        else:
            if not self.suppress_print:
                print(f"List of files successfully retrieved (and converted)...")
            if self.print_content:
                print(f"{content}")
            return content, response.status_code
               

    def get_dataset_path(self, 
                         access: Literal["user", "project", "public"], 
                         project: str, 
                         internal_id: str, 
                         username: str="") -> str | None:
        """
            Returns a path for an existing dataset as the combination of access, project, internalID and username.

            Parameters:
            -----------
            access : Literal["public", "project", "user"]
                Access mode of the project (user, project, public)
            project : str
                Project's shortname.
            internal_id : str
                Dataset's internalID as UUID.
            username : str, optional
                The iRODS username. Needed when user access is defined

            Returns:
            --------
            str
                Staging dataset path.

        """
        is_uuid = check_if_uuid(self.session, internal_id)
        if not is_uuid:
            return None
        
        path: str = assemble_dataset_path(access=access, 
                                          project=project, 
                                          internal_id=internal_id, 
                                          username=username)

        return path
