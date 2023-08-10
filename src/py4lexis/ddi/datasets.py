# datasets.py

from typing import Optional
import requests as req
from requests import Response
from datetime import date, datetime
from py4lexis.ddi.tus_client import TusClient
from tusclient import exceptions
from pandas import DataFrame
from py4lexis.session import LexisSession
from py4lexis.utils import convert_content_of_get_all_datasets_to_pandas, \
                           convert_content_of_get_datasets_status_to_pandas, \
                           convert_content_of_get_list_of_files_in_datasets_to_pandas
from py4lexis.ddi.uploader import Uploader
import json
import time

class Datasets:
    def __init__(self,
                 session: LexisSession,
                 suppress_print: Optional[bool]=True) -> None:
        """
            A class holds methods to manage datasets within LEXIS platform.

            Attributes
            ----------
            session : class
                Class that holds LEXIS session
            suppress_print: bool, optional
                If True then all prints are suppressed. By default: suppress_print=True

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
                           title: Optional[str]=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int] | tuple[None, None]
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

            get_dataset_status(content_as_pandas: Optional[bool]=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
                Prints a table of the datasets' staging states.

            get_all_datasets(content_as_pandas: Optional[bool]=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
                Prints a table of the all existing datasets.

            delete_dataset_by_id(internal_id: str, 
                                 access: str, 
                                 project: str) -> tuple[list[dict], int] | tuple[None, None]
                Deletes a dataset by a specified internalID.

            download_dataset(acccess: str, 
                             project: str, 
                             internal_id: str, 
                             zone: str,
                             path: Optional[str]="",
                             destination_file: Optional[str]="./download.tar.gz") -> None
                Downloads dataset by a specified informtions as access, zone, project, Interna_Id.
                It is possible to specify by path parameter which exact file in the dataset should be downloaded.
                It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"

            get_list_of_files_in_dataset(self, 
                                         internal_id: str, 
                                         access: str,
                                         project: str, 
                                         zone: str, 
                                         path: Optional[str]="",
                                         content_as_pandas: Optional[bool]=False) -> dict[str] | DataFrame:
                List all files within the dataset.
        """
        self.session = session
        self.suppress_print = suppress_print


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
                       title: Optional[str]=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int] | tuple[None, None]:
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

            Returns
            -------
            dict | None
                Content of the HTTP request as JSON. None is returned if some errors have occured.
            int | None
                Status of the HTTP request. None is returned if some errors have occured.
        """
        url: str = self.session.API_PATH + "dataset"
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
        req_status: int = -1
        is_error: bool = False
        try:
            while not status_solved:
                response: Response = req.post(
                        self.session.API_PATH + "dataset",
                        headers=self.session.API_HEADER,
                        json=post_body)
                    
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"POST -- {url} -- CREATE", 
                                                                             self.suppress_print)  
            if not is_error:
                content = response.json()
        
        except json.decoder.JSONDecodeError:
            is_error = False
            self.session.logging.debug(f"POST -- {url} -- CREATE -- JSON response can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"POST -- {url} -- CREATE -- JSON response can't be decoded -- FAILED")

        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
            return None, None
        else:
            return content, req_status
    

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
            Upload a data by TUS client to the specified dataset.

            Parameters
            ----------
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
        url: str = self.session.API_PATH + "transfer/upload/"
        metadata: dict = {
            "path": path,
            "zone": zone,
            "filename": filename,
            "user": self.session.username,
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

        status: bool = True
        try:
            # TODO: Refresh token if not valid and repeat
            tus_client: TusClient = TusClient(url,
                                              headers=self.session.API_HEADER)
            
            self.session.logging.debug(f"TUS UPLOAD -- TUS client initialised -- OK")

            uploader: Uploader = tus_client.uploader(file_path=file_path + filename, 
                                                     chunk_size=1048576,
                                                     metadata=metadata)
            
            self.session.logging.debug(f"TUS UPLOAD -- TUS upload initialised -- OK")
            uploader.upload()

        except exceptions.TusCommunicationError as te:
            status = False
            self.session.logging.error(f"TUS UPLOAD -- Upload error: {te.response_content} -- FAILED")
            if not self.suppress_print:
                print(f"TUS UPLOAD -- Upload error: {te.response_content} -- FAILED")

        if not status:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")


    def get_dataset_status(self, 
                           content_as_pandas: Optional[bool]=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
        """
            Get datasets' status information.

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
        url: str = self.session.API_PATH + "transfer/status"

        self.session.logging.debug(f"GET -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = []
        req_status: int = -1
        is_error: bool = True
        try:
            while not status_solved:
                response: Response = req.get(url,
                                             headers={"Authorization": "Bearer " + self.session.TOKEN})
                
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"GET -- {url}", 
                                                                             self.suppress_print)        
                        
            if not is_error:
                    content = response.json()

            if not is_error and content_as_pandas:
                content = convert_content_of_get_datasets_status_to_pandas(self.session, 
                                                                           content, 
                                                                           supress_print=self.suppress_print)

                if content is None:
                    is_error = True
                    self.session.logging.debug(f"GET -- {url} -- DATAFRAME -- FAILED")
                    
                    if not self.suppress_print:
                        print(f"GET -- {url} -- DATAFRAME -- FAILED")
                else:
                    self.session.logging.debug(f"GET -- {url} -- DATAFRAME -- OK")            

        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"GET -- {url} -- JSON response can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"GET -- {url} -- JSON response can't be decoded -- FAILED")

        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
            return None, None
        else:
            return content, req_status


    def get_all_datasets(self, 
                         content_as_pandas: Optional[bool]=False) -> tuple[list[dict] | DataFrame, int] | tuple[None, None]:
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
        url: str = self.session.API_PATH + "dataset/search/metadata/"

        self.session.logging.debug(f"POST -- {url} -- PROGRESS")
        status_solved: bool = False
        content: list[dict] | DataFrame | None = None
        req_status: int = -1
        is_error: bool = True
        try:
            while not status_solved:
                response: Response = req.post(url,
                                              headers=self.session.API_HEADER,
                                              json={})
                
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"POST -- {url}", 
                                                                             self.suppress_print)
            
            if not is_error:
                content = response.json()

            if not is_error and content_as_pandas:
                content = convert_content_of_get_all_datasets_to_pandas(self.session, 
                                                                        content, 
                                                                        supress_print=self.suppress_print)

                if content is None:
                    is_error = True
                    self.session.logging.debug(f"POST -- {url} -- DATAFRAME -- FAILED")
                    
                    if not self.suppress_print:
                        print(f"POST -- {url} -- DATAFRAME -- FAILED")
                else:
                    self.session.logging.debug(f"POST -- {url} -- DATAFRAME -- OK")
    
        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"POST -- {url} -- JSON response can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"POST -- {url} -- JSON response can't be decoded -- FAILED")

        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
            return None, None
        else:
            return content, req_status


    def delete_dataset_by_id(self, 
                             internal_id: str, 
                             access: str, 
                             project: str) -> tuple[list[dict], int] | tuple[None, None]:
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
            list[dict] | None
                Content of the HTTP request as JSON. None if some errors have occured.
            int | None
                Status of the HTTP request. None if some errors have occured.
        """
        url: str = self.session.API_PATH + "dataset"
        delete_body: dict = {
            "access": access,
            "project": project,
            "internalID": internal_id
        }

        self.session.logging.debug(f"DELETE -- {url} -- ID:{internal_id} -- PROGRESS")
        status_solved: bool = False
        content: dict = {}
        req_status: int = -1
        is_error: bool = False
        try:
            while not status_solved:
                response = req.delete(url,
                                      headers=self.session.API_HEADER,
                                      json=delete_body)
                
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"DELETE -- {url} -- ID:{internal_id}", 
                                                                             self.suppress_print)
            if not is_error:
                content = response.json()

        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"DELETE -- {url} -- ID:{internal_id} -- JSON response can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"DELETE -- {url} -- ID:{internal_id} -- JSON response can't be decoded -- FAILED")
        
        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
            return None, None
        else:
            return content, req_status
    

    def _ddi_submit_download(self, 
                             dataset_id: str, 
                             zone: str, 
                             access: str, 
                             project: str, 
                             path: str) -> str | None:
        """
            Private method to obtain 'requestID' for the dataset download.

            Parameters
            ----------
            dataset_id : str
                InternalID of the dataset.
            zone : str
                Zone of the dataset.
            access : str
                Access of the dataset. One of the access types [public, project, user].
            project : str
                Project's short name.

            Returns
            -------
            str | None
                ID of the request. None if some errors occur
        """
        url: str = self.session.API_PATH + "transfer/download"
        download_body: dict = {
            "zone": zone,
            "access": access,
            "project": project,
            "dataset_id": dataset_id,
            "path": path
        }
        
        status_solved: bool = False
        is_error: bool = False
        content: dict = {}
        req_status: int = -1
        try:
            while not status_solved:
                response: Response = req.post(url,
                                              headers=self.session.API_HEADER,
                                              json=download_body)
                
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"POST -- {url} -- DATA_ID:{dataset_id}", 
                                                                             self.suppress_print)
            
            if not is_error:
                request_id: str = content["request_id"]

        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"POST -- {url} -- DATA_ID:{dataset_id} -- JSON response of can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"POST -- {url} -- DATA_ID:{dataset_id} -- JSON response of can't be decoded -- FAILED")

        except KeyError as kerr:
            is_error = True
            self.session.logging.debug(f"POST -- {url} -- DATA_ID:{dataset_id} -- Wrong or missing key '{kerr}' in response -- FAILED")
            
            if not self.suppress_print:
                print(f"POST -- {url} -- DATA_ID:{dataset_id} -- Wrong or missing key '{kerr}' in response -- FAILED")
        
        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
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
        url: str = self.session.API_PATH + "transfer/status/" + request_id

        status_solved: bool = False
        is_error: bool = False
        content: dict = {}
        req_status: int = -1
        try:
            while not status_solved:
                response: Response = req.get(url, headers=self.session.API_HEADER)
                
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"GET -- {url} -- REQ_ID:{request_id}", 
                                                                             self.suppress_print)

            if not is_error:
                content = response.json()

        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"GET -- {url} -- REQ_ID:{request_id} -- JSON response of can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"GET -- {url} -- REQ_ID:{request_id} -- JSON response of can't be decoded -- FAILED")
        
        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
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
        url: str = self.session.API_PATH + "transfer/download/" + request_id

        status_solved: bool = False
        is_error: bool = False
        content: dict = {}
        req_status: int = -1
        try:
            while not status_solved:
                response: Response = req.get(url,
                                             headers=self.session.API_HEADER,
                                             stream=True)
                
                req_status = response.status_code

                if 200 <= req_status <= 299:
                    status_solved = True
                    is_error = False
                else:
                    content = response.json()
                    status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                                 content, 
                                                                                 f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD", 
                                                                                 self.suppress_print)
            
            # File ready, start downloading
            total_length = response.headers.get("content-length")
            self.session.logging.debug(f"GET -- {url} -- REQ_ID:{request_id} -- STARTING DOWNLOAD -- OK")

            with open(destination_file, "wb") as f:
                if total_length is None: # no content length header
                    f.write(response.content)
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

        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- JSON response of can't be decoded -- FAILED")

            if not self.suppress_print:
                    print(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- JSON response of can't be decoded -- FAILED")

        except KeyError as kerr:
            is_error = True
            self.session.logging.debug(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- Wrong or missing key '{kerr}' in response -- FAILED")
            
            if not self.suppress_print:
                print(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- Wrong or missing key '{kerr}' in response -- FAILED")

        except IOError as ioe:
            is_error = True
            self.session.logging.error(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- FAILED")
            self.session.logging.error(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- ERROR -- {ioe}")
            
            if not self.suppress_print:
                print(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- FAILED")
                print(f"GET -- {url} -- REQ_ID:{request_id} -- DOWNLOAD -- ERROR -- {ioe}")
        
        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
        
        '''
        with req.get(url=self.session.API_PATH + 'transfer/download/' + request_id,
                        headers=self.session.API_HEADER,
                        stream=True) as request:
            
            if request.status_code != 200:
                self.session.logging.error(f"GET -- {self.session.API_PATH}transfer/download -- ID:{request_id} -- DOWNLOAD -- FAILED")
                raise Py4LexisException(request.content)
            
            # File ready, start downloading
            total_length = request.headers.get('content-length')
            self.session.logging.debug(f"STARTING DOWNLOADING -- OK")
            try:
                with open(destination_file, "wb") as f:
                    if total_length is None: # no content length header
                        f.write(request.content)
                    else:
                        dl = 0
                        total_length = int(total_length)
                        current: int = int(0)
                        chunk_size: int = int(4096)
                        for data in request.iter_content(chunk_size=chunk_size):
                            if progress_func:
                                progress_func(current, total_length, prefix='Progress: ', suffix='Downloaded', length=50)
                                current += chunk_size
                            dl += len(data)
                            f.write(data)
                        if progress_func:
                            progress_func(total_length, total_length, prefix='Progress: ', suffix='Downloaded', length=50)
                        
            except IOError as ioe:
                self.session.logging.error(f"GET -- {self.session.API_PATH}transfer/download -- ID:{request_id} -- DOWNLOAD -- FAILED")
                raise Py4LexisException(str(ioe))
        
        self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/download -- ID:{request_id} -- DOWNLOAD -- OK")
        '''


    def download_dataset(self, 
                         acccess: str, 
                         project: str, 
                         internal_id: str, 
                         zone: str,
                         path: Optional[str]="",
                         destination_file: Optional[str]="./download.tar.gz") -> None:
        """
            Downloads dataset by a specified information as access, zone, project, InternalID.
            It is possible to specify by path parameter which exact file in the dataset should be downloaded.
            It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"
            
            Parameters
            ----------
            access : str
                One of the access types [public, project, user]. Can be obtain by get_all_datasets() method.
            project: str
                Project's short name in which dataset is stored. Can be obtain by get_all_datasets() method.
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

        down_request = self._ddi_submit_download(dataset_id=internal_id, 
                                                 zone=zone, 
                                                 access=acccess,
                                                 project=project, 
                                                 path=path)

        # Wait until it is ready
        retries_max: int = 200
        retries: int = 0
        delay: int = 5 # secs
        is_error: bool = False
        while retries < retries_max:
            status = self._ddi_get_download_status(request_id=down_request)

            if status["task_state"] == "SUCCESS":
                # Download file
                self._ddi_download_dataset(request_id=down_request, destination_file=destination_file)
                break

            if status["task_state"] == "ERROR" or status["task_state"] == "FAILURE":
                is_error = True
                self.session.logging.error(f"DOWNLOAD -- REQ_ID:{down_request} -- request failed: {status['task_result']} -- FAILED")

                if not self.suppress_print:
                    print(f"DOWNLOAD -- REQ_ID:{down_request} -- request failed: {status['task_result']} -- FAILED")
                break

            self.session.logging.debug(f"DOWNLOAD -- REQ_ID:{down_request} -- waiting for download to become ready -- remaining retries {retries}/{retries_max} -- OK")    

            # Refresh
            self.session.refresh_token()
            retries = retries + 1
            time.sleep(delay)
        
        if retries == retries_max and not is_error:
            is_error = True
            self.session.logging.debug(f"DOWNLOAD -- REQ_ID:{down_request} -- Reached maximum retries: {retries}/{retries_max} -- FAILED")    

            if not self.suppress_print:
                print(f"DOWNLOAD -- REQ_ID:{down_request} -- Reached maximum retries: {retries}/{retries_max} -- FAILED")

        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")


    def get_list_of_files_in_dataset(self, 
                                     internal_id: str, 
                                     access: str,
                                     project: str, 
                                     zone: str, 
                                     path: Optional[str]="",
                                     content_as_pandas: Optional[bool]=False) -> dict[str] | DataFrame:
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
            path : str, optional
                Path within the dataset. By default: path="".
            content_as_pandas: bool, optional
                Convert HTTP response content from JSON to pandas DataFrame. By default: content_as_pandas=False

            Returns
            -------
            list[dict] | DataFrame | None
                Content of the HTTP request as JSON or as pandas DataFrame if 'content_as_pandas=True'. None if some errors have occured.
            int | None
                Status of the HTTP request. None if some errors have occured.
        """
        url: str = self.session.API_PATH + "dataset/listing/"

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
        req_status: int = -1
        is_error: bool = True
        try:
            while not status_solved:
                response: Response = req.post(url,
                                              headers=self.session.API_HEADER,
                                              json=post_body)
                
                req_status = response.status_code

                status_solved, is_error = self.session.handle_request_status(req_status, 
                                                                             content, 
                                                                             f"POST -- {url}", 
                                                                             self.suppress_print)

            if not is_error:
                content = response.json()

            if not is_error and content_as_pandas:
                content = convert_content_of_get_list_of_files_in_datasets_to_pandas(self.session, 
                                                                                     content, 
                                                                                     supress_print=self.suppress_print)

                if content is None:
                    is_error = True
                    self.session.logging.debug(f"POST -- {url} -- DATAFRAME -- FAILED")
                    
                    if not self.suppress_print:
                        print(f"POST -- {url} -- DATAFRAME -- FAILED")
                else:
                    self.session.logging.debug(f"POST -- {url} -- DATAFRAME -- OK")
    
        except json.decoder.JSONDecodeError:
            is_error = True
            self.session.logging.debug(f"POST -- {url} -- JSON response can't be decoded -- FAILED")

            if not self.suppress_print:
                print(f"POST -- {url} -- JSON response can't be decoded -- FAILED")

        if is_error:
            if self.suppress_print:
                print("Some errors occurred. See log file, please.")
            return None, None
        else:
            return content, req_status
        
