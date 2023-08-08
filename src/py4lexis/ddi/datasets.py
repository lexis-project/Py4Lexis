# datasets.py

import requests as req
from requests import Response
from datetime import date, datetime
from py4lexis.ddi.tus_client import TusClient
from py4lexis.exceptions import Py4LexisException
from tusclient import exceptions
from pandas import DataFrame
from py4lexis.session import LexisSession
from py4lexis.utils import convert_content_of_get_all_datasets_to_pandas, \
                           convert_content_of_get_datasets_status_to_pandas
from py4lexis.ddi.uploader import Uploader
import json
import time

class Datasets:
    def __init__(self, session: LexisSession, suppress_print: bool=True) -> None:
        """
            A class holds methods to manage datasets within LEXIS platform.

            Attributes
            ----------
            session : class, LEXIS session
            suppress_print: bool, optional if True then all prints are suppressed. By default: suppress_print=True

            Methods
            -------
            create_dataset(access: str, project: str, 
                           push_method: str="empty", 
                           zone: str="IT4ILexisZone",
                           path: str="", 
                           contributor: list[str]=["UNKNOWN contributor"], 
                           creator: list[str]=["UNKNOWN creator"],
                           owner: list[str]=["UNKNOWN owner"], 
                           publicationYear: str=str(date.today().year),
                           publisher: list[str]=["UNKNOWN publisher"], 
                           resourceType: str=str("UNKNOWN resource type"),
                           title: str=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int]
                Create an empty dataset with specified attributes.

            tus_uploader(access: str, project: str, filename: str, 
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
                Create a dataset and upload a data by TUS client.

            get_dataset_status()
                Prints a table of the datasets' staging states.

            get_all_datasets(content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int]
                Prints a table of the all existing datasets.

            delete_dataset_by_id(internal_id, access, project)
                Deletes a dataset by a specified internalID.

            _ddi_submit_download(dataset_id: str, zone: str, access: str, project: str, path: str) -> str:
                Private method to obtain 'requestID' for the dataset download.

            _ddi_get_download_status(request_id: str) -> dict:
                Private method to get download status for the dataset downloading.

            _ddi_download_dataset(request_id: str, destination_file: str, progress_func: callable = None) -> None
                Private method providing download of the dataset.

            download_dataset(acccess: str, project: str, internal_id: str, zone: str,
                             path: str = "", destination_file: str = "./download.tar.gz") -> None:
                Downloads dataset by a specified informtions as access, zone, project, Interna_Id.
                It is possible to specify by path parameter which exact file in the dataset should be downloaded.
                It is popsible to specify local desination folder. Default is set to = "./download.tar.gz"
        """
        self.session = session
        self.suppress_print = suppress_print

    def create_dataset(self, access: str, project: str, 
                       push_method: str="empty", 
                       zone: str="IT4ILexisZone",
                       path: str="", 
                       contributor: list[str]=["UNKNOWN contributor"], 
                       creator: list[str]=["UNKNOWN creator"],
                       owner: list[str]=["UNKNOWN owner"], 
                       publicationYear: str=str(date.today().year),
                       publisher: list[str]=["UNKNOWN publisher"], 
                       resourceType: str=str("UNKNOWN resource type"),
                       title: str=str("UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S"))) -> tuple[dict, int]:
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
                iRODS zone name, one of ["IT4ILexisZone", "LRZLexisZone"]. By default: "IT4ILexisZone".
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
            dict
                Content of the HTTP request as JSON.
            int
                Status of the HTTP request.
        """

        self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset -- PROGRESS")
        has_content: bool = False
        content: dict = {}
        req_status: int = None
        status: bool = True
        while not has_content:
            response: Response = req.post(
                self.session.API_PATH + "dataset",
                headers=self.session.API_HEADER,
                json={
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
                })
            
            content = response.json()
            req_status = response.status_code

            # check token if valid
            if "message" in content:
                if content["message"] == "token no longer valid":
                    self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset -- TOKEN -- FAIL")
                    self.session.logging.debug(f"Refreshing token -- PROGRESS")
                    self.session.refresh_token()
                    self.session.logging.debug(f"Refreshing token -- OK")
                else:
                    has_content = True
                    status = False
                    self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset -- TOKEN -- FAIL")
            else:
                has_content = True
                self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset -- OK")

            if not status:
                print("Some errors occurred. See log file, please.")

        return content, req_status

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

        status: bool = True
        try:
            # TODO: Refresh token if not valid and repeat
            tus_client: TusClient = TusClient(self.session.API_PATH + "transfer/upload/",
                                              headers=self.session.API_HEADER)
            self.session.logging.debug(f"TUS client initialised -- OK")

            uploader: Uploader = tus_client.uploader(
                file_path=file_path + filename, chunk_size=1048576,
                metadata={
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
                })
            
            self.session.logging.debug(f"TUS upload initialised -- OK")
            uploader.upload()

        except exceptions.TusCommunicationError as te:
            self.session.logging.error("Upload error: {0}".format(te.response_content))
            status = False

        if not status:
            print("Some errors occurred. See log file, please.")

    def get_dataset_status(self, content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int]:
        """
            Get datasets' status

            Returns
            -------
            list[dict]
                Content of the HTTP request as list of JSONs
            int
                Status of the HTTP request
        """

        self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/status -- PROGRESS")
        has_content: bool = False
        content: list[dict] = []
        req_status: int = 0
        status: bool = True
        while not has_content:
            response: Response = req.get(self.session.API_PATH + "transfer/status",
                                         headers={"Authorization": "Bearer " + self.session.TOKEN})
            content = response.json()
            req_status = response.status_code

            # check token if valid
            if "message" in content:
                if content["message"] == "token no longer valid":
                    self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/status -- TOKEN -- FAIL")
                    self.session.logging.debug(f"Refreshing token -- PROGRESS")
                    self.session.refresh_token()
                    self.session.logging.debug(f"Refreshing token -- OK")
                else:
                    has_content = True
                    status = False
                    self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/status -- TOKEN -- FAIL")
            else:
                has_content = True
                self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/status -- OK")

        if content is not None and content_as_pandas:
            if 200 <= req_status <= 299:
                content = convert_content_of_get_datasets_status_to_pandas(self.session, content, supress_print=self.suppress_print)
            else:
                self.session.logging.debug(f"Converting datasets to list pandas Dataframe -- FAIL")
                self.session.logging.debug(f"Bad request status: '{req_status}'")
                self.session.logging.debug(f"Printing HTTP request content:")
                self.session.logging.debug(json.dumps(content, indent=4))

                if not self.suppress_print:
                    print(f"Bad request status: '{req_status}'")
                    print(f"Printing HTTP request content:")
                    print(json.dumps(content, indent=4))

        if not status:
            print("Some errors occurred. See log file, please.")

        return content, req_status

    def get_all_datasets(self, content_as_pandas: bool=False) -> tuple[list[dict] | DataFrame, int]:
        """
            Get all existing datasets

            Parameters
            ----------
            content_as_pandas: bool, optional
                By default: content_as_pandas=False

            Returns
            -------
            list[dict] | DataFrame
                Content of the HTTP request as JSON or as pandas DataFrame if content_as_pandas is set to True.
            int
                Status of the HTTP request
        """
        self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- PROGRESS")
        has_content: bool = False
        content: list[dict] | DataFrame = None
        req_status: int = None
        status: bool = True
        while not has_content:
            response: Response = req.post(self.session.API_PATH + 'dataset/search/metadata',
                                          headers=self.session.API_HEADER,
                                          json={})
            content = response.json()
            req_status = response.status_code

            # check token if valid
            if 'message' in content:
                if content['message'] == 'token no longer valid':
                    self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- TOKEN -- FAIL")
                    self.session.logging.debug(f"Refreshing token -- PROGRESS")
                    self.session.refresh_token()
                    self.session.logging.debug(f"Token refreshed -- OK")
                else:
                    has_content = True
                    status = False
                    content = None
                    self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- TOKEN -- FAIL")
            else:
                has_content = True
                self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- OK")

        if content is not None and content_as_pandas:
            if 200 <= req_status <= 299:
                content = convert_content_of_get_all_datasets_to_pandas(self.session, content, supress_print=self.suppress_print)
            else:
                self.session.logging.debug(f"Converting datasets to list pandas Dataframe -- FAIL")
                self.session.logging.debug(f"Bad request status: '{req_status}'")
                self.session.logging.debug(f"Printing HTTP request content:")
                self.session.logging.debug(json.dumps(content, indent=4))

                if not self.suppress_print:
                    print(f"Bad request status: '{req_status}'")
                    print(f"Printing HTTP request content:")
                    print(json.dumps(content, indent=4))

        if not status:
            print("Some errors occurred. See log file, please.")

        return content, req_status

    def delete_dataset_by_id(self, internal_id, access, project):
        """
            Deletes a dataset by a specified internalID.

            Parameters
            ----------
            internal_id: str
                InternalID of the dataset
            access : str
                One of the access types [public, project, user]
            project: str
                Project's short name.

            Returns
            -------
            list[dict] | DataFrame
                Content of the HTTP request as JSON
            int
                Status of the HTTP request
        """
        self.session.logging.debug(f"DELETE -- {self.session.API_PATH}dataset -- ID:{internal_id} -- PROGRESS")
        deleted = False
        content = None
        req_status = None
        status = True
        while not deleted:
            response = req.delete(self.session.API_PATH + 'dataset',
                                  headers=self.session.API_HEADER,
                                  json={
                                      "access": access,
                                      "project": project,
                                      "internalID": internal_id
                                  })
            content = response.json()
            req_status = response.status_code

            # check token if valid
            if 'message' in content:
                if content['message'] == 'token no longer valid':
                    self.session.logging.debug(f"DELETE -- {self.session.API_PATH}dataset -- ID:{internal_id} -- TOKEN -- FAILED")
                    self.session.logging.debug(f"Refreshing token -- PROGRESS")
                    self.session.refresh_token()
                    self.session.logging.debug(f"Refreshing token -- OK")
                else:
                    deleted = True
                    status = False
                    self.session.logging.debug(f"DELETE -- {self.session.API_PATH}dataset -- ID:{internal_id} -- TOKEN -- FAILED")
            else:
                deleted = True
                self.session.logging.debug(f"DELETE -- {self.session.API_PATH}dataset -- ID:{internal_id} -- OK")

        if not status:
            print("Some errors occurred. See log file, please.")

        return content, req_status
    
    def _ddi_submit_download(self, dataset_id: str, zone: str, access: str, project: str, path: str) -> str:
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
            str
                ID of the request.
        """
        download_body: dict = {
            'zone': zone,
            'access': access,
            'project': project,
            'dataset_id': dataset_id,
            'path': path
        }
        
        res: Response = req.post(url=self.session.API_PATH + 'transfer/download',
                                 headers=self.session.API_HEADER,
                                 json=download_body)
        
        if res.status_code != 200:
            self.session.logging.error(f"POST -- {self.session.API_PATH}transfer/download -- ID:{dataset_id} -- FAILED")
            raise Py4LexisException(res.content)
        else:
            self.session.logging.debug(f"POST -- {self.session.API_PATH}transfer/download -- ID:{dataset_id} -- OK")
            return res.json()['requestId']

    def _ddi_get_download_status(self, request_id: str) -> dict:
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

        res: Response = req.get(url=self.session.API_PATH + 'transfer/status/' + request_id, headers=self.session.API_HEADER)
        if res.status_code != 200:
            self.session.logging.error(f"GET -- {self.session.API_PATH}transfer/status -- ID:{request_id} -- FAILED")
            raise Py4LexisException(res.content)
        else:
            self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/status -- ID:{request_id} -- ok")
            return res.json()

    def _ddi_download_dataset(self, request_id: str, destination_file: str, progress_func: callable = None) -> None:
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
        with req.get(url=self.session.API_PATH + 'transfer/download/' + request_id,
                        headers=self.session.API_HEADER,
                        stream=True) as request:
            
            if request.status_code != 200:
                self.session.logging.error(f"GET -- {self.session.API_PATH}transfer/download -- ID:{request_id} -- FAILED")
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
                self.session.logging.error(f"DOWNLOAD -- FAILED")
                raise Py4LexisException(str(ioe))
        
        self.session.logging.debug(f"DOWNLOAD -- OK")

    def download_dataset(self, acccess: str, project: str, internal_id: str, zone: str,
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
                InternalID of the dataset
            zone: str
                iRODS zone name
            path: str, optional
                Path to exact folder, by default  = ""
            destination_file: str, optional
                Path to the local destination folder, by default "./download.tar.gz"


            Returns
            -------
            None
        """

        down_request = self._ddi_submit_download(dataset_id=internal_id, 
                            zone=zone, access=acccess, project=project, path=path)

        # Wait until it is ready
        retries_max: int = 200
        retries: int = 0
        delay: int = 5 # secs
        while retries < retries_max:
            status = self._ddi_get_download_status(request_id=down_request)

            if status['task_state'] == 'SUCCESS':
                # Download file
                self._ddi_download_dataset(request_id=down_request, destination_file=destination_file)
                break

            if status['task_state'] == 'ERROR' or status['task_state'] == "FAILURE":
                self.session.logging.error('DOWNLOAD --  request failed: {0} -- FAILED'.format(status['task_result']))
                raise Py4LexisException(status['task_result'])

            self.session.logging.debug(f"DOWNLOAD -- waiting for download to become ready -- remaining retries {retries} -- OK")    

            # Refresh
            self.session.token_refresh()
            retries = retries + 1
            time.sleep(delay)
        
