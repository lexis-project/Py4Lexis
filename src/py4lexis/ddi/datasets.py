# datasets.py

import requests as req
from datetime import date, datetime
from py4lexis.ddi.tus_client import TusClient
from tusclient import exceptions
import json


class Datasets:
    def __init__(self, session):
        """
            A class holds methods to manage datasets within LEXIS platform.

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

            Return
            ------
            content: content of the HTTP request as JSON
            req_status: status of the HTTP request
        """
        if push_method is None:
            push_method = "empty"
        if path is None:
            path = ""
        if contributor is None:
            contributor = ["UNKNOWN contributor"]
        if creator is None:
            creator = ["UNKNOWN creator"]
        if owner is None:
            owner = ["UNKNOWN owner"]
        if publicationYear is None:
            publicationYear = str(date.today().year)
        if publisher is None:
            publisher = ["UNKNOWN publisher"]
        if resourceType is None:
            resourceType = "UNKNOWN resource type"
        if title is None:
            title = "UNTITLED_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S")

        self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset -- PROGRESS")
        has_content = False
        content = None
        req_status = None
        status = True
        while not has_content:
            response = req.post(self.session.API_PATH + 'dataset',
                                headers=self.session.API_HEADER,
                                json={
                                    "push_method": push_method,
                                    "access": access,
                                    "project": project,
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
                                )
            content = response.json()
            req_status = response.status_code

            # check token if valid
            if 'message' in content:
                if content['message'] == 'token no longer valid':
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

            Return
            ------
            Prints a progress bar of the processing upload.
        """
        if path is None:
            path = ""
        if file_path is None:
            file_path = "/"
        if contributor is None:
            contributor = ["NONAME contributor"]
        if creator is None:
            creator = ["NONAME creator"]
        if owner is None:
            owner = ["NONAME owner"]
        if publicationYear is None:
            publicationYear = str(date.today().year)
        if publisher is None:
            publisher = ["NONAME publisher"]
        if resourceType is None:
            resourceType = "NONAME resource type"
        if title is None:
            title = "UNTITLED_TUS_Dataset_" + datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
        if expand is None:
            expand = "no"
        if encryption is None:
            encryption = "no"

        status = True
        try:
            # TODO: Refresh token if not valid and repeat
            tus_client = TusClient(self.session.API_PATH + 'transfer/upload/',
                                   headers=self.session.API_HEADER)
            self.session.logging.debug(f"TUS client initialised -- OK")

            uploader = tus_client.uploader(file_path + filename, chunk_size=1048576,
                                           metadata={
                                               'path': path,
                                               'zone': self.session.ZONENAME,
                                               'filename': filename,
                                               'user': self.session.username,
                                               'project': project,
                                               'access': access,
                                               'expand': expand,
                                               'encryption': encryption,
                                               'metadata': json.dumps(
                                                   {
                                                       'contributor': contributor,
                                                       'creator': creator,
                                                       'owner': owner,
                                                       'publicationYear': publicationYear,
                                                       'publisher': publisher,
                                                       'resourceType': resourceType,
                                                       'title': title
                                                   })
                                           }
                                           )
            self.session.logging.debug(f"TUS upload initialised -- OK")
            uploader.upload()

        except exceptions.TusCommunicationError as te:
            self.session.logging.error("Upload error: {0}".format(te.response_content))
            status = False

        if not status:
            print("Some errors occurred. See log file, please.")

    def get_dataset_status(self):
        """
            Get datasets' status

            Return
            ------
            content: content of the HTTP request as JSON
            req_status: status of the HTTP request
        """

        self.session.logging.debug(f"GET -- {self.session.API_PATH}transfer/status -- PROGRESS")
        has_content = False
        content = None
        req_status = None
        status = True
        while not has_content:
            response = req.get(self.session.API_PATH + 'transfer/status',
                               headers={'Authorization': 'Bearer ' + self.session.TOKEN})
            content = response.json()
            req_status = response.status_code

            # check token if valid
            if 'message' in content:
                if content['message'] == 'token no longer valid':
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

        if not status:
            print("Some errors occurred. See log file, please.")

        return content, req_status

    def get_all_datasets(self):
        """
            Get all existing datasets

            Return
            ------
            content: content of the HTTP request as JSON
            req_status: status of the HTTP request
        """
        self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- PROGRESS")
        has_content = False
        content = None
        req_status = None
        status = True
        while not has_content:
            response = req.post(self.session.API_PATH + 'dataset/search/metadata',
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
                    self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- TOKEN -- FAIL")
            else:
                has_content = True
                self.session.logging.debug(f"POST -- {self.session.API_PATH}dataset/search/metadata/ -- OK")

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

            Return
            ------
            content: content of the HTTP request as JSON
            req_status: status of the HTTP request
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
