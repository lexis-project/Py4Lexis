from keycloak import KeycloakOpenID
import requests as req
from datetime import date, datetime
from py4lexis.tus_client_py4Lexis import TusClient
from tusclient import exceptions
import json

# Making ASCII table
# Source: https://stackoverflow.com/questions/5909873/how-can-i-pretty-print-ascii-tables-with-python
from tabulate import tabulate


class LexisSession:
    """
        A class used to represent an LEXIS API SESSION.

        Attributes
        ----------
        username : str
            LEXIS username
        pwd : str
            LEXIS password
        keycloak_url : str
        realm : str
        client_id : str
        client_secret : str
        ddi_endpoint_url : str
        zonename : str

        Methods
        -------
        get_token()
            Returns the user's keycloak token.

        create_dataset(access, project, push_method=None, path=None, contributor=None, creator=None,
                        owner=None, publicationYear=None, publisher=None, resourceType=None, title=None)
            Create an empty dataset with specified attributes.

        tus_client_uploader(access, project, filename, file_path=None, path=None, contributor=None, creator=None,
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
    def __init__(self, username, pwd, keycloak_url, realm, client_id, client_secret, url, zonename):
        self.username = username
        self.pwd = pwd

        self.KEYCLOAK_URL = keycloak_url
        self.REALM = realm
        self.CLIENT_ID = client_id
        self.CLIENT_SECRET = client_secret
        self.DDI_ENDPOINT_URL = url
        self.ZONENAME = zonename

        # create api url path
        if not url[-1] == '/':
            url = url + '/'

        self.DDI_ENDPOINT_URL = url
        self.API_PATH = url + 'api/v0.2/'

        # check url if valid
        try:
            response = req.get(self.API_PATH)
            print("API URL is successfully initialised!")
        except req.ConnectionError as exception:
            print("Initialisation of API URL failed! Wrong DDI_ENDPOINT_URL!")

        self.keycloak_openid = None
        self.REFRESH_TOKEN = None
        self.TOKEN = None
        self.set_tokens()

        if self.TOKEN is not None:
            self.API_HEADER = {'Authorization': 'Bearer ' + self.TOKEN}
        else:
            print('Access token is not defined!!!')

    def set_tokens(self):
        """
            Set user's access and refresh tokens from keycloak based on defined username + password.
        """
        self.keycloak_openid = KeycloakOpenID(server_url=self.KEYCLOAK_URL + "/auth/",
                                              realm_name=self.REALM,
                                              client_id=self.CLIENT_ID,
                                              client_secret_key=self.CLIENT_SECRET)

        # Get WellKnow
        config_well_known = self.keycloak_openid.well_know()

        # Get tokens
        token = self.keycloak_openid.token(self.username, self.pwd, scope=['openid'])
        self.REFRESH_TOKEN = token['refresh_token']
        self.TOKEN = token['access_token']
        print('Keycloak tokens are successfully initialised!')

    def get_token(self):
        """
            Return
            -------
            User's keycloak access token.
        """
        return self.TOKEN

    def get_refresh_token(self):
        """
            Return
            -------
            User's keycloak refresh token.
        """
        return self.TOKEN

    def refresh_token(self):
        """
            Refresh user's token from keycloak.

            Return
            -------
            User's keycloak token.
        """
        token = self.keycloak_openid.refresh_token(self.REFRESH_TOKEN)
        self.TOKEN = token['access_token']
        self.REFRESH_TOKEN = token['refresh_token']
        self.API_HEADER = {'Authorization': 'Bearer ' + self.TOKEN}

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
            Prints a response content of the POST request.
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
        response = req.post(self.API_PATH + 'dataset',
                            headers=self.API_HEADER,
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
        print(response.content)

    def tus_client_uploader(self, access, project, filename, file_path=None, path=None, contributor=None, creator=None,
                            owner=None, publicationYear=None, publisher=None, resourceType=None, title=None,
                            expand=None, encryption=None):
        """
            Creates a dataset and upload a data by TUS client.

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
            file_path = "./"
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

        print("Initialising TUS client...")
        try:
            tmp_client = TusClient(self.API_PATH + 'transfer/upload/',
                                headers=self.API_HEADER)
            print("Initialising TUS upload...")

            uploader = tmp_client.uploader(file_path + filename, chunk_size=1048576,
                                        metadata={
                                            'path': path,
                                            'zone': self.ZONENAME,
                                            'filename': filename,
                                            'user': self.username,
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
            print("Starting TUS upload...")  
            uploader.upload()
        except exceptions.TusCommunicationError as te:
            print("Upload error: {0}".format(te.response_content))

    def get_dataset_status(self):
        """
            Prints a table of the datasets' staging states.
        """
        print("Sending request...")
        response = req.get(self.API_PATH + '/transfer/status', headers={'Authorization': 'Bearer ' + self.TOKEN})
        tmp_resp = response.content.decode('utf8')
        try:
            print("Formatting response into ASCII table...")
            resp_in_json = json.loads(tmp_resp)
            data_table = []
            for i in range(len(resp_in_json)):
                data_table.append([resp_in_json[i]['filename'], resp_in_json[i]['task_state']])
            print(tabulate(data_table, ["Filename", "State"], tablefmt="grid"))
        except:
            print("Some errors occured!!!")
            print("Saving error to HTML file...")
            error = tmp_resp
            error = error.replace("\\n", "")
            error = error.replace("b'", "")
            error = error.replace("'", "")
            error_file = open('last_error.html', 'w')
            error_file.write(error)
            error_file.close()
            print("Error message is saved in last_error.html.")

    def get_all_datasets(self):
        """
            Prints a table of the all existing datasets.
        """
        print("Sending request...")
        response = req.post(self.API_PATH + 'dataset/search/metadata/',
                            headers=self.API_HEADER,
                            json={})
        tmp_resp = response.content.decode('utf8')
        try:
            print("Formatting response into ASCII table...")
            resp_in_json = json.loads(tmp_resp)
            data_table = []
            for i in range(len(resp_in_json)):
                if 'title' in resp_in_json[i]['metadata']:
                    title = resp_in_json[i]['metadata']['title']
                else:
                    title = 'UKNOWN Title'

                if 'CreationDate' in resp_in_json[i]['metadata']:
                    date_time = resp_in_json[i]['metadata']['CreationDate']
                else:
                    date_time = 'UKNOWN Creation Date'

                if 'access' in resp_in_json[i]['location']:
                    access = resp_in_json[i]['location']['access']
                else:
                    access = 'UKNOWN Access'

                if 'project' in resp_in_json[i]['location']:
                    project = resp_in_json[i]['location']['project']
                else:
                    project = 'UKNOWN Project'

                if 'zone' in resp_in_json[i]['location']:
                    zone = resp_in_json[i]['location']['zone']
                else:
                    zone = 'UKNOWN Zone'

                if 'internalID' in resp_in_json[i]['location']:
                    internalID = resp_in_json[i]['location']['internalID']
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
        except:
            print("Some errors occured!!!")
            print("Saving error to HTML file...")
            error = tmp_resp
            error = error.replace("\\n", "")
            error = error.replace("b'", "")
            error = error.replace("'", "")
            error_file = open('last_error.html', 'w')
            error_file.write(error)
            error_file.close()
            print("Error message is saved in last_error.html.")

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
            ----------
            Prints a response content of the DELETE request.
        """
        response = req.delete(self.API_PATH + 'dataset',
                              headers=self.API_HEADER,
                              json={
                                  "access": access,
                                  "project": project,
                                  "internalID": internal_id
                              })
        print(response.content)





