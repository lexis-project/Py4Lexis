import logging
from typing import Optional
from requests import Response, get, ConnectionError
from py4lexis.exceptions import Py4LexisException
import json
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from keycloak import KeycloakOpenID
from keycloak.exceptions import KeycloakPostError


class LexisSession:

    def __init__(self, 
                 config_file: str, 
                 exception_on_error: bool = False,
                 log_file: Optional[str]="./lexis_logs.log") -> None:
        """
            A class holds an LEXIS API SESSION.

            Attributes
            ----------
            config_file : str, 
                path to a config file
            exception_on_error : bool, optional
                If True, the exception will be raised on error. By default is set to False.
            log_file : str, optional
                path to a log file. DEFAULT: "./lexis_logs.log"

            Methods
            -------
            get_token() -> str
                Returns the user's keycloak token.

            get_refresh_token() -> str
                Returns the user's refresh keycloak token.

            refresh_token() -> bool
                Refresh the user's keycloak tokens

            handle_request_status(self, 
                                  response: Response, 
                                  log_msg: str, 
                                  to_json: bool = True,
                                  suppress_print: Optional[bool]=True) -> tuple[dict | bytes, bool, bool]
                Method which handles request status. If token is invalid then it tries to refresh it.
        """
        # Exception on error
        self.exception_on_error = exception_on_error

        # Initialise logging
        self.log_path: str = log_file
        self.logging = logging
        self.logging.basicConfig(filename=log_file,
                                 filemode="w",
                                 level=logging.DEBUG,
                                 format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

        # Prepare tokens and Keycloak session
        self.keycloak_openid: KeycloakOpenID | None = None
        self.REFRESH_TOKEN: str = ""
        self.TOKEN: str = ""

        is_error: bool = False
        # Load config file and initialise LEXIS session
        try:
            with open(config_file, "rb") as _toml_file:
                _cfg = tomllib.load(_toml_file)

            self.username: str = str(_cfg["config"]["USERNAME"])
            self._set_tokens(str(_cfg["config"]["PWD"]),
                             str(_cfg["config"]["CLIENT_ID"]),
                             str(_cfg["config"]["CLIENT_SECRET"]),
                             str(_cfg["config"]["REALM"]),
                             str(_cfg["config"]["KEYCLOAK_URL"]))

            self.ZONENAME: str = str(_cfg["config"]["ZONENAME"])

            _ddi_endpoint_url: str = str(_cfg["config"]["DDI_ENDPOINT_URL"])
            if _ddi_endpoint_url[-1] == "/":
                _ddi_endpoint_url = _ddi_endpoint_url[:-1]

            # create api url path
            self.DDI_ENDPOINT_URL = _ddi_endpoint_url
            self.API_PATH = self.DDI_ENDPOINT_URL + "/api/v0.2"
            self.logging.debug(f"Reading {config_file} -- OK")

            # check url if valid
            response = get(self.API_PATH)
            self.logging.debug(f"Initialise '{self.API_PATH}' -- OK")

            # Prepare requests header
            if self.TOKEN is not None:
                self.API_HEADER = {'Authorization': 'Bearer ' + self.TOKEN,
                                'Content-type': 'application/json'}
                self.logging.debug(f"Set api_header -- OK")

            else:
                self.logging.debug(f"Set api_header -- FAILED")
                is_error = False

        except FileNotFoundError:
            is_error = True
            self.logging.error(f"Reading '{config_file}' -- FAILED")
        
        except ConnectionError:
            is_error = True
            self.logging.error(f"Initialise '{self.API_PATH}' -- FAILED")

        if is_error:
            if self.exception_on_error:
                raise Py4LexisException(f"Some errors occurred. See log file, please.")
            else:
                print(f"Some errors occurred. See log file, please.")


    def _set_tokens(self, 
                    pwd: str, 
                    client_id: str, 
                    client_secret: str, 
                    realm: str, 
                    keycloak_url: str) -> None:
        """
            Set user's access and refresh tokens from keycloak based on defined username + password.

            Returns
            -------
            None
        """
        is_error: bool = False
        try:
            self.keycloak_openid = KeycloakOpenID(server_url=keycloak_url + "/auth/",
                                                  realm_name=realm,
                                                  client_id=client_id,
                                                  client_secret_key=client_secret)

            # Get WellKnow
            config_well_known = self.keycloak_openid.well_known()

            # Get tokens
            token: dict | dict[str, str] = self.keycloak_openid.token(self.username, pwd, scope=["openid"])
            self.REFRESH_TOKEN = token["refresh_token"]
            self.TOKEN = token["access_token"]
            self.logging.debug(f"POST -- KEYCLOACK -- TOKEN -- OK")
        
        except KeycloakPostError as err:
            is_error = True
            self.logging.error(f"POST -- KEYCLOACK -- TOKEN -- FAILED")
            self.logging.error(f"POST -- KEYCLOACK -- TOKEN -- STATUS -- {err.response_code} -- FAILED")
            self.logging.error(f"POST -- KEYCLOACK -- TOKEN -- MESSAGE -- {err.error_message} -- FAILED")

        except KeyError as kerr:
            is_error = True
            self.logging.error(f"POST -- KEYCLOACK -- TOKEN -- FAILED")
            self.logging.error(f"POST -- KEYCLOACK -- TOKEN -- Missing key '{kerr}' -- FAILED")

        if is_error:
            if self.exception_on_error:
                raise Py4LexisException(f"Some errors occurred. See log file, please.")
            else:
                print(f"Some errors occurred. See log file, please.")


    def get_token(self) -> str:
        """
            Returns
            -------
            str
                User's keycloak access token.
        """
        return self.TOKEN

    def get_refresh_token(self) -> str:
        """
            Returns
            -------
            str
                User's keycloak refresh token.
        """
        return self.REFRESH_TOKEN

    def refresh_token(self) -> bool:
        """
            Refresh user's token from keycloak.

            Returns
            -------
            bool
                True if the tokens were successfully refreshed. Otherwise False.
        """
        is_error: bool = False
        try:
            tokens: dict = {}
            self.logging.error(f"POST -- KEYCLOACK -- REFRESH TOKEN -- PROGRESS")
            tokens = self.keycloak_openid.refresh_token(self.get_refresh_token())                      
            self.TOKEN = tokens["access_token"]
            self.REFRESH_TOKEN = tokens["refresh_token"]
            self.API_HEADER = {"Authorization": "Bearer " + self.TOKEN}
            self.logging.debug(f"POST -- KEYCLOACK -- REFRESH TOKEN -- OK")

        except KeycloakPostError as err:
            is_error = True
            self.logging.error(f"POST -- KEYCLOACK -- REFRESH TOKEN -- Probably refresh token is not valid anymore -- FAILED")
            self.logging.error(f"POST -- KEYCLOACK -- REFRESH TOKEN -- STATUS -- {err.response_code} -- FAILED")
            self.logging.error(f"POST -- KEYCLOACK -- REFRESH TOKEN -- MESSAGE -- {err.error_message} -- FAILED")

        except KeyError as kerr:
            is_error = True
            self.logging.error(f"POST -- KEYCLOACK -- REFRESH TOKEN -- FAILED")
            self.logging.error(f"POST -- KEYCLOACK -- REFRESH TOKEN -- Missing key '{kerr}' -- FAILED")

        if is_error:
            if self.exception_on_error:
                raise Py4LexisException(f"Some errors occurred while handling Keycloak tokens. See log file, please.")
            else:
                print(f"Some errors occurred while handling Keycloak tokens. See log file, please.")            
            return False
        else:
            return True
        
    def handle_request_status(self, 
                              response: Response, 
                              log_msg: str, 
                              to_json: bool = True,
                              suppress_print: Optional[bool]=True) -> tuple[dict | bytes, bool, bool]:
        """
            Method which handles request status. If token is invalid then it tries to refresh it.

            Parameters
            ----------
            response : Response
                HTTP Request response.
            log_msg : str
                Message for the logger.
            to_json : bool, optional
                Convert content of response to JSON. By default is set to True.
            suppress_print : bool, optional
                If True, the errors prints to console will be suppressed.
            
            Returns
            -------
            bool
                request_solved: If True, request status is solved.
            bool
                is_error: If True, some errors have occured.
        """

        status_solved: bool = False
        content: dict | bytes = {}
        is_error: bool = False
        try:
            if 200 <= response.status_code <= 299:
                status_solved = True
                is_error = False
                if to_json:
                    content = response.json()
                else:
                    content = response.content

                self.logging.debug(log_msg + " -- OK")
            else:
                if response.status_code == 404:
                    is_error = True
                    status_solved = True
                    content = response.content
                else:
                    content = response.json()
                    if "errorString" in content.keys():
                        if content["errorString"] == "Inactive token":
                            self.logging.debug(log_msg + " -- TOKEN -- FAILED")
                            is_refreshed: bool = self.refresh_token()
                            if is_refreshed:
                                status_solved = False
                                is_error = False
                                self.logging.debug(log_msg + " -- REFRESH TOKEN -- OK")
                            else:
                                status_solved = True
                                is_error = True
                                self.logging.debug(log_msg + " -- REFRESH TOKEN -- FAILED")
                                if not suppress_print:
                                    print(log_msg + " -- REFRESH TOKEN -- FAILED")
                        else:
                            status_solved = True
                            is_error = True
                            self.logging.debug(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED")
                            self.logging.debug(content)

                            if not suppress_print:
                                print(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED")
                    else:
                        status_solved = True
                        is_error = True
                        self.logging.debug(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED")
                        self.logging.debug(content)

                        if not suppress_print:
                            print(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED") 
        
        except json.decoder.JSONDecodeError:
            is_error = True
            self.logging.debug(log_msg + f" -- JSON response can't be decoded -- FAILED")

            if not suppress_print:
                print(log_msg + f" -- JSON response can't be decoded -- FAILED")
        
        return content, status_solved, is_error