from __future__ import annotations
import logging
from typing import Optional
from requests import Response, get
from py4lexis.exceptions import Py4LexisAuthException, Py4LexisException, Py4LexisPostException
from getpass import getpass
from py4lexis.kck_session import kck_oi
from py4lexis.helper import Clr, sfouiro, _itbbra
from requests import get
from urllib3 import disable_warnings
from time import perf_counter
import json

disable_warnings()


class LexisSession(object):

    def __init__(self, 
                 show_prints: Optional[bool] = True,
                 exception_on_error: Optional[bool] = False,
                 log_file: Optional[str]="./lexis_logs.log") -> None:
        """
            A class holds an LEXIS SESSION.

            Attributes
            ----------
            show_prints: bool, optional
                If True, messages will be printed.
            exception_on_error : bool, optional
                If True, the exception will be raised on error. By default is set to False.
            log_file : str, optional
                path to a log file. DEFAULT: "./lexis_logs.log"

            Methods
            -------
            get_token() -> str
                Returns the user's token.

            get_refresh_token() -> str
                Returns the user's refresh token.

            refresh_token() -> bool
                Refresh the user's tokens

            handle_request_status(self, 
                                  response: Response, 
                                  log_msg: str, 
                                  to_json: bool = True,
                                  suppress_print: Optional[bool]=True) -> tuple[dict | bytes, bool, bool]
                Method which handles request status. If token is invalid then it tries to refresh it.
        """
        # Exception on error
        self.exception_on_error = exception_on_error
        self.Clr = Clr()

        # Initialise logging
        self.show_prints: bool = show_prints
        self.log_path: str = log_file
        self.logging = logging
        self.logging.basicConfig(filename=log_file,
                                 filemode="w",
                                 level=logging.DEBUG,
                                 format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

        # Prepare tokens
        self.uc = kck_oi()
        self.REFRESH_TOKEN: str = ""
        self.TOKEN: str = ""
        self._token_retrieved_at = 0
        self._token_expiration = 0
        self._refresh_expiration = 0

        self.USERNAME: str = ""
        self.DFLT_Z = self.Clr.get("Z")
        self.API_AIR: str = self.Clr.get("AIR")
        self.API_PATH: str = self.Clr.get("API")

        # check API if valid
        response = get(self.API_PATH)
        self.logging.debug(f"Initialise API path '{self.API_PATH}' -- OK")      
        
        is_error: bool = False
        # Initialise LEXIS session
        self._set_tokens()

        # Prepare requests header
        if self.TOKEN is not None:
            self.API_HEADER = {
                "Authorization": "Bearer " + self.TOKEN,
                "Content-type": "application/json"
            }
            self.logging.debug(f"Set api_header -- OK")
        else:
            self.logging.error(f"Set api_header -- FAILED")
            is_error = False

        if is_error:
                if self.exception_on_error:
                    raise Py4LexisException(f"Some errors occurred. See log file, please.")
                else:
                    print(f"Some errors occurred. See log file, please.")

    def _set_tokens(self) -> None:
        """
            Set user's access and refresh tokens based on defined username + password.

            Returns
            -------
            None
        """
        is_error: bool = False
        try:
            print(f"Welcome to the Py4Lexis!")
            print(f"Please provide your credentials...")
            self.USERNAME: str = input("Username: ")
            pwd: str = getpass()
            token: dict | dict[str, str] = self.uc.token(self.USERNAME, pwd)

            self.REFRESH_TOKEN = token["refresh_token"]
            self.TOKEN = token["access_token"]
            self._token_expiration = token["expires_in"]
            self._refresh_expiration = token["refresh_expires_in"]
            self._token_retrieved_at = perf_counter()
            self.logging.debug(f"POST -- AUTH -- TOKEN -- OK")

        except Py4LexisAuthException:
            is_error = True
            if self.show_prints:
                print(f"Invalid user credentials! Cannot be logged in!")
            
            self.logging.error("AUTH -- INVALID USER CREDENTIALS -- FAILED")    
        
        except Py4LexisPostException as err:
            is_error = True
            self.logging.error(f"POST -- AUTH -- TOKEN -- FAILED")
            self.logging.error(f"POST -- AUTH -- TOKEN -- STATUS -- {err.response_code} -- FAILED")
            self.logging.error(f"POST -- AUTH -- TOKEN -- MESSAGE -- {err.error_message} -- FAILED")

        except KeyError as kerr:
            is_error = True
            self.logging.error(f"POST -- AUTH -- TOKEN -- FAILED")
            self.logging.error(f"POST -- AUTH -- TOKEN -- Missing key '{kerr}' -- FAILED")

        if is_error:
            if self.exception_on_error:
                raise Py4LexisException(f"Some errors occurred. See log file, please.")
            else:
                print(f"Some errors occurred. See log file, please.")
        else:
            if self.show_prints:
                print(f"You have been successfully logged in LEXIS session.")


    def get_token(self) -> str:
        """
            Returns
            -------
            str
                User's access token.
        """
        return self.TOKEN

    def get_refresh_token(self) -> str:
        """
            Returns
            -------
            str
                User's refresh token.
        """
        return self.REFRESH_TOKEN
    
    def check_token(self):
        now: float = perf_counter()
        elapsed: float = now - self._token_retrieved_at

        if elapsed < self._token_expiration and elapsed < self._refresh_expiration:
            pass
        elif elapsed >= self._token_expiration and elapsed < self._refresh_expiration:
            self.refresh_token()
        else:
            print(f"elapsed: {elapsed}")
            print(f"token exp: {self._token_expiration}")
            print(f"refrsh exp: {self._refresh_expiration}")
            if self.exception_on_error:
                raise Py4LexisException("Token has expired and can't be refreshed. Please, relog the session.")
            
            if self.show_prints:
                print(f"Token has expired and can't be refreshed. Please, relog the session.")

    def refresh_token(self) -> bool:
        """
            Refresh user's token.

            Returns
            -------
            bool
                True if the tokens were successfully refreshed. Otherwise False.
        """
        is_error: bool = False
        try:
            tokens: dict = {}
            self.logging.debug(f"POST -- AUTH -- REFRESH TOKEN -- PROGRESS")
            tokens = self.uc.rfsh_token(self.get_refresh_token())                      
            self.TOKEN = tokens["access_token"]
            self.REFRESH_TOKEN = tokens["refresh_token"]
            self._token_retrieved_at = perf_counter()
            self.API_HEADER = {"Authorization": "Bearer " + self.TOKEN}
            self.logging.debug(f"POST -- AUTH -- REFRESH TOKEN -- OK")

        except Py4LexisPostException as err:
            is_error = True
            self.logging.error(f"POST -- AUTH -- REFRESH TOKEN -- Probably refresh token is not valid anymore -- FAILED")
            self.logging.error(f"POST -- AUTH -- REFRESH TOKEN -- STATUS -- {err.response_code} -- FAILED")
            self.logging.error(f"POST -- AUTH -- REFRESH TOKEN -- MESSAGE -- {err.error_message} -- FAILED")

        except KeyError as kerr:
            is_error = True
            self.logging.error(f"POST -- AUTH -- REFRESH TOKEN -- FAILED")
            self.logging.error(f"POST -- AUTH -- REFRESH TOKEN -- Missing key '{kerr}' -- FAILED")

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
                if response.status_code == 404 or response.status_code >= 500:
                    is_error = True
                    status_solved = True
                    content = response.content
                else:
                    content = response.json()
                    if "errorString" in content.keys():
                        if content["errorString"] == "Inactive token":
                            self.logging.error(log_msg + " -- TOKEN -- FAILED")
                            is_refreshed: bool = self.refresh_token()
                            if is_refreshed:
                                status_solved = False
                                is_error = False
                                self.logging.debug(log_msg + " -- REFRESH TOKEN -- OK")
                            else:
                                status_solved = True
                                is_error = True
                                self.logging.error(log_msg + " -- REFRESH TOKEN -- FAILED")
                                if not suppress_print:
                                    print(log_msg + " -- REFRESH TOKEN -- FAILED")
                        else:
                            status_solved = True
                            is_error = True
                            self.logging.error(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED")
                            self.logging.debug(content)

                            if not suppress_print:
                                print(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED")
                    else:
                        status_solved = True
                        is_error = True
                        self.logging.error(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED")
                        self.logging.debug(content)

                        if not suppress_print:
                            print(log_msg + f" -- Bad request status: '{response.status_code}' -- FAILED") 
        
        except json.decoder.JSONDecodeError:
            is_error = True
            self.logging.error(log_msg + f" -- JSON response can't be decoded -- FAILED")

            if not suppress_print:
                print(log_msg + f" -- JSON response can't be decoded -- FAILED")
        
        return content, status_solved, is_error


class iRODSSession(LexisSession):
    """
        Class that holds the iRODS Lexis Session.

        TODO: STILL UNDER DEVELOPMENT
    """
    
    def __init__(self, config_file: str | None = None, exception_on_error: bool = False, log_file: str | None = "./lexis_logs.log") -> None:
        super().__init__(config_file, exception_on_error, log_file)
    
        self.irods = self.get_session()

    
    def validate_irods(self):
        """
            Validate token on all iRODS broker.
        """
        try:
            status_solved: bool = False
            content: list[dict] | None = None
            is_error: bool = True
            while not status_solved:          
                response: Response = get(self.Clr.yhbrr(sfouiro) + "/validate_token",
                                        params={
                                            "provider": self.Clr.yhbrr(_itbbra),
                                            "access_token": "Bearer " + self.TOKEN
                                        }, verify=False)
                
                content, status_solved, is_error = self.handle_request_status(response, 
                                                                              f"GET -- {self.Clr.yhbrr(sfouiro)}", 
                                                                              to_json=False,
                                                                              suppress_print=True if not self.show_prints else False)
                
                if is_error:
                    if self.show_prints:
                        print(f"Some errors occurred while validating token on iRODS. See log file, please.")
                    
                    if self.exception_on_error:
                        raise Py4LexisException(f"Some errors occurred while validating token on iRODS. See log file, please.")
                    else:
                        return is_error
                else:
                    if self.show_prints:
                        print(f"Validate token on iRODS was successfull...")
                    else:
                        self.logging.debug("GET -- VALIDATE IRODS -- OK")

                    return is_error
            
        except Exception as ke:
            self.logging.error("GET -- VALIDATE IRODS -- Error when connecting to Keycloak: {0}".format(ke))
            raise Py4LexisException


    def get_session(self):
        """
            Retrieve iRODS session.
        """

        is_error = self.validate_irods()
        if not is_error and is_error is not None:
            session = self.uc.get_irods_session(self.USERNAME, self.TOKEN)

            if self.show_prints:
                print(f"The iRODS session was successfully initialised.")
            
            self.logging.debug("iRODS -- INITIALISED -- OK")

            return session
        else:
            if self.show_prints:
                print(f"Some problems occurred while initialising iRODS session. Please, see log file.")
            
            self.logging.error("iRODS -- INITIALISED -- FAILED")       