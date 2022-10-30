import logging
import requests as req

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from keycloak import KeycloakOpenID
from keycloak.exceptions import KeycloakGetError


class LexisSession:

    def __init__(self, config_file: str, log_file: str = "./lexis_logs.log"):
        """
            A class holds an LEXIS API SESSION.

            Attributes
            ----------
            config_file : str, path to a config file
            log_file : str, path to a log file. DEFAULT: "./lexis_logs.log"

            Methods
            -------
            get_token()
                Returns the user's keycloak token.

            get_refresh_token()
                Returns the user's refresh keycloak token.

            token_refresh()
                Refresh the user's keycloak tokens
        """

        # Initialise logging
        self.log_path = log_file
        self.logging = logging
        self.logging.basicConfig(filename=log_file,
                                 filemode="w",
                                 level=logging.DEBUG,
                                 format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

        # Prepare tokens and Keycloak session
        self.keycloak_openid = None
        self.REFRESH_TOKEN = None
        self.TOKEN = None

        status = True
        # Load config file and initialise LEXIS session
        try:
            with open(config_file, "rb") as _toml_file:
                _cfg = tomllib.load(_toml_file)

            self.username = _cfg["config"]["USERNAME"]
            self._set_tokens(_cfg["config"]["PWD"],
                             _cfg["config"]["CLIENT_ID"],
                             _cfg["config"]["CLIENT_SECRET"],
                             _cfg["config"]["REALM"],
                             _cfg["config"]["KEYCLOAK_URL"])

            self.ZONENAME = _cfg["config"]["ZONENAME"]

            _ddi_endpoint_url = _cfg["config"]["DDI_ENDPOINT_URL"]
            if not _ddi_endpoint_url[-1] == '/':
                _ddi_endpoint_url = _ddi_endpoint_url + '/'

            # create api url path
            self.DDI_ENDPOINT_URL = _ddi_endpoint_url
            self.API_PATH = self.DDI_ENDPOINT_URL + 'api/v0.2/'
            self.logging.debug(f"Reading {config_file} -- OK")

        except FileNotFoundError:
            self.logging.error(f"Reading '{config_file}' -- FAIL")
            status = False

        # check url if valid
        try:
            response = req.get(self.API_PATH)
            self.logging.debug(f"Initialise '{self.API_PATH}' -- OK")

        except req.ConnectionError:
            self.logging.error(f"Initialise '{self.API_PATH}' -- FAIL")
            status = False

        # Prepare requests header
        if self.TOKEN is not None:
            self.API_HEADER = {'Authorization': 'Bearer ' + self.TOKEN}
            self.logging.debug(f"Set api_header -- OK")

        else:
            self.logging.debug(f"Set api_header -- FAIL")
            status = False

        if not status:
            print(f"Some errors occurred. See log file, please.")

    def _set_tokens(self, pwd, client_id, client_secret, realm, keycloak_url):
        """
            Set user's access and refresh tokens from keycloak based on defined username + password.
        """
        try:
            self.keycloak_openid = KeycloakOpenID(server_url=keycloak_url + "/auth/",
                                                  realm_name=realm,
                                                  client_id=client_id,
                                                  client_secret_key=client_secret)

            # Get WellKnow
            config_well_known = self.keycloak_openid.well_know()

            # Get tokens
            token = self.keycloak_openid.token(self.username, pwd, scope=['openid'])
            self.REFRESH_TOKEN = token['refresh_token']
            self.TOKEN = token['access_token']
            self.logging.debug(f"Token retrieved -- OK")
        except KeycloakGetError as err:
            self.logging.error(f"Token retrieved -- FAIL")
            self.logging.error(f"{err}")

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
        return self.REFRESH_TOKEN

    def token_refresh(self):
        """
            Refresh user's token from keycloak.

            Return
            -------
            User's keycloak token.
        """
        try:
            token = self.keycloak_openid.refresh_token(self.REFRESH_TOKEN)
            self.TOKEN = token['access_token']
            self.REFRESH_TOKEN = token['refresh_token']
            self.API_HEADER = {'Authorization': 'Bearer ' + self.TOKEN}
            self.logging.debug(f"Token refreshed -- OK")

        except KeycloakGetError as err:
            self.logging.error(f"Token refreshed -- FAIL")
            self.logging.error(f"{err}")

