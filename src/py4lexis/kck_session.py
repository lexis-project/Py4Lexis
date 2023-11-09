import base64
import hashlib
import random
import string
import socket
import webbrowser
import requests
from oauthlib.oauth2 import WebApplicationClient
from keycloak import KeycloakOpenID, KeycloakPostError
from irods.session import iRODSSession
from py4lexis.backend import OAuthHttpHandler, OAuthHttpServer
from py4lexis.exceptions import Py4LexisAuthException, Py4LexisPostException
from py4lexis.helper import Clr, _RR, igev, _vreen, ngano, _urby, _gomiz, _ulme, _itbbra, _yuo, cihar, erdtirec, _uitrauh, _utikeron, _eastt, hdmathesho


class kck_oi():

    def __init__(self, logging):
        self.Clr = Clr() 
        self.logging = logging
        self._oid = KeycloakOpenID(self.Clr.yhbrr(_RR) + "/auth/", self.Clr.yhbrr(igev), self.Clr.yhbrr(_vreen), client_secret_key=self.Clr.yhbrr(ngano))


    def rfsh_token(self, token):
        try:
            return self._oid.refresh_token(token)
        
        except KeycloakPostError:
            raise Py4LexisPostException
    
    
    def get_irods_session(self, username, token):
        return iRODSSession(
            host=self.Clr.yhbrr(_urby),
            port=int(self.Clr.yhbrr(_gomiz)),
            authentication_scheme=self.Clr.yhbrr(_ulme),
            openid_provider=self.Clr.yhbrr(_itbbra),
            zone=self.Clr.yhbrr(_yuo),
            access_token=token,
            user=username,
            block_on_authURL=False)
    

    def login(self) -> dict[str]:

        try:
            # Check if port is in use. If yes, select next one
            self.logging.debug(f"AUTH -- Trying to get SOCKET stream...")
            a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            self.logging.debug(f"AUTH -- Identifying used ports...")
            port: int = int(self.Clr.yhbrr(cihar))
            in_use: bool = True
            while in_use:
                location: tuple[str, int] = (self.Clr.yhbrr(erdtirec), port)
                self.logging.debug(f"AUTH -- checking address: {location}")
                check = a_socket.connect_ex(location)

                if check == 0:
                    port += 1
                else:
                    in_use = False

            self.logging.debug(f"AUTH -- Starting server to parse tokens...")
            with OAuthHttpServer(("", port), OAuthHttpHandler) as httpd:
                self.logging.debug(f"AUTH -- Starting oauth webclient...")
                web_client = WebApplicationClient(self.Clr.yhbrr(_vreen))
                
                self.logging.debug(f"AUTH -- Generating code...")
                code_verifier, code_challenge = self._generate_code()

                self.logging.debug(f"AUTH -- Preparing request uri...")
                auth_uri = web_client.prepare_request_uri(self.Clr.yhbrr(_uitrauh), 
                                                          redirect_uri=f"http://{self.Clr.yhbrr(erdtirec)}:{port}", 
                                                          scope=[self.Clr.yhbrr(_ulme)], 
                                                          state=self.Clr.yhbrr(_eastt), 
                                                          code_challenge=code_challenge, 
                                                          code_challenge_method=self.Clr.yhbrr(hdmathesho) )
                
                self.logging.debug(f"AUTH -- Opening browser...")
                webbrowser.open_new(auth_uri)

                self.logging.debug(f"AUTH -- Catching the response in client server...")
                httpd.handle_request()

                self.logging.debug(f"AUTH -- Obtaining authorization code...")
                auth_code = httpd.authorization_code

                self.logging.debug(f"AUTH -- Preparing data for token request...")
                data: dict = {
                    "code": auth_code,
                    "client_id": self.Clr.yhbrr(_vreen),
                    "grant_type": "authorization_code",
                    "scopes": [self.Clr.yhbrr(_ulme)],
                    "redirect_uri": f"http://{self.Clr.yhbrr(erdtirec)}:{port}",
                    "code_verifier": code_verifier
                }
                
                self.logging.debug(f"AUTH -- Sending token request...")
                response: requests.Request = requests.post(self.Clr.yhbrr(_utikeron), 
                                                           data=data,           
                                                           verify=False)

                self.logging.debug(f"AUTH -- Parsing tokens...")
                tokens: dict = {
                    "access_token": response.json()["access_token"],
                    "refresh_token": response.json()["refresh_token"],
                    "expires_in": response.json()["expires_in"],
                    "refresh_expires_in": response.json()["refresh_expires_in"]
                }            
                
                print("Logged in successfully...")

                return tokens
        except:
            raise Py4LexisAuthException
        

    @staticmethod
    def _generate_code() -> tuple[str, str]:
        print(f"GENERATE CODE: Get system random...")
        rand = random.SystemRandom()
        print(f"GENERATE CODE: joining string...")
        code_verifier: str = "".join(rand.choices(string.ascii_letters + string.digits, k=128))

        print(f"GENERATE CODE: hashing...")
        code_sha_256 = hashlib.sha256(code_verifier.encode('utf-8')).digest()
        print(f"GENERATE CODE: encoding...")
        b64 = base64.urlsafe_b64encode(code_sha_256)
        print(f"GENERATE CODE: decoding...")
        code_challenge = b64.decode('utf-8').replace('=', '')

        return (code_verifier, code_challenge)