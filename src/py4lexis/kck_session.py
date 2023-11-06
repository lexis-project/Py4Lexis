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

    def __init__(self):
        self.Clr = Clr() 
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
            a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            port: int = int(self.Clr.yhbrr(cihar))
            in_use: bool = True
            while in_use:
                location: tuple[str, int] = (self.Clr.yhbrr(erdtirec), port)
                check = a_socket.connect_ex(location)

                if check == 0 or check == 10061:
                    in_use = False
                else:
                    port += 1


            with OAuthHttpServer(("", port), OAuthHttpHandler) as httpd:
                web_client = WebApplicationClient(self.Clr.yhbrr(_vreen))
                
                code_verifier, code_challenge = self._generate_code()

                auth_uri = web_client.prepare_request_uri(self.Clr.yhbrr(_uitrauh), 
                                                          redirect_uri=f"http://{self.Clr.yhbrr(erdtirec)}:{port}", 
                                                          scope=[self.Clr.yhbrr(_ulme)], 
                                                          state=self.Clr.yhbrr(_eastt), 
                                                          code_challenge=code_challenge, 
                                                          code_challenge_method=self.Clr.yhbrr(hdmathesho) )
                
                webbrowser.open_new(auth_uri)

                httpd.handle_request()

                auth_code = httpd.authorization_code

                data: dict = {
                    "code": auth_code,
                    "client_id": self.Clr.yhbrr(_vreen),
                    "grant_type": "authorization_code",
                    "scopes": [self.Clr.yhbrr(_ulme)],
                    "redirect_uri": f"http://{self.Clr.yhbrr(erdtirec)}:{port}",
                    "code_verifier": code_verifier
                }

                response: requests.Request = requests.post(self.Clr.yhbrr(_utikeron), 
                                                           data=data, 
                                                           verify=False)

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
        rand = random.SystemRandom()
        code_verifier: str = "".join(rand.choices(string.ascii_letters + string.digits, k=128))

        code_sha_256 = hashlib.sha256(code_verifier.encode('utf-8')).digest()
        b64 = base64.urlsafe_b64encode(code_sha_256)
        code_challenge = b64.decode('utf-8').replace('=', '')

        return (code_verifier, code_challenge)