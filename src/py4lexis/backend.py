from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import parse


class OAuthHttpServer(HTTPServer):

    def __init__(self, *args, **kwargs) -> None:
        HTTPServer.__init__(self, *args, **kwargs)
        self.authorization_code: str = str("")


class OAuthHttpHandler(BaseHTTPRequestHandler):

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write("<script type=\"application/javascript\">window.close();</script>".encode("UTF-8"))
        
        parsed: dict = parse.urlparse(self.path)
        qs: dict = parse.parse_qs(parsed.query)
        
        self.server.authorization_code: str = qs["code"][0]