from typing import Optional
import time
import asyncio
from urllib.parse import urljoin

import requests
import aiohttp

from tusclient.uploader.baseuploader import BaseUploader

from tusclient.exceptions import TusUploadFailed, TusCommunicationError
from tusclient.request import TusRequest, AsyncTusRequest, catch_requests_error

from py4lexis.utils import printProgressBar
from math import ceil


def _verify_upload(request: TusRequest):
    if 200 <= request.status_code < 300:
        return True
    else:
        raise TusUploadFailed('', request.status_code,
                              request.response_content)


class Uploader(BaseUploader):
    def __init__(self, log_func=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_func = log_func
        self.upload_checksum = upload_checksum
        self.__checksum_algorithm_name, self.__checksum_algorithm = \
            self.CHECKSUM_ALGORITHM_PAIR

    # it is important to have this as a @property so it gets
    # updated client headers.
    @property
    def headers(self):
        """
        Return headers of the uploader instance. This would include the headers of the
        client instance.
        """
        client_headers = getattr(self.client, 'headers', {})
        return dict(self.DEFAULT_HEADERS, **client_headers)

    @property
    def headers_as_list(self):
        """
        Does the same as 'headers' except it is returned as a list.
        """
        headers = self.headers
        headers_list = ['{}: {}'.format(key, value) for key, value in iteritems(headers)]
        return headers_list

    @property
    def checksum_algorithm(self):
        """The checksum algorithm to be used for the Upload-Checksum extension.
        """
        return self.__checksum_algorithm

    @property
    def checksum_algorithm_name(self):
        """The name of the checksum algorithm to be used for the Upload-Checksum
        extension.
        """
        return self.__checksum_algorithm_name

    @_catch_requests_error
    def get_offset(self):
        """
        Return offset from tus server.

        This is different from the instance attribute 'offset' because this makes an
        http request to the tus server to retrieve the offset.
        """
        resp = requests.head(self.url, headers=self.headers)
        offset = resp.headers.get('upload-offset')
        if offset is None:
            msg = 'Attempt to retrieve offset fails with status {}'.format(resp.status_code)
            raise TusCommunicationError(msg, resp.status_code, resp.content)
        return int(offset)

    def encode_metadata(self):
        """
        Return list of encoded metadata as defined by the Tus protocol.
        """
        encoded_list = []
        for key, value in iteritems(self.metadata):
            key_str = str(key)  # dict keys may be of any object type.

            # confirm that the key does not contain unwanted characters.
            if re.search(r'^$|[\s,]+', key_str):
                msg = 'Upload-metadata key "{}" cannot be empty nor contain spaces or commas.'
                raise ValueError(msg.format(key_str))

            value_bytes = b(value)  # python 3 only encodes bytes
            encoded_list.append('{} {}'.format(key_str, b64encode(value_bytes).decode('ascii')))
        return encoded_list

    def get_url(self):
        """
        Return the tus upload url.

        If resumability is enabled, this would try to get the url from storage if available,
        otherwise it would request a new upload url from the tus server.
        """
        if self.store_url and self.url_storage:
            key = self.fingerprinter.get_fingerprint(self.get_file_stream())
            url = self.url_storage.get_item(key)
            if not url:
                url = self.create_url()
                self.url_storage.set_item(key, url)
            return url
        else:
            return self.create_url()

    @_catch_requests_error
    def create_url(self):
        """
        Return upload url.

        Makes request to tus server to create a new upload url for the required file upload.
        """
        headers = self.headers
        headers['upload-length'] = str(self.file_size)
        headers['upload-metadata'] = ','.join(self.encode_metadata())
        resp = requests.post(self.client.url, headers=headers)
        # --------------------------------------------------------------------------------------------------------------
        url = resp.headers.get("location")
        # --------------------------------------------------------------------------------------------------------------
        if url is None:
            msg = 'Attempt to retrieve create file url with status {}'.format(resp.status_code)
            raise TusCommunicationError(msg, resp.status_code, resp.content)
        return urljoin(self.client.url, url)

    @property
    def request_length(self):
        """
        Return length of next chunk upload.
        """
        remainder = self.stop_at - self.offset
        return self.chunk_size if remainder > self.chunk_size else remainder

    def verify_upload(self):
        """
        Confirm that the last upload was sucessful.
        Raises TusUploadFailed exception if the upload was not sucessful.
        """
        if self.request.status_code == 204:
            return True
        else:
            raise TusUploadFailed('', self.request.status_code, self.request.response_content)

    def get_file_stream(self):
        """
        Return a file stream instance of the upload.
        """
        if self.file_stream:
            self.file_stream.seek(0)
            return self.file_stream
        elif os.path.isfile(self.file_path):
            return open(self.file_path, 'rb')
        else:
            raise ValueError("invalid file {}".format(self.file_path))

    @property
    def file_size(self):
        """
        Return size of the file.
        """
        stream = self.get_file_stream()
        stream.seek(0, os.SEEK_END)
        return stream.tell()

    def upload(self, stop_at=None):
        """
        Perform file upload.

        Performs continous upload of chunks of the file. The size uploaded at each cycle is
        the value of the attribute 'chunk_size'.

        :Args:
            - stop_at (Optional[int]):
                Determines at what offset value the upload should stop. If not specified this
                defaults to the file size.
        """
        self.stop_at = stop_at or self.get_file_size()
        
        if self.log_func is None:
            total = int(ceil(self.get_file_size() / self.chunk_size))
            iter = 0
            printProgressBar(iter, total, prefix='Progress: ', suffix='Uploaded', length=50)
        while self.offset < self.stop_at:
            self.upload_chunk()
            if self.log_func is None:
                iter += 1
                printProgressBar(iter, total, prefix='Progress: ', suffix='Uploaded', length=50)
        else:
            if self.log_func:
                self.log_func("maximum upload specified({} bytes) has been reached".format(self.stop_at))

    def upload_chunk(self):
        """
        Upload chunk of file.
        """
        self._retried = 0
        if not self.url:
            self.set_url(self.create_url())
            self.offset = 0
        self._do_request()
        self.offset = int(self.request.response_headers.get('upload-offset'))
        if self.log_func:
            msg = '{} bytes uploaded ...'.format(self.offset)
            self.log_func(msg)

    @catch_requests_error
    def create_url(self):
        """
        Return upload url.

        Makes request to tus server to create a new upload url for the required file upload.
        """
        resp = requests.post(
            self.client.url, headers=self.get_url_creation_headers(),
            verify=self.verify_tls_cert)
        url = resp.headers.get("location")
        if url is None:
            msg = 'Attempt to retrieve create file url with status {}'.format(
                resp.status_code)
            raise TusCommunicationError(msg, resp.status_code, resp.content)
        return urljoin(self.client.url, url)

    def _do_request(self):
        self.request = TusRequest(self)
        try:
            self.request.perform()
            _verify_upload(self.request)
        except TusUploadFailed as error:
            self._retry_or_cry(error)

    def _retry_or_cry(self, error):
        if self.retries > self._retried:
            time.sleep(self.retry_delay)

            self._retried += 1
            try:
                self.offset = self.get_offset()
            except TusCommunicationError as err:
                self._retry_or_cry(err)
            else:
                self._do_request()
        else:
            raise error


class AsyncUploader(BaseUploader):
    def __init__(self, log_func=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_func = log_func

    async def upload(self, stop_at: Optional[int] = None):
        """
        Perform file upload.

        Performs continous upload of chunks of the file. The size uploaded at each cycle is
        the value of the attribute 'chunk_size'.

        :Args:
            - stop_at (Optional[int]):
                Determines at what offset value the upload should stop. If not specified this
                defaults to the file size.
        """
        self.stop_at = stop_at or self.get_file_size()
        if self.log_func is None:
            total = int(ceil(self.get_file_size() / self.chunk_size))
            iter = 0
            printProgressBar(iter, total, prefix='Progress: ', suffix='Uploaded', length=50)
        while self.offset < self.stop_at:
            await self.upload_chunk()
            if self.log_func is None:
                iter += 1
                printProgressBar(iter, total, prefix='Progress: ', suffix='Uploaded', length=50)
        else:
            if self.log_func:
                self.log_func("maximum upload specified({} bytes) has been reached".format(self.stop_at))
                
    async def upload_chunk(self):
        """
        Upload chunk of file.
        """
        self._retried = 0
        if not self.url:
            self.set_url(await self.create_url())
            self.offset = 0
        await self._do_request()
        self.offset = int(self.request.response_headers.get('upload-offset'))
        if self.log_func:
            msg = '{} bytes uploaded ...'.format(self.offset)
            self.log_func(msg)

    async def create_url(self):
        """
        Return upload url.

        Makes request to tus server to create a new upload url for the required file upload.
        """
        try:
            async with aiohttp.ClientSession() as session:
                headers = self.get_url_creation_headers()
                ssl = None if self.verify_tls_cert else False
                async with session.post(
                        self.client.url, headers=headers, ssl=ssl) as resp:
                    url = resp.headers.get("location")
                    if url is None:
                        msg = 'Attempt to retrieve create file url with status {}'.format(
                            resp.status)
                        raise TusCommunicationError(msg, resp.status, await resp.content.read())
                    return urljoin(self.client.url, url)
        except aiohttp.ClientError as error:
            raise TusCommunicationError(error)

    async def _do_request(self):
        self.request = AsyncTusRequest(self)
        try:
            await self.request.perform()
            _verify_upload(self.request)
        except TusUploadFailed as error:
            await self._retry_or_cry(error)

    async def _retry_or_cry(self, error):
        if self.retries > self._retried:
            await asyncio.sleep(self.retry_delay)

            self._retried += 1
            try:
                self.offset = self.get_offset()
            except TusCommunicationError as err:
                await self._retry_or_cry(err)
            else:
                await self._do_request()
        else:
            raise error

