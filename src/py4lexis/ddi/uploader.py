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
    
    def upload(self, stop_at: Optional[int] = None):
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

