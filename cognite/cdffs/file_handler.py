"""Download the file from a cloud storage."""
import time
from typing import Any, Dict, Union

import requests
from requests.exceptions import RequestException

_URL_CACHE_EXPIRY_SECONDS = 28


class FileException(Exception):
    """Common exception for FileHandler."""


class FileHandler:
    """Handle the requests to a cloud storage.

    Cache the download URLs, download the file contents from a cloud storage using custom headers.

    Attributes:
        _url_container (Dict): Cache the file download URLs extracted from CDF.
        session (requests.Session): Session object to interact with a cloud storage.
    """

    def __init__(self) -> None:
        """Initialize the FileHandler and creates a session object to interact with a cloud storage.

        Args:
            None
        """
        self._url_container: Dict[str, Dict[str, Any]] = {}
        self.session = requests.Session()

    def download_file(
        self,
        download_url: str,
        start_byte: Union[int, None] = None,
        end_byte: Union[int, None] = None,
    ) -> bytes:
        """Download the file from a cloud storage using the download URL & offsets provided.

        Args:
            download_url (str): download URL for the file.
            start_byte (int): Start byte for the file only if a specific portion of the file is needed.
            end_byte (int): End offset for the file only if a specific portion of the file is needed.

        Returns:
            bytes: Returns a file contents as is.

        Raises:
            FileException: If unexpected status received or any connection errors.
        """
        try:
            headers = {}
            if start_byte is not None and end_byte is not None:  # start_byte can be 0.
                headers = {"Range": f"bytes={start_byte}-{end_byte}"}
            file_response = self.session.get(download_url, headers=headers)
            file_response.raise_for_status()
            return file_response.content

        except RequestException as request_exception:
            raise FileException() from request_exception

    def add_or_update_url(self, external_id: str, download_url: str) -> None:
        """Add or update the download url to the cache.

        Args:
            external_id (str): External Id for the file.
            download_url (str): Download URL for the file.
        """
        self._url_container[external_id] = {
            "url": download_url,
            "expiry_time": time.time(),
        }

    def get_url(self, external_id: str) -> Any:
        """Get download url from a cache if they are valid.

        Args:
            external_id (str): External Id for the file.

        Returns:
            str: Download URL for the file from cache if it is still valid.
        """
        download_url = None
        if (
            external_id in self._url_container
            and (time.time() - self._url_container[external_id]["expiry_time"]) < _URL_CACHE_EXPIRY_SECONDS
        ):
            download_url = self._url_container[external_id]["url"]
        elif external_id in self._url_container:
            self._url_container.pop(external_id)

        return download_url
