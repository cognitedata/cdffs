"""Abstract upload strategy."""
import abc
import threading
from typing import Dict, List

import requests
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata


class UploadStrategy(abc.ABC):
    """File upload strategy interface to work with a specific file.

    Provides an interface to upload single data chunk or command to finalize the upload. Works with unbounded files.
    """

    def __init__(self, metadata: FileMetadata, cognite_client: CogniteClient):
        """Initialize the Strategy.

        Args:
            metadata (FileMetadata): File Metadata that a user can use when opening a file to write.
            cognite_client (ClientClient): Cognite client to work with Cdf.
        """
        self.session = requests.Session()
        self.lock = threading.Lock()
        self.indexes: List[int] = list()
        self.cognite_client = cognite_client
        self.params = self.init_uploader(metadata)

    def init_uploader(self, metadata: FileMetadata) -> Dict:
        """Initialize the upload.

        Args:
            metadata (FileMetadata): File Metadata that a user can use when opening a file to write.

        Returns:
            Dictionary with unpacked parameters
        """
        file_descriptor = self.cognite_client.post(
            f"/api/v1/projects/{self.cognite_client.config.project}/files?overwrite=true",
            json=metadata.dump(camel_case=True),
        ).json()
        return {
            "id": file_descriptor["id"],
            "mime_type": metadata.mime_type,
            "upload_url": file_descriptor["uploadUrl"],
        }

    @abc.abstractmethod
    def upload_chunk(self, data: bytes, index: int) -> None:
        """Upload single chunk."""

    @abc.abstractmethod
    def merge_chunks(self) -> int:
        """Finalize the upload."""
