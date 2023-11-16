"""Inmemory file upload strategy."""
import threading
from typing import Dict

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata

from .upload_strategy import UploadStrategy


class InMemoryUploadStrategy(UploadStrategy):
    """Inmemory implementation for upload strategy."""

    def __init__(self, metadata: FileMetadata, cognite_client: CogniteClient):
        """Initializer."""
        super().__init__(metadata, cognite_client)
        self.blocks: Dict[int, bytes] = {}
        self.lock = threading.Lock()
        self.metadata = metadata

    def upload_chunk(self, data: bytes, index: int) -> None:
        """Upload single chunk."""
        with self.lock:
            self.blocks[index] = data

    def merge_chunks(self) -> int:
        """Merge all uploaded blocks into the final blob."""
        content = b"".join([self.blocks[key] for key in sorted(self.blocks.keys())])
        self.cognite_client.files.upload_bytes(content=content, **self.metadata.dump(camel_case=False), overwrite=True)
        return len(content)
