"""Google native file upload strategy."""
import logging
from collections import OrderedDict
from typing import Dict

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata

from .upload_strategy import UploadStrategy


class GoogleUploadStrategy(UploadStrategy):
    """Google implementation for upload strategy."""

    def __init__(self, metadata: FileMetadata, cognite_client: CogniteClient):
        """Initializer."""
        super().__init__(metadata, cognite_client)
        self.chunk_cache: Dict = OrderedDict()  # Store chunks in the order received
        self.last_written_index = -1  # The last consecutive chunk index that was written
        self.last_written_byte = -1  # The last byte position that was written
        self.logger = logging.getLogger("cdffs.GoogleUploadStrategy")

    def _write_chunk(self, index: int, data: bytes) -> None:
        start_byte = self.last_written_byte + 1
        end_byte = start_byte + len(data) - 1

        headers = {
            "Content-Length": str(len(data)),
            "Content-Range": f"bytes {start_byte}-{end_byte}/*",
        }

        response = self.session.put(self.params["upload_url"], headers=headers, data=data)
        response.raise_for_status()
        self.indexes.append(index)

        self.logger.debug(
            f"Finished uploading chunk {index}=[{start_byte}:{end_byte}]. Took {response.elapsed.total_seconds()} sec"
        )

        # Update the last written byte position
        self.last_written_byte = end_byte

    def upload_chunk(self, data: bytes, index: int) -> None:
        """Uploads a single chunk."""
        with self.lock:
            self.chunk_cache[index] = data

            # Check for consecutive chunks in cache and write them
            while self.last_written_index + 1 in self.chunk_cache:
                next_index = self.last_written_index + 1
                self._write_chunk(next_index, self.chunk_cache[next_index])
                del self.chunk_cache[next_index]  # Remove the written chunk from cache
                self.last_written_index = next_index

        self.logger.debug(f"Received chunk {index}. Cache size: {len(self.chunk_cache)} chunks")

    def merge_chunks(self) -> int:
        """Google Cloud Storage handles merging internally. So, this method is a no-op for the GCS strategy."""
        return self.last_written_byte + 1
