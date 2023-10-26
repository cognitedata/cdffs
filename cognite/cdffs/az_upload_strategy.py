"""Azure native file upload strategy."""
import base64
import logging

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata

from .upload_strategy import UploadStrategy


class AzureUploadStrategy(UploadStrategy):
    """Azure implementation for upload strategy."""

    def __init__(self, metadata: FileMetadata, cognite_client: CogniteClient):
        """Initializer."""
        super().__init__(metadata, cognite_client)
        self.total_size = 0

    def _generate_block_blob_block_id(self, index: int, block_name_prefix: str = __file__.__str__()) -> str:
        while len(block_name_prefix) < 19:
            block_name_prefix += "x"
        block_id = f"{block_name_prefix[:19]}{index:05}".encode("utf-8")

        return base64.b64encode(block_id).decode("utf-8")

    def upload_chunk(self, data: bytes, index: int) -> None:
        """Uploads a single block."""
        try:
            url = self.params["upload_url"].split("?")
            block_id = self._generate_block_blob_block_id(index=index)
            upload_block_url = f"{url[0]}?blockid={block_id}&comp=block&{url[1]}"
            response = self.session.put(
                upload_block_url,
                data=data,
                headers={
                    "Accept": "application/xml",
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(data)),
                    "x-ms-version": "2019-12-12",
                },
            )
            response.raise_for_status()
            self.total_size += len(data)  # track total object size
            with self.lock:
                self.indexes.append(index)
            logging.info(f"Finished uploading block {index}. Took {response.elapsed.total_seconds()} sec")
        except Exception as ex:
            logging.warning("Failed to upload on of the blocks: {ex}", exc_info=ex)
            raise

    def merge_chunks(self) -> int:
        """Merge all uploaded blocks into the final blob."""
        try:
            upload_url = self.params["upload_url"]
            commit_url = f"{upload_url}&comp=blocklist"
            block_list_xml = '<?xml version="1.0" encoding="utf-8"?><BlockList>'
            for index in sorted(self.indexes):
                block_list_xml += f"<Latest>{self._generate_block_blob_block_id(index)}</Latest>"
            block_list_xml += "</BlockList>"

            response = self.session.put(
                commit_url,
                data=block_list_xml,
                headers={
                    "x-ms-blob-content-type": self.params["mime_type"],
                    "Content-Type": "application/xml",
                    "Content-Length": str(len(block_list_xml)),
                    "x-ms-version": "2019-12-12",
                },
            )
            response.raise_for_status()

            return self.total_size
        except Exception as ex:
            logging.warning("Failed to merge all blocks: {ex}", exc_info=ex)
            raise