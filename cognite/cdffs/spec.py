import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from cognite.client import ClientConfig, CogniteClient
from cognite.client.exceptions import (
    CogniteAPIError,
    CogniteAuthError,
    CogniteConnectionError,
    CogniteConnectionRefused,
)
from fsspec import AbstractFileSystem
from fsspec.caching import AllBytes
from fsspec.spec import AbstractBufferedFile

logger = logging.getLogger(__name__)

# TODO: Update All doc strings


class CdfFileSystem(AbstractFileSystem):
    """File-system specification for CDF Files.

    #TODO: write longer description

    Attributes:
        protocol (str): Protocol name.
        connection_config (ClientConfig): Cognite client connection configurations.
        metadata (Dict): File Metadata that user can send when uploading new files to CDF.
    """

    protocol: str = "cdffs"

    def __init__(self, **kwargs: Optional[Any]) -> None:
        """Initializes the CdfFileSystem and creates a connection to CDF.

        Creates a cognite client by extracting the connection_config from the keyword arguments.
        Connection config must be provided by the user when working with any CDF files. Refer:
        https://cognite-sdk-python.readthedocs-hosted.com/en/latest/cognite.html#cognite.client.config.ClientConfig

        Args:
            **kwargs (Optional[Any]): Set of keyword arguments to create a CDF client connection and a file metadata
            information.

        Returns:
            None.

        Raises:
            ValueError: An error occurred when creating connection to CDF.
        """
        super().__init__(**kwargs)
        self.connection_config: ClientConfig = kwargs.get("connection_config")
        self.metadata: Union[Any, Dict[Any, Any]] = kwargs.get("metadata", {})

        # Create a connection to Cdf
        self.do_connect()

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Remove the protocol from the input path

        Args:
            path (str): Path to remove the protocol from

        Returns:
            str: Returns a path without the protocol
        """
        stripped_path = path.replace("cdffs://", "")
        return stripped_path

    def do_connect(self) -> None:
        """Connect to CDF using the connection configurations provided by the user.

        Args:
            None.

        Raises:
            ValueError: An error occurred when creating connection to CDF or a connection_config is missing.
        """
        try:
            if self.connection_config is not None:
                self.cognite_client = CogniteClient(self.connection_config)
            else:
                raise ValueError("User must provide a `connection_config` in storage_options")

        except (CogniteAuthError, CogniteConnectionError, CogniteConnectionRefused, CogniteAPIError) as e:
            logger.error(f"Connection error occurred: {str(e)}")
            raise ValueError("Unable to connect to CDF")

    def _suffix_exists(self, path: str) -> List:
        """Parse the path and verify if the path contains a valid file suffix.

        Args:
            path (str): Path to perform validation on.

        Returns:
            List: Returns the list of parts with a valid file suffix.
        """
        return [file_part for file_part in path.split("/") if Path(file_part).suffix]

    def split_path(self, path: str) -> Tuple[str, str, str]:
        """Split the path and extract root_dir, external_id and a filename.

        Args:
            path (str): Path to split.

        Returns:
            List: Returns the list of parts with a valid file suffix.

        Raises:
            ValueError: When an invalid input path is given.
        """
        if "directory" in self.metadata:
            root_dir = self.metadata.get("directory", "").strip("/")
            external_id = path.replace(root_dir, "").lstrip("/")
            external_id_prefix = Path(external_id).parts[0]

        elif self._suffix_exists(path):  # TODO: rewrite this further
            external_id_prefix = [x for x in Path(path).parts if Path(x).suffix][0]
            root_dir = path[: path.find(external_id_prefix)].strip("/")
            external_id = path[path.find(external_id_prefix) :]

        elif len(Path(path).parts):  # TODO:
            external_id_prefix = ""
            root_dir = path.strip("/")
            external_id = ""
        else:
            raise ValueError("Path provided is not valid.")

        return "/" + root_dir, external_id_prefix, external_id

    def _ls(self, path: str) -> None:
        """List the files based on the directory & external Id prefixes extracted from path.

        Args:
            path (str): Path to use to extract the list of files from Cdf.

        Returns:
            None.

        Raises:
            FileNotFoundError: An error occurred when extracting file metadata.
        """
        root_dir, external_id_prefix, _ = self.split_path(path)
        list_query = {
            x[0]: x[1]
            for x in zip(("directory_prefix", "external_id_prefix"), (root_dir, external_id_prefix))
            if x[1] != "/"
        }

        try:
            directories = {}
            self.dircache[path] = [{"type": "directory", "name": path.lstrip("/")}]
            for file_met in self.cognite_client.files.list(**list_query, limit=-1):
                inp_path = Path(file_met.directory, file_met.external_id)
                file_meta = {
                    "type": "file",
                    "name": str(inp_path).lstrip("/"),
                    "size": file_met.metadata.get("size", -1) if file_met.metadata else -1,
                }

                parent_path = str(inp_path.parent).lstrip("/")
                if parent_path not in directories:
                    directories[parent_path] = {"type": "directory", "name": parent_path.lstrip("/")}

                if parent_path not in self.dircache:
                    self.dircache[parent_path] = []
                    self.dircache[parent_path].append(file_meta)
                else:
                    self.dircache[parent_path].append(file_meta)

            for dir in directories:
                parent_path = str(Path(dir).parent)
                if parent_path not in self.dircache:
                    self.dircache[parent_path] = []
                    self.dircache[parent_path].append(directories[dir])
                else:
                    self.dircache[parent_path].append(directories[dir])

        except CogniteAPIError:
            raise FileNotFoundError  # TODO: Analyze the consequences of raising a FileNotFoundError

    def ls(
        self, path: str, detail: bool = False, invalidate_cache: bool = False, **kwargs: Optional[Any]
    ) -> Union[Any, List[str]]:
        """List the files based on the directory & external Id prefixes extracted from path.

        Args:
            path (str): Path to use to extract the list of files from Cdf.
            detail (bool): Flag to specify if detail list is expected.
            invalidate_cache (bool): Invalidate cache and extract the data from API. #TODO:
            **kwargs (Optional[Any]): Set of keyword arguments to support additional filtration's
            when listing the files.

        Returns:
            List: Returns the list of files/directories that match the path given.

        Raises:
            FileNotFoundError: When there are no files matching the path given.
        """
        self._ls(path)

        file_list = self.dircache.get(path, [])
        if not file_list:
            raise FileNotFoundError
        if detail:
            return self.dircache[path]
        else:
            return [x["name"] for x in self.dircache[path]]

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """Create a directory at a path given.

        Args:
            path (str): Path to use to create a directory.
            exist_ok (bool): Flag to specify if error can be ignored when directory already exists.

        Returns:
            None.

        Raises:
            FileExistsError: When the directory prefixes already exists.
        """
        if path in self.dircache and not exist_ok:
            raise FileExistsError

        self.dircache[path] = [{"type": "directory", "name": path.lstrip("/")}]

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Optional[Any]) -> None:
        """Create a directory at a path given.

        Args:
            path (str): Path to use to create a directory.
            create_parents (bool): Flag to specify if parents needs to be created. #TODO: Analyze implement

        Returns:
            None.
        """
        self.makedirs(path, exist_ok=bool(kwargs.get("exist_ok")))

    def makedir(self, path: str, exist_ok: bool = False) -> None:
        """Create a directory at a path given.

        Args:
            path (str): Path to use to create a directory.
            exist_ok (bool): Flag to specify if error can be ignored when directory already exists.

        Returns:
            None.
        """
        self.makedirs(path, exist_ok=exist_ok)

    def rm(self, path: str, recursive: bool = False) -> None:
        """Remove the files and directories at a path given.

        Args:
            path (str): Path to use to remove the files and directories.
            recursive (bool): Flag to recursively delete a directory.

        Returns:
            None.

        Raises:
            NotImplementedError: Error as it is not implemented yet.
        """
        raise NotImplementedError

    def mv(self, source_path: str, destination_path: str, recursive: bool = False, maxdepth: int = None) -> None:
        """Move the files and directories at a path given to a new path.

        Args:
            source_path (str): Path to use to move the files and directories.
            destination_path (str): Path to use as destination.
            recursive (bool): Flag to recursively move the files and directories.
            maxdepth (int): Maximum depth to use when moving the files and directories.

        Returns:
            None.

        Raises:
            NotImplementedError: Error as it is not implemented yet.
        """
        raise NotImplementedError

    def cd(self, path: str, **kwargs: Optional[Any]) -> None:
        """Change the directory to a path given.

        Args:
            path (str): Path to use to move the files and directories.
            **kwargs (Optional[Any]): Set of keyword arguments to perform change directory.

        Returns:
            None.

        Raises:
            NotImplementedError: Error as it is not implemented yet.
        """
        raise NotImplementedError

    def open(self, path: str, mode: str = "rb", block_size: str = "default", **kwargs: Optional[Any]) -> "CdfFile":
        """Open the file for reading and writing.

        Args:
            path (str): File name with absolute path to open.
            mode (str): Mode to use when opening a file.
            block_size (str): Block size to use when opening a file.
            **kwargs (Optional[Any]): Set of keyword arguments to allow additional options
            when opening a file.

        Returns:
            CdfFile: CdfFile to allow reading/writing files to Cdf.
        """
        root_dir, _, external_id = self.split_path(path)
        return CdfFile(self, self.cognite_client, path, root_dir, external_id, mode=mode, block_size=block_size)

    # def __contains__(self, key):
    #     key = self._normalize_key(key)
    #     file_path = os.path.join(self.path, key)
    #     return False

    def cat_file(self, external_id: str) -> Any:
        """Open and read the contents of a file.

        Args:
            external_id (str): External Id of the file to fetch the contents.

        Returns:
            bytes: File contents as such from

        Raises:
            FileNotFoundError: When there is no file matching the external_id given.
        """
        try:
            return self.cognite_client.files.download_bytes(external_id=external_id)
        except CogniteAPIError:
            raise FileNotFoundError

    def cat(self, path: Union[str, list], **kwargs: Optional[Any]) -> Union[bytes, Any, Dict[str, bytes]]:
        """Open and read the contents of a file(s).

        Args:
            path (str): Path to use to extract the read the contents of file(s) from Cdf.
            **kwargs (Optional[Any]): Set of keyword arguments to reading the file contents.

        Returns:
            bytes: File contents for the file name given.
            Dict[str, bytes]: File contents for the list of file given - if list of files were
            given as an argument it will be returned a dictionary with key as path name and contents
            of the files as value for each path.

        Raises:
            ValueError: When the path is empty.
        """
        if path:
            ValueError("Path cannot be empty")

        if isinstance(path, list):
            out_data = {}
            for inp_path in path:
                external_id = self.split_path(inp_path)[2]
                out_data[inp_path] = self.cat_file(external_id)
            return out_data
        else:
            external_id = self.split_path(path)[2]
            return self.cat_file(external_id)


class CdfFile(AbstractBufferedFile):
    """CDF File interface to read/write the files.

    #TODO: write longer description

    Attributes:
        DEFAULT_BLOCK_SIZE (int): Block size to read and write the data.
        cognite_client (ClientClient): Cognite client.
        root_dir (str): Root directory for the file.
        external_id (str): External Id for the file.
    """

    DEFAULT_BLOCK_SIZE: int = 5 * 2**20

    def __init__(
        self,
        fs: CdfFileSystem,
        coginte_client: CogniteClient,
        path: str,
        directory: str,
        external_id: str,
        mode: str = "rb",
        block_size: str = "default",
        cache_type: str = "bytes",
        cache_options: Dict[Any, Any] = {},
        **kwargs: Optional[Any],
    ) -> None:
        """Initializes the CdfFile and initializes a connection to CDF.

        Args:
            fs (CdfFileSystem): CdfFileSystem.
            coginte_client (CogniteClient): Cognite Client.
            path (str): Absolute path for the file.
            directory (str): Root directory for the file.
            external_id (str): External Id for the file.
            mode (str): mode to work with the file.
            block_size (str): Block size to read/write the file.
            cache_type (str): Caching policy for the file.
            cache_options (str): Additional caching options for the file.
            **kwargs (Optional[Any]): Set of keyword arguments to read/write the file contents.

        Returns:
            None.
        """
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=self.DEFAULT_BLOCK_SIZE,
            autocommit=True,
            **kwargs,
        )
        self.cognite_client: CogniteClient = coginte_client
        self.root_dir: str = directory
        self.external_id: str = external_id

    def _upload_chunk(self) -> None:
        """Upload file contents to CDF.

        Args:
            None

        Returns:
            None.

        Raises:
            RuntimeError: When an unexpected error occurred.
        """
        try:
            self.cognite_client.files.upload_bytes(
                content=self.buffer.getbuffer(),
                name=Path(self.external_id).name,
                external_id=self.external_id,
                directory=self.root_dir,
                source=self.fs.metadata.get("source"),
                metadata={"size": self.buffer.getbuffer().nbytes},
                overwrite=True,
            )
        except CogniteAPIError:
            raise RuntimeError  # TODO: Raise appropriate exception

    def read(self, length: int = -1) -> Any:
        """Read file contents from CDF.

        Args:
            length (int): Length of a data to read.

        Returns:
            None.
        """
        inp_data = self.fs.cat_file(self.external_id)
        self.size = len(inp_data)
        self.cache = AllBytes(size=len(inp_data), fetcher=None, blocksize=None, data=inp_data)
        return super().read(length)
