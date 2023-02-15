"""File System Specification for CDF Files."""
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from cognite.client import ClientConfig, CogniteClient
from cognite.client.data_classes.files import FileMetadata
from cognite.client.exceptions import (
    CogniteAPIError,
    CogniteAuthError,
    CogniteConnectionError,
    CogniteConnectionRefused,
    CogniteNotFoundError,
)
from fsspec import AbstractFileSystem
from fsspec.caching import AllBytes
from fsspec.spec import AbstractBufferedFile

from .file_handler import FileException, FileHandler

logger = logging.getLogger(__name__)
_COMMON_EXCEPTIONS = (CogniteAuthError, CogniteConnectionError, CogniteConnectionRefused, CogniteAPIError)
_CACHE_SLEEP_INTERVAL = 5


class CdfFileSystem(AbstractFileSystem):
    """File system interface to work with the CDF files.

    Provides an interface to the files that are stored in CDF. Only a handful of methods that are required to support
    the popular python packages are implemented.

    Attributes:
        protocol (str): (class attribute) Protocol name to use when interacting with CDF Files.
        cdf_list_cache (Dict): Cache the file list results from CDF.
        cdf_list_expiry_time (int): Expiry time in seconds to invalidate the file list cache.
        file_metadata (FileMetadata): File Metadata that a user can use when reading/writing the files to CDF.
        file_cache (Dict): Cache the contents of the files.
        file_handler (FileHandler): File handler to manage the requests to a cloud storage.
    """

    protocol: str = "cdffs"

    def __init__(
        self,
        connection_config: ClientConfig,
        file_metadata: FileMetadata = FileMetadata(metadata={}),
        **kwargs: Optional[Any],
    ) -> None:
        """Initialize the CdfFileSystem and creates a connection to CDF.

        Creates a cognite client using the connection_config provided in storage_options.
        Connection config must be provided by the user when working with any CDF files. Refer:
        https://cognite-sdk-python.readthedocs-hosted.com/en/latest/cognite.html#cognite.client.config.ClientConfig
        User can also provide FileMetadata in storage_options to add metadata to CDF files. Refer:
        https://cognite-sdk-python.readthedocs-hosted.com/en/latest/cognite.html#id19

        Args:
            connection_config (ClientConfig): Cognite client connection configurations.
            file_metadata (FileMetadata): File Metadata that a user can use when reading/writing the files to CDF.
            **kwargs (Optional[Any]): Set of keyword arguments to create a CDF client connection and a file metadata
            information.

        Raises:
            ValueError: An error occurred when creating connection to CDF.
        """
        super().__init__(**kwargs)
        if isinstance(file_metadata, FileMetadata):
            self.file_metadata: FileMetadata = file_metadata
        else:
            raise ValueError("User must provide a valid `file_metadata` in storage_options")
        self.cdf_list_cache: Dict[str, float] = {}
        self.cdf_list_expiry_time: int = kwargs.get("cdf_list_expiry_time", 60)  # type: ignore
        self.file_cache: Dict[str, Dict[str, Any]] = {}
        self.file_handler: FileHandler = FileHandler()

        # Create a connection to Cdf
        self.do_connect(connection_config)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Remove the protocol from the input path.

        Args:
            path (str): Path to use to extract the protocol.

        Returns:
            str: Returns a path without the protocol
        """
        stripped_path = path.replace(f"{cls.protocol}://", "")
        return stripped_path

    def do_connect(self, connection_config: ClientConfig) -> None:
        """Connect to CDF using the connection configurations provided by the user.

        Args:
            connection_config (ClientConfig): Cognite client connection configurations.

        Raises:
            ValueError: When an invalid connection_config is provided.
        """
        if isinstance(connection_config, ClientConfig):
            self.cognite_client = CogniteClient(connection_config)
        else:
            raise ValueError("User must provide a valid `connection_config` in storage_options")

    def _suffix_exists(self, path: str) -> List:
        """Parse the path and verify if the path contains a valid file suffix.

        Args:
            path (str): Path to perform validation on.

        Returns:
            List: Returns the list of parts with a valid file suffix.
        """
        return [file_part for file_part in path.split("/") if Path(file_part).suffix]

    def split_path(self, path: str, validate_suffix: bool = True) -> Tuple[str, str, str]:
        """Split the path and extract root_dir, external_id and a filename.

        Args:
            path (str): Path to split.
            validate_suffix (bool): Flag to validate if the file name must have a valid suffix.

        Returns:
            Tuple: Returns a tuple with root_dir, external_id_prefix and external_id.

        Raises:
            ValueError: When an invalid input path is given.
        """
        if self.file_metadata.directory:
            root_dir = self.file_metadata.directory.strip("/")
            external_id = path.replace(root_dir, "").lstrip("/")
            external_id_prefix = Path(external_id).parts[0]

        elif self._suffix_exists(path):
            external_id_prefix = [x for x in Path(path).parts if Path(x).suffix][0]
            root_dir = path[: path.find(external_id_prefix)].strip("/")
            external_id = path[path.find(external_id_prefix) :]

        elif Path(path).parts and not validate_suffix:
            external_id_prefix = ""
            root_dir = path.strip("/")
            external_id = ""
        else:
            raise ValueError("Path provided is not valid or the file name doesn't have a valid suffix")

        return "/" + root_dir, external_id_prefix, external_id

    def cache_path(self, root_dir: str, external_id: str, file_size: int) -> None:
        """Cache the file details in dircache to allow subsequent calls to make use of the file details.

        Args:
            root_dir (str): Root directory for the file.
            external_id (str): External Id for the file.
            file_size (int): File size (in bytes).
        """
        inp_path = Path(root_dir, external_id)
        file_meta = {"type": "file", "name": str(inp_path).lstrip("/"), "size": file_size}
        parent_path = str(inp_path.parent).lstrip("/")

        if parent_path not in self.dircache:
            self.dircache[parent_path] = [file_meta]
        else:
            self.dircache[parent_path].append(file_meta)

        grand_parent_path = str(Path(parent_path).parent).lstrip("/")
        dir_meta = {"type": "directory", "name": parent_path.lstrip("/")}
        if grand_parent_path not in self.dircache:
            self.dircache[grand_parent_path] = [dir_meta]
        else:
            self.dircache[grand_parent_path].append(dir_meta)

    def _add_dirs_to_cache(self, directories: Dict[str, Dict[str, Any]]) -> None:
        """Add all the directories extracted from file metadata to the dircache.

        Args:
            directories (Dict[str, Dict[str, Any]]): A dictionary with directory path as key and
            list of all it's child directories as its value.
        """
        for dir_name, dir_val in directories.items():
            parent_path = str(Path(dir_name).parent)
            if parent_path not in self.dircache:
                self.dircache[parent_path] = [dir_val]
            else:
                self.dircache[parent_path].append(dir_val)

    def _ls(self, root_dir: str, external_id_prefix: str) -> None:
        """List the files based on the directory & external Id prefixes extracted from path.

        Args:
            root_dir (str): Root directory for the file.
            external_id (str): External Id for the file.

        Raises:
            FileNotFoundError: An error occurred when extracting a file metadata.
        """
        try:
            directories: Dict[str, Dict[str, Any]] = {}
            list_query: Dict[str, str] = {}
            inp_key = str(Path(root_dir, external_id_prefix)).lstrip("/")

            # Add directory_prefix and external_id_prefix only if they are valid.
            if root_dir not in ("/", ""):
                list_query["directory_prefix"] = root_dir
            if external_id_prefix != "":
                list_query["external_id_prefix"] = external_id_prefix

            # Get all the files that were previously cached when writing. (if applicable)
            _file_write_cache = {d_info["name"]: True for d_path in self.dircache for d_info in self.dircache[d_path]}

            for file_met in self.cognite_client.files.list(**list_query, limit=-1):
                inp_path = Path(file_met.directory, file_met.external_id)
                file_name = str(inp_path).lstrip("/")
                file_size = int(file_met.metadata.get("size", -1)) if file_met.metadata else -1
                file_meta = {"type": "file", "name": file_name, "size": file_size}

                # Add directory information.
                parent_path = str(inp_path.parent).lstrip("/")
                if parent_path not in directories:
                    directories[parent_path] = {"type": "directory", "name": parent_path.lstrip("/")}

                if file_name not in _file_write_cache:
                    if parent_path not in self.dircache:
                        self.dircache[parent_path] = [file_meta]
                    else:
                        self.dircache[parent_path].append(file_meta)

            self._add_dirs_to_cache(directories)

            self.cdf_list_cache[inp_key] = time.time()

        except _COMMON_EXCEPTIONS as cognite_exp:
            raise FileNotFoundError from cognite_exp

    def ls(self, path: str, detail: bool = False, **kwargs: Optional[Any]) -> Union[Any, List[str]]:
        """List the files based on the directory & external Id prefixes extracted from path.

        Args:
            path (str): Path to use to extract the list of files from Cdf.
            detail (bool): Flag to specify if detail list is expected.
            **kwargs (Optional[Any]): Set of keyword arguments to support additional filtration's
            when listing the files.

        Returns:
            List: Returns the list of files/directories that match the path given.

        Raises:
            FileNotFoundError: When there are no files matching the path given.
        """
        root_dir, external_id_prefix, _ = self.split_path(path, validate_suffix=False)
        inp_key = str(Path(root_dir, external_id_prefix)).lstrip("/")
        if not (
            inp_key in self.dircache
            and inp_key in self.cdf_list_cache
            and time.time() - self.cdf_list_cache[inp_key] < self.cdf_list_expiry_time
        ):
            self._ls(root_dir, external_id_prefix)

        inp_path = path.rstrip("/")
        file_list = self.dircache.get(inp_path, [])
        if not file_list:
            raise FileNotFoundError

        if inp_path not in self.dircache:
            self.dircache[inp_path] = [{"type": "directory", "name": inp_path}]
        else:
            self.dircache[inp_path].append({"type": "directory", "name": inp_path})

        return self.dircache[inp_path] if detail else [x["name"] for x in self.dircache[inp_path]]

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """Create a directory at a path given.

        Args:
            path (str): Path to use to create a directory.
            exist_ok (bool): Flag to specify if error can be ignored when directory already exists.

        Raises:
            FileExistsError: When the directory prefixes already exists.
        """
        if path in self.dircache and not exist_ok:
            raise FileExistsError

        if path not in self.dircache:
            self.dircache[path] = [{"type": "directory", "name": path}]

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Optional[Any]) -> None:
        """Create a directory at a path given.

        Args:
            path (str): Path to use to create a directory.
            create_parents (bool): Flag to specify if parents needs to be created.
            **kwargs (Optional[Any]): Set of keyword arguments to support additional options
            to create a directory.
        """
        self.makedirs(path, exist_ok=bool(kwargs.get("exist_ok")))

    def makedir(self, path: str, create_parents: bool = False, **kwargs: Optional[Any]) -> None:
        """Create a directory at a path given.

        Args:
            path (str): Path to use to create a directory.
            create_parents (bool): Flag to specify if parents needs to be created.
            **kwargs (Optional[Any]): Set of keyword arguments to support additional options
            to create a directory.
        """
        self.makedirs(path)

    def rm(self, path: str, recursive: bool = False, maxdepth: Union[int, None] = None) -> None:
        """Remove the files and directories at a path given.

        Args:
            path (str): Path to use to remove the files and directories.
            recursive (bool): Flag to recursively delete a directory.
            maxdepth (Union[int, None]): Maximum depth to traverse and delete if recursive option is used.

        Raises:
            NotImplementedError: Error as it is not supported.
        """
        raise NotImplementedError

    def mv(
        self,
        source_path: str,
        destination_path: str,
        recursive: bool = False,
        maxdepth: Union[int, None] = None,
        **kwargs: Optional[Any],
    ) -> None:
        """Move the files and directories at a path given to a new path.

        Args:
            source_path (str): Path to use as source to move the files and directories.
            destination_path (str): Path to use as destination.
            recursive (bool): Flag to recursively move the files and directories.
            maxdepth (int): Maximum depth to use when moving the files and directories.
            **kwargs (Optional[Any]): Set of keyword arguments to support additional options
            to move directory.

        Raises:
            NotImplementedError: Error as it is not supported.
        """
        raise NotImplementedError

    def cd(self, path: str, **kwargs: Optional[Any]) -> None:
        """Change the directory to a path given.

        Args:
            path (str): Path to use to change directory.
            **kwargs (Optional[Any]): Set of keyword arguments to perform change directory.

        Raises:
            NotImplementedError: Error as it is not supported.
        """
        raise NotImplementedError

    def open(
        self,
        path: str,
        mode: str = "rb",
        block_size: str = "default",
        cache_options: Optional[Dict[Any, Any]] = None,
        **kwargs: Optional[Any],
    ) -> "CdfFile":
        """Open the file for reading and writing.

        Args:
            path (str): File name with absolute path to open.
            mode (str): Mode to use when opening a file.
            block_size (str): Block size to use when opening a file.
            cache_options: (Optional[Dict[Any, Any]]): Additional user defined options to work with caching.
            **kwargs (Optional[Any]): Set of keyword arguments to allow additional options
            when opening a file.

        Returns:
            CdfFile: An instance of a 'CdfFile' to allow reading/writing file to Cdf.
        """
        root_dir, _, external_id = self.split_path(path)
        return CdfFile(
            self,
            self.cognite_client,
            path,
            root_dir,
            external_id,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs,
        )

    def read_file(
        self, external_id: str, start_byte: Union[int, None] = None, end_byte: Union[int, None] = None
    ) -> Any:
        """Open and read the contents of a file.

        Args:
            external_id (str): External Id of the file to fetch the contents.
            start_byte (int): Start byte for the file only if a specific portion of the file is needed.
            end_byte (int): End offset for the file only if a specific portion of the file is needed.

        Returns:
            bytes: File contents as is from a cloud storage.

        Raises:
            FileNotFoundError: When there is no file matching the external_id given.
        """
        _download_retries = 0
        _retry_wait_seconds = 0.5
        _download_max_retries = 5
        while True:
            try:
                if not (download_url := self.file_handler.get_url(external_id)):
                    url_dict = self.cognite_client.files.retrieve_download_urls(external_id=external_id)
                    download_url = url_dict[external_id]
                    self.file_handler.add_or_update_url(external_id, download_url)

                return self.file_handler.download_file(download_url, start_byte, end_byte)

            except (CogniteAPIError, FileException) as cognite_exp:
                if _download_retries < _download_max_retries:
                    _download_retries += 1
                    time.sleep(_retry_wait_seconds)
                    _retry_wait_seconds *= 2
                    continue

                raise FileNotFoundError from cognite_exp

            except (*_COMMON_EXCEPTIONS, CogniteNotFoundError) as cognite_exp:
                raise FileNotFoundError from cognite_exp

    def cat(
        self, path: Union[str, list], recursive: bool = False, on_error: str = "raise", **kwargs: Optional[Any]
    ) -> Union[bytes, Any, Dict[str, bytes]]:
        """Open and read the contents of a file(s).

        Args:
            path (str): Path to use to extract the read the contents of file(s) from Cdf.
            recursive (bool): Flag to recursively read multiple files.
            on_error (str): Flag to indicate how to handle file read exceptions.
            **kwargs (Optional[Any]): Set of keyword arguments to read the file contents.

        Returns:
            bytes: File contents for the file name given.
            Dict[str, bytes]: File contents for the list of files given - if list of files were
            given as an argument it will be returned a dictionary with key as path name and contents
            of the files as value for each path.

        Raises:
            ValueError: When the path is empty.
        """
        if not path:
            ValueError("Path cannot be empty")

        if isinstance(path, list):
            out_data = {}
            for inp_path in path:
                external_id = self.split_path(inp_path)[2]
                out_data[inp_path] = self.read_file(external_id)
            return out_data
        else:
            external_id = self.split_path(path)[2]
            return self.read_file(external_id)

    def _fetch_file(self, external_id: str) -> AllBytes:
        """Read the contents of a file using external_id and Cache the file contents.

        if the file is already cached, it will return the cached file contents. Otherwise, It will
        fetch the data from CDF.

        Args:
            external_id (str): External Id of the file to fetch the contents.

        Returns:
            AllBytes: Cached file contents.
        """
        # If same file is requested again when it is already being downloaded, wait the download to complete.
        while external_id in self.file_cache and not self.file_cache[external_id]["fetch_status"]:
            time.sleep(_CACHE_SLEEP_INTERVAL)

        if external_id not in self.file_cache:
            self.file_cache[external_id] = {}
            self.file_cache[external_id]["fetch_status"] = False
            inp_data = self.read_file(external_id)
            self.file_cache[external_id]["cache"] = AllBytes(
                size=len(inp_data), fetcher=None, blocksize=None, data=inp_data
            )
            self.file_cache[external_id]["fetch_status"] = True

        return self.file_cache[external_id]["cache"]


class CdfFile(AbstractBufferedFile):
    """CDF File interface to work with a specific file.

    Provides an interface to a file to either read or write the data based on the mode defined.

    Attributes:
        cognite_client (ClientClient): Cognite client to work with Cdf.
        root_dir (str): Root directory for the file.
        external_id (str): External Id for the file.
        all_bytes_caching (bool): Flag to indicate if the cache type is all bytes caching.
    """

    def __init__(
        self,
        fs: CdfFileSystem,
        coginte_client: CogniteClient,
        path: str,
        directory: str,
        external_id: str,
        mode: str = "rb",
        block_size: str = "default",
        cache_options: Optional[Union[Dict[Any, Any], None]] = None,
        **kwargs: Optional[Any],
    ) -> None:
        """Initialize the CdfFile.

        Args:
            fs (CdfFileSystem): An instance of a CdfFileSystem.
            coginte_client (CogniteClient): Cognite client to work with Cdf.
            path (str): Absolute path for the file.
            directory (str): Root directory for the file.
            external_id (str): External Id for the file.
            mode (str): Mode to work with the file.
            block_size (str): Block size to read/write the file.
            cache_options (str): Additional caching options for the file.
            **kwargs (Optional[Any]): Set of keyword arguments to read/write the file contents.
        """
        self.cognite_client: CogniteClient = coginte_client
        self.root_dir: str = directory
        self.external_id: str = external_id
        self.all_bytes_caching: bool = "cache_type" in kwargs and kwargs["cache_type"] == "all"

        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs,
        )

    def _upload_chunk(self, final: bool = False) -> bool:
        """Upload file contents to CDF.

        Args:
            final (bool): Flag to indicate if this the last block.

        Returns:
            bool: Flag to indicate if the file contents are expected to buffered.

        Raises:
            RuntimeError: When an unexpected error occurred.
        """
        try:
            if final:
                response = self.cognite_client.files.upload_bytes(
                    content=self.buffer.getbuffer(),
                    name=Path(self.external_id).name,
                    external_id=self.external_id,
                    directory=self.root_dir,
                    source=self.fs.file_metadata.source,
                    asset_ids=self.fs.file_metadata.asset_ids,
                    data_set_id=self.fs.file_metadata.data_set_id,
                    mime_type=self.fs.file_metadata.mime_type,
                    geo_location=self.fs.file_metadata.geo_location,
                    metadata=(
                        self.fs.file_metadata.metadata.update({"size": self.buffer.getbuffer().nbytes})
                        if self.fs.file_metadata.metadata
                        else {"size": self.buffer.getbuffer().nbytes}
                    ),
                    overwrite=True,
                )

                self.fs.cache_path(
                    self.root_dir,
                    self.external_id,
                    int(response.metadata.get("size")) if response.metadata.get("size") else -1,
                )

            return final
        except _COMMON_EXCEPTIONS as cognite_exp:
            raise RuntimeError from cognite_exp

    def _fetch_range(self, start: int, end: int) -> Any:
        """Read file contents from CDF.

        Read all the file contents and preserve it in cache (AllBytes).

        Args:
            start (int): Start position of the file.
            end (int): End position of the file.

        Returns:
            bytes: Subset of file contents.
        """
        if self.all_bytes_caching:
            cache = self.fs._fetch_file(self.external_id)
            file_contents = cache._fetch(start, end)
        else:
            file_contents = self.fs.read_file(self.external_id, start_byte=start, end_byte=end)
        return file_contents
