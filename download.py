#!/usr/bin/env python3

"""
Python script to download the content of Onedata Share (https://onedata.org/#/home/documentation/20.02/doc/using_onedata/shares.html).
The script allows you to recursively download an entire directory structure or even a single file.
"""

import argparse
import os
import sys
import random
import re
import threading
import queue
from pathlib import Path
from typing import Optional, Generator

try:
    import requests
except ImportError:
    requests = None
    print(
        "ModuleNotFoundError: No module named 'requests' (module 'requests' is not installed)"
    )
    print("You can try install it by command:")
    print("pip3 install requests")
    print("Or you can follow the instructions available here:")
    print("https://github.com/CERIT-SC/onedata-downloader")
    sys.exit(1)


class Verbosity:
    DEF = 0
    V = 1
    VV = 2


V = Verbosity

"""Max (lowest) priority for downloading the file 
"""
MAX_PRIORITY: int = 3

"""Tries of downloading the file after error occurred
"""
TRIES_NUMBER: int = 2

"""
Default verbosity level.
"""
VERBOSITY = V.DEF

"""
Default Onezone service URL.
"""
DEFAULT_ONEZONE: str = "https://datahub.egi.eu"

"""
Used Onezone API URI.
"""
ONEZONE_API: str = "/api/v3/onezone/"

"""
Chunk size for downloading files as stream in bytes.
"""
CHUNK_SIZE: int = 32 * 1024 * 1024  # 32 MB - 33_554_432

"""
File extension of not yet completely downloaded (part) file.
"""
PART_FILE_EXTENSION: str = ".oddown_part"

"""
Threads number for parallel downloading.
"""
THREADS_NUMBER: int = 1

"""
Number of seconds between two tries to download the file
"""
TRIES_DELAY: int = 1

ONEZONE: str = DEFAULT_ONEZONE

DIRECTORY: Path = Path(".")

FILE_ID: Optional[str] = None


def priority_subtractor():
    iterator = 0
    while True:
        yield -1 if (iterator == 0) else 0
        iterator = (iterator + 1) % 3


class URLs:
    def __init__(self, onezone: str, file_id: str):
        self._content = onezone + ONEZONE_API + "shares/data/" + file_id + "/content"
        self._children = onezone + ONEZONE_API + "shares/data/" + file_id + "/children"
        self._node_attributes = onezone + ONEZONE_API + "shares/data/" + file_id

    @property
    def content(self):
        # https://onedata.org/#/home/api/stable/oneprovider?anchor=operation/download_file_content
        return self._content

    @property
    def children(self):
        # https://onedata.org/#/home/api/stable/oneprovider?anchor=operation/list_children
        return self._children

    @property
    def node_attrs(self):
        # https://onedata.org/#/home/api/stable/oneprovider?anchor=operation/get_attrs
        return self._node_attributes


class DownloadableItem(object):
    """Represents an item that can be downloaded from Onezone.
    This class encapsulates the necessary information for downloading a file,
    including the Onezone URL, file ID, node name, and the directory where the file will be saved.
    """

    def __init__(self, onezone: str, file_id: str, node_name: str, directory: Path):
        self._onezone: str = onezone
        self._file_id: str = file_id
        self._node_name: str = node_name
        self._directory: Path = directory
        self._priority: int = MAX_PRIORITY  # internal value, lowering
        self._ttl: int = TRIES_NUMBER
        self._part_filename: str = generate_random_string(size=16) + PART_FILE_EXTENSION
        self._priority_subtractor: Generator = priority_subtractor()
        self._path: Path = self._directory / self._node_name  # not to compute it again
        self._part_path: Path = (
            self._directory / self._part_filename
        )  # not to compute it again
        self._urls = URLs(self._onezone, self._file_id)

    @property
    def onezone(self) -> str:
        """The Onezone URL where the file is hosted."""
        return self._onezone

    @property
    def file_id(self) -> str:
        """The unique identifier of the file (file id) in Onezone."""
        return self._file_id

    @property
    def node_name(self) -> str:
        """The name of the node (file or directory) in Onezone."""
        return self._node_name

    @property
    def directory(self) -> Path:
        """The directory where the file will be saved."""
        return self._directory

    @property
    def path(self) -> Path:
        """The full path where the file will be saved, including the directory and node name."""
        return self._path

    @property
    def priority(self) -> int:
        """Number representing priority, lower number is higher priority"""
        return MAX_PRIORITY - self._priority

    @property
    def part_filename(self) -> str:
        """The name of the part file that is used during the download process."""
        return self._part_filename

    @property
    def part_path(self) -> Path:
        return self._part_path

    @property
    def URL(self) -> URLs:
        """URLs object containing the URLs for downloading the file content and its attributes."""
        return self._urls

    def _decrease_priority(self) -> None:
        """Lowers the priority by one step"""
        self._priority = max(0, self._priority + next(self._priority_subtractor))

    def try_to_download(self) -> bool:
        if self._ttl == 0:
            return False

        self._ttl -= 1
        self._decrease_priority()
        return True

    def __lt__(self, other) -> bool:
        """Sorting by priority, lower number is higher priority"""
        if not isinstance(other, DownloadableItem):
            raise TypeError(
                f"Instance of {type(other).__name__} cannot be compared with {type(self).__name__}"
            )
        return self.priority < other.priority


class QueuePool:
    """A pool of queues with weighted fair access to them.
    This class allows multiple threads to access queues in a fair manner based on their weights.
    Each queue has a weight, and the access to the queues is distributed according to these weights.
    The queues are accessed in a round-robin manner,
    with the weights determining how many times each queue is accessed before moving to the next one.

    Round-robin by example:
        - If there are two queues with weights 2 and 1, the access order will be: 0, 0, 1, 0, 0, 1, ...
        - If there are three queues with weights 1, 2, and 3, the access order will be: 0, 1, 1, 2, 1, 1, 2, 0, ...
    """

    def __init__(self, queues: tuple[queue.Queue, ...], weights: tuple[int, ...]):
        if len(queues) != len(weights):
            raise AttributeError(
                "Number of queues must be equal to number of their weights"
            )

        self._queues = queues
        """A tuple of queues to be managed."""
        self._weights = weights
        """A tuple of weights corresponding to each queue."""

        # create a list of indices according to their weights
        _weights = []
        for item in [[index] * weight for index, weight in enumerate(weights)]:
            _weights.extend(item)
        random.shuffle(_weights)  # dequeue to be more fair

        self._weight_queue = queue.Queue()
        """A queue that holds the indices of the queues according to their weights."""
        # fill the weight queue with indices of queues according to their weights
        for weight in _weights:
            self._weight_queue.put(weight)

        self._queue_to_finish = 0
        """The index of the queue that is currently being finished. It is increased when the last item in the queue is processed."""
        self._mutex = threading.Lock()
        """A mutex to ensure that only one thread can access the queue at a time (critical section)."""

    def __len__(self):
        return len(self._queues)

    def join(self) -> None:
        """Waits for all queues to be processed."""
        for key, act_queue in enumerate(self._queues):
            act_queue.join()

    def _increase_weight(self, index: int, thread_number: int) -> None:
        """Increases the index of the queue which is currently being finished.

        Arguments:
            index (int): The index of the queue to increase the weight for.
            thread_number (int): The number of the thread that is trying to increase the weight.
        """
        v_print(
            V.VV, f"Thread {thread_number}: Trying to increase weight of queue {index}"
        )

        # the last queue cannot be skipped
        if index == len(self) - 1:
            v_print(V.VV, f"Thread {thread_number}: Last queue, not increasing weight")
            return

        passed_queue_items = 0
        v_print(
            V.VV,
            f"Thread {thread_number}: Removing indices of index {index} from the weight queue",
        )
        v_print(
            V.V,
            f"Thread {thread_number}: Possible deadlock while removing indices of index {index} from the weight queue",
        )
        while self._weight_queue.qsize() != sum(
            self._weights[self._queue_to_finish + 1 :]
        ) and passed_queue_items < sum(self._weights):
            # deadlock possibility if different thread has not yet put number back
            try:
                v_print(
                    V.VV, f"Thread {thread_number}: Getting index from the weight queue"
                )
                act_ind = self._weight_queue.get(block=False)
                v_print(
                    V.VV,
                    f"Thread {thread_number}: Got index {act_ind} from the weight queue",
                )
            except queue.Empty:
                v_print(
                    V.VV, f"Thread {thread_number}: Weight queue is empty, continuing"
                )
                passed_queue_items += 1
                continue
            if act_ind != index:
                v_print(
                    V.VV,
                    f"Thread {thread_number}: Putting index back to the weighted queue",
                )
                self._weight_queue.put(act_ind)
            passed_queue_items += 1

        v_print(V.V, f"Thread {thread_number}: Possible deadlock not occurred")
        v_print(
            V.VV,
            f"Thread {thread_number}: Indices of index {index} removed from the weight queue",
        )

        if self._queue_to_finish == index:
            v_print(
                V.V,
                f"Thread {thread_number}: Increasing queue index {self._queue_to_finish}++",
            )
            self._queue_to_finish += 1

    def _try_to_increase_weight(self, index: int, thread_number: int) -> int:
        """Tries to increase the weight of the queue at the given index.
        This method acquires a mutex to ensure that only one thread can access the critical section at a time.

        Arguments:
            index (int): The index of the queue to increase the weight for.
            thread_number (int): The number of the thread that is trying to increase the weight.

        Returns:
            int: The index of the queue that should be accessed by the thread after trying to increase the weight.
        """

        v_print(V.VV, f"Thread {thread_number}: Trying to acquire mutex")
        self._mutex.acquire(blocking=True)
        v_print(V.VV, f"Thread {thread_number}: Mutex acquired")

        if not (
            self._queue_to_finish >= index
            and self.get_queue(index, thread_number).qsize() == 0
        ):
            v_print(V.VV, f"Thread {thread_number}: Condition not met, releasing mutex")
            self._mutex.release()
            index = self._weight_queue.get()  # the index is put back to the queue later
            return index

        self._increase_weight(index, thread_number)

        v_print(V.VV, f"Thread {thread_number}: Releasing mutex")
        self._mutex.release()

        index = self._weight_queue.get()  # the index is put back to the queue later
        return index

    def fair_queue_index(self, thread_number: int) -> int:
        """Returns the index of the queue that should be accessed by the thread.
        This method ensures that the access to the queues is fair and based on their weights.
        If the queue at the returned index is empty,
        it tries to increase the index of the queue that is currently being finished.

        Arguments:
            thread_number (int): The number of the thread that is trying to access the queue.

        Returns:
            int: The index of the queue that should be accessed by the thread.
        """
        v_print(
            V.VV, f"Thread {thread_number}: Acquiring index of the queue to be used"
        )
        index = self._weight_queue.get()
        v_print(
            V.VV, f"Thread {thread_number}: Got index {index} from the weight queue"
        )

        if (
            self._queue_to_finish >= index
            and self.get_queue(index, thread_number).qsize() == 0
        ):
            v_print(
                V.VV,
                f"Thread {thread_number}: Queue {index} is empty, trying to increase weight",
            )
            self._weight_queue.put(index)
            index = self._try_to_increase_weight(index, thread_number)

        v_print(
            V.VV, f"Thread {thread_number}: Returning index {index} to the weight queue"
        )
        self._weight_queue.put(index)
        return index

    def get_queue(self, index: int, thread_number: int = None) -> queue.Queue:
        """Returns the queue at the given index.

        Arguments:
            index (int): The index of the queue to return.
            thread_number (int, optional): The number of the thread that is trying to access the queue.

        Returns:
            queue.Queue: The queue at the given index.

        Raises:
            IndexError: If the index is out of bounds.
        """
        v_print(V.VV, f"Thread {thread_number}: Getting queue at index {index}")
        if index >= len(self._queues) or index < 0:
            raise IndexError("Queue index out of bounds")
        return self._queues[index]


ROOT_DIRECTORY_SIZE = 0
ALL_DIRECTORIES = 0
DIRECTORIES_CREATED = 0
DIRECTORIES_NOT_CREATED_OS_ERROR = 0

ALL_FILES = 0
EXISTENT_FILES = queue.Queue()
FINISHED_FILES = queue.Queue()
PART_FILES = queue.Queue()


_file_queue = queue.Queue()
_priority_file_queue = queue.PriorityQueue()
QP = QueuePool(queues=(_file_queue, _priority_file_queue), weights=(15, 1))

ERROR_QUEUE = queue.Queue()


def convert_chunk_size(chunk_size: str) -> int:
    """Converts user-given chunk size to integer.
    User can input values as number (bytes) or number + unit (eg. 32M)

    Arguments:
        chunk_size (str): The chunk size as a string, e.g. "32M", "16k", "2G".

    Returns:
        int: The chunk size in bytes, or -1 if the input is invalid.
    """
    chunk_size = chunk_size.strip()
    unit = "b"
    if chunk_size[-1].isalpha():
        unit = chunk_size[-1].lower()
        chunk_size = chunk_size[:-1]

    try:
        chunk_size = int(chunk_size)
    except ValueError as e:
        v_print(
            V.DEF,
            f"Failed while converting size to integer, exception occured: {e.__class__.__name__}",
        )
        return -1

    units = ("b", "k", "m", "g")
    # unit already lowercased
    if unit not in units:
        v_print(
            V.DEF,
            "failed while converting mapping unit, unit is not in the right format",
        )
        return -1

    unit_power = units.index(unit)
    chunk_size = chunk_size * (1024**unit_power)

    return chunk_size


def generate_random_string(size: int = 16) -> str:
    """Generates random string of characters of given size

    Arguments:
        size (int): The size of the random string to generate. Default is 16.

    Returns:
        str: A random string of the specified size, or an empty string if size is negative.
    """
    if size < 0:
        return ""

    characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890"
    random_string = "".join(random.choices(characters, k=size))
    return random_string


def remove_part_files(root_directory: Path) -> int:
    """Removes files in a tree with extension defined by global value PART_FILE_EXTENSION
    from the given directory and its subdirectories.

    Arguments:
        root_directory (str): The directory to search for part files.

    Returns:
        int: 0 if successful, 1 if an error occurred.
    """
    pattern = ".*\\" + PART_FILE_EXTENSION + "$"
    try:
        for root, directories, files in os.walk(root_directory):
            for actual_file in files:
                if not re.match(pattern, actual_file):
                    continue

                file_path = Path(root) / actual_file
                try:
                    os.remove(
                        file_path
                    )  # cannot get OSError, because not going through directories
                except FileNotFoundError:
                    v_print(V.DEF, f"cannot remove {file_path}, it does not exist")
                else:
                    v_print(V.DEF, f"Partially downloaded file {file_path} removed")
    except OSError as e:
        v_print(
            V.DEF,
            "failed while removing part files, exception occured:",
            e.__class__.__name__,
        )
        return 1

    return 0


def verbose_print(level: int, *args, **kwargs) -> None:
    """Prints messages to the console based on the verbosity level.

    Arguments:
        level (int): The verbosity level required to print the message.
        *args: as usual for print function
        **kwargs: as usual for print function
    """
    if VERBOSITY >= level:
        print(*args, **kwargs)


v_print = verbose_print


def error_printer(
    response: requests.Response, thread_number: int, file: DownloadableItem
) -> None:
    """Prints error messages when the download fails.

    Arguments:
        response (requests.Response): The HTTP response object containing the error.
        thread_number (int): The number of the thread that encountered the error.
        file (DownloadableItem): The file that was being downloaded when the error occurred.
    """
    v_print(V.DEF, "failed", end="")
    response_json = response.json()

    v_print(V.V, f"Thread {thread_number}:", end=" ")
    if (
        "error" in response_json  # dict
        and "details" in response_json["error"]  # dict
        and "errno" in response_json["error"]["details"]
        and response_json["error"]["details"]["errno"] in ("eaccess", "enoent")
    ):
        if "eacces" in response_json["error"]["details"]["errno"]:
            v_print(
                V.DEF,
                f"Downloading of {file.path} failed, response error: permission denied",
            )
        if "enoent" in response_json["error"]["details"]["errno"]:
            v_print(
                V.DEF,
                f"Downloading of {file.path} failed, response error: no such file or directory",
            )
    else:
        v_print(
            V.DEF,
            f"Downloading of {file.path} failed, returned HTTP response code = {response.status_code}",
        )

    v_print(V.V, f"Thread {thread_number}:", response_json)


def chunkwise_downloader(
    request: requests.Response, file: DownloadableItem, thread_number: int
) -> int:
    """Downloads the file in chunks and writes it to the part file.

    Arguments:
        request (requests.Response): The HTTP response object containing the file content.
        file (DownloadableItem): The file to download.
        thread_number (int): The number of the thread performing the download.

    Returns:
        int: 0 if successful, 1 if an error occurred.
    """
    v_print(V.VV, f"Thread {thread_number}:", end=" ")
    v_print(V.V, f"Downloading file {file.path} in chunks", end=" ")
    try:
        with open(
            file.part_path, "ab"
        ) as f:  # if file was already opened and written into, it will continue
            for chunk in request.iter_content(
                chunk_size=CHUNK_SIZE, decode_unicode=True
            ):
                # for chunk in request.iter_content():
                f.write(chunk)
                # flushing automatically as OS says
        # the file is closed now
    except EnvironmentError as e:
        v_print(V.V, f"Thread {thread_number}:", end=" ")
        v_print(V.DEF, f"Failed {file.path}, exception occured:", e.__class__.__name__)
        v_print(V.V, str(e))
        return 1

    return 0


def renamer(file: DownloadableItem, thread_number: int) -> int:
    """Renames the part file to the final file name.

    Arguments:
        file (DownloadableItem): The file to rename.
        thread_number (int): The number of the thread performing the operation.

    Returns:
        int: 0 if successful, 1 if an error occurred.
    """
    try:
        os.rename(file.part_path, file.path)
        FINISHED_FILES.put(file.path)

        v_print(
            V.VV,
            f"Thread {thread_number}: {file.part_filename} renamed to {file.path}",
        )
        v_print(V.V, f"Thread {thread_number}:", end=" ")
    except OSError:
        v_print(
            V.V,
            f"Thread {thread_number}: Could not rename {file.part_filename} to {file.path}",
        )
        return 1

    return 0


def download_file(file: DownloadableItem, thread_number: int):
    """Download file with given file_id to given directory."""
    v_print(
        V.VV,
        f"download_file({file.onezone}, {file.file_id}, {file.node_name}, {file.directory})",
    )
    # don't download the file when it exists

    v_print(V.V, f"Thread {thread_number}:", end=" ")
    v_print(V.V, "Downloading file", file.path, end=" ")
    v_print(V.VV, " (temporary filename " + file.part_filename + ") ", end="")
    v_print(V.V, "started", flush=True)

    if os.path.exists(file.path):
        EXISTENT_FILES.put(file.path)
        v_print(V.V, f"Thread {thread_number}:", end=" ")
        v_print(V.DEF, "File", file.path, "exists, skipped")
        return 0

    headers = {}
    already_downloaded = 0
    if os.path.exists(file.part_path):  # incorrectly downloaded
        v_print(V.VV, f"Thread {thread_number}:", end=" ")
        v_print(V.V, f"part file exists ({file.part_path})", end=", ")
        already_downloaded = os.path.getsize(file.part_path)
        v_print(V.V, f"already downloaded {already_downloaded} bytes")
        headers["Range"] = f"bytes={already_downloaded}-"

    with requests.get(
        file.URL.content, headers=headers, allow_redirects=True, stream=True
    ) as request:
        if request.status_code == 416:
            v_print(V.VV, f"Thread {thread_number}:", end=" ")
            v_print(
                V.V,
                "got status code 416 while downloading, trying to get the original size",
            )
            with requests.get(
                file.URL.content, allow_redirects=True, stream=True
            ) as request_size:
                original_size = request_size.headers.get("content-length")
                if already_downloaded != original_size:
                    v_print(
                        V.V,
                        f"the original size does not match, already downloaded: {already_downloaded}, "
                        f"file size: {original_size}",
                    )
                    return 5
                v_print(
                    V.V,
                    f"the original size does matches, the size is: {already_downloaded}",
                )
        else:
            if not request.ok:
                error_printer(request, thread_number, file)
                return 2

            if chunkwise_downloader(request, file, thread_number) != 0:
                return 3

    if renamer(file, thread_number) != 0:
        return 4

    v_print(V.DEF, f"Downloading file {file.path} was successful")

    return 0


def process_directory(onezone: str, file_id, file_name: str, directory: Path):
    """
    Process directory and recursively its content.
    """
    v_print(V.VV, f"process_directory({onezone}, {file_id}, {file_name}, {directory})")
    global ALL_DIRECTORIES
    global DIRECTORIES_CREATED
    global DIRECTORIES_NOT_CREATED_OS_ERROR
    # don't create the the directory when it exists
    v_print(V.DEF, "Processing directory", directory / file_name, flush=True)
    try:
        os.mkdir(directory / file_name, mode=0o777)
        DIRECTORIES_CREATED += 1
        v_print(V.V, "directory created")
    except FileExistsError:  # directory already existent
        v_print(V.DEF, "directory exists, not created")
    except FileNotFoundError as e:  # parent directory non existent
        DIRECTORIES_NOT_CREATED_OS_ERROR += 1
        v_print(V.DEF, "failed, exception occured:", e.__class__.__name__)
        v_print(V.V, str(e))
        return 2

    # get content of new directory

    response = requests.get(URLs(onezone, file_id).children)
    if not response.ok:
        v_print(V.DEF, "Error: failed to process directory", file_name)
        v_print(V.V, "processed directory", file_name, " with File ID =", file_id)
        v_print(V.V, response.json())
        return 2

    response_json = response.json()
    result = 0
    # process child nodes
    for child in response_json["children"]:
        # difference between Onezone version 20 and 21 in name of the key containing the file_id attribute
        if "file_id" in child:
            child_file_id = child["file_id"]
        else:
            child_file_id = child["id"]
        result = process_node(onezone, child_file_id, directory / file_name) or result

    return result


def process_node(onezone: str, file_id: str, directory: Path):
    """
    Process given node (directory or file).
    """
    v_print(V.VV, "process_node(%s, %s, %s)" % (onezone, file_id, directory))
    global ROOT_DIRECTORY_SIZE
    global ALL_FILES
    global ALL_DIRECTORIES
    # get basic node's attributes

    response = requests.get(URLs(onezone, file_id).node_attrs)
    if response.ok:
        response_json = response.json()
        node_type = response_json["type"].upper()
        node_name = response_json["name"]
        node_size = response_json["size"]

        if node_size > ROOT_DIRECTORY_SIZE:
            ROOT_DIRECTORY_SIZE = node_size

        result = 0
        # check if node is directory or folder
        if node_type == "REG" or node_type == "SYMLNK":
            ALL_FILES += 1
            node_path = os.path.join(directory, node_name)
            if os.path.exists(node_path):
                EXISTENT_FILES.put(node_path)
                v_print(V.DEF, f"File {node_path} exists, it will not be downloaded")
                return 0

            v_print(V.V, "Adding file to queue", node_path)
            file_queue = QP.get_queue(0)
            file_queue.put(DownloadableItem(onezone, file_id, node_name, directory))
        elif node_type == "DIR":
            ALL_DIRECTORIES += 1
            result = process_directory(onezone, file_id, node_name, directory) or result
        else:
            v_print(V.DEF, "Error: unknown node type")
            v_print(
                V.V, "returned node type", node_type, " of node with File ID =", file_id
            )
            v_print(V.V, response.json())
            return 2

        return result
    else:
        v_print(
            V.DEF,
            "Error: failed to retrieve information about the node. The requested node may not exist.",
        )
        v_print(V.V, "requested node File ID =", file_id)
        v_print(V.V, response.json())
        return 1


def clean_onezone(onezone):
    """
    Clean and test of given Onezone service.
    """
    # add protocol name if not specified
    if not onezone.startswith("https://") and not onezone.startswith("http://"):
        onezone = "http://" + onezone

    v_print(V.V, "Use Onezone:", onezone)

    # test if such Onezone exists
    url = onezone + ONEZONE_API + "configuration"
    try:
        response = requests.get(url)
    except Exception as e:
        v_print(
            V.DEF, "Error: failure while trying to communicate with Onezone:", onezone
        )
        v_print(V.V, str(e))
        sys.exit(2)

    if not response.ok:
        v_print(V.DEF, "Error: failure while connecting to Onezone:", onezone)
        sys.exit(2)

    try:
        response_json = response.json()
        v_print(V.VV, "Onezone configuration:")
        v_print(V.VV, response_json)
    except Exception as e:
        v_print(
            V.DEF,
            "Error: failure while parsing JSON response from Onezone:",
            e.__class__.__name__,
        )
        sys.exit(2)

    # get Onezone version
    onezone_version = response_json["version"].split(".")[0]  # 21.02.0-alpha28 -> 21
    v_print(V.V, "Onezone version:", onezone_version)

    return onezone


def clean_directory(directory):
    """Checks if the given directory exists, and if not, tries to create it.

    Arguments:
        directory (str): The directory to check

    Returns:
        int: 0 if the directory exists or was created successfully, 1 if an error occurred.

    Raises:
        SystemExit: If the directory cannot be created.
    """
    # test if given directory exists
    if not os.path.isdir(directory):
        v_print(V.DEF, "Error: output directory", directory, "does not exist")
        sys.exit(2)

    return directory


def thread_worker(thread_number: int) -> int:
    """Worker function for each thread to download files from the queue.

    Arguments:
        thread_number (int): The number of the thread.

    Returns:
        int: Return code indicating success (0) or various error states.
    """
    result = 0

    while True:
        queue_index = QP.fair_queue_index(thread_number)
        actual_queue = QP.get_queue(queue_index, thread_number)
        try:
            v_print(
                V.V,
                f"Thread {thread_number}: Acquiring download or blocked state in queue {queue_index}",
            )
            downloadable_item: DownloadableItem = actual_queue.get(block=True)
            v_print(V.VV, f"Thread {thread_number}: Acquired download in {queue_index}")
        except queue.Empty:  # incorrect queue
            v_print(
                V.VV,
                f"Thread {thread_number}: Queue {queue_index} is empty, continuing",
            )
            continue

        if queue_index == 0:
            PART_FILES.put(downloadable_item.part_path)

        v_print(
            V.VV,
            f"Thread: {thread_number}, Actual queue index: {queue_index}, file priority: {downloadable_item.priority}, ttl: {downloadable_item._ttl}",
        )

        if downloadable_item.try_to_download():
            result = download_file(downloadable_item, thread_number)

            if result != 0:
                QP.get_queue(1).put(downloadable_item)
        else:
            ERROR_QUEUE.put(
                f"The file {downloadable_item.path} could not be downloaded"
            )

        actual_queue.task_done()

    return result


def print_download_statistics(directory_to_search: Path, finished: bool = True):
    errors = ERROR_QUEUE.qsize()

    existent_files = EXISTENT_FILES.qsize()
    finished_files = FINISHED_FILES.qsize()

    part_size = 0
    while not PART_FILES.empty():
        file_path = PART_FILES.get()
        if os.path.exists(file_path):
            part_size += os.path.getsize(file_path)

    finished_size = 0
    while not FINISHED_FILES.empty():
        file_path = FINISHED_FILES.get()
        finished_size += os.path.getsize(file_path)

    existent_size = 0
    while not EXISTENT_FILES.empty():
        file_path = EXISTENT_FILES.get()
        existent_size += os.path.getsize(file_path)

    downloaded_size = finished_size + part_size

    print()
    if errors != 0:
        print("Errors during execution:")
        while not ERROR_QUEUE.empty():
            print(ERROR_QUEUE.get())
        print()

    print("Download statistics:")
    if ALL_FILES != 0:
        print(
            f"Files created: {finished_files}/{ALL_FILES} ({(finished_files/ALL_FILES * 100):.2f}%), already existent: {existent_files}, error while creating: {ALL_FILES - (existent_files + finished_files)}"
        )
    else:
        print(
            f"Files created: 0, already existent: {existent_files}, error while creating: {ALL_FILES - (existent_files + finished_files)}"
        )
    if ALL_DIRECTORIES != 0:
        print(
            f"Directories created: {DIRECTORIES_CREATED}/{ALL_DIRECTORIES} ({DIRECTORIES_CREATED/ALL_DIRECTORIES * 100:.2f}%), already existent: {ALL_DIRECTORIES - (DIRECTORIES_NOT_CREATED_OS_ERROR + DIRECTORIES_CREATED)}, error while creating: {DIRECTORIES_NOT_CREATED_OS_ERROR}"
        )
    else:
        print(
            f"Directories created: 0, already existent: {ALL_DIRECTORIES - (DIRECTORIES_NOT_CREATED_OS_ERROR + DIRECTORIES_CREATED)}, error while creating: {DIRECTORIES_NOT_CREATED_OS_ERROR}"
        )
    if ROOT_DIRECTORY_SIZE != 0:
        print(
            f"Downloaded size: {downloaded_size}/{ROOT_DIRECTORY_SIZE} bytes ({downloaded_size/ROOT_DIRECTORY_SIZE * 100:.2f}%), finished: {finished_size} bytes, existent: {existent_size} bytes, part files: {part_size} bytes, not downloaded yet or error: {ROOT_DIRECTORY_SIZE - (finished_size + existent_size + part_size)} bytes"
        )
    else:
        print(
            f"Downloaded size: 0 bytes, finished: {finished_size} bytes, existent: {existent_size} bytes, part files: {part_size} bytes, not downloaded yet or error: {ROOT_DIRECTORY_SIZE - (finished_size + existent_size + part_size)} bytes"
        )
    if not finished:
        print("RESULTS MAY BE INCORRECT, PROGRAM DID NOT FINISH CORRECTLY")


def setup_parser() -> argparse.ArgumentParser:
    """Sets up the command line argument parser for the script.
    Lists all available options and their defaults.

    Returns:
        argparse.ArgumentParser: Configured argument parser.
    """
    parser = argparse.ArgumentParser(
        description="Script allowing you to download a complete shared space, directory or a single file from the Onedata system."
    )
    parser.add_argument(
        "-o",
        "--onezone",
        default=DEFAULT_ONEZONE,
        type=str,
        help="Onedata Onezone URL with specified protocol (default: https://datahub.egi.eu)",
    )
    parser.add_argument(
        "-d",
        "--directory",
        default=".",
        type=str,
        help="Output directory (default: current directory)",
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        default="32M",
        type=str,
        help="The size of downloaded file segments (chunks) after which the file is written to disk. Value can be in bytes, or a number with unit e.g. 16k, 32M or 2G (default: 32M).",
    )
    parser.add_argument(
        "-j",
        "--threads-number",
        default=1,
        type=int,
        help="Number of threads for parallel downloading. Setting this parameter to a reasonable value can significantly reduce the overall download time (default: 1).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Set verbose (debug) prints. There are two levels of these prints.",
    )
    parser.add_argument(
        "file_id", type=str, help="Public File ID of shared space, directory or a file"
    )

    return parser


def process_parser(parser: argparse.ArgumentParser) -> int:
    """Processes the command line arguments and sets global variables accordingly.

    Arguments:
        parser (argparse.ArgumentParser): The argument parser with defined options.

    Returns:
        int: Return code indicating success (0) or various error states.

        - 1: Invalid Onezone URL.
        - 2: Invalid directory.
        - 3: Invalid chunk size.
        - 4: Invalid number of threads.
        - 5: Error while removing part files.
    """
    args = parser.parse_args()

    # set up verbosity level
    global VERBOSITY
    VERBOSITY = args.verbose

    global CHUNK_SIZE
    CHUNK_SIZE = convert_chunk_size(args.chunk_size)
    if CHUNK_SIZE < 0:
        return 3

    global THREADS_NUMBER
    THREADS_NUMBER = args.threads_number
    if THREADS_NUMBER < 1:
        v_print(V.DEF, "failed on startup; number of threads cannot be lower than one")
        return 4

    global ONEZONE
    ONEZONE = clean_onezone(args.onezone)

    global DIRECTORY
    DIRECTORY = clean_directory(args.directory)

    global FILE_ID
    FILE_ID = args.file_id

    return 0


def main():
    """Main function to execute the script.

    Returns:
        int: Return code indicating success (0) or various error states.

        - 0: Success.
        - 1: Invalid Onezone URL.
        - 2: Invalid directory.
        - 3: Invalid chunk size.
        - 4: Invalid number of threads.
        - 5: Error while removing part files.
        - 6: Error while processing the node.
    """
    parser = setup_parser()
    result = process_parser(parser)
    if result != 0:
        return result

    result = remove_part_files(DIRECTORY)
    if not result:
        return 5

    try:
        v_print(V.DEF, "Exploring and creating the directory structure")
        result = process_node(ONEZONE, FILE_ID, DIRECTORY)
        if result:
            print_download_statistics(DIRECTORY, finished=False)
            return result

        v_print(V.DEF, "Downloading files")
        for thread_number in range(THREADS_NUMBER):
            result = (
                threading.Thread(
                    target=thread_worker, args=(thread_number,), daemon=True
                ).start()
                or result
            )
        QP.join()
        result = 0 if ERROR_QUEUE.qsize() == 0 else 1
        print_download_statistics(DIRECTORY)
        return result
    except KeyboardInterrupt as e:
        v_print(V.DEF, " prematurely interrupted (" + e.__class__.__name__ + ")")
        print_download_statistics(DIRECTORY, finished=False)
        return 2


if __name__ == "__main__":
    return_code = main()
    sys.exit(return_code)
