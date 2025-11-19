"""Unit test utilities used by many tests."""
from contextlib import ExitStack
import os
import subprocess
import pathlib
import multiprocessing
import json
import time
import socket
import glob
from utils.memory import MemoryProfiler


# Temporary directory.  Tests will create files here.
TMPDIR = pathlib.Path("tmp")

# Directory containing unit test input files, mapper executables,
# reducer executables, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent.parent/"testdata"

# Time in between two heart beats.  This in the spec.
TIME_BETWEEN_HEARTBEATS = 2

# Set default timeout and long timeout for tests where we need to
# wait for Workers to die.
#
# We'll need longer wait times on slow machines like the autograder.
if pathlib.Path("/home/autograder/working_dir").exists():
    TIMEOUT = 30
    TIMEOUT_LONG = 60
else:
    TIMEOUT = 10
    TIMEOUT_LONG = 30


class PathJSONEncoder(json.JSONEncoder):
    """
    Extended the Python JSON encoder to encode Pathlib objects.

    Docs: https://docs.python.org/3/library/json.html

    Usage:
    >>> json.dumps({
            "executable": TESTDATA_DIR/"exec/wc_map.sh",
        }, cls=PathJSONEncoder)
    """

    # Avoid pylint false positive.  There's a style problem in the JSON library
    # that we're inheriting from.
    # https://github.com/PyCQA/pylint/issues/414#issuecomment-212158760
    # pylint: disable=E0202

    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)


def worker_heartbeat_generator(mock_sendall, *ports):
    """Fake Worker heartbeat messages."""
    if mock_sendall:
        # Wait until the Manager sends at least one message before starting
        # to mock heartbeats.
        while not get_messages(mock_sendall):
            time.sleep(1)

        # Verify that the first message is a register_ack and start mocking
        # heartbeats
        assert get_messages(mock_sendall)[0]["message_type"] == "register_ack"

    while True:
        time.sleep(TIME_BETWEEN_HEARTBEATS)
        # Avoid sending heartbeats too fast
        for port in ports:
            time.sleep(TIME_BETWEEN_HEARTBEATS)
            # Avoid sending heartbeats too fast
            yield json.dumps({
                "message_type": "heartbeat",
                "worker_host": "localhost",
                "worker_port": port,
            }).encode("utf-8")


def get_messages(mock_sendall):
    """Return a list decoded JSON messages sent via mock socket sendall()."""
    messages = []
    for args, _ in mock_sendall.call_args_list:
        message_str = args[0].decode("utf-8")
        message_dict = json.loads(message_str)
        messages.append(message_dict)
    return messages


def get_message_destinations(mock_connect):
    """Return a list of host/port tuples to which TCP messages were sent."""
    return [args[0] for args, _ in mock_connect.call_args_list]


def get_messages_with_destinations(mock_sendall, mock_connect):
    """Return a list of tuples containing JSON messages and destinations.

    The output will be in the format:

    [
        (
            {
                "message_type": "",
                ...
            },
            {
                "destination": ("localhost", PORT),
            },
        ),
    ]
    Each tuple in the list holds a message and the host/port that received it.
    """
    messages = get_messages(mock_sendall)
    destinations = get_message_destinations(mock_connect)
    assert len(messages) == len(destinations), (
        "sock.connect() was not called the same number of times "
        "as sock.sendall()."
    )
    destinations = [
        {"destination": destination} for destination in destinations
    ]
    return list(zip(messages, destinations))


def is_register_message(message):
    """Return True if message is Worker registration."""
    return (
        "message_type" in message and
        message["message_type"] == "register"
    )


def is_register_ack_message(message):
    """Return True if message is Worker registration ACK."""
    return (
        "message_type" in message and
        message["message_type"] == "register_ack"
    )


def is_map_message(message):
    """Return True if message starts a map job."""
    return (
        "message_type" in message and
        message["message_type"] == "new_map_task"
    )


def is_reduce_message(message):
    """Return True if message starts a reduce job."""
    return (
        "message_type" in message and
        message["message_type"] == "new_reduce_task"
    )


def is_status_finished_message(message):
    """Return True message is a status finished message."""
    return (
        "message_type" in message and
        message["message_type"] == "finished"
    )


def is_heartbeat_message(message):
    """Return True if message is a heartbeat message."""
    return (
        "message_type" in message and
        message["message_type"] == "heartbeat"
    )


def filter_heartbeat_messages(messages):
    """Return a subset of messages including only heartbeat messages."""
    return [m for m in messages if is_heartbeat_message(m)]


def filter_not_heartbeat_messages(messages):
    """Return a subset of messages excluding heartbeat messages."""
    return [m for m in messages if not is_heartbeat_message(m)]


def wait_for_exists(*paths):
    """Return when paths exist."""
    for _ in range(TIMEOUT):
        if all(os.path.exists(p) for p in paths):
            return
        time.sleep(1)
    raise FileNotFoundError(f"Failed to create paths: {paths}")


def wait_for_not_exists(*paths):
    """Return when paths no longer exist."""
    for _ in range(TIMEOUT):
        if all(not os.path.exists(p) for p in paths):
            return
        time.sleep(1)
    raise FileExistsError(f"Failed to delete paths: {paths}")


def wait_for_exists_glob(pattern):
    """Return path when path matching pattern has been created."""
    for _ in range(TIMEOUT):
        matches = glob.glob(pattern)
        if matches:
            if len(matches) > 1:
                raise FileExistsError(f"Found >1 directory: {matches}")
            path = pathlib.Path(matches[0])
            if path.exists():
                yield path
                return
        yield None
        time.sleep(1)
    raise FileNotFoundError(f"Failed to create path: {pattern}")


def wait_for_call_count(mock_function, num=1):
    """Return when a mock function has been called num times."""
    for _ in range(TIMEOUT):
        if mock_function.call_count == num:
            return
        yield
        time.sleep(1)
    raise AssertionError(f"Failed to call {mock_function} {num} times")


def wait_for_messages(function, mock_sendall, num=1):
    """Yield every 1s, return when function()==True on num messages."""
    for _ in range(TIMEOUT_LONG):
        messages = get_messages(mock_sendall)
        n_true_messages = sum(function(m) for m in messages)
        if n_true_messages == num:
            return
        yield
        time.sleep(1)
    raise AssertionError(f"Expected {num} messages, got {n_true_messages}.")


def wait_for_status_finished_messages(mock_sendall, num=1):
    """Return after num status finished messages."""
    return wait_for_messages(is_status_finished_message, mock_sendall, num)


def wait_for_register_messages(mock_sendall, num=1):
    """Return after num register messages."""
    return wait_for_messages(is_register_message, mock_sendall, num)


def wait_for_register_ack_messages(mock_sendall, num=1):
    """Return after num register_ack messages."""
    return wait_for_messages(is_register_ack_message, mock_sendall, num)


def wait_for_map_messages(mock_sendall, num=1):
    """Return after num map messages."""
    return wait_for_messages(is_map_message, mock_sendall, num)


def wait_for_reduce_messages(mock_sendall, num=1):
    """Return after num map messages."""
    return wait_for_messages(is_reduce_message, mock_sendall, num)


def send_message(message, port):
    """Send JSON-encoded TCP message."""
    host = "localhost"
    message_str = json.dumps(message, cls=PathJSONEncoder)
    message_bytes = str.encode(message_str)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.sendall(message_bytes)


def assert_no_prohibited_terms(*terms):
    """Check for prohibited terms before testing style."""
    for term in terms:
        completed_process = subprocess.run(
            [
                "grep",
                "-r",
                "-n",
                term,
                "--include=*.py",
                "--exclude=submit.py",
                "mapreduce"
            ],
            check=False,  # We'll check the return code manually
            stdout=subprocess.PIPE,
            text=True,
        )

        # Grep exit code should be non-zero, indicating that the prohibited
        # term was not found.  If the exit code is zero, crash and print a
        # helpful error message with a filename and line number.
        assert completed_process.returncode != 0, (
            f"The term '{term}' is prohibited.\n{completed_process.stdout}"
        )


def get_open_port(nports=1):
    """Return a port or list of ports available for use on localhost.

    This function has two limitations:

    1) It's not atomic. The ports it returns might not be open anymore
       by the time the Manager/Workers try to bind to them.
    2) A port that's open for TCP is not necessarily open for UDP.
    """
    ports = []
    with ExitStack() as stack:
        for _ in range(nports):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            stack.enter_context(sock)
            sock.bind(("", 0))
            port = sock.getsockname()[1]
            ports.append(port)
    return ports if len(ports) > 1 else ports[0]


def get_tmpdir_name(mock_tmpdir, index=0):
    """Return the name of a tmpdir using a spy.

    If index is 0, return the name of the first tmpdir created on the spy,
    and so on. This function assumes that mock_tmpdir is a mock object which is
    tracking calls to the constructor of tempfile.TemporaryDirectory. For a
    given call, args[0] will hold the TemporaryDirectory object initialized by
    the constructor.
    """
    assert mock_tmpdir.call_count > index, \
        "Not enough calls to tempfile.TemporaryDirectory()"

    assert len(mock_tmpdir.call_args_list[index].args) > 0, \
        "mock_tmpdir is not a mock for the TemporaryDirectory() constructor"

    return mock_tmpdir.call_args_list[index].args[0].name
