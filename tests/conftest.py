"""Shared test fixtures.

Pytest fixture docs:
https://docs.pytest.org/en/latest/fixture.html#conftest-py-sharing-fixture-functions
"""

import collections
import contextlib
import logging
import shutil
import socket
import subprocess
import time
import pytest
import utils


# Set up logging
LOGGER = logging.getLogger("autograder")

# How long to wait for server in separate process to start or stop
TIMEOUT = 5

# Number of Workers which will run in separate processes
N_WORKERS = 3

# The mapreduce_client fixture will return a MapReduceClient object
MapReduceClient = collections.namedtuple("MapReduceClient", [
    "manager_host",
    "manager_port",
])


@pytest.fixture(name='mapreduce_client')
def setup_teardown_mapreduce_client():
    """Start a MapReduce Manager and Worker Servers in separate processes."""
    LOGGER.info("Setup test fixture 'mapreduce_client'")

    # Acquire open ports
    manager_port, *worker_ports = \
        utils.get_open_port(nports=1 + N_WORKERS)

    # Match the server log level to the current log level.  For that we
    # need to access a protected member of the logging library.
    # pylint: disable=protected-access
    loglevel = logging._levelToName[logging.root.level]

    # Each server is a LiveServer object whose lifetime is automatically
    # managed by a context manager
    with contextlib.ExitStack() as stack:
        processes = []

        # Start Manager
        LOGGER.info("Starting Manager")
        process = stack.enter_context(subprocess.Popen([
            shutil.which("mapreduce-manager"),
            "--port", str(manager_port),
            "--loglevel", loglevel,
        ]))
        processes.append(process)
        wait_for_server_ready(process, manager_port)

        # Start Workers
        for worker_port in worker_ports:
            LOGGER.info("Starting Worker")
            process = stack.enter_context(subprocess.Popen([
                shutil.which("mapreduce-worker"),
                "--port", str(worker_port),
                "--manager-port", str(manager_port),
                "--loglevel", loglevel,
            ]))
            processes.append(process)
            wait_for_server_ready(process, worker_port)

        # Transfer control to test.  The code before the "yield" statement is
        # setup code, which is executed before the test.  Code after the
        # "yield" is teardown code, which is executed at the end of the test.
        # Teardown code is executed whether the test passed or failed.
        yield MapReduceClient("localhost", manager_port)

        # Send shutdown message
        utils.send_message({
            "message_type": "shutdown"
        }, port=manager_port)

        # Wait for processes to stop
        wait_for_process_all_stopped(processes)

        # Check for clean exit
        for process in processes:
            assert process.returncode == 0, \
                f"{process} returncode={process.returncode}"

        # Kill servers
        LOGGER.info("Teardown test fixture 'mapreduce_client'")
        for process in processes:
            process.terminate()


def wait_for_server_ready(process, port):
    """Wait for server to respond, raise exception on timeout."""
    for _ in range(10*TIMEOUT):
        if port_is_open("localhost", port):
            return
        if process.poll() is not None:
            raise ChildProcessError(f"Premature exit: {process}")
        time.sleep(0.1)
    raise ChildProcessError(f"Failed to start: {process}")


def port_is_open(host, port):
    """Return True if host:port is open."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
        except ConnectionRefusedError:
            return False
        return True


def wait_for_process_all_stopped(processes):
    """Return after all processes are not alive."""
    for _ in range(10*TIMEOUT):
        if all(p.poll() is not None for p in processes):
            return
        time.sleep(0.1)
    raise ChildProcessError("Some processes failed to stop.")
