"""See unit test function docstring."""

import socket
import json
import threading
import mapreduce
import utils


def test_shutdown(mocker):
    """Verify Manager shuts down.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.
    """
    # Mock the socket library socket class
    mock_socket = mocker.patch("socket.socket")

    # accept() returns a mock client socket
    mock_clientsocket = mocker.MagicMock()
    mock_accept = mock_socket.return_value.__enter__.return_value.accept
    mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))

    # TCP recv() returns a sequence of hardcoded values
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = [
        json.dumps({"message_type": "shutdown"}).encode("utf-8"),
        None
    ]

    # UDP recv() returns heartbeat messages. There are no Workers in this
    # test, so the heartbeats are from an unregistered Worker. We only do this
    # so that when the student Manager calls recv() on a UDP server, the
    # response will be a real heartbeat message instead of an invalid object.
    # If heartbeats aren't implemented yet, this will be silently ignored.
    mock_udp_recv = mock_socket.return_value.__enter__.return_value.recv
    mock_udp_recv.side_effect = utils.worker_heartbeat_generator(
        None,
        3001,
    )

    # Run student Manager code.  When student Manager calls recv(), it will
    # receive the faked responses configured above.  When the student code
    # calls sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    # The Manager may have several threads, so we make sure that they have all
    # been stopped.
    try:
        mapreduce.manager.Manager("localhost", 6000)
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify that the student code called the correct socket functions with
    # the correct arguments.
    #
    # NOTE: to see a list of all calls
    # >>> print(mock_socket.mock_calls)
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the Manager uses
        # to receive JSON formatted commands from mapreduce-submit.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(("localhost", 6000)),
        mocker.call().__enter__().listen(),
    ], any_order=True)


def test_shutdown_workers(mocker):
    """Verify Manager shuts down and tells Workers to shut down.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.
    """
    # Mock the socket library socket class
    mock_socket = mocker.patch("socket.socket")

    # sendall() records messages
    mock_sendall = mock_socket.return_value.__enter__.return_value.sendall

    # accept() returns a mock client socket
    mock_clientsocket = mocker.MagicMock()
    mock_accept = mock_socket.return_value.__enter__.return_value.accept
    mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))

    # TCP recv() returns a sequence of hardcoded values
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = [
        # First fake Worker registers with Manager
        json.dumps({
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3001,
        }).encode("utf-8"),
        None,

        # Second fake Worker registers with Manager
        json.dumps({
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3002,
        }).encode("utf-8"),
        None,

        # Fake shutdown message sent to Manager
        json.dumps({
            "message_type": "shutdown",
        }).encode("utf-8"),
        None,
    ]

    # UDP recv() returns heartbeat messages
    mock_udp_recv = mock_socket.return_value.__enter__.return_value.recv
    mock_udp_recv.side_effect = utils.worker_heartbeat_generator(
        mock_sendall,
        3001,
    )

    # Mock the os library kill() function so we can verify it isn't used
    mock_oskill = mocker.patch("os.kill")

    # Run student Manager code.  When student Manager calls recv(), it will
    # return the faked responses configured above.
    try:
        mapreduce.manager.Manager("localhost", 6000)
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify student code did not call os.kill(), which is prohibited.
    # Instead, solutions should send a TCP message to the Worker, telling it to
    # shut down.
    assert not mock_oskill.called, "os.kill() is prohibited"

    # Verify that the student code called the correct socket functions with
    # the correct arguments.
    #
    # NOTE: to see a list of all calls
    # >>> print(mock_socket.mock_calls)
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the Manager uses
        # to receive status update JSON messages from the Manager.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(("localhost", 6000)),
        mocker.call().__enter__().listen(),

        # Manager should have sent two shutdown messages, one to each Worker
        mocker.call().__enter__().sendall(json.dumps({
            "message_type": "shutdown",
        }).encode("utf-8")),
        mocker.call().__enter__().sendall(json.dumps({
            "message_type": "shutdown",
        }).encode("utf-8")),
    ], any_order=True)
