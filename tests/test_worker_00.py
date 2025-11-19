"""See unit test function docstring."""

import json
import socket
import threading
import mapreduce


def test_shutdown(mocker):
    """Verify Worker shuts down.

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

    # recv() returns a sequence of hardcoded values.  When the student Worker
    # calls recv(), it will receive a shutdown message.
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = [
        json.dumps({"message_type": "shutdown"}).encode("utf-8"),
        None,  # None value terminates while recv loop
    ]

    # Run student Worker code.  When student Worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
        )
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify that the student code called the correct socket functions with
    # the correct arguments.
    #
    # NOTE: to see a list of all calls
    # >>> print(mock_socket.mock_calls)
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the Worker uses
        # to receive status update JSON messages from the Manager.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(("localhost", 6001)),
        mocker.call().__enter__().listen(),
    ], any_order=True)
