"""Example TCP socket client."""
import socket
import json


def main():
    """Test TCP Socket Client."""
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        # connect to the server
        sock.connect(("localhost", 8000))

        # send a message
        message = json.dumps({"hello": "world"})
        sock.sendall(message.encode('utf-8'))


if __name__ == "__main__":
    main()
