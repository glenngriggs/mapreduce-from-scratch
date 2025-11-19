"""Example UDP socket client."""
import socket
import json


def main():
    """Test UDP Socket Client."""
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Connect to the UDP socket on server
        sock.connect(("localhost", 8001))

        # Send a message
        message = json.dumps({"hello": "world"})
        sock.sendall(message.encode('utf-8'))


if __name__ == "__main__":
    main()
