"""Utilities for MapReduce framework."""
import logging
import json
import socket


# Configure logging
LOGGER = logging.getLogger(__name__)


def start_tcp_listen(self, host, port, position):
    """Start tcp listen."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        # Socket accept() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        sock.settimeout(1)
        while not self.shutdown:
            # register worker if not yet registered
            if position == "worker" and not self.registered:
                send_tcp(
                    json.dumps(
                        {
                            "message_type": "register",
                            "worker_host": self.host,
                            "worker_port": self.port,
                        }
                    ).encode("utf-8"),
                    self.manager_host,
                    self.manager_port,
                )
            try:
                clientsocket, address = sock.accept()
                LOGGER.info("Connectfrom %s", address[0])
            except socket.timeout:
                continue
            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)
            # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)
            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b"".join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            LOGGER.info("TCP recv\n%s", json.dumps(message_dict, indent=2))
            # If the message is shutdown
            if message_dict["message_type"] == "shutdown":
                self.shut_down()
            else:
                if position == "manager":
                    self.process_message_m(message_dict)
                else:
                    self.process_message_w(message_dict)


def start_udp_listen(self, host, port):
    """Start udp listen."""
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(2)
        # No sock.listen() since UDP doesn't establish connections like TCP
        # Receive incoming UDP messages
        while not self.shutdown:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            LOGGER.info("UDP recv\n%s", json.dumps(message_dict, indent=2))
            self.process_heartbeat(message_dict)


def send_tcp(message, host, port):
    """Send tcp."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # connect to the server
        sock.connect((host, port))
        # send a message
        sock.sendall(message)


def send_udp(message, host, port):
    """Send udp."""
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Connect to the UDP socket on server
        sock.connect((host, port))
        # Send a message
        sock.sendall(message)
