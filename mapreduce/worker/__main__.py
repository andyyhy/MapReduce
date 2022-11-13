"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import threading
import subprocess
import hashlib
import tempfile
import pathlib
import heapq
import shutil
from contextlib import ExitStack
import click
from mapreduce.utils.utils import start_tcp_listen
from mapreduce.utils.utils import send_tcp, send_udp

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host,
            manager_port,
        )
        # member variables
        self.shutdown = False
        self.registered = False
        self.working = False
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.host = host
        self.port = port

        # Start thread for TCP listen
        tcp_thread = threading.Thread(
            target=start_tcp_listen,
            args=(self, host, port, "worker", ),
        )
        tcp_thread.start()

        self.process_heartbeat()

        tcp_thread.join()

    def shut_down(self):
        """Handle Shutdown."""
        LOGGER.info("Worker busy?: %s", self.working)
        while self.working:
            time.sleep(0.1)
            LOGGER.info("Worker busy: %s Port: %s", self.host, self.port)
        LOGGER.info("Worker shutting down Host: %s Port: %s",
                    self.host, self.port)
        self.shutdown = True

    def process_heartbeat(self):
        """Send heartbeat."""
        while True:
            if self.registered:
                message = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port
                }
                send_udp(
                    json.dumps(message).encode("utf-8"),
                    self.manager_host,
                    self.manager_port
                )
                time.sleep(2)
            if self.shutdown:
                break
            if not self.registered:
                time.sleep(0.1)
                continue

    def process_message_w(self, message):
        """Process messages."""
        if message["message_type"] == "register_ack":
            self.registered = True
        if message["message_type"] == "new_map_task":
            self.working = True
            self.process_map_task(message)
        if message["message_type"] == "new_reduce_task":
            self.working = True
            self.process_reduce_task(message)

    def create_output_paths(self, num_partitions, task_id, tmpdir):
        """Create output paths."""
        paths = []
        for partition in range(num_partitions):
            temp_file_path = os.path.join(
                tmpdir,
                f"maptask{task_id:05d}-part{partition:05d}"
            )
            paths.append(temp_file_path)
        return paths

    def map_helper(self, line, message, temp_files):
        """Write to temp files."""
        key, value = line.split("\t")
        hexdigest = hashlib.md5(
                    key.encode("utf-8")
                    ).hexdigest()
        keyhash = int(hexdigest, base=16)
        partition = keyhash % message["num_partitions"]
        temp_files[partition].write(key + "\t" + value)

    def process_map_task(self, message):
        """Process Map Task."""
        # Create temp directory
        task_id = message["task_id"]
        temp_files = []
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as tmpdir:
            tmp_file_paths = self.create_output_paths(
                message["num_partitions"],
                task_id,
                tmpdir
            )
            with ExitStack() as stack:
                # Open all intermediate files
                for file_path in tmp_file_paths:
                    temp_files.append(
                        stack.enter_context(
                            open(
                                file_path,
                                "a+",
                                encoding="utf-8"
                            )
                        )
                    )
                # Run the map executable and pipe the output to memory
                for input_path in message["input_paths"]:
                    with open(input_path, encoding="utf-8") as infile:
                        with subprocess.Popen(
                            [message["executable"]],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as map_process:
                            for line in map_process.stdout:
                                # Add line to correct partition output file
                                self.map_helper(line, message, temp_files)
            # Sort each file
            for file_path in pathlib.Path(tmpdir).iterdir():
                temp_list = []
                with open(file_path, 'r', encoding="utf-8") as file:
                    for item in file:
                        heapq.heappush(temp_list, item)
                with open(file_path, 'w', encoding="utf-8") as file:
                    while len(temp_list) > 0:
                        file.write(heapq.heappop(temp_list))
                shutil.copy(
                    file_path,
                    pathlib.Path(message["output_directory"])
                )
        send_tcp(
            json.dumps({
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port,
             }).encode("utf-8"),
            self.manager_host, self.manager_port
        )
        self.working = False

    def process_reduce_task(self, message):
        """Process Reduce Task."""
        task_id = message["task_id"]
        prefix = f"mapreduce-local-task{task_id:05d}-"
        output_file_name = f"part-{task_id:05d}"
        # Put all input in list
        with ExitStack() as stack:
            # Put all input files in list
            input_list = []
            for input_path in message["input_paths"]:
                input_list.append(
                    stack.enter_context(open(input_path, encoding="utf-8"))
                )
            # Create temp directory
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                executable = message["executable"]
                # Create temp output path
                temp_output_file_path = os.path.join(tmpdir, output_file_name)
                with open(
                    temp_output_file_path, "w", encoding="utf-8"
                ) as outfile:
                    with subprocess.Popen(
                        [executable],
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                        text=True,
                    ) as reduce_process:
                        # Merge files
                        for line in heapq.merge(*input_list):
                            reduce_process.stdin.write(line)
                shutil.copy(
                    temp_output_file_path,
                    pathlib.Path(message["output_directory"])
                )
        message = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        }
        send_tcp(
            json.dumps(message).encode("utf-8"),
            self.manager_host, self.manager_port
        )
        self.working = False


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
