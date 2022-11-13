"""MapReduce framework Manager node."""
import os
import threading
import tempfile
import logging
import json
import time
import shutil
from socket import error as socket_error
import pathlib
import click
from mapreduce.utils.utils import start_tcp_listen, start_udp_listen, send_tcp


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )
        self.job_counter = 0
        self.workers = {}
        self.workers["ready"] = []
        self.workers["busy"] = []
        self.workers["dead"] = []
        self.shutdown = False
        self.task_to_do = 0
        self.task_queue = []
        self.job_active = {}
        self.jobs_waiting = []

        # Start thread for TCP listen
        tcp_thread = threading.Thread(
            target=start_tcp_listen,
            args=(self, host, port, "manager", ),
        )
        tcp_thread.start()

        # Start thread for UDP listen
        udp_thread = threading.Thread(
            target=start_udp_listen,
            args=(self, host, port, ),
        )
        udp_thread.start()

        # Process Job
        self.process_job()

        tcp_thread.join()
        udp_thread.join()

    def process_tcp_error(self, host, port):
        """Process tcp error."""
        for worker in self.workers["ready"]:
            if worker["worker_host"] == host and worker["worker_port"] == port:
                self.workers["dead"].append(worker)
                self.workers["ready"].remove(worker)
        for worker in self.workers["busy"]:
            if worker["worker_host"] == host and worker["worker_port"] == port:
                self.task_queue.insert(0, worker["task"])
                self.workers["dead"].append(worker)
                self.workers["busy"].remove(worker)

    def shut_down(self):
        """Shut down."""
        for worker in self.workers["ready"]:
            try:
                send_tcp(
                    json.dumps({"message_type": "shutdown"}).encode("utf-8"),
                    worker["worker_host"],
                    worker["worker_port"],
                )
            except socket_error:
                self.process_tcp_error(worker["host"], worker["port"])
        for worker in self.workers["busy"]:
            try:
                send_tcp(
                    json.dumps({"message_type": "shutdown"}).encode("utf-8"),
                    worker["worker_host"],
                    worker["worker_port"],
                )
            except socket_error:
                self.process_tcp_error(worker["host"], worker["port"])
        self.shutdown = True

    def register_worker(self, worker_host, worker_port):
        """Register worker."""
        # Add worker to the ready list
        worker = {
            "worker_host": worker_host,
            "worker_port": worker_port,
            "heartbeat": 0,
            "task": {},
        }
        self.workers["ready"].append(worker)
        # Send Ack message to worker
        try:
            send_tcp(
                json.dumps(
                    {
                        "message_type": "register_ack",
                        "worker_host": worker_host,
                        "worker_port": worker_port,
                    }
                ).encode("utf-8"),
                worker["worker_host"],
                worker["worker_port"],
            )
        except socket_error:
            self.process_tcp_error(worker["host"], worker["port"])

        LOGGER.info(
            "Sent Ack to worker %s %s",
            worker["worker_host"], worker["worker_port"]
        )

    def process_heartbeat(self, message):
        """Process heartbeat."""
        if message["message_type"] == "heartbeat":
            for worker in self.workers["ready"]:
                if (
                    worker["worker_host"] == message["worker_host"]
                    and worker["worker_port"] == message["worker_port"]
                ):
                    worker["heartbeat"] = 0
            for worker in self.workers["busy"]:
                if (
                    worker["worker_host"] == message["worker_host"]
                    and worker["worker_port"] == message["worker_port"]
                ):
                    worker["heartbeat"] = 0
        for worker in self.workers["ready"]:
            worker["heartbeat"] += 1
            if worker["heartbeat"] >= 5:
                self.workers["dead"].append(worker)
                self.workers["ready"].remove(worker)
        for worker in self.workers["busy"]:
            worker["heartbeat"] += 1
            if worker["heartbeat"] >= 5:
                self.task_queue.append(worker["task"])
                self.workers["dead"].append(worker)
                self.workers["busy"].remove(worker)

    def process_message_m(self, message):
        """Process message."""
        LOGGER.info("Processing Message: \n%s", json.dumps(message, indent=2))
        if message["message_type"] == "register":
            self.register_worker(message["worker_host"],
                                 message["worker_port"])
        if message["message_type"] == "new_manager_job":
            self.add_job(message)
        if message["message_type"] == "finished":
            self.reset_worker(message)
            self.task_to_do -= 1

    def add_job(self, message):
        """Add job."""
        job = message
        job["job_id"] = self.job_counter
        job["stage"] = "mapping"
        self.job_counter += 1

        # Check if the output directory exits
        if pathlib.Path(job["output_directory"]).is_dir():
            shutil.rmtree(
                pathlib.Path(job["output_directory"]),
                ignore_errors=False, onerror=None
            )
        pathlib.Path(job["output_directory"]).mkdir(parents=True,
                                                    exist_ok=True)

        # Add Job to the job queue
        self.jobs_waiting.append(job)

        LOGGER.info("Job Added \n%s", json.dumps(job, indent=2))

    def input_partitioning_map(self, input_dir, num_mappers):
        """Input partition for map."""
        list_files = os.listdir(input_dir)
        list_files.sort()
        partitions = []
        for i in range(num_mappers):
            partition_list = []
            for j in range(i, len(list_files), num_mappers):
                partition_list.append(input_dir + "/" + list_files[j])
            partitions.append(partition_list)
        return partitions

    def input_partitioning_reduce(self, input_dir, num_reducers):
        """Input partition for reduce."""
        list_files = os.listdir(input_dir)
        list_files.sort()
        LOGGER.info("reduce partition files: %s", list_files)
        partitions = []
        for i in range(num_reducers):
            partition_list = []
            for j in list_files:
                if i == int(j[-5:]):
                    partition_list.append(os.path.join(input_dir, j))

            partitions.append(partition_list)
        return partitions

    def reset_worker(self, message):
        """Reset worker."""
        new_busy = []
        for worker in self.workers["busy"]:
            if (
                worker["worker_host"] == message["worker_host"]
                and worker["worker_port"] == message["worker_port"]
            ):
                worker["heartbeat"] = 0
                worker["task"] = {}
                self.workers["ready"].append(worker)
            else:
                new_busy.append(worker)
        self.workers["busy"] = new_busy

    def handle_mapping_task(self, tmpdir):
        """Handle mapping task."""
        # Partition input
        input_partitions = self.input_partitioning_map(
            self.job_active["input_directory"], self.job_active["num_mappers"]
        )
        counter = 0
        self.task_to_do = len(input_partitions)
        for partition in input_partitions:
            message = {
                "message_type": "new_map_task",
                "task_id": counter,
                "input_paths": partition,
                "executable": self.job_active["mapper_executable"],
                "output_directory": tmpdir,
                "num_partitions": self.job_active["num_reducers"],
            }
            self.task_queue.append(message)
            counter += 1
        while (self.task_queue or self.task_to_do) and not self.shutdown:
            if self.task_queue:
                sucessful = False
                while not sucessful and not self.shutdown:
                    LOGGER.info("workers %s", self.workers["ready"])
                    # Check if there are workers available
                    if not self.workers["ready"]:
                        time.sleep(0.1)
                        continue
                    # pop task off the queue
                    task = self.task_queue.pop(0)
                    # pop worker off ready queue
                    # and add to busy queue and assign work
                    worker = self.workers["ready"].pop(0)
                    task["worker_host"] = worker["worker_host"]
                    task["worker_port"] = worker["worker_port"]
                    worker["task"] = task
                    self.workers["busy"].append(worker)
                    try:
                        send_tcp(
                            json.dumps(task).encode("utf-8"),
                            worker["worker_host"],
                            worker["worker_port"],
                        )
                    except socket_error:
                        self.process_tcp_error(worker["host"], worker["port"])
                    sucessful = True
                    LOGGER.info(
                        "New mapper task sent to worker %s %s %s %s",
                        worker["worker_host"],
                        worker["worker_port"],
                        counter,
                        len(input_partitions),
                    )
            else:
                time.sleep(0.1)
                LOGGER.info("waiting for task to finish")
                continue
        self.job_active["stage"] = "reducing"

    def handle_reducing_task(self, tmpdir):
        """Handle reducing task."""
        input_partitions = self.input_partitioning_reduce(
            tmpdir, self.job_active["num_reducers"]
        )
        counter = 0
        self.task_to_do = len(input_partitions)
        for partition in input_partitions:
            message = {
                "message_type": "new_reduce_task",
                "task_id": counter,
                "input_paths": partition,
                "executable": self.job_active["reducer_executable"],
                "output_directory": self.job_active["output_directory"],
            }
            self.task_queue.append(message)
            counter += 1
        while (self.task_queue or self.task_to_do) and not self.shutdown:
            if self.task_queue:
                sucessful = False
                while not sucessful and not self.shutdown:
                    # Check if there are workers available
                    if not self.workers["ready"]:
                        time.sleep(0.1)
                        continue
                    # pop task off the queue
                    task = self.task_queue.pop(0)
                    # pop worker off ready queue and
                    # add to busy queue and assign work
                    worker = self.workers["ready"].pop(0)
                    task["worker_host"] = worker["worker_host"]
                    task["worker_port"] = worker["worker_port"]
                    worker["task"] = task
                    self.workers["busy"].append(worker)
                    try:
                        send_tcp(
                            json.dumps(task).encode("utf-8"),
                            worker["worker_host"],
                            worker["worker_port"],
                        )
                    except socket_error:
                        self.process_tcp_error(worker["host"], worker["port"])
                    sucessful = True
                    LOGGER.info(
                        "New reducer task sent to worker %s %s %s %s",
                        worker["worker_host"],
                        worker["worker_port"],
                        counter,
                        len(input_partitions),
                    )
            else:
                time.sleep(0.1)
                LOGGER.info("waiting for task to finish")
                continue
        self.job_active.clear()

    def process_job(self):
        """Process job."""
        while not self.shutdown:
            LOGGER.info("Starting Job")
            time.sleep(0.5)
            # If no jobs to do then keep looping
            if not self.job_active and not self.jobs_waiting:
                continue
            # If no jobs active but jobs in queue
            if not self.job_active:
                self.job_active = self.jobs_waiting.pop(0)

            active_job_id = self.job_active["job_id"]

            # Create temp directory
            prefix = f"mapreduce-shared-job{active_job_id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                time.sleep(0.1)
                while not self.shutdown and self.job_active:
                    if self.job_active["stage"] == "mapping":
                        LOGGER.info("Start mapping task")
                        self.handle_mapping_task(tmpdir)
                    if self.job_active["stage"] == "reducing":
                        LOGGER.info("Start reducing task")
                        self.handle_reducing_task(tmpdir)

            LOGGER.info("Cleaned up tmpdir %s", tmpdir)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
        )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
