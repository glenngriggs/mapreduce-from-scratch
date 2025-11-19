"""MapReduce framework Worker node."""
from __future__ import annotations

import os
import logging
import json
import time
import threading
import subprocess
import tempfile
import shutil
import hashlib
import heapq
import contextlib
from pathlib import Path
from typing import Dict, Optional

import click
from mapreduce.utils import tcp_server, tcp_send, udp_send, PathJSONEncoder

# Configure logging
LOGGER = logging.getLogger(__name__)


def _pad5(n: int) -> str:
    return f"{n:05d}"


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd()
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host,
            manager_port
        )

        self.host = host
        self.port = int(port)
        self.manager_host = manager_host
        self.manager_port = int(manager_port)

        self.signals = {"stop": False}
        self._registered = threading.Event()
        self._task_lock = threading.Lock()

        # lifetime TCP listener
        tcp_thread = threading.Thread(
            target=tcp_server,
            args=(self.host, self.port, self.signals, self._handle_tcp),
            daemon=True
        )
        tcp_thread.start()

        # register
        tcp_send(
            self.manager_host,
            self.manager_port,
            {
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,
            },
        )

        # wait for register_ack OR shutdown
        while not (self._registered.is_set() or self.signals["stop"]):
            time.sleep(0.02)

        # start heartbeats only if registered (spec: after register_ack)
        hb_thread: Optional[threading.Thread] = None
        if self._registered.is_set() and not self.signals["stop"]:
            hb_thread = threading.Thread(
                target=self._heartbeats,
                daemon=True
            )
            hb_thread.start()

        # wait for shutdown
        while not self.signals["stop"]:
            time.sleep(0.2)

        # join threads
        tcp_thread.join(timeout=2)
        if hb_thread is not None:
            hb_thread.join(timeout=2)
        LOGGER.info("Worker shutting down")

    def stop(self):
        """Request this worker to stop."""
        self.signals["stop"] = True

    # ---------- TCP handler ----------

    def _handle_tcp(self, message: Dict):
        LOGGER.debug(
            "TCP recv\n%s",
            json.dumps(message, cls=PathJSONEncoder, indent=2)
        )
        mtype = message.get("message_type")
        if mtype == "register_ack":
            self._registered.set()
            return
        if mtype == "shutdown":
            self.signals["stop"] = True
            return
        if mtype == "new_map_task":
            with self._task_lock:
                self._run_map_task(message)
            return
        if mtype == "new_reduce_task":
            with self._task_lock:
                self._run_reduce_task(message)
            return
        # ignore invalid messages

    # ---------- Heartbeats ----------

    def _heartbeats(self):
        while not self.signals["stop"]:
            try:
                udp_send(
                    self.manager_host,
                    self.manager_port,
                    {
                        "message_type": "heartbeat",
                        "worker_host": self.host,
                        "worker_port": self.port,
                    },
                )
            except (ConnectionError, TimeoutError, OSError) as exc:
                # best effort
                LOGGER.debug("Heartbeat failed: %s", exc)
            time.sleep(2.0)

    # ---------- Map execution ----------

    def _run_map_task(self, msg: Dict):
        task_id = int(msg["task_id"])
        input_paths = [Path(p) for p in msg["input_paths"]]
        executable = Path(msg["executable"])
        shared_output = Path(msg["output_directory"])
        num_partitions = int(msg["num_partitions"])

        # local tmp dir
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{_pad5(task_id)}-"
        ) as tmpdir:
            tmpdir_p = Path(tmpdir)

            # lazily open partition files
            partition_files: Dict[int, Path] = {
                p: tmpdir_p / f"maptask{_pad5(task_id)}-part{_pad5(p)}"
                for p in range(num_partitions)
            }
            open_handles: Dict[int, object] = {}

            try:
                self._map_stream_inputs(
                    input_paths,
                    executable,
                    num_partitions,
                    partition_files,
                    open_handles,
                )

                self._map_finalize_partitions(
                    partition_files,
                    shared_output,
                )

            finally:
                # ensure handles closed on error
                for fh in list(open_handles.values()):
                    try:
                        fh.close()
                    except OSError as exc:
                        LOGGER.debug("Close failed: %s", exc)

        # notify manager
        tcp_send(
            self.manager_host,
            self.manager_port,
            {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port,
            },
        )

    def _map_stream_inputs(
        self,
        input_paths,
        executable,
        num_partitions,
        partition_files,
        open_handles,
    ):
        for inpath in input_paths:
            with inpath.open() as infile, subprocess.Popen(
                [str(executable)],
                stdin=infile,
                stdout=subprocess.PIPE,
                text=True,
            ) as proc:
                assert proc.stdout is not None
                for line in proc.stdout:
                    # only bind the piece we need to a name
                    key = line.partition("\t")[0]
                    hval = hashlib.md5(key.encode("utf-8")).hexdigest()
                    part = int(hval, 16) % num_partitions
                    if part not in open_handles:
                        open_handles[part] = partition_files[part].open(
                            "w",
                            encoding="utf-8",
                        )
                    open_handles[part].write(line)

        for fh in open_handles.values():
            fh.close()

    def _map_finalize_partitions(self, partition_files, shared_output):
        for _, fpath in partition_files.items():  # was "for p, fpath in ..."
            if fpath.exists():
                subprocess.run(
                    ["sort", "-o", str(fpath), str(fpath)],
                    check=True,
                )
                dest = shared_output / fpath.name
                shutil.move(str(fpath), str(dest))
                LOGGER.info("Moved %s -> %s", fpath, dest)

    # ---------- Reduce execution ----------

    def _run_reduce_task(self, msg: Dict):
        input_paths = [Path(p) for p in msg["input_paths"]]
        executable = Path(msg["executable"])
        final_output_dir = Path(msg["output_directory"])

        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{_pad5(int(msg['task_id']))}-"
        ) as tmpdir:
            tmpdir_p = Path(tmpdir)
            out_file = tmpdir_p / f"part-{_pad5(int(msg['task_id']))}"

            self._reduce_merge_and_run(
                input_paths,
                executable,
                out_file,
            )

            # move to final output dir
            final_output_dir.mkdir(parents=True, exist_ok=True)
            dest = final_output_dir / out_file.name
            shutil.move(str(out_file), str(dest))
            LOGGER.info("Moved %s -> %s", out_file, dest)

        # notify manager
        tcp_send(
            self.manager_host,
            self.manager_port,
            {
                "message_type": "finished",
                "task_id": int(msg["task_id"]),
                "worker_host": self.host,
                "worker_port": self.port,
            },
        )

    def _reduce_merge_and_run(self, input_paths, executable, out_file):
        with contextlib.ExitStack() as stack:
            files = [
                stack.enter_context(p.open(encoding="utf-8"))
                for p in input_paths
            ]
            merged_iter = heapq.merge(*files)

            with out_file.open("w", encoding="utf-8") as outfile, (
                subprocess.Popen(
                    [str(executable)],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                )
            ) as proc:
                assert proc.stdin is not None
                for line in merged_iter:
                    proc.stdin.write(line)
                proc.stdin.close()
                proc.wait()


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
