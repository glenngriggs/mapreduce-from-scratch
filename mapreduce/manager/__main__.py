"""MapReduce framework Manager node."""
from __future__ import annotations

import os
import tempfile
import logging
import json
import time
import threading
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Deque
from collections import deque
from pathlib import Path
import shutil

import click
from mapreduce.utils import (
    tcp_server,
    tcp_send,
    udp_server,
    ThreadSafeOrderedDict,
    PathJSONEncoder,
)

# Configure logging
LOGGER = logging.getLogger(__name__)

# ---------- Data classes ----------


@dataclass
class MapTask:
    """Represent a single map task to send to a worker."""

    task_id: int
    input_paths: List[Path]
    executable: Path
    output_directory: Path  # manager shared job dir
    num_partitions: int

    def to_message(self) -> Dict:
        """Convert this map task to a TCP message payload."""
        return {
            "message_type": "new_map_task",
            "task_id": self.task_id,
            "input_paths": self.input_paths,
            "executable": self.executable,
            "output_directory": self.output_directory,
            "num_partitions": self.num_partitions,
        }


@dataclass
class ReduceTask:
    """Represent a single reduce task to send to a worker."""

    task_id: int
    input_paths: List[Path]
    executable: Path
    output_directory: Path  # final output dir

    def to_message(self) -> Dict:
        """Convert this reduce task to a TCP message payload."""
        return {
            "message_type": "new_reduce_task",
            "task_id": self.task_id,
            "input_paths": self.input_paths,
            "executable": self.executable,
            "output_directory": self.output_directory,
        }


@dataclass
class MapPhaseState:
    """State for the map phase of a job."""

    pending: Deque[MapTask] = field(default_factory=deque)
    inflight: Dict[int, str] = field(default_factory=dict)
    done: set[int] = field(default_factory=set)
    tasks_by_id: Dict[int, MapTask] = field(default_factory=dict)

    def has_work(self) -> bool:
        """Return True if there is map work not yet completed."""
        return bool(self.pending or self.inflight)


@dataclass
class ReducePhaseState:
    """State for the reduce phase of a job."""

    pending: Deque[ReduceTask] = field(default_factory=deque)
    inflight: Dict[int, str] = field(default_factory=dict)
    done: set[int] = field(default_factory=set)
    tasks_by_id: Dict[int, ReduceTask] = field(default_factory=dict)

    def has_work(self) -> bool:
        """Return True if there is reduce work not yet completed."""
        return bool(self.pending or self.inflight)


@dataclass
class JobRuntime:
    """Runtime / mutable state for a job, separated to reduce attributes."""

    phase: str = "pending"  # pending|map|reduce|done
    job_tmp_dir: Optional[Path] = None
    map_state: MapPhaseState = field(default_factory=MapPhaseState)
    reduce_state: ReducePhaseState = field(default_factory=ReducePhaseState)

    def phase_is(self, name: str) -> bool:
        """Return True if the current phase matches name."""
        return self.phase == name


@dataclass
class JobConfig:
    """Static configuration for a job."""

    input_directory: Path
    output_directory: Path
    mapper_executable: Path
    reducer_executable: Path
    num_mappers: int
    num_reducers: int

    def output_dir_path(self) -> Path:
        """Return the output directory path."""
        return self.output_directory


@dataclass
class Job:
    """Represent one MapReduce job and its current phase."""

    job_id: int
    config: JobConfig
    runtime: JobRuntime = field(default_factory=JobRuntime)

    def is_done(self) -> bool:
        """Return True if this job reached the 'done' phase."""
        return self.runtime.phase == "done"


@dataclass
class JobState:
    """Hold job queue and current job for the manager."""

    queue: Deque[Job] = field(default_factory=deque)
    current: Optional[Job] = None
    counter: int = 0

    def has_current(self) -> bool:
        """Return True if a job is currently running."""
        return self.current is not None


@dataclass
class WorkerState:
    """Hold registered workers and their registration order."""

    table: ThreadSafeOrderedDict[str, Dict] = field(
        default_factory=ThreadSafeOrderedDict
    )
    order: List[str] = field(default_factory=list)

    def get(self, wid: str) -> Optional[Dict]:
        """Return worker info for the given worker id."""
        return self.table.get(wid)

    def has_worker(self, wid: str) -> bool:
        """Return True if worker id exists."""
        return wid in self.table


@dataclass
class ManagerRuntime:
    """Bundle runtime structures to reduce instance attributes."""

    workers: WorkerState
    jobs: JobState
    cv: threading.Condition

    def notify_all(self) -> None:
        """Wake any threads waiting on the condition variable."""
        with self.cv:
            self.cv.notify_all()


# ---------- Manager ----------

@dataclass
class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, shared_dir=None):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )

        port = int(port)
        self.signals = {"stop": False}
        self._shutdown_forwarded = False

        runtime = ManagerRuntime(
            workers=WorkerState(),
            jobs=JobState(),
            cv=threading.Condition(),
        )
        self.runtime = runtime

        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(
            prefix=prefix, dir=shared_dir or None
        ) as tmpdir:
            self.shared_root = Path(tmpdir)
            LOGGER.info("Listening on UDP port %s", port)
            LOGGER.info("Listening on TCP port %s", port)
            LOGGER.info("Created tmpdir %s", self.shared_root)

            tcp_thread = threading.Thread(
                target=tcp_server,
                args=(host, port, self.signals, self._handle_tcp),
                daemon=True,
            )
            udp_thread = threading.Thread(
                target=udp_server,
                args=(host, port, self.signals, self._handle_udp),
                daemon=True,
            )
            liveness_thread = threading.Thread(
                target=self._liveness_monitor,
                daemon=True,
            )
            scheduler_thread = threading.Thread(
                target=self._scheduler,
                daemon=True,
            )

            tcp_thread.start()
            udp_thread.start()
            liveness_thread.start()
            scheduler_thread.start()

            while not self.signals["stop"]:
                time.sleep(0.2)

            # Join threads
            for th, name in (
                (tcp_thread, "tcp_thread"),
                (udp_thread, "udp_thread"),
                (liveness_thread, "liveness_thread"),
                (scheduler_thread, "scheduler_thread"),
            ):
                try:
                    th.join()
                except RuntimeError:
                    LOGGER.exception("Failed to join %s", name)

            try:
                self._forward_shutdown_to_workers()
            except (ConnectionError, TimeoutError, OSError):
                LOGGER.debug(
                    "Could not forward shutdown to %s:%s", host, port
                )

            LOGGER.info("Cleaned up tmpdir %s", self.shared_root)

    # ---------- TCP handlers ----------

    def _handle_tcp(self, message: Dict) -> None:
        """Handle an incoming TCP message."""
        LOGGER.debug(
            "received\n%s", json.dumps(message, cls=PathJSONEncoder, indent=2)
        )
        mtype = message.get("message_type")
        if mtype == "shutdown":
            self._on_shutdown()
        elif mtype == "register":
            self._on_register(message)
        elif mtype == "new_manager_job":
            self._on_new_job(message)
        elif mtype == "finished":
            self._on_finished(message)
        # else: ignore invalid messages per spec

    def _on_shutdown(self) -> None:
        """Handle shutdown request from client."""
        LOGGER.info("shutting down")
        self._forward_shutdown_to_workers()
        self.signals["stop"] = True
        self.runtime.notify_all()

    def _on_register(self, msg: Dict) -> None:
        """Handle a worker registration message."""
        host = msg["worker_host"]
        port = int(msg["worker_port"])
        wid = f"{host}:{port}"
        rt = self.runtime
        with rt.cv:
            if wid not in rt.workers.table:
                rt.workers.order.append(wid)
            rt.workers.table[wid] = {
                "host": host,
                "port": port,
                "state": "ready",
                "last_ts": time.time(),
                "missed": 0,
                "assigned": None,
            }
            try:
                tcp_send(host, port, {"message_type": "register_ack"})
            except ConnectionRefusedError:
                rt.workers.table[wid]["state"] = "dead"
            rt.cv.notify_all()

    def _on_new_job(self, msg: Dict) -> None:
        """Handle a new job submission."""
        rt = self.runtime
        js = rt.jobs
        config = JobConfig(
            input_directory=Path(msg["input_directory"]),
            output_directory=Path(msg["output_directory"]),
            mapper_executable=Path(msg["mapper_executable"]),
            reducer_executable=Path(msg["reducer_executable"]),
            num_mappers=int(msg["num_mappers"]),
            num_reducers=int(msg["num_reducers"]),
        )
        job = Job(job_id=js.counter, config=config)
        js.counter += 1
        with rt.cv:
            if config.output_directory.exists():
                shutil.rmtree(config.output_directory)
            config.output_directory.mkdir(parents=True, exist_ok=True)
            job.runtime.job_tmp_dir = (
                self.shared_root / f"job-{job.job_id:05d}"
            )
            job.runtime.job_tmp_dir.mkdir(parents=True, exist_ok=True)
            js.queue.append(job)
            rt.cv.notify_all()
        LOGGER.info("Created %s", config.output_directory)
        LOGGER.info("Created %s", job.runtime.job_tmp_dir)

    def _on_finished(self, msg: Dict) -> None:
        """Handle notification from a worker that a task is finished."""
        task_id = int(msg["task_id"])
        wid = f"{msg['worker_host']}:{int(msg['worker_port'])}"
        rt = self.runtime
        with rt.cv:
            w = rt.workers.table.get(wid)
            if w:
                w["state"] = "ready"
                w["assigned"] = None

            job = rt.jobs.current
            if not job:
                return

            jrt = job.runtime
            if jrt.phase == "map":
                jrt.map_state.inflight.pop(task_id, None)
                jrt.map_state.done.add(task_id)
            elif jrt.phase == "reduce":
                jrt.reduce_state.inflight.pop(task_id, None)
                jrt.reduce_state.done.add(task_id)
            rt.cv.notify_all()

    # ---------- UDP handler (heartbeats) ----------

    def _handle_udp(self, message: Dict) -> None:
        """Handle a UDP heartbeat from a worker."""
        if message.get("message_type") != "heartbeat":
            return
        rt = self.runtime
        wid = (
            f"{message.get('worker_host')}:"
            f"{int(message.get('worker_port'))}"
        )
        w = rt.workers.table.get(wid)
        if not w:
            LOGGER.warning("Heartbeat from unregistered Worker")
            return
        w["last_ts"] = time.time()
        w["missed"] = 0

    # ---------- Liveness monitor ----------

    def _liveness_monitor(self) -> None:
        """Poll workers and mark dead ones, requeueing their tasks."""
        rt = self.runtime
        while not self.signals["stop"]:
            time.sleep(2.0)
            with rt.cv:
                now = time.time()
                changed = False
                for _wid, worker in rt.workers.table.items():
                    if self._check_worker_liveness(now, worker):
                        self._requeue_worker_task(rt, worker)
                        changed = True
                if changed:
                    rt.cv.notify_all()

    def _check_worker_liveness(self, now: float, worker: Dict) -> bool:
        """Return True if the worker should be marked dead."""
        if worker["state"] == "dead":
            return False

        if now - worker["last_ts"] > 2.0:
            worker["missed"] += 1

        ready_and_stale = (
            worker["state"] == "ready"
            and worker["assigned"]
            and (now - worker["last_ts"] > 10)
        )
        if worker["missed"] > 5 or ready_and_stale:
            worker["state"] = "dead"
            return True
        return False

    def _requeue_worker_task(self, rt: ManagerRuntime, worker: Dict) -> None:
        """If dead worker had task, put it back in the appropriate queue."""
        task = worker.get("assigned")
        worker["assigned"] = None
        job = rt.jobs.current
        if not job or task is None:
            return

        jrt = job.runtime
        if jrt.phase == "map":
            if task not in jrt.map_state.done:
                jrt.map_state.inflight.pop(task, None)
                tsk = jrt.map_state.tasks_by_id.get(task)
                if tsk is not None:
                    jrt.map_state.pending.append(tsk)
        elif jrt.phase == "reduce":
            if task not in jrt.reduce_state.done:
                jrt.reduce_state.inflight.pop(task, None)
                tsk = jrt.reduce_state.tasks_by_id.get(task)
                if tsk is not None:
                    jrt.reduce_state.pending.append(tsk)

    # ---------- Scheduler ----------

    def _scheduler(self) -> None:
        """Schedule map/reduce tasks onto ready workers."""
        rt = self.runtime
        while True:
            if self.signals["stop"]:
                break
            with rt.cv:
                if self.signals["stop"]:
                    break

                self._maybe_start_job()
                job = rt.jobs.current

                if not job:
                    rt.cv.wait(timeout=0.1)
                    continue

                if job.runtime.phase == "map":
                    self._handle_map_phase(job)
                    continue

                if job.runtime.phase == "reduce":
                    self._handle_reduce_phase(job)
                    continue

    # ---------- Helpers ----------

    def _forward_shutdown_to_workers(self) -> None:
        """Send shutdown to all registered, non-dead workers (best-effort)."""
        if self._shutdown_forwarded:
            return

        rt = self.runtime
        targets = []
        for _wid, w in rt.workers.table.items():
            host = w.get("host")
            port = w.get("port")
            state = w.get("state")
            if host and (port is not None) and state != "dead":
                targets.append((host, int(port)))

        for host, port in targets:
            try:
                tcp_send(host, port, {"message_type": "shutdown"})
            except (ConnectionError, TimeoutError, OSError):
                # Don't crash on mocks or refused connections.
                pass
        self._shutdown_forwarded = True

    def _assign_map(self, job: Job) -> None:
        """Assign pending map tasks to ready workers."""
        rt = self.runtime
        jrt = job.runtime
        mstate = jrt.map_state
        for wid in rt.workers.order:
            if not mstate.pending:
                break
            w = rt.workers.table.get(wid)
            if not w or w["state"] != "ready" or w["assigned"] is not None:
                continue
            task = mstate.pending.popleft()
            try:
                tcp_send(w["host"], w["port"], task.to_message())
            except ConnectionRefusedError:
                w["state"] = "dead"
                mstate.pending.append(task)
                continue
            w["state"] = "busy"
            w["assigned"] = task.task_id
            mstate.inflight[task.task_id] = wid
            LOGGER.info("Assigned task to Worker %s %s", task, wid)

    def _assign_reduce(self, job: Job) -> None:
        """Assign pending reduce tasks to ready workers."""
        rt = self.runtime
        jrt = job.runtime
        rstate = jrt.reduce_state
        for wid in rt.workers.order:
            if not rstate.pending:
                break
            w = rt.workers.table.get(wid)
            if not w or w["state"] != "ready" or w["assigned"] is not None:
                continue
            task = rstate.pending.popleft()
            try:
                tcp_send(w["host"], w["port"], task.to_message())
            except ConnectionRefusedError:
                w["state"] = "dead"
                rstate.pending.append(task)
                continue
            w["state"] = "busy"
            w["assigned"] = task.task_id
            rstate.inflight[task.task_id] = wid
            LOGGER.info("Assigned task to Worker %s %s", task, wid)

    def _build_map_tasks(self, job: Job) -> None:
        """Create and queue all map tasks for the given job."""
        jrt = job.runtime
        mstate = jrt.map_state
        cfg = job.config
        files = sorted(
            [p for p in Path(cfg.input_directory).iterdir() if p.is_file()]
        )
        partitions: List[List[Path]] = [[] for _ in range(cfg.num_mappers)]
        for idx, fpath in enumerate(files):
            partitions[idx % cfg.num_mappers].append(fpath)
        mstate.pending.clear()
        mstate.tasks_by_id.clear()
        for tid, group in enumerate(partitions):
            task = MapTask(
                task_id=tid,
                input_paths=group,
                executable=cfg.mapper_executable,
                output_directory=jrt.job_tmp_dir,
                num_partitions=cfg.num_reducers,
            )
            mstate.pending.append(task)
            mstate.tasks_by_id[tid] = task

    def _build_reduce_tasks(self, job: Job) -> None:
        """Create and queue all reduce tasks for the given job."""
        jrt = job.runtime
        rstate = jrt.reduce_state
        cfg = job.config
        rstate.pending.clear()
        rstate.tasks_by_id.clear()
        for r in range(cfg.num_reducers):
            globbed = sorted(
                jrt.job_tmp_dir.glob(f"maptask*-part{r:05d}")
            )
            task = ReduceTask(
                task_id=r,
                input_paths=globbed,
                executable=cfg.reducer_executable,
                output_directory=cfg.output_directory,
            )
            rstate.pending.append(task)
            rstate.tasks_by_id[r] = task

    def _maybe_start_job(self) -> None:
        """Start a new job if none is currently running."""
        rt = self.runtime
        if not rt.jobs.current and rt.jobs.queue:
            rt.jobs.current = rt.jobs.queue.popleft()
            self._build_map_tasks(rt.jobs.current)
            rt.jobs.current.runtime.phase = "map"
            LOGGER.info("Begin Map Stage")

    def _handle_map_phase(self, job: Job) -> None:
        """Drive the map phase for the current job."""
        jrt = job.runtime
        mstate = jrt.map_state
        self._assign_map(job)
        rt = self.runtime
        if (not mstate.pending) and (not mstate.inflight):
            LOGGER.info("End Map Stage")
            self._build_reduce_tasks(job)
            jrt.phase = "reduce"
            LOGGER.info("begin Reduce Stage")
        else:
            rt.cv.wait(timeout=0.05)

    def _handle_reduce_phase(self, job: Job) -> None:
        """Drive the reduce phase for the current job."""
        jrt = job.runtime
        rstate = jrt.reduce_state
        self._assign_reduce(job)
        rt = self.runtime
        if (not rstate.pending) and (not rstate.inflight):
            LOGGER.info("end Reduce Stage")
            shutil.rmtree(jrt.job_tmp_dir, ignore_errors=True)
            LOGGER.info("Removed %s", jrt.job_tmp_dir)
            rt.jobs.current = None
        else:
            rt.cv.wait(timeout=0.05)


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
    Manager(host, port, shared_dir=shared_dir)


if __name__ == "__main__":
    main()
