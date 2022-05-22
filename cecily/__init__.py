import concurrent.futures
import logging
import multiprocessing
import multiprocessing.connection as mc
import os
import shutil
import tempfile
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from enum import Enum
from multiprocessing.managers import SyncManager
from pathlib import Path
from queue import Queue
from typing import Any, Callable, Generic, Iterator, TypeVar, final
from uuid import UUID, uuid4

T = TypeVar('T')
RT = TypeVar('RT')

logger = logging.getLogger(__name__)


class Alias:
    WORKER = 'WORKER'
    MAIN = 'MAIN'
    OTHER = 'OTHER'
    MANAGER = 'MANAGER'


def trace(process_alias, msg, *args, **kwargs):
    current_process = multiprocessing.current_process()
    logger.debug(f'[{process_alias}] {msg} ({current_process}, pid={os.getpid()})', *args, **kwargs)


class Status(Enum):
    READY = 1


@final
class JobFinished:
    pass


# Sentinel object that indicates that a job has finished execution
JOB_FINISHED = JobFinished()


class Job:
    id: UUID
    status: Status
    socket_file: Path

    task_fn: Callable

    listener: mc.Listener | None
    conns: list[mc.Connection]

    conn_listener: threading.Thread

    send: mc.Connection
    recv: mc.Connection

    def __init__(
        self,
        job_id: UUID,
        queue: Queue,
        task_fn: Callable,
        args: list,
        kwargs: dict,
    ) -> None:
        super().__init__()
        self.id = job_id
        self.status = Status.READY
        # self.socket_file = socket_file
        self.task_fn = task_fn
        self.args = args
        self.kwargs = kwargs
        self.conns = []
        self.listener = None
        self.acceptor_started_event = threading.Event()
        self.recv, self.send = multiprocessing.Pipe()

        self.q = queue

    # def start_listening(self):
    #     trace(Alias.OTHER, 'socket path listener: %s', self.socket_file)
    #     self.listener = mc.Listener(str(self.socket_file), family='AF_UNIX')
    #
    #     def acceptor(listener: mc.Listener, conns: list[mc.Connection], started: threading.Event):
    #         logger.debug('[WORKER] started listener for task id=%s', self.id)
    #
    #         started.set()
    #
    #         while True:
    #             conn = listener.accept()
    #             logger.debug('[WORKER] accepted conn: %s', conn)
    #             conns.append(conn)
    #
    #     # FIXME: kill thread on done
    #     self.conn_listener = threading.Thread(
    #         target=acceptor, args=(self.listener, self.conns, self.acceptor_started_event), daemon=True
    #     )
    #     self.conn_listener.start()

    # def notify(self, obj):
    #     for conn in self.conns:
    #         conn.send(obj)

    def start_work(self) -> Any:
        trace(Alias.WORKER, 'job starting for id=%s', self.id)

        # started_evt = threading.Event()

        # recv, send = multiprocessing.Pipe()

        # def notifier(started: threading.Event, conn: mc.Connection, job: Job):
        #     started.set()
        #
        #     while not conn.closed:
        #         # FIXME: this currently exits by closing conn and throwing an exception
        #         #   preferably this should be handled nicer than dying
        #         try:
        #             job.notify(conn.recv())
        #         finally:
        #             break

        # threading.Thread(target=notifier, args=(started_evt, self.recv, self), daemon=True).start()

        # XXX: arbitrary wait
        # started_evt.wait(timeout=15)

        result = self.task_fn(*self.args, **self.kwargs, notifier=self.q)  # noqa

        return result

    def execute(self) -> Any:
        # self.start_listening()
        # self.acceptor_started_event.wait()

        result = self.start_work()

        # self.q.close()
        # self.q.join()

        self.q.put(JOB_FINISHED)

        # XXX: don't know if this is necessary
        # forcefully delete weakref to queue object
        del self.q

        # self.listener.close()
        # self.recv.close()
        # self.send.close()
        return result


@dataclass
class CecilyFuture(Generic[RT]):
    job_id: UUID
    queue: Queue

    _future: concurrent.futures.Future

    def collect(self) -> Iterator[RT]:
        if not self._future.running:
            return

        # with mc.Client(str(self.socket_file), family='AF_UNIX') as client:
        #     # try:
        #     #     def done_callback(_future):
        #     #         raise RuntimeError
        #     #
        #     #     self._future.add_done_callback(done_callback)
        #
        #     while not client.closed and self._future.running():
        #         yield client.recv()

        while True:
            v = self.queue.get()

            if isinstance(v, JobFinished):
                break

            yield v

        # finally:
        #     pass

    def result(self) -> Any:
        return self._future.result()


class CecilyTask(Callable, ABC):
    @abstractmethod
    def apply(self, *args, **kwargs) -> CecilyFuture:
        ...

    @abstractmethod
    def __call__(self, *args, **kwargs):
        ...


def worker(task_fn, job_id, queue, args, kwargs):
    trace(Alias.WORKER, 'init new job with task_fn=%s id=%s', task_fn.__name__, job_id)

    job = Job(job_id, queue, task_fn, args, kwargs)

    return job.execute()


@dataclass
class Task:
    app_ref: 'Cecily'
    task_fn: Callable

    def __call__(self, *args, **kwargs) -> CecilyFuture:
        trace(Alias.OTHER, 'task for task_fn=%s called', self.task_fn.__name__)

        # create id
        job_id = uuid4()

        # create pub/sub conn
        # temp_socket_file: Path = self.app_ref.sock_dir / str(job_id)
        # temp_socket_file = temp_socket_file.with_suffix('.sock')

        queue_proxy = self.app_ref.manager.Queue()
        # self.app_ref.manager.register(job_id, queue)

        future = self.app_ref.executor.submit(
            worker,
            self.task_fn,
            job_id,
            # temp_socket_file,
            # self.task_fn.__name__,
            queue_proxy,
            args,
            kwargs,
        )

        return CecilyFuture(job_id, queue_proxy, future)


class Cecily:
    executor: ProcessPoolExecutor
    sock_dir: Path

    manager_sock: Path

    serialized_tasks: list[bytes]

    spawned: bool

    manager: SyncManager

    def __init__(self, max_workers: int | None = None) -> None:
        current_process = multiprocessing.current_process()

        # XXX: determines if this app is called from the Main app's ProcessPoolExecutor
        #   by checking if the current process is spawned
        if isinstance(current_process, multiprocessing.context.SpawnProcess):
            self.spawned = True
            trace(Alias.OTHER, 'skipping app init')
            return

        trace(Alias.MAIN, 'creating app')
        self.spawned = False
        self.executor = ProcessPoolExecutor(max_workers)
        self.sock_dir = Path(tempfile.mkdtemp())

        self.deferred_functions = []
        self.manager = SyncManager(address='manager.sock')

    def start(self):
        if self.spawned:
            return

        self.manager.start()

        logger.debug('[MAIN] starting app')

    def task(self, fn) -> CecilyTask:
        if self.spawned:
            return fn

        logger.debug('[MAIN] registering new task: %s', fn.__name__)

        fn.apply = Task(self, fn)

        # TODO: no duplicate registers
        self.deferred_functions.append(fn)

        return fn

    def close(self):
        logger.debug('[MAIN] shutting down app')
        self.manager.shutdown()

        self.executor.shutdown(cancel_futures=True)

        shutil.rmtree(self.sock_dir)
        logger.debug('[MAIN] shutdown complete')
