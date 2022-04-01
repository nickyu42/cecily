import concurrent.futures
import logging
import multiprocessing
import multiprocessing.connection as mc
import os
import pickle
import sys
import tempfile
import threading
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from enum import Enum
from multiprocessing.managers import BaseManager
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Generic, Iterator, TypeVar
from uuid import UUID, uuid4

T = TypeVar("T")
RT = TypeVar("RT")

logger = logging.getLogger(__name__)


class Status(Enum):
    READY = 1


class Job:
    id: UUID
    status: Status
    socket_file: Path

    task_fn: Callable

    listener: mc.Listener | None
    conns: list[mc.Connection]

    conn_listener: threading.Thread

    def __init__(
        self,
        job_id: UUID,
        socket_file: Path,
        task_fn: Callable,
        args: list,
        kwargs: dict,
    ) -> None:
        super().__init__()

        self.id = job_id
        self.status = Status.READY
        self.socket_file = socket_file
        self.task_fn = task_fn
        self.args = args
        self.kwargs = kwargs
        self.conns = []
        self.listener = None

    def start_listening(self):
        self.listener = mc.Listener('test.sock', family="AF_UNIX")

        def conn_listener(listener: mc.Listener, conns: list[mc.Connection]):
            logger.debug("[WORKER] Started listener for task %s", self.id)
            while True:
                conn = listener.accept()
                logger.debug("[WORKER] Accepted conn: %s", conn)
                conns.append(conn)

        self.conn_listener = threading.Thread(
            target=conn_listener, args=(self.listener, self.conns), daemon=True
        )
        self.conn_listener.start()

    def notify(self, obj):
        for conn in self.conns:
            conn.send(obj)

    def start_work(self):
        logger.debug("[WORKER] Job for %s starting from %s", self.id, multiprocessing.current_process())

        print(self.task_fn)

        # spec = inspect.signature(self.task_fn)

        print(self.task_fn.__name__)

        recv, send = multiprocessing.Pipe()

        def notifier(conn, job):
            while True:
                try:
                    job.notify(conn.recv())
                except EOFError:
                    break

        threading.Thread(target=notifier, args=(recv, self), daemon=True).start()
        self.notify(self.task_fn(*self.args, **self.kwargs, notifier=send)._getvalue())

        # for res in self.task_fn(*self.args, **self.kwargs):
        #     self.notify(res)

    # def close(self):
    #     self.listener.close()


@dataclass
class ParsleyFuture(Generic[RT]):
    job_id: UUID
    socket_file: Path

    _future: concurrent.futures.Future

    def collect(self) -> Iterator[RT]:
        if not self._future.running:
            return

        with mc.Client('test.sock', family="AF_UNIX") as client:
            while True:
                yield client.recv()

    def wait(self) -> Any:
        return self._future.result()


def worker(job_id, temp_socket_file, manager_sock, task_fn_name, args, kwargs):
    logger.debug('[WORKER] Starting for %s with task %s (%s, pid=%s)', job_id, task_fn_name,
                 multiprocessing.current_process(), os.getpid())
    man = TaskManager(address="manager.sock")
    man.register(task_fn_name)
    man.connect()

    task_fn = getattr(man, task_fn_name)
    print('foo', type(task_fn), dir(task_fn))

    job = Job(job_id, temp_socket_file, task_fn, args, kwargs)
    job.start_listening()
    job.start_work()


@dataclass
class Task:
    app_ref: "Parsley"
    task_fn: str

    def __call__(self, *args, **kwargs) -> ParsleyFuture:
        # create id
        job_id = uuid4()

        # create pub/sub conn
        temp_socket_file: Path = self.app_ref.sock_dir / str(job_id)
        temp_socket_file = temp_socket_file.with_suffix(".sock")
        temp_socket_file.touch(exist_ok=True)

        print(f"submit {self.app_ref.executor}")
        future = self.app_ref.executor.submit(
            worker,
            job_id,
            temp_socket_file,
            self.app_ref.manager_sock,
            self.task_fn,
            args,
            kwargs,
        )
        # future = self._app_ref.executor.submit(
        #     worker, job_id, temp_socket_file, self.task_fn, args, kwargs
        # )

        return ParsleyFuture(job_id, temp_socket_file, future)


class TaskManager(BaseManager):
    pass


def manager_worker(tasks, sock='manager.sock'):
    logger.debug('[MANAGER] Starting (%s, pid=%s)', multiprocessing.current_process(), os.getpid())
    m = TaskManager(sock)

    for task in tasks:
        task_fn = pickle.loads(task)
        logger.debug('[MANAGER] Registering %s', task_fn.__name__)
        # g = globals()
        # g[task_fn.__name__] = task_fn
        m.register(task_fn.__name__, task_fn)

    s = m.get_server()
    s.serve_forever()


class Parsley:
    executor: ProcessPoolExecutor
    sock_dir: Path

    manager: TaskManager | None
    manager_sock: Path

    serialized_tasks: list[bytes]

    MODULE_NAME = 'sandbox'

    def __init__(self, max_workers: int | None = None) -> None:
        logger.debug('[MAIN] Creating app (%s, pid=%s)', multiprocessing.current_process(), os.getpid())
        self.executor = ProcessPoolExecutor(max_workers)
        self.sock_dir = Path(tempfile.mkdtemp())
        self.manager = TaskManager(address="manager.sock")

        logger.debug("[MAIN] Created sock dir: %s", self.sock_dir)
        self.manager_sock = self.sock_dir / "manager.sock"

        logger.debug('[MAIN] Created module: %s', self.MODULE_NAME)
        sys.modules[self.MODULE_NAME] = ModuleType(self.MODULE_NAME)

        self.serialized_tasks = []

    def start(self):
        logger.debug('[MAIN] Creating manager process')
        # self.manager.start()
        p = multiprocessing.Process(target=manager_worker, args=(self.serialized_tasks,), daemon=True)
        p.start()

    def task(self, fn) -> Task:
        logger.debug("[MAIN] Registering new task: %s", fn.__name__)

        # print(fn.__module__, fn.__qualname__)
        # print(sys.modules['tasks'])
        #
        # task_wrapper = type(fn.__name__, tuple(), {'execute': fn})
        # globals()[fn.__name__] = task_wrapper

        fn.__module__ = self.MODULE_NAME
        setattr(sys.modules[self.MODULE_NAME], fn.__name__, fn)

        self.serialized_tasks.append(pickle.dumps(fn))

        # TODO: no duplicate registers
        # fn.apply = Task(self, fn.__name__)
        # self.manager.register(fn.__name__, fn)
        # TaskManager.register(fn.__name__, pickle.dumps(task_wrapper, pickle.HIGHEST_PROTOCOL))
        return Task(self, fn.__name__)

    def close(self):
        self.sock_dir.rmdir()
        logger.debug("[MAIN] Shutting down Parsley")
