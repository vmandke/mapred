import base64
import json
import pickle
import sys
from abc import ABC, abstractmethod
from multiprocessing import Lock
from typing import Any, Optional
from pathlib import Path

import requests

from mapred.constants import TaskType, WorkerStatus, TaskStatus

WORKER_ID = None
WORKER_LOCK = Lock()
WORKER_STATUS = WorkerStatus.IDLE
WORKER_TASK_ID = None
WORKER_TASK_STATUS = None


class Worker(ABC):
    def __init__(
        self,
        driver_uri: str,
        worker_id: str,
        worker_uri: str,
        task_id: str,
        task_metadata: dict,
        task_type: str,
        parent_dependencies: list,
        child_dependencies: list,
        *args,
        **kwargs,
    ):
        self.driver_uri = driver_uri
        self.worker_id = worker_id
        self.worker_uri = worker_uri
        self.task_id = task_id
        self.task_metadata = task_metadata
        self.task_type = TaskType(task_type)
        self.parent_dependencies = parent_dependencies
        self.child_dependencies = child_dependencies

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def save_result(self, result: Any):
        pass


class MapperWorker(Worker):
    def run(self):
        # mapper needs to be evaled; UNSAFE!!
        mapper = eval(pickle.loads(base64.b64decode(self.task_metadata["mapper"])))
        fetcher = pickle.loads(base64.b64decode(self.task_metadata["fetcher_pkl"]))
        records = fetcher.get_records_from_metadata(self.task_metadata["metadata"])
        print(f"Received {len(records)} records")
        records = [mapper(r) for r in records]
        self.save_result(records)

    def save_result(self, result: Any):
        Path(f"/tmp/{self.worker_id}").mkdir(parents=True, exist_ok=True)
        with open(f"/tmp/{self.worker_id}/{self.task_id}", "w") as fd:
            fd.write(json.dumps(result))
        print(f"Saved result for task {self.task_id} by worker {self.worker_id}")


class ReducerWorker(Worker):
    def run(self):
        # reducer needs to be evaled; UNSAFE!!
        reducer = eval(pickle.loads(base64.b64decode(self.task_metadata["reducer"])))
        fetcher = pickle.loads(base64.b64decode(self.task_metadata["fetcher_pkl"]))
        records = []
        for parent in self.parent_dependencies:
            metadata = self.task_metadata.get("metadata", {})
            metadata["uri"] = (
                f"{parent['worker_uri']}/result?task_id={parent['task_id']}"
            )
            parent_chunk = fetcher.get_records_from_metadata(metadata)
            records.extend(parent_chunk)
        print(f"Reducer:: Received {len(records)} records")
        records = reducer(records)
        print(f"Reducer:: Reduced to value:: {records}")
        self.save_result(records)

    def save_result(self, result: Any):
        # Send the result to the driver
        requests.put(
            f"{self.driver_uri}/result",
            json={"task_id": self.task_id, "result": result},
            timeout=1,
        )


def work_runner(args: dict):
    if args["task_type"] == TaskType.MAP.name:
        worker = MapperWorker(**args)
    elif args["task_type"] == TaskType.REDUCE.name:
        worker = ReducerWorker(**args)
    else:
        raise ValueError("Invalid task type")
    try:
        worker.run()
        requests.post(
            f"{worker.worker_uri}/notify_task_done",
            json={"task_id": worker.task_id, "status": TaskStatus.COMPLETED.name},
            timeout=1.5,
        )
    except Exception as e:
        print(f"Worker {worker.worker_id} failed with error: {e}")
        requests.post(
            f"{worker.worker_uri}/notify_task_done",
            json={"task_id": worker.task_id, "status": TaskStatus.FAILED.name},
            timeout=1.5,
        )
    sys.exit()


def update_worker_status(
    worker_status: WorkerStatus, task_id: Optional[str], worker_task_status: TaskStatus
):
    global WORKER_STATUS, WORKER_TASK_ID, WORKER_TASK_STATUS
    WORKER_STATUS = worker_status
    WORKER_TASK_ID = task_id
    WORKER_TASK_STATUS = worker_task_status
