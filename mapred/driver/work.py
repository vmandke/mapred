import base64
import pickle
import random
from typing import Callable, Optional, Dict
from uuid import uuid4

import requests

from mapred.constants import DEFAULT_M, DEFAULT_R, TaskType, WorkerStatus, TaskStatus
from mapred.store.api_fetcher import APIFetcher
from mapred.store.fetcher import Fetcher


class KnownWorker:
    def __init__(self, wid: str, uri: str):
        self.worker_id = wid
        self.worker_uri = uri
        self.task_id = None
        self.task_status = None
        self.status = WorkerStatus.REGISTERED
        self.last_heartbeat = None


class Task:
    def __init__(self, task_id: str, task_metadata: dict, task_type: TaskType):
        self.task_id = task_id
        self.task_type = task_type
        self.task_metadata = task_metadata
        self.assigned_worker_uri = None
        self.status = TaskStatus.PENDING
        self.parent_dependencies = []
        self.child_dependencies = []
        self.result = None

    def get_status(self):
        return {
            "task_id": self.task_id,
            "task_type": self.task_type.name,
            "status": self.status.name,
            "assigned_worker_uri": self.assigned_worker_uri,
            "parent_dependencies": [p.task_id for p in self.parent_dependencies],
            "child_dependencies": [c.task_id for c in self.child_dependencies],
        }

    def assignment_metadata(self):
        return {
            "task_id": self.task_id,
            "task_metadata": self.task_metadata,
            "task_type": self.task_type.name,
            "result": self.result,
            "parent_dependencies": [
                {"worker_uri": p.assigned_worker_uri, "task_id": p.task_id}
                for p in self.parent_dependencies
            ],
            "child_dependencies": [
                {"worker_uri": c.assigned_worker_uri, "task_id": c.task_id}
                for c in self.child_dependencies
            ],
        }


class DriverWork:
    def __init__(
        self,
        mapper: Callable,
        reducer: Callable,
        fetcher: Fetcher,
        M: Optional[int] = None,
        R: Optional[int] = None,
    ):
        self.M = M or DEFAULT_M
        self.R = R or DEFAULT_R
        self.work_id = str(uuid4())
        map_chunks = fetcher.get_chunks_metadata(self.M)
        map_tasks = []
        for i, chunk in enumerate(map_chunks):
            map_tasks.append(
                Task(
                    task_id=f"M_{i}",
                    task_metadata=dict(
                        chunk,
                        mapper=base64.b64encode(pickle.dumps(mapper)).decode("utf-8"),
                    ),
                    task_type=TaskType.MAP,
                )
            )
        reduce_tasks = []
        reduce_fetcher = APIFetcher(data_path=None, parser=None)
        for i in range(0, len(map_tasks), R):
            map_tasks_chunk = map_tasks[i : i + R]
            reduce_task = Task(
                task_id=f"R_{len(reduce_tasks)}",
                task_metadata={
                    "fetcher_pkl": reduce_fetcher.pickle(),
                    "reducer": base64.b64encode(pickle.dumps(reducer)).decode("utf-8"),
                },
                task_type=TaskType.REDUCE,
            )
            for map_task in map_tasks_chunk:
                map_task.child_dependencies.append(reduce_task)
                reduce_task.parent_dependencies.append(map_task)
            reduce_tasks.append(reduce_task)
        self.all_tasks = map_tasks + reduce_tasks
        self.final_task = Task(
            task_id="final",
            task_metadata={
                "reducer": base64.b64encode(pickle.dumps(reducer)).decode("utf-8"),
            },
            task_type=TaskType.REDUCE,
        )
        for reduce_task in reduce_tasks:
            reduce_task.child_dependencies.append(self.final_task)
            self.final_task.parent_dependencies.append(reduce_task)

    def update_task_status(
        self, task_id: str, task_status: TaskStatus, worker_uri: str, worker_id: str
    ):
        task = next(t for t in self.all_tasks if t.task_id == task_id)
        task.status = task_status
        task.assigned_worker_uri = worker_uri
        task.worker_id = worker_id

    def rebalance_work(self, workers: Dict[str, KnownWorker]):
        inactive_workers = [
            w.worker_uri for w in workers.values() if w.status == WorkerStatus.INACTIVE
        ]
        # Mark all tasks assigned to inactive workers as pending
        for task in self.all_tasks:
            if task.assigned_worker_uri in inactive_workers:
                task.assigned_worker_uri = None
                task.status = TaskStatus.PENDING
                for child_task in task.child_dependencies:
                    child_task.status = TaskStatus.PENDING
                    child_task.assigned_worker_uri = None
        # Assign pending tasks to idle workers
        idle_workers = [
            w.worker_uri for w in workers.values() if w.status == WorkerStatus.IDLE
        ]
        pending_pickable_tasks = []
        for task in self.all_tasks:
            if task.status == TaskStatus.PENDING and all(
                p.status == TaskStatus.COMPLETED for p in task.parent_dependencies
            ):
                pending_pickable_tasks.append(task)
        random_tasks: Task = random.choices(pending_pickable_tasks, k=len(idle_workers))
        for random_task, random_worker_uri in zip(random_tasks, idle_workers):
            # Try and assign task to worker
            requests.post(
                f"{random_worker_uri}/assign_task",
                json=random_task.assignment_metadata(),
            )

    def save_result(self, task_id: str, result: dict):
        task = next(t for t in self.all_tasks if t.task_id == task_id)
        task.status = TaskStatus.COMPLETED
        task.result = result

    def collate_results(self):
        if all(t.status == TaskStatus.COMPLETED for t in self.all_tasks):
            reducer = eval(
                pickle.loads(base64.b64decode(self.final_task.task_metadata["reducer"]))
            )
            records = []
            for parent in self.final_task.parent_dependencies:
                records.extend(parent.result)
            result = reducer(records)
            print(f"Final result: {result}")
            return result
        else:
            return None

    def get_status(self):
        return [t.get_status() for t in self.all_tasks]
