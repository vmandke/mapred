import base64
import pickle
import time
from datetime import datetime
from typing import Callable, Optional
from threading import Lock, Thread

import requests
import schedule
from flask import request, Flask, jsonify, render_template

from mapred.constants import (
    WorkerStatus,
    DriverStatus,
    TaskStatus,
    WORKER_TIMEOUT_SECONDS,
)
from mapred.driver.work import KnownWorker, DriverWork

DRIVER_PORT = None
REGISTERED_WORKERS = {}
RESULT_SETS = {}
DRIVER_STATUS = DriverStatus.IDLE
DRIVER_WORK: Optional[DriverWork] = None

driver_app = Flask(__name__, template_folder=".")


class Locker:
    def __init__(self):
        self.lock = Lock()
        self.holder = None

    def hold(self, holder):
        self.holder = holder

    def __enter__(self):
        acquired_state = self.lock.acquire(timeout=2)
        if not acquired_state:
            raise Exception(
                f"Could not acquire lock, held by another process:: {self.holder}"
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


MASTER_LOCK: Optional[Locker] = None


@driver_app.route("/result", methods=["PUT"])
def save_result():
    global RESULT_SETS
    data = request.json
    if DRIVER_WORK:
        DRIVER_WORK.save_result(task_id=data["task_id"], result=data["result"])
    return jsonify({"status": "Result saved"}), 200


@driver_app.route("/collate", methods=["GET"])
def collate_results():
    global DRIVER_WORK, DRIVER_STATUS
    with MASTER_LOCK:
        MASTER_LOCK.hold("collate")
        result = None
        if DRIVER_WORK:
            result = DRIVER_WORK.collate_results()
            if result:
                RESULT_SETS[DRIVER_WORK.work_id] = result
                DRIVER_WORK = None
                DRIVER_STATUS = DriverStatus.IDLE
        return jsonify(result), 200


@driver_app.route("/driver_status", methods=["GET"])
def get_driver_status():
    global DRIVER_STATUS
    return jsonify({"status": DRIVER_STATUS.name}), 200


@driver_app.route("/get_result", methods=["GET"])
def get_result():
    work_id = request.args.get("work_id")
    if work_id not in RESULT_SETS:
        return jsonify({"status": "Work not found"}), 404
    return jsonify(RESULT_SETS[work_id]), 200


@driver_app.route("/start_master_work", methods=["POST"])
def start_master_work():
    data = request.json
    global DRIVER_WORK, DRIVER_STATUS
    with MASTER_LOCK:
        MASTER_LOCK.hold("start_master_work")
        if DRIVER_STATUS == DriverStatus.BUSY:
            return jsonify({"status": "Driver is busy"}), 400
        DRIVER_WORK = DriverWork(
            mapper=pickle.loads(base64.b64decode(data["mapper"].encode("utf-8"))),
            reducer=pickle.loads(base64.b64decode(data["reducer"].encode("utf-8"))),
            fetcher=pickle.loads(base64.b64decode(data["fetcher"].encode("utf-8"))),
            M=data.get("M"),
            R=data.get("R"),
        )
        DRIVER_STATUS = DriverStatus.BUSY
        status = DRIVER_WORK.get_status()
    return jsonify({"status": status, "work_id": DRIVER_WORK.work_id}), 200


@driver_app.route("/register_worker", methods=["POST"])
def register_new_worker():
    uri = request.json["uri"]
    with MASTER_LOCK:
        MASTER_LOCK.hold("register_worker")
        wid = f"{hash(uri)}_{datetime.now().timestamp()}"
        REGISTERED_WORKERS[wid] = KnownWorker(wid, uri)
        # As the worker is new, we can assume it is active
        REGISTERED_WORKERS[wid].last_heartbeat = datetime.now().timestamp()
        return wid


@driver_app.route("/get_work_status", methods=["GET"])
def get_work_status():
    global DRIVER_WORK
    with MASTER_LOCK:
        MASTER_LOCK.hold("get_work_status")
        data = {
            "driver_work": (DRIVER_WORK and DRIVER_WORK.get_status()) or [],
            "driver_status": DRIVER_STATUS.name,
            "registered_workers": {
                worker.worker_id: {
                    "worker_uri": worker.worker_uri,
                    "task_id": worker.task_id,
                    "task_status": worker.task_status and worker.task_status.name,
                    "status": worker.status.name,
                    "last_heartbeat": (
                        worker.last_heartbeat
                        and datetime.now().timestamp() - worker.last_heartbeat
                    ),
                }
                for worker in REGISTERED_WORKERS.values()
            },
        }
        return render_template("display_status.html", **data)


@driver_app.route("/heartbeat", methods=["POST"])
def get_update_worker_status():
    data = request.json
    print(f"Received heartbeat: {data}")
    wid = data["wid"]
    with MASTER_LOCK:
        MASTER_LOCK.hold("heartbeat")
        if wid not in REGISTERED_WORKERS:
            raise Exception("Worker not found")
        worker = REGISTERED_WORKERS[wid]
        if worker.status == WorkerStatus.INACTIVE:
            return jsonify({"status": "Worker is inactive, cannot update status"}), 400
        REGISTERED_WORKERS[wid].status = WorkerStatus(data["status"])
        REGISTERED_WORKERS[wid].last_heartbeat = data["last_heartbeat"]
        REGISTERED_WORKERS[wid].task_id = data.get("task_id")
        REGISTERED_WORKERS[wid].task_status = data.get("task_status") and TaskStatus(
            data.get("task_status")
        )
        if DRIVER_WORK and REGISTERED_WORKERS[wid].task_id:
            DRIVER_WORK.update_task_status(
                task_id=REGISTERED_WORKERS[wid].task_id,
                task_status=REGISTERED_WORKERS[wid].task_status,
                worker_id=wid,
                worker_uri=REGISTERED_WORKERS[wid].worker_uri,
            )
            print(
                f"Worker {wid} has task {REGISTERED_WORKERS[wid].task_id} and"
                f" status {REGISTERED_WORKERS[wid].task_status}"
            )
        print(f"Worker {wid} is now {str(REGISTERED_WORKERS[wid].task_status)}")
    return jsonify({"status": "Worker status updated"}), 200


@driver_app.route("/rebalance", methods=["GET"])
def rebalance_route():
    global DRIVER_WORK
    with MASTER_LOCK:
        MASTER_LOCK.hold("rebalance")
        try:
            timestamp_now = datetime.now().timestamp()
            for worker in REGISTERED_WORKERS.values():
                if worker.last_heartbeat and (
                    (timestamp_now - worker.last_heartbeat) > WORKER_TIMEOUT_SECONDS
                ):
                    worker.status = WorkerStatus.INACTIVE
                    print(f"Worker {worker.worker_id} is now {worker.status.name}")
            DRIVER_WORK and DRIVER_WORK.rebalance_work(REGISTERED_WORKERS)
        except Exception as e:
            print(f"Error rebalancing work: {e}")
    return jsonify({"status": "Work rebalanced"}), 200


def rebalance_caller():
    global DRIVER_PORT
    requests.get(f"http://localhost:{DRIVER_PORT}/rebalance")
    requests.get(f"http://localhost:{DRIVER_PORT}/collate")


def schedule_work():
    schedule.every(3).seconds.do(rebalance_caller)
    while True:
        schedule.run_pending()
        time.sleep(0.2)


def start_driver(port: int):
    global DRIVER_PORT, MASTER_LOCK
    DRIVER_PORT = port
    MASTER_LOCK = Locker()
    scheduler_thread = Thread(target=schedule_work)
    scheduler_thread.start()
    driver_app.run(port=port)
