import time
from datetime import datetime
from multiprocessing import Process
from threading import Thread

import requests
import schedule
from flask import Flask, request

from mapred.constants import WorkerStatus, TaskStatus
from mapred.worker.work import (
    WORKER_LOCK,
    WORKER_ID,
    WORKER_STATUS,
    WORKER_TASK_ID,
    WORKER_TASK_STATUS,
    update_worker_status,
    work_runner,
)

DRIVER_URI = None
WORKER_THREAD = None


worker_app = Flask(__name__)


@worker_app.route("/result", methods=["GET"])
def get_result():
    global WORKER_ID
    task_id = request.args.get("task_id")
    path = f"/tmp/{WORKER_ID}/{task_id}"
    try:
        with open(path, "r") as fd:
            return fd.read()
    except FileNotFoundError:
        return "Task not found", 404


@worker_app.route("/notify_task_done", methods=["POST"])
def notify():
    global WORKER_STATUS, WORKER_TASK_STATUS
    data = request.json
    print(f"Received notification: {data}")
    with WORKER_LOCK:
        WORKER_STATUS = WorkerStatus.IDLE
        WORKER_TASK_STATUS = TaskStatus(data["status"])
        WORKER_TASK_ID = data["task_id"]
        print(
            f"Worker {WORKER_ID} is now {WORKER_STATUS.name} with task {WORKER_TASK_ID} :: {WORKER_TASK_STATUS.name}"
        )
    return "Notification received"


@worker_app.route("/assign_task", methods=["POST"])
def assign_work():
    global WORKER_STATUS, WORKER_TASK_STATUS, WORKER_TASK_ID, WORKER_THREAD
    data = request.json
    data["worker_uri"] = WORKER_URI
    data["worker_id"] = WORKER_ID
    data["driver_uri"] = DRIVER_URI
    print(f"Received work: {data}")
    with WORKER_LOCK:
        if WORKER_STATUS == WorkerStatus.BUSY:
            return "Worker is busy", 400
        if WORKER_THREAD:
            WORKER_THREAD.join(timeout=1)
        WORKER_STATUS = WorkerStatus.BUSY
        WORKER_TASK_ID = data["task_id"]
        WORKER_TASK_STATUS = TaskStatus.IN_PROGRESS
        print(
            f"Worker {WORKER_ID} is now {WORKER_STATUS.name} with task {WORKER_TASK_ID} :: {WORKER_TASK_STATUS.name}"
        )
        worker_thread = Process(target=work_runner, args=(data,))
        worker_thread.daemon = False
        worker_thread.start()
        WORKER_THREAD = worker_thread
    return "Work assigned"


def send_heartbeat():
    global WORKER_ID, WORKER_STATUS, WORKER_TASK_ID, WORKER_TASK_STATUS
    with WORKER_LOCK:
        payload = {
            "wid": WORKER_ID,
            "status": WORKER_STATUS.name,
            "task_id": WORKER_TASK_ID,
            "task_status": WORKER_TASK_STATUS and WORKER_TASK_STATUS.name,
            "last_heartbeat": datetime.now().timestamp(),
        }
        requests.post(f"{DRIVER_URI}/heartbeat", json=payload)


def register_worker(port: int):
    global WORKER_ID, WORKER_URI
    WORKER_URI = f"http://127.0.0.1:{port}"
    payload = {"uri": WORKER_URI}
    response = requests.post(f"{DRIVER_URI}/register_worker", json=payload)
    WORKER_ID = response.text
    return WORKER_ID


def schedule_work():
    schedule.every(1).seconds.do(send_heartbeat)
    while True:
        schedule.run_pending()
        time.sleep(0.2)


def start_worker(driver_uri: str, port: int):
    global DRIVER_URI
    DRIVER_URI = driver_uri
    register_worker(port)
    scheduler_thread = Thread(target=schedule_work)
    scheduler_thread.start()
    worker_app.run(port=port)
