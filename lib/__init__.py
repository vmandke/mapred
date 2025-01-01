# This is intended as a SDK / API library for the project
import base64
import pickle
import time
from typing import Callable, Optional

import requests

from mapred.store.fetcher import Fetcher


def submit_mapred_job_and_wait(
    driver_uri: str,
    mapper: str,
    reducer: str,
    fetcher: Fetcher,
    m: Optional[int] = None,
    r: Optional[int] = None,
):
    post_data = {
        "mapper": base64.b64encode(pickle.dumps(mapper)).decode("utf-8"),
        "reducer": base64.b64encode(pickle.dumps(reducer)).decode("utf-8"),
        "fetcher": fetcher.pickle(),
        "M": m,
        "R": r,
    }
    response = requests.post(f"{driver_uri}/start_master_work", json=post_data)
    if response.status_code != 200:
        return response.json()

    work_id = response.json()["work_id"]
    while True:
        time.sleep(10)
        response = requests.get(f"{driver_uri}/driver_status")
        if response.json()["status"] == "IDLE":
            break

    response = requests.get(f"{driver_uri}/get_result?work_id={work_id}")
    return response.json()
