import json
import os

from lib import submit_mapred_job_and_wait
from mapred.store.parser import JSONRecordParser
from mapred.store.file_fetcher import FileFetcher


def count_records():
    driver_uri = "http://127.0.0.1:5001"

    data_path = os.path.join(os.path.dirname(__file__), "data", "Books.jsonl")
    parser = JSONRecordParser(record_delimiter="\n", data_parser=json.loads)
    fetcher = FileFetcher(data_path=data_path, parser=parser)
    response = submit_mapred_job_and_wait(
        driver_uri=driver_uri,
        mapper="lambda x: 1",
        reducer="lambda x: [sum(x)]",
        fetcher=fetcher,
        m=100,
        r=20,
    )
    print(response)


if __name__ == "__main__":
    count_records()
