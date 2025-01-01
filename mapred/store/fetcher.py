import base64
from abc import ABC, abstractmethod
from typing import Any, Optional
import pickle

from mapred.store.parser import RecordParser


class Fetcher(ABC):
    def __init__(self, data_path: Optional[str], parser: Optional[RecordParser]):
        self.data_path = data_path
        self.parser = parser

    @abstractmethod
    def get_chunks_metadata(self, num_chunks: int):
        pass

    @abstractmethod
    def get_records_from_metadata(self, chunk_metadata: Any):
        pass

    def pickle(self):
        return base64.b64encode(pickle.dumps(self)).decode("utf-8")


def get_records(chunk_metadata: Any):
    fetcher_obj = pickle.loads(chunk_metadata["fetcher_pkl"])
    return fetcher_obj.get_records_from_metadata(chunk_metadata["metadata"])
