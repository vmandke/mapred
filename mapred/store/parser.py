from abc import ABC, abstractmethod
from typing import Callable, List


class RecordParser(ABC):
    def __init__(self, record_delimiter: str, data_parser: Callable):
        self.record_delimiter = record_delimiter
        self.data_parser = data_parser

    @abstractmethod
    def get_records(self, buffer: str) -> List:
        pass


class JSONRecordParser(RecordParser):
    def get_records(self, buffer: str) -> List:
        if not buffer:
            return []
        return [
            self.data_parser(record) for record in buffer.split(self.record_delimiter)
        ]
