import os
from typing import Any, List

from mapred.store.fetcher import Fetcher


class FileFetcher(Fetcher):
    def get_chunks_metadata(self, num_chunks: int):
        file_size = os.path.getsize(self.data_path)
        chunk_size = file_size // num_chunks
        covered_bytes = 0
        while covered_bytes < file_size:
            yield {
                "fetcher_pkl": self.pickle(),
                "metadata": {
                    "start": covered_bytes,
                    "end": min(covered_bytes + chunk_size, file_size),
                },
            }
            covered_bytes += chunk_size

    def find(self, file_descriptor: Any, start: int) -> int:
        file_descriptor.seek(start)
        file_size = os.path.getsize(self.data_path)
        if start == 0 or start > file_size:
            return min(start, file_size)
        while True:
            char = file_descriptor.read(1)
            if not char:
                break
            if char == self.parser.record_delimiter:
                break
        return file_descriptor.tell()

    def get_records_from_metadata(self, metadata: Any) -> List:
        with open(self.data_path, "r") as file_descriptor:
            chunk_start = self.find(file_descriptor, metadata["start"])
            chunk_end = self.find(file_descriptor, metadata["end"])
            file_descriptor.seek(chunk_start)
            buffer = file_descriptor.read(chunk_end - chunk_start)
            buffer = buffer.strip("\n")
            return self.parser.get_records(buffer)
