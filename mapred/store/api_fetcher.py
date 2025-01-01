from typing import Any, List
import requests

from mapred.store.fetcher import Fetcher


class APIFetcher(Fetcher):
    def get_chunks_metadata(self, num_chunks: int):
        pass

    def get_records_from_metadata(self, metadata: Any) -> List:
        return requests.get(metadata["uri"]).json()
