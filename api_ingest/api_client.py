from requests import HTTPError

from .base_client import BaseApiClient


class MyApiClient(BaseApiClient):
    def fetch_data(self):
        data = []
        for url in self.urls_to_ingest:
            page = 1
            while True:
                try:
                    response = self.get(url, params={**self.params, "page": page})
                except HTTPError as e:

                    if e.response.status_code == 404:
                        break
                    raise
                items = response.get("results", [])
                if not items:
                    break

                data.extend(items)
                page += 1
        return data

