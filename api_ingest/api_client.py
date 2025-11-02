import logging

from requests import HTTPError

from .base_client import BaseApiClient

logger = logging.getLogger(__name__)
class MyApiClient(BaseApiClient):
    def fetch_data(self, url: str):
        data = []
        logger.info(f"Starting fetch for {url}")
        page = 1
        while True:
            try:
                response = self.get(url, params={**self.params, "page": page})
                logger.info(f"response code: {response.status_code} for url: {url}")
            except HTTPError as e:
                if e.response.status_code == 404:
                    break
                raise
            except Exception as e:
                break

            items = response.get("results", [])
            if not items:
                print(f"  No items returned for {url}, stopping pagination.")
                break

            data.extend(items)
            page += 1

        return data

