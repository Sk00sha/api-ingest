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
                logger.debug(f"Fetching {url} page={page}")
                response = self.get(url, params={**self.params, "page": page})
            except HTTPError as e:
                status = e.response.status_code if e.response else "Unknown"
                logger.warning(f"HTTPError {status} for {url} on page {page}")
                if status == 404:
                    logger.info(f"No more pages for {url}")
                    break
                raise
            except Exception as e:
                logger.error(f"Unexpected error fetching {url} page {page}: {e}")
                break

            items = response.get("results", [])
            if not items:
                logger.info(f"No items returned for {url} page {page}, stopping pagination.")
                break

            logger.debug(f"Fetched {len(items)} items from {url} page {page}")
            data.extend(items)
            page += 1

        logger.info(f"Finished fetch for {url}. Total items: {len(data)}")
        return data
