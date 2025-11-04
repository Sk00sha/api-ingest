import json
import os
import logging
from requests import HTTPError

from .base_client import BaseApiClient
from .app_context import AppContext, Endpoint

logger = logging.getLogger(__name__)


class ApiDataClient(BaseApiClient):
    def __init__(self, ctx: AppContext, output_dir: str = "data"):
        super().__init__()
        self.ctx = ctx
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _build_params(self, endpoint: Endpoint):
        """Merge params + history load if enabled"""
        base_params = endpoint.params or {}
        if self.ctx.history_load.enabled:
            base_params |= {
                "start": self.ctx.history_load.start_date,
                "end": self.ctx.history_load.end_date,
            }
        return base_params

    def _extract_items(self, response):
        """
        Handle different API response shapes gracefully.
        """
        if response is None:
            return []

        # If response is already a list → assume it’s the data
        if isinstance(response, list):
            return response

        # If it's a dict with a known data key
        if isinstance(response, dict):
            for key in ("results", "data", "items", "records"):
                if key in response and isinstance(response[key], list):
                    return response[key]
            # If dict but no known key, treat whole dict as one record
            return [response]

        # If it's a string → try to parse JSON
        if isinstance(response, str):
            try:
                parsed = json.loads(response)
                return self._extract_items(parsed)
            except json.JSONDecodeError:
                logger.warning("Non-JSON response received, returning as raw text.")
                return [{"raw_response": response}]

        # Unexpected type
        logger.warning(f"Unexpected response type: {type(response)}")
        return [{"raw_response": str(response)}]

    def fetch_data(self):
        """Fetch data for each endpoint and save each as JSON file"""
        logger.info(f"Fetching data for {len(self.ctx.endpoints)} endpoints")
        all_items = []
        for ep in self.ctx.endpoints:
            endpoint_name = ep.endpoint_name
            url = f"{self.ctx.url}{endpoint_name}"
            output_path = os.path.join(self.output_dir, f"{endpoint_name}.json")
            page = 1

            logger.info(f"Fetching data for endpoint: {endpoint_name} ({url})")

            while page<2:
                try:
                    params = {**self._build_params(ep), "page": page}
                    auth_tuple = (
                        (ep.auth.username, ep.auth.password) if ep.auth else None
                    )
                    response = self.get(url, params=params, headers=ep.headers, auth=auth_tuple)

                except HTTPError as e:
                    if e.response.status_code == 404:
                        logger.warning(f"404 on {url} page {page} — stopping pagination.")
                        break
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error for {url}: {e}")
                    break

                items = self._extract_items(response)
                if not items:
                    logger.info(f"No items returned for {endpoint_name}, stopping pagination.")
                    break

                all_items.extend(items)
                page += 1

        return all_items
