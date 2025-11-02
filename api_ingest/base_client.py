import requests
from requests.adapters import HTTPAdapter, Retry

from api_ingest.app_context import AppContext


class BaseApiClient:
    def __init__(self, ctx: AppContext):
        self.base_url = ctx.url
        self.session = requests.Session()
        retries = Retry(total=1, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.headers = ctx.headers or {}

    def get(self, endpoint: str, params=None, auth=None):
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, headers=self.headers, params=params, auth=auth)
        response.raise_for_status()
        return response.json()
