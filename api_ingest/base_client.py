import requests
from requests.adapters import HTTPAdapter, Retry

from api_ingest.app_context import AppContext


class BaseApiClient:
    def __init__(self, ctx: AppContext):
        self.session = requests.Session()
        retries = Retry(total=1, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.headers = ctx.headers or {}
        self.params = self._set_up_api_params(ctx)
        self.endpoint = ctx.endpoints
        self.base_url = ctx.url

    def _set_up_api_params(self, ctx: AppContext):
        return self._validate_history_load_into_params(ctx.history_load.enabled, ctx.history_load.start_date,
                                                       ctx.history_load.end_date) | ctx.params

    def _validate_history_load_into_params(self, enabled: bool, start_date: str = None, end_date: str = None):
        if enabled:
            return {"start": start_date, "end": end_date}
        else:
            return {}

    def get(self, url: str, params=None, auth=None):
        response = self.session.get(url, headers=self.headers, params=params, auth=auth)
        response.raise_for_status()
        return response.json()
