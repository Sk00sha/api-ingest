import json
import logging
from api_ingest.app_context import AppContext, HistoryLoadConfig, Endpoint, AuthConfig, NotificationContext
from api_ingest.api_client import ApiDataClient
from api_ingest.snow_client import ServiceNowClient
from api_ingest.conf import ConfigLoader

logging.basicConfig(level=logging.INFO)


class Runner:
    def __init__(self, path):
        self.path = path

    def run(self):
        conf = ConfigLoader()
        try:
            ctx = conf.load_app_context(self.path)
            api_client = ApiDataClient(ctx)
            data = api_client.fetch_data()
            print(f"Fetched {len(data)} records in total.")
        except Exception as e:
            logging.error(f"Job failed: {e}")
            sn_ctx = NotificationContext(
                base_url="https://instance.service-now.com",
                username="api_user",
                password="secret"
            )
        # sn_client = ServiceNowClient(sn_ctx)
        """
        sn_client.send_incident(
            short_description="API Ingestion Job Failed",
            description=str(e)
            )
            """
