import logging
from sqlite3 import connect
from typing import Any

from api_ingest.app_context import AppContext, HistoryLoadConfig, Endpoint, AuthConfig, NotificationContext
from api_ingest.api_client import ApiDataClient
from api_ingest.snow_client import ServiceNowClient
from api_ingest.conf import ConfigLoader
from api_ingest.rest_connector import RestConnector
logging.basicConfig(level=logging.INFO)


class Runner:
    def __init__(self, path:str):
        self.path = path

    def run(self,spark:Any):
        conf = ConfigLoader()
        try:
            ctx = conf.load_app_context(self.path)
            connector = RestConnector(ctx)
            df = connector.get_data_source_reader(spark)
            df.show()
            #api_client = ApiDataClient(ctx)
            #api_client.fetch_data()
        except Exception as e:
            logging.error(f"Job failed : {e}")
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
