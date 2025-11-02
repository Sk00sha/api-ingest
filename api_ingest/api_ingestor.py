from api_ingest.api_client import MyApiClient
from api_ingest.app_context import AppContext
from api_ingest.loader import Loader

class ApiIngestor:
    def __init__(self, ctx: AppContext):
        self.api_client = MyApiClient(ctx)
        self.loader = Loader()

    def run(self):
        data = self.api_client.fetch_data()
        self.loader.save_json(data, "./output/fetched_data.json")
        print(f"✅ Ingested {len(data)} records to ./output/fetched_data.json")
