from api_ingest.api_client import MyApiClient
from api_ingest.app_context import AppContext
from api_ingest.loader import Loader


class ApiIngestor:
    def __init__(self, ctx: AppContext):
        self.api_client = MyApiClient(ctx)
        self.loader = Loader()

    def run(self):
        for endpoint in self.api_client.endpoint:
            data = self.api_client.fetch_data(f"{self.api_client.base_url}{endpoint}")
            self.loader.save_json(data, f"./output/{endpoint}.json")
            print(f"✅ Ingested {len(data)} records to ./output/fetched_data.json")
