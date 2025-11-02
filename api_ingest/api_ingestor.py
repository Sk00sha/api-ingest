import json
from datetime import date
from api_ingest.api_client import MyApiClient
from api_ingest.app_context import AppContext


class ApiIngestor:
    def __init__(self, ctx: AppContext):
        self.api_client = MyApiClient(ctx)

    def run(self):
        today = date.today().isoformat()
        data = self.api_client.fetch_data(start_date=today, end_date=today)
        output_path = f"/Volumes/raw/my_api/date={today}/data.json"


        print(f"✅ Ingested {len(data)} records to {output_path}")
        return output_path
