from .base_client import BaseApiClient


class MyApiClient(BaseApiClient):
    def fetch_data(self, start_date, end_date):
        data = []
        page = 1
        while True:
            response = self.get("records", params={"start": start_date, "end": end_date, "page": page})
            items = response.get("results", [])
            if not items:
                break
            data.extend(items)
            page += 1
        return data
