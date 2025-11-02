from api_ingest.config_loader import ConfigLoader
from api_ingest.api_client import MyApiClient
from api_ingest.api_ingestor import ApiIngestor
from api_ingest.loader import Loader


def main():
    config = ConfigLoader()
    ingestor = ApiIngestor(config.get_app_context())
    ingestor.run("./output/")


if __name__ == "__main__":
    main()
