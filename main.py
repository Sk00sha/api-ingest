
from api_ingest.job_runner import Runner

from api_ingest.conf import SchemaParser
def main():

    SchemaParser.load_schema("config/schema.json")


if __name__ == "__main__":
    main()
