
from api_ingest.job_runner import Runner


def main():
    runner = Runner("config/config.json")
    runner.run()


if __name__ == "__main__":
    main()
