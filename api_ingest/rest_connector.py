import json
from typing import Any

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

from api_ingest.app_context import AppContext

#TODO -> setup partitioning for parallel api calls
class RestApiPartition(InputPartition):
    def __init__(self, url, pid):
        self.url = url
        self.pid = pid

class RestApiReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options


    def read(self, partition: RestApiPartition):
        response = requests.get(self.options.get("url"), timeout=30)
        response.raise_for_status()
        data = response.json()

        # Normalize single dict/list into iterable
        records = data if isinstance(data, list) else [data]

        for record in records:
            yield (json.dumps(record),)


class RestApiDataSource(DataSource):

    @classmethod
    def name(cls):
        return "restapi"

    def schema(self):
        return StructType([StructField(self.options.get("schemaname"), StringType(), True)])

    def reader(self, schema):
        return RestApiReader(schema, self.options)

class RestConnector:
    def __init__(self,ctx: AppContext):
        self.url = ctx.url

    def get_data_source_reader(self,spark:Any):
        spark.dataSource.register(RestApiDataSource)
        return spark.read.format("restapi").option("url", self.url).option(
                "schemaname", "skema").load()

