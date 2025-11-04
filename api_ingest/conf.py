import json
from typing import List, Dict, Any
from pyspark.sql.types import *
from api_ingest.app_context import AppContext, AuthConfig, HistoryLoadConfig, Endpoint


class ConfigValidator:
    def validate_history_load(self, history_cfg: dict):
        """Validate history load configuration block."""
        enabled_value = self._to_bool(history_cfg.get("enabled"))

        if enabled_value:
            required_keys = ["start-date", "end-date"]
            invalid = [k for k in required_keys if not history_cfg.get(k)]
            if invalid:
                raise ValueError(
                    f"Invalid configuration: history load enabled but missing fields: {invalid}"
                )

    def _to_bool(self, value):
        """Convert various string representations to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "1", "yes", "y"):
                return True
            elif v in ("false", "0", "no", "n", ""):
                return False
        raise ValueError(f"Invalid boolean value: {value!r}")


class ConfigLoader:
    """
    Loads the config JSON into structured objects (AppContext, etc.)
    Supports multiple endpoints under one base URL.
    """
    def load_app_context(self, path: str) -> AppContext:
        with open(path, "r") as f:
            cfg = json.load(f)["ingest-souce-config"]
        endpoints = [
            Endpoint(
                endpoint_name=endpoint["endpoint-name"],
                params=endpoint.get("params"),
                headers=endpoint.get("headers"),
                auth=AuthConfig(**endpoint.get("auth", {}))
            )
            for endpoint in cfg.get("endpoints")
        ]
        return AppContext(
            app_name=cfg["app-name"],
            url=cfg["base-url"],
            history_load=HistoryLoadConfig(**cfg["history-load"]),
            endpoints=endpoints
        )

import json
from pyspark.sql.types import *


class SchemaParser:
    TYPE_MAP = {
        "string": StringType,
        "integer": IntegerType,
        "int": IntegerType,
        "long": LongType,
        "short": ShortType,
        "byte": ByteType,
        "float": FloatType,
        "double": DoubleType,
        "boolean": BooleanType,
        "binary": BinaryType,
        "date": DateType,
        "timestamp": TimestampType,
        "null": NullType,
        "daytimeinterval": DayTimeIntervalType,
        "yearmonthinterval": YearMonthIntervalType,
        "variant": VariantType,
    }

    @classmethod
    def load_schema(cls, schema_json):
        """
        Load schema from a JSON string or Python dict.
        """
        if isinstance(schema_json, str):
            schema_json = json.loads(schema_json)
        return StructType([cls._parse_field(f) for f in schema_json["fields"]])

    @classmethod
    def load_from_file(cls, file_path):
        """
        Load schema directly from a .json file path.
        """
        with open(file_path, "r") as f:
            content = f.read()
        return cls.load_schema(content)

    @classmethod
    def _parse_field(cls, field_obj):
        name = field_obj["name"]
        data_type = cls._parse_type(field_obj["type"])
        nullable = field_obj.get("nullable", True)
        return StructField(name, data_type, nullable)

    @classmethod
    def _parse_type(cls, field_type):
        if isinstance(field_type, str):
            return cls._parse_basic_type(field_type)

        type_name = field_type.get("type", "").lower()
        if type_name == "struct":
            return StructType([cls._parse_field(f) for f in field_type["fields"]])
        elif type_name == "array":
            return ArrayType(cls._parse_type(field_type["elementType"]), field_type.get("containsNull", True))
        elif type_name == "map":
            return MapType(
                cls._parse_type(field_type["keyType"]),
                cls._parse_type(field_type["valueType"]),
                field_type.get("valueContainsNull", True)
            )
        elif type_name == "decimal":
            return DecimalType(field_type.get("precision", 10), field_type.get("scale", 0))
        elif type_name in cls.TYPE_MAP:
            return cls.TYPE_MAP[type_name]()
        else:
            raise ValueError(f"Unsupported type: {field_type}")

    @classmethod
    def _parse_basic_type(cls, t):
        t = t.lower()
        if t not in cls.TYPE_MAP:
            raise ValueError(f"Unsupported type: {t}")
        return cls.TYPE_MAP[t]()

