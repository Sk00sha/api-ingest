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