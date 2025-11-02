import json
from pathlib import Path


class Loader:
    def __init__(self):
        pass

    def save_json(self, data, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
