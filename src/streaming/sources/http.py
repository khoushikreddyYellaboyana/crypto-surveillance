"""Tiny HTTP helper for the exchanges' public REST APIs (stdlib only, no extra dependency)."""
from __future__ import annotations

import json
import urllib.request

UA = "crypto-surveillance-student-project/1.0"


def get_json(url: str, timeout: float = 10.0):
    """Returns (parsed_json, headers)."""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read()), r.headers
