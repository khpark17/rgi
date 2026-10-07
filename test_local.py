import os
os.environ.pop("KIS_APP_KEY", None)
os.environ.pop("KIS_APP_SECRET", None)
from fastapi.testclient import TestClient
from main import app

with TestClient(app) as c:
    assert c.get("/health").status_code == 200
    assert c.get("/health").json()["ok"] is True
    assert c.get("/ready").status_code == 503
    assert c.get("/rgi/status").status_code == 200
    assert c.get("/kis/price/123").status_code == 400
    assert c.get("/kis/realtime/123").status_code == 400
    assert c.post("/kis/realtime/subscribe/000660").status_code == 200
    assert "000660" in c.get("/kis/realtime/status").json()["symbols"]
print("CLOUD_BRIDGE_V2_TESTS_OK 8/8")
