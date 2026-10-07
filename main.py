import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional, Set

import httpx
import websockets
from fastapi import FastAPI, HTTPException

REAL_BASE = "https://openapi.koreainvestment.com:9443"
DEMO_BASE = "https://openapivts.koreainvestment.com:29443"
REAL_WS = "ws://ops.koreainvestment.com:21000"
DEMO_WS = "ws://ops.koreainvestment.com:31000"
TOKEN_PATH = "/oauth2/tokenP"
APPROVAL_PATH = "/oauth2/Approval"
PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
PRICE_TR = "FHKST01010100"
WS_TRADE_TR = "H0UNCNT0"


def now() -> float:
    return time.time()


class KISClient:
    def __init__(self):
        self.key = os.getenv("KIS_APP_KEY", "").strip()
        self.secret = os.getenv("KIS_APP_SECRET", "").strip()
        self.demo = os.getenv("KIS_DEMO", "false").lower() == "true"
        self.base = DEMO_BASE if self.demo else REAL_BASE
        self.ws_url = DEMO_WS if self.demo else REAL_WS
        self.token: Optional[str] = None
        self.token_at = 0.0
        self.token_ttl = 20 * 3600
        self.auth_ok_at: Optional[float] = None
        self.last_auth_error: Optional[str] = None

    @property
    def configured(self) -> bool:
        return bool(self.key and self.secret)

    async def auth(self) -> dict:
        if not self.configured:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET not configured")
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.key,
            "appsecret": self.secret,
        }
        try:
            async with httpx.AsyncClient(timeout=15.0) as c:
                r = await c.post(
                    self.base + TOKEN_PATH,
                    headers={"content-type": "application/json"},
                    json=payload,
                )
                r.raise_for_status()
                j = r.json()
            token = j.get("access_token")
            if not token:
                raise RuntimeError(f"KIS token response missing access_token: {j.get('msg1') or j.get('msg_cd') or 'unknown'}")
            self.token = token
            self.token_at = now()
            self.auth_ok_at = self.token_at
            self.last_auth_error = None
            return {
                "ok": True,
                "environment": "demo" if self.demo else "real",
                "token_type": j.get("token_type", "Bearer"),
                "issued_at": self.token_at,
            }
        except Exception as e:
            self.last_auth_error = type(e).__name__
            raise

    async def ensure_token(self) -> None:
        if not self.token or now() - self.token_at > self.token_ttl:
            await self.auth()

    async def price(self, symbol: str) -> dict:
        await self.ensure_token()
        headers = {
            "authorization": f"Bearer {self.token}",
            "appkey": self.key,
            "appsecret": self.secret,
            "tr_id": PRICE_TR,
            "custtype": "P",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(self.base + PRICE_PATH, headers=headers, params=params)
            r.raise_for_status()
            j = r.json()
        if j.get("rt_cd") != "0":
            if j.get("msg_cd") == "EGW00123":
                self.token = None
            raise RuntimeError(j.get("msg1") or j.get("msg_cd") or "KIS API error")
        o = j.get("output") or {}
        return {
            "symbol": symbol,
            "price": o.get("stck_prpr"),
            "change_rate": o.get("prdy_ctrt"),
            "volume": o.get("acml_vol"),
            "trading_value": o.get("acml_tr_pbmn"),
            "open": o.get("stck_oprc"),
            "high": o.get("stck_hgpr"),
            "low": o.get("stck_lwpr"),
            "source": "KIS_REST",
            "fetched_at": now(),
        }

    async def approval_key(self) -> str:
        if not self.configured:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET not configured")
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.key,
            "secretkey": self.secret,
        }
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(
                self.base + APPROVAL_PATH,
                headers={"content-type": "application/json"},
                json=payload,
            )
            r.raise_for_status()
            j = r.json()
        key = j.get("approval_key")
        if not key:
            raise RuntimeError("KIS WebSocket approval_key missing")
        return key


class RealtimeManager:
    def __init__(self, kis: KISClient):
        self.kis = kis
        default_symbols = os.getenv("KIS_WS_SYMBOLS", "005930")
        self.symbols: Set[str] = {s.strip() for s in default_symbols.split(",") if s.strip()}
        self.latest: Dict[str, dict] = {}
        self.connected = False
        self.last_connected_at: Optional[float] = None
        self.last_message_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.task: Optional[asyncio.Task] = None
        self._stop = False
        self._resubscribe = asyncio.Event()

    def validate_symbol(self, symbol: str) -> None:
        if len(symbol) != 6 or not symbol.isdigit():
            raise ValueError("6-digit stock code required")

    async def add(self, symbol: str) -> None:
        self.validate_symbol(symbol)
        self.symbols.add(symbol)
        self._resubscribe.set()

    async def remove(self, symbol: str) -> None:
        self.symbols.discard(symbol)
        self.latest.pop(symbol, None)
        self._resubscribe.set()

    async def start(self):
        if not self.task:
            self.task = asyncio.create_task(self._run(), name="kis-realtime")

    async def stop(self):
        self._stop = True
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except BaseException:
                pass

    def status(self) -> dict:
        return {
            "configured": self.kis.configured,
            "connected": self.connected,
            "last_connected_at": self.last_connected_at,
            "last_message_at": self.last_message_at,
            "last_error": self.last_error,
            "symbols": sorted(self.symbols),
            "cached_symbols": sorted(self.latest.keys()),
        }

    def _subscription(self, approval_key: str, symbol: str) -> str:
        return json.dumps({
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": WS_TRADE_TR, "tr_key": symbol}},
        }, ensure_ascii=False)

    def _parse_trade_frame(self, msg: str) -> None:
        if not msg.startswith("0|"):
            return
        parts = msg.split("|", 3)
        if len(parts) < 4 or parts[1] != WS_TRADE_TR:
            return
        count = int(parts[2]) if parts[2].isdigit() else 1
        fields = parts[3].split("^")
        width = 46
        for i in range(max(count, 1)):
            row = fields[i * width:(i + 1) * width]
            if len(row) < 15:
                continue
            symbol = row[0]
            if len(symbol) != 6 or not symbol.isdigit() or symbol not in self.symbols:
    continue


            
            data = {
                "symbol": symbol,
                "trade_time": row[1],
                "price": row[2],
                "change_rate": row[5],
                "trade_volume": row[12],
                "cum_volume": row[13],
                "cum_trading_value": row[14],
                "source": "KIS_WEBSOCKET",
                "received_at": now(),
            }
            self.latest[symbol] = data
            self.last_message_at = data["received_at"]

    async def _run(self):
        backoff = 1
        while not self._stop:
            if not self.kis.configured:
                self.connected = False
                self.last_error = "credentials_not_configured"
                await asyncio.sleep(10)
                continue
            try:
                approval = await self.kis.approval_key()
                async with websockets.connect(
                    self.kis.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=2**20,
                ) as ws:
                    self.connected = True
                    self.last_connected_at = now()
                    self.last_error = None
                    backoff = 1
                    for symbol in sorted(self.symbols):
                        await ws.send(self._subscription(approval, symbol))

                    while not self._stop:
                        recv_task = asyncio.create_task(ws.recv())
                        resub_task = asyncio.create_task(self._resubscribe.wait())
                        done, pending = await asyncio.wait(
                            {recv_task, resub_task}, return_when=asyncio.FIRST_COMPLETED
                        )
                        for p in pending:
                            p.cancel()
                        if resub_task in done and self._resubscribe.is_set():
                            self._resubscribe.clear()
                            await ws.close()
                            break
                        if recv_task in done:
                            msg = recv_task.result()
                            if isinstance(msg, bytes):
                                msg = msg.decode("utf-8", errors="ignore")
                            if msg == "PINGPONG":
                                await ws.send(msg)
                                continue
                            if msg.startswith("{"):
                                try:
                                    j = json.loads(msg)
                                    if j.get("header", {}).get("tr_id") == "PINGPONG":
                                        await ws.send(msg)
                                except Exception:
                                    pass
                                continue
                            self._parse_trade_frame(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                self.last_error = f"{type(e).__name__}: {str(e)[:180]}"
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                self.connected = False


kis = KISClient()
rt = RealtimeManager(kis)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await rt.start()
    yield
    await rt.stop()


app = FastAPI(title="KIS-RGI Cloud Bridge", version="2.0.0", lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "service": "KIS-RGI Cloud Bridge",
        "version": "2.0.0",
        "configured": kis.configured,
        "environment": "demo" if kis.demo else "real",
        "realtime": rt.status(),
        "next": "/kis/smoke",
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "configured": kis.configured,
        "environment": "demo" if kis.demo else "real",
        "realtime_connected": rt.connected,
    }


@app.get("/ready")
async def ready():
    if not kis.configured:
        raise HTTPException(503, "KIS credentials not configured")
    return {"ok": True, "configured": True}


@app.post("/kis/auth")
async def auth():
    try:
        return await kis.auth()
    except Exception as e:
        raise HTTPException(502, f"KIS auth failed: {type(e).__name__}")


@app.get("/kis/price/{symbol}")
async def price(symbol: str):
    if len(symbol) != 6 or not symbol.isdigit():
        raise HTTPException(400, "6-digit stock code required")
    try:
        return await kis.price(symbol)
    except Exception as e:
        raise HTTPException(502, f"KIS price failed: {type(e).__name__}: {str(e)[:160]}")


@app.get("/kis/smoke")
async def smoke(symbol: str = "005930"):
    if len(symbol) != 6 or not symbol.isdigit():
        raise HTTPException(400, "6-digit stock code required")
    try:
        a = await kis.auth()
        p = await kis.price(symbol)
        return {"ok": True, "auth": a, "price": p, "kis_rest_verified": True}
    except Exception as e:
        raise HTTPException(502, f"KIS smoke failed: {type(e).__name__}: {str(e)[:160]}")


@app.get("/kis/realtime/status")
async def realtime_status():
    return rt.status()


@app.get("/kis/realtime/{symbol}")
async def realtime(symbol: str):
    try:
        rt.validate_symbol(symbol)
    except ValueError as e:
        raise HTTPException(400, str(e))
    data = rt.latest.get(symbol)
    if not data:
        raise HTTPException(404, "No realtime trade cached yet; subscribe and wait for market data")
    return data


@app.post("/kis/realtime/subscribe/{symbol}")
async def realtime_subscribe(symbol: str):
    try:
        await rt.add(symbol)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "symbol": symbol, "symbols": sorted(rt.symbols)}


@app.delete("/kis/realtime/subscribe/{symbol}")
async def realtime_unsubscribe(symbol: str):
    await rt.remove(symbol)
    return {"ok": True, "symbol": symbol, "symbols": sorted(rt.symbols)}


@app.get("/rgi/status")
async def rgi_status():
    return {
        "rgi": "v.261007_13",
        "rgis": "v.261007_13",
        "bridge_version": "2.0.0",
        "bridge_configured": kis.configured,
        "kis_rest_verified_at": kis.auth_ok_at,
        "kis_ws_connected": rt.connected,
        "production": "TEMPORARY/CONDITIONAL — REVALIDATION REQUIRED",
        "scope": "read-only market-data bridge; no trading/order endpoints",
    }
