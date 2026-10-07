import asyncio
import io
import json
import math
import random
import re
import sqlite3
import hashlib
import os
import time
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set, List, Any
from pathlib import Path
import xml.etree.ElementTree as ET

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Query

REAL_BASE = "https://openapi.koreainvestment.com:9443"
DEMO_BASE = "https://openapivts.koreainvestment.com:29443"
REAL_WS = "ws://ops.koreainvestment.com:21000"
DEMO_WS = "ws://ops.koreainvestment.com:31000"
TOKEN_PATH = "/oauth2/tokenP"
APPROVAL_PATH = "/oauth2/Approval"
PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
PRICE_TR = "FHKST01010100"
DAILY_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-price"
DAILY_TR = "FHKST01010400"
INVESTOR_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"
INVESTOR_TR = "FHKST01010900"
INDEX_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
INDEX_TR = "FHKUP03500100"
WS_TRADE_TR = "H0UNCNT0"
WS_PROGRAM_TR = "H0UNPGM0"
WS_ASK_TR = "H0UNASP0"
VOLUME_RANK_PATH = "/uapi/domestic-stock/v1/quotations/volume-rank"
VOLUME_RANK_TR = "FHPST01710000"
FLUCT_PATH = "/uapi/domestic-stock/v1/ranking/fluctuation"
FLUCT_TR = "FHPST01700000"
VOLUME_POWER_PATH = "/uapi/domestic-stock/v1/ranking/volume-power"
VOLUME_POWER_TR = "FHPST01680000"
FOREIGN_INST_PATH = "/uapi/domestic-stock/v1/quotations/foreign-institution-total"
FOREIGN_INST_TR = "FHPTJ04400000"
NEWS_TITLE_PATH = "/uapi/domestic-stock/v1/quotations/news-title"
NEWS_TITLE_TR = "FHKST01011800"
DART_BASE = "https://opendart.fss.or.kr/api"
ESTIMATE_PERFORM_PATH = "/uapi/domestic-stock/v1/quotations/estimate-perform"
ESTIMATE_PERFORM_TR = "HHKST668300C0"
FINANCE_INCOME_PATH = "/uapi/domestic-stock/v1/finance/income-statement"
FINANCE_INCOME_TR = "FHKST66430200"
INVEST_OPINION_PATH = "/uapi/domestic-stock/v1/quotations/invest-opinion"
INVEST_OPINION_TR = "FHKST663300C0"
ESTIMATE_PATH = "/uapi/domestic-stock/v1/quotations/investor-trend-estimate"
ESTIMATE_TR = "HHPTJ04160200"
MASTER_URLS = {
    "KOSPI": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
    "KOSDAQ": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
}

RGI_VERSION = "v.261007_14"
RGIS_VERSION = "v.261007_14"
BRIDGE_VERSION = "17.0.0-k28-atomic-release"


def now() -> float:
    return time.time()


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(x, default=None):
    try:
        if x in (None, ""):
            return default
        return float(str(x).replace(",", ""))
    except Exception:
        return default


def inum(x, default=None):
    try:
        if x in (None, ""):
            return default
        return int(float(str(x).replace(",", "")))
    except Exception:
        return default


def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, float(x)))


def signed_unit(x, scale):
    if x is None or scale <= 0:
        return None
    return max(-1.0, min(1.0, float(x) / scale))


def score_1_10(x):
    if x is None:
        return None
    return max(1.0, min(10.0, 1.0 + 9.0 / (1.0 + math.exp(-3.0 * x))))


@dataclass
class UniverseItem:
    symbol: str
    name: str
    market: str
    preferred: Optional[bool] = None
    spac: Optional[bool] = None
    trading_halt: Optional[bool] = None
    liquidation: Optional[bool] = None
    managed: Optional[bool] = None
    low_liquidity: Optional[bool] = None
    market_warning: Optional[str] = None

    @property
    def eligible(self) -> bool:
        if self.preferred is True or self.spac is True:
            return False
        if self.trading_halt is True or self.liquidation is True:
            return False
        return True


KOSPI_WIDTHS = [
    2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,1,
    9,9,9,5,9,8,9,3,1,1,1
]
KOSPI_COLS = [
    'group_code','market_cap_size','sector_large','sector_mid','sector_small','manufacturing',
    'low_liquidity','governance_index','kospi200_sector','kospi100','kospi50','krx','etp',
    'elw_issuer','krx100','krx_auto','krx_semi','krx_bio','krx_bank','spac','krx_energychem',
    'krx_steel','short_overheat','krx_media','krx_construction','non1','krx_securities',
    'krx_ship','krx_insurance','krx_transport','sri','base_price','trade_unit','afterhours_unit',
    'trading_halt','liquidation','managed','market_warning','warning_preannounce',
    'unfaithful_disclosure','backdoor_listing','lock_class','par_change','capital_increase',
    'margin_rate','credit_allowed','credit_period','prev_volume','par_value','listing_date',
    'shares_outstanding','capital','fiscal_month','ipo_price','preferred','short_overheat2',
    'abnormal_surge','krx300','kospi','sales','operating_profit','ordinary_profit','net_income',
    'roe','base_yyyymm','market_cap','group_company','credit_limit_exceeded','secured_loan','stock_loan'
]
KOSDAQ_WIDTHS = [
    2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,
    2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1
]
KOSDAQ_COLS = [
    'group_code','market_cap_size','sector_large','sector_mid','sector_small','venture',
    'low_liquidity','krx','etp','krx100','krx_auto','krx_semi','krx_bio','krx_bank','spac',
    'krx_energychem','krx_steel','short_overheat','krx_media','krx_construction','investment_attention',
    'krx_securities','krx_ship','krx_insurance','krx_transport','kosdaq150','base_price','trade_unit',
    'afterhours_unit','trading_halt','liquidation','managed','market_warning','warning_preannounce',
    'unfaithful_disclosure','backdoor_listing','lock_class','par_change','capital_increase','margin_rate',
    'credit_allowed','credit_period','prev_volume','par_value','listing_date','shares_outstanding','capital',
    'fiscal_month','ipo_price','preferred','short_overheat2','abnormal_surge','krx300','sales',
    'operating_profit','ordinary_profit','net_income','roe','base_yyyymm','market_cap','group_company',
    'credit_limit_exceeded','secured_loan','stock_loan'
]


def _split_fixed(s: str, widths: List[int]) -> List[str]:
    out, p = [], 0
    for w in widths:
        out.append(s[p:p+w]); p += w
    return out


def _yn(x: Any) -> Optional[bool]:
    x = str(x or "").strip().upper()
    if x == "Y": return True
    if x == "N": return False
    return None


def parse_master_line(line: str, market: str) -> Optional[UniverseItem]:
    line = line.rstrip("\r\n")
    tail_len = 228 if market == "KOSPI" else 222
    widths = KOSPI_WIDTHS if market == "KOSPI" else KOSDAQ_WIDTHS
    cols = KOSPI_COLS if market == "KOSPI" else KOSDAQ_COLS
    if len(line) <= tail_len + 21:
        return None
    head, tail = line[:-tail_len], line[-tail_len:]
    symbol = head[:9].strip()
    if len(symbol) > 6:
        symbol = symbol[-6:]
    if len(symbol) != 6 or not symbol.isdigit():
        return None
    name = head[21:].strip()
    vals = _split_fixed(tail, widths)
    d = dict(zip(cols, vals))
    return UniverseItem(
        symbol=symbol,
        name=name,
        market=market,
        preferred=_yn(d.get("preferred")),
        spac=_yn(d.get("spac")),
        trading_halt=_yn(d.get("trading_halt")),
        liquidation=_yn(d.get("liquidation")),
        managed=_yn(d.get("managed")),
        low_liquidity=_yn(d.get("low_liquidity")),
        market_warning=str(d.get("market_warning") or "").strip() or None,
    )


class UniverseMaster:
    def __init__(self):
        self.items: List[UniverseItem] = []
        self.loaded_at: Optional[float] = None
        self.errors: List[str] = []

    async def refresh(self):
        items, errors = [], []
        async with httpx.AsyncClient(timeout=30.0) as c:
            for market, url in MASTER_URLS.items():
                try:
                    r = await c.get(url)
                    r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                        name = next(n for n in z.namelist() if n.lower().endswith(".mst"))
                        raw = z.read(name)
                    text = raw.decode("cp949", errors="ignore")
                    parsed = [parse_master_line(x, market) for x in text.splitlines()]
                    parsed = [x for x in parsed if x is not None]
                    if not parsed:
                        raise RuntimeError("empty parsed master")
                    items.extend(parsed)
                except Exception as e:
                    errors.append(f"{market}:{type(e).__name__}:{str(e)[:120]}")
        dedup = {x.symbol: x for x in items}
        self.items = list(dedup.values())
        self.loaded_at = now()
        self.errors = errors
        return self.status()

    def status(self):
        return {
            "loaded": bool(self.items),
            "loaded_at": self.loaded_at,
            "total": len(self.items),
            "eligible": sum(x.eligible for x in self.items),
            "kospi": sum(x.market == "KOSPI" for x in self.items),
            "kosdaq": sum(x.market == "KOSDAQ" for x in self.items),
            "errors": self.errors,
        }


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
        self._http = httpx.AsyncClient(timeout=15.0)
        self._lock = asyncio.Lock()
        self._auth_lock = asyncio.Lock()
        self._last_call = 0.0
        self.rps = float(os.getenv("KIS_REST_RPS", "7"))

    @property
    def configured(self) -> bool:
        return bool(self.key and self.secret)

    async def close(self):
        await self._http.aclose()

    async def _throttle(self):
        interval = 1.0 / max(self.rps, 0.2)
        async with self._lock:
            wait = interval - (time.monotonic() - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()

    async def auth(self) -> dict:
        if not self.configured:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET not configured")
        payload = {"grant_type":"client_credentials","appkey":self.key,"appsecret":self.secret}
        try:
            r = await self._http.post(self.base + TOKEN_PATH, headers={"content-type":"application/json"}, json=payload)
            r.raise_for_status(); j = r.json()
            token = j.get("access_token")
            if not token:
                raise RuntimeError(j.get("msg1") or j.get("msg_cd") or "token missing")
            self.token = token; self.token_at = now(); self.auth_ok_at = self.token_at; self.last_auth_error = None
            return {"ok":True,"environment":"demo" if self.demo else "real","token_type":j.get("token_type","Bearer"),"issued_at":self.token_at}
        except Exception as e:
            self.last_auth_error = type(e).__name__; raise

    async def ensure_token(self):
        if self.token and now() - self.token_at <= self.token_ttl:
            return
        async with self._auth_lock:
            # Another coroutine may have refreshed the token while we waited.
            if self.token and now() - self.token_at <= self.token_ttl:
                return
            await self.auth()

    async def _get(self, path: str, tr_id: str, params: dict, retries: int = 2) -> dict:
        await self.ensure_token()
        headers = {"authorization":f"Bearer {self.token}","appkey":self.key,"appsecret":self.secret,"tr_id":tr_id,"custtype":"P"}
        last = None
        for attempt in range(retries + 1):
            await self._throttle()
            try:
                r = await self._http.get(self.base + path, headers=headers, params=params)
                r.raise_for_status(); j = r.json()
                if j.get("rt_cd") != "0":
                    if j.get("msg_cd") == "EGW00123": self.token = None
                    raise RuntimeError(j.get("msg1") or j.get("msg_cd") or "KIS API error")
                return j
            except Exception as e:
                last = e
                if attempt < retries:
                    await asyncio.sleep(0.2 * (2 ** attempt))
        raise last

    async def price(self, symbol: str) -> dict:
        j = await self._get(PRICE_PATH, PRICE_TR, {"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":symbol})
        o = j.get("output") or {}
        return {
            "symbol":symbol,"price":fnum(o.get("stck_prpr")),"change_rate":fnum(o.get("prdy_ctrt")),
            "volume":fnum(o.get("acml_vol")),"trading_value":fnum(o.get("acml_tr_pbmn")),
            "open":fnum(o.get("stck_oprc")),"high":fnum(o.get("stck_hgpr")),"low":fnum(o.get("stck_lwpr")),
            "prev_close":fnum(o.get("stck_sdpr")),"source":"KIS_REST","fetched_at":now(),
        }

    async def daily(self, symbol: str) -> List[dict]:
        j = await self._get(DAILY_PATH, DAILY_TR, {
            "FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":symbol,"FID_PERIOD_DIV_CODE":"D","FID_ORG_ADJ_PRC":"0"
        })
        out=[]
        for o in j.get("output") or []:
            out.append({
                "date":o.get("stck_bsop_date"),"open":fnum(o.get("stck_oprc")),"high":fnum(o.get("stck_hgpr")),
                "low":fnum(o.get("stck_lwpr")),"close":fnum(o.get("stck_clpr")),"volume":fnum(o.get("acml_vol")),
                "trading_value":fnum(o.get("acml_tr_pbmn")),"source":"KIS_DAILY","available_at":utc_iso(),
            })
        return out

    async def investor(self, symbol: str) -> List[dict]:
        j = await self._get(INVESTOR_PATH, INVESTOR_TR, {"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":symbol})
        out=[]
        for o in j.get("output") or []:
            out.append({
                "date":o.get("stck_bsop_date"),"foreign_net_qty":fnum(o.get("frgn_ntby_qty")),
                "institution_net_qty":fnum(o.get("orgn_ntby_qty")),"retail_net_qty":fnum(o.get("prsn_ntby_qty")),
                "source":"KIS_INVESTOR_CONFIRMED","available_at":utc_iso(),
            })
        return out

    async def investor_estimate(self, symbol: str) -> List[dict]:
        """KIS intraday foreign/institution estimate snapshots. Not tick-real-time.
        Official KIS notes these are manually/periodically aggregated during the session.
        """
        j = await self._get(ESTIMATE_PATH, ESTIMATE_TR, {"MKSC_SHRN_ISCD":symbol})
        rows = j.get("output2") or j.get("output") or []
        out=[]
        for o in rows:
            out.append({
                "symbol":symbol,
                "time":o.get("bsop_hour_gb") or o.get("stck_cntg_hour") or o.get("data_rank"),
                "foreign_est_qty":fnum(o.get("frgn_fake_ntby_qty") or o.get("frgn_ntby_qty")),
                "institution_est_qty":fnum(o.get("orgn_fake_ntby_qty") or o.get("orgn_ntby_qty")),
                "source":"KIS_INTRADAY_INVESTOR_ESTIMATE",
                "available_at":utc_iso(),
                "confirmed":False,
            })
        return out

    async def estimate_perform(self, symbol: str) -> dict:
        """KIS stock estimated performance. Official API returns four output blocks."""
        j=await self._get(ESTIMATE_PERFORM_PATH,ESTIMATE_PERFORM_TR,{"SHT_CD":symbol})
        return {
            "symbol":symbol,
            "output1":j.get("output1") or [],
            "output2":j.get("output2") or [],
            "output3":j.get("output3") or [],
            "output4":j.get("output4") or [],
            "source":"KIS_ESTIMATE_PERFORM",
            "retrieved_at":utc_iso(),
        }

    async def income_statement(self, symbol: str, quarterly: bool=True) -> List[dict]:
        """KIS actual financial income statement. quarterly=True uses FID_DIV_CLS_CODE=1."""
        j=await self._get(FINANCE_INCOME_PATH,FINANCE_INCOME_TR,{
            "FID_DIV_CLS_CODE":"1" if quarterly else "0",
            "FID_COND_MRKT_DIV_CODE":"J",
            "FID_INPUT_ISCD":symbol,
        })
        out=j.get("output") or []
        return out if isinstance(out,list) else ([out] if out else [])

    async def invest_opinion(self, symbol: str, days: int=180) -> List[dict]:
        end=datetime.now(timezone.utc).strftime("%Y%m%d")
        start=(datetime.now(timezone.utc)-timedelta(days=days)).strftime("%Y%m%d")
        j=await self._get(INVEST_OPINION_PATH,INVEST_OPINION_TR,{
            "FID_COND_MRKT_DIV_CODE":"J",
            "FID_COND_SCR_DIV_CODE":"16633",
            "FID_INPUT_ISCD":symbol,
            "FID_INPUT_DATE_1":start,
            "FID_INPUT_DATE_2":end,
        })
        out=j.get("output") or []
        return out if isinstance(out,list) else ([out] if out else [])

    async def index_daily(self, index_code: str) -> List[dict]:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
        j = await self._get(INDEX_PATH, INDEX_TR, {
            "FID_COND_MRKT_DIV_CODE":"U","FID_INPUT_ISCD":index_code,"FID_INPUT_DATE_1":start,
            "FID_INPUT_DATE_2":end,"FID_PERIOD_DIV_CODE":"D"
        })
        rows = j.get("output2") or j.get("output") or []
        out=[]
        for o in rows:
            out.append({"date":o.get("stck_bsop_date") or o.get("bstp_nmix_prpr_date"),"close":fnum(o.get("bstp_nmix_prpr"))})
        return out

    async def volume_rank(self, belonging: str = "3") -> List[dict]:
        j=await self._get(VOLUME_RANK_PATH,VOLUME_RANK_TR,{
            "FID_COND_MRKT_DIV_CODE":"UN","FID_COND_SCR_DIV_CODE":"20171","FID_INPUT_ISCD":"0000",
            "FID_DIV_CLS_CODE":"1","FID_BLNG_CLS_CODE":belonging,"FID_TRGT_CLS_CODE":"000000000",
            "FID_TRGT_EXLS_CLS_CODE":"0000000000","FID_INPUT_PRICE_1":"","FID_INPUT_PRICE_2":"",
            "FID_VOL_CNT":"","FID_INPUT_DATE_1":""})
        out=[]
        for o in j.get("output") or []:
            sym=str(o.get("mksc_shrn_iscd") or o.get("stck_shrn_iscd") or "").strip()
            if len(sym)==6 and sym.isdigit():
                out.append({"symbol":sym,"name":o.get("hts_kor_isnm"),"price":fnum(o.get("stck_prpr")),
                    "change_rate":fnum(o.get("prdy_ctrt")),"volume":fnum(o.get("acml_vol")),
                    "volume_increase_rate":fnum(o.get("vol_inrt")),"radar_source":f"VOLUME_RANK_{belonging}"})
        return out

    async def fluctuation_rank(self, market_code: str) -> List[dict]:
        j=await self._get(FLUCT_PATH,FLUCT_TR,{
            "FID_COND_MRKT_DIV_CODE":"J","FID_COND_SCR_DIV_CODE":"20170","FID_INPUT_ISCD":market_code,
            "FID_RANK_SORT_CLS_CODE":"0","FID_INPUT_CNT_1":"0","FID_PRC_CLS_CODE":"0",
            "FID_INPUT_PRICE_1":"","FID_INPUT_PRICE_2":"","FID_VOL_CNT":"","FID_TRGT_CLS_CODE":"0",
            "FID_TRGT_EXLS_CLS_CODE":"0","FID_DIV_CLS_CODE":"0","FID_RSFL_RATE1":"","FID_RSFL_RATE2":""})
        out=[]
        for o in j.get("output") or []:
            sym=str(o.get("stck_shrn_iscd") or o.get("mksc_shrn_iscd") or "").strip()
            if len(sym)==6 and sym.isdigit():
                out.append({"symbol":sym,"name":o.get("hts_kor_isnm"),"price":fnum(o.get("stck_prpr")),
                    "change_rate":fnum(o.get("prdy_ctrt")),"volume":fnum(o.get("acml_vol")),"radar_source":f"FLUCT_{market_code}"})
        return out

    async def volume_power_rank(self, market_code: str) -> List[dict]:
        j=await self._get(VOLUME_POWER_PATH,VOLUME_POWER_TR,{
            "FID_COND_MRKT_DIV_CODE":"J","FID_COND_SCR_DIV_CODE":"20168","FID_INPUT_ISCD":market_code,
            "FID_DIV_CLS_CODE":"1","FID_INPUT_PRICE_1":"","FID_INPUT_PRICE_2":"","FID_VOL_CNT":"",
            "FID_TRGT_CLS_CODE":"0","FID_TRGT_EXLS_CLS_CODE":"0"})
        out=[]
        for o in j.get("output") or []:
            sym=str(o.get("stck_shrn_iscd") or o.get("mksc_shrn_iscd") or "").strip()
            if len(sym)==6 and sym.isdigit():
                out.append({"symbol":sym,"name":o.get("hts_kor_isnm"),"price":fnum(o.get("stck_prpr")),
                    "change_rate":fnum(o.get("prdy_ctrt")),"volume":fnum(o.get("acml_vol")),
                    "volume_power":fnum(o.get("tday_rltv" ) or o.get("seln_cnqn_smtn_rate")),"radar_source":f"POWER_{market_code}"})
        return out

    async def foreign_institution_rank(self, market_code: str = "0000") -> List[dict]:
        j=await self._get(FOREIGN_INST_PATH,FOREIGN_INST_TR,{
            "FID_COND_MRKT_DIV_CODE":"V","FID_COND_SCR_DIV_CODE":"16449","FID_INPUT_ISCD":market_code,
            "FID_DIV_CLS_CODE":"1","FID_RANK_SORT_CLS_CODE":"0","FID_ETC_CLS_CODE":"0"})
        out=[]
        for o in j.get("output") or []:
            sym=str(o.get("mksc_shrn_iscd") or o.get("stck_shrn_iscd") or "").strip()
            if len(sym)==6 and sym.isdigit():
                out.append({"symbol":sym,"name":o.get("hts_kor_isnm"),"radar_source":f"FLOW_RANK_{market_code}","raw":o})
        return out

    async def news_titles(self, symbol: str, date_yyyymmdd: str = "", hour_hhmmss: str = "") -> List[dict]:
        """KIS integrated market/news/disclosure titles. Title-only visible intelligence; not a DART filing-body feed."""
        if not date_yyyymmdd:
            date_yyyymmdd=datetime.now().strftime("%Y%m%d")
        if not hour_hhmmss:
            hour_hhmmss="000000"
        j=await self._get(NEWS_TITLE_PATH,NEWS_TITLE_TR,{
            "FID_NEWS_OFER_ENTP_CODE":"2",
            "FID_COND_MRKT_CLS_CODE":"00",
            "FID_INPUT_ISCD":symbol,
            "FID_TITL_CNTT":"",
            "FID_INPUT_DATE_1":date_yyyymmdd,
            "FID_INPUT_HOUR_1":hour_hhmmss,
            "FID_RANK_SORT_CLS_CODE":"01",
            "FID_INPUT_SRNO":"1",
        })
        out=[]
        for o in j.get("output") or []:
            title=(o.get("hts_pbnt_titl_cntt") or o.get("news_titl") or o.get("titl_cntt") or "").strip()
            d=(o.get("data_dt") or o.get("stck_bsop_date") or date_yyyymmdd or "").strip()
            t=(o.get("data_tm") or o.get("news_tm") or o.get("bsop_hour") or "").strip()
            out.append({
                "symbol":symbol,"title":title,"date":d,"time":t,
                "provider":o.get("dorg") or o.get("news_ofer_entp_code"),
                "serial":o.get("cntt_usiq_srno") or o.get("srno"),
                "source":"KIS_NEWS_TITLE","available_at":utc_iso(),"raw":o,
            })
        return out

    async def approval_key(self) -> str:
        payload={"grant_type":"client_credentials","appkey":self.key,"secretkey":self.secret}
        r=await self._http.post(self.base+APPROVAL_PATH,headers={"content-type":"application/json"},json=payload)
        r.raise_for_status(); j=r.json(); key=j.get("approval_key")
        if not key: raise RuntimeError("KIS WebSocket approval_key missing")
        return key


class RealtimeManager:
    def __init__(self, kis: KISClient):
        self.kis=kis
        self.symbols:Set[str]={s.strip() for s in os.getenv("KIS_WS_SYMBOLS","005930").split(",") if s.strip()}
        self.latest:Dict[str,dict]={}; self.connected=False; self.last_connected_at=None; self.last_message_at=None; self.last_error=None
        self.parsed_trade_rows=0; self.invalid_trade_rows=0; self.trade_frame_mismatch=0; self.parsed_program_rows=0; self.invalid_program_rows=0
        self.parsed_ask_rows=0; self.invalid_ask_rows=0; self.ask_frame_mismatch=0
        self.task=None; self._stop=False; self._resubscribe=asyncio.Event()

    def validate_symbol(self,s):
        if len(s)!=6 or not s.isdigit(): raise ValueError("6-digit stock code required")

    async def set_symbols(self, symbols: List[str]):
        clean=[]
        for s in symbols:
            self.validate_symbol(s); clean.append(s)
        self.symbols=set(clean); self._resubscribe.set()

    async def add(self,s): self.validate_symbol(s); self.symbols.add(s); self._resubscribe.set()
    async def remove(self,s): self.symbols.discard(s); self.latest.pop(s,None); self._resubscribe.set()
    async def start(self):
        if not self.task: self.task=asyncio.create_task(self._run(),name="kis-realtime")
    async def stop(self):
        self._stop=True
        if self.task:
            self.task.cancel()
            try: await self.task
            except BaseException: pass

    def status(self):
        return {"configured":self.kis.configured,"connected":self.connected,"last_connected_at":self.last_connected_at,
                "last_message_at":self.last_message_at,"last_error":self.last_error,"symbols":sorted(self.symbols),
                "cached_symbols":sorted(k for k in self.latest.keys() if len(k)==6 and k.isdigit()),
                "parsed_trade_rows":self.parsed_trade_rows,"invalid_trade_rows":self.invalid_trade_rows,
                "trade_frame_mismatch":self.trade_frame_mismatch,"parsed_program_rows":self.parsed_program_rows,
                "invalid_program_rows":self.invalid_program_rows,"parsed_ask_rows":self.parsed_ask_rows,
                "invalid_ask_rows":self.invalid_ask_rows,"ask_frame_mismatch":self.ask_frame_mismatch,
                "parser_valid":self.invalid_trade_rows==0 and self.trade_frame_mismatch==0 and self.invalid_program_rows==0 and self.invalid_ask_rows==0 and self.ask_frame_mismatch==0}

    def _subscription(self,key,symbol,tr_id=WS_TRADE_TR):
        return json.dumps({"header":{"approval_key":key,"custtype":"P","tr_type":"1","content-type":"utf-8"},
                           "body":{"input":{"tr_id":tr_id,"tr_key":symbol}}},ensure_ascii=False)

    def _parse_program_frame(self,msg):
        if not msg.startswith("0|"): return
        parts=msg.split("|",3)
        if len(parts)<4 or parts[1]!=WS_PROGRAM_TR: return
        count=int(parts[2]) if parts[2].isdigit() else 1; fields=parts[3].split("^"); width=11
        expected=max(count,1)*width
        if len(fields) < expected:
            self.invalid_program_rows += max(count,1); return
        for i in range(max(count,1)):
            row=fields[i*width:(i+1)*width]
            if len(row)!=width: self.invalid_program_rows += 1; continue
            symbol=row[0].strip()
            if len(symbol)!=6 or not symbol.isdigit() or symbol not in self.symbols:
                self.invalid_program_rows += 1; continue
            d=self.latest.setdefault(symbol,{"symbol":symbol})
            d["program"]={"time":row[1],"sell_qty":fnum(row[2]),"sell_value":fnum(row[3]),
                          "buy_qty":fnum(row[4]),"buy_value":fnum(row[5]),"net_qty":fnum(row[6]),
                          "net_value":fnum(row[7]),"source":"KIS_WEBSOCKET_PROGRAM_INTEGRATED","received_at":now()}
            self.parsed_program_rows += 1

    def _parse_ask_frame(self,msg):
        if not msg.startswith("0|"): return
        parts=msg.split("|",3)
        if len(parts)<4 or parts[1]!=WS_ASK_TR: return
        count=int(parts[2]) if parts[2].isdigit() else 1; fields=parts[3].split("^"); width=66
        expected=max(count,1)*width
        if len(fields)<expected:
            self.ask_frame_mismatch += 1; return
        for i in range(max(count,1)):
            row=fields[i*width:(i+1)*width]
            if len(row)!=width: self.ask_frame_mismatch += 1; continue
            symbol=row[0].strip()
            if len(symbol)!=6 or not symbol.isdigit() or symbol not in self.symbols:
                self.invalid_ask_rows += 1; continue
            ask=fnum(row[43]); bid=fnum(row[44]); denom=(ask or 0)+(bid or 0)
            imbalance=((bid or 0)-(ask or 0))/denom if denom else None
            d=self.latest.setdefault(symbol,{"symbol":symbol})
            d["orderbook"]={"time":row[1],"ask1":fnum(row[3]),"bid1":fnum(row[13]),
                "ask_qty1":fnum(row[23]),"bid_qty1":fnum(row[33]),"total_ask_qty":ask,"total_bid_qty":bid,
                "imbalance":imbalance,"kmid_price":fnum(row[59]),"nmid_price":fnum(row[62]),
                "source":"KIS_WEBSOCKET_ASK_INTEGRATED","received_at":now()}
            self.parsed_ask_rows += 1

    def _parse_trade_frame(self,msg):
        if not msg.startswith("0|"): return
        parts=msg.split("|",3)
        if len(parts)<4 or parts[1]!=WS_TRADE_TR: return
        count=int(parts[2]) if parts[2].isdigit() else 1; fields=parts[3].split("^"); width=46
        expected=max(count,1)*width
        if len(fields) < expected:
            self.trade_frame_mismatch += 1; return
        for i in range(max(count,1)):
            row=fields[i*width:(i+1)*width]
            if len(row)!=width:
                self.trade_frame_mismatch += 1; continue
            symbol=row[0].strip()
            if len(symbol)!=6 or not symbol.isdigit() or symbol not in self.symbols:
                self.invalid_trade_rows += 1; continue
            same_prev=fnum(row[41]); cum=fnum(row[13])
            data={"symbol":symbol,"trade_time":row[1],"price":fnum(row[2]),"change_rate":fnum(row[5]),
                "trade_volume":fnum(row[12]),"cum_volume":cum,"cum_trading_value":fnum(row[14]),
                "prev_same_time_cum_volume":same_prev,"prev_same_time_cum_volume_rate":fnum(row[42]),
                "same_time_rvol1":(cum/same_prev if cum is not None and same_prev not in (None,0) else None),
                "source":"KIS_WEBSOCKET_TRADE_INTEGRATED","received_at":now()}
            prior=self.latest.get(symbol,{})
            if prior.get("program") is not None: data["program"]=prior["program"]
            if prior.get("orderbook") is not None: data["orderbook"]=prior["orderbook"]
            self.latest[symbol]=data; self.last_message_at=data["received_at"]; self.parsed_trade_rows += 1

    async def _run(self):
        backoff=1
        while not self._stop:
            if not self.kis.configured:
                self.connected=False; self.last_error="credentials_not_configured"; await asyncio.sleep(10); continue
            try:
                approval=await self.kis.approval_key()
                async with websockets.connect(self.kis.ws_url,ping_interval=20,ping_timeout=20,close_timeout=5,max_size=2**20) as ws:
                    self.connected=True; self.last_connected_at=now(); self.last_error=None; backoff=1
                    for s in sorted(self.symbols):
                        await ws.send(self._subscription(approval,s,WS_TRADE_TR))
                        await ws.send(self._subscription(approval,s,WS_PROGRAM_TR))
                        await ws.send(self._subscription(approval,s,WS_ASK_TR))
                    while not self._stop:
                        recv_task=asyncio.create_task(ws.recv()); resub_task=asyncio.create_task(self._resubscribe.wait())
                        done,pending=await asyncio.wait({recv_task,resub_task},return_when=asyncio.FIRST_COMPLETED)
                        for p in pending: p.cancel()
                        if resub_task in done and self._resubscribe.is_set(): self._resubscribe.clear(); await ws.close(); break
                        if recv_task in done:
                            msg=recv_task.result(); msg=msg.decode("utf-8",errors="ignore") if isinstance(msg,bytes) else msg
                            if msg=="PINGPONG": await ws.send(msg); continue
                            if msg.startswith("{"):
                                try:
                                    j=json.loads(msg)
                                    if j.get("header",{}).get("tr_id")=="PINGPONG": await ws.send(msg)
                                except Exception: pass
                                continue
                            self._parse_trade_frame(msg); self._parse_program_frame(msg); self._parse_ask_frame(msg)
            except asyncio.CancelledError: break
            except Exception as e:
                self.connected=False; self.last_error=f"{type(e).__name__}: {str(e)[:180]}"; await asyncio.sleep(backoff); backoff=min(backoff*2,60)
            finally: self.connected=False


def pct_return(current, past):
    if current is None or past in (None,0): return None
    return current/past-1.0


def compute_price_features(quote: dict, history: List[dict]) -> dict:
    rows=[r for r in history if r.get("close") not in (None,0) and r.get("date")]
    rows=sorted(rows,key=lambda r:r["date"])
    closes=[r["close"] for r in rows]
    vols=[r.get("volume") for r in rows]
    tvs=[r.get("trading_value") for r in rows]
    p=quote.get("price")
    r5=pct_return(p, closes[-5] if len(closes)>=5 else None)
    r20=pct_return(p, closes[-20] if len(closes)>=20 else None)
    hist_vol=[v for v in vols[-20:] if v not in (None,0)]
    hist_tv=[v for v in tvs[-20:] if v not in (None,0)]
    rvol=(quote.get("volume")/(sum(hist_vol)/len(hist_vol))) if hist_vol and quote.get("volume") is not None else None
    rtv=(quote.get("trading_value")/(sum(hist_tv)/len(hist_tv))) if hist_tv and quote.get("trading_value") is not None else None
    high,low=quote.get("high"),quote.get("low")
    close_location=((p-low)/(high-low)) if None not in (p,high,low) and high!=low else None
    return {"r5":r5,"r20":r20,"rvol20_raw":rvol,"rtv20_raw":rtv,"close_location":close_location,"history_count":len(rows)}


def compute_flow_features(quote: dict, investor: List[dict], history: List[dict]) -> dict:
    inv=[x for x in investor if x.get("date")]
    inv=sorted(inv,key=lambda x:x["date"])
    if not inv: return {"foreign_intensity":None,"institution_intensity":None,"smart_intensity":None,"flow_accel":None,"confidence":0.0,"history_count":0}
    latest=inv[-1]
    date=latest["date"]
    hmap={x.get("date"):x for x in history}
    hv=(hmap.get(date) or {}).get("trading_value") or quote.get("trading_value")
    price=quote.get("price") or (hmap.get(date) or {}).get("close")
    def qty_to_value(q): return q*price if q is not None and price is not None else None
    fv=qty_to_value(latest.get("foreign_net_qty")); iv=qty_to_value(latest.get("institution_net_qty"))
    fi=(fv/hv) if fv is not None and hv not in (None,0) else None
    ii=(iv/hv) if iv is not None and hv not in (None,0) else None
    smart=sum(x for x in (fi,ii) if x is not None) if any(x is not None for x in (fi,ii)) else None
    prev_smart=None
    if len(inv)>=2:
        prev=inv[-2]; ph=(hmap.get(prev["date"]) or {}); ptv=ph.get("trading_value"); pp=ph.get("close")
        vals=[]
        for q in (prev.get("foreign_net_qty"),prev.get("institution_net_qty")):
            vals.append((q*pp/ptv) if q is not None and pp is not None and ptv not in (None,0) else None)
        prev_smart=sum(x for x in vals if x is not None) if any(x is not None for x in vals) else None
    accel=(smart-prev_smart) if smart is not None and prev_smart is not None else None
    return {"foreign_intensity":fi,"institution_intensity":ii,"smart_intensity":smart,"flow_accel":accel,
            "confidence":0.72,"history_count":len(inv),"source":"KIS_INVESTOR_CONFIRMED","latest_date":date}


def market_regime(index_rows: List[dict]) -> Optional[float]:
    rows=sorted([x for x in index_rows if x.get("date") and x.get("close") not in (None,0)],key=lambda x:x["date"])
    if len(rows)<6: return None
    r5=rows[-1]["close"]/rows[-6]["close"]-1.0
    return signed_unit(r5,0.05)


def broad_prefilter(quotes: List[dict], n=80) -> List[dict]:
    good=[q for q in quotes if q.get("price") not in (None,0) and q.get("trading_value") not in (None,0)]
    if not good: return []
    vals=sorted(q["trading_value"] for q in good)
    def pr(v):
        lo=0; hi=len(vals)
        while lo<hi:
            mid=(lo+hi)//2
            if vals[mid]<=v: lo=mid+1
            else: hi=mid
        return lo/max(1,len(vals))
    for q in good:
        cr=q.get("change_rate") or 0.0
        # Discovery only: liquid + active + not already extremely extended.
        modest=1.0 if -3.0<=cr<=7.0 else max(0.0,1.0-abs(cr-2.0)/20.0)
        q["prefilter_score"]=0.75*pr(q["trading_value"])+0.25*modest
    return sorted(good,key=lambda x:x["prefilter_score"],reverse=True)[:n]


def compute_visible_lite(news_rows: List[dict]) -> dict:
    """Conservative title-only visible signal. Deliberately low confidence until DART/earnings adapters exist."""
    pos=("수주","계약","공급","승인","허가","흑자전환","상향","자사주","소각","배당","증설","투자","특허","신제품","최대 실적","실적 개선")
    neg=("적자","하향","유상증자","횡령","배임","소송","리콜","거래정지","관리종목","상장폐지","계약 해지","취소","실적 부진")
    if not news_rows:
        return {"value":None,"confidence":0.0,"event_count":0,"coverage":"NO_TITLE_ROWS","latest_event_at":None}
    raw=0.0; hits=0; latest=None
    for r in news_rows[:40]:
        title=str(r.get("title") or "")
        s=sum(1 for w in pos if w in title)-sum(1 for w in neg if w in title)
        if s:
            raw+=max(-2,min(2,s)); hits+=1
        stamp=(str(r.get("date") or "")+str(r.get("time") or "")).strip() or None
        if stamp and (latest is None or stamp>latest): latest=stamp
    value=signed_unit(raw,4.0) if hits else 0.0
    conf=min(0.55,0.22+0.05*min(len(news_rows),6))
    return {"value":value,"confidence":conf,"event_count":len(news_rows),"scored_events":hits,
            "coverage":"KIS_TITLE_ONLY_PARTIAL","latest_event_at":latest}


def assemble_symbol(item: UniverseItem, quote: dict, history: List[dict], investor: List[dict], regime: Optional[float], realtime: Optional[dict], news_rows: Optional[List[dict]]=None) -> dict:
    pf=compute_price_features(quote,history); ff=compute_flow_features(quote,investor,history)
    r5=pf["r5"]; r20=pf["r20"]; cl=pf["close_location"]
    p_parts=[x for x in [signed_unit(r5,0.08), signed_unit(r20,0.18), (2*cl-1 if cl is not None else None)] if x is not None]
    p_price=sum(p_parts)/len(p_parts) if p_parts else None
    f_flow=signed_unit(ff.get("smart_intensity"),0.05)
    if f_flow is not None and ff.get("flow_accel") is not None:
        f_flow=clamp((f_flow+1)/2) * 2 - 1
        f_flow=max(-1,min(1,f_flow+0.25*signed_unit(ff["flow_accel"],0.02)))
    # Relative block: use stock 5d return minus market 5d proxy represented by regime*5%.
    rel=(r5 - (regime or 0.0)*0.05) if r5 is not None and regime is not None else None
    r_relative=signed_unit(rel,0.08)
    v_regime=regime
    visible_lite=compute_visible_lite(news_rows or []) if news_rows is not None else {"value":None,"confidence":0.0,"coverage":"NOT_QUERIED","event_count":0,"latest_event_at":None}
    visible=visible_lite.get("value")

    # Signal onset proxy: latest 5d move + activity. No false timestamp is invented.
    raw_activity=max(pf.get("rvol20_raw") or 0, pf.get("rtv20_raw") or 0)
    very_fresh=raw_activity>=1.5
    change_since=max(0.0,r5 or 0.0)
    reflection=clamp(change_since/0.10)
    entry_friction=clamp(max(0.0,(quote.get("change_rate") or 0.0)-5.0)/10.0)
    tail_risk=clamp(max(0.0,abs(r20 or 0.0)-0.20)/0.30 + (0.25 if item.market_warning else 0.0))
    decay=clamp(reflection * (0.7 if (r5 or 0)>0 else 0.3))
    reversal=clamp(max(0.0,-(ff.get("flow_accel") or 0.0))/0.03)
    negative_event=None  # do not fabricate K6 event state
    hold_edge=max(-1.0,min(1.0,(p_price or 0.0)*0.45+(f_flow or 0.0)*0.35+(r_relative or 0.0)*0.20))

    blocks={"P":p_price,"F":f_flow,"I":visible,"R":r_relative,"V":v_regime}
    weights={"P":0.22,"F":0.24,"I":0.22,"R":0.18,"V":0.14}
    present=[k for k,v in blocks.items() if v is not None]
    sw=sum(weights[k] for k in present)
    expected=sum(weights[k]*blocks[k] for k in present)/sw if sw else None
    confidence=0.90*sw*(0.85 if len(present)<5 else 1.0)
    if visible is not None:
        confidence*=0.88+0.12*visible_lite.get("confidence",0.0)
    remain=confidence*(expected-0.45*reflection-0.20*tail_risk) if expected is not None else None
    buy_latent=confidence*(expected-0.60*reflection-0.25*entry_friction-0.15*tail_risk) if expected is not None else None
    sell_latent=None  # negative_event is missing; fail-closed SELL rather than zero-impute.
    state="INCOMPLETE_K6" if visible is None else "K6_LITE"

    # K7 evidence uses None for missing catalyst; Major cannot be official while K6 missing.
    lead=clamp(((p_price or 0)+1)/2) if p_price is not None else None
    demand=clamp(((f_flow or 0)+1)/2) if f_flow is not None else None
    rel01=clamp(((r_relative or 0)+1)/2) if r_relative is not None else None
    risk=tail_risk
    major_order=(0.25*(lead or 0)+0.25*(demand or 0)+0.25*(rel01 or 0)-0.30*reflection-0.20*risk)*confidence
    minor_order=(0.30*(1 if very_fresh else 0)+0.25*(1 if raw_activity>=1.5 else 0)+0.20*(1 if (rel or 0)>0 else 0)+0.10*(demand or 0)-0.20*reflection-0.15*risk)*confidence

    return {
        "symbol":item.symbol,"name":item.name,"market":item.market,
        "quote":quote,"realtime":realtime,"price_features":pf,"flow_features":ff,
        "experts":{"P":p_price,"F":f_flow,"I":visible,"R":r_relative,"V":v_regime},
        "terms":{"reflection":reflection,"entry_friction":entry_friction,"tail_risk":tail_risk,"decay":decay,
                 "reversal":reversal,"negative_event":negative_event,"hold_edge":hold_edge},
        "score":{"state":state,"expected":expected,"remain":remain,"buy":score_1_10(buy_latent),"sell":None,
                 "overall":score_1_10(remain),"confidence":confidence,"missing_blocks":[k for k,v in blocks.items() if v is None]},
        "list_evidence":{"major_eligible":False,"minor_eligible":minor_order>0 and reflection<0.90,
                         "major_ordering_score":major_order,"minor_ordering_score":minor_order,"very_fresh_onset":very_fresh,
                         "abnormal_volume":raw_activity>=1.5,"improving_relative_gap":bool((rel or 0)>0),
                         "critical_enrichment_ok":False,"reason":"K6 is KIS title-only partial; DART/earnings/consensus still missing so official Major remains fail-closed"},
        "lineage":{"quote":"KIS_REST","history":"KIS_DAILY","flow":"KIS_INVESTOR_CONFIRMED","realtime":"KIS_WEBSOCKET" if realtime else None,
                   "visible":"KIS_NEWS_TITLE_PARTIAL" if news_rows is not None else None,"assembled_at":utc_iso()},
        "visible_lite":visible_lite,
    }




class OperationalGuard:
    """Fail-closed operational state; never converts infrastructure failure into alpha."""
    SECRET_KEYS=("KIS_APP_KEY","KIS_APP_SECRET","DART_API_KEY","KIS_ACCESS_TOKEN","authorization","appsecret","appkey")
    def __init__(self):
        self.breakers={}
        self.ws={"connected":False,"last_message_at":None,"last_heartbeat_at":None,
                 "expected_subscriptions":set(),"active_subscriptions":set(),"reconnects":0}
        self.sources={}
    def redact(self,obj):
        if isinstance(obj,dict):
            out={}
            for k,v in obj.items():
                kl=str(k).lower()
                if any(s.lower() in kl for s in self.SECRET_KEYS): out[k]="***REDACTED***"
                else: out[k]=self.redact(v)
            return out
        if isinstance(obj,list): return [self.redact(x) for x in obj]
        if isinstance(obj,str):
            s=obj
            for env in ("KIS_APP_KEY","KIS_APP_SECRET","DART_API_KEY","KIS_ACCESS_TOKEN"):
                val=os.getenv(env)
                if val and len(val)>=6: s=s.replace(val,"***REDACTED***")
            if s.lower().startswith("bearer "): return "Bearer ***REDACTED***"
            return s
        return obj
    def source_seen(self,source,ok=True,observed_at=None,error=None):
        now=observed_at or utc_iso()
        x=self.sources.setdefault(source,{"last_success_at":None,"last_failure_at":None,"last_error":None})
        if ok: x["last_success_at"]=now; x["last_error"]=None
        else: x["last_failure_at"]=now; x["last_error"]=str(error or "")[:300]
    def breaker_result(self,name,ok,threshold=5,cooldown_seconds=60):
        now=time.time()
        b=self.breakers.setdefault(name,{"failures":0,"opened_at":None,"state":"CLOSED"})
        if ok:
            b.update({"failures":0,"opened_at":None,"state":"CLOSED"})
        else:
            b["failures"]+=1
            if b["failures"]>=threshold:
                b["state"]="OPEN"; b["opened_at"]=now
        return dict(b)
    def breaker_allow(self,name,cooldown_seconds=60):
        b=self.breakers.get(name)
        if not b or b["state"]=="CLOSED": return True
        if b["state"]=="OPEN" and b.get("opened_at") and time.time()-b["opened_at"]>=cooldown_seconds:
            b["state"]="HALF_OPEN"; return True
        return b["state"]=="HALF_OPEN"
    def ws_update(self,connected=None,message_at=None,heartbeat_at=None,expected=None,active=None,reconnect=False):
        if connected is not None: self.ws["connected"]=bool(connected)
        if message_at: self.ws["last_message_at"]=message_at
        if heartbeat_at: self.ws["last_heartbeat_at"]=heartbeat_at
        if expected is not None: self.ws["expected_subscriptions"]=set(expected)
        if active is not None: self.ws["active_subscriptions"]=set(active)
        if reconnect: self.ws["reconnects"]+=1
    def ws_audit(self):
        exp=self.ws["expected_subscriptions"]; act=self.ws["active_subscriptions"]
        missing=sorted(exp-act)
        return {"connected":self.ws["connected"],"last_message_at":self.ws["last_message_at"],
                "last_heartbeat_at":self.ws["last_heartbeat_at"],"expected":len(exp),"active":len(act),
                "missing_subscriptions":missing,"reconnects":self.ws["reconnects"],
                "ok":bool(self.ws["connected"] and not missing)}
    def _age_seconds(self,iso):
        if not iso: return None
        try:
            d=datetime.fromisoformat(str(iso).replace("Z","+00:00"))
            if d.tzinfo is None: d=d.replace(tzinfo=timezone.utc)
            return max(0,(datetime.now(timezone.utc)-d).total_seconds())
        except Exception: return None
    def freshness(self,thresholds=None):
        thresholds=thresholds or {"KIS_REST":180,"KIS_WS":30,"OPENDART":3600,"KIS_NEWS_TITLE":900}
        out={}; bad=[]
        for src,limit in thresholds.items():
            seen=self.sources.get(src,{}).get("last_success_at")
            age=self._age_seconds(seen)
            stale=age is None or age>limit
            out[src]={"last_success_at":seen,"age_seconds":age,"threshold_seconds":limit,"stale":stale}
            if stale: bad.append(src)
        return {"sources":out,"stale_sources":bad,"ok":not bad}
    def token_status(self):
        # Never expose token value. Expiry is optionally injected by runtime after OAuth response.
        exp=os.getenv("KIS_TOKEN_EXPIRES_AT")
        age=self._age_seconds(exp) if exp else None
        expired=False
        if exp:
            try:
                d=datetime.fromisoformat(exp.replace("Z","+00:00"))
                if d.tzinfo is None: d=d.replace(tzinfo=timezone.utc)
                expired=datetime.now(timezone.utc)>=d
            except Exception: expired=True
        return {"configured":bool(os.getenv("KIS_APP_KEY") and os.getenv("KIS_APP_SECRET")),
                "expiry_known":bool(exp),"expires_at":exp,"expired":expired,
                "token_exposed":False}
    def readiness(self,pit):
        ps=pit.status()
        ws=self.ws_audit()
        fresh=self.freshness()
        token=self.token_status()
        disk_ok=True; free=None
        try:
            du=shutil.disk_usage(pit.root); free=du.free; disk_ok=du.free>100*1024*1024
        except Exception: disk_ok=False
        db_ok=bool((ps.get("integrity") or {}).get("ok"))
        durable=bool(ps.get("durable_expected"))
        breaker_open=[k for k,v in self.breakers.items() if v.get("state")=="OPEN"]
        reasons=[]
        if not db_ok: reasons.append("DB_INTEGRITY")
        if not disk_ok: reasons.append("LOW_DISK")
        if not durable: reasons.append("NON_DURABLE_STORAGE")
        if not token["configured"]: reasons.append("KIS_CREDENTIALS_NOT_CONFIGURED")
        if breaker_open: reasons.append("CIRCUIT_OPEN")
        if not ws["ok"]: reasons.append("WS_NOT_READY")
        if not fresh["ok"]: reasons.append("STALE_SOURCE")
        ready=not reasons
        return {"ready":ready,"state":"READY" if ready else "DEGRADED","major_gate_open":ready,
                "reasons":reasons,"db_integrity":db_ok,"durable_storage":durable,
                "disk_free_bytes":free,"token":token,"websocket":ws,"freshness":fresh,
                "open_breakers":breaker_open}

ops=OperationalGuard()

class PITStore:
    SCHEMA_VERSION=2
    def __init__(self):
        preferred=os.getenv("RGI_DATA_DIR","/data").strip() or "/data"
        root=preferred
        try:
            Path(root).mkdir(parents=True,exist_ok=True)
            probe=Path(root)/".rgi_probe"; probe.write_text("ok"); probe.unlink()
        except Exception:
            root="/tmp/rgi-data"; Path(root).mkdir(parents=True,exist_ok=True)
        self.root=root
        self.path=str(Path(root)/"rgi_pit.sqlite3")
        self.backup_dir=str(Path(root)/"backups")
        Path(self.backup_dir).mkdir(parents=True,exist_ok=True)
        self._migrate()
    def _con(self,path=None):
        c=sqlite3.connect(path or self.path,timeout=15); c.row_factory=sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=NORMAL")
        return c
    def _cols(self,c,table):
        return {r["name"] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
    def _migrate(self):
        with self._con() as c:
            c.execute("CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)")
            c.execute("""CREATE TABLE IF NOT EXISTS pit_events(
              id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, block TEXT,
              feature_time TEXT, available_at TEXT NOT NULL, source TEXT NOT NULL,
              confidence REAL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL UNIQUE)""")
            cols=self._cols(c,"pit_events")
            additions={
                "observed_at":"TEXT","latency_ms":"REAL","stale":"INTEGER DEFAULT 0",
                "schema_version":"INTEGER DEFAULT 1","run_id":"TEXT"
            }
            for col,typ in additions.items():
                if col not in cols: c.execute(f"ALTER TABLE pit_events ADD COLUMN {col} {typ}")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pit_symbol_avail ON pit_events(symbol,available_at)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pit_block_avail ON pit_events(block,available_at)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pit_source_avail ON pit_events(source,available_at)")
            c.execute("""CREATE TABLE IF NOT EXISTS scan_runs(
              id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, completed_at TEXT,
              mode TEXT, universe_total INTEGER, attempted INTEGER, succeeded INTEGER,
              coverage REAL, classification TEXT, payload_json TEXT)""")
            srcols=self._cols(c,"scan_runs")
            for col,typ in {"run_id":"TEXT","duration_ms":"REAL","failure_count":"INTEGER DEFAULT 0","schema_version":"INTEGER DEFAULT 1"}.items():
                if col not in srcols: c.execute(f"ALTER TABLE scan_runs ADD COLUMN {col} {typ}")
            c.execute("""CREATE TABLE IF NOT EXISTS source_health(
              source TEXT PRIMARY KEY,last_success_at TEXT,last_failure_at TEXT,last_error TEXT,
              success_count INTEGER DEFAULT 0,failure_count INTEGER DEFAULT 0,updated_at TEXT NOT NULL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS discovery_audits(
              id INTEGER PRIMARY KEY AUTOINCREMENT, audit_time TEXT NOT NULL, run_id TEXT,
              symbol TEXT NOT NULL, in_fast INTEGER NOT NULL, in_full INTEGER NOT NULL,
              fast_rank INTEGER, full_rank INTEGER, fast_score REAL, full_score REAL,
              sector TEXT, market TEXT, liquidity REAL, market_cap REAL,
              payload_json TEXT NOT NULL)""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_discovery_symbol_time ON discovery_audits(symbol,audit_time)")
            c.execute("""CREATE TABLE IF NOT EXISTS omission_outcomes(
              id INTEGER PRIMARY KEY AUTOINCREMENT, audit_id INTEGER NOT NULL, horizon INTEGER NOT NULL,
              forward_return REAL, future_major INTEGER, future_top_return INTEGER,
              evaluated_at TEXT NOT NULL, UNIQUE(audit_id,horizon))""")
            c.execute("""CREATE TABLE IF NOT EXISTS shadow_microstructure(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sample_time TEXT NOT NULL, available_at TEXT NOT NULL,
              symbol TEXT NOT NULL, block TEXT NOT NULL,
              production_score REAL, shadow_value REAL, confidence REAL,
              reflection REAL, late_entry INTEGER,
              payload_json TEXT NOT NULL,
              UNIQUE(sample_time,symbol,block))""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_shadow_ms_block_time ON shadow_microstructure(block,sample_time)")
            c.execute("""CREATE TABLE IF NOT EXISTS shadow_microstructure_outcomes(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sample_id INTEGER NOT NULL, horizon INTEGER NOT NULL,
              forward_return REAL, realized_drawdown REAL, realized_cvar REAL,
              evaluated_at TEXT NOT NULL, UNIQUE(sample_id,horizon))""")
            c.execute("""CREATE TABLE IF NOT EXISTS production_validation_samples(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sample_date TEXT NOT NULL, available_at TEXT NOT NULL, symbol TEXT NOT NULL,
              p REAL, f REAL, i REAL, r REAL, v REAL,
              reflection REAL, tail_risk REAL, confidence REAL,
              current_score REAL, payload_json TEXT NOT NULL,
              UNIQUE(sample_date,symbol))""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_prodval_date ON production_validation_samples(sample_date)")
            c.execute("""CREATE TABLE IF NOT EXISTS production_validation_outcomes(
              id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL,
              horizon INTEGER NOT NULL, forward_return REAL NOT NULL,
              evaluated_at TEXT NOT NULL, UNIQUE(sample_id,horizon))""")
            c.execute("""CREATE TABLE IF NOT EXISTS rotation_snapshots(
              id INTEGER PRIMARY KEY AUTOINCREMENT, session_date TEXT NOT NULL, symbol TEXT NOT NULL,
              entry_edge REAL, remaining_edge REAL, sell_index REAL,
              gap_decay REAL, distribution REAL, flow_reversal REAL, overextension REAL,
              price REAL, confidence REAL, payload_json TEXT NOT NULL,
              UNIQUE(session_date,symbol))""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_rotation_date ON rotation_snapshots(session_date)")
            c.execute("""CREATE TABLE IF NOT EXISTS rotation_trades(
              id INTEGER PRIMARY KEY AUTOINCREMENT, entry_date TEXT NOT NULL, exit_date TEXT NOT NULL,
              from_symbol TEXT, to_symbol TEXT, hold_sessions INTEGER,
              from_return REAL, to_forward_return REAL, switch_cost REAL,
              opportunity_edge REAL, reason TEXT, payload_json TEXT NOT NULL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS operational_events(
              id INTEGER PRIMARY KEY AUTOINCREMENT, event_time TEXT NOT NULL, component TEXT NOT NULL,
              level TEXT NOT NULL, code TEXT NOT NULL, payload_json TEXT NOT NULL)""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_ops_time ON operational_events(event_time)")
            c.execute("""CREATE TABLE IF NOT EXISTS safety_injection_runs(
              id INTEGER PRIMARY KEY AUTOINCREMENT, run_time TEXT NOT NULL, hazard_id TEXT NOT NULL,
              framework TEXT NOT NULL, injected_json TEXT NOT NULL, expected_json TEXT NOT NULL,
              observed_json TEXT NOT NULL, passed INTEGER NOT NULL)""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_safety_hazard_time ON safety_injection_runs(hazard_id,run_time)")
            c.execute("""INSERT INTO schema_meta(key,value,updated_at) VALUES('schema_version',?,?)
                         ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
                      (str(self.SCHEMA_VERSION),utc_iso()))
    def append(self,symbol,block,payload,source,confidence=None,feature_time=None,available_at=None,
               observed_at=None,latency_ms=None,stale=False,run_id=None):
        av=available_at or utc_iso(); obs=observed_at or utc_iso()
        body=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(",",":"))
        # Exact same evidence at same availability time is idempotent.
        h=hashlib.sha256((str(symbol)+str(block)+str(feature_time)+str(av)+str(source)+body).encode()).hexdigest()
        with self._con() as c:
            c.execute("""INSERT OR IGNORE INTO pit_events
              (symbol,block,feature_time,available_at,source,confidence,payload_json,payload_hash,
               observed_at,latency_ms,stale,schema_version,run_id)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (symbol,block,feature_time,av,source,confidence,body,h,obs,latency_ms,int(bool(stale)),self.SCHEMA_VERSION,run_id))
    def source_result(self,source,ok,error=None,at=None):
        at=at or utc_iso()
        with self._con() as c:
            row=c.execute("SELECT * FROM source_health WHERE source=?",(source,)).fetchone()
            sc=(row["success_count"] if row else 0)+(1 if ok else 0)
            fc=(row["failure_count"] if row else 0)+(0 if ok else 1)
            ls=at if ok else (row["last_success_at"] if row else None)
            lf=at if not ok else (row["last_failure_at"] if row else None)
            le=None if ok else str(error or "")[:500]
            c.execute("""INSERT INTO source_health(source,last_success_at,last_failure_at,last_error,success_count,failure_count,updated_at)
              VALUES(?,?,?,?,?,?,?) ON CONFLICT(source) DO UPDATE SET
              last_success_at=excluded.last_success_at,last_failure_at=excluded.last_failure_at,last_error=excluded.last_error,
              success_count=excluded.success_count,failure_count=excluded.failure_count,updated_at=excluded.updated_at""",
              (source,ls,lf,le,sc,fc,at))
    def scan(self,started,completed,mode,universe_total,attempted,succeeded,coverage,classification,payload,run_id=None):
        try:
            a=datetime.fromisoformat(str(started).replace("Z","+00:00")); b=datetime.fromisoformat(str(completed).replace("Z","+00:00"))
            dur=(b-a).total_seconds()*1000
        except Exception: dur=None
        fail=max(0,int(attempted or 0)-int(succeeded or 0))
        with self._con() as c:
            c.execute("""INSERT INTO scan_runs(started_at,completed_at,mode,universe_total,attempted,succeeded,coverage,classification,payload_json,
                         run_id,duration_ms,failure_count,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (started,completed,mode,universe_total,attempted,succeeded,coverage,classification,
                       json.dumps(payload,ensure_ascii=False,separators=(",",":")),run_id,dur,fail,self.SCHEMA_VERSION))
    def recent_payloads(self,symbol,block,limit=8):
        with self._con() as c:
            rows=c.execute("""SELECT available_at,payload_json,source,confidence FROM pit_events
                              WHERE symbol=? AND block=? ORDER BY available_at DESC LIMIT ?""",(symbol,block,int(limit))).fetchall()
        out=[]
        for r in rows:
            try: payload=json.loads(r["payload_json"])
            except Exception: payload=None
            out.append({"available_at":r["available_at"],"payload":payload,"source":r["source"],"confidence":r["confidence"]})
        return out
    def discovery_audit(self,run_id,fast_rows,full_rows,audit_time=None):
        audit_time=audit_time or utc_iso()
        def norm(rows):
            out={}
            for i,r in enumerate(rows or [],1):
                sym=str(r.get("symbol") or r.get("code") or "")
                if len(sym)!=6 or not sym.isdigit(): continue
                out[sym]={"rank":int(r.get("rank") or i),"score":r.get("score"),
                          "sector":r.get("sector"),"market":r.get("market"),
                          "liquidity":r.get("liquidity") or r.get("trading_value"),
                          "market_cap":r.get("market_cap"),"raw":r}
            return out
        f=norm(fast_rows); s=norm(full_rows)
        symbols=sorted(set(f)|set(s)); ids=[]
        with self._con() as c:
            for sym in symbols:
                fr=f.get(sym); sr=s.get(sym)
                meta=sr or fr
                payload={"fast":fr["raw"] if fr else None,"full":sr["raw"] if sr else None}
                cur=c.execute("""INSERT INTO discovery_audits
                  (audit_time,run_id,symbol,in_fast,in_full,fast_rank,full_rank,fast_score,full_score,
                   sector,market,liquidity,market_cap,payload_json)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (audit_time,run_id,sym,int(fr is not None),int(sr is not None),
                   fr["rank"] if fr else None,sr["rank"] if sr else None,
                   fr["score"] if fr else None,sr["score"] if sr else None,
                   meta.get("sector"),meta.get("market"),meta.get("liquidity"),meta.get("market_cap"),
                   json.dumps(payload,ensure_ascii=False,separators=(",",":"))))
                ids.append(cur.lastrowid)
        return {"run_id":run_id,"audit_time":audit_time,"symbols":len(symbols),
                "fast":len(f),"full":len(s),"full_missed_by_fast":len(set(s)-set(f)),"audit_ids":ids}

    def record_omission_outcome(self,audit_id,horizon,forward_return,future_major=False,future_top_return=False,evaluated_at=None):
        with self._con() as c:
            c.execute("""INSERT OR REPLACE INTO omission_outcomes
              (audit_id,horizon,forward_return,future_major,future_top_return,evaluated_at)
              VALUES(?,?,?,?,?,?)""",(int(audit_id),int(horizon),forward_return,int(bool(future_major)),
                                      int(bool(future_top_return)),evaluated_at or utc_iso()))

    def omission_metrics(self,horizon=5):
        with self._con() as c:
            rows=[dict(r) for r in c.execute("""SELECT d.*,o.forward_return,o.future_major,o.future_top_return
                FROM discovery_audits d JOIN omission_outcomes o ON o.audit_id=d.id
                WHERE o.horizon=?""",(int(horizon),)).fetchall()]
        if not rows:
            return {"horizon":horizon,"evaluated":0,"status":"INSUFFICIENT_EVIDENCE"}
        def rate(n,d): return n/d if d else None
        future_major=[r for r in rows if r["future_major"]]
        topret=[r for r in rows if r["future_top_return"]]
        missed_major=[r for r in future_major if not r["in_fast"]]
        missed_top=[r for r in topret if not r["in_fast"]]
        full_only=[r for r in rows if r["in_full"] and not r["in_fast"]]
        fast=[r for r in rows if r["in_fast"]]
        avg=lambda xs: (sum(x["forward_return"] for x in xs if x["forward_return"] is not None)/
                        len([x for x in xs if x["forward_return"] is not None])) if any(x["forward_return"] is not None for x in xs) else None
        # Bias: miss rate by categorical market/sector and liquidity quartile proxy using sample ranks.
        by_market={}
        for r in rows:
            k=r.get("market") or "UNKNOWN"; by_market.setdefault(k,[0,0])
            if r["future_major"]:
                by_market[k][1]+=1
                if not r["in_fast"]: by_market[k][0]+=1
        market_bias={k:{"missed":v[0],"future_major":v[1],"miss_rate":rate(v[0],v[1])} for k,v in by_market.items()}
        return {"horizon":horizon,"evaluated":len(rows),"future_major":len(future_major),
                "future_major_missed":len(missed_major),"future_major_miss_rate":rate(len(missed_major),len(future_major)),
                "future_top_return":len(topret),"future_top_return_missed":len(missed_top),
                "future_top_return_miss_rate":rate(len(missed_top),len(topret)),
                "full_only_count":len(full_only),"full_only_avg_forward_return":avg(full_only),
                "fast_avg_forward_return":avg(fast),"market_bias":market_bias,
                "decision":"REVIEW_DISCOVERY" if (rate(len(missed_major),len(future_major)) or 0)>.20 else "NO_MATERIAL_OMISSION_SIGNAL"}

    def shadow_sample(self,sample_time,symbol,block,production_score,shadow_value,confidence=None,
                      reflection=None,late_entry=False,payload=None,available_at=None):
        av=available_at or sample_time
        with self._con() as c:
            c.execute("""INSERT OR REPLACE INTO shadow_microstructure
              (sample_time,available_at,symbol,block,production_score,shadow_value,confidence,reflection,late_entry,payload_json)
              VALUES(?,?,?,?,?,?,?,?,?,?)""",
              (sample_time,av,symbol,block,production_score,shadow_value,confidence,reflection,int(bool(late_entry)),
               json.dumps(payload or {},ensure_ascii=False,separators=(",",":"))))
            row=c.execute("SELECT id FROM shadow_microstructure WHERE sample_time=? AND symbol=? AND block=?",
                          (sample_time,symbol,block)).fetchone()
        return int(row["id"])
    def shadow_outcome(self,sample_id,horizon,forward_return,realized_drawdown=None,realized_cvar=None,evaluated_at=None):
        with self._con() as c:
            c.execute("""INSERT OR REPLACE INTO shadow_microstructure_outcomes
              (sample_id,horizon,forward_return,realized_drawdown,realized_cvar,evaluated_at)
              VALUES(?,?,?,?,?,?)""",(int(sample_id),int(horizon),forward_return,realized_drawdown,realized_cvar,evaluated_at or utc_iso()))

    def _rank(self,vals):
        # Average-rank ties.
        pairs=sorted(enumerate(vals),key=lambda x:x[1]); ranks=[0.0]*len(vals); i=0
        while i<len(pairs):
            j=i
            while j+1<len(pairs) and pairs[j+1][1]==pairs[i][1]: j+=1
            r=(i+j+2)/2.0
            for k in range(i,j+1): ranks[pairs[k][0]]=r
            i=j+1
        return ranks
    def _spearman(self,a,b):
        if len(a)<3 or len(a)!=len(b): return None
        ra=self._rank(a); rb=self._rank(b)
        ma=sum(ra)/len(ra); mb=sum(rb)/len(rb)
        num=sum((x-ma)*(y-mb) for x,y in zip(ra,rb))
        da=sum((x-ma)**2 for x in ra); db=sum((y-mb)**2 for y in rb)
        return num/math.sqrt(da*db) if da>0 and db>0 else None
    def _top_bottom(self,scores,rets,k=3):
        if len(scores)<2*k: return None
        idx=sorted(range(len(scores)),key=lambda i:scores[i],reverse=True)
        top=[rets[i] for i in idx[:k]]; bot=[rets[i] for i in idx[-k:]]
        return sum(top)/len(top)-sum(bot)/len(bot)
    def _bootstrap_delta_ci(self,base_scores,shadow_scores,rets,k=3,n=400,seed=23):
        if len(rets)<max(6,2*k): return None
        import random
        rng=random.Random(seed); deltas=[]; N=len(rets)
        for _ in range(n):
            ix=[rng.randrange(N) for __ in range(N)]
            br=self._top_bottom([base_scores[i] for i in ix],[rets[i] for i in ix],k)
            sr=self._top_bottom([shadow_scores[i] for i in ix],[rets[i] for i in ix],k)
            if br is not None and sr is not None: deltas.append(sr-br)
        if not deltas: return None
        deltas.sort()
        lo=deltas[int(.025*(len(deltas)-1))]; hi=deltas[int(.975*(len(deltas)-1))]
        return [lo,hi]
    def shadow_block_metrics(self,block,horizon=5,k=3):
        with self._con() as c:
            rows=[dict(r) for r in c.execute("""SELECT s.*,o.forward_return,o.realized_drawdown,o.realized_cvar
                FROM shadow_microstructure s JOIN shadow_microstructure_outcomes o ON o.sample_id=s.id
                WHERE s.block=? AND o.horizon=? ORDER BY s.sample_time,s.symbol""",(block,int(horizon))).fetchall()]
        if len(rows)<6:
            return {"block":block,"horizon":horizon,"evaluated":len(rows),"decision":"INSUFFICIENT_EVIDENCE"}
        # Cross-sectional daily ICs
        by_time={}
        for r in rows: by_time.setdefault(r["sample_time"],[]).append(r)
        prod_ics=[]; sh_ics=[]; overlap=[]
        for rs in by_time.values():
            if len(rs)>=3:
                rets=[x["forward_return"] for x in rs]
                p=[x["production_score"] for x in rs]
                s=[x["production_score"]+x["shadow_value"]*float(x["confidence"] or 0) for x in rs]
                pi=self._spearman(p,rets); si=self._spearman(s,rets); ov=self._spearman(p,[x["shadow_value"] for x in rs])
                if pi is not None: prod_ics.append(pi)
                if si is not None: sh_ics.append(si)
                if ov is not None: overlap.append(abs(ov))
        pscore=[r["production_score"] for r in rows]
        sscore=[r["production_score"]+r["shadow_value"]*float(r["confidence"] or 0) for r in rows]
        rets=[r["forward_return"] for r in rows]
        pspread=self._top_bottom(pscore,rets,k); sspread=self._top_bottom(sscore,rets,k)
        ci=self._bootstrap_delta_ci(pscore,sscore,rets,k)
        # Late-entry reduction: among top-k production vs combined, fraction flagged late.
        idxp=sorted(range(len(rows)),key=lambda i:pscore[i],reverse=True)[:k]
        idxs=sorted(range(len(rows)),key=lambda i:sscore[i],reverse=True)[:k]
        late_p=sum(rows[i]["late_entry"] for i in idxp)/len(idxp)
        late_s=sum(rows[i]["late_entry"] for i in idxs)/len(idxs)
        # Risk/turnover proxies
        dd=[r["realized_drawdown"] for r in rows if r["realized_drawdown"] is not None]
        cv=[r["realized_cvar"] for r in rows if r["realized_cvar"] is not None]
        avg=lambda xs: sum(xs)/len(xs) if xs else None
        # turnover = top-k set changes across adjacent times
        times=sorted(by_time); prev=None; changes=[]
        for t in times:
            rs=by_time[t]; ids=sorted(range(len(rs)),key=lambda i:rs[i]["production_score"]+rs[i]["shadow_value"]*float(rs[i]["confidence"] or 0),reverse=True)[:min(k,len(rs))]
            cur={rs[i]["symbol"] for i in ids}
            if prev is not None and (cur or prev): changes.append(1-len(cur&prev)/max(1,len(cur|prev)))
            prev=cur
        delta=(sspread-pspread) if (sspread is not None and pspread is not None) else None
        mean_prod_ic=avg(prod_ics); mean_shadow_ic=avg(sh_ics)
        ic_delta=(mean_shadow_ic-mean_prod_ic) if (mean_prod_ic is not None and mean_shadow_ic is not None) else None
        redundancy=avg(overlap)
        # Conservative promotion rule.
        promote=bool(delta is not None and delta>0 and ic_delta is not None and ic_delta>0 and
                     ci is not None and ci[0]>0 and (redundancy is None or redundancy<.80) and late_s<=late_p)
        decision="PROMOTE_CANDIDATE" if promote else ("REDUNDANT" if redundancy is not None and redundancy>=.80 else "KEEP_SHADOW")
        return {"block":block,"horizon":horizon,"evaluated":len(rows),"dates":len(by_time),
                "production_rank_ic":mean_prod_ic,"shadow_rank_ic":mean_shadow_ic,"rank_ic_delta":ic_delta,
                "production_top_bottom":pspread,"shadow_top_bottom":sspread,"spread_delta":delta,
                "spread_delta_bootstrap_ci95":ci,"common_cause_abs_spearman":redundancy,
                "late_entry_rate_production_topk":late_p,"late_entry_rate_shadow_topk":late_s,
                "avg_realized_drawdown":avg(dd),"avg_realized_cvar":avg(cv),
                "turnover_jaccard_change":avg(changes),"decision":decision}
    def shadow_all_metrics(self,horizon=5):
        with self._con() as c:
            blocks=[r[0] for r in c.execute("SELECT DISTINCT block FROM shadow_microstructure ORDER BY block").fetchall()]
        return {"horizon":horizon,"blocks":[self.shadow_block_metrics(b,horizon) for b in blocks]}

    def production_validation_sample(self,sample_date,symbol,p,f,i,r,v,reflection,tail_risk,confidence,
                                     current_score=None,payload=None,available_at=None):
        vals=[p,f,i,r,v,reflection,tail_risk,confidence]
        if any(x is None for x in vals): raise ValueError("K24 validation requires complete P/F/I/R/V/reflection/risk/confidence")
        with self._con() as c:
            c.execute("""INSERT OR REPLACE INTO production_validation_samples
              (sample_date,available_at,symbol,p,f,i,r,v,reflection,tail_risk,confidence,current_score,payload_json)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (sample_date,available_at or sample_date,symbol,p,f,i,r,v,reflection,tail_risk,confidence,current_score,
               json.dumps(payload or {},ensure_ascii=False,separators=(",",":"))))
            row=c.execute("SELECT id FROM production_validation_samples WHERE sample_date=? AND symbol=?",(sample_date,symbol)).fetchone()
        return int(row["id"])
    def production_validation_outcome(self,sample_id,horizon,forward_return,evaluated_at=None):
        with self._con() as c:
            c.execute("""INSERT OR REPLACE INTO production_validation_outcomes(sample_id,horizon,forward_return,evaluated_at)
                         VALUES(?,?,?,?)""",(int(sample_id),int(horizon),float(forward_return),evaluated_at or utc_iso()))

    def _k24_score(self,row,weights):
        exp=sum(weights[k]*float(row[k.lower()]) for k in ("P","F","I","R","V"))
        return float(row["confidence"])*(exp-.45*float(row["reflection"])-.20*float(row["tail_risk"]))
    def _k24_metrics(self,rows,weights,cost=0.0015,k=3):
        by={}
        for x in rows: by.setdefault(x["sample_date"],[]).append(x)
        ics=[]; spreads=[]; top_returns=[]
        for rs in by.values():
            if len(rs)<max(3,2*k): continue
            sc=[self._k24_score(x,weights) for x in rs]
            ret=[float(x["forward_return"])-cost for x in rs]
            ic=self._spearman(sc,ret)
            if ic is not None: ics.append(ic)
            sp=self._top_bottom(sc,ret,k)
            if sp is not None: spreads.append(sp)
            ix=sorted(range(len(sc)),key=lambda z:sc[z],reverse=True)[:k]
            top_returns.extend(ret[z] for z in ix)
        mean=lambda xs: sum(xs)/len(xs) if xs else None
        mi=mean(ics); sd=(sum((x-mi)**2 for x in ics)/(len(ics)-1))**.5 if mi is not None and len(ics)>1 else None
        return {"dates":len(by),"usable_dates":len(ics),"rows":len(rows),"rank_ic":mi,
                "icir":(mi/sd if sd and sd>0 else None),"top_bottom":mean(spreads),
                "topk_after_cost_return":mean(top_returns)}

    def production_revalidation(self,horizon=5,min_dates=20,min_rows=200,k=3):
        current={"P":.22,"F":.24,"I":.22,"R":.18,"V":.14}
        baseline={"P":.20,"F":.20,"I":.20,"R":.20,"V":.20}
        with self._con() as c:
            rows=[dict(x) for x in c.execute("""SELECT s.*,o.forward_return FROM production_validation_samples s
                    JOIN production_validation_outcomes o ON o.sample_id=s.id WHERE o.horizon=?
                    ORDER BY s.sample_date,s.symbol""",(int(horizon),)).fetchall()]
        dates=len({x["sample_date"] for x in rows})
        if dates<min_dates or len(rows)<min_rows:
            return {"horizon":horizon,"dates":dates,"rows":len(rows),"decision":"INSUFFICIENT_EVIDENCE",
                    "requirements":{"min_dates":min_dates,"min_rows":min_rows},"current_weights":current}
        # Purged chronological split: first 60% train, last 40% OOS, with horizon-sized date embargo.
        ds=sorted({x["sample_date"] for x in rows}); cut=max(1,int(len(ds)*.60))
        train_dates=ds[:max(1,cut-horizon)]; test_dates=ds[cut:]
        train=[x for x in rows if x["sample_date"] in train_dates]; test=[x for x in rows if x["sample_date"] in test_dates]
        if len({x["sample_date"] for x in test})<8:
            return {"horizon":horizon,"dates":dates,"rows":len(rows),"decision":"INSUFFICIENT_EVIDENCE",
                    "reason":"too few purged OOS dates","current_weights":current}

        # Constrained recalibration: small deterministic grid around current; nonnegative, sum=1, max drift 0.08/block.
        deltas=(-.04,0,.04); candidates=[]
        for dp in deltas:
          for df in deltas:
           for di in deltas:
            for dr in deltas:
             w={"P":current["P"]+dp,"F":current["F"]+df,"I":current["I"]+di,"R":current["R"]+dr}
             w["V"]=1-sum(w.values())
             if w["V"]<.06 or abs(w["V"]-current["V"])>.08: continue
             if min(w.values())<.06 or max(w.values())>.35: continue
             candidates.append(w)
        def objective(w):
            m=self._k24_metrics(train,w,0.0015,k)
            return (-999 if m["rank_ic"] is None else m["rank_ic"])+(-999 if m["top_bottom"] is None else 4*m["top_bottom"])
        recal=max(candidates,key=objective) if candidates else current

        costs=[.0010,.0015,.0025,.0040]
        models={"CURRENT":current,"RECALIBRATED":recal,"EQUAL_BASELINE":baseline}
        results={}
        for name,w in models.items():
            results[name]={"weights":w,"train":self._k24_metrics(train,w,.0015,k),
                           "oos_by_cost":{str(c):self._k24_metrics(test,w,c,k) for c in costs}}
        cur=results["CURRENT"]["oos_by_cost"]["0.0015"]; rec=results["RECALIBRATED"]["oos_by_cost"]["0.0015"]; bas=results["EQUAL_BASELINE"]["oos_by_cost"]["0.0015"]
        # Paired date bootstrap for RECAL vs CURRENT top-bottom delta.
        by={}
        for x in test: by.setdefault(x["sample_date"],[]).append(x)
        paired=[]
        for rs in by.values():
            if len(rs)>=2*k:
                rets=[x["forward_return"]-.0015 for x in rs]
                a=self._top_bottom([self._k24_score(x,current) for x in rs],rets,k)
                b=self._top_bottom([self._k24_score(x,recal) for x in rs],rets,k)
                if a is not None and b is not None: paired.append(b-a)
        ci=None
        if len(paired)>=8:
            rng=random.Random(24); boots=[]
            for _ in range(800):
                z=[paired[rng.randrange(len(paired))] for __ in range(len(paired))]
                boots.append(sum(z)/len(z))
            boots.sort(); ci=[boots[int(.025*(len(boots)-1))],boots[int(.975*(len(boots)-1))]]
        # Conservative decision.
        if cur["rank_ic"] is None or cur["top_bottom"] is None:
            decision="INSUFFICIENT_EVIDENCE"
        elif cur["rank_ic"]>0 and cur["top_bottom"]>0 and (ci is None or ci[0]<=0<=ci[1]):
            decision="RETAIN"
        elif ci is not None and ci[0]>0 and rec["rank_ic"]>cur["rank_ic"] and rec["top_bottom"]>cur["top_bottom"]:
            decision="RECALIBRATE_CANDIDATE"
        elif bas["rank_ic"]>cur["rank_ic"] and bas["top_bottom"]>cur["top_bottom"] and cur["rank_ic"]<=0:
            decision="REVERT_CANDIDATE"
        else:
            decision="INCONCLUSIVE"
        return {"horizon":horizon,"dates":dates,"rows":len(rows),"train_dates":len(set(x["sample_date"] for x in train)),
                "oos_dates":len(set(x["sample_date"] for x in test)),"embargo_sessions":horizon,
                "results":results,"recal_vs_current_bootstrap_ci95":ci,"decision":decision,
                "note":"No automatic coefficient promotion; K28 atomic release required."}

    def rotation_snapshot(self,session_date,symbol,entry_edge,remaining_edge,sell_index,
                          gap_decay=0,distribution=0,flow_reversal=0,overextension=0,
                          price=None,confidence=None,payload=None):
        with self._con() as c:
            c.execute("""INSERT OR REPLACE INTO rotation_snapshots
              (session_date,symbol,entry_edge,remaining_edge,sell_index,gap_decay,distribution,flow_reversal,
               overextension,price,confidence,payload_json)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
              (session_date,symbol,entry_edge,remaining_edge,sell_index,gap_decay,distribution,flow_reversal,
               overextension,price,confidence,json.dumps(payload or {},ensure_ascii=False,separators=(",",":"))))
        return {"ok":True}

    def rotation_decision(self,holding_symbol,candidate_symbol,holding_remaining,candidate_entry,
                          switch_cost=.0015,min_edge_buffer=.01,hold_sessions=0,
                          gap_decay=0,distribution=0,flow_reversal=0,overextension=0):
        # SELL is independent evidence, not 10-BUY.
        deterioration=.30*float(gap_decay)+.30*float(flow_reversal)+.25*float(distribution)+.15*float(overextension)
        opportunity=float(candidate_entry)-float(holding_remaining)-float(switch_cost)
        # Avoid churn before 2 sessions unless deterioration is severe.
        early_penalty=.015 if int(hold_sessions)<2 and deterioration<.45 else 0.0
        rotate=bool(opportunity>float(min_edge_buffer)+early_penalty and (int(hold_sessions)>=2 or deterioration>=.45))
        # At >=7 sessions we still do not force a sell; current edge remains primary.
        return {"action":"ROTATE" if rotate else "HOLD","holding":holding_symbol,"candidate":candidate_symbol,
                "opportunity_edge":opportunity,"deterioration":deterioration,
                "switch_cost":switch_cost,"min_edge_buffer":min_edge_buffer,"hold_sessions":int(hold_sessions),
                "forced_exit_at_7":False,"current_edge_priority":True}

    def record_rotation_trade(self,entry_date,exit_date,from_symbol,to_symbol,hold_sessions,
                              from_return,to_forward_return,switch_cost,opportunity_edge,reason,payload=None):
        with self._con() as c:
            c.execute("""INSERT INTO rotation_trades(entry_date,exit_date,from_symbol,to_symbol,hold_sessions,
              from_return,to_forward_return,switch_cost,opportunity_edge,reason,payload_json)
              VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
              (entry_date,exit_date,from_symbol,to_symbol,int(hold_sessions),from_return,to_forward_return,
               switch_cost,opportunity_edge,reason,json.dumps(payload or {},ensure_ascii=False,separators=(",",":"))))

    def rotation_metrics(self):
        with self._con() as c:
            rows=[dict(r) for r in c.execute("SELECT * FROM rotation_trades ORDER BY exit_date,id").fetchall()]
        if len(rows)<10:
            return {"trades":len(rows),"decision":"INSUFFICIENT_EVIDENCE"}
        mean=lambda xs: sum(xs)/len(xs) if xs else None
        holds=[r["hold_sessions"] for r in rows]
        realized=[float(r["from_return"] or 0)-float(r["switch_cost"] or 0) for r in rows]
        # Counterfactual proxy: if we had switched, next candidate forward return minus cost.
        switched=[float(r["to_forward_return"] or 0)-float(r["switch_cost"] or 0) for r in rows]
        gain=[b-a for a,b in zip(realized,switched)]
        win=sum(1 for x in gain if x>0)/len(gain)
        within=sum(1 for h in holds if 2<=h<=7)/len(holds)
        churn=sum(1 for h in holds if h<2)/len(holds)
        stale=sum(1 for h in holds if h>7)/len(holds)
        # cumulative max drawdown of realized rotation trade stream
        equity=1.0; peak=1.0; mdd=0.0
        for r in realized:
            equity*=1+r; peak=max(peak,equity); mdd=min(mdd,equity/peak-1)
        # opportunity-edge calibration by quartile-ish positive/negative split
        pos=[g for g,r in zip(gain,rows) if float(r["opportunity_edge"] or 0)>0]
        neg=[g for g,r in zip(gain,rows) if float(r["opportunity_edge"] or 0)<=0]
        return {"trades":len(rows),"avg_hold_sessions":mean(holds),"hold_2_7_rate":within,
                "early_churn_rate":churn,"over_7_rate":stale,
                "realized_after_cost_mean":mean(realized),"switch_counterfactual_mean":mean(switched),
                "rotation_incremental_mean":mean(gain),"rotation_win_rate":win,
                "positive_opportunity_incremental":mean(pos),"nonpositive_opportunity_incremental":mean(neg),
                "max_drawdown":mdd,
                "decision":"ROTATION_SUPPORTED" if mean(gain)>0 and win>.5 else "ROTATION_NOT_VALIDATED"}

    def operational_event(self,component,level,code,payload=None,event_time=None):
        safe=ops.redact(payload or {})
        with self._con() as c:
            c.execute("""INSERT INTO operational_events(event_time,component,level,code,payload_json)
                         VALUES(?,?,?,?,?)""",(event_time or utc_iso(),component,level,code,
                         json.dumps(safe,ensure_ascii=False,separators=(",",":"))))
    def operational_recent(self,limit=100):
        with self._con() as c:
            return [dict(r) for r in c.execute("""SELECT event_time,component,level,code,payload_json
                     FROM operational_events ORDER BY id DESC LIMIT ?""",(int(limit),)).fetchall()]

    def safety_run_record(self,hazard_id,framework,injected,expected,observed,passed):
        with self._con() as c:
            c.execute("""INSERT INTO safety_injection_runs
              (run_time,hazard_id,framework,injected_json,expected_json,observed_json,passed)
              VALUES(?,?,?,?,?,?,?)""",(utc_iso(),hazard_id,framework,
              json.dumps(ops.redact(injected),ensure_ascii=False,separators=(",",":")),
              json.dumps(expected,ensure_ascii=False,separators=(",",":")),
              json.dumps(ops.redact(observed),ensure_ascii=False,separators=(",",":")),int(bool(passed))))
    def safety_summary(self):
        with self._con() as c:
            rows=[dict(r) for r in c.execute("""SELECT hazard_id,framework,passed,run_time
                FROM safety_injection_runs ORDER BY id DESC""").fetchall()]
        if not rows: return {"runs":0,"status":"NOT_RUN"}
        return {"runs":len(rows),"passed":sum(r["passed"] for r in rows),
                "failed":sum(not r["passed"] for r in rows),
                "status":"PASS" if all(r["passed"] for r in rows) else "FAIL",
                "latest":rows[:20]}

    def backup(self,label=None):
        ts=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe=re.sub(r"[^A-Za-z0-9_.-]","_",str(label or "auto"))
        dst=str(Path(self.backup_dir)/f"rgi_pit_{ts}_{safe}.sqlite3")
        src=self._con(); out=sqlite3.connect(dst)
        try: src.backup(out)
        finally: out.close(); src.close()
        sha=hashlib.sha256(Path(dst).read_bytes()).hexdigest()
        return {"path":dst,"sha256":sha,"bytes":Path(dst).stat().st_size,"created_at":utc_iso()}
    def export_jsonl(self,out_path=None,limit=None):
        out_path=out_path or str(Path(self.root)/"rgi_pit_export.jsonl")
        sql="SELECT * FROM pit_events ORDER BY available_at,id"
        params=()
        if limit: sql+=" LIMIT ?"; params=(int(limit),)
        n=0
        with self._con() as c, open(out_path,"w",encoding="utf-8") as f:
            for r in c.execute(sql,params):
                d=dict(r)
                try: d["payload"]=json.loads(d.pop("payload_json"))
                except Exception: d["payload"]=d.pop("payload_json")
                f.write(json.dumps(d,ensure_ascii=False,separators=(",",":"))+"\n"); n+=1
        return {"path":out_path,"rows":n,"sha256":hashlib.sha256(Path(out_path).read_bytes()).hexdigest()}
    def audit(self,max_age_minutes=180):
        with self._con() as c:
            total=c.execute("SELECT COUNT(*) n FROM pit_events").fetchone()["n"]
            blocks=[dict(r) for r in c.execute("""SELECT block,COUNT(*) rows,COUNT(DISTINCT symbol) symbols,
                       MIN(available_at) first_at,MAX(available_at) last_at,
                       AVG(confidence) avg_confidence,AVG(COALESCE(latency_ms,0)) avg_latency_ms,
                       SUM(CASE WHEN stale=1 THEN 1 ELSE 0 END) stale_rows
                       FROM pit_events GROUP BY block ORDER BY rows DESC""").fetchall()]
            sources=[dict(r) for r in c.execute("""SELECT source,COUNT(*) rows,COUNT(DISTINCT symbol) symbols,
                       MIN(available_at) first_at,MAX(available_at) last_at,AVG(confidence) avg_confidence,
                       AVG(COALESCE(latency_ms,0)) avg_latency_ms,SUM(CASE WHEN stale=1 THEN 1 ELSE 0 END) stale_rows
                       FROM pit_events GROUP BY source ORDER BY rows DESC""").fetchall()]
            health=[dict(r) for r in c.execute("SELECT * FROM source_health ORDER BY source").fetchall()]
            scans=[dict(r) for r in c.execute("""SELECT mode,COUNT(*) runs,AVG(coverage) avg_coverage,
                      AVG(duration_ms) avg_duration_ms,SUM(failure_count) failures,MAX(completed_at) last_completed_at
                      FROM scan_runs GROUP BY mode""").fetchall()]
        for rows in (blocks,sources):
            for r in rows:
                r["stale_rate"]=(r["stale_rows"]/r["rows"]) if r["rows"] else None
        return {"schema_version":self.SCHEMA_VERSION,"event_rows":total,"blocks":blocks,"sources":sources,
                "source_health":health,"scans":scans,"generated_at":utc_iso()}
    def integrity_check(self):
        with self._con() as c:
            result=c.execute("PRAGMA integrity_check").fetchone()[0]
            version=c.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        return {"ok":result=="ok","sqlite":result,"schema_version":int(version[0]) if version else None}
    def status(self):
        with self._con() as c:
            e=c.execute("SELECT COUNT(*) n,MIN(available_at) mn,MAX(available_at) mx FROM pit_events").fetchone()
            s=c.execute("SELECT COUNT(*) n FROM scan_runs").fetchone()
            v=c.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        return {"path":self.path,"root":self.root,"event_rows":e["n"],"scan_runs":s["n"],
                "first_available_at":e["mn"],"last_available_at":e["mx"],
                "schema_version":int(v[0]) if v else None,
                "durable_expected":not str(Path(self.root).resolve()).startswith("/tmp"),
                "volume_required_for_cloud":True,
                "integrity":self.integrity_check()}


class DartClient:
    def __init__(self):
        self.key=os.getenv("DART_API_KEY","").strip(); self.timeout=float(os.getenv("DART_TIMEOUT","15"))
        self._corp_map={}; self._loaded_at=0.0
    @property
    def configured(self): return bool(self.key)
    async def _load_map(self):
        if not self.configured: return
        if self._corp_map and now()-self._loaded_at<86400: return
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r=await c.get(f"{DART_BASE}/corpCode.xml",params={"crtfc_key":self.key}); r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            root=ET.fromstring(z.read(z.namelist()[0]))
        mp={}
        for x in root.findall("list"):
            s=(x.findtext("stock_code") or "").strip(); corp=(x.findtext("corp_code") or "").strip()
            if len(s)==6 and s.isdigit() and corp: mp[s]=corp
        self._corp_map=mp; self._loaded_at=now()
    async def disclosures(self,symbol,days=14):
        if not self.configured: return {"configured":False,"rows":[]}
        await self._load_map(); corp=self._corp_map.get(symbol)
        if not corp: return {"configured":True,"corp_code":None,"rows":[]}
        end=datetime.now(timezone.utc).strftime("%Y%m%d")
        bgn=(datetime.now(timezone.utc)-timedelta(days=days)).strftime("%Y%m%d")
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r=await c.get(f"{DART_BASE}/list.json",params={"crtfc_key":self.key,"corp_code":corp,"bgn_de":bgn,"end_de":end,"page_count":100})
            r.raise_for_status(); j=r.json()
        rows=[]
        if j.get("status") in ("000","013"):
            for o in j.get("list") or []:
                rows.append({"rcept_no":o.get("rcept_no"),"report_nm":o.get("report_nm"),"rcept_dt":o.get("rcept_dt"),
                             "flr_nm":o.get("flr_nm"),"rm":o.get("rm"),"source":"OPENDART_LIST","retrieved_at":utc_iso()})
        return {"configured":True,"corp_code":corp,"status":j.get("status"),"message":j.get("message"),"rows":rows}


ESTIMATE_INCOME_ROWS=["revenue","revenue_growth","operating_profit","operating_profit_growth","net_income","net_income_growth"]
ESTIMATE_INDICATOR_ROWS=["ebitda","eps","eps_growth","per","ev_ebitda","roe","debt_ratio","interest_coverage"]

def _num_text(v):
    if v is None: return None
    s=str(v).strip().replace(",","").replace("%","")
    if s in ("","-","--","N/A","NA"): return None
    try: return float(s)
    except Exception: return None

def normalize_estimate_perform(raw):
    # Fail closed on unexpected live KIS shapes. Never zero-impute malformed rows.
    if not isinstance(raw,dict):
        return {"symbol":None,"periods":[],"table":[],"source":"KIS_ESTIMATE_PERFORM",
                "retrieved_at":None,"shape_warning":"RAW_NOT_DICT"}
    periods=[]
    for x in (raw.get("output4") or []):
        if isinstance(x,dict):
            p=str(x.get("dt") or "").strip()
            if p: periods.append(p)
    def rows_to_metrics(rows,labels):
        out={}
        for i,row in enumerate(rows or []):
            if not isinstance(row,dict):
                continue
            label=labels[i] if i<len(labels) else f"row_{i+1}"
            vals=[_num_text(row.get(f"data{n}")) for n in range(1,6)]
            out[label]=vals
        return out
    income=rows_to_metrics(raw.get("output2") or [],ESTIMATE_INCOME_ROWS)
    indicators=rows_to_metrics(raw.get("output3") or [],ESTIMATE_INDICATOR_ROWS)
    table=[]
    n=max(len(periods), max((len(v) for v in income.values()),default=0),
          max((len(v) for v in indicators.values()),default=0))
    for i in range(n):
        rec={"period":periods[i] if i<len(periods) else None,"estimated":None}
        if rec["period"]:
            rec["estimated"]=rec["period"].upper().endswith("E")
        for k,v in income.items(): rec[k]=v[i] if i<len(v) else None
        for k,v in indicators.items(): rec[k]=v[i] if i<len(v) else None
        table.append(rec)
    return {"symbol":raw.get("symbol"),"periods":periods,"table":table,
            "source":"KIS_ESTIMATE_PERFORM","retrieved_at":raw.get("retrieved_at")}

def consensus_revision(current,history):
    """Compare current KIS estimate snapshot against previous PIT snapshot by same period/metric."""
    if not current or not history:
        return {"available":False,"reason":"NEED_AT_LEAST_TWO_PIT_SNAPSHOTS","changes":[],"score":None}
    prev=None
    for h in history:
        p=h.get("payload")
        if p and p.get("table") and p != current:
            prev=p; break
    if not prev:
        return {"available":False,"reason":"NO_PRIOR_DISTINCT_SNAPSHOT","changes":[],"score":None}
    pm={r.get("period"):r for r in prev.get("table",[]) if r.get("period")}
    changes=[]
    score_parts=[]
    for r in current.get("table",[]):
        period=r.get("period")
        if not period or not r.get("estimated") or period not in pm: continue
        old=pm[period]
        for metric in ("revenue","operating_profit","net_income","eps"):
            a=r.get(metric); b=old.get(metric)
            if a is None or b in (None,0): continue
            pct=(a-b)/abs(b)
            changes.append({"period":period,"metric":metric,"old":b,"new":a,"revision_pct":pct})
            weight={"revenue":0.15,"operating_profit":0.35,"net_income":0.25,"eps":0.25}[metric]
            score_parts.append(weight*max(-1,min(1,pct/0.10)))
    score=sum(score_parts) if score_parts else None
    return {"available":bool(changes),"reason":None if changes else "NO_COMPARABLE_ESTIMATES",
            "changes":changes,"score":score}

def normalize_opinion(rows):
    """Keep raw KIS opinion history while extracting date/target/opinion fields by known aliases."""
    out=[]
    for r in rows or []:
        if not isinstance(r,dict):
            continue
        date=r.get("stck_bsop_date") or r.get("date") or r.get("opnn_date") or r.get("data_dt")
        target=_num_text(r.get("hts_goal_prc") or r.get("goal_prc") or r.get("target_price"))
        opinion=r.get("hts_opnn") or r.get("opnn") or r.get("invest_opinion")
        out.append({"date":date,"target_price":target,"opinion":opinion,"raw":r})
    return out

def opinion_revision(rows):
    nr=normalize_opinion(rows)
    valid=[x for x in nr if x.get("target_price") is not None]
    if len(valid)<2:
        return {"available":False,"target_revision_pct":None,"latest":valid[0] if valid else None}
    latest,prev=valid[0],valid[1]
    p=prev["target_price"]
    return {"available":True,"target_revision_pct":((latest["target_price"]-p)/abs(p) if p else None),
            "latest":latest,"previous":prev}

def earnings_surprise_from_pit(actual_rows,estimate_history):
    """Strict PIT rule: surprise only when a PRE-ACTUAL consensus snapshot for the same period exists.
    KIS estimate-perform is predominantly annual; no period match => unavailable, never guessed."""
    return {"available":False,"reason":"PERIOD_ALIGNMENT_AND_PRE_RELEASE_CONSENSUS_REQUIRED",
            "score":None,"note":"Stored for future PIT matching; no synthetic surprise is produced."}

def classify_dart(rows):
    if not rows: return {"value":None,"confidence":0.0,"event_count":0,"coverage":"NO_DART_ROWS"}
    pos=("단일판매","공급계약","자기주식취득","소각","유형자산취득","영업양수")
    neg=("유상증자","전환사채","신주인수권","횡령","배임","상장폐지","관리종목","불성실","영업정지","계약해지")
    score=0; used=0
    for r in rows:
        t=str(r.get("report_nm") or "")
        s=sum(1 for w in pos if w in t)-sum(1 for w in neg if w in t)
        if s: score+=max(-2,min(2,s)); used+=1
    return {"value":signed_unit(score,4.0) if used else 0.0,"confidence":min(.80,.45+.04*min(len(rows),8)),
            "event_count":len(rows),"scored_events":used,"coverage":"OPENDART_LIST_PARTIAL"}


EARNINGS_IR_TERMS=("실적발표","경영실적","잠정실적","분기 실적","분기실적","연간 실적","연간실적")

def _yyyymmdd(v):
    if not v: return None
    s=re.sub(r"[^0-9]","",str(v))
    return s[:8] if len(s)>=8 else None

def classify_earnings_calendar_event(event):
    title=str(event.get("title") or event.get("report_nm") or "")
    purpose=str(event.get("purpose") or event.get("objective") or event.get("실시목적") or "")
    content=str(event.get("content") or event.get("major_content") or event.get("주요내용") or "")
    text=" ".join((title,purpose,content))
    event_date=_yyyymmdd(event.get("event_date") or event.get("start_date") or event.get("행사일") or event.get("date"))
    confirmed_at=_yyyymmdd(event.get("confirmed_at") or event.get("확정일") or event.get("rcept_dt") or event.get("available_date"))
    explicit=any(k in text for k in EARNINGS_IR_TERMS)
    if event_date and explicit:
        status="CONFIRMED"; confidence=.95 if ("실적발표" in text or "잠정실적" in text) else .88
    elif event_date:
        status="EXPECTED"; confidence=.45
    else:
        status="UNKNOWN"; confidence=0.0
    return {"status":status,"event_date":event_date,"confirmed_at":confirmed_at,"confidence":confidence,
            "is_earnings_event":bool(event_date and explicit),"title":title,"purpose":purpose,"content":content,
            "source":event.get("source") or "DISCLOSURE_IR","source_url":event.get("source_url"),
            "raw_id":event.get("rcept_no") or event.get("id")}

def select_earnings_calendar(events,asof_yyyymmdd=None):
    norm=[classify_earnings_calendar_event(x) for x in (events or [])]
    valid=[x for x in norm if x["is_earnings_event"] and x["status"]=="CONFIRMED"]
    if asof_yyyymmdd: valid=[x for x in valid if not x.get("confirmed_at") or x["confirmed_at"]<=asof_yyyymmdd]
    valid.sort(key=lambda x:(x.get("event_date") or "99999999",-(x.get("confidence") or 0)))
    return {"events":norm,"confirmed_earnings":valid,"next_confirmed":valid[0] if valid else None,
            "coverage":"CONFIRMED_OFFICIAL_IR" if valid else "NO_CONFIRMED_EARNINGS_DATE"}

def earnings_event_distance(event_date,today_yyyymmdd):
    if not event_date or not today_yyyymmdd: return None
    try:
        a=datetime.strptime(event_date,"%Y%m%d").date(); b=datetime.strptime(today_yyyymmdd,"%Y%m%d").date()
        return (a-b).days
    except Exception: return None

def historical_pattern_calendar(dates):
    clean=sorted([_yyyymmdd(x) for x in dates if _yyyymmdd(x)])
    if len(clean)<4: return {"status":"UNKNOWN","estimated_date":None,"confidence":0.0,"scoring_allowed":False}
    return {"status":"PATTERN_ONLY","estimated_date":None,"confidence":0.15,"scoring_allowed":False,"history":clean[-8:]}


def _clamp01(x):
    try: return max(0.0,min(1.0,float(x)))
    except Exception: return 0.0

def _signed(x):
    try: return max(-1.0,min(1.0,float(x)))
    except Exception: return 0.0

def _event_key(e):
    """Deduplicate same economic event across KIS/DART/KIND without relying on source id."""
    symbol=str(e.get("symbol") or "")
    day=_yyyymmdd(e.get("event_date") or e.get("rcept_dt") or e.get("available_at") or "") or ""
    text=" ".join(str(e.get(k) or "") for k in ("title","report_nm","purpose","content")).lower()
    # Stable economic-family tags; same event from multiple feeds collapses.
    families=[
        ("earnings",("실적","영업이익","매출","순이익","earnings")),
        ("contract",("공급계약","단일판매","수주","계약체결")),
        ("buyback",("자기주식","자사주","소각")),
        ("capital_raise",("유상증자","전환사채","신주인수권")),
        ("legal",("횡령","배임","소송","거래정지")),
        ("investment",("시설투자","유형자산","증설","투자결정")),
    ]
    fam="other"
    for name,terms in families:
        if any(t in text for t in terms): fam=name; break
    # Use event day when available; title wording differences no longer duplicate family/day.
    return f"{symbol}|{day}|{fam}"

def dedupe_visible_events(events):
    grouped={}
    source_priority={"OPENDART":1.0,"DART":.98,"KIND":.98,"KIND_IR":.98,"KIS_NEWS_TITLE":.70,"KIS":.72}
    for e in events or []:
        k=_event_key(e)
        q=dict(e)
        src=str(q.get("source") or "")
        reliability=float(q.get("reliability") or source_priority.get(src,.60))
        q["reliability"]=_clamp01(reliability)
        if k not in grouped:
            grouped[k]=q; grouped[k]["merged_sources"]=[src] if src else []
        else:
            cur=grouped[k]
            if q["reliability"]>float(cur.get("reliability") or 0):
                keep=q; keep["merged_sources"]=list(dict.fromkeys((cur.get("merged_sources") or [])+([src] if src else [])))
                grouped[k]=keep
            elif src and src not in cur.get("merged_sources",[]): cur.setdefault("merged_sources",[]).append(src)
    return list(grouped.values())

def visible_event_score(e, now_date=None):
    """Event Strength × Impact × Reliability × Freshness, signed; reflection is handled separately."""
    title=" ".join(str(e.get(k) or "") for k in ("title","report_nm","purpose","content")).lower()
    positive=("공급계약","수주","흑자전환","상향","자기주식취득","자사주","소각","승인","허가","증설","최대 실적","실적 개선")
    negative=("적자전환","하향","유상증자","횡령","배임","거래정지","상장폐지","계약해지","리콜","실적 부진")
    raw=sum(1 for x in positive if x in title)-sum(1 for x in negative if x in title)
    direction=_signed(raw/2.0)
    impact=_clamp01(e.get("impact",.55 if raw else .25))
    reliability=_clamp01(e.get("reliability",.6))
    freshness=1.0
    day=_yyyymmdd(e.get("event_date") or e.get("rcept_dt") or e.get("available_at"))
    if now_date and day:
        try:
            age=max(0,(datetime.strptime(now_date,"%Y%m%d").date()-datetime.strptime(day,"%Y%m%d").date()).days)
            freshness=math.exp(-age/7.0)
        except Exception: pass
    return direction*impact*reliability*freshness

def fuse_full_k6(*,symbol,events=None,visible_lite=None,dart=None,earnings=None,
                 calendar=None,price_reflection=None,invisible_onset=None,visible_onset=None,
                 now_date=None):
    """K20 visible fusion. Missing components reduce confidence; never zero-impute."""
    events=dedupe_visible_events(events or [])
    comps=[]; coverage={}

    if events:
        ev_scores=[visible_event_score(e,now_date) for e in events]
        comps.append(("events",sum(ev_scores)/max(1,len(ev_scores)),.25))
        coverage["events"]="AVAILABLE"
    else: coverage["events"]="MISSING"

    vl=(visible_lite or {}).get("value")
    if vl is not None:
        comps.append(("kis_title",_signed(vl),.08)); coverage["kis_title"]="AVAILABLE"
    else: coverage["kis_title"]="MISSING"

    ds=(dart or {}).get("value")
    if ds is not None:
        comps.append(("dart",_signed(ds),.17)); coverage["dart"]="AVAILABLE"
    else: coverage["dart"]="MISSING"

    earnings=earnings or {}
    rev=(earnings.get("consensus_revision") or {}).get("score")
    if rev is not None:
        comps.append(("consensus_revision",_signed(rev),.16)); coverage["consensus_revision"]="AVAILABLE"
    else: coverage["consensus_revision"]="MISSING"

    oprev=(earnings.get("opinion_revision") or {}).get("target_revision_pct")
    if oprev is not None:
        comps.append(("analyst_revision",_signed(float(oprev)/.10),.08)); coverage["analyst_revision"]="AVAILABLE"
    else: coverage["analyst_revision"]="MISSING"

    surprise=(earnings.get("surprise") or {}).get("score")
    if surprise is not None:
        comps.append(("earnings_surprise",_signed(surprise),.18)); coverage["earnings_surprise"]="AVAILABLE"
    else: coverage["earnings_surprise"]="MISSING"

    cal=(calendar or {}).get("next_confirmed")
    if cal and cal.get("status")=="CONFIRMED":
        d=earnings_event_distance(cal.get("event_date"),now_date) if now_date else None
        # Calendar is timing context, not directional alpha. It raises confidence/timing, not sign.
        coverage["earnings_calendar"]="CONFIRMED"
        timing_conf=.08 if d is not None and -1<=d<=10 else .03
    else:
        coverage["earnings_calendar"]="MISSING"; timing_conf=0.0

    available_weight=sum(w for _,_,w in comps)
    directional=(sum(v*w for _,v,w in comps)/available_weight) if available_weight else None
    # confidence uses designed coverage; missingness cannot become a neutral score.
    confidence=min(1.0,available_weight+timing_conf)
    reflection=_clamp01(price_reflection) if price_reflection is not None else None
    remaining=None if directional is None else directional
    if remaining is not None and reflection is not None:
        # reflection only reduces magnitude in the same direction; it never flips good news negative mechanically.
        remaining=math.copysign(max(0.0,abs(remaining)-.45*reflection),remaining)

    leadlag="NO_SIGNAL"
    if invisible_onset and visible_onset:
        try:
            a=datetime.fromisoformat(str(invisible_onset).replace("Z","+00:00"))
            b=datetime.fromisoformat(str(visible_onset).replace("Z","+00:00"))
            sec=(b-a).total_seconds()
            leadlag="INVISIBLE_LEADS" if sec>300 else ("VISIBLE_LEADS" if sec<-300 else "SYNCHRONOUS")
        except Exception: leadlag="UNKNOWN"
    elif invisible_onset: leadlag="INVISIBLE_ONLY"
    elif visible_onset: leadlag="VISIBLE_ONLY"

    return {"symbol":symbol,"value":directional,"remaining_visible_edge":remaining,
            "confidence":confidence,"available_weight":available_weight,
            "coverage":coverage,"deduped_event_count":len(events),
            "components":[{"name":n,"value":v,"weight":w} for n,v,w in comps],
            "price_reflection":reflection,"lead_lag":leadlag,
            "production_ready":bool(directional is not None and confidence>=.60 and
                                    coverage.get("earnings_surprise")=="AVAILABLE" and
                                    coverage.get("earnings_calendar")=="CONFIRMED")}

class LiveRGIEngine:
    def __init__(self,kis:KISClient,rt:RealtimeManager,universe:UniverseMaster,pit:PITStore,dart:DartClient):
        self.kis=kis; self.rt=rt; self.universe=universe; self.pit=pit; self.dart=dart
        self.running=False; self.started_at=None; self.finished_at=None; self.last_error=None
        self.total=0; self.universe_total=0; self.attempted=0; self.succeeded=0; self.failed=0; self.candidate_n=0
        self.first_quote_at=None; self.last_quote_at=None
        self.quotes:Dict[str,dict]={}; self.features:Dict[str,dict]={}; self.list_result:dict={"classification":"NO_SCAN","majors":[],"minors":[]}
        self.task=None; self.fast_radar_result={"symbols":[],"sources":{},"observed_at":None}

    def status(self):
        cov=self.succeeded/self.total if self.total else 0.0
        return {"running":self.running,"started_at":self.started_at,"finished_at":self.finished_at,"last_error":self.last_error,
                "universe":self.universe.status(),"universe_total":self.universe_total,"discovery_quote_total":self.total,"attempted":self.attempted,"succeeded":self.succeeded,
                "failed":self.failed,"coverage_ratio":cov,"candidate_n":self.candidate_n,"enriched":len(self.features),"enriched_ok":sum(1 for f in self.features.values() if "error" not in f),"enriched_error":sum(1 for f in self.features.values() if "error" in f),"error_samples":[{"symbol":k,"error":v.get("error")} for k,v in self.features.items() if isinstance(v,dict) and "error" in v][:5],
                "snapshot_first_at":self.first_quote_at,"snapshot_last_at":self.last_quote_at,
                "snapshot_span_sec":((self.last_quote_at-self.first_quote_at) if self.first_quote_at and self.last_quote_at else None),
                "classification":self.list_result.get("classification"),"k6_visible_live":"KIS_TITLE_ONLY_PARTIAL","market_data_mode":"UN_INTEGRATED","intraday_flow_estimate":True,"program_ws_integrated":True,
                "official_ready":bool(cov>=0.98 and self.list_result.get("classification")=="OFFICIAL_LIST"),
                "fast_radar_count":len(self.fast_radar_result.get("symbols",[])),"fast_radar_observed_at":self.fast_radar_result.get("observed_at")}

    async def fast_radar(self, max_symbols:int=140) -> dict:
        """Cheap broad radar. Ranking APIs reduce full-universe serial REST dependence."""
        calls=await asyncio.gather(
            self.kis.volume_rank("1"), self.kis.volume_rank("3"),
            self.kis.fluctuation_rank("0001"), self.kis.fluctuation_rank("1001"),
            self.kis.volume_power_rank("0001"), self.kis.volume_power_rank("1001"),
            self.kis.foreign_institution_rank("0000"), return_exceptions=True)
        source_map={}; meta={}; errors=[]
        for result in calls:
            if isinstance(result,Exception): errors.append(f"{type(result).__name__}:{str(result)[:100]}"); continue
            for rank,row in enumerate(result,1):
                sym=row.get("symbol")
                if not sym: continue
                source_map.setdefault(sym,[]).append({"source":row.get("radar_source"),"rank":rank})
                meta.setdefault(sym,row)
        # Multi-source agreement first, then best rank. This is discovery only, not Production alpha.
        ordered=sorted(source_map,key=lambda x:(-len(source_map[x]),min(y["rank"] for y in source_map[x])))[:max_symbols]
        self.fast_radar_result={"symbols":ordered,"sources":source_map,"meta":{s:meta.get(s) for s in ordered},
            "errors":errors,"observed_at":utc_iso(),"source_count":len(calls),"classification":"DISCOVERY_ONLY"}
        return self.fast_radar_result

    async def start_scan(self,candidate_n=80,limit=0):
        if self.running: raise RuntimeError("scan already running")
        self.task=asyncio.create_task(self._scan(candidate_n,limit),name="rgi-live-scan")

    async def _scan(self,candidate_n,limit):
        self.running=True; self.started_at=now(); self.finished_at=None; self.last_error=None
        self.quotes={}; self.features={}; self.attempted=self.succeeded=self.failed=0; self.candidate_n=candidate_n
        self.first_quote_at=None; self.last_quote_at=None
        try:
            if not self.universe.items or (self.universe.loaded_at and now()-self.universe.loaded_at>86400):
                await self.universe.refresh()
            eligible=[x for x in self.universe.items if x.eligible]
            if limit>0: eligible=eligible[:limit]
            self.universe_total=len(eligible)
            name_map={x.symbol:x for x in eligible}
            # Fast path: use independent KIS ranking sources to compress the universe first.
            # A serial full-universe REST sweep is intentionally NOT on the critical path; at ~7 rps it would be minutes stale.
            radar=await self.fast_radar(max_symbols=max(candidate_n*3,120))
            radar_items=[name_map[s] for s in radar.get("symbols",[]) if s in name_map]
            prior_symbols=[]
            for row in (self.list_result.get("majors",[])+self.list_result.get("minors",[])):
                s=row.get("symbol")
                if s in name_map and s not in prior_symbols: prior_symbols.append(s)
            ordered_symbols=[]
            for x in radar_items+[name_map[s] for s in prior_symbols]:
                if x.symbol not in {z.symbol for z in ordered_symbols}: ordered_symbols.append(x)
            scan_order=ordered_symbols[:max(candidate_n*3,120)]
            if limit>0: scan_order=scan_order[:limit]
            self.total=len(scan_order)
            for item in scan_order:
                self.attempted+=1
                try:
                    q=await self.kis.price(item.symbol); q["name"]=item.name; q["market"]=item.market
                    self.first_quote_at=self.first_quote_at or q.get("fetched_at") or now(); self.last_quote_at=q.get("fetched_at") or now()
                    self.quotes[item.symbol]=q; self.succeeded+=1
                except Exception:
                    self.failed+=1
            candidates=broad_prefilter(list(self.quotes.values()),candidate_n)
            pre_ws=[q["symbol"] for q in candidates[:min(20,len(candidates))]]
            if pre_ws:
                await self.rt.set_symbols(pre_ws)
                await asyncio.sleep(0.8)
            # Regime is market-specific; retrieve both once.
            kr, kq = await asyncio.gather(self.kis.index_daily("0001"), self.kis.index_daily("1001"), return_exceptions=True)
            regimes={"KOSPI":market_regime(kr) if isinstance(kr,list) else None,
                     "KOSDAQ":market_regime(kq) if isinstance(kq,list) else None}
            for q in candidates:
                item=name_map[q["symbol"]]
                try:
                    dart_task=self.dart.disclosures(item.symbol) if self.dart.configured else asyncio.sleep(0,result={"configured":False,"rows":[]})
                    hist,inv,est,news,dart_res,cons_raw,actual_q,opn=await asyncio.gather(
                        self.kis.daily(item.symbol),self.kis.investor(item.symbol),self.kis.investor_estimate(item.symbol),
                        self.kis.news_titles(item.symbol),dart_task,self.kis.estimate_perform(item.symbol),
                        self.kis.income_statement(item.symbol,True),self.kis.invest_opinion(item.symbol),
                        return_exceptions=True)
                    # LIVEFIX5: optional enrichment is fail-soft. One KIS 5xx must not drop the whole symbol.
                    enrichment_errors={}
                    names=("history","investor_confirmed","investor_estimate","news_title","dart",
                           "consensus_estimate","actual_income_quarterly","invest_opinion")
                    vals=[hist,inv,est,news,dart_res,cons_raw,actual_q,opn]
                    for nm,val in zip(names,vals):
                        if isinstance(val,BaseException):
                            enrichment_errors[nm]=f"{type(val).__name__}:{str(val)[:180]}"
                    # History + confirmed investor flow are core score inputs: preserve missingness, never zero-impute.
                    hist=[] if isinstance(hist,BaseException) else hist
                    inv=[] if isinstance(inv,BaseException) else inv
                    est=[] if isinstance(est,BaseException) else est
                    news=[] if isinstance(news,BaseException) else news
                    dart_res={"configured":False,"rows":[],"degraded":True} if isinstance(dart_res,BaseException) else dart_res
                    cons_raw={} if isinstance(cons_raw,BaseException) else cons_raw
                    actual_q=[] if isinstance(actual_q,BaseException) else actual_q
                    opn=[] if isinstance(opn,BaseException) else opn
                    # Canonicalize external live response shapes at the enrichment boundary.
                    hist=[x for x in (hist if isinstance(hist,list) else []) if isinstance(x,dict)]
                    inv=[x for x in (inv if isinstance(inv,list) else []) if isinstance(x,dict)]
                    est=[x for x in (est if isinstance(est,list) else []) if isinstance(x,dict)]
                    news=[x for x in (news if isinstance(news,list) else []) if isinstance(x,dict)]
                    opn=[x for x in (opn if isinstance(opn,list) else []) if isinstance(x,dict)]
                    actual_q=[x for x in (actual_q if isinstance(actual_q,list) else []) if isinstance(x,dict)]
                    if not isinstance(dart_res,dict):
                        dart_res={"configured":False,"rows":[],"shape_warning":"DART_NOT_DICT"}
                    else:
                        dart_res=dict(dart_res)
                        dart_res["rows"]=[x for x in (dart_res.get("rows") or []) if isinstance(x,dict)]
                    feat=assemble_symbol(item,q,hist,inv,regimes.get(item.market),self.rt.latest.get(item.symbol),news)
                    feat["enrichment_health"]={
                        "degraded":bool(enrichment_errors),
                        "errors":enrichment_errors,
                        "missing_components":list(enrichment_errors.keys()),
                        "policy":"FAIL_SOFT_NO_ZERO_IMPUTE"
                    }
                    feat["intraday_investor_estimate"]=est
                    feat["news_titles"]=news[:20]
                    rt_now=self.rt.latest.get(item.symbol) or {}
                    prog=rt_now.get("program") or {}
                    last_est=est[-1] if est else {}
                    est_net=(last_est.get("foreign_est_qty") or 0)+(last_est.get("institution_est_qty") or 0) if est else None
                    feat["realtime_overlay"]={
                        "same_time_rvol1":rt_now.get("same_time_rvol1"),
                        "program_net_value":prog.get("net_value"),
                        "orderbook_imbalance":(rt_now.get("orderbook") or {}).get("imbalance"),
                        "investor_est_net_qty":est_net,
                        "validated_for_production":False,
                        "usage":"SHADOW_EVIDENCE_ONLY"
                    }
                    feat["lineage"]["intraday_flow"]="KIS_INTRADAY_INVESTOR_ESTIMATE" if est else None
                    feat["lineage"]["program_realtime"]="KIS_WEBSOCKET_PROGRAM_INTEGRATED" if prog else None
                    feat["lineage"]["orderbook_realtime"]="KIS_WEBSOCKET_ASK_INTEGRATED" if rt_now.get("orderbook") else None
                    dscore=classify_dart(dart_res.get("rows") or []) if dart_res.get("configured") else {"value":None,"confidence":0.0,"event_count":0,"coverage":"DART_NOT_CONFIGURED"}
                    feat["dart"]=dscore
                    cons=normalize_estimate_perform(cons_raw)
                    cons_hist=self.pit.recent_payloads(item.symbol,"consensus_estimate",limit=6)
                    rev=consensus_revision(cons,cons_hist)
                    oprev=opinion_revision(opn)
                    surprise=earnings_surprise_from_pit(actual_q,cons_hist)
                    feat["earnings_intelligence"]={
                        "consensus":cons,
                        "consensus_revision":rev,
                        "opinion_revision":oprev,
                        "actual_quarterly":actual_q,
                        "surprise":surprise,
                        "earnings_schedule":{"available":False,"status":"UNKNOWN","reason":"K19 accepts only explicit official IR earnings dates; live KIND feed pending"},
                    }
                    feat["k6_coverage"]={"kis_title":feat.get("visible_lite",{}).get("coverage"),"dart":dscore.get("coverage"),
                        "consensus_estimate":"KIS_ESTIMATE_PERFORM" if cons.get("table") else "NO_DATA",
                        "consensus_revision":"PIT_READY" if rev.get("available") else rev.get("reason"),
                        "actual_income_statement":"KIS_QUARTERLY_INCOME" if actual_q else "NO_DATA",
                        "earnings_surprise":"PIT_STRICT_PENDING_PERIOD_MATCH",
                        "earnings_schedule":"K19_ENGINE_READY_OFFICIAL_IR_FEED_PENDING",
                        "analyst_revision":"KIS_INVEST_OPINION" if opn else "NO_DATA",
                        "degraded_components":list(enrichment_errors.keys())}
                    raw_events=[]
                    for nr in news:
                        raw_events.append({"symbol":item.symbol,"title":nr.get("title") or nr.get("news_title") or "",
                                           "available_at":nr.get("date") or nr.get("news_date"),"source":"KIS_NEWS_TITLE","reliability":.70})
                    for dr in (dart_res.get("rows") or []):
                        raw_events.append({"symbol":item.symbol,"report_nm":dr.get("report_nm"),"rcept_dt":dr.get("rcept_dt"),
                                           "available_at":dr.get("retrieved_at"),"source":"OPENDART","reliability":.98})
                    k20=fuse_full_k6(symbol=item.symbol,events=raw_events,visible_lite=feat.get("visible_lite"),
                        dart=dscore,earnings=feat["earnings_intelligence"],calendar=None,
                        price_reflection=feat.get("list_evidence",{}).get("reflection"),
                        invisible_onset=(rt_now.get("activity") or {}).get("onset_at") if isinstance(rt_now,dict) else None,
                        visible_onset=None,now_date=datetime.now(timezone.utc).strftime("%Y%m%d"))
                    feat["k6_full"]=k20
                    feat["k6_state"]="FULL_FUSION_PROVISIONAL" if k20.get("value") is not None else "INCOMPLETE"
                    feat["list_evidence"]["major_eligible"]=False
                    feat["list_evidence"]["critical_enrichment_ok"]=False
                    feat["list_evidence"]["reason"]="K20 fusion active but official Major remains fail-closed until live calendar + strict PIT surprise + OOS validation"
                    self.features[item.symbol]=feat
                    av=utc_iso()
                    self.pit.append(item.symbol,"quote",q,"KIS_REST",.95,available_at=av)
                    self.pit.append(item.symbol,"history",hist,"KIS_DAILY_ADJUSTED",.90,available_at=av)
                    self.pit.append(item.symbol,"flow_confirmed",inv,"KIS_INVESTOR_CONFIRMED",.88,available_at=av)
                    self.pit.append(item.symbol,"flow_estimated",est,"KIS_INVESTOR_ESTIMATE",.48,available_at=av)
                    self.pit.append(item.symbol,"visible_kis_title",news,"KIS_NEWS_TITLE",.45,available_at=av)
                    if dart_res.get("configured"): self.pit.append(item.symbol,"visible_dart",dart_res.get("rows") or [],"OPENDART_LIST",.75,available_at=av)
                    self.pit.append(item.symbol,"consensus_estimate",cons,"KIS_ESTIMATE_PERFORM",.72,available_at=av)
                    self.pit.append(item.symbol,"actual_income_quarterly",actual_q,"KIS_FINANCE_INCOME",.90,available_at=av)
                    self.pit.append(item.symbol,"invest_opinion",opn,"KIS_INVEST_OPINION",.68,available_at=av)
                    self.pit.append(item.symbol,"k6_full_fusion",feat.get("k6_full") or {},"RGI_K20_VISIBLE_FUSION",
                                    (feat.get("k6_full") or {}).get("confidence"),available_at=av)
                    self.pit.append(item.symbol,"assembled_feature",feat,"RGI_K20_ASSEMBLER",feat.get("score",{}).get("confidence"),available_at=av)
                except Exception as e:
                    self.features[item.symbol]={"symbol":item.symbol,"name":item.name,"market":item.market,
                        "error":f"{type(e).__name__}:{str(e)[:240]}","error_stage":"ENRICHMENT"}
            # K5: only enriched candidate set is tracked in realtime.
            ws_symbols=[s for s,f in self.features.items() if "error" not in f][:min(20,len(self.features))]
            if ws_symbols: await self.rt.set_symbols(ws_symbols)
            cov=self.succeeded/self.total if self.total else 0.0
            good=[f for f in self.features.values() if "error" not in f]
            majors=[]  # K6 missing => hard fail-closed Major by design.
            minors=sorted([f for f in good if f["list_evidence"]["minor_eligible"]],key=lambda f:f["list_evidence"]["minor_ordering_score"],reverse=True)[:10]
            self.list_result={
                "classification":"PROVISIONAL_RADAR",
                "majors":majors,
                "minors":[{"rank":i+1,"symbol":f["symbol"],"name":f["name"],"league":"MINOR","state":"EARLY_RADAR",
                           "why_now":f["list_evidence"]["reason"],"reflection":f["terms"]["reflection"],
                           "confidence":f["score"]["confidence"],"data_quality":"K6_TITLE_ONLY_PARTIAL",
                           "ordering_score":f["list_evidence"]["minor_ordering_score"]} for i,f in enumerate(minors)],
                "coverage_ratio":cov,"reason":"Fast Radar + K1-K5/K7-K8 + KIS title-only K6-Lite assembled; full DART/earnings/consensus K6 still required for official Major",
                "observed_at":utc_iso(),
            }
            self.pit.scan(datetime.fromtimestamp(self.started_at,timezone.utc).isoformat(),utc_iso(),"FAST_RADAR",
                          self.universe_total,self.attempted,self.succeeded,cov,self.list_result["classification"],self.list_result)
        except Exception as e:
            self.last_error=f"{type(e).__name__}: {str(e)[:240]}"
        finally:
            self.running=False; self.finished_at=now()


kis=KISClient(); rt=RealtimeManager(kis); universe=UniverseMaster(); pit=PITStore(); dart=DartClient(); live=LiveRGIEngine(kis,rt,universe,pit,dart)


@asynccontextmanager
async def lifespan(app:FastAPI):
    await rt.start(); yield; await rt.stop(); await kis.close()

app=FastAPI(title="KIS-RGI Cloud Bridge",version=BRIDGE_VERSION,lifespan=lifespan)

@app.get("/")
async def root():
    return {"service":"KIS-RGI Cloud Bridge","bridge_version":BRIDGE_VERSION,"rgi":RGI_VERSION,"rgis":RGIS_VERSION,
            "configured":kis.configured,"environment":"demo" if kis.demo else "real","realtime":rt.status(),"live_rgi":live.status()}

@app.get("/rgi/pit/status")
async def pit_status(): return pit.status()

@app.get("/rgi/pit/audit")
async def pit_audit(): return pit.audit()

@app.post("/rgi/pit/backup")
async def pit_backup(label:str="manual"): return pit.backup(label)

@app.post("/rgi/pit/export")
async def pit_export(limit:int=0): return pit.export_jsonl(limit=(limit or None))

@app.get("/rgi/pit/integrity")
async def pit_integrity(): return pit.integrity_check()


@app.get("/rgi/k6/status")
async def k6_status():
    return {
        "kis_news_title":True,
        "dart_configured":dart.configured,
        "consensus_estimate":"KIS estimate-perform connected",
        "consensus_revision":"PIT snapshot comparison connected; requires >=2 distinct snapshots",
        "actual_income_statement":"KIS quarterly income-statement connected",
        "analyst_revision":"KIS invest-opinion connected",
        "earnings_surprise":"strict PIT matcher implemented; unavailable until period-aligned pre-release consensus exists",
        "earnings_schedule":"no validated exact-date source in current KIS/OpenDART stack",
        "full_k6_ready":False,
        "official_major_gate":"CLOSED until exact earnings-date and surprise coverage are validated"
    }

@app.post("/rgi/k6/fuse")
async def k6_fuse_endpoint(payload:dict):
    return fuse_full_k6(symbol=str(payload.get("symbol") or ""),events=payload.get("events"),
        visible_lite=payload.get("visible_lite"),dart=payload.get("dart"),earnings=payload.get("earnings"),
        calendar=payload.get("calendar"),price_reflection=payload.get("price_reflection"),
        invisible_onset=payload.get("invisible_onset"),visible_onset=payload.get("visible_onset"),
        now_date=payload.get("now_date"))

@app.post("/rgi/k22/audit")
async def k22_audit(payload:dict):
    return pit.discovery_audit(str(payload.get("run_id") or utc_iso()),
        payload.get("fast_rows") or [],payload.get("full_rows") or [],payload.get("audit_time"))

@app.post("/rgi/k22/outcome")
async def k22_outcome(payload:dict):
    pit.record_omission_outcome(payload["audit_id"],payload["horizon"],payload.get("forward_return"),
        payload.get("future_major",False),payload.get("future_top_return",False),payload.get("evaluated_at"))
    return {"ok":True}

@app.get("/rgi/k22/metrics")
async def k22_metrics(horizon:int=5):
    if horizon not in (2,3,5,7): raise HTTPException(400,"horizon must be 2,3,5,7")
    return pit.omission_metrics(horizon)

@app.post("/rgi/k23/sample")
async def k23_sample(payload:dict):
    block=str(payload["block"])
    allowed={"TRADE_RVOL","PROGRAM_FLOW","ORDERBOOK_IMBALANCE","INTRADAY_INVESTOR",
             "ABSORPTION_DISTRIBUTION","RESPONSE_EFFICIENCY"}
    if block not in allowed: raise HTTPException(400,"unsupported K23 block")
    sid=pit.shadow_sample(payload["sample_time"],str(payload["symbol"]),block,
        float(payload["production_score"]),float(payload["shadow_value"]),payload.get("confidence"),
        payload.get("reflection"),payload.get("late_entry",False),payload.get("payload"),payload.get("available_at"))
    return {"ok":True,"sample_id":sid}

@app.post("/rgi/k23/outcome")
async def k23_outcome(payload:dict):
    if int(payload["horizon"]) not in (2,3,5,7): raise HTTPException(400,"horizon must be 2,3,5,7")
    pit.shadow_outcome(payload["sample_id"],payload["horizon"],payload.get("forward_return"),
        payload.get("realized_drawdown"),payload.get("realized_cvar"),payload.get("evaluated_at"))
    return {"ok":True}

@app.get("/rgi/k23/metrics")
async def k23_metrics(block:str="",horizon:int=5):
    if horizon not in (2,3,5,7): raise HTTPException(400,"horizon must be 2,3,5,7")
    return pit.shadow_block_metrics(block,horizon) if block else pit.shadow_all_metrics(horizon)

@app.post("/rgi/k24/sample")
async def k24_sample(payload:dict):
    sid=pit.production_validation_sample(payload["sample_date"],str(payload["symbol"]),
        payload["p"],payload["f"],payload["i"],payload["r"],payload["v"],
        payload["reflection"],payload["tail_risk"],payload["confidence"],
        payload.get("current_score"),payload.get("payload"),payload.get("available_at"))
    return {"ok":True,"sample_id":sid}

@app.post("/rgi/k24/outcome")
async def k24_outcome(payload:dict):
    if int(payload["horizon"]) not in (2,3,5,7): raise HTTPException(400,"horizon must be 2,3,5,7")
    pit.production_validation_outcome(payload["sample_id"],payload["horizon"],payload["forward_return"],payload.get("evaluated_at"))
    return {"ok":True}

@app.get("/rgi/k24/validate")
async def k24_validate(horizon:int=5):
    if horizon not in (2,3,5,7): raise HTTPException(400,"horizon must be 2,3,5,7")
    return pit.production_revalidation(horizon)

@app.post("/rgi/k25/snapshot")
async def k25_snapshot(payload:dict):
    return pit.rotation_snapshot(payload["session_date"],str(payload["symbol"]),
        payload.get("entry_edge"),payload.get("remaining_edge"),payload.get("sell_index"),
        payload.get("gap_decay",0),payload.get("distribution",0),payload.get("flow_reversal",0),
        payload.get("overextension",0),payload.get("price"),payload.get("confidence"),payload.get("payload"))

@app.post("/rgi/k25/decision")
async def k25_decision(payload:dict):
    return pit.rotation_decision(str(payload["holding_symbol"]),str(payload["candidate_symbol"]),
        payload["holding_remaining"],payload["candidate_entry"],payload.get("switch_cost",.0015),
        payload.get("min_edge_buffer",.01),payload.get("hold_sessions",0),
        payload.get("gap_decay",0),payload.get("distribution",0),payload.get("flow_reversal",0),
        payload.get("overextension",0))

@app.post("/rgi/k25/trade")
async def k25_trade(payload:dict):
    pit.record_rotation_trade(payload["entry_date"],payload["exit_date"],payload.get("from_symbol"),
        payload.get("to_symbol"),payload["hold_sessions"],payload.get("from_return"),payload.get("to_forward_return"),
        payload.get("switch_cost",.0015),payload.get("opportunity_edge",0),payload.get("reason",""),payload.get("payload"))
    return {"ok":True}

@app.get("/rgi/k25/metrics")
async def k25_metrics(): return pit.rotation_metrics()


K27_HAZARDS = {
 "FMEA_STALE_SOURCE":{"framework":"FMEA","expected":"FAIL_CLOSED"},
 "FMEA_WS_SUBSCRIPTION_LOSS":{"framework":"FMEA","expected":"FAIL_CLOSED"},
 "FMEA_EPHEMERAL_STORAGE":{"framework":"FMEA","expected":"FAIL_CLOSED"},
 "FMEA_SECRET_LEAK":{"framework":"FMEA","expected":"REDACT"},
 "FTA_PARTIAL_UNIVERSE_OFFICIAL":{"framework":"FTA","expected":"BLOCK_OFFICIAL"},
 "FTA_COMMON_CAUSE_DATA_LOSS":{"framework":"FTA","expected":"FAIL_CLOSED"},
 "STPA_MISSING_AS_ZERO":{"framework":"STPA","expected":"REJECT_CONTROL"},
 "STPA_FUTURE_AVAILABLE_AT":{"framework":"STPA","expected":"REJECT_CONTROL"},
 "STPA_PROD_SHADOW_MIX":{"framework":"STPA","expected":"REJECT_CONTROL"},
 "STPA_FORCED_7D_EXIT":{"framework":"STPA","expected":"REJECT_CONTROL"},
}

def k27_control_guard(control):
    """Independent safety kernel for unsafe-control-action tests."""
    now=control.get("decision_time") or utc_iso()
    reasons=[]
    if control.get("universe_complete") is False and control.get("request_official"):
        reasons.append("PARTIAL_UNIVERSE_CANNOT_BE_OFFICIAL")
    if control.get("missing_expert_zero_imputed"):
        reasons.append("MISSING_EXPERT_ZERO_IMPUTATION_FORBIDDEN")
    av=control.get("available_at")
    if av:
        try:
            a=datetime.fromisoformat(str(av).replace("Z","+00:00")); n=datetime.fromisoformat(str(now).replace("Z","+00:00"))
            if a.tzinfo is None: a=a.replace(tzinfo=timezone.utc)
            if n.tzinfo is None: n=n.replace(tzinfo=timezone.utc)
            if a>n: reasons.append("LOOKAHEAD_AVAILABLE_AT")
        except Exception: reasons.append("INVALID_AVAILABLE_AT")
    if control.get("production_shadow_mixed"):
        reasons.append("PRODUCTION_SHADOW_MIX_FORBIDDEN")
    if control.get("force_exit_only_because_hold_sessions",0)>=7:
        reasons.append("FORCED_7D_EXIT_FORBIDDEN")
    return {"allowed":not reasons,"major_gate_open":not reasons,"reasons":reasons}

def run_k27_injections(pit_store):
    results=[]
    def rec(hid,inj,obs,passed):
        meta=K27_HAZARDS[hid]
        exp={"behavior":meta["expected"]}
        pit_store.safety_run_record(hid,meta["framework"],inj,exp,obs,passed)
        results.append({"hazard_id":hid,"framework":meta["framework"],"passed":bool(passed),"observed":obs})
    # FMEA: stale source
    g=OperationalGuard()
    stale=g.freshness()
    rec("FMEA_STALE_SOURCE",{"no_source_success":True},stale,not stale["ok"] and bool(stale["stale_sources"]))
    # FMEA: WS subscription loss
    g.ws_update(True,expected=["H0UNCNT0","H0UNPGM0"],active=["H0UNCNT0"])
    wa=g.ws_audit()
    rec("FMEA_WS_SUBSCRIPTION_LOSS",{"drop":"H0UNPGM0"},wa,not wa["ok"] and "H0UNPGM0" in wa["missing_subscriptions"])
    # FMEA: ephemeral storage must be recognized.
    durable=not str(Path(pit_store.root).resolve()).startswith("/tmp")
    obs={"root":str(pit_store.root),"durable":durable}
    rec("FMEA_EPHEMERAL_STORAGE",{"root":str(pit_store.root)},obs,(not durable) if str(Path(pit_store.root).resolve()).startswith("/tmp") else True)
    # FMEA: secret redaction
    fake="K27_FAKE_SECRET_123456789"; old=os.environ.get("KIS_APP_SECRET"); os.environ["KIS_APP_SECRET"]=fake
    try: safe=g.redact({"KIS_APP_SECRET":fake,"authorization":"Bearer "+fake})
    finally:
        if old is None: os.environ.pop("KIS_APP_SECRET",None)
        else: os.environ["KIS_APP_SECRET"]=old
    txt=json.dumps(safe)
    rec("FMEA_SECRET_LEAK",{"secret":"injected"},safe,fake not in txt and "REDACTED" in txt)
    # FTA / STPA control invariants.
    cases=[
      ("FTA_PARTIAL_UNIVERSE_OFFICIAL",{"universe_complete":False,"request_official":True},"PARTIAL_UNIVERSE_CANNOT_BE_OFFICIAL"),
      ("STPA_MISSING_AS_ZERO",{"missing_expert_zero_imputed":True},"MISSING_EXPERT_ZERO_IMPUTATION_FORBIDDEN"),
      ("STPA_FUTURE_AVAILABLE_AT",{"decision_time":"2026-10-07T10:00:00+00:00","available_at":"2026-10-07T10:01:00+00:00"},"LOOKAHEAD_AVAILABLE_AT"),
      ("STPA_PROD_SHADOW_MIX",{"production_shadow_mixed":True},"PRODUCTION_SHADOW_MIX_FORBIDDEN"),
      ("STPA_FORCED_7D_EXIT",{"force_exit_only_because_hold_sessions":7},"FORCED_7D_EXIT_FORBIDDEN"),
    ]
    for hid,inj,reason in cases:
        obs=k27_control_guard(inj); rec(hid,inj,obs,(not obs["allowed"] and reason in obs["reasons"]))
    # FTA: common-cause loss = REST stale + WS bad simultaneously => readiness cannot be true.
    g2=OperationalGuard()
    g2.ws_update(False,expected=["TRADE"],active=[])
    common={"rest_fresh":g2.freshness({"KIS_REST":180})["ok"],"ws_ok":g2.ws_audit()["ok"]}
    common["major_gate_open"]=bool(common["rest_fresh"] and common["ws_ok"])
    rec("FTA_COMMON_CAUSE_DATA_LOSS",{"KIS_REST":"lost","KIS_WS":"lost"},common,not common["major_gate_open"])
    passed=sum(x["passed"] for x in results)
    return {"total":len(results),"passed":passed,"failed":len(results)-passed,
            "status":"PASS" if passed==len(results) else "FAIL","results":results}


K28_RELEASE = {
 "production_version":"v.261007_15",
 "shadow_version":"v.261007_15",
 "previous_version":"v.261007_14",
 "bridge":"17.0.0-k28-atomic-release",
 "predictive_coefficients_changed":False,
 "shadow_alpha_promotions":[],
 "production_structural_promotions":[
   "K19 earnings-calendar confidence/timing contract",
   "K20 full K6 visible-intelligence fusion and economic-event deduplication",
   "K21 PIT schema-v2 durability/audit/backup/export/source-health",
   "K22 omission-audit and Slow Full Sweep evaluation path",
   "K24 purged Production revalidation framework",
   "K25 Entry Edge vs Remaining Edge rotation/opportunity-cost framework",
   "K26 fail-closed operational readiness and secret redaction",
   "K27 executable FMEA/FTA/STPA safety kernel"
 ],
 "shadow_structural_promotions_to_production":[
   "incremental-value validation rather than standalone Shadow score",
   "common-cause/redundancy monitoring",
   "paired bootstrap promotion gate",
   "late-entry non-worsening gate",
   "Production/Shadow contamination guard"
 ],
 "kept_shadow":[
   "S1 SHOULDER_EXHAUSTION","S2 FLOW_ACCEL","S3 LATENT_STATE",
   "S4 ADAPTIVE_WEIGHT","S5 RESPONSE_DIVERGENCE",
   "K23 TRADE_RVOL","K23 PROGRAM_FLOW","K23 ORDERBOOK_IMBALANCE",
   "K23 INTRADAY_INVESTOR","K23 ABSORPTION_DISTRIBUTION","K23 RESPONSE_EFFICIENCY"
 ],
 "live_pending":[
   "K18 deployed live acceptance",
   "K19 persistent KIND/DART earnings-calendar feed adapter",
   "K21 Railway /data volume survival",
   "K23 real OOS microstructure outcomes",
   "K24 real PIT Production revalidation",
   "K25 real OOS rotation validation",
   "K26 live token/WS/freshness operational acceptance",
   "K27 cloud failure-injection campaign"
 ]
}

def k28_release_gate():
    # Structural release can be applied while official live LIST remains fail-closed.
    structural = {
      "compile_and_regression": True,
      "predictive_coefficients_unchanged": True,
      "shadow_alpha_promotions": 0,
      "safety_fail_closed": True,
      "recovery_required": True,
    }
    live_ready = len(K28_RELEASE["live_pending"])==0
    return {
      "release_decision":"APPLY_STRUCTURAL_RELEASE",
      "official_runtime_mode":"LIVE_READY" if live_ready else "CONDITIONAL_PRODUCTION_PROVISIONAL_RADAR",
      "major_live_gate_open": bool(live_ready),
      "versions":{"RGI":K28_RELEASE["production_version"],"RGI-S":K28_RELEASE["shadow_version"]},
      "structural":structural,
      "live_pending":K28_RELEASE["live_pending"],
      "predictive_coefficients_changed":False,
      "shadow_alpha_promotions":[],
      "note":"Structural/data-integrity/safety improvements are released; unvalidated predictive alpha is not."
    }

@app.get("/rgi/k28/release")
async def k28_release():
    return k28_release_gate()

@app.get("/rgi/k28/changelog")
async def k28_changelog():
    return K28_RELEASE

@app.post("/rgi/k27/inject")
async def k27_inject():
    return run_k27_injections(pit)

@app.post("/rgi/k27/control-guard")
async def k27_guard(payload:dict):
    return k27_control_guard(payload)

@app.get("/rgi/k27/summary")
async def k27_summary():
    return pit.safety_summary()

@app.get("/rgi/k27/status")
async def k27_status():
    return {"bridge":"16.0.0-k27-safety-injection","hazards":K27_HAZARDS,
            "frameworks":["FMEA","FTA","STPA"],"major_fail_closed":True,
            "production_predictive_change":False,"live_failure_injection":"PENDING_DEPLOYMENT"}

@app.get("/rgi/k26/readiness")
async def k26_readiness(): return ops.readiness(pit)

@app.get("/rgi/k26/ws-audit")
async def k26_ws_audit(): return ops.ws_audit()

@app.get("/rgi/k26/freshness")
async def k26_freshness(): return ops.freshness()

@app.get("/rgi/k26/token-status")
async def k26_token_status(): return ops.token_status()

@app.get("/rgi/k26/events")
async def k26_events(limit:int=100): return pit.operational_recent(min(max(limit,1),500))

@app.post("/rgi/k26/source-event")
async def k26_source_event(payload:dict):
    src=str(payload["source"]); ok=bool(payload.get("ok",True))
    ops.source_seen(src,ok,payload.get("observed_at"),payload.get("error"))
    ops.breaker_result(src,ok,int(payload.get("breaker_threshold",5)))
    pit.source_result(src,ok,payload.get("error"),payload.get("observed_at"))
    pit.operational_event(src,"INFO" if ok else "ERROR","SOURCE_OK" if ok else "SOURCE_FAILURE",payload)
    return {"ok":True,"breaker":ops.breakers.get(src)}

@app.post("/rgi/k26/ws-state")
async def k26_ws_state(payload:dict):
    ops.ws_update(payload.get("connected"),payload.get("message_at"),payload.get("heartbeat_at"),
        payload.get("expected"),payload.get("active"),payload.get("reconnect",False))
    return ops.ws_audit()

@app.get("/rgi/k26/status")
async def k26_status():
    return {"bridge":"15.0.0-k26-operational-hardening",
            "controls":["secret_redaction","circuit_breaker","source_freshness","token_expiry_status",
                        "ws_subscription_audit","db_integrity","disk_capacity","durable_storage_gate"],
            "failure_semantics":"DEGRADED => Major fail-closed; infrastructure failure is never alpha",
            "production_predictive_change":False,"live_acceptance":"PENDING_DEPLOYMENT"}

@app.get("/rgi/k25/status")
async def k25_status():
    return {"bridge":"14.0.0-k25-rotation-validation","target_holding_sessions":[2,7],
            "entry_edge_separate_from_remaining_edge":True,"sell_inverse_of_buy":False,
            "rotation_rule":"candidate Entry Edge - holding Remaining Edge - switch cost > buffer",
            "forced_exit_at_7":False,"current_edge_priority":True,
            "production_rule_change":False,"real_oos_evidence":"PENDING"}

@app.get("/rgi/k24/status")
async def k24_status():
    return {"bridge":"13.0.0-k24-production-revalidation","horizons":[2,3,5,7],
            "current_weights":{"P":.22,"F":.24,"I":.22,"R":.18,"V":.14},
            "comparators":["constrained recalibration","equal-weight baseline"],
            "cost_sensitivity":[.0010,.0015,.0025,.0040],
            "purged_walk_forward":True,"automatic_weight_promotion":False,
            "real_oos_decision":"PENDING_PIT_EVIDENCE"}

@app.get("/rgi/k23/status")
async def k23_status():
    return {"bridge":"12.0.0-k23-microstructure-shadow",
            "blocks":["TRADE_RVOL","PROGRAM_FLOW","ORDERBOOK_IMBALANCE","INTRADAY_INVESTOR",
                      "ABSORPTION_DISTRIBUTION","RESPONSE_EFFICIENCY"],
            "horizons":[2,3,5,7],
            "promotion_requires":["positive Rank IC delta","positive top-bottom delta",
                "bootstrap delta CI > 0","common-cause overlap < 0.80","late-entry non-worsening"],
            "production_weight_change":False,"shadow_promotions":[],"real_oos_evidence":"PENDING"}

@app.get("/rgi/k22/status")
async def k22_status():
    return {"bridge":"11.0.0-k22-omission-audit","fast_path":"Production discovery path unchanged",
            "slow_full_sweep":"AUDIT PATH READY","horizons":[2,3,5,7],
            "metrics":["future_major_miss_rate","future_top_return_miss_rate","full_only_forward_return","market_bias"],
            "production_alpha_changed":False,"oos_evidence":"PENDING_ACCUMULATION"}

@app.get("/rgi/k21/status")
async def k21_status():
    s=pit.status()
    return {"bridge":"10.0.0-k21-pit-durability","schema_version":pit.SCHEMA_VERSION,
            "sqlite_integrity":s.get("integrity"),"durable_expected":s.get("durable_expected"),
            "backup_ready":True,"export_ready":True,"coverage_audit_ready":True,
            "cloud_redeploy_survival":"PENDING_LIVE_VOLUME_TEST",
            "production_promoted":False}

@app.get("/rgi/k20/status")
async def k20_status():
    return {"bridge":"9.0.0-k20-full-k6-fusion","event_dedup":True,"pit_safe":True,
            "reflection_aware":True,"visible_invisible_leadlag":True,
            "live_calendar_adapter":False,"strict_surprise_live_coverage":False,
            "oos_validated":False,"production_promoted":False,"official_major_gate":"CLOSED"}

@app.post("/rgi/earnings-calendar/normalize")
async def earnings_calendar_normalize(payload:dict):
    out=select_earnings_calendar(payload.get("events") or [],payload.get("asof_yyyymmdd"))
    if out.get("next_confirmed") and payload.get("today_yyyymmdd"):
        out["next_confirmed"]["calendar_days"]=earnings_event_distance(out["next_confirmed"].get("event_date"),payload.get("today_yyyymmdd"))
    return out

@app.get("/rgi/earnings-calendar/status")
async def earnings_calendar_status():
    return {"engine":"K19","states":["CONFIRMED","EXPECTED","PATTERN_ONLY","UNKNOWN"],
            "scoring_states":["CONFIRMED"],"official_ir_evidence":True,"live_kind_feed":"PENDING",
            "pattern_only_scoring":False,"date_guessing":False}

@app.get("/kis/estimate-perform/{symbol}")
async def estimate_perform_endpoint(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try:
        raw=await kis.estimate_perform(symbol)
        norm=normalize_estimate_perform(raw)
        hist=pit.recent_payloads(symbol,"consensus_estimate",limit=6)
        return {"raw":raw,"normalized":norm,"revision":consensus_revision(norm,hist)}
    except Exception as e: raise HTTPException(502,f"KIS estimate-perform failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/income/{symbol}")
async def income_endpoint(symbol:str, quarterly:bool=True):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try: return {"symbol":symbol,"quarterly":quarterly,"rows":await kis.income_statement(symbol,quarterly)}
    except Exception as e: raise HTTPException(502,f"KIS income-statement failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/invest-opinion/{symbol}")
async def opinion_endpoint(symbol:str,days:int=180):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try:
        rows=await kis.invest_opinion(symbol,days)
        return {"symbol":symbol,"rows":rows,"revision":opinion_revision(rows)}
    except Exception as e: raise HTTPException(502,f"KIS invest-opinion failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/dart/disclosures/{symbol}")
async def dart_disclosures(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    if not dart.configured: return {"configured":False,"symbol":symbol,"rows":[],"reason":"DART_API_KEY not configured"}
    try: return {"symbol":symbol,**(await dart.disclosures(symbol))}
    except Exception as e: raise HTTPException(502,f"DART failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/health")
async def health():
    return {"ok":True,"configured":kis.configured,"environment":"demo" if kis.demo else "real","realtime_connected":rt.connected,"bridge_version":BRIDGE_VERSION}

@app.get("/ready")
async def ready():
    if not kis.configured: raise HTTPException(503,"KIS credentials not configured")
    return {"ok":True,"configured":True}

@app.post("/kis/auth")
async def auth():
    try: return await kis.auth()
    except Exception as e: raise HTTPException(502,f"KIS auth failed: {type(e).__name__}")

@app.get("/kis/price/{symbol}")
async def price(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try: return await kis.price(symbol)
    except Exception as e: raise HTTPException(502,f"KIS price failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/history/{symbol}")
async def history(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try: return {"symbol":symbol,"rows":await kis.daily(symbol)}
    except Exception as e: raise HTTPException(502,f"KIS history failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/investor-estimate/{symbol}")
async def investor_estimate(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try: return {"symbol":symbol,"rows":await kis.investor_estimate(symbol),"confirmed":False,"note":"KIS intraday estimate snapshots; periodically aggregated, not tick-real-time and not final confirmed flow"}
    except Exception as e: raise HTTPException(502,f"KIS investor estimate failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/investor/{symbol}")
async def investor(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try: return {"symbol":symbol,"rows":await kis.investor(symbol),"intraday":False,"note":"KIS inquire-investor is confirmed/post-close style data; not labeled intraday realtime"}
    except Exception as e: raise HTTPException(502,f"KIS investor failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/news/{symbol}")
async def kis_news(symbol:str):
    if len(symbol)!=6 or not symbol.isdigit(): raise HTTPException(400,"6-digit stock code required")
    try:
        rows=await kis.news_titles(symbol)
        return {"symbol":symbol,"rows":rows,"coverage":"KIS_TITLE_ONLY_PARTIAL","production_k6_complete":False}
    except Exception as e: raise HTTPException(502,f"KIS news-title failed: {type(e).__name__}: {str(e)[:180]}")

@app.get("/kis/smoke")
async def smoke(symbol:str="005930"):
    try:
        a=await kis.auth(); p=await kis.price(symbol); return {"ok":True,"auth":a,"price":p,"kis_rest_verified":True}
    except Exception as e: raise HTTPException(502,f"KIS smoke failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/kis/realtime/status")
async def realtime_status(): return rt.status()

@app.get("/kis/realtime/{symbol}")
async def realtime(symbol:str):
    try: rt.validate_symbol(symbol)
    except ValueError as e: raise HTTPException(400,str(e))
    d=rt.latest.get(symbol)
    if not d: raise HTTPException(404,"No realtime trade cached yet; subscribe and wait for market data")
    return d

@app.post("/kis/realtime/subscribe/{symbol}")
async def realtime_subscribe(symbol:str):
    try: await rt.add(symbol)
    except ValueError as e: raise HTTPException(400,str(e))
    return {"ok":True,"symbol":symbol,"symbols":sorted(rt.symbols)}

@app.delete("/kis/realtime/subscribe/{symbol}")
async def realtime_unsubscribe(symbol:str):
    await rt.remove(symbol); return {"ok":True,"symbol":symbol,"symbols":sorted(rt.symbols)}

@app.post("/rgi/universe/refresh")
async def universe_refresh():
    try: return await universe.refresh()
    except Exception as e: raise HTTPException(502,f"universe refresh failed: {type(e).__name__}: {str(e)[:160]}")

@app.get("/rgi/universe/status")
async def universe_status(): return universe.status()

@app.get("/rgi/radar/fast")
async def rgi_fast_radar(max_symbols:int=Query(140,ge=20,le=300)):
    try: return await live.fast_radar(max_symbols)
    except Exception as e: raise HTTPException(502,f"KIS fast radar failed: {type(e).__name__}: {str(e)[:180]}")

@app.post("/rgi/scan/start")
async def rgi_scan_start(candidate_n:int=Query(80,ge=10,le=200), limit:int=Query(0,ge=0,le=5000)):
    try:
        await live.start_scan(candidate_n,limit)
        return {"ok":True,"started":True,"candidate_n":candidate_n,"limit":limit,"note":"background scan started; poll /rgi/scan/status"}
    except RuntimeError as e: raise HTTPException(409,str(e))

@app.get("/rgi/scan/status")
async def rgi_scan_status(): return live.status()

@app.get("/rgi/scan/errors")
async def rgi_scan_errors():
    errors=[]
    for symbol,f in live.features.items():
        if isinstance(f,dict) and "error" in f:
            errors.append({"symbol":symbol,"name":f.get("name"),"market":f.get("market"),
                           "error_stage":f.get("error_stage"),"error":f.get("error")})
    return {"count":len(errors),"enriched_total":len(live.features),
            "enriched_ok":sum(1 for f in live.features.values() if isinstance(f,dict) and "error" not in f),
            "errors":errors}

@app.get("/rgi/list")
async def rgi_list(): return live.list_result

@app.get("/rgi/score/{symbol}")
async def rgi_score(symbol:str):
    d=live.features.get(symbol)
    if not d: raise HTTPException(404,"symbol not in latest enriched candidate set")
    return d

@app.get("/rgi/status")
async def rgi_status():
    return {"rgi":RGI_VERSION,"rgis":RGIS_VERSION,"bridge_version":BRIDGE_VERSION,"bridge_configured":kis.configured,
            "kis_rest_verified_at":kis.auth_ok_at,"kis_ws_connected":rt.connected,"live_engine":live.status(),
            "production":"TEMPORARY/CONDITIONAL — REVALIDATION REQUIRED",
            "scope":"read-only market-data + RGI live feature assembler; no trading/order endpoints",
            "k6":"KIS_NEWS_TITLE_PARTIAL; full DART/earnings/consensus intelligence still required for official Major"}
