#!/usr/bin/env python3
"""Scrape gold prices from major HK gold retailers."""

import json
import os
import sys
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

HKT = timezone(timedelta(hours=8))
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "prices.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
}


def parse_number(s):
    """Parse a number string that may contain commas."""
    if not s:
        return 0.0
    return float(str(s).replace(",", ""))


def scrape_chow_tai_fook():
    """周大福 — parse JSON from hidden input in HTML.
    Fields: Gold_Sell/Buy, Gold_Pellet_Sell/Buy (with _g suffix for per-gram).
    """
    url = "https://www.chowtaifook.com/zh-hk/eshop/realtime-gold-price.html"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    inp = soup.find("input", class_="gold-price-data")
    if not inp:
        raise ValueError("Could not find gold-price-data input element")

    d = json.loads(inp["value"])
    return {
        "name": "周大福",
        "url": url,
        "updated_time": d.get("Updated_Time", ""),
        "飾金": {
            "賣出_克": float(d["Gold_Sell_g"]),
            "賣出_兩": float(d["Gold_Sell"]),
            "買入_克": float(d["Gold_Buy_g"]),
            "買入_兩": float(d["Gold_Buy"]),
        },
        "金粒": {
            "賣出_克": float(d["Gold_Pellet_Sell_g"]),
            "賣出_兩": float(d["Gold_Pellet_Sell"]),
            "買入_克": float(d["Gold_Pellet_Buy_g"]),
            "買入_兩": float(d["Gold_Pellet_Buy"]),
        },
    }


def scrape_chow_sang_sang():
    """周生生 — JSON API.
    Response: {"goldRates": [{"type": "G_JW_SELL", "ptRate": ..., "ptRateInGram": ...}, ...]}
    G_JW = jewellery gold (飾金), G_BAR = gold bars (金粒).
    """
    url = "https://www.chowsangsang.com/script/api/crm/getGoldPrices.php?country=hk"
    headers = {**HEADERS, "Referer": "https://www.chowsangsang.com/tc/gold-price"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    rates = {item["type"]: item for item in data["goldRates"]}

    def get(code, field):
        return float(rates.get(code, {}).get(field, 0))

    # Use the latest entryDate among the gold rates
    dates = [rates[c].get("entryDate", "") for c in ["G_JW_SELL", "G_BAR_SELL"] if c in rates]
    latest = max(dates) if dates else ""

    return {
        "name": "周生生",
        "url": "https://www.chowsangsang.com/tc/gold-price",
        "updated_time": latest,
        "飾金": {
            "賣出_克": get("G_JW_SELL", "ptRateInGram"),
            "賣出_兩": get("G_JW_SELL", "ptRate"),
            "買入_克": get("G_JW_BUY", "ptRateInGram"),
            "買入_兩": get("G_JW_BUY", "ptRate"),
        },
        "金粒": {
            "賣出_克": get("G_BAR_SELL", "ptRateInGram"),
            "賣出_兩": get("G_BAR_SELL", "ptRate"),
            "買入_克": get("G_BAR_BUY", "ptRateInGram"),
            "買入_兩": get("G_BAR_BUY", "ptRate"),
        },
    }


def scrape_luk_fook():
    """六福珠寶 — JSON API.
    Response: {"status":1, "data":{"hk":{"data":{
      "9999/999金(両)": {"賣出(HKD)":"52,288","買入(HKD)":"42,332"},
      "9999/999金(克)": {...},
      "9999金粒(両)": {...},
      "9999金粒(克)": {...}, ...
    }}}}
    """
    url = "https://www.lukfook.com/api/goldprice"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    hk = resp.json()["data"]["hk"]["data"]

    return {
        "name": "六福珠寶",
        "url": "https://www.lukfook.com/zh-hk/gold-price",
        "updated_time": "",
        "飾金": {
            "賣出_克": parse_number(hk["9999/999金(克)"]["賣出(HKD)"]),
            "賣出_兩": parse_number(hk["9999/999金(両)"]["賣出(HKD)"]),
            "買入_克": parse_number(hk["9999/999金(克)"]["買入(HKD)"]),
            "買入_兩": parse_number(hk["9999/999金(両)"]["買入(HKD)"]),
        },
        "金粒": {
            "賣出_克": parse_number(hk["9999金粒(克)"]["賣出(HKD)"]),
            "賣出_兩": parse_number(hk["9999金粒(両)"]["賣出(HKD)"]),
            "買入_克": parse_number(hk["9999金粒(克)"]["買入(HKD)"]),
            "買入_兩": parse_number(hk["9999金粒(両)"]["買入(HKD)"]),
        },
    }


TROY_OZ_TO_GRAM = 31.1035


def _gold_usd_from_yahoo():
    """Gold price in USD/oz from Yahoo Finance."""
    resp = requests.get(
        "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1d&range=1d",
        headers=HEADERS, timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]


def _gold_usd_from_kitco():
    """Gold bid price in USD/oz scraped from Kitco."""
    import re
    resp = requests.get("https://www.kitco.com/gold-price-today-usa/", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    matches = re.findall(r"(\d{3,4}\.\d{2})", resp.text)
    candidates = [float(m) for m in matches if 2000 < float(m) < 8000]
    if not candidates:
        raise ValueError("No gold price found on Kitco")
    return min(candidates)  # Bid is the lower price


def fetch_spot_gold_hkd():
    """Fetch spot gold price in HKD per gram.
    Tries Yahoo Finance first, falls back to Kitco.
    Uses Frankfurter for USD/HKD rate.
    """
    # Gold price in USD per troy ounce
    gold_usd = None
    for name, fn in [("Yahoo", _gold_usd_from_yahoo), ("Kitco", _gold_usd_from_kitco)]:
        try:
            gold_usd = fn()
            print(f"    Gold source: {name} — USD {gold_usd}/oz")
            break
        except Exception as e:
            print(f"    {name} failed: {e}")

    if gold_usd is None:
        raise ValueError("All gold price sources failed")

    # USD/HKD exchange rate
    fx_resp = requests.get(
        "https://api.frankfurter.app/latest?from=USD&to=HKD",
        headers=HEADERS, timeout=15,
    )
    fx_resp.raise_for_status()
    usd_hkd = fx_resp.json()["rates"]["HKD"]

    hkd_per_gram = gold_usd * usd_hkd / TROY_OZ_TO_GRAM
    hkd_per_tael = hkd_per_gram * 37.429  # 1 HK tael = 37.429g

    return {
        "usd_per_oz": round(gold_usd, 2),
        "usd_hkd": round(usd_hkd, 4),
        "hkd_per_gram": round(hkd_per_gram, 2),
        "hkd_per_tael": round(hkd_per_tael, 2),
    }


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    existing = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f).get("retailers", {})

    retailers = {}
    scrapers = [
        ("ctf", scrape_chow_tai_fook),
        ("css", scrape_chow_sang_sang),
        ("lf", scrape_luk_fook),
    ]

    for key, scraper_fn in scrapers:
        try:
            print(f"Scraping {key}...")
            result = scraper_fn()
            retailers[key] = result
            result["scraped_at"] = datetime.now(HKT).isoformat()
            print(f"  ✓ {result['name']}: 飾金賣出 {result['飾金']['賣出_克']}/克, 金粒賣出 {result['金粒']['賣出_克']}/克 (報價時間: {result['updated_time'] or 'N/A'})")
        except Exception as e:
            print(f"  ✗ {key} failed: {e}", file=sys.stderr)
            if key in existing:
                retailers[key] = existing[key]
                print(f"  → Using cached data for {key}")

    # Fetch spot gold price
    spot = None
    try:
        print("Fetching spot gold price...")
        spot = fetch_spot_gold_hkd()
        print(f"  ✓ Spot: USD {spot['usd_per_oz']}/oz × {spot['usd_hkd']} = HKD {spot['hkd_per_gram']}/克")
    except Exception as e:
        print(f"  ✗ Spot price failed: {e}", file=sys.stderr)
        prev = existing.get("_spot")
        if prev:
            spot = prev

    output = {
        "updated_at": datetime.now(HKT).isoformat(),
        "spot": spot,
        "retailers": retailers,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {OUTPUT_FILE}")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
