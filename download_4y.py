"""
SETUP INITIAL — Telechargement 4 ans de donnees historiques Binance
6 paires: BTC, ETH, SOL, AVAX, DOGE, BNB (~35000 bougies 1h par paire)

A n'executer qu'une seule fois pour initialiser le dossier data_4y/.
Les mises a jour hebdomadaires incrementales sont gerees automatiquement
par dca_production.py (commande 'recommend' ou 'update').

Usage:
    python download_4y.py
"""

import urllib.request
import json
import csv
import os
import time
from datetime import datetime, timedelta

# ============================================================
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "LINKUSDT"]
INTERVAL = "1h"
DAYS_BACK = 1461  # 4 ans (365.25 * 4)
OUTPUT_DIR = "data_4y"
BATCH_SIZE = 1000
BASE_URL = "https://api.binance.com/api/v3/klines"
# ============================================================


def fetch_klines(symbol, interval, start_ms, end_ms, limit=1000):
    url = (f"{BASE_URL}?symbol={symbol}&interval={interval}"
           f"&startTime={start_ms}&endTime={end_ms}&limit={limit}")
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0')
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def download_symbol(symbol, interval, days):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    all_klines = []
    current_start = start_ms
    batch_num = 0

    print(f"\n{'='*55}")
    print(f"  {symbol} | {interval} | {days} jours")
    print(f"  {start_time.strftime('%Y-%m-%d')} -> {end_time.strftime('%Y-%m-%d')}")
    print(f"{'='*55}")

    while current_start < end_ms:
        batch_num += 1
        retries = 0
        batch = None

        while retries < 3 and batch is None:
            try:
                batch = fetch_klines(symbol, interval, current_start, end_ms, BATCH_SIZE)
            except Exception as e:
                retries += 1
                print(f"   Erreur batch {batch_num} (retry {retries}/3): {e}")
                time.sleep(3 * retries)

        if not batch:
            print(f"   ECHEC apres 3 retries, on continue...")
            # Skip ahead 1000 hours
            current_start += BATCH_SIZE * 3600 * 1000
            continue

        all_klines.extend(batch)
        last_close_time = batch[-1][6]
        current_start = last_close_time + 1

        if batch_num % 5 == 0:
            print(f"   Batch {batch_num}: {len(all_klines)} bougies...", flush=True)

        # Rate limiting (1200 req/min max)
        time.sleep(0.25)

    print(f"   TOTAL: {len(all_klines)} bougies")

    # Save CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"{symbol}_{interval}_{days}d.csv")

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades',
            'taker_buy_base_vol', 'taker_buy_quote_vol'
        ])

        seen = set()
        for k in all_klines:
            ts = k[0]
            if ts in seen:
                continue
            seen.add(ts)
            writer.writerow([
                datetime.utcfromtimestamp(k[0]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                k[1], k[2], k[3], k[4], k[5],
                datetime.utcfromtimestamp(k[6]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                k[7], k[8], k[9], k[10]
            ])

    row_count = len(seen)
    size_kb = os.path.getsize(filename) / 1024
    print(f"   Sauvegarde: {filename} ({row_count} lignes, {size_kb:.0f} KB)")
    return filename


def main():
    print()
    print("=" * 60)
    print("  DOWNLOAD 4 ANS - Binance Public API")
    print("  Pas besoin de cle API")
    print("=" * 60)
    print(f"  Symboles:  {', '.join(SYMBOLS)}")
    print(f"  Interval:  {INTERVAL}")
    print(f"  Periode:   {DAYS_BACK} jours (~4 ans)")
    print(f"  Output:    {OUTPUT_DIR}/")
    print(f"  Estimation: ~{DAYS_BACK*24} bougies/symbole")
    print(f"  Temps estime: ~3-5 minutes")
    print("=" * 60)

    files = []
    for symbol in SYMBOLS:
        filepath = download_symbol(symbol, INTERVAL, DAYS_BACK)
        files.append(filepath)

    print()
    print("=" * 60)
    print("  TERMINE ! Fichiers generes :")
    total_size = 0
    for f in files:
        if os.path.exists(f):
            size_mb = os.path.getsize(f) / (1024*1024)
            total_size += size_mb
            lines = sum(1 for _ in open(f)) - 1
            print(f"    {f} ({lines} lignes, {size_mb:.1f} MB)")
    print(f"\n  Total: {total_size:.1f} MB")
    print("=" * 60)
    print()
    print("Setup initial termine. Lance dca_production.py pour la suite.")
    print()


if __name__ == "__main__":
    main()
