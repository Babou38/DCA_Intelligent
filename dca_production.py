#!/usr/bin/env python3
"""
DCA Intelligent v2 — Production Edition
=========================================
Core-Satellite v7 : BTC/ETH/SOL + BNB/XRP/LINK
Signal combine : RSI par paire + Fear & Greed global (moyenne des deux)
Fallback automatique sur RSI seul si l'API F&G est indisponible.
Logging fiscal complet (PMP), export CSV fiscal francais.

Usage:
    python dca_production.py recommend              # Mise a jour donnees + recommandation semaine
    python dca_production.py recommend --no-update  # Recommandation sans mise a jour
    python dca_production.py update                 # Mise a jour des donnees uniquement
    python dca_production.py buy                    # Enregistrer les achats de la semaine
    python dca_production.py sell BTCUSDT 0.01 95000  # Enregistrer une vente
    python dca_production.py status                 # Portfolio actuel + PMP
    python dca_production.py tax 2025               # Rapport fiscal annee
    python dca_production.py history                # Historique complet
    python dca_production.py backtest               # Backtest 4 ans
"""

import os
import sys
import json
import csv
import time
import urllib.request
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================

WEEKLY_BUDGET = 50.0  # Adapte a ton budget reel

PORTFOLIO_WEIGHTS = {
    # Core (75%)
    'BTCUSDT':  0.40,  # store of value, ETFs institutionnels
    'ETHUSDT':  0.20,  # smart contracts, DeFi, L2 ecosystem
    'SOLUSDT':  0.15,  # L1 haute perf, meilleur challenger ETH
    # Satellite (25%)
    'BNBUSDT':  0.10,  # perf solide + reduit les frais Binance
    'XRPUSDT':  0.08,  # adoption bancaire, top 5 market cap
    'LINKUSDT': 0.07,  # oracles DeFi, revenus reels
}

RSI_MULTIPLIERS = [
    (30, 3.0),
    (45, 2.0),
    (55, 1.0),
    (70, 0.5),
    (100, 0.0),
]

# Fear & Greed : memes paliers que le RSI mais sur sentiment global (0=peur, 100=avidite)
FNG_MULTIPLIERS = [
    (25, 3.0),
    (45, 2.0),
    (55, 1.0),
    (75, 0.5),
    (100, 0.0),
]

LOG_FILE      = 'dca_log.json'
FISCAL_CSV    = 'dca_fiscal.csv'
DATA_DIR      = 'data_4y'
FNG_CACHE     = 'fng_cache.json'

BINANCE_FEE_PCT = 0.075  # 0.075% avec BNB
LEDGER_TRANSFER_THRESHOLD = 200.0

# API Binance
BASE_URL = "https://api.binance.com/api/v3/klines"
INTERVAL = "1h"
SYMBOLS  = list(PORTFOLIO_WEIGHTS.keys())


# =============================================================================
# MISE A JOUR DES DONNEES (incrementale)
# =============================================================================

def fetch_klines(symbol, start_ms, end_ms, limit=1000):
    url = (f"{BASE_URL}?symbol={symbol}&interval={INTERVAL}"
           f"&startTime={start_ms}&endTime={end_ms}&limit={limit}")
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0')
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _read_last_line(filepath):
    """Lit la derniere ligne d'un fichier efficacement."""
    with open(filepath, 'rb') as f:
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            if f.tell() < 2:
                f.seek(0)
                break
            f.seek(-2, os.SEEK_CUR)
        return f.readline().decode().strip()


def update_symbol_data(data_dir, symbol):
    """
    Telecharge les bougies manquantes depuis la derniere mise a jour.
    Si le fichier CSV n'existe pas, telecharge les 4 dernieres annees.
    """
    filename = os.path.join(data_dir, f"{symbol}_{INTERVAL}_1461d.csv")
    end_ms = int(datetime.utcnow().timestamp() * 1000)

    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        last_line = _read_last_line(filename)
        if last_line and last_line != 'timestamp':
            last_ts_str = last_line.split(',')[0]
            try:
                last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S')
                start_ms = int((last_ts + timedelta(hours=1)).timestamp() * 1000)
            except ValueError:
                start_ms = int((datetime.utcnow() - timedelta(days=1461)).timestamp() * 1000)
        else:
            start_ms = int((datetime.utcnow() - timedelta(days=1461)).timestamp() * 1000)

        if start_ms >= end_ms - 3600 * 1000:
            print(f"   {symbol}: deja a jour")
            return

        mode = 'a'
        write_header = False
    else:
        os.makedirs(data_dir, exist_ok=True)
        start_ms = int((datetime.utcnow() - timedelta(days=1461)).timestamp() * 1000)
        mode = 'w'
        write_header = True

    new_klines = []
    current = start_ms
    batch_num = 0

    while current < end_ms:
        batch_num += 1
        retries = 0
        batch = None

        while retries < 3 and batch is None:
            try:
                batch = fetch_klines(symbol, current, end_ms)
            except Exception as e:
                retries += 1
                time.sleep(3 * retries)

        if not batch:
            current += 1000 * 3600 * 1000
            continue

        new_klines.extend(batch)
        current = batch[-1][6] + 1
        time.sleep(0.25)

    if not new_klines:
        print(f"   {symbol}: aucune nouvelle bougie")
        return

    with open(filename, mode, newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(['timestamp', 'open', 'high', 'low', 'close', 'volume',
                             'close_time', 'quote_volume', 'trades',
                             'taker_buy_base_vol', 'taker_buy_quote_vol'])

        seen = set()
        for k in new_klines:
            ts_str = datetime.utcfromtimestamp(k[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            if ts_str in seen:
                continue
            seen.add(ts_str)
            writer.writerow([
                ts_str, k[1], k[2], k[3], k[4], k[5],
                datetime.utcfromtimestamp(k[6] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                k[7], k[8], k[9], k[10]
            ])

    label = "telechargement initial" if write_header else "mise a jour"
    print(f"   {symbol}: +{len(seen)} bougies ({label})")


def cmd_update(args):
    """Met a jour les donnees de marche (bougies manquantes uniquement)."""
    print(f"\n{'=' * 65}")
    print(f"MISE A JOUR DES DONNEES — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'=' * 65}")
    for symbol in SYMBOLS:
        update_symbol_data(args.data_dir, symbol)
    print(f"   OK.")


# =============================================================================
# RSI
# =============================================================================

def compute_rsi(close, period=14):
    rsi = np.full(len(close), np.nan)
    if len(close) < period + 1:
        return rsi
    deltas = np.diff(close)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        rsi[period] = 100.0
    else:
        rsi[period] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi[i + 1] = 100.0
        else:
            rsi[i + 1] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    return rsi


def get_rsi_multiplier(rsi_value):
    for threshold, mult in RSI_MULTIPLIERS:
        if rsi_value < threshold:
            return mult
    return 0.0


def rsi_zone_label(rsi):
    if rsi < 30: return "PANIQUE (3x)"
    if rsi < 45: return "DEPRIME (2x)"
    if rsi < 55: return "NEUTRE (1x)"
    if rsi < 70: return "OPTIMISTE (0.5x)"
    return "EUPHORIE (skip)"


# =============================================================================
# FEAR & GREED INDEX
# =============================================================================

def fetch_fng_api():
    """Appel API alternative.me. Retourne (value, label) ou None si echec."""
    try:
        url = 'https://api.alternative.me/fng/?limit=1&format=json'
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        entry = data['data'][0]
        return int(entry['value']), entry['value_classification']
    except Exception:
        return None


def get_fng():
    """
    Retourne (value, label, source) pour le Fear & Greed Index du jour.

    Priorite :
      1. Cache du jour (evite un appel API inutile)
      2. API alternative.me
      3. Derniere valeur en cache (si API down)
      4. Valeur neutre 50 (fallback ultime)
    """
    today = datetime.now().strftime('%Y-%m-%d')

    cache = {}
    if os.path.exists(FNG_CACHE):
        with open(FNG_CACHE, 'r') as f:
            cache = json.load(f)

    # Cache frais du jour
    if cache.get('date') == today:
        return cache['value'], cache['label'], 'cache du jour'

    # Tentative API
    result = fetch_fng_api()
    if result:
        value, label = result
        cache = {'date': today, 'value': value, 'label': label}
        with open(FNG_CACHE, 'w') as f:
            json.dump(cache, f)
        return value, label, 'api'

    # Fallback : derniere valeur connue
    if cache:
        return cache['value'], cache['label'], f'cache ({cache["date"]}) — API indisponible'

    # Fallback ultime : neutre
    return 50, 'Neutral', 'fallback neutre — API indisponible, aucun cache'


def get_fng_multiplier(fng_value):
    for threshold, mult in FNG_MULTIPLIERS:
        if fng_value < threshold:
            return mult
    return 0.0


def fng_zone_label(fng):
    if fng < 25: return "PEUR EXTREME (3x)"
    if fng < 45: return "PEUR (2x)"
    if fng < 55: return "NEUTRE (1x)"
    if fng < 75: return "AVIDITE (0.5x)"
    return "AVIDITE EXTREME (skip)"


def get_combined_multiplier(rsi_value, fng_value):
    """Moyenne RSI et F&G — les deux signaux ont le meme poids."""
    return (get_rsi_multiplier(rsi_value) + get_fng_multiplier(fng_value)) / 2


# =============================================================================
# TRANSACTION LOG
# =============================================================================

def load_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            return json.load(f)
    return {'transactions': [], 'ledger_pending': {}}


def save_log(log):
    with open(LOG_FILE, 'w') as f:
        json.dump(log, f, indent=2, default=str)


def add_transaction(log, tx_type, pair, quantity, price_unit, total_eur, fee=0, note=""):
    tx = {
        'id':             len(log['transactions']) + 1,
        'date':           datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'type':           tx_type,
        'pair':           pair,
        'asset':          pair.replace('USDT', ''),
        'quantity':       quantity,
        'price_unit':     price_unit,
        'total':          total_eur,
        'fee':            fee,
        'total_with_fee': total_eur + fee,
        'note':           note,
    }
    log['transactions'].append(tx)

    asset = pair.replace('USDT', '')
    if asset not in log['ledger_pending']:
        log['ledger_pending'][asset] = 0
    if tx_type == 'BUY':
        log['ledger_pending'][asset] += total_eur

    save_log(log)
    return tx


# =============================================================================
# PRIX MOYEN PONDERE (PMP) — methode fiscale francaise
# =============================================================================

def compute_pmp(log, pair):
    """
    Calcule le PMP courant pour un asset.
    PMP se recalcule a chaque achat, ne change pas lors d'une vente.
    """
    asset = pair.replace('USDT', '')
    total_quantity = 0.0
    total_cost     = 0.0
    pmp            = 0.0
    history        = []

    for tx in log['transactions']:
        if tx['asset'] != asset:
            continue

        if tx['type'] == 'BUY':
            total_cost     += tx['total_with_fee']
            total_quantity += tx['quantity']
            pmp = total_cost / total_quantity if total_quantity > 0 else 0
            history.append({
                'date': tx['date'], 'type': 'BUY',
                'quantity': tx['quantity'], 'price': tx['price_unit'],
                'total_quantity': total_quantity, 'total_cost': total_cost, 'pmp': pmp,
            })

        elif tx['type'] == 'SELL':
            sell_value   = tx['quantity'] * tx['price_unit']
            cost_basis   = tx['quantity'] * pmp
            capital_gain = sell_value - cost_basis - tx['fee']
            if total_quantity > 0:
                total_cost -= (tx['quantity'] / total_quantity) * total_cost
            total_quantity -= tx['quantity']
            history.append({
                'date': tx['date'], 'type': 'SELL',
                'quantity': tx['quantity'], 'price': tx['price_unit'],
                'sell_value': sell_value, 'cost_basis': cost_basis,
                'capital_gain': capital_gain, 'total_quantity': total_quantity, 'pmp': pmp,
            })

    return {
        'asset': asset, 'pair': pair,
        'total_quantity': total_quantity, 'total_cost': total_cost,
        'pmp': pmp, 'history': history,
    }


def compute_pmp_at_date(log, pair, date_str):
    """Calcule le PMP tel qu'il etait juste avant une date donnee."""
    asset   = pair.replace('USDT', '')
    total_q = 0.0
    total_c = 0.0

    for tx in log['transactions']:
        if tx['date'] >= date_str:
            break
        if tx['asset'] != asset:
            continue
        if tx['type'] == 'BUY':
            total_c += tx['total_with_fee']
            total_q += tx['quantity']
        elif tx['type'] == 'SELL' and total_q > 0:
            total_c -= (tx['quantity'] / total_q) * total_c
            total_q -= tx['quantity']

    return total_c / total_q if total_q > 0 else 0


# =============================================================================
# COMMANDES
# =============================================================================

def cmd_recommend(args):
    """Affiche la recommandation d'achat de la semaine (signal RSI + Fear & Greed)."""
    if not args.no_update:
        cmd_update(args)

    budget = args.budget or WEEKLY_BUDGET

    # --- Fear & Greed ---
    fng_value, fng_label, fng_source = get_fng()
    fng_mult = get_fng_multiplier(fng_value)

    print(f"\n{'=' * 70}")
    print(f"RECOMMANDATION DCA — {datetime.now().strftime('%d/%m/%Y')}")
    print(f"{'=' * 70}")
    print(f"   Budget hebdo:   ${budget:.0f}")
    print(f"   Fear & Greed:   {fng_value}/100 — {fng_label} "
          f"[mult F&G: x{fng_mult}] (source: {fng_source})")
    print(f"   Signal final:   moyenne(RSI paire, F&G global)")

    total = 0
    recs  = []

    for pair, weight in PORTFOLIO_WEIGHTS.items():
        path = os.path.join(args.data_dir, f'{pair}_1h_1461d.csv')
        if not os.path.exists(path):
            print(f"   MANQUANT: {path}")
            continue

        df      = pd.read_csv(path)
        close   = df['close'].values
        rsi_arr = compute_rsi(close, 14)

        price    = close[-1]
        rsi      = rsi_arr[-1] if not np.isnan(rsi_arr[-1]) else 50.0
        rsi_mult = get_rsi_multiplier(rsi)
        combined = get_combined_multiplier(rsi, fng_value)

        base   = budget * weight
        amount = base * combined
        coins  = amount / price if amount > 0 else 0

        recs.append({
            'pair': pair, 'asset': pair.replace('USDT', ''),
            'price': price, 'rsi': rsi, 'rsi_mult': rsi_mult,
            'fng': fng_value, 'fng_mult': fng_mult,
            'combined': combined, 'zone': rsi_zone_label(rsi),
            'weight': weight, 'base': base, 'amount': amount, 'coins': coins,
        })
        total += amount

    print(f"\n   {'Paire':<12} {'Prix':>10} {'RSI':>6} {'xRSI':>5} {'xF&G':>5} {'xFinal':>7} {'Montant':>10} {'Coins':>12}")
    print(f"   {'-' * 78}")

    for r in recs:
        if r['amount'] > 0:
            print(f"   {r['pair']:<12} ${r['price']:>9,.2f} {r['rsi']:>5.1f} "
                  f" {r['rsi_mult']:>3.1f}  {r['fng_mult']:>3.1f}  x{r['combined']:>4.2f} "
                  f"${r['amount']:>9.2f} {r['coins']:>11.6f}")
        else:
            print(f"   {r['pair']:<12} ${r['price']:>9,.2f} {r['rsi']:>5.1f} "
                  f" {r['rsi_mult']:>3.1f}  {r['fng_mult']:>3.1f}  x{r['combined']:>4.2f} "
                  f"{'SKIP':>10} {'—':>12}")

    print(f"\n   Total a investir: ${total:.2f}")

    if total == 0:
        print(f"   RSI + F&G tous en zone euphorie/avidite — on attend la semaine prochaine.")

    log     = load_log()
    pending = log.get('ledger_pending', {})
    for asset, amount in pending.items():
        if amount >= LEDGER_TRANSFER_THRESHOLD:
            print(f"\n   RAPPEL LEDGER: ${amount:.0f} de {asset} en attente de transfert!")

    return recs, total, fng_value, fng_label


def cmd_buy(args):
    """Enregistre les achats de la semaine."""
    recs, total, fng_value, fng_label = cmd_recommend(args)

    if total == 0:
        print("\n   Rien a acheter cette semaine.")
        return

    print(f"\n{'=' * 65}")
    confirm = input("   Confirmer les achats ? (o/n): ").strip().lower()

    if confirm != 'o':
        print("   Annule.")
        return

    log = load_log()

    for r in recs:
        if r['amount'] <= 0:
            continue
        fee = r['amount'] * BINANCE_FEE_PCT / 100
        add_transaction(
            log, 'BUY', r['pair'],
            quantity=r['coins'], price_unit=r['price'],
            total_eur=r['amount'], fee=fee,
            note=f"DCA auto | RSI={r['rsi']:.1f} xRSI={r['rsi_mult']} | F&G={fng_value} ({fng_label}) xFNG={r['fng_mult']} | xFinal={r['combined']:.2f}"
        )
        print(f"   OK {r['pair']}: {r['coins']:.6f} @ ${r['price']:,.2f} = ${r['amount']:.2f} (frais ${fee:.2f})")

    pending          = log.get('ledger_pending', {})
    transfers_needed = [(a, v) for a, v in pending.items() if v >= LEDGER_TRANSFER_THRESHOLD]

    if transfers_needed:
        print(f"\n   TRANSFERT LEDGER RECOMMANDE:")
        for asset, amount in transfers_needed:
            print(f"      {asset}: ${amount:.0f} en attente sur Binance")
        if input("   Transfert effectue ? (o/n): ").strip().lower() == 'o':
            for asset, _ in transfers_needed:
                log['ledger_pending'][asset] = 0
            save_log(log)
            print("   Soldes Binance remis a zero.")

    print(f"\n   Log sauvegarde: {LOG_FILE}")
    export_fiscal_csv(log)


def cmd_sell(args):
    """Enregistre une vente."""
    sell_args = args.sell_args
    if len(sell_args) < 3:
        print("Usage: python dca_production.py sell BTCUSDT 0.01 95000")
        print("       (paire, quantite, prix unitaire)")
        return

    pair     = sell_args[0].upper()
    quantity = float(sell_args[1])
    price    = float(sell_args[2])

    log      = load_log()
    pmp_data = compute_pmp(log, pair)

    if pmp_data['total_quantity'] < quantity:
        print(f"\n   Erreur: tu n'as que {pmp_data['total_quantity']:.6f} "
              f"{pmp_data['asset']} (demande: {quantity})")
        return

    total_sell   = quantity * price
    cost_basis   = quantity * pmp_data['pmp']
    fee          = total_sell * BINANCE_FEE_PCT / 100
    capital_gain = total_sell - cost_basis - fee

    print(f"\n{'=' * 65}")
    print(f"VENTE — {pair}")
    print(f"{'=' * 65}")
    print(f"   Quantite:           {quantity:.6f} {pmp_data['asset']}")
    print(f"   Prix unitaire:      ${price:,.2f}")
    print(f"   Total vente:        ${total_sell:,.2f}")
    print(f"   Frais:              ${fee:.2f}")
    print(f"   PMP actuel:         ${pmp_data['pmp']:,.2f}")
    print(f"   Cout d'acquisition: ${cost_basis:,.2f}")

    if capital_gain >= 0:
        print(f"   Plus-value:         ${capital_gain:+,.2f}")
        print(f"   Impot estime (flat tax 30%): ${capital_gain * 0.30:,.2f}")
    else:
        print(f"   Moins-value:        ${capital_gain:+,.2f}")
        print(f"   Reportable sur les plus-values futures")

    if input(f"\n   Confirmer la vente ? (o/n): ").strip().lower() != 'o':
        print("   Annule.")
        return

    tx = add_transaction(
        log, 'SELL', pair,
        quantity=quantity, price_unit=price,
        total_eur=total_sell, fee=fee,
        note=f"Vente manuelle | PMP={pmp_data['pmp']:.2f} | PV={capital_gain:+.2f}"
    )
    print(f"\n   Vente enregistree (tx #{tx['id']})")
    export_fiscal_csv(log)


def cmd_status(args):
    """Affiche le portfolio actuel avec PMP."""
    log = load_log()

    if not log['transactions']:
        print("\n   Aucune transaction enregistree.")
        print(f"   Commence avec: python dca_production.py buy")
        return

    print(f"\n{'=' * 70}")
    print(f"PORTFOLIO — {datetime.now().strftime('%d/%m/%Y')}")
    print(f"{'=' * 70}")

    print(f"\n   {'Asset':<8} {'Quantite':>12} {'PMP':>10} {'Investi':>10} {'Achats':>7} {'Ventes':>7}")
    print(f"   {'-' * 60}")

    total_invested = 0
    assets_seen    = {tx['pair'] for tx in log['transactions']}

    for pair in sorted(assets_seen):
        pmp    = compute_pmp(log, pair)
        n_buys = sum(1 for tx in log['transactions'] if tx['pair'] == pair and tx['type'] == 'BUY')
        n_sells= sum(1 for tx in log['transactions'] if tx['pair'] == pair and tx['type'] == 'SELL')

        if pmp['total_quantity'] > 0:
            print(f"   {pmp['asset']:<8} {pmp['total_quantity']:>12.6f} "
                  f"${pmp['pmp']:>9,.2f} ${pmp['total_cost']:>9,.2f} "
                  f"{n_buys:>7} {n_sells:>7}")
            total_invested += pmp['total_cost']

    print(f"\n   Total investi (cout d'acquisition): ${total_invested:,.2f}")

    pending    = log.get('ledger_pending', {})
    has_pending= any(v > 0 for v in pending.values())
    if has_pending:
        print(f"\n   En attente transfert Ledger:")
        for asset, amount in pending.items():
            if amount > 0:
                flag = " ← A TRANSFERER" if amount >= LEDGER_TRANSFER_THRESHOLD else ""
                print(f"      {asset}: ${amount:.0f} sur Binance{flag}")


def cmd_tax(args):
    """Genere le rapport fiscal pour une annee."""
    year = int(args.year) if args.year else datetime.now().year
    log  = load_log()

    print(f"\n{'=' * 70}")
    print(f"RAPPORT FISCAL — Annee {year}")
    print(f"{'=' * 70}")

    sells = [tx for tx in log['transactions']
             if tx['type'] == 'SELL' and int(tx['date'][:4]) == year]

    if not sells:
        print(f"\n   Aucune vente en {year}. Rien a declarer.")
        print(f"   En France, seules les cessions sont imposables.")
        return

    total_gains  = 0
    total_losses = 0

    print(f"\n   {'Date':<12} {'Asset':<8} {'Qte':>10} {'Prix vente':>12} "
          f"{'PMP':>10} {'Plus-value':>12}")
    print(f"   {'-' * 70}")

    for tx in sells:
        pmp        = compute_pmp_at_date(log, tx['pair'], tx['date'])
        sell_value = tx['quantity'] * tx['price_unit']
        cost_basis = tx['quantity'] * pmp
        gain       = sell_value - cost_basis - tx['fee']

        if gain >= 0:
            total_gains += gain
        else:
            total_losses += abs(gain)

        print(f"   {tx['date'][:10]:<12} {tx['asset']:<8} {tx['quantity']:>10.6f} "
              f"${tx['price_unit']:>11,.2f} ${pmp:>9,.2f} ${gain:>+11,.2f}")

    net = total_gains - total_losses

    print(f"\n   {'-' * 50}")
    print(f"   Plus-values totales:  ${total_gains:>+,.2f}")
    print(f"   Moins-values totales: -${total_losses:>,.2f}")
    print(f"   Plus-value nette:     ${net:>+,.2f}")

    if net > 0:
        print(f"\n   Flat tax (30%):       ${net * 0.30:,.2f}")
        print(f"     - Impot (12.8%):     ${net * 0.128:,.2f}")
        print(f"     - Prelevements soc. (17.2%): ${net * 0.172:,.2f}")
        print(f"\n   A reporter sur le formulaire 2086 (cerfa)")
    else:
        print(f"\n   Moins-value nette — reportable sur les plus-values futures")

    export_fiscal_csv(log, year)


def cmd_history(args):
    """Affiche l'historique complet."""
    log = load_log()

    if not log['transactions']:
        print("\n   Aucune transaction.")
        return

    print(f"\n{'=' * 80}")
    print(f"HISTORIQUE DES TRANSACTIONS")
    print(f"{'=' * 80}")
    print(f"\n   {'#':>4} {'Date':<20} {'Type':<6} {'Asset':<8} {'Quantite':>12} "
          f"{'Prix':>10} {'Total':>10} {'Frais':>8}")
    print(f"   {'-' * 80}")

    for tx in log['transactions']:
        sign = "+" if tx['type'] == 'BUY' else "-"
        print(f"   {tx['id']:>4} {tx['date']:<20} {sign}{tx['type']:<5} {tx['asset']:<8} "
              f"{tx['quantity']:>12.6f} ${tx['price_unit']:>9,.2f} "
              f"${tx['total']:>9,.2f} ${tx['fee']:>7.2f}")

    n_buys      = sum(1 for tx in log['transactions'] if tx['type'] == 'BUY')
    n_sells     = sum(1 for tx in log['transactions'] if tx['type'] == 'SELL')
    total_bought= sum(tx['total'] for tx in log['transactions'] if tx['type'] == 'BUY')
    total_fees  = sum(tx['fee']   for tx in log['transactions'])

    print(f"\n   {n_buys} achats | {n_sells} ventes | "
          f"${total_bought:,.2f} investi | ${total_fees:,.2f} frais")


# =============================================================================
# EXPORT FISCAL CSV
# =============================================================================

def export_fiscal_csv(log, year=None):
    """Export CSV compatible declaration fiscale. Colonnes conformes au formulaire 2086."""
    rows        = []
    pmp_tracker = {}

    for tx in log['transactions']:
        asset = tx['asset']
        if asset not in pmp_tracker:
            pmp_tracker[asset] = {'qty': 0.0, 'cost': 0.0}

        if tx['type'] == 'BUY':
            pmp_tracker[asset]['cost'] += tx['total_with_fee']
            pmp_tracker[asset]['qty']  += tx['quantity']
            pmp = pmp_tracker[asset]['cost'] / pmp_tracker[asset]['qty']
            row = {
                'Date': tx['date'], 'Type': 'ACHAT', 'Asset': asset,
                'Paire': tx['pair'], 'Quantite': tx['quantity'],
                'Prix_unitaire': tx['price_unit'], 'Montant_total': tx['total'],
                'Frais': tx['fee'], 'Cout_acquisition': tx['total_with_fee'],
                'PMP_apres_operation': pmp, 'Plus_value': '', 'Note': tx.get('note', ''),
            }

        elif tx['type'] == 'SELL':
            pmp  = (pmp_tracker[asset]['cost'] / pmp_tracker[asset]['qty']
                    if pmp_tracker[asset]['qty'] > 0 else 0)
            sell_value = tx['quantity'] * tx['price_unit']
            cost_basis = tx['quantity'] * pmp
            gain       = sell_value - cost_basis - tx['fee']

            if pmp_tracker[asset]['qty'] > 0:
                pmp_tracker[asset]['cost'] -= (tx['quantity'] / pmp_tracker[asset]['qty']) * pmp_tracker[asset]['cost']
            pmp_tracker[asset]['qty'] -= tx['quantity']

            row = {
                'Date': tx['date'], 'Type': 'VENTE', 'Asset': asset,
                'Paire': tx['pair'], 'Quantite': tx['quantity'],
                'Prix_unitaire': tx['price_unit'], 'Montant_total': tx['total'],
                'Frais': tx['fee'], 'Cout_acquisition': cost_basis,
                'PMP_apres_operation': pmp, 'Plus_value': gain, 'Note': tx.get('note', ''),
            }
        else:
            continue

        tx_year = int(tx['date'][:4])
        if year is None or tx_year == year:
            rows.append(row)

    if not rows:
        return

    suffix    = f"_{year}" if year else ""
    filepath  = f"dca_fiscal{suffix}.csv"
    fieldnames = ['Date', 'Type', 'Asset', 'Paire', 'Quantite', 'Prix_unitaire',
                  'Montant_total', 'Frais', 'Cout_acquisition', 'PMP_apres_operation',
                  'Plus_value', 'Note']

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"   Export fiscal: {filepath}")


# =============================================================================
# BACKTEST
# =============================================================================

def cmd_backtest(args):
    """Backtest comparatif v1 (poids egaux) vs v2 (core-satellite) sur 4 ans."""
    budget        = args.budget or WEEKLY_BUDGET
    equal_weights = {p: 1 / 6 for p in PORTFOLIO_WEIGHTS}

    print(f"\n{'=' * 70}")
    print(f"BACKTEST 4 ANS — Budget ${budget}/sem")
    print(f"{'=' * 70}")

    for label, weights in [("v1 Poids Egaux", equal_weights),
                            ("v2 Core-Satellite", PORTFOLIO_WEIGHTS)]:
        total_inv, total_val = 0, 0
        pair_results = []

        for pair, weight in weights.items():
            path = os.path.join(args.data_dir, f'{pair}_1h_1461d.csv')
            if not os.path.exists(path):
                continue

            df    = pd.read_csv(path)
            close = df['close'].values
            rsi   = compute_rsi(close, 14)
            pair_budget = budget * weight

            inv, coins = 0, 0
            for i in range(0, len(close), 168):
                r      = rsi[i] if not np.isnan(rsi[i]) else 50.0
                amount = pair_budget * get_rsi_multiplier(r)
                if amount > 0:
                    coins += amount / close[i]
                    inv   += amount

            val     = coins * close[-1]
            pnl     = val - inv
            pnl_pct = pnl / inv * 100 if inv > 0 else 0
            total_inv += inv
            total_val += val
            pair_results.append((pair, weight, inv, val, pnl, pnl_pct))

        total_pnl = total_val - total_inv
        total_pct = total_pnl / total_inv * 100 if total_inv > 0 else 0
        sign      = '+' if total_pnl > 0 else ''

        print(f"\n  {label}: ${total_inv:,.0f} investi -> ${total_val:,.0f} "
              f"-> {sign}${total_pnl:,.0f} ({sign}{total_pct:.2f}%)")

        for pair, w, inv, val, pnl, pct in sorted(pair_results, key=lambda x: -x[4]):
            sign = '+' if pnl > 0 else ''
            print(f"     {pair:<12} {w*100:>5.0f}% | ${inv:>8,.0f} -> ${val:>8,.0f} "
                  f"| {sign}${pnl:>8,.0f} ({sign}{pct:.1f}%)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='DCA Intelligent v2 — Production')
    parser.add_argument('command', nargs='?', default='recommend',
                        choices=['recommend', 'update', 'buy', 'sell',
                                 'status', 'tax', 'history', 'backtest'],
                        help='Commande a executer')
    parser.add_argument('sell_args', nargs='*', default=[],
                        help='Arguments pour sell (PAIR QUANTITY PRICE) ou tax (YEAR)')
    parser.add_argument('--budget',    type=float, default=None)
    parser.add_argument('--year',      type=str,   default=None)
    parser.add_argument('--data-dir',  type=str,   default=DATA_DIR)
    parser.add_argument('--no-update', action='store_true',
                        help='Ne pas mettre a jour les donnees avant recommend')

    args = parser.parse_args()

    # Recuperer l'annee pour la commande tax
    if args.command == 'tax' and not args.year and args.sell_args:
        args.year = args.sell_args[0]

    print("=" * 70)
    print("DCA INTELLIGENT v2 — Production")
    print("=" * 70)

    commands = {
        'recommend': cmd_recommend,
        'update':    cmd_update,
        'buy':       cmd_buy,
        'sell':      cmd_sell,
        'status':    cmd_status,
        'tax':       cmd_tax,
        'history':   cmd_history,
        'backtest':  cmd_backtest,
    }

    commands[args.command](args)
    print()


if __name__ == '__main__':
    main()
