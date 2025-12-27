import asyncio
import os
import sys
import matplotlib
matplotlib.use('Agg') 
import discord
from dotenv import load_dotenv
from discord.ext import commands, tasks
from discord.commands import Option
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import io
import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo
import random
import yfinance as yf
import sqlite3
import json
import time
import scipy.stats as si
import pytz
from matplotlib.patches import Rectangle, Patch

# --- CONFIGURATION ---
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN") 

# --- BEEKS QUOTES ---
MOVIE_QUOTES = [
    "Hey. Back off! I'll rip out your eyes and piss on your brain.",
    "That's happy. In this country we say Happy New Year.",
    "This is as far as we go. No more cockamamie cigar smoke. No more Swedish meatballs there, tootsie. And no more phony Irish whiskey. No more goddamn jerky beef! The party's over.",
    "Alright, on your feet. Up! Let's go. Bunch of fucking weirdos! We're going to take a little walk. And don't try anything funny or the whore loses a kidney. Let's go!",
    "Operation Strange Brew proceeding according to plan. I anticipate penetration and acquisition at 2100 hours tomorrow.",
    "How'd you like to make a fast hundred?",
    "Fuck off.",
    "Money isn't everything, Mortimer. But having it is.",
    "When I was a kid, if we wanted a jacuzzi, we had to fart in the tub.",
    "May I suggest using your night stick officer?",
    "That's called the 'quart of blood' technique. You do that, a quart of blood will drop out of a person's body.",
    "Those men wanted to have sex with me!",
    "I had the most absurd nightmare. I was poor and no one liked me. I lost my job, I lost my house, Penelope hated me and it was all because of this terrible, awful Negro.",
    "He was wearing my Harvard tie. Can you believe it? MY Harvard tie. Like oh, sure, HE went to Harvard.",
    "And she stepped on the ball.",
    "It ain't cool being no jive turkey so close to Thanksgiving.",
    "Yeah!",
    "Winthorpe, my boy, what have you got for us?",
    "Cause I'm a karate man, see! And a karate man bruises on the inside! They don't show their weakness. But you don't know that because you're a big Barry White looking motherfucker! So get outta my face!",
    "Why don't you retire, sir? You've got a big day ahead of you tomorrow.",
    "You want me to break something else?",
    "That was a cheap vase, right? That was a fake? Right?",
    "Sounds to me like you guys a couple of bookies!",
    "Man, that watch is so hot, it's smokin'.",
    "This is a Rouchefoucauld. The thinnest water-resistant watch in the world. Singularly unique, sculptured in design, hand-crafted in Switzerland, and water resistant to three atmospheres. This is the sports watch of the '80s.",
    "Look, it tells time simultaneously in Monte Carlo, Beverly Hills, London, Paris, Rome, and Gstaad.",
    "In Philadelphia, it's worth 50 bucks.",
    "How much for the gun?",
    "Exactly why do you think the price of pork bellies is going to keep going down, William?",
    "It … was … the … Dukes! It … was … the … Dukes!",
    "Yeah. You know, it occurs to me that the best way you hurt rich people is by turning them into poor people.",
    "Merry New Year!",
    "Boo bwele boo bwele boo bwele ah ha! Boo bwele boo bwele boo bwele ah ha!",
    "Something's wrong! Where's Wilson?"
]

# --- SYMBOL MAPPINGS ---
YF_SYMBOLS = {
    "spx": "^GSPC", "es": "^GSPC",
    "ndx": "^NDX",  "nq": "^NDX",
    "rut": "^RUT",  "rty": "^RUT",
    "ym": "YM=F",   "dow": "YM=F",
    "vix": "^VIX",  "sp": "^GSPC",
    "spy": "SPY",   "qqq": "QQQ",
    "iwm": "IWM",   "dia": "DIA",
    "vixy": "VIXY", "uvxy": "UVXY",
    "tlt": "TLT",   "hyg": "HYG"
}

IV_PROXIES = { "^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM" }

def resolve_yf_symbol(user_input: str) -> str:
    return YF_SYMBOLS.get(user_input.lower(), user_input.upper())

def get_options_ticker(yf_sym):
    if yf_sym == "^GSPC": return "^SPX"
    return yf_sym

# --- DATABASE ENGINE ---
def init_db():
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS market_data (ticker TEXT PRIMARY KEY, data_json TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS chain_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, tag TEXT, anchor_price REAL, dividend_yield REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, data_json TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings (user_id INTEGER, setting_key TEXT, setting_value TEXT, PRIMARY KEY (user_id, setting_key))''')
    conn.commit()
    conn.close()
    print("DEBUG: Database initialized (beeks.db).")

def save_to_db(ticker, data_dict):
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        json_str = json.dumps(data_dict, default=str)
        c.execute('INSERT OR REPLACE INTO market_data (ticker, data_json, timestamp) VALUES (?, ?, CURRENT_TIMESTAMP)', (ticker, json_str))
        conn.commit()
        conn.close()
    except Exception as e: print(f"ERROR: Could not save to DB: {e}")

def load_from_db(ticker):
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        c.execute('SELECT data_json, timestamp FROM market_data WHERE ticker = ?', (ticker,))
        row = c.fetchone()
        conn.close()
        if row:
            data = json.loads(row[0])
            data['saved_at'] = row[1]
            return data
    except: return None

def save_snapshot(ticker, full_chain_data, price, div_yield, tag="MANUAL", custom_timestamp=None):
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        full_chain_data['anchor_price'] = price
        full_chain_data['dividend_yield'] = div_yield
        json_str = json.dumps(full_chain_data, default=str)
        if not custom_timestamp:
            now_ny = datetime.datetime.now(ZoneInfo("America/New_York"))
            custom_timestamp = now_ny.strftime("%Y-%m-%d %H:%M:%S")
        date_part = custom_timestamp.split(" ")[0]
        c.execute("SELECT id FROM chain_snapshots WHERE ticker = ? AND tag = ? AND date(timestamp) = ?", (ticker, tag, date_part))
        if c.fetchone():
            conn.close()
            print(f"DEBUG: 🛑 Save Rejected. {ticker} [{tag}] already exists for {date_part}.")
            return False
        c.execute('INSERT INTO chain_snapshots (ticker, tag, anchor_price, dividend_yield, data_json, timestamp) VALUES (?, ?, ?, ?, ?, ?)', 
                  (ticker, tag, price, div_yield, json_str, custom_timestamp))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"ERROR: Could not save snapshot: {e}")
        return False

def get_latest_tag_for_date(ticker, date_str):
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        c.execute("SELECT tag FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? ORDER BY id DESC LIMIT 1", (ticker, date_str))
        row = c.fetchone()
        conn.close()
        if row: return row[0]
    except: pass
    return "CLOSE"

def get_user_terminal_setting(user_id):
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    c.execute('SELECT setting_value FROM user_settings WHERE user_id = ? AND setting_key = ?', (user_id, "terminal_global"))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 'bloomberg'

def set_user_terminal_setting(user_id, value):
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO user_settings (user_id, setting_key, setting_value) VALUES (?, ?, ?)', (user_id, "terminal_global", value))
    conn.commit()
    conn.close()

def delete_snapshot_from_db(ticker, date_str, tag):
    ticker = get_options_ticker(ticker)
    deleted_count = 0
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        c.execute("DELETE FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? AND tag = ?", (ticker, date_str, tag))
        deleted_count = c.rowcount
        conn.commit()
        conn.close()
    except: pass
    return deleted_count > 0

# --- HELPERS ---
def validate_atm_data(tkr, current_price):
    try:
        exps = tkr.options
        if not exps: return False
        chain = tkr.option_chain(exps[0])
        df = pd.concat([chain.calls, chain.puts])
        if df.empty: return False
        df['distance'] = abs(df['strike'] - current_price)
        atm_opts = df.sort_values('distance').head(4)
        if len(atm_opts) < 4: return False
        valid_count = len(atm_opts[atm_opts['impliedVolatility'] > 0.001])
        return valid_count >= 3
    except Exception as e:
        print(f"DEBUG: Validation failed: {e}")
        return False

def get_current_yield(ticker):
    try:
        if ticker in ["^GSPC", "^SPX"]: sym = "SPY"
        elif ticker in ["^NDX"]: sym = "QQQ"
        else: sym = ticker
        tkr = yf.Ticker(sym)
        y = tkr.info.get('trailingAnnualDividendYield')
        if y is None: y = tkr.info.get('dividendYield')
        return y if y else 0.0
    except: return 0.0

def is_third_friday(date_str):
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        return d.weekday() == 4 and 15 <= d.day <= 21
    except: return False

def get_next_market_date(current_dt):
    current_date = current_dt.date()
    is_after_market_close = current_dt.weekday() == 4 and current_dt.hour >= 16
    if current_date.weekday() >= 5 or is_after_market_close:
        days_ahead = (7 - current_date.weekday()) % 7
        if days_ahead == 0: days_ahead = 1
        elif days_ahead > 2: days_ahead = 3 
        next_market_date = current_date + timedelta(days=days_ahead)
    else:
        next_market_date = current_date + timedelta(days=1)
    if next_market_date.weekday() >= 5: 
        next_market_date += timedelta(days=(7 - next_market_date.weekday()))
    return next_market_date.strftime("%Y-%m-%d")

# --- CORE LOGIC ---
def fetch_market_data(ticker):
    yf_sym = resolve_yf_symbol(ticker)
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")
    
    try:
        tkr = yf.Ticker(yf_sym)
        hist = tkr.history(period="3mo") 
        if hist.empty: return None 
        
        last_row = hist.iloc[-1]
        is_market_open = (last_row.name.date() == today)
        anchor_price = last_row['Open'] if is_market_open else last_row['Close']
        
        returns = np.log(hist['Close'] / hist['Close'].shift(1)).dropna().tail(30)
        hv_annual = returns.std() * np.sqrt(252)
        if np.isnan(hv_annual) or hv_annual == 0: return None 
        
        iv_annual = hv_annual 
        try:
            search_tkr = tkr
            if yf_sym == "^GSPC":
                try: 
                    spx = yf.Ticker("^SPX")
                    if spx.options: search_tkr = spx
                except: pass
            
            if not validate_atm_data(search_tkr, anchor_price): pass
            else:
                all_exps = search_tkr.options
                valid_exps = [e for e in all_exps if (datetime.datetime.strptime(e, "%Y-%m-%d").date() - today).days >= 30]
                target_exp = None
                for exp in valid_exps:
                    if is_third_friday(exp): target_exp = exp; break
                if not target_exp and valid_exps: target_exp = valid_exps[0]
                
                if target_exp:
                    chain = search_tkr.option_chain(target_exp)
                    calls = chain.calls.copy(); calls['type'] = 'C'
                    puts = chain.puts.copy(); puts['type'] = 'P'
                    calls['abs_diff'] = abs(calls['strike'] - anchor_price)
                    puts['abs_diff'] = abs(puts['strike'] - anchor_price)
                    all_opts = pd.concat([calls, puts]).sort_values('abs_diff')
                    valid_opts = all_opts[all_opts['impliedVolatility'] > 0].head(4)
                    if not valid_opts.empty:
                        avg_iv = valid_opts['impliedVolatility'].mean()
                        if 0.01 < avg_iv < 5.0: iv_annual = avg_iv
        except: pass

        packet = {"date": today_str, "ticker": yf_sym, "anchor_price": anchor_price, "iv": iv_annual, "hv": hv_annual, "date_obj": last_row.name}
        save_to_db(yf_sym, packet)
        return packet
    except: 
        return load_from_db(yf_sym)

def fetch_historical_data(ticker, date_str, tag):
    db_ticker = get_options_ticker(ticker)
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        c.execute("SELECT data_json FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? AND tag = ? ORDER BY id DESC LIMIT 1", (db_ticker, date_str, tag))
        row = c.fetchone()
        conn.close()
        if not row: return None
        snapshot = json.loads(row[0])
        anchor_price = snapshot.get('anchor_price')
        if not anchor_price: return None

        return {"date": date_str, "ticker": ticker, "anchor_price": anchor_price, "iv": 0.15, "hv": 0.15, "saved_at": snapshot['timestamp'], "is_backtest": True}
    except: return None

def fetch_and_enrich_chain(ticker, expiry_date, snapshot_date=None, snapshot_tag=None, target_strike=None, range_count=None, pivot=None, scope="Front Month"):
    yf_sym = resolve_yf_symbol(ticker)
    display_ticker = get_options_ticker(yf_sym)
    
    S = 0.0; q = 0.0; r = 0.045
    chain_data = {'calls': [], 'puts': []}
    
    # --- SNAPSHOT REPLAY ---
    if snapshot_date:
        tag = snapshot_tag if snapshot_tag else "CLOSE"
        try:
            conn = sqlite3.connect("beeks.db")
            c = conn.cursor()
            c.execute("SELECT data_json, timestamp, anchor_price, dividend_yield FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? AND tag = ? ORDER BY id DESC LIMIT 1", (display_ticker, snapshot_date, tag))
            row = c.fetchone()
            conn.close()
            if not row: return None
            data = json.loads(row[0])
            try: snap_ts = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            except: snap_ts = datetime.datetime.fromisoformat(row[1])
            S = row[2] if row[2] else data.get('anchor_price', 0)
            q = row[3] if row[3] is not None else data.get('dividend_yield', 0.0)
            
            target_exps = []
            if scope == "0DTE":
                if expiry_date and expiry_date in data['expirations']: target_exps = [expiry_date]
                else:
                    snap_date_str = snap_ts.strftime("%Y-%m-%d")
                    if snap_date_str in data['expirations']: target_exps = [snap_date_str]
                    else: return None
            elif scope == "Front Month":
                target_exps = [e for e in data['expirations'] if 0 <= (datetime.datetime.strptime(e, "%Y-%m-%d") - snap_ts).days <= 30]
            elif scope == "Total Market": target_exps = list(data['expirations'].keys())
            else: 
                if expiry_date in data['expirations']: target_exps = [expiry_date]
            
            for e in target_exps:
                exp_dt = datetime.datetime.strptime(e, "%Y-%m-%d")
                t_val = (exp_dt - snap_ts).days / 365.0
                if t_val < 0.001: t_val = 0.001 
                c_list = data['expirations'][e]['calls']; p_list = data['expirations'][e]['puts']
                for x in c_list: x['time_year'] = t_val
                for x in p_list: x['time_year'] = t_val
                chain_data['calls'].extend(c_list); chain_data['puts'].extend(p_list)
        except: return None
        
    # --- LIVE DATA ---
    else:
        try:
            tkr = yf.Ticker(yf_sym)
            hist = tkr.history(period="1d")
            if hist.empty: return None
            S = hist['Close'].iloc[-1]
            q = get_current_yield(yf_sym)

            # --- GATEKEEPER VALIDATION ---
            if not validate_atm_data(tkr, S): return None
            
            search_tkr = tkr
            if yf_sym == "^GSPC":
                 try: 
                     spx = yf.Ticker("^SPX"); 
                     if spx.options: search_tkr = spx
                 except: pass

            all_exps = search_tkr.options
            if not all_exps: return None
            
            target_exps = []
            now = datetime.datetime.now()
            
            if scope == "0DTE":
                today_str = now.strftime("%Y-%m-%d")
                if today_str in all_exps: target_exps = [today_str]
                else: return []
            elif scope == "Total Market": target_exps = all_exps 
            elif scope == "Front Month":
                target_exps = [e for e in all_exps if 0 <= (datetime.datetime.strptime(e, "%Y-%m-%d") - now).days <= 35]
            else:
                if expiry_date in all_exps: target_exps = [expiry_date]

            for e in target_exps:
                try:
                    opt = search_tkr.option_chain(e)
                    c_list = opt.calls.to_dict(orient='records'); p_list = opt.puts.to_dict(orient='records')
                    exp_dt = datetime.datetime.strptime(e, "%Y-%m-%d")
                    t_val = (exp_dt - now).days / 365.0
                    if t_val < 0.001: t_val = 0.001
                    for x in c_list: x['time_year'] = t_val
                    for x in p_list: x['time_year'] = t_val
                    chain_data['calls'].extend(c_list); chain_data['puts'].extend(p_list)
                except: pass
        except: return None

    all_options = []
    for c in chain_data['calls']: c['type'] = 'Call'; all_options.append(c)
    for p in chain_data['puts']: p['type'] = 'Put'; all_options.append(p)
    df = pd.DataFrame(all_options)
    if df.empty: return []
    
    df.rename(columns={'impliedVolatility': 'iv', 'openInterest': 'oi', 'volume': 'vol', 'lastPrice': 'price'}, inplace=True, errors='ignore')

    if target_strike: df = df[df['strike'] == target_strike]
    elif range_count:
        anchor = pivot if pivot else S
        unique_strikes = df['strike'].unique()
        dists = pd.DataFrame({'strike': unique_strikes, 'dist': abs(unique_strikes - anchor)})
        keep_strikes = dists.sort_values('dist').head(range_count)['strike'].values
        df = df[df['strike'].isin(keep_strikes)]

    results = []
    if range_count is not None:
         for _, row in df.iterrows():
            K = row['strike']; IV = row['iv']
            if not IV or IV < 0.001: continue
            delta, gamma, theta, vanna, charm = calculate_black_scholes(S, K, row['time_year'], r, IV, q, row['type'].lower())
            item = {'strike': K, 'type': row['type'], 'price': row.get('price', 0), 'bid': row.get('bid', 0), 'ask': row.get('ask', 0), 'volume': row.get('vol', 0), 'oi': row.get('oi', 0), 'iv': IV, 'delta': delta, 'gamma': gamma, 'theta': theta, 'vanna': vanna, 'charm': charm, 'time_year': row.get('time_year', 0), 'spot': S}
            results.append(item)
         return sorted(results, key=lambda x: x['strike'])
    else: return df.to_dict(orient='records')

def calculate_levels(price, iv, hv, engine="Insider Info"):
    divisor = np.sqrt(365) if engine.strip().lower() == "insider info" else np.sqrt(252)
    move_iv = price * (iv / divisor); move_hv = price * (hv / divisor)
    levels = {"meta": {"engine": engine, "daily_move_iv": move_iv, "daily_move_hv": move_hv}, "valentine": {}, "winthorpe": {}}
    for i in range(1, 5):
        levels["valentine"][f"+{i}σ"] = price + (move_iv * i); levels["valentine"][f"-{i}σ"] = price - (move_iv * i)
        levels["winthorpe"][f"+{i}σ"] = price + (move_hv * i); levels["winthorpe"][f"-{i}σ"] = price - (move_hv * i)
    return levels

def calculate_black_scholes(S, K, T, r, sigma, q=0.0, option_type="call"):
    try:
        S = np.array(S, dtype=float); K = np.array(K, dtype=float); T = np.array(T, dtype=float); sigma = np.array(sigma, dtype=float)
        T = np.maximum(T, 1e-5)
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        N_d1 = si.norm.cdf(d1); N_d2 = si.norm.cdf(d2); pdf_d1 = si.norm.pdf(d1)
        gamma = (pdf_d1 * np.exp(-q * T)) / (S * sigma * np.sqrt(T))
        vanna = -np.exp(-q * T) * pdf_d1 * (d2 / sigma)
        
        if option_type == "call":
            delta = np.exp(-q * T) * N_d1
            theta = (- (S * sigma * np.exp(-q * T) * pdf_d1) / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * N_d2 + q * S * np.exp(-q * T) * N_d1) / 365.0
            term1 = q * np.exp(-q * T) * N_d1; term2 = np.exp(-q * T) * pdf_d1 * (2 * (r - q) * T - d2 * sigma * np.sqrt(T)) / (2 * T * sigma * np.sqrt(T))
            charm = (term1 - term2) / 365.0
        else:
            delta = np.exp(-q * T) * (N_d1 - 1)
            theta = (- (S * sigma * np.exp(-q * T) * pdf_d1) / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * (1 - N_d2) - q * S * np.exp(-q * T) * (1 - N_d1)) / 365.0
            term1 = -q * np.exp(-q * T) * (1 - N_d1); term2 = np.exp(-q * T) * pdf_d1 * (2 * (r - q) * T - d2 * sigma * np.sqrt(T)) / (2 * T * sigma * np.sqrt(T))
            charm = (term1 - term2) / 365.0

        if np.ndim(delta) == 0: return float(delta), float(gamma), float(theta), float(vanna), float(charm)
        return delta, gamma, theta, vanna, charm
    except: return 0.0, 0.0, 0.0, 0.0, 0.0

def calculate_gamma_flip(chain_data, current_spot, r=0.045, q=0.0):
    if not chain_data: return None, None
    df = pd.DataFrame(chain_data)
    if df.empty: return None, None
    df = df[(df['iv'] > 0.001) & (df['oi'] > 0)]
    if df.empty: return None, None
    
    strikes = df['strike'].values; ivs = df['iv'].values; Ts = df.get('time_year', pd.Series([0.1]*len(df))).values 
    is_call = (df['type'] == 'Call').values; ois = df['oi'].values
    sim_spots = np.linspace(current_spot * 0.93, current_spot * 1.07, 100)
    net_gammas = []
    
    for sim_S in sim_spots:
        _, gammas, _, _, _ = calculate_black_scholes(sim_S, strikes, Ts, r, ivs, q, "call") 
        call_gex = np.sum(gammas[is_call] * ois[is_call] * 100)
        put_gex = np.sum(gammas[~is_call] * ois[~is_call] * 100 * -1)
        net_gammas.append(call_gex + put_gex)
        
    net_gammas = np.array(net_gammas); signs = np.sign(net_gammas); diffs = np.diff(signs); cross_indices = np.where(diffs != 0)[0]
    flip_price = None
    if len(cross_indices) > 0:
        idx = cross_indices[np.abs(sim_spots[cross_indices] - current_spot).argmin()]
        y1, y2 = net_gammas[idx], net_gammas[idx+1]; x1, x2 = sim_spots[idx], sim_spots[idx+1]
        if y2 != y1: flip_price = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
    return flip_price, (sim_spots, net_gammas)

def calculate_market_exposures(chain_data, spot_price):
    total_gex = 0.0; total_dex = 0.0; total_vex = 0.0
    for opt in chain_data:
        delta = opt['delta']; gamma = opt['gamma']; vanna = opt.get('vanna', 0.0); oi = opt['oi']
        if pd.isna(oi) or oi <= 0: continue
        is_call = opt['type'].lower() == 'call'
        
        contract_gex = (gamma * oi * 100) * (spot_price**2) * 0.01
        if not is_call: contract_gex *= -1
        total_gex += contract_gex
        
        contract_dex = (delta * oi * 100 * spot_price)
        if is_call: contract_dex *= -1 
        else: contract_dex *= 1 
        total_dex += contract_dex

        contract_vex = (vanna * 0.01) * oi * 100 * spot_price
        if is_call: contract_vex *= -1 
        else: contract_vex *= 1 
        total_vex += contract_vex
        
    return total_gex, total_dex, total_vex

def calculate_strike_exposures(chain_data, spot_price, ticker):
    strikes = {}
    if ticker in ["^SPX", "^GSPC", "SPX", "ES=F"]: r_pts = 125 
    elif ticker in ["^NDX", "NDX", "NQ=F"]: r_pts = 250
    else: r_pts = spot_price * 0.10 
    min_strike = spot_price - r_pts; max_strike = spot_price + r_pts
    
    for opt in chain_data:
        k = opt['strike']
        if k < min_strike or k > max_strike: continue
        if k not in strikes: strikes[k] = {'gex': 0.0, 'dex': 0.0, 'vex': 0.0, 'cex': 0.0}
        
        gamma = opt['gamma']; delta = opt['delta']; vanna = opt.get('vanna', 0.0); charm = opt.get('charm', 0.0); oi = opt['oi']
        if pd.isna(oi) or oi <= 0: continue
        is_call = opt['type'].lower() == 'call'
        
        g_val = (gamma * oi * 100) * (spot_price**2) * 0.01
        if not is_call: g_val *= -1
        strikes[k]['gex'] += g_val
        
        d_val = (delta * oi * 100 * spot_price)
        if is_call: d_val *= -1 
        else: d_val *= 1 
        strikes[k]['dex'] += d_val
        
        v_val = (vanna * 0.01) * oi * 100 * spot_price
        if is_call: v_val *= -1
        else: v_val *= 1
        strikes[k]['vex'] += v_val

        c_val = (charm * oi * 100 * spot_price)
        if is_call: c_val *= -1
        else: c_val *= 1
        strikes[k]['cex'] += c_val
        
    sorted_strikes = sorted(strikes.keys())
    return {
        'strikes': sorted_strikes,
        'gex': [strikes[k]['gex'] for k in sorted_strikes],
        'dex': [strikes[k]['dex'] for k in sorted_strikes],
        'vex': [strikes[k]['vex'] for k in sorted_strikes],
        'cex': [strikes[k]['cex'] for k in sorted_strikes]
    }

# --- PLOTTING ---
def create_beeks_chart(display_ticker, data, levels, view_mode="Insider Info"):
    fig = plt.figure(figsize=(12, 8)); plt.style.use('dark_background'); plt.subplots_adjust(left=0.02, right=0.98, top=0.88, bottom=0.02)
    price = data['anchor_price']; ax = plt.gca(); trans = ax.get_yaxis_transform(); all_values = [price]
    for label, val in levels['winthorpe'].items():
        all_values.append(val); plt.axhline(val, color='orange', linestyle=':', alpha=0.7, linewidth=1.5); plt.text(0.01, val, f"{label}: {val:.2f}", color='orange', fontsize=9, va='bottom', fontweight='bold', transform=trans)
    for label, val in levels['valentine'].items():
        all_values.append(val); plt.axhline(val, color='cyan', linestyle='-', alpha=0.7, linewidth=1.5); plt.text(0.99, val, f"{label}: {val:.2f}", color='cyan', fontsize=9, va='bottom', ha='right', fontweight='bold', transform=trans)
    plt.axhline(price, color='white', linewidth=2, linestyle='--'); plt.text(0.5, price, f"{price:.2f}", color='white', fontsize=10, fontweight='bold', va='bottom', ha='center', transform=trans)
    y_min = min(all_values); y_max = max(all_values); diff = y_max - y_min; 
    if diff < 1.0: diff = price * 0.01 
    buffer = diff * 0.10; plt.ylim(y_min - buffer, y_max + buffer)
    plt.title(f"Clarence Beeks Report: {display_ticker}\nEngine: {view_mode}", color='white', fontsize=14, weight='bold')
    plt.grid(False); ax.tick_params(axis='y', colors='white', labelsize=10); ax.tick_params(axis='x', colors='white', bottom=False, labelbottom=False) 
    from matplotlib.lines import Line2D
    custom_lines = [Line2D([0], [0], color='cyan', lw=2), Line2D([0], [0], color='orange', lw=2, linestyle=':')]
    plt.legend(custom_lines, ['VALENTINE', 'WINTHORPE'], loc='upper left', facecolor='black', labelcolor='white', framealpha=0.6)
    buf = io.BytesIO(); fig.savefig(buf, format='png', facecolor='black', dpi=120); buf.seek(0); plt.close()
    return buf

def generate_exposure_dashboard(ticker, spot, gex, dex, vex, scope_label, expiry_label):
    plt.figure(figsize=(12, 6)); plt.style.use('dark_background'); fig, ax = plt.subplots(figsize=(12, 6)); ax.axis('off'); fig.patch.set_facecolor('#1e1e1e')
    def fmt_val(val, suffix="B"): d = 1_000_000_000 if suffix == "B" else 1_000_000; return f"${val/d:+.2f} {suffix}"
    
    # Left: DEX
    plt.text(0.20, 0.70, "DELTA (DEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.20, 0.55, fmt_val(dex, "B"), color='#00ffff' if dex > 0 else '#ff00ff', fontsize=22, ha='center', weight='bold')
    plt.text(0.20, 0.45, "Net Notional", color='#888888', fontsize=10, ha='center')

    # Center: GEX
    plt.text(0.50, 0.70, "GAMMA (GEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.50, 0.55, fmt_val(gex, "B"), color='#00ff00' if gex > 0 else '#ff0000', fontsize=22, ha='center', weight='bold')
    plt.text(0.50, 0.45, "per 1% Move", color='#888888', fontsize=10, ha='center')

    # Right: VEX
    plt.text(0.80, 0.70, "VANNA (VEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.80, 0.55, fmt_val(vex, "M"), color='#ffff00' if vex > 0 else '#ff8800', fontsize=22, ha='center', weight='bold')
    plt.text(0.80, 0.45, "per 1% IV Chg", color='#888888', fontsize=10, ha='center')

    # Footer
    info_text = f"{ticker} @ {spot:.2f}  |  {scope_label}"
    if expiry_label: info_text += f"  |  EXP: {expiry_label}"
    plt.text(0.5, 0.10, info_text, color='white', fontsize=11, ha='center', style='italic')

    buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
    return buf

def generate_strike_chart(ticker, spot, data, metric="ALL", confluence_data=None):
    plt.style.use('dark_background')
    if metric == "CONFLUENCE":
        if not confluence_data: return None
        active_levels = [x for x in confluence_data if x['score'] > 0]
        if not active_levels: return None
        active_levels.sort(key=lambda x: x['strike'])
        c_strikes = [x['strike'] for x in active_levels]; c_scores = [x['score'] for x in active_levels]; c_tags = [x['tags'] for x in active_levels]
        fig, ax = plt.subplots(figsize=(10, 8)); fig.patch.set_facecolor('#1e1e1e'); ax.set_facecolor('#1e1e1e')
        colors = []
        for s in c_scores:
            if s == 4: colors.append('#FF00FF') # Quadruple
            elif s == 3: colors.append('#FFD700') # Triple
            elif s == 2: colors.append('#00FFFF') # Double
            else: colors.append('#555555')
        bars = ax.barh(c_strikes, c_scores, color=colors, height=(c_strikes[1]-c_strikes[0])*0.8 if len(c_strikes)>1 else 2, alpha=0.8)
        ax.axhline(spot, color='white', linestyle='--', linewidth=2, label=f'Spot: {spot:.2f}')
        for bar, tag, score in zip(bars, c_tags, c_scores):
            width = bar.get_width()
            ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, f" {tag}", va='center', color='white', fontsize=10, fontweight='bold')
        ax.set_xlabel("Confluence Score (D+G+V+C)", color='white'); ax.set_ylabel("Strike Price", color='white')
        ax.set_title(f"{ticker} Structural Confluence Profile", color='white', weight='bold', fontsize=14)
        ax.set_xlim(0, 5.5); ax.set_xticks([0, 1, 2, 3, 4]); ax.set_xticklabels(['', '1', '2', '3', '4 (Max)']); ax.grid(True, axis='x', alpha=0.15)
        legend_elements = [Patch(facecolor='#FF00FF', label='Quadruple (Max)'), Patch(facecolor='#FFD700', label='Triple Threat'), Patch(facecolor='#00FFFF', label='Double')]
        ax.legend(handles=legend_elements, loc='lower right')
        buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
        return buf

    strikes = np.array(data['strikes'])
    if metric == "ALL":
        plot_metrics = ["DEX", "GEX", "VEX", "CEX"] # UPDATED ORDER
        fig, axes = plt.subplots(4, 1, figsize=(12, 14), sharex=True) # 4 Panels
        plt.subplots_adjust(hspace=0.3)
    else:
        plot_metrics = [metric]
        fig, axes = plt.subplots(1, 1, figsize=(12, 6))
        axes = [axes] 
        
    fig.patch.set_facecolor('#1e1e1e')
    key_strikes = {}
    if confluence_data:
        for item in confluence_data:
            if item['score'] >= 3: key_strikes[item['strike']] = item['score']

    for i, m in enumerate(plot_metrics):
        ax = axes[i]; vals = np.array(data[m.lower()]); colors = ['#00ff00' if v >= 0 else '#ff0000' for v in vals]
        ax.bar(strikes, vals, color=colors, width=(strikes[1]-strikes[0])*0.8 if len(strikes)>1 else 1, alpha=0.8)
        if key_strikes:
            for strike_val in key_strikes:
                ax.axvline(strike_val, color='#FFD700', linestyle=':', alpha=0.4, linewidth=1)
                if i == 0: ax.text(strike_val, ax.get_ylim()[1]*0.95, "★", color='#FFD700', fontsize=12, ha='center')
        ax.axhline(0, color='white', linewidth=1.5)
        if len(vals) > 0:
            max_mag = max(abs(np.min(vals)), abs(np.max(vals))); ax.set_ylim(-max_mag*1.15, max_mag*1.15)
        ax.axvline(spot, color='white', linestyle='--', linewidth=2, label=f'Spot: {spot:.2f}')
        ax.set_ylabel(f"Net {m} ($)", color='white', fontsize=12); ax.set_title(f"Net Dealer {m} by Strike", color='white', weight='bold')
        ax.grid(True, alpha=0.15); ax.set_facecolor('#1e1e1e')
        def human_format(num, pos):
            magnitude = 0
            while abs(num) >= 1000: magnitude += 1; num /= 1000.0
            return '%.1f%s' % (num, ['', 'K', 'M', 'B'][magnitude])
        ax.yaxis.set_major_formatter(plt.FuncFormatter(human_format))

    plt.xlabel("Strike Price", color='white', fontsize=12); plt.suptitle(f"{ticker} Exposure Profile", color='white', fontsize=16, weight='bold', y=0.95)
    buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
    return buf

# --- DISCORD INIT ---
try: asyncio.get_running_loop()
except RuntimeError: asyncio.set_event_loop(asyncio.new_event_loop())
bot = discord.Bot(debug_guilds=None)

# --- COMMAND HELPERS ---
async def get_db_dates(ctx: discord.AutocompleteContext):
    ticker_input = ctx.options.get("ticker", "^SPX"); yf_sym = resolve_yf_symbol(ticker_input); db_ticker = get_options_ticker(yf_sym)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    c.execute("SELECT DISTINCT date(timestamp) FROM chain_snapshots WHERE ticker = ? ORDER BY timestamp DESC LIMIT 25", (db_ticker,))
    rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

async def get_db_tags(ctx: discord.AutocompleteContext):
    selected_date = ctx.options.get("replay_date") or ctx.options.get("snapshot_date")
    if not selected_date: return ["⬅️ Select a DATE first"]
    ticker_input = ctx.options.get("ticker", "^SPX"); yf_sym = resolve_yf_symbol(ticker_input); db_ticker = get_options_ticker(yf_sym)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    c.execute("SELECT DISTINCT tag FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ?", (db_ticker, selected_date))
    rows = c.fetchall(); conn.close(); tags = [r[0] for r in rows]
    return tags if tags else ["❌ No sessions found"]

# --- COMMANDS ---
beeks = bot.create_group("beeks", "Official Dukes Bros. Fixer")

@beeks.command(name="help", description="Learn how to use Beeks")
async def beeks_help(
    ctx: discord.ApplicationContext, 
    command: Option(str, description="Select a command for detailed manual", choices=["chain", "dailyrange", "dailylevels", "dom_flip", "dom_exposures", "dom_vig", "dom_skew", "setmode"], required=False)
):
    embed = discord.Embed(color=0xFFA500) # Duke & Duke Orange
    
    # --- GENERAL HELP (Default) ---
    if not command:
        embed.title = "🍊 Clarence Beeks: The Duke & Duke Fixer"
        embed.description = "**Institutional Market Structure & Volatility Analytics**\n\nI don't guess. I calculate. This terminal provides live option chain analysis, dealer positioning (GEX/DEX), and statistical volatility ranges to find the 'Structural Pins' in the market."
        
        # Explain the Engine (Data Logic)
        embed.add_field(
            name="⛽️ The Fuel (Data)", 
            value=(
                "**1. Live First:** I always attempt to fetch live data from the floor (Yahoo) first.\n"
                "**2. Daily Range only:** If the live feed is dark, I auto-load the last saved snapshot if one exists.\n"
                "**3. Time:** All analysis is strictly **New York Time (ET)**."
            ), 
            inline=False
        )
        
        # The Toolset Summary
        embed.add_field(
            name="🧰 The Toolset", 
            value=(
                "**/beeks dailyrange:** Intraday Volatility Bands as Standard Deviations.\n"
                "**/beeks dailylevels:** Dealer Exposure By Strike (DEX/GEX/VEX/CEX).\n"
                "**/beeks dom:** Deep Market Structure (exposures, flip, skew, vig).\n"
            ), 
            inline=False
        )
        embed.set_footer(text="Tip: Type '/beeks help [command]' for specific command descriptions.")
        await ctx.respond(embed=embed, ephemeral=True)
        return

    # --- SPECIFIC COMMAND HELP ---
    if command == "chain":
        embed.title="📘 Manual: The Chain"
        embed.description="**Raw Data Feed (DOM Style)**\nView the raw Option Chain data. Unlike Yahoo, we sort High Strikes at the top (Standard DOM view) to match your trading platform."
        embed.add_field(name="Features", value="**Backtesting:** Request a chain from any date in the DB.\n**The Greeks:** Full breakdown of Delta, Gamma, Theta, and IV per strike.", inline=False)
        
    elif command == "dailyrange":
        embed.title="📘 Manual: Daily Range"
        embed.description="**Intraday Volatility Bands**\nWe don't guess where price is going. We calculate where it *should* stop based on Energy and History."
        embed.add_field(name="The Levels", value="**🟦 Valentine:** Expected Range.\n**🟧 Winthorpe:** Historical Deviation.\n**⬜ Reference:** Todays Open (if Market 0pen) or previous close during after hours.", inline=False)
        embed.add_field(name="Resiliency", value="**Auto-Fallback:** If live data fails, this command will automatically load the last known good data from the DB.", inline=False)

    elif command == "dailylevels":
        embed.title="📘 Manual: Daily Levels"
        embed.description="**Intraday Structural Pins**\nVisualizes the net Dealer Exposure per strike. These are the walls the market must fight through."
        embed.add_field(name="Metrics", value="**DEX (Delta):** Directional Risk (The Walls).\n**GEX (Gamma):** Stability (The Magnets).\n**VEX (Vanna):** Volatility Sensitivity (The Gas Pedal).\n**CEX (Charm):** Time Decay (The 2:00pm Flush).", inline=False)
        embed.add_field(name="Confluence", value="Use `metric:CONFLUENCE` to find strikes where multiple Greeks overlap. These are the strongest levels on the board.", inline=False)

    elif command == "dom_exposures":
        embed.title="📘 Manual: Exposures"
        embed.description="**Total Dealer Positioning (The Big 3)**\nShows the Net GEX, DEX, and VEX for the entire market scope. Use this to determine the broader regime."
        embed.add_field(name="Interpretation", value="**Positive GEX:** Dampened Volatility (Buy the dip, Sell the rip).\n**Negative GEX:** Accelerated Volatility (Trend days).\n**Vanna (VEX):** If high, market moves aggressively when IV drops.", inline=False)

    elif command == "dom_flip":
        embed.title="📘 Manual: Gamma Flip"
        embed.description="**The Zero Gamma Level**\nThe theoretical price level where Dealers flip from Long Gamma (Stable) to Short Gamma (Volatile)."
        embed.add_field(name="Strategy", value="**Above Flip:** Market tends to be stable.\n**Below Flip:** Market tends to be volatile/directional.\n", inline=False)

    elif command == "dom_vig":
        embed.title="📘 Manual: The Vig"
        embed.description="**Expected Move (ATM Straddle)**\nCalculates the cost of the At-The-Money Straddle. This is the 'Vig' (fee) Market Makers are charging to play the game."
        embed.add_field(name="How To Use", value="**Cost:** This is the breakeven distance.\n**Breakevens:** The upper and lower bounds where the 'House' starts losing money.", inline=False)

    elif command == "dom_skew":
        embed.title="📘 Manual: Skew"
        embed.description="**Sentiment Detector (Put/Call Ratio)**\nCompares the cost of OTM Puts (Downside Protection) vs OTM Calls (Upside FOMO)."
        embed.add_field(name="Modes", value="**Intraday:** Checks 0DTE Skew (1% OTM). Immediate sentiment.\n**Macro:** Checks 30-Day Skew (5% OTM). Hedging sentiment.", inline=False)

    elif command == "setmode":
        embed.title="📘 Manual: Set Mode"
        embed.description="**Global Terminal View**\nConfigure how the bot delivers data to you."
        embed.add_field(name="Options", value="**Modern:** Generates visual charts and PNG dashboards. (Best for Desktop)\n**Bloomberg:** Returns raw text and ASCII tables. (Best for Mobile/Low Data)", inline=False)

    await ctx.respond(embed=embed, ephemeral=True)

@beeks.command(name="setmode", description="Configure Global Terminal View Settings")
async def beeks_setmode(ctx: discord.ApplicationContext, terminal: Option(str, choices=["Modern", "Bloomberg"], required=True)):
    set_user_terminal_setting(ctx.author.id, terminal.lower())
    await ctx.respond(f"🌎 **Global Terminal View** set to: **{terminal.upper()}**", ephemeral=True)

@beeks.command(name="dailyrange", description="Get Volatility Ranges")
async def beeks_dailyrange(ctx: discord.ApplicationContext, ticker: Option(str, required=True), engine: Option(str, choices=["Insider Info", "Market Floor"], default="Insider Info"), replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False)):
    await ctx.defer(ephemeral=True)
    raw_ticker = ticker.upper(); yf_sym = resolve_yf_symbol(raw_ticker)
    FUTURES_MAP = {"ES": "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F"}; is_future = raw_ticker in FUTURES_MAP; futures_ticker = FUTURES_MAP.get(raw_ticker)
    
    if replay_date:
        tag_to_use = session if session and "Select" not in session else get_latest_tag_for_date(get_options_ticker(yf_sym), replay_date)
        data = fetch_historical_data(yf_sym, replay_date, tag_to_use)
        if not data: await ctx.respond(f"❌ **Beeks:** 'File not found.'", ephemeral=True); return
        clean_footer = f"📼 REPLAY: {replay_date} [{tag_to_use}]"; movie_quote = random.choice(MOVIE_QUOTES)
    else:
        if not (yf_sym in ["^GSPC", "^NDX"] or is_future or (not yf_sym.startswith("^") and not yf_sym.endswith("=F"))): await ctx.respond(f"❌ **Beeks:** 'Incompatible Ticker.'", ephemeral=True); return
        data = fetch_market_data(yf_sym)
        if not data: await ctx.respond(f"❌ **Beeks:** 'Data corrupted.'", ephemeral=True); return
        movie_quote = random.choice(MOVIE_QUOTES)
        ny_tz = pytz.timezone('America/New_York'); ny_now = datetime.datetime.now(ny_tz)
        data_date_ny = data['date_obj'].astimezone(ny_tz).date() if data.get('date_obj') else ny_now.date()
        anchor_label = "OPEN" if data_date_ny == ny_now.date() else "PREV CLOSE"
        clean_footer = f"NY TIME: {ny_now.strftime('%H:%M')} ET | REF: {anchor_label}"
        if 'saved_at' in data: clean_footer = f"⚠️ ARCHIVE DATA | {data['saved_at']}"

    levels = calculate_levels(data['anchor_price'], data['iv'], data['hv'], engine)
    display_price = data['anchor_price']
    if is_future and not replay_date:
        try:
            ft = yf.Ticker(futures_ticker); spx = yf.Ticker(yf_sym); f_hist = ft.history(period="5d"); s_hist = spx.history(period="5d")
            if len(f_hist) >= 2 and len(s_hist) >= 2:
                offset = f_hist['Close'].iloc[-2] - s_hist['Close'].iloc[-2]; display_price += offset
                for key in levels['valentine']: levels['valentine'][key] += offset
                for key in levels['winthorpe']: levels['winthorpe'][key] += offset
                clean_footer += " | OFFSET IN USE"
        except: pass
        
    chart_data = data.copy(); chart_data['anchor_price'] = display_price
    view_setting = get_user_terminal_setting(ctx.author.id) 
    if view_setting == "bloomberg":
        msg = f"> **{movie_quote}**\n\n**{raw_ticker} VOLATILITY REPORT ({engine})**\n\n`{'RANGE':<12} | {'VALUE':<12} | {'MOVE':<12} | {'TYPE':<12}\n" + "-"*56 + "\n"
        msg += f"{'IV':<12} | {data['iv']:<12.4f} | {levels['meta']['daily_move_iv']:<12.2f} | {'Implied'}\n{'HV':<12} | {data['hv']:<12.4f} | {levels['meta']['daily_move_hv']:<12.2f} | {'Historical'}\n`\n\n**{raw_ticker} LEVELS**\n`{'LEVEL':<12} | {'VALENTINE':<15} | {'WINTHORPE':<15}\n" + "-"*48 + "\n"
        for i in range(4, 0, -1): msg += f"+{i}σ          | {levels['valentine'][f'+{i}σ']:<15.2f} | {levels['winthorpe'][f'+{i}σ']:<15.2f}\n" 
        msg += f"REF          | {display_price:.2f}\n" 
        for i in range(1, 5): msg += f"-{i}σ          | {levels['valentine'][f'-{i}σ']:<15.2f} | {levels['winthorpe'][f'-{i}σ']:<15.2f}\n" 
        msg += f"`\n*{clean_footer}*"
        await ctx.respond(msg, ephemeral=True)
    else: 
        buf = create_beeks_chart(raw_ticker, chart_data, levels, engine)
        file = discord.File(buf, filename="beeks_report.png")
        embed = discord.Embed(description=f"**{movie_quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_report.png"); embed.set_footer(text=clean_footer)
        await ctx.respond(embed=embed, file=file, ephemeral=True)

@beeks.command(name="dailylevels", description="Visualize Net Exposure per Strike (DEX/GEX/VEX/CEX)")
async def beeks_dailylevels(
    ctx: discord.ApplicationContext, ticker: Option(str, required=True), scope: Option(str, choices=["Front Month", "Total Market", "0DTE"], required=True),
    metric: Option(str, choices=["CONFLUENCE", "ALL", "DEX", "GEX", "VEX", "CEX"], default="ALL"), target_expiry: Option(str, required=False),
    replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
    calc_date = None; calc_tag = session; market_time = datetime.datetime.now(ZoneInfo("America/New_York"))
    if target_expiry: target_date = target_expiry; calc_date = replay_date if replay_date else None
    elif scope == "0DTE" and not replay_date:
        target_date = market_time.strftime("%Y-%m-%d") if market_time.weekday() < 4 and market_time.hour < 16 else get_next_market_date(market_time)
        conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT date(timestamp) FROM chain_snapshots WHERE ticker = ? AND tag = 'CLOSE' ORDER BY timestamp DESC LIMIT 1", (display_ticker,)); row = c.fetchone(); conn.close()
        if row: calc_date = row[0]; calc_tag = "CLOSE"
    elif replay_date: calc_date = replay_date; target_date = replay_date
    else: target_date = None

    await ctx.interaction.edit_original_response(content=f"⏳ **Beeks:** 'Mapping {metric}...'")
    raw_data = fetch_and_enrich_chain(ticker=ticker, expiry_date=target_date, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope, range_count=9999)
    if not raw_data: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'Live Data Feed Is Currently Dark. Can you Try a Replay Date?'"); return
    
    spot_price = raw_data[0]['spot']; strike_data = calculate_strike_exposures(raw_data, spot_price, display_ticker)
    
    def get_top(vals): return {x[0] for x in sorted([p for p in zip(strike_data['strikes'], vals) if abs(p[1])>0], key=lambda x: abs(x[1]), reverse=True)[:5]}
    all_sig = get_top(strike_data['gex']) | get_top(strike_data['dex']) | get_top(strike_data['vex']) | get_top(strike_data['cex'])
    confluence_map = []
    for k in all_sig:
        score = (1 if k in get_top(strike_data['gex']) else 0) + (1 if k in get_top(strike_data['dex']) else 0) + (1 if k in get_top(strike_data['vex']) else 0) + (1 if k in get_top(strike_data['cex']) else 0)
        tags = ("D" if k in get_top(strike_data['dex']) else "") + ("G" if k in get_top(strike_data['gex']) else "") + ("V" if k in get_top(strike_data['vex']) else "") + ("C" if k in get_top(strike_data['cex']) else "")
        confluence_map.append({'strike': k, 'score': score, 'tags': tags})
    confluence_map.sort(key=lambda x: (x['score'], x['strike']), reverse=True)

    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
    if view_setting == 'modern':
        img_buf = generate_strike_chart(display_ticker, spot_price, strike_data, metric, confluence_map)
        file = discord.File(img_buf, filename="strikes.png")
        desc = f"**{quote}**\n" + (f"\n**🎯 CONFLUENCE (Top 5)**\n" + "\n".join([f"`{int(i['strike']):<5}` {'⭐'*i['score']} ({i['tags']})" for i in confluence_map[:5]]) if metric == "CONFLUENCE" else "")
        embed = discord.Embed(color=0x2b2d31, description=desc); embed.set_image(url="attachment://strikes.png"); embed.set_footer(text=f"Spot: {spot_price:.2f} | {scope}")
        await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
    else:
        lines = [f"> **{quote}**"]
        if metric == "CONFLUENCE":
            lines += ["```yaml", "+---------------------------------------+", "| KEY LEVELS (HIGHEST OVERLAP)          |", "+---------------------------------------+"]
            for i in confluence_map: lines.append(f"| {int(i['strike']):<7} | {'*'*i['score']:<4} | {i['tags']:<5} |")
            lines.append("```")
        await ctx.interaction.edit_original_response(content="\n".join(lines))

@beeks.command(name="chain", description="View Raw Chain")
async def beeks_chain(ctx: discord.ApplicationContext, ticker: Option(str, required=True), expiry: Option(str, required=False), center: Option(float, required=False), rows: Option(int, choices=[1, 3, 5, 10], default=10), replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False)):
    await ctx.defer(ephemeral=True)
    yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym); actual_rows = 11 if rows == 10 else rows
    calc_date = replay_date if replay_date else None; calc_tag = session; 
    if calc_date and not calc_tag: calc_tag = get_latest_tag_for_date(display_ticker, calc_date)
    data = fetch_and_enrich_chain(ticker=ticker, expiry_date=expiry, snapshot_date=calc_date, snapshot_tag=calc_tag, scope="Specific" if expiry else "Front Month", range_count=actual_rows, pivot=center)
    if not data: await ctx.respond(f"❌ **Beeks:** 'Feed Dark. Try a snapshot: `/beeks db catalog`'", ephemeral=True); return
    spot = data[0]['spot']; target_date = expiry if expiry else "FRONT MONTH"; view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES); source_label = f"DB: {calc_date} [{calc_tag}]" if calc_date else "LIVE"
    closest_strike = min([d['strike'] for d in data], key=lambda x: abs(x - spot))
    table_rows = []; grouped = {}
    for row in data:
        k = row['strike']
        if k not in grouped: grouped[k] = {'C': None, 'P': None}
        if row['type'].lower().startswith('c'): grouped[k]['C'] = row
        else: grouped[k]['P'] = row
    sorted_strikes = sorted(grouped.keys(), reverse=True)
    for k in sorted_strikes:
        c = grouped[k]['C']; p = grouped[k]['P']
        c_iv = c.get('iv', 0) if c else 0; c_delta = c.get('delta', 0) if c else 0; c_gamma = c.get('gamma', 0) if c else 0; c_theta = c.get('theta', 0) if c else 0; c_vol = c.get('volume', 0) if c else 0; c_oi = c.get('oi', 0) if c else 0
        p_iv = p.get('iv', 0) if p else 0; p_delta = p.get('delta', 0) if p else 0; p_gamma = p.get('gamma', 0) if p else 0; p_theta = p.get('theta', 0) if p else 0; p_vol = p.get('volume', 0) if p else 0; p_oi = p.get('oi', 0) if p else 0
        table_rows.append({'strike': k, 'c_iv': c_iv, 'c_delta': c_delta, 'c_gamma': c_gamma, 'c_theta': c_theta, 'c_vol': c_vol, 'c_oi': c_oi, 'p_iv': p_iv, 'p_delta': p_delta, 'p_gamma': p_gamma, 'p_theta': p_theta, 'p_vol': p_vol, 'p_oi': p_oi})
    if view_setting == 'modern':
        plt.figure(figsize=(16, len(table_rows) * 0.5 + 3)); plt.style.use('dark_background'); ax = plt.gca(); ax.axis('off')
        cols = ['VOL', 'OI', 'IV', 'THETA', 'GAMMA', 'DELTA', 'STRIKE', 'DELTA', 'GAMMA', 'THETA', 'IV', 'OI', 'VOL']; cell_text = []; cell_colors = []
        for row in table_rows:
            c_bg = '#003300' if row['strike'] < spot else '#222222'; p_bg = '#330000' if row['strike'] > spot else '#222222'; s_bg = '#AA8800' if row['strike'] == closest_strike else '#444444' 
            r_colors = [c_bg]*6 + [s_bg] + [p_bg]*6
            r_data = [f"{int(row['c_vol'])}", f"{int(row['c_oi'])}", f"{row['c_iv']:.1%}", f"{row['c_theta']:.2f}", f"{row['c_gamma']:.3f}", f"{row['c_delta']:.2f}", f"{row['strike']:.0f}", f"{row['p_delta']:.2f}", f"{row['p_gamma']:.3f}", f"{row['p_theta']:.2f}", f"{row['p_iv']:.1%}", f"{int(row['p_oi'])}", f"{int(row['p_vol'])}"]
            cell_text.append(r_data); cell_colors.append(r_colors)
        table = plt.table(cellText=cell_text, colLabels=cols, cellColours=cell_colors, loc='center', cellLoc='center')
        table.auto_set_font_size(False); table.set_fontsize(9); table.scale(1, 1.8) 
        for (i, j), cell in table.get_celld().items():
            if i == 0: cell.set_text_props(weight='bold', color='white'); cell.set_facecolor('#111111'); cell.set_edgecolor('white'); cell.set_linewidth(1)
            else: cell.set_edgecolor('#555555'); cell.set_linewidth(0.5)
        plt.title(f"{display_ticker} CHAIN  |  EXPIRY: {target_date}  |  SPOT: {spot:.2f}\nFEED: {source_label}", color='white', pad=20, fontsize=14, weight='bold')
        buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e', dpi=120); buf.seek(0); plt.close()
        file = discord.File(buf, filename="chain_modern.png"); embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://chain_modern.png"); await ctx.respond(embed=embed, file=file, ephemeral=True)
    else:
        def fmt_5(val): s = f"{int(val)/1000:.0f}k" if int(val) >= 1000 else str(int(val)); return f"{s:>5}"
        lines = [f"> **{quote}**", f"```yaml", f"TICKER: {display_ticker}  SPOT: {spot:.2f}", f"EXP: {target_date}  SRC: {source_label}", f"-"*74, f"|{'CALLS':^32}|{'':^6}|{'PUTS':^32}|", f"|{'V':>5}{'OI':>5}{'IV':>4}{'TH':>6}{'GM':>6}{'DL':>6}|{'STRK':^6}|{'DL':>6}{'GM':>6}{'TH':>6}{'IV':>4}{'OI':>5}{'V':>5}|", f"-"*74]
        for row in table_rows:
            s_str = f"{row['strike']:.0f}"; strike_display = f">{s_str}<" if row['strike'] == closest_strike else s_str
            line = (f"|{fmt_5(row['c_vol'])}{fmt_5(row['c_oi'])}{row['c_iv']:>4.0%}{row['c_theta']:>6.2f}{row['c_gamma']:>6.3f}{row['c_delta']:>6.2f}|{strike_display:^6}|{row['p_delta']:>6.2f}{row['p_gamma']:>6.3f}{row['p_theta']:>6.2f}{row['p_iv']:>4.0%}{fmt_5(row['p_oi'])}{fmt_5(row['p_vol'])}|")
            lines.append(line)
        lines.append(f"-"*74); lines.append(f"```"); final_text = "\n".join(lines); await ctx.respond(final_text[:1950] + ("\n..." if len(final_text)>1950 else ""), ephemeral=True)

# --- DOM SUITE ---
dom_group = beeks.create_subgroup("dom", "Dealer Open Market (Structure & Positioning)")

@dom_group.command(name="flip", description="Find the Zero Gamma Level")
async def dom_flip(ctx: discord.ApplicationContext, ticker: Option(str, required=True), scope: Option(str, choices=["Front Month", "Total Market", "0DTE"], default="Front Month"), replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False), target_expiry: Option(str, required=False)):
    await ctx.defer(ephemeral=True)
    yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
    calc_date = None; calc_tag = session; market_time = datetime.datetime.now(ZoneInfo("America/New_York"))
    if target_expiry: target_date = target_expiry; status_msg = f"⏳ **Beeks:** 'Targeting **{target_date}**...'"
    elif scope == "0DTE" and not replay_date:
        target_date = market_time.strftime("%Y-%m-%d") if market_time.weekday() < 4 and market_time.hour < 16 else get_next_market_date(market_time)
        conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT date(timestamp) FROM chain_snapshots WHERE ticker = ? AND tag = 'CLOSE' ORDER BY timestamp DESC LIMIT 1", (display_ticker,)); row = c.fetchone(); conn.close()
        if not row: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'No recent CLOSE snapshot for 0DTE.'"); return
        calc_date = row[0]; calc_tag = "CLOSE"; status_msg = f"⏳ **Beeks:** 'Loading 0DTE from {calc_date} [CLOSE]...'"
    elif replay_date: calc_date = replay_date; status_msg = f"⏳ **Beeks:** 'Simulation on {replay_date}...'"; target_date = replay_date 
    else: status_msg = f"⏳ **Beeks:** 'Scanning {scope}...'"; target_date = None
    await ctx.interaction.edit_original_response(content=status_msg)
    raw_data = fetch_and_enrich_chain(ticker=ticker, expiry_date=target_date, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope)
    if not raw_data: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'Feed Dark. Try a snapshot: `/beeks db catalog`'"); return
    spot_price = raw_data[0].get('spot') or yf.Ticker(yf_sym).history(period="1d")['Close'].iloc[-1]; flip_price, plot_data = calculate_gamma_flip(raw_data, spot_price)
    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
    if view_setting == 'modern' and plot_data:
        sim_spots, net_gammas = plot_data
        plt.figure(figsize=(10, 6)); plt.style.use('dark_background'); plt.plot(sim_spots, net_gammas, color='cyan', linewidth=2); plt.fill_between(sim_spots, net_gammas, 0, where=(net_gammas >= 0), color='green', alpha=0.3); plt.fill_between(sim_spots, net_gammas, 0, where=(net_gammas < 0), color='red', alpha=0.3); plt.axhline(0, color='white', linestyle='--', linewidth=1); plt.axvline(spot_price, color='yellow', linestyle=':', label='Spot')
        if flip_price: plt.axvline(flip_price, color='magenta', linewidth=2, label='Flip'); plt.text(flip_price, max(net_gammas)*0.1, f"{flip_price:.0f}", color='magenta', fontweight='bold')
        plt.title(f"Gamma Profile: {display_ticker}", color='white', fontweight='bold'); plt.ylabel("Net Gamma ($)", color='white'); plt.grid(True, alpha=0.2); buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
        file = discord.File(buf, filename="gamma_flip.png"); embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://gamma_flip.png"); await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
    else:
        res_str = f"**{quote}**\n\n**GAMMA FLIP REPORT: {display_ticker}**\nSpot Price: {spot_price:.2f}\n" + ("-" * 30) + "\n"
        if flip_price: res_str += f"**FLIP LEVEL: {flip_price:.2f}**\nDistance: {flip_price - spot_price:+.2f}\nRegime: {'BULLISH' if spot_price > flip_price else 'BEARISH'}\n"
        else: res_str += "⚠️ **NO FLIP DETECTED**\n"
        await ctx.interaction.edit_original_response(content=res_str)

@dom_group.command(name="vig", description="The Expected Move (ATM Straddle)")
async def dom_vig(ctx: discord.ApplicationContext, ticker: Option(str, required=True), expiry: Option(str, description="YYYY-MM-DD (Optional)", required=False), replay_date: Option(str, description="Backtest Date", autocomplete=get_db_dates, required=False), session: Option(str, description="Session Tag", autocomplete=get_db_tags, required=False)):
    await ctx.defer(ephemeral=True)
    calc_date = replay_date if replay_date else None; calc_tag = session
    if calc_date and not calc_tag: yf_sym = resolve_yf_symbol(ticker); db_ticker = get_options_ticker(yf_sym); calc_tag = get_latest_tag_for_date(db_ticker, calc_date)
    scope = "Specific" if expiry else "0DTE"
    data = fetch_and_enrich_chain(ticker=ticker, expiry_date=expiry, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope, range_count=9999)
    if not data: await ctx.respond(f"❌ **Beeks:** 'Feed Dark. Try a snapshot: `/beeks db catalog`'", ephemeral=True); return
    spot = data[0]['spot']; df = pd.DataFrame(data); unique_strikes = np.array(sorted(df['strike'].unique())); atm_strike = unique_strikes[np.abs(unique_strikes - spot).argmin()]
    atm_opts = df[df['strike'] == atm_strike]; call = atm_opts[atm_opts['type'].str.lower() == 'call']; put = atm_opts[atm_opts['type'].str.lower() == 'put']
    if call.empty or put.empty: await ctx.respond(f"❌ **Beeks:** 'ATM Straddle incomplete for {atm_strike}.'", ephemeral=True); return
    c_price = (call.iloc[0]['bid'] + call.iloc[0]['ask']) / 2; c_price = call.iloc[0]['price'] if c_price == 0 else c_price
    p_price = (put.iloc[0]['bid'] + put.iloc[0]['ask']) / 2; p_price = put.iloc[0]['price'] if p_price == 0 else p_price
    vig = c_price + p_price; breakeven_up = atm_strike + vig; breakeven_down = atm_strike - vig
    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES); source = f"DB: {calc_date} [{calc_tag}]" if calc_date else "LIVE"
    if view_setting == "modern":
        plt.figure(figsize=(6, 9)); plt.style.use('dark_background'); ax = plt.gca(); ax.axis('off')
        margin = vig * 1.4; y_min = breakeven_down - margin; y_max = breakeven_up + margin; plt.ylim(y_min, y_max); plt.xlim(0, 10) 
        header_y = y_max - (margin * 0.1); plt.text(5, header_y, "ATM EXPECTED MOVE", color='#888888', ha='center', fontsize=11, weight='bold'); plt.text(5, header_y - (margin * 0.15), f"${vig:.2f}", color='white', ha='center', fontsize=32, weight='bold')
        rect_border = Rectangle((3.5, breakeven_down), 3, (breakeven_up - breakeven_down), linewidth=2, edgecolor='#00e5ff', facecolor='none', zorder=2); ax.add_patch(rect_border)
        rect_fill = Rectangle((3.5, breakeven_down), 3, (breakeven_up - breakeven_down), linewidth=0, facecolor='#00e5ff', alpha=0.15, zorder=1); ax.add_patch(rect_fill)
        plt.hlines(atm_strike, 3.5, 6.5, color='#00e5ff', linestyle=':', linewidth=1, alpha=0.5)
        plt.text(7, breakeven_up, f"{breakeven_up:.2f}", color='#00e5ff', va='center', ha='left', fontsize=12, weight='bold'); plt.text(3, breakeven_up, "UPPER", color='#00e5ff', va='center', ha='right', fontsize=9)
        plt.text(7, breakeven_down, f"{breakeven_down:.2f}", color='#00e5ff', va='center', ha='left', fontsize=12, weight='bold'); plt.text(3, breakeven_down, "LOWER", color='#00e5ff', va='center', ha='right', fontsize=9)
        c_spot = '#00ff00' if spot > breakeven_up else '#ff0000' if spot < breakeven_down else 'white'
        plt.hlines(spot, 2.5, 7.5, color=c_spot, linewidth=2, zorder=3); plt.plot(2.5, spot, marker='D', markersize=8, color=c_spot, zorder=4); plt.text(2, spot, f"{spot:.2f}", color=c_spot, ha='right', va='center', fontsize=14, weight='bold')
        plt.text(5, y_min, f"{ticker.upper()} | {source}", color='#444444', ha='center', fontsize=9)
        buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close(); file = discord.File(buf, filename="vig_holo.png"); embed = discord.Embed(description=f"**{quote}**", color=0x00e5ff); embed.set_image(url="attachment://vig_holo.png"); await ctx.respond(embed=embed, file=file, ephemeral=True)
    else:
        msg = f"> **{quote}**\n**{ticker.upper()} THE VIG (ATM STRADDLE)**\nSource: `{source}`\nSpot: `{spot:.2f}` | ATM: `{atm_strike}`\n```yaml\nCOST (EXPECTED MOVE): ${vig:.2f}\n--------------------------------\nUPPER BREAKEVEN:      {breakeven_up:.2f}\nLOWER BREAKEVEN:      {breakeven_down:.2f}\n```"
        await ctx.respond(msg, ephemeral=True)

@dom_group.command(name="skew", description="Put/Call Sentiment Ratio")
async def dom_skew(ctx: discord.ApplicationContext, ticker: Option(str, required=True), mode: Option(str, choices=["Intraday", "Macro"], default="Intraday", description="Intraday (0DTE/1%) or Macro (30D/5%)"), expiry: Option(str, description="Override Date (YYYY-MM-DD)", required=False), replay_date: Option(str, description="Backtest Date", autocomplete=get_db_dates, required=False), session: Option(str, description="Session Tag", autocomplete=get_db_tags, required=False)):
    await ctx.defer(ephemeral=True)
    if mode == "Intraday": scope_req = "0DTE"; otm_dist = 0.01; dist_label = "1%"
    else: scope_req = "Front Month"; otm_dist = 0.05; dist_label = "5%"
    if expiry: scope_req = "Specific"
    calc_date = replay_date if replay_date else None; calc_tag = session
    if calc_date and not calc_tag: yf_sym = resolve_yf_symbol(ticker); db_ticker = get_options_ticker(yf_sym); calc_tag = get_latest_tag_for_date(db_ticker, calc_date)
    data = fetch_and_enrich_chain(ticker=ticker, expiry_date=expiry, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope_req, range_count=9999)
    if not data: await ctx.respond(f"❌ **Beeks:** 'Feed Dark. Try a snapshot: `/beeks db catalog`'", ephemeral=True); return
    spot = data[0]['spot']; df = pd.DataFrame(data)
    if expiry: skew_chain = df; target_time = df['time_year'].iloc[0]
    elif mode == "Intraday": unique_times = sorted(df['time_year'].unique()); target_time = unique_times[0]; skew_chain = df[df['time_year'] == target_time]
    else: unique_times = sorted(df['time_year'].unique()); target_time = min(unique_times, key=lambda x: abs(x - 0.082)); skew_chain = df[df['time_year'] == target_time]
    put_strike_target = spot * (1 - otm_dist); call_strike_target = spot * (1 + otm_dist)
    puts = skew_chain[skew_chain['type'].str.lower() == 'put']; calls = skew_chain[skew_chain['type'].str.lower() == 'call']
    if puts.empty or calls.empty: await ctx.respond(f"❌ **Beeks:** 'Chain too thin for Skew.'", ephemeral=True); return
    put_row = puts.iloc[np.abs(puts['strike'] - put_strike_target).argmin()]; call_row = calls.iloc[np.abs(calls['strike'] - call_strike_target).argmin()]
    put_iv = put_row['iv'] * 100; call_iv = call_row['iv'] * 100; ratio = (put_iv / call_iv) if call_iv != 0 else 0
    sentiment = "BEARISH (HEDGING)" if ratio > 1.2 else "BULLISH (FOMO)" if ratio < 0.8 else "NEUTRAL"
    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES); source = f"DB: {calc_date} [{calc_tag}]" if calc_date else "LIVE"; dte_days = int(target_time * 365); expiry_str = f"{dte_days} DTE" if dte_days > 0 else "EXPIRES TODAY"
    if view_setting == "modern":
        plt.figure(figsize=(10, 5)); plt.style.use('dark_background'); ax = plt.gca(); ax.axis('off')
        c_sent = '#ff5555' if ratio > 1.2 else '#55ff55' if ratio < 0.8 else '#ffff55'
        plt.text(0.5, 0.85, f"VOLATILITY SKEW ({mode.upper()})", color='white', fontsize=16, weight='bold', ha='center'); plt.text(0.5, 0.77, f"{expiry_str}", color='#888888', fontsize=10, weight='bold', ha='center')
        plt.text(0.5, 0.55, f"{ratio:.2f}", color=c_sent, fontsize=36, weight='bold', ha='center'); plt.text(0.5, 0.45, sentiment, color=c_sent, fontsize=12, weight='bold', ha='center', bbox=dict(facecolor='#222222', edgecolor=c_sent, pad=5))
        plt.text(0.20, 0.30, f"PUTS (-{dist_label})", color='#ff99cc', fontsize=10, ha='center'); plt.text(0.20, 0.15, f"{put_iv:.1f}%", color='white', fontsize=16, weight='bold', ha='center')
        plt.text(0.80, 0.30, f"CALLS (+{dist_label})", color='#99ccff', fontsize=10, ha='center'); plt.text(0.80, 0.15, f"{call_iv:.1f}%", color='white', fontsize=16, weight='bold', ha='center')
        plt.text(0.5, 0.05, f"{ticker.upper()} @ {spot:.2f} | {source}", color='#666666', fontsize=9, ha='center')
        buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close(); file = discord.File(buf, filename="skew.png"); embed = discord.Embed(description=f"**{quote}**", color=0xFF99CC); embed.set_image(url="attachment://skew.png"); await ctx.respond(embed=embed, file=file, ephemeral=True)
    else:
        msg = f"**{ticker.upper()} SKEW ({mode.upper()})**\nSource: `{source}` | {expiry_str}\n```yaml\nPUT IV  (-{dist_label}): {put_iv:.1f}%\nCALL IV (+{dist_label}): {call_iv:.1f}%\nRATIO:         {ratio:.2f}\nSENTIMENT:     {sentiment}\n```"
        await ctx.respond(msg, ephemeral=True)

@dom_group.command(name="exposures", description="Total Dealer Exposure (GEX/DEX/VEX)")
async def dom_exposures(ctx: discord.ApplicationContext, ticker: Option(str, required=True), scope: Option(str, choices=["Front Month", "Total Market", "0DTE"], default="Front Month"), replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False), target_expiry: Option(str, required=False)):
    await ctx.defer(ephemeral=True)
    yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
    calc_date = None; calc_tag = session; market_time = datetime.datetime.now(ZoneInfo("America/New_York"))
    if target_expiry: target_date = target_expiry; status_msg = f"⏳ **Beeks:** 'Calculating **{target_date}**...'"
    elif scope == "0DTE" and not replay_date:
        target_date = market_time.strftime("%Y-%m-%d") if market_time.weekday() < 4 and market_time.hour < 16 else get_next_market_date(market_time)
        conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT date(timestamp) FROM chain_snapshots WHERE ticker = ? AND tag = 'CLOSE' ORDER BY timestamp DESC LIMIT 1", (display_ticker,)); row = c.fetchone(); conn.close()
        if not row: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'No recent CLOSE snapshot.'"); return
        calc_date = row[0]; calc_tag = "CLOSE"; status_msg = f"⏳ **Beeks:** 'Loading 0DTE from {calc_date}...'"
    elif replay_date: calc_date = replay_date; status_msg = f"⏳ **Beeks:** 'Simulation on {replay_date}...'"; target_date = replay_date 
    else: status_msg = f"⏳ **Beeks:** 'Scanning {scope}...'"; target_date = None
    await ctx.interaction.edit_original_response(content=status_msg)
    raw_data = fetch_and_enrich_chain(ticker=ticker, expiry_date=target_date, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope, range_count=9999)
    if not raw_data: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'Feed Dark. Try a snapshot: `/beeks db catalog`'"); return
    spot_price = raw_data[0]['spot']; gex, dex, vex = calculate_market_exposures(raw_data, spot_price)
    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
    if view_setting == 'modern':
        img_buf = generate_exposure_dashboard(display_ticker, spot_price, gex, dex, vex, scope, target_date)
        file = discord.File(img_buf, filename="exposures.png"); embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://exposures.png"); await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
    else:
        def fmt(v, s="B"): d=1_000_000_000 if s=="B" else 1_000_000; return f"${v/d:>7.2f} {s}"
        msg = f"> **{quote}**\n```yaml\n+--------------------------------------------------+\n| CLARENCE BEEKS TERMINAL           [EXPOSURE]     |\n+--------------------------------------------------+\n| TICKER: {display_ticker:<16} SPOT: {spot_price:<15.2f} |\n| SCOPE:  {scope:<16} DATE: {target_date if target_date else 'LIVE':<15} |\n+--------------------------------------------------+\n| DEX (DELTA)   : {fmt(dex, 'B'):<12} Net Notional   |\n| GEX (GAMMA)   : {fmt(gex, 'B'):<12} / 1% Move      |\n| VEX (VANNA)   : {fmt(vex, 'M'):<12} / 1% IV Change |\n+--------------------------------------------------+\n| REGIME: {'DAMPENED VOL' if gex > 0 else 'ACCELERATED VOL':<32} |\n+--------------------------------------------------+\n```"
        await ctx.interaction.edit_original_response(content=msg)

# --- DB ADMIN SUITE ---
db_group = beeks.create_subgroup("db", "Database Administration Tools")

@db_group.command(name="catalog", description="List all tickers and file counts")
@commands.has_permissions(administrator=True)
async def db_catalog(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT ticker, COUNT(*) FROM chain_snapshots GROUP BY ticker ORDER BY COUNT(*) DESC"); rows = c.fetchall(); conn.close()
    if not rows: await ctx.respond("📭 **Database is Empty.**", ephemeral=True); return
    msg = "**🗄️ DATABASE CATALOG**\n```yaml\n" + f"{'TICKER':<10} | {'SNAPSHOTS':<10}\n" + "-"*25 + "\n"
    for r in rows: msg += f"{r[0]:<10} | {r[1]:<10}\n"
    msg += "```"; await ctx.respond(msg, ephemeral=True)

@db_group.command(name="inspect", description="View Snapshot Tree for a Ticker")
@commands.has_permissions(administrator=True)
async def db_inspect(ctx: discord.ApplicationContext, ticker: Option(str, required=True)):
    await ctx.defer(ephemeral=True); yf_sym = resolve_yf_symbol(ticker); db_ticker = get_options_ticker(yf_sym)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT date(timestamp), tag, time(timestamp) FROM chain_snapshots WHERE ticker = ? ORDER BY timestamp DESC", (db_ticker,)); rows = c.fetchall(); conn.close()
    if not rows: await ctx.respond(f"❌ **Beeks:** 'No records for **{db_ticker}**.'", ephemeral=True); return
    tree = {}
    for r in rows:
        d, tag, t = r
        if d not in tree: tree[d] = []
        tree[d].append(f"{tag} ({t})")
    lines = [f"**📂 MANIFEST: {db_ticker}**", "```yaml"]
    for date_key in sorted(tree.keys(), reverse=True):
        lines.append(f"{date_key}"); [lines.append(f"  {'└─' if i == len(tree[date_key]) - 1 else '├─'} {entry}") for i, entry in enumerate(tree[date_key])]
    lines.append("```"); final_msg = "\n".join(lines); await ctx.respond(final_msg[:1900] + ("\n..." if len(final_msg)>1900 else ""), ephemeral=True)

@db_group.command(name="snapshot", description="Force Manual Snapshot (Live Data)")
@commands.has_permissions(administrator=True)
async def db_snapshot(ctx: discord.ApplicationContext, ticker: Option(str, required=True), session: Option(str, required=True), force_date: Option(str, required=False)):
    await ctx.defer(ephemeral=True); yf_sym = get_options_ticker(resolve_yf_symbol(ticker))
    try:
        tkr = yf.Ticker(yf_sym); hist = tkr.history(period="1d")
        if hist.empty and not tkr.options: await ctx.respond(f"❌ **Beeks:** 'Never heard of **{ticker.upper()}**.'", ephemeral=True); return
        if not validate_atm_data(tkr, hist['Close'].iloc[-1]): await ctx.respond(f"❌ **Beeks:** 'Bad Exchange Data.'", ephemeral=True); return
        start_time = time.time(); exps = tkr.options; div_yield = get_current_yield(yf_sym)
        ts_str = f"{force_date} 23:59:59" if force_date else datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
        full_chain = {"symbol": yf_sym, "timestamp": ts_str, "expirations": {}}
        for e in exps:
            try:
                opt = tkr.option_chain(e); full_chain["expirations"][e] = {"calls": opt.calls.to_dict(orient='records'), "puts": opt.puts.to_dict(orient='records')}
            except: pass
        if save_snapshot(yf_sym, full_chain, hist['Close'].iloc[-1], div_yield, tag=session.upper(), custom_timestamp=ts_str):
            await ctx.respond(f"✅ **Asset Secured.**\n**{yf_sym}** @ {hist['Close'].iloc[-1]:.2f}\n🏷️ Session: `{session.upper()}`\n⏱️ {time.time() - start_time:.2f}s", ephemeral=True)
        else: await ctx.respond(f"❌ **Beeks:** 'Duplicate Tag.'", ephemeral=True)
    except Exception as e: await ctx.respond(f"⚠️ **Beeks:** 'Snapshot failed: {e}'", ephemeral=True)

@db_group.command(name="delete", description="Delete a single snapshot file")
@commands.has_permissions(administrator=True)
async def db_delete(ctx: discord.ApplicationContext, ticker: str, replay_date: Option(str, autocomplete=get_db_dates), session: Option(str, autocomplete=get_db_tags)):
    await ctx.defer(ephemeral=True); yf_sym = get_options_ticker(resolve_yf_symbol(ticker))
    if delete_snapshot_from_db(yf_sym, replay_date, session): await ctx.respond(f"🗑️ **Beeks:** 'Shredded file for **{yf_sym}** on **{replay_date}** [{session}].'", ephemeral=True)
    else: await ctx.respond(f"❌ **Beeks:** 'File not found.'", ephemeral=True)

@db_group.command(name="purge", description="⚠️ NUKE ALL data for a specific ticker")
@commands.has_permissions(administrator=True)
async def db_purge(ctx: discord.ApplicationContext, ticker: Option(str, required=True)):
    yf_sym = resolve_yf_symbol(ticker); db_ticker = get_options_ticker(yf_sym); await ctx.defer(ephemeral=True)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    c.execute("DELETE FROM chain_snapshots WHERE ticker = ?", (db_ticker,)); snap_count = c.rowcount
    c.execute("DELETE FROM market_data WHERE ticker = ?", (yf_sym,)); cache_count = c.rowcount
    conn.commit(); conn.close()
    if snap_count == 0 and cache_count == 0: await ctx.respond(f"❌ **Beeks:** 'No data found.'", ephemeral=True)
    else: await ctx.respond(f"🗑️ **Beeks:** 'Purged **{snap_count}** snapshots and **{cache_count}** cache files for **{db_ticker}**.'", ephemeral=True)

@db_group.command(name="prune", description="Cleanup: Delete OLD Open/Mid. Keep CLOSE.")
@commands.has_permissions(administrator=True)
async def db_prune(ctx: discord.ApplicationContext, ticker: Option(str, description="Ticker (or 'ALL')", required=True), days: Option(int, description="Archive older than X days", default=7)):
    await ctx.defer(ephemeral=True)
    cutoff_date = (datetime.datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    target_ticker = None if ticker.upper() == "ALL" else get_options_ticker(resolve_yf_symbol(ticker))
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    query = "DELETE FROM chain_snapshots WHERE tag != 'CLOSE' AND date(timestamp) < ?"; params = [cutoff_date]
    if target_ticker: query += " AND ticker = ?"; params.append(target_ticker)
    c.execute(query, tuple(params)); deleted_count = c.rowcount; conn.commit(); conn.close()
    scope_str = f"**{target_ticker}**" if target_ticker else "**ALL TICKERS**"
    await ctx.respond(f"🗃️ **Beeks:** 'Pruned (Deleted) **{deleted_count}** intraday files for {scope_str} older than {days} days. CLOSE files preserved.'", ephemeral=True)

# --- SCHEDULER ---
ny_tz = ZoneInfo("America/New_York")
sched_times = [datetime.time(hour=9, minute=45, tzinfo=ny_tz), datetime.time(hour=12, minute=0, tzinfo=ny_tz), datetime.time(hour=15, minute=55, tzinfo=ny_tz)]

@tasks.loop(time=sched_times)
async def auto_fetch_heavy_chains():
    try:
        now = datetime.datetime.now(ny_tz)
        if now.weekday() > 4: return
        session_tag = "OPEN" if now.hour < 11 else "MID" if now.hour < 14 else "CLOSE"
        print(f"\n⏰ AUTO-FETCH TRIGGERED [{session_tag}] at {now.strftime('%H:%M:%S')} ET")
        for symbol in ["^SPX"]:
            try:
                tkr = yf.Ticker(symbol); hist = tkr.history(period="1d")
                if hist.empty: continue
                if not validate_atm_data(tkr, hist['Close'].iloc[-1]): print(f"   ⚠️ Skipping {symbol}: Bad Data"); continue
                full_chain = {"symbol": symbol, "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"), "expirations": {}}
                exps = tkr.options[:6] if tkr.options else []
                for e in exps:
                    try:
                        opt = tkr.option_chain(e); full_chain["expirations"][e] = {"calls": opt.calls.to_dict(orient='records'), "puts": opt.puts.to_dict(orient='records')}
                    except: pass
                save_snapshot(symbol, full_chain, hist['Close'].iloc[-1], get_current_yield(symbol), tag=session_tag, custom_timestamp=now.strftime("%Y-%m-%d %H:%M:%S"))
                print(f"   ✅ Snapshot Saved: {symbol} [{session_tag}]")
            except Exception as e: print(f"   ❌ Failed {symbol}: {e}")
    except Exception as e: print(f"❌ CRITICAL SCHEDULER ERROR: {e}")

@auto_fetch_heavy_chains.before_loop
async def before_scheduler(): await bot.wait_until_ready(); print("⏰ Scheduler armed (NY Time).")

@auto_fetch_heavy_chains.error
async def scheduler_error(error): print(f"💀 SCHEDULER CRASHED: {error}"); auto_fetch_heavy_chains.restart()

@bot.event
async def on_ready(): init_db(); (auto_fetch_heavy_chains.start() if not auto_fetch_heavy_chains.is_running() else None); print(f"🍊 Duke & Duke: Clarence Beeks is Online. Logged in as {bot.user}"); await bot.sync_commands()

if TOKEN: bot.run(TOKEN)
else: bot.run("YOUR_TOKEN_HERE")