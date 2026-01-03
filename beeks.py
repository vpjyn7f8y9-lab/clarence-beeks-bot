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
import requests
from matplotlib.patches import Rectangle, Patch

# --- CONFIGURATION ---
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN") 
PROXY_URL = os.getenv("PROXY_URL")

# Apply Proxy if it exists in .env
if PROXY_URL:
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    print(f"🔒 Proxy Loaded: {PROXY_URL.split('@')[-1]}") # Prints just the host:port for privacy
else:
    print("⚠️ No Proxy Configured. Running Raw IP.")

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
    c.execute('''CREATE TABLE IF NOT EXISTS price_history (ticker TEXT, date TEXT, close REAL, PRIMARY KEY (ticker, date))''')
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
        
        ny_now = datetime.datetime.now(ZoneInfo("America/New_York"))
        today_str = ny_now.strftime("%Y-%m-%d")
        market_closed = ny_now.hour >= 16 or ny_now.weekday() >= 5
        
        target_exp = None
        for e in exps:
            # Skip today if market is closed
            if e == today_str and market_closed: continue
            if datetime.datetime.strptime(e, "%Y-%m-%d").date() < ny_now.date(): continue
            target_exp = e; break
            
        if not target_exp: return False

        chain = tkr.option_chain(target_exp)
        df = pd.concat([chain.calls, chain.puts])
        if df.empty: return False
        
        df['distance'] = abs(df['strike'] - current_price)
        atm_opts = df.sort_values('distance').head(4)
        return len(atm_opts[atm_opts['impliedVolatility'] > 0.001]) >= 3
    except: return False

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

def calculate_max_pain(chain_data):
    if not chain_data: return None
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(chain_data)
    if df.empty: return None
    
    # Filter for relevant strikes (with OI)
    relevant_strikes = sorted(df[df['oi'] > 0]['strike'].unique())
    if not relevant_strikes: return None
    
    # Calculate cash value of all options at each potential strike settlement
    cash_values = {}
    for simulation_price in relevant_strikes:
        total_payout = 0
        
        # Call Payouts: max(0, SimPrice - Strike) * OI
        calls = df[df['type'].str.lower() == 'call']
        call_payouts = (simulation_price - calls['strike']).clip(lower=0) * calls['oi']
        total_payout += call_payouts.sum()
        
        # Put Payouts: max(0, Strike - SimPrice) * OI
        puts = df[df['type'].str.lower() == 'put']
        put_payouts = (puts['strike'] - simulation_price).clip(lower=0) * puts['oi']
        total_payout += put_payouts.sum()
        
        cash_values[simulation_price] = total_payout
        
    # Max Pain is the strike with the MINIMUM payout
    return min(cash_values, key=cash_values.get) if cash_values else None

def update_price_history(ticker):
    yf_sym = resolve_yf_symbol(ticker)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    c.execute("SELECT MAX(date) FROM price_history WHERE ticker = ?", (yf_sym,))
    last_date = c.fetchone()[0]
    
    start_date = None
    today = datetime.date.today()

    # Logic Fix: Don't download if we already have today's data
    if last_date:
        last_dt = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
        if last_dt >= today: 
            conn.close()
            return # Already up to date
        start_date = (last_dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        # Warning Fix: Added auto_adjust=True
        if start_date: 
            data = yf.download(yf_sym, start=start_date, progress=False, auto_adjust=True)
        else: 
            data = yf.download(yf_sym, period="5y", progress=False, auto_adjust=True)
        
        if not data.empty:
            if isinstance(data.columns, pd.MultiIndex): data = data.xs(yf_sym, axis=1, level=1, drop_level=True) if yf_sym in data.columns.levels[1] else data
            for dt, row in data.iterrows():
                try: c.execute("INSERT OR IGNORE INTO price_history (ticker, date, close) VALUES (?, ?, ?)", (yf_sym, dt.strftime("%Y-%m-%d"), row['Close']))
                except: pass
            conn.commit(); print(f"DEBUG: History updated for {yf_sym}")
    except: pass
    conn.close()

def get_hv_from_history(ticker, days=30, target_date=None):
    yf_sym = resolve_yf_symbol(ticker)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    query = "SELECT close FROM price_history WHERE ticker = ?"
    params = [yf_sym]
    if target_date: query += " AND date <= ?"; params.append(target_date)
    query += " ORDER BY date DESC LIMIT ?"
    params.append(days + 1)
    
    c.execute(query, tuple(params)); rows = c.fetchall(); conn.close()
    if len(rows) < days + 1: return None
    
    prices = pd.Series([r[0] for r in rows][::-1])
    returns = np.log(prices / prices.shift(1)).dropna()
    hv = returns.std() * np.sqrt(252)
    return hv if not np.isnan(hv) and hv != 0 else None

# --- CORE LOGIC ---
def fetch_market_data(ticker):
    yf_sym = resolve_yf_symbol(ticker)
    
    # 1. Update History & Calc HV Locally
    update_price_history(yf_sym)
    hv_annual = get_hv_from_history(yf_sym, days=30)
    if hv_annual is None: return None
    
    # 2. Fetch Live Data
    try:
        tkr = yf.Ticker(yf_sym)
        hist = tkr.history(period="1d")
        if hist.empty: return None
        
        # --- NEW ANCHOR LOGIC ---
        ny_now = datetime.datetime.now(ZoneInfo("America/New_York"))
        last_row = hist.iloc[-1]
        last_date = last_row.name.date()
        today = ny_now.date()
        
        # Default to CLOSE (for yesterday/old data)
        anchor_price = last_row['Close']
        anchor_type = "PREV CLOSE"
        
        # If data is from TODAY
        if last_date == today:
            # If market is ACTIVE (Before 4:15 PM to account for settlement delay)
            if ny_now.hour < 16 or (ny_now.hour == 16 and ny_now.minute < 15):
                anchor_price = last_row['Open']
                anchor_type = "OPEN"
            else:
                # Market is CLOSED. Switch anchor to CLOSE for next-day prep.
                anchor_price = last_row['Close']
                anchor_type = "CLOSE"
        # ------------------------

        search_tkr = tkr
        if yf_sym == "^GSPC":
             try: 
                 spx = yf.Ticker("^SPX")
                 if spx.options: search_tkr = spx
             except: pass

        # Find expiry >= 30 Days
        all_exps = search_tkr.options
        valid_exps = [e for e in all_exps if (datetime.datetime.strptime(e, "%Y-%m-%d").date() - today).days >= 30]
        
        target_exp = None
        for exp in valid_exps:
            if is_third_friday(exp): target_exp = exp; break
        if not target_exp and valid_exps: target_exp = valid_exps[0]
        
        if not target_exp: return None 

        if validate_atm_data(search_tkr, anchor_price):
             chain = search_tkr.option_chain(target_exp)
             calls = chain.calls; puts = chain.puts
             if not calls.empty and not puts.empty:
                 calls['abs_diff'] = abs(calls['strike'] - anchor_price)
                 puts['abs_diff'] = abs(puts['strike'] - anchor_price)
                 valid_opts = pd.concat([calls, puts]).sort_values('abs_diff').head(4)
                 
                 avg_iv = valid_opts[valid_opts['impliedVolatility'] > 0.001]['impliedVolatility'].mean()
                 if 0.01 < avg_iv < 5.0:
                     return {
                         "ticker": yf_sym, 
                         "anchor_price": anchor_price, 
                         "anchor_type": anchor_type, # <--- NEW TAG
                         "iv": avg_iv, 
                         "hv": hv_annual, 
                         "date_obj": last_row.name
                     }

    except: pass
    return None

def fetch_historical_data(ticker, date_str, tag):
    db_ticker = get_options_ticker(ticker); yf_sym = resolve_yf_symbol(ticker)
    conn = sqlite3.connect("beeks.db"); c = conn.cursor()
    c.execute("SELECT data_json, timestamp, anchor_price FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? AND tag = ? ORDER BY id DESC LIMIT 1", (db_ticker, date_str, tag))
    row = c.fetchone(); conn.close()
    if not row: return None
    
    snapshot = json.loads(row[0])
    anchor_price = row[2] if row[2] else snapshot.get('anchor_price')
    
    # 1. Calc True Historical HV
    update_price_history(yf_sym)
    hv_annual = get_hv_from_history(yf_sym, days=30, target_date=date_str)
    if hv_annual is None: return None

    # 2. Calc True Historical IV from Snapshot
    try: snap_ts = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    except: snap_ts = datetime.datetime.fromisoformat(row[1])
    
    iv_annual = None
    exps = sorted(snapshot['expirations'].keys())
    target_exp = None
    
    # Find expiry >= 30 days
    for e in exps:
        if (datetime.datetime.strptime(e, "%Y-%m-%d") - snap_ts).days >= 30:
            if is_third_friday(e): target_exp = e; break
    
    if not target_exp: # Fallback to any >= 30 if no monthly
         for e in exps:
             if (datetime.datetime.strptime(e, "%Y-%m-%d") - snap_ts).days >= 30: target_exp = e; break
             
    if target_exp:
        chain = snapshot['expirations'][target_exp]
        df = pd.concat([pd.DataFrame(chain['calls']), pd.DataFrame(chain['puts'])])
        if 'impliedVolatility' in df.columns:
            df['iv'] = pd.to_numeric(df['impliedVolatility'], errors='coerce')
            df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
            df['dist'] = abs(df['strike'] - anchor_price)
            valid_opts = df[(df['iv'] > 0.001) & (df['dist'] < (anchor_price * 0.05))]
            if not valid_opts.empty:
                iv_annual = valid_opts.sort_values('dist').head(4)['iv'].mean()

    if iv_annual is None: return None
    
    return {"date": date_str, "ticker": ticker, "anchor_price": anchor_price, "iv": iv_annual, "hv": hv_annual, "saved_at": row[1], "is_backtest": True}

def fetch_and_enrich_chain(ticker, expiry_date, snapshot_date=None, snapshot_tag=None, target_strike=None, range_count=None, pivot=None, scope="Front Month"):
    yf_sym = resolve_yf_symbol(ticker)
    display_ticker = get_options_ticker(yf_sym)
    
    S = 0.0; q = 0.0; r = 0.045
    chain_data = {'calls': [], 'puts': []}
    
    # --- SNAPSHOT REPLAY (BACKTEST) ---
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
            
            # LOGIC HIERARCHY
            if expiry_date:
                if expiry_date in data['expirations']: target_exps = [expiry_date]
            elif scope == "0DTE":
                snap_day = snap_ts.strftime("%Y-%m-%d")
                if snap_day in data['expirations']: target_exps = [snap_day]
            elif scope == "Front Month":
                snap_day = snap_ts.date()
                target_exps = [e for e in data['expirations'] if 1 <= (datetime.datetime.strptime(e, "%Y-%m-%d").date() - snap_day).days <= 30]
            elif scope == "Total Market": 
                target_exps = list(data['expirations'].keys())
            
            for e in target_exps:
                exp_dt = datetime.datetime.strptime(e, "%Y-%m-%d")
                t_val = (exp_dt - snap_ts).days / 365.0
                if t_val < 0.001: t_val = 0.001 
                c_list = data['expirations'][e]['calls']; p_list = data['expirations'][e]['puts']
                
                # Pre-inject time and spot for consistency
                for x in c_list: x['time_year'] = t_val; x['spot'] = S
                for x in p_list: x['time_year'] = t_val; x['spot'] = S
                    
                chain_data['calls'].extend(c_list); chain_data['puts'].extend(p_list)
        except: return None
        
    # --- LIVE DATA ---
    else:
        try:
            update_price_history(yf_sym)
            
            tkr = yf.Ticker(yf_sym)
            hist = tkr.history(period="1d")
            if hist.empty: return None
            
            ny_now = datetime.datetime.now(ZoneInfo("America/New_York"))
            last_row = hist.iloc[-1]
            
            if last_row.name.date() == ny_now.date() and (ny_now.hour < 16 or (ny_now.hour == 16 and ny_now.minute < 15)):
                S = last_row['Open'] 
            else:
                S = last_row['Close']
            
            q = get_current_yield(yf_sym)
            
            search_tkr = tkr
            if yf_sym == "^GSPC":
                 try: 
                     spx = yf.Ticker("^SPX")
                     if spx.options: search_tkr = spx
                 except: pass

            if not validate_atm_data(search_tkr, S): return None
            
            all_exps = search_tkr.options
            if not all_exps: return None            
            
            target_exps = []
            today = datetime.date.today()
            
            if expiry_date:
                if expiry_date in all_exps: target_exps = [expiry_date]
            elif scope == "0DTE":
                today_str = today.strftime("%Y-%m-%d")
                is_market_closed = ny_now.hour >= 16 or ny_now.weekday() >= 5
                if not is_market_closed and today_str in all_exps:
                    target_exps = [today_str]
                else:
                    future_exps = [e for e in all_exps if datetime.datetime.strptime(e, "%Y-%m-%d").date() > today]
                    if future_exps: target_exps = [future_exps[0]]
            elif scope == "Front Month":
                target_exps = [e for e in all_exps if 1 <= (datetime.datetime.strptime(e, "%Y-%m-%d").date() - today).days <= 30]
            elif scope == "Total Market": 
                target_exps = all_exps 

            for e in target_exps:
                try:
                    opt = search_tkr.option_chain(e)
                    c_list = opt.calls.to_dict(orient='records'); p_list = opt.puts.to_dict(orient='records')
                    exp_dt = datetime.datetime.strptime(e, "%Y-%m-%d")
                    t_val = (exp_dt - ny_now.replace(tzinfo=None)).days / 365.0
                    if t_val < 0.001: t_val = 0.001
                    
                    for x in c_list: x['time_year'] = t_val; x['spot'] = S
                    for x in p_list: x['time_year'] = t_val; x['spot'] = S
                        
                    chain_data['calls'].extend(c_list); chain_data['puts'].extend(p_list)
                except: pass
        except: return None

    all_options = []
    for c in chain_data['calls']: c['type'] = 'Call'; all_options.append(c)
    for p in chain_data['puts']: p['type'] = 'Put'; all_options.append(p)
    df = pd.DataFrame(all_options)
    if df.empty: return []
    
    df.rename(columns={'impliedVolatility': 'iv', 'openInterest': 'oi', 'volume': 'vol', 'lastPrice': 'price'}, inplace=True, errors='ignore')

    # FILTERING
    if target_strike: df = df[df['strike'] == target_strike]
    elif range_count:
        anchor = pivot if pivot else S
        unique_strikes = df['strike'].unique()
        dists = pd.DataFrame({'strike': unique_strikes, 'dist': abs(unique_strikes - anchor)})
        keep_strikes = dists.sort_values('dist').head(range_count)['strike'].values
        df = df[df['strike'].isin(keep_strikes)]

    # ENRICHMENT (ALWAYS CALCULATE GREEKS)
    results = []
    for _, row in df.iterrows():
        K = row['strike']; IV = row['iv']
        # Skip garbage data
        if pd.isna(IV) or IV < 0.001: continue
        
        # Calculate Greeks
        delta, gamma, theta, vanna, charm = calculate_black_scholes(S, K, row['time_year'], r, IV, q, row['type'].lower())
        
        item = {
            'strike': K, 'type': row['type'], 
            'price': row.get('price', 0), 'bid': row.get('bid', 0), 'ask': row.get('ask', 0), 
            'volume': row.get('vol', 0), 'oi': row.get('oi', 0), 'iv': IV, 
            'delta': delta, 'gamma': gamma, 'theta': theta, 'vanna': vanna, 'charm': charm, 
            'time_year': row.get('time_year', 0), 'spot': S
        }
        results.append(item)
        
    return sorted(results, key=lambda x: x['strike'])

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
        
        # FIX: Ensure we don't go out of bounds if flip is at the edge
        if idx + 1 < len(net_gammas):
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
    # FIX: Guard against empty strikes to prevent plotting errors
    if len(strikes) == 0: return None

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
        
        # FIX: Check if vals has content before running max() to avoid crash
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

def generate_flip_chart(ticker, spot, flip_level, sim_spots, net_gammas, scope_label):
    plt.figure(figsize=(12, 6)); plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 6)); fig.patch.set_facecolor('#1e1e1e'); ax.set_facecolor('#1e1e1e')
    
    # Plot the Gamma Curve
    # Green where > 0 (Stable), Red where < 0 (Volatile)
    ax.plot(sim_spots, net_gammas, color='white', linewidth=1, alpha=0.5)
    ax.fill_between(sim_spots, net_gammas, 0, where=(net_gammas >= 0), color='#00ff00', alpha=0.3, interpolate=True)
    ax.fill_between(sim_spots, net_gammas, 0, where=(net_gammas < 0), color='#ff0000', alpha=0.3, interpolate=True)
    
    # Zero Line
    ax.axhline(0, color='white', linewidth=1)
    
    # Spot Price Line
    ax.axvline(spot, color='yellow', linestyle='--', linewidth=2, label=f'Spot: {spot:.2f}')
    
    # Flip Level Line
    if flip_level:
        ax.axvline(flip_level, color='cyan', linewidth=2, label=f'Flip: {flip_level:.2f}')
        
    ax.set_title(f"{ticker} GAMMA FLIP SIMULATION", color='white', weight='bold', fontsize=14)
    ax.set_xlabel("Underlying Price", color='gray'); ax.set_ylabel("Net Gamma ($)", color='gray')
    ax.legend(facecolor='#333333', labelcolor='white')
    
    # Footer
    plt.figtext(0.5, 0.02, f"SCOPE: {scope_label} | MODE: {'STABLE' if spot > flip_level else 'VOLATILE'}", ha="center", color="white", fontsize=10)
    
    buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
    return buf

def generate_vig_chart(ticker, spot, vig, upper, lower, scope_label):
    plt.figure(figsize=(10, 4)); plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 4)); fig.patch.set_facecolor('#1e1e1e'); ax.set_facecolor('#1e1e1e')
    ax.get_yaxis().set_visible(False)
    
    # Draw Range Line
    plt.plot([lower, upper], [0, 0], color='#444444', linewidth=4, zorder=1)
    
    # Draw Breakeven Markers
    plt.scatter([lower, upper], [0, 0], color='cyan', s=200, zorder=2, label='Breakevens')
    plt.text(lower, 0.1, f"{lower:.2f}", color='cyan', ha='center', weight='bold')
    plt.text(upper, 0.1, f"{upper:.2f}", color='cyan', ha='center', weight='bold')
    
    # Draw Spot Marker
    plt.scatter([spot], [0], color='yellow', s=300, marker='|', linewidth=4, zorder=3, label='Spot')
    plt.text(spot, -0.15, f"SPOT\n{spot:.2f}", color='yellow', ha='center', weight='bold')

    ax.set_title(f"{ticker} INTRADAY EXPECTED MOVE (VIG)", color='white', weight='bold', pad=20)
    
    # Footer
    plt.figtext(0.5, 0.05, f"SCOPE: {scope_label} | COST: ${vig:.2f}", ha="center", color="gray", fontsize=10)
    
    # Remove spines
    for spine in ax.spines.values(): spine.set_visible(False)
    ax.set_xticks([])

    buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
    return buf

def generate_skew_chart(ticker, spot, c_iv, p_iv, ratio, scope_label):
    plt.figure(figsize=(8, 6)); plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(8, 6)); fig.patch.set_facecolor('#1e1e1e'); ax.set_facecolor('#1e1e1e')
    
    # Bar Chart
    bars = ax.bar(['Put IV (Fear)', 'Call IV (Greed)'], [p_iv*100, c_iv*100], color=['#ff5555', '#55ff55'], alpha=0.8, width=0.5)
    ax.bar_label(bars, fmt='%.2f%%', padding=3, color='white', fontsize=12, weight='bold')
    
    ax.set_title(f"{ticker} 25-DELTA SKEW", color='white', weight='bold', fontsize=14)
    ax.set_ylim(0, max(c_iv, p_iv)*100 * 1.3) # Add headroom
    
    # Remove borders
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False); ax.spines['left'].set_visible(False)
    ax.get_yaxis().set_visible(False)
    
    # Footer
    status = "BEARISH" if ratio > 1.1 else "BULLISH" if ratio < 0.9 else "NEUTRAL"
    plt.figtext(0.5, 0.02, f"SCOPE: {scope_label} | RATIO: {ratio:.2f}x ({status})", ha="center", color="white", fontsize=11, weight='bold')

    buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
    return buf

def generate_pcr_dashboard(ticker, spot, vol_pcr, oi_pcr, vol_data, oi_data, scope_label, expiry_label, max_pain, total_vol):
    plt.figure(figsize=(14, 9)) # Increased height slightly
    plt.style.use('dark_background')
    
    # Grid: Top=Scoreboard, Mid=Charts, Bottom=Stats/Footer
    gs = matplotlib.gridspec.GridSpec(3, 2, height_ratios=[0.25, 0.60, 0.15])
    fig = plt.gcf()
    fig.patch.set_facecolor('#1e1e1e')

    # --- TOP ROW: SCOREBOARD ---
    ax_score_l = plt.subplot(gs[0, 0]); ax_score_l.axis('off')
    c_vol = '#ff5555' if vol_pcr > 1.0 else '#55ff55'
    ax_score_l.text(0.5, 0.6, "VOLUME PCR (FLOW)", color='white', fontsize=12, ha='center', weight='bold')
    ax_score_l.text(0.5, 0.1, f"{vol_pcr:.2f}", color=c_vol, fontsize=34, ha='center', weight='bold')

    ax_score_r = plt.subplot(gs[0, 1]); ax_score_r.axis('off')
    c_oi = '#ff5555' if oi_pcr > 1.0 else '#55ff55'
    ax_score_r.text(0.5, 0.6, "OPEN INTEREST PCR", color='white', fontsize=12, ha='center', weight='bold')
    ax_score_r.text(0.5, 0.1, f"{oi_pcr:.2f}", color=c_oi, fontsize=34, ha='center', weight='bold')

    # --- MID ROW: CHARTS ---
    ax1 = plt.subplot(gs[1, 0]); ax1.set_facecolor('#1e1e1e')
    total_v = vol_data['calls'] + vol_data['puts']
    if total_v > 0:
        bars = ax1.bar(['Calls', 'Puts'], [vol_data['calls'], vol_data['puts']], color=['#00cc00', '#cc0000'], alpha=0.8)
        ax1.bar_label(bars, labels=[f"{int(vol_data['calls']/1000)}k", f"{int(vol_data['puts']/1000)}k"], padding=3, color='white', weight='bold')
    ax1.set_title(f"Aggression (Vol)", color='#888888', fontsize=10)
    ax1.spines['top'].set_visible(False); ax1.spines['right'].set_visible(False)

    ax2 = plt.subplot(gs[1, 1]); ax2.set_facecolor('#1e1e1e')
    total_o = oi_data['calls'] + oi_data['puts']
    if total_o > 0:
        bars = ax2.bar(['Calls', 'Puts'], [oi_data['calls'], oi_data['puts']], color=['#006600', '#660000'], alpha=0.6)
        ax2.bar_label(bars, labels=[f"{int(oi_data['calls']/1000)}k", f"{int(oi_data['puts']/1000)}k"], padding=3, color='white', weight='bold')
    ax2.set_title("Structure (OI)", color='#888888', fontsize=10)
    ax2.spines['top'].set_visible(False); ax2.spines['right'].set_visible(False)

    # --- BOTTOM ROW: STATS & FOOTER ---
    ax_bot = plt.subplot(gs[2, :]); ax_bot.axis('off')
    
    # 1. Signal
    signal = "NEUTRAL"; sig_c = "white"
    if vol_pcr > oi_pcr and vol_pcr > 1.0: signal = "FEAR (SHORT TERM)"; sig_c = "#ffaa00"
    elif vol_pcr < oi_pcr and vol_pcr < 0.8: signal = "COMPLACENCY"; sig_c = "#00ffff"
    elif vol_pcr > 1.0 and oi_pcr > 1.0: signal = "BEARISH (ALIGNED)"; sig_c = "#ff0000"
    elif vol_pcr < 0.7 and oi_pcr < 0.7: signal = "BULLISH (ALIGNED)"; sig_c = "#00ff00"
    
    # 2. Key Stats Line (Max Pain & Volume)
    # FIX: Handle max_pain being None or 0 gracefully for display
    if max_pain and max_pain != 0:
        mp_str = f"{int(max_pain)}"
    else:
        mp_str = "N/A"

    stats_text = f"MAX PAIN: {mp_str}  |  TOTAL VOL: {int(total_vol/1000)}k  |  SPOT: {spot:.2f}"
    
    ax_bot.text(0.5, 0.7, stats_text, color='white', fontsize=14, ha='center', weight='bold', bbox=dict(facecolor='#333333', edgecolor='none', pad=6))
    ax_bot.text(0.5, 0.3, f"SIGNAL: {signal}", color=sig_c, fontsize=12, ha='center', weight='bold')
    ax_bot.text(0.5, 0.05, f"{ticker} | {expiry_label if expiry_label else scope_label}", color='#666666', ha='center', fontsize=9)

    plt.subplots_adjust(top=0.95, bottom=0.05, hspace=0.4, wspace=0.2)
    buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
    return buf

def generate_vrp_gauge(ticker, iv, hv, spread, ratio):
    plt.figure(figsize=(10, 5))
    plt.style.use('dark_background')
    ax = plt.gca(); ax.axis('off')
    
    # Title
    plt.text(0.5, 0.9, f"VOLATILITY RISK PREMIUM (VRP)", color='white', fontsize=16, weight='bold', ha='center')
    
    # The Big Number (Spread)
    c_spr = '#00ff00' if spread > 0 else '#ff0000' # Green = Sell Prem, Red = Buy Prem
    sign = "+" if spread > 0 else ""
    plt.text(0.5, 0.6, f"{sign}{spread:.2f}%", color=c_spr, fontsize=40, weight='bold', ha='center')
    
    # Subtitle (Action)
    action = "OVERPRICED (SELL)" if spread > 0 else "UNDERPRICED (BUY)"
    plt.text(0.5, 0.45, action, color=c_spr, fontsize=12, weight='bold', ha='center', bbox=dict(facecolor='#222222', edgecolor=c_spr, pad=5))
    
    # Details Row
    plt.text(0.25, 0.25, "IMPLIED (IV)", color='#00ccff', fontsize=10, ha='center')
    plt.text(0.25, 0.15, f"{iv*100:.2f}%", color='white', fontsize=14, weight='bold', ha='center')
    
    plt.text(0.75, 0.25, "REALIZED (HV)", color='#ffaa00', fontsize=10, ha='center')
    plt.text(0.75, 0.15, f"{hv*100:.2f}%", color='white', fontsize=14, weight='bold', ha='center')
    
    # Footer
    plt.text(0.5, 0.05, f"{ticker} | Ratio: {ratio:.2f}x", color='#666666', fontsize=9, ha='center')
    
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
        embed.description="**Sentiment Detector (Put/Call Ratio)**\nCompares the cost of Downside Protection (Puts) vs Upside Speculation (Calls)."
        embed.add_field(name="Methods", value="**Percentage:** Fixed distance (Spot ±1% or ±5%). Best for levels/scalping.\n**Delta:** Institutional standard (25 Delta). Best for pure sentiment/fear.", inline=False)
        embed.add_field(name="Modes", value="**Intraday:** 0DTE (Expires Today).\n**Macro:** 30-Day (Hedging View).", inline=False)

    elif command == "setmode":
        embed.title="📘 Manual: Set Mode"
        embed.description="**Global Terminal View**\nConfigure how the bot delivers data to you."
        embed.add_field(name="Options", value="**Modern:** Generates visual charts and PNG dashboards. (Best for Desktop)\n**Bloomberg:** Returns raw text and ASCII tables. (Best for Mobile/Low Data)", inline=False)

    elif command == "dom_pcr":
        embed.title="📘 Manual: Put-Call Ratio (PCR)"
        embed.description="**Volume vs. Open Interest (Flow vs. Structure)**\nCompare the intraday aggression (Volume) against the overnight positioning (Open Interest)."
        embed.add_field(name="The Logic", value="**Volume PCR:** Measures today's panic/greed.\n**OI PCR:** Measures structural hedges.\n**Divergence:** If Vol PCR spikes > OI PCR, it's often short-term panic.", inline=False)
        embed.add_field(name="Signals", value="**> 1.0:** Bearish.\n**< 0.7:** Bullish.\n**Vol > OI:** Speculative Fear.", inline=False)

    elif command == "dom_vrp":
        embed.title="📘 Manual: Volatility Risk Premium"
        embed.description="**The Edge Meter (IV vs RV)**\nCompares Implied Volatility (what we expect) vs Realized Volatility (what happened)."
        embed.add_field(name="Strategy", value="**Green (Positive):** Options are expensive. Edge is in SELLING (Iron Condors).\n**Red (Negative):** Options are cheap. Edge is in BUYING (Long Straddles).", inline=False)

    await ctx.respond(embed=embed, ephemeral=True)

@beeks.command(name="setmode", description="Configure Global Terminal View Settings")
async def beeks_setmode(ctx: discord.ApplicationContext, terminal: Option(str, choices=["Modern", "Bloomberg"], required=True)):
    set_user_terminal_setting(ctx.author.id, terminal.lower())
    await ctx.respond(f"🌎 **Global Terminal View** set to: **{terminal.upper()}**", ephemeral=True)

@beeks.command(name="dailyrange", description="Get Volatility Ranges")
async def beeks_dailyrange(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True), 
    engine: Option(str, choices=["Insider Info", "Market Floor"], default="Insider Info"), 
    replay_date: Option(str, autocomplete=get_db_dates, required=False), 
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        raw_ticker = ticker.upper(); yf_sym = resolve_yf_symbol(raw_ticker)
        FUTURES_MAP = {"ES": "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F"}; is_future = raw_ticker in FUTURES_MAP; futures_ticker = FUTURES_MAP.get(raw_ticker)
        
        if replay_date:
            tag_to_use = session if session and "Select" not in session else get_latest_tag_for_date(get_options_ticker(yf_sym), replay_date)
            data = fetch_historical_data(yf_sym, replay_date, tag_to_use)
            if not data: 
                await ctx.respond(f"❌ **Beeks:** 'File not found.'", ephemeral=True)
                return
            clean_footer = f"📼 REPLAY: {replay_date} [{tag_to_use}]"; movie_quote = random.choice(MOVIE_QUOTES)
            anchor_label = "SNAPSHOT" 
        else:
            if not (yf_sym in ["^GSPC", "^NDX"] or is_future or (not yf_sym.startswith("^") and not yf_sym.endswith("=F"))): 
                await ctx.respond(f"❌ **Beeks:** 'Incompatible Ticker.'", ephemeral=True)
                return
            
            data = fetch_market_data(yf_sym)
            if not data: 
                await ctx.respond(f"❌ **Beeks:** 'Data corrupted.'", ephemeral=True)
                return
            
            movie_quote = random.choice(MOVIE_QUOTES)
            
            ny_tz = pytz.timezone('America/New_York'); ny_now = datetime.datetime.now(ny_tz)
            anchor_label = data.get('anchor_type', "REF")
            clean_footer = f"NY TIME: {ny_now.strftime('%H:%M')} ET | REF: {anchor_label}"
            if 'saved_at' in data: clean_footer = f"⚠️ ARCHIVE DATA | {data['saved_at']}"

        levels = calculate_levels(data['anchor_price'], data['iv'], data['hv'], engine)
        display_price = data['anchor_price']
        
        # Futures Offset Logic
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
        
        # Logic to handle fallback
        should_send_text = (view_setting == "bloomberg")
        forced_fallback = False
        
        if not should_send_text:
            try:
                buf = create_beeks_chart(raw_ticker, chart_data, levels, engine)
                file = discord.File(buf, filename="beeks_dailyrange.png")
                embed = discord.Embed(description=f"**{movie_quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_dailyrange.png"); embed.set_footer(text=clean_footer)
                await ctx.respond(embed=embed, file=file, ephemeral=True)
            except:
                # If charting fails, force text mode and flag it
                should_send_text = True
                forced_fallback = True

        if should_send_text:
            msg = ""
            if forced_fallback:
                msg += "⚠️ **[VISUAL ENGINE FAIL]** Displaying text backup.\n\n"

            msg += f"> **{movie_quote}**\n\n**{raw_ticker} VOLATILITY REPORT ({engine})**\n\n`{'RANGE':<12} | {'VALUE':<12} | {'MOVE':<12} | {'TYPE':<12}\n" + "-"*56 + "\n"
            msg += f"{'IV':<12} | {data['iv']:<12.4f} | {levels['meta']['daily_move_iv']:<12.2f} | {'Implied'}\n{'HV':<12} | {data['hv']:<12.4f} | {levels['meta']['daily_move_hv']:<12.2f} | {'Historical'}\n`\n\n**{raw_ticker} LEVELS**\n`{'LEVEL':<12} | {'VALENTINE':<15} | {'WINTHORPE':<15}\n" + "-"*48 + "\n"
            for i in range(4, 0, -1): msg += f"+{i}σ           | {levels['valentine'][f'+{i}σ']:<15.2f} | {levels['winthorpe'][f'+{i}σ']:<15.2f}\n" 
            msg += f"{anchor_label:<12} | {display_price:.2f}\n" 
            for i in range(1, 5): msg += f"-{i}σ           | {levels['valentine'][f'-{i}σ']:<15.2f} | {levels['winthorpe'][f'-{i}σ']:<15.2f}\n" 
            msg += f"`\n*{clean_footer}*"
            await ctx.respond(msg, ephemeral=True)

    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'Range Analysis Failed: {e}'", ephemeral=True)

@beeks.command(name="dailylevels", description="Key Dealer GEX Levels & Volatility")
async def daily_levels(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True), 
    metric: Option(str, choices=["ALL", "CONFLUENCE", "DEX", "GEX", "VEX", "CEX"], default="ALL"), 
    scope: Option(str, choices=["0DTE", "Front Month", "Total Market"], default="0DTE"),
    target_expiry: Option(str, description="Override Scope (Format: YYYY-MM-DD)", required=False), 
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False), 
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
        calc_date = None; calc_tag = session; market_time = datetime.datetime.now(ZoneInfo("America/New_York"))
        
        # Date Logic
        if target_expiry: target_date = target_expiry; calc_date = replay_date if replay_date else None
        elif scope == "0DTE" and not replay_date:
            target_date = market_time.strftime("%Y-%m-%d") if market_time.weekday() <= 4 and market_time.hour < 16 else get_next_market_date(market_time)
            # Check if we should use DB for today (e.g. after close)
            conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT date(timestamp) FROM chain_snapshots WHERE ticker = ? AND tag = 'CLOSE' ORDER BY timestamp DESC LIMIT 1", (display_ticker,)); row = c.fetchone(); conn.close()
            if row and row[0] == target_date: calc_date = row[0]; calc_tag = "CLOSE"
        elif replay_date: calc_date = replay_date; target_date = replay_date
        else: target_date = None

        await ctx.interaction.edit_original_response(content=f"⏳ **Beeks:** 'Mapping {metric}...'")
        
        # Fetch Data
        raw_data = fetch_and_enrich_chain(ticker=ticker, expiry_date=target_date, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope, range_count=9999)
        if not raw_data: 
            await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'Live Data Feed Is Currently Dark. Can you Try a Replay Date?'")
            return
        
        spot_price = raw_data[0]['spot']; strike_data = calculate_strike_exposures(raw_data, spot_price, display_ticker)
        
        # Confluence Calc
        def get_top(vals): return {x[0] for x in sorted([p for p in zip(strike_data['strikes'], vals) if abs(p[1])>0], key=lambda x: abs(x[1]), reverse=True)[:5]}
        all_sig = get_top(strike_data['gex']) | get_top(strike_data['dex']) | get_top(strike_data['vex']) | get_top(strike_data['cex'])
        confluence_map = []
        for k in all_sig:
            score = (1 if k in get_top(strike_data['gex']) else 0) + (1 if k in get_top(strike_data['dex']) else 0) + (1 if k in get_top(strike_data['vex']) else 0) + (1 if k in get_top(strike_data['cex']) else 0)
            tags = ("D" if k in get_top(strike_data['dex']) else "") + ("G" if k in get_top(strike_data['gex']) else "") + ("V" if k in get_top(strike_data['vex']) else "") + ("C" if k in get_top(strike_data['cex']) else "")
            confluence_map.append({'strike': k, 'score': score, 'tags': tags})
        confluence_map.sort(key=lambda x: (x['score'], x['strike']), reverse=True)

        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
        
        # VISUALIZATION
        if view_setting == 'modern':
            img_buf = generate_strike_chart(display_ticker, spot_price, strike_data, metric, confluence_map)
            
            # FIX: Check if chart generation failed (returned None)
            if not img_buf:
                await ctx.interaction.edit_original_response(content="❌ **Beeks:** 'Not enough data to map levels.'")
                return

            file = discord.File(img_buf, filename="beeks_strikes.png")
            desc = f"**{quote}**\n" + (f"\n**🎯 CONFLUENCE (Top 5)**\n" + "\n".join([f"`{int(i['strike']):<5}` {'⭐'*i['score']} ({i['tags']})" for i in confluence_map[:5]]) if metric == "CONFLUENCE" else "")
            embed = discord.Embed(color=0x2b2d31, description=desc); embed.set_image(url="attachment://beeks_strikes.png"); embed.set_footer(text=f"Spot: {spot_price:.2f} | {scope}")
            await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
        else:
            # Text Mode
            lines = [f"> **{quote}**"]
            def fmt(n):
                if abs(n) >= 1_000_000_000: return f"{n/1_000_000_000:.1f}B"
                if abs(n) >= 1_000_000: return f"{n/1_000_000:.1f}M"
                if abs(n) >= 1_000: return f"{n/1_000:.0f}K"
                return f"{n:.0f}"

            if metric == "CONFLUENCE":
                lines += ["```yaml", f"SPOT REF: {spot_price:.2f}", "+---------------------------------------+", "| KEY LEVELS (HIGHEST OVERLAP)          |", "+---------------------------------------+"]
                for i in confluence_map: lines.append(f"| {int(i['strike']):<7} | {'*'*i['score']:<4} | {i['tags']:<5} |")
                lines.append("```")
            
            else:
                s_list = strike_data['strikes']
                # FIX: Verify list is not empty before indexing
                if not s_list:
                    await ctx.interaction.edit_original_response(content="❌ **Beeks:** 'No strikes found.'")
                    return

                closest_idx = min(range(len(s_list)), key=lambda i: abs(s_list[i]-spot_price))
                start = max(0, closest_idx - 10); end = min(len(s_list), closest_idx + 11)
                
                lines.append("```yaml")
                if metric == "ALL":
                    lines.append(f"|{'STRIKE':^8}|{'GEX':^7}|{'DEX':^7}|{'VEX':^7}|")
                    lines.append("-" * 32)
                else:
                    lines.append(f"|{'STRIKE':^8}|{metric:^10}|")
                    lines.append("-" * 20)

                for i in range(start, end):
                    k = s_list[i]; prefix = ">" if i == closest_idx else " "
                    if metric == "ALL":
                        g = fmt(strike_data['gex'][i]); d = fmt(strike_data['dex'][i]); v = fmt(strike_data['vex'][i])
                        lines.append(f"{prefix}|{int(k):<7}|{g:>7}|{d:>7}|{v:>7}|")
                    else:
                        val = fmt(strike_data[metric.lower()][i])
                        lines.append(f"{prefix}|{int(k):<7}|{val:>10}|")
                lines.append("```"); lines.append(f"**Spot:** {spot_price:.2f} | **Range:** ±10 Strikes")

            await ctx.interaction.edit_original_response(content="\n".join(lines))
            
    except Exception as e:
        await ctx.interaction.edit_original_response(content=f"⚠️ **Beeks:** 'Level Mapping Failed: {e}'")

@beeks.command(name="chain", description="View Raw Option Chain Data")
async def dom_chain(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True), 
    target_expiry: Option(str, description="Target Event (Format: YYYY-MM-DD)", required=False),
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        scope_label = "0DTE" if not target_expiry else f"EXP: {target_expiry}"
        
        # Fetch Data
        data = fetch_and_enrich_chain(ticker, target_expiry, replay_date, session, scope="0DTE")
        
        # FIX: Check for empty data to prevent crash
        if not data: 
            await ctx.respond("❌ **Beeks:** 'Chain is dark. No data found.'", ephemeral=True)
            return

        spot = data[0]['spot']
        # Sort by strike to make the list readable
        data.sort(key=lambda x: x['strike'])
        
        # Find the index of the strike closest to spot
        closest_idx = min(range(len(data)), key=lambda i: abs(data[i]['strike'] - spot))
        
        # Slice: Show 5 strikes above and 5 below ATM (10 total rows approx)
        start_idx = max(0, closest_idx - 5)
        end_idx = min(len(data), closest_idx + 6)
        subset = data[start_idx:end_idx]

        quote = random.choice(MOVIE_QUOTES)
        display_ticker = get_options_ticker(ticker)

        msg = f"> **{quote}**\n```yaml\n"
        msg += f"[{display_ticker} CHAIN SNAPSHOT]\n"
        msg += f"SCOPE : {scope_label}\n"
        msg += f"SPOT  : {spot:.2f}\n"
        msg += "-" * 65 + "\n"
        msg += f"|{'STRIKE':^8}|{'TYPE':^4}|{'BID':^6}|{'ASK':^6}|{'VOL':^6}|{'OI':^6}|{'GEX':^8}|\n"
        msg += "-" * 65 + "\n"

        for row in subset:
            # Highlight ATM row
            prefix = ">" if row == data[closest_idx] else " "
            
            # formatting helpers
            typ = "C" if row['type'] == 'Call' else "P"
            gex_short = f"{int(row['gex']/1000)}k" if abs(row['gex']) < 1000000 else f"{row['gex']/1000000:.1f}M"
            
            line = f"{prefix}|{row['strike']:<8.1f}|{typ:^4}|{row['bid']:>6.2f}|{row['ask']:>6.2f}|{row['volume']:>6}|{row['oi']:>6}|{gex_short:>8}|"
            msg += line + "\n"
            
        msg += "```"
        await ctx.respond(msg, ephemeral=True)

    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'Chain retrieval failed: {e}'", ephemeral=True)

# --- DOM SUITE ---
dom_group = beeks.create_subgroup("dom", "Dealer Open Market (Structure & Positioning)")

@dom_group.command(name="report", description="RUN ALL: Generates the full Clarence Beeks Executive Dashboard")
async def dom_report(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True),
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
        calc_date = replay_date if replay_date else None; calc_tag = session
        if calc_date and not calc_tag: calc_tag = get_latest_tag_for_date(display_ticker, calc_date)

        # 1. FETCH TOTAL MARKET
        await ctx.interaction.edit_original_response(content=f"⏳ **Beeks:** 'Compiling full dossier on {display_ticker}...'")
        vrp_data = fetch_market_data(yf_sym)
        raw_data = fetch_and_enrich_chain(ticker, None, calc_date, calc_tag, scope="Total Market")

        if not raw_data: 
            await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'Data Dark. Cannot compile report.'"); return

        spot = raw_data[0]['spot']

        # 2. SLICE DATA
        data_0dte = [x for x in raw_data if x['time_year'] <= 0.004] 
        data_front = [x for x in raw_data if 0.004 < x['time_year'] <= 0.083]
        data_total = raw_data

        # 3. CALCULATE METRICS
        # A. EXPOSURES
        gex, dex, vex = calculate_market_exposures(data_total, spot)
        regime = "DAMPENED (Positive GEX)" if gex > 0 else "ACCELERATED (Negative GEX)"

        # B. FLIP
        flip_price, _ = calculate_gamma_flip(data_front, spot)
        
        # FIX: Check for None before comparison to avoid Type Error
        if flip_price is not None:
            flip_status = "BULLISH" if spot > flip_price else "BEARISH"
        else:
            flip_status = "UNKNOWN"

        # C. VIG
        vig_val = 0.0; vig_upper = 0.0; vig_lower = 0.0
        if data_0dte:
            closest = min(data_0dte, key=lambda x: abs(x['strike'] - spot))
            atm_c = next((x for x in data_0dte if x['strike'] == closest['strike'] and x['type'] == 'Call'), None)
            atm_p = next((x for x in data_0dte if x['strike'] == closest['strike'] and x['type'] == 'Put'), None)
            if atm_c and atm_p:
                c_p = (atm_c['bid'] + atm_c['ask'])/2 if atm_c['bid']>0 else atm_c['price']
                p_p = (atm_p['bid'] + atm_p['ask'])/2 if atm_p['bid']>0 else atm_p['price']
                vig_val = c_p + p_p
                vig_upper = closest['strike'] + vig_val; vig_lower = closest['strike'] - vig_val

        # D. SKEW
        skew_ratio = 0.0; skew_status = "NEUTRAL"
        if data_front:
            calls = [x for x in data_front if x['type'] == 'Call']; puts = [x for x in data_front if x['type'] == 'Put']
            
            # FIX: Ensure we have both calls and puts before running min()
            if calls and puts:
                c_25 = min(calls, key=lambda x: abs(x['delta'] - 0.25))
                p_25 = min(puts, key=lambda x: abs(abs(x['delta']) - 0.25))
                if c_25 and p_25 and c_25['iv'] > 0:
                    skew_ratio = p_25['iv'] / c_25['iv']
                    skew_status = "FEAR" if skew_ratio > 1.15 else "GREED" if skew_ratio < 0.9 else "NEUTRAL"

        # E. PCR
        vol_pcr = 0.0; max_pain = 0.0
        if data_0dte:
            c_vol = sum(x['volume'] for x in data_0dte if x['type'] == 'Call'); p_vol = sum(x['volume'] for x in data_0dte if x['type'] == 'Put')
            vol_pcr = p_vol / c_vol if c_vol > 0 else 0
            max_pain = calculate_max_pain(data_0dte)

        # F. VRP
        vrp_spread = 0.0
        if vrp_data: vrp_spread = (vrp_data['iv'] - vrp_data['hv']) * 100

        # 4. REPORT GENERATION
        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
        
        if view_setting == 'modern':
            # Generate the Main Dashboard Image
            img_exp = generate_exposure_dashboard(display_ticker, spot, gex, dex, vex, "TOTAL MARKET", None)
            
            # NOTE: We are intentionally SKIPPING the VRP thumbnail to fix formatting.
            # If you want VRP visual, we should merge it into the main dashboard later. 
            # For now, text representation is cleaner.

            file_exp = discord.File(img_exp, filename="beeks_exposures.png")
            
            # Executive Summary Embed
            embed = discord.Embed(title=f"🍊 THE BEEKS REPORT: {display_ticker}", description=f"**\"{quote}\"**", color=0x2b2d31)
            
            # FULL WIDTH HEADER (Regime)
            embed.add_field(name="📍 MARKET REGIME (Total)", value=f"**{regime}**\nGEX: ${gex/1_000_000_000:.2f}B", inline=False)
            
            # ROW 1: Structure (Flip / Skew / VRP)
            embed.add_field(name="🔄 FLIP (Front)", value=f"**{flip_price:.2f}**\n{flip_status}" if flip_price else "**N/A**", inline=True)
            embed.add_field(name="⚖️ SKEW (Front)", value=f"**{skew_ratio:.2f}x**\n{skew_status}", inline=True)
            embed.add_field(name="📉 VALUE (VRP)", value=f"**{vrp_spread:+.2f}%**\n{'SELL' if vrp_spread>0 else 'BUY'}", inline=True)
            
            # ROW 2: Intraday (Vig / PCR / Pain)
            embed.add_field(name="💰 VIG (0DTE)", value=f"**${vig_val:.2f}**\n{vig_lower:.0f}-{vig_upper:.0f}", inline=True)
            embed.add_field(name="📊 PCR (0DTE)", value=f"**{vol_pcr:.2f}**\n(Vol Ratio)", inline=True)
            embed.add_field(name="📌 PAIN (0DTE)", value=f"**{max_pain:.0f}**\n(Max Pain)", inline=True)

            embed.set_image(url="attachment://beeks_exposures.png")
            # NO THUMBNAIL SET HERE
            
            await ctx.interaction.edit_original_response(content="", embed=embed, file=file_exp)

        else:
            # BLOOMBERG TEXT MODE (Unchanged)
            lines = [f"> **{quote}**", "```yaml", f"CLARENCE BEEKS EXECUTIVE REPORT: {display_ticker}", "="*45]
            lines.append(f"SPOT: {spot:.2f}  |  {datetime.datetime.now().strftime('%H:%M')} ET")
            lines.append("-" * 45)
            lines.append(f"1. REGIME (TOTAL): {regime}")
            lines.append(f"   Net GEX: ${gex/1_000_000_000:.2f}B")
            lines.append("-" * 45)
            lines.append(f"2. STRUCTURE (FRONT MONTH)")
            lines.append(f"   Gamma Flip: {flip_price:.2f} ({flip_status})" if flip_price else "   Gamma Flip: N/A")
            lines.append(f"   Skew (25D): {skew_ratio:.2f}x ({skew_status})")
            lines.append("-" * 45)
            lines.append(f"3. INTRADAY (0DTE)")
            lines.append(f"   Vig (Cost): ${vig_val:.2f} [{vig_lower:.0f}-{vig_upper:.0f}]")
            lines.append(f"   Vol PCR:    {vol_pcr:.2f}")
            lines.append(f"   Max Pain:   {max_pain:.0f}")
            lines.append("-" * 45)
            lines.append(f"4. VALUE (VRP)")
            lines.append(f"   Premium:    {vrp_spread:+.2f}% ({'SELL' if vrp_spread>0 else 'BUY'})")
            lines.append("```")
            await ctx.interaction.edit_original_response(content="\n".join(lines))
    
    except Exception as e:
        # SAFETY WRAPPER END
        await ctx.interaction.edit_original_response(content=f"⚠️ **Beeks:** 'Report compilation failed: {e}'")

@dom_group.command(name="exposures", description="Total Dealer Exposure (GEX/DEX/VEX)")
async def dom_exposures(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True), 
    scope: Option(str, choices=["0DTE", "Front Month", "Total Market"], default="Total Market"), 
    target_expiry: Option(str, description="Override Scope (Format: YYYY-MM-DD)", required=False), # <--- FIXED
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False), # <--- FIXED
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
        calc_date = replay_date if replay_date else None; calc_tag = session
        if calc_date and not calc_tag: calc_tag = get_latest_tag_for_date(display_ticker, calc_date)
        
        # FETCH DATA (Includes Spot Price & Greeks now)
        raw_data = fetch_and_enrich_chain(
            ticker=ticker, 
            expiry_date=target_expiry, 
            snapshot_date=calc_date, 
            snapshot_tag=calc_tag, 
            scope=scope
        )
        
        if not raw_data: 
            await ctx.respond(f"❌ **Beeks:** 'Live Data Feed Is Currently Dark. Can you Try a Replay Date?'", ephemeral=True); return

        # CALCULATION
        spot_price = raw_data[0]['spot'] # Now safe
        gex, dex, vex = calculate_market_exposures(raw_data, spot_price)
        
        # VISUALIZATION
        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
        if target_expiry: label = f"EXP: {target_expiry}"
        elif replay_date: label = f"REPLAY: {replay_date} ({scope})"
        else: label = f"LIVE: {scope}"

        if view_setting == 'modern':
            img_buf = generate_exposure_dashboard(display_ticker, spot_price, gex, dex, vex, label, None)
            file = discord.File(img_buf, filename="beeks_exposures.png"); embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_exposures.png"); await ctx.respond(embed=embed, file=file, ephemeral=True)
        else:
            def fmt(v, s="B"): d=1_000_000_000 if s=="B" else 1_000_000; return f"${v/d:>7.2f} {s}"
            msg = f"> **{quote}**\n```yaml\n"; msg += f"+--------------------------------------------------+\n"; msg += f"| CLARENCE BEEKS TERMINAL           [EXPOSURE]     |\n"; msg += f"+--------------------------------------------------+\n"; msg += f"| TICKER: {display_ticker:<16} SPOT: {spot_price:<15.2f} |\n"; msg += f"| SCOPE:  {label:<32} |\n"; msg += f"+--------------------------------------------------+\n"; msg += f"| DEX (DELTA)   : {fmt(dex, 'B'):<12} Net Notional   |\n"; msg += f"| GEX (GAMMA)   : {fmt(gex, 'B'):<12} / 1% Move      |\n"; msg += f"| VEX (VANNA)   : {fmt(vex, 'M'):<12} / 1% IV Change |\n"; msg += f"+--------------------------------------------------+\n"; msg += f"| REGIME: {'DAMPENED VOL (Stable)' if gex > 0 else 'ACCELERATED VOL (Unstable)':<32} |\n"; msg += f"+--------------------------------------------------+\n```"; await ctx.respond(msg, ephemeral=True)
            
    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'Exposure Calculation Failed: {e}'", ephemeral=True)

@dom_group.command(name="flip", description="Find the Gamma Flip Level (Zero Gamma)")
async def dom_flip(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True), 
    target_expiry: Option(str, description="Override Scope (Format: YYYY-MM-DD)", required=False),
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
        calc_date = replay_date if replay_date else None; calc_tag = session
        if calc_date and not calc_tag: calc_tag = get_latest_tag_for_date(display_ticker, calc_date)
        
        # Scope is always Front Month for Flip
        scope_label = "Front Month" if not target_expiry else f"Exp: {target_expiry}"

        raw_data = fetch_and_enrich_chain(ticker, target_expiry, calc_date, calc_tag, scope="Front Month")
        if not raw_data: 
            await ctx.respond(f"❌ **Beeks:** 'Live Data Feed Is Currently Dark. Can you Try a Replay Date?'", ephemeral=True)
            return

        spot = raw_data[0]['spot']
        flip_level, sim_data = calculate_gamma_flip(raw_data, spot)
        
        status = "NEUTRAL"
        dist_str = "N/A"
        if flip_level:
            dist = ((spot - flip_level) / flip_level) * 100
            dist_str = f"{dist:+.1f}%"
            status = "POSITIVE GAMMA (Stable)" if spot > flip_level else "NEGATIVE GAMMA (Volatile)"

        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)

        if view_setting == 'modern' and sim_data:
            # IMAGE VIEW
            img_buf = generate_flip_chart(display_ticker, spot, flip_level, sim_data[0], sim_data[1], scope_label)
            file = discord.File(img_buf, filename="beeks_flip.png")
            embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_flip.png")
            await ctx.respond(embed=embed, file=file, ephemeral=True)
        else:
            # BLOOMBERG VIEW (Enhanced)
            msg = f"> **{quote}**\n```yaml\n"
            msg += f"[{display_ticker} GAMMA FLIP ANALYSIS]\n"
            msg += f"SCOPE : {scope_label}\n"
            msg += f"SPOT  : {spot:.2f}\n"
            msg += "-" * 30 + "\n"
            msg += f"FLIP  : {flip_level:.2f} ({dist_str})\n" if flip_level else "FLIP  : UNDEFINED\n"
            msg += f"MODE  : {status}\n"
            msg += f"```"
            await ctx.respond(msg, ephemeral=True)
            
    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'Flip Calculation Failed: {e}'", ephemeral=True)

@dom_group.command(name="vig", description="Calculate Intraday Expected Move (ATM Straddle)")
async def dom_vig(
    ctx, 
    ticker: Option(str), 
    target_expiry: Option(str, description="Target Event (Format: YYYY-MM-DD)", required=False),
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        scope_label = "0DTE (Intraday)" if not target_expiry else f"Exp: {target_expiry}"
        data = fetch_and_enrich_chain(ticker, target_expiry, replay_date, session, scope="0DTE") 
        
        # FIX: Explicit check for None or Empty Data
        if not data: 
            await ctx.respond("❌ **Beeks:** 'Data Dark.'", ephemeral=True)
            return
        
        spot = data[0]['spot']
        closest = min(data, key=lambda x: abs(x['strike'] - spot))
        
        atm_c = next((x for x in data if x['strike'] == closest['strike'] and x['type'] == 'Call'), None)
        atm_p = next((x for x in data if x['strike'] == closest['strike'] and x['type'] == 'Put'), None)
        
        # FIX: Ensure we found both legs of the straddle
        if not atm_c or not atm_p: 
            await ctx.respond("❌ **Beeks:** 'ATM Options Missing.'", ephemeral=True)
            return
        
        c_price = (atm_c['bid'] + atm_c['ask']) / 2 if atm_c['bid'] > 0 else atm_c['price']
        p_price = (atm_p['bid'] + atm_p['ask']) / 2 if atm_p['bid'] > 0 else atm_p['price']
        vig = c_price + p_price
        upper = closest['strike'] + vig; lower = closest['strike'] - vig
        
        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
        display_ticker = get_options_ticker(ticker)

        if view_setting == 'modern':
            # IMAGE VIEW
            img_buf = generate_vig_chart(display_ticker, spot, vig, upper, lower, scope_label)
            file = discord.File(img_buf, filename="beeks_vig.png")
            embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_vig.png")
            await ctx.respond(embed=embed, file=file, ephemeral=True)
        else:
            # BLOOMBERG VIEW (Enhanced)
            msg = f"> **{quote}**\n```yaml\n"
            msg += f"[{display_ticker} EXPECTED MOVE]\n"
            msg += f"SCOPE : {scope_label}\n"
            msg += f"SPOT  : {spot:.2f}\n"
            msg += "-" * 30 + "\n"
            msg += f"ATM   : {closest['strike']:.0f}\n"
            msg += f"COST  : ${vig:.2f}\n"
            msg += f"UPPER : {upper:.2f} (+{(upper/spot-1)*100:.2f}%)\n"
            msg += f"LOWER : {lower:.2f} ({(lower/spot-1)*100:.2f}%)\n```"
            await ctx.respond(msg, ephemeral=True)

    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'Vig Calculation Failed: {e}'", ephemeral=True)

@dom_group.command(name="skew", description="Put/Call Skew (Fear Gauge)")
async def dom_skew(
    ctx, 
    ticker: Option(str), 
    target_expiry: Option(str, description="Target Event (Format: YYYY-MM-DD)", required=False),
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        scope_label = "Front Month" if not target_expiry else f"Exp: {target_expiry}"
        data = fetch_and_enrich_chain(ticker, target_expiry, replay_date, session, scope="Front Month")
        if not data: 
            await ctx.respond("❌ **Beeks:** 'Data Dark.'", ephemeral=True)
            return
        
        spot = data[0]['spot']
        calls = [x for x in data if x['type'] == 'Call']; puts = [x for x in data if x['type'] == 'Put']
        
        # Check if lists are valid before processing
        if not calls or not puts:
            await ctx.respond("❌ **Beeks:** 'Missing call/put data.'", ephemeral=True)
            return

        c_25 = min(calls, key=lambda x: abs(x['delta'] - 0.25)) if calls else None
        p_25 = min(puts, key=lambda x: abs(abs(x['delta']) - 0.25)) if puts else None
        
        if not c_25 or not p_25: 
            await ctx.respond("❌ **Beeks:** 'Cannot find 25-Delta Wings.'", ephemeral=True)
            return
        
        ratio = p_25['iv'] / c_25['iv'] if c_25['iv'] > 0 else 0
        status = "BEARISH (High Fear)" if ratio > 1.1 else "BULLISH (Call Demand)" if ratio < 0.9 else "NEUTRAL"
        
        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
        display_ticker = get_options_ticker(ticker)

        if view_setting == 'modern':
            # IMAGE VIEW
            img_buf = generate_skew_chart(display_ticker, spot, c_25['iv'], p_25['iv'], ratio, scope_label)
            file = discord.File(img_buf, filename="beeks_skew.png")
            embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_skew.png")
            await ctx.respond(embed=embed, file=file, ephemeral=True)
        else:
            # BLOOMBERG VIEW (Enhanced)
            msg = f"> **{quote}**\n```yaml\n"
            msg += f"[{display_ticker} 25-DELTA SKEW]\n"
            msg += f"SCOPE : {scope_label}\n"
            msg += f"SPOT  : {spot:.2f}\n"
            msg += "-" * 30 + "\n"
            msg += f"P-IV  : {p_25['iv']:.1%} (Strike: {p_25['strike']})\n"
            msg += f"C-IV  : {c_25['iv']:.1%} (Strike: {c_25['strike']})\n"
            msg += f"RATIO : {ratio:.2f}x ({status})\n```"
            await ctx.respond(msg, ephemeral=True)
            
    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'Skew Calculation Failed: {e}'", ephemeral=True)

@dom_group.command(name="pcr", description="Put/Call Ratio & Max Pain")
async def dom_pcr(
    ctx, 
    ticker: Option(str), 
    target_expiry: Option(str, description="Target Event (Format: YYYY-MM-DD)", required=False),
    replay_date: Option(str, description="Historical Snapshot (Select from list)", autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    try: # <--- SAFETY WRAPPER START
        scope_label = "0DTE" if not target_expiry else f"EXP: {target_expiry}"
        data = fetch_and_enrich_chain(ticker, target_expiry, replay_date, session, scope="0DTE")
        
        # FIX: Explicit check for None or Empty data
        if not data: 
            await ctx.respond("❌ **Beeks:** 'Data Dark.'", ephemeral=True)
            return
        
        spot = data[0]['spot']
        c_vol = sum(x['volume'] for x in data if x['type'] == 'Call')
        p_vol = sum(x['volume'] for x in data if x['type'] == 'Put')
        c_oi = sum(x['oi'] for x in data if x['type'] == 'Call')
        p_oi = sum(x['oi'] for x in data if x['type'] == 'Put')
        
        vol_pcr = p_vol / c_vol if c_vol > 0 else 0
        oi_pcr = p_oi / c_oi if c_oi > 0 else 0
        
        # FIX: Use helper and handle None
        max_pain = calculate_max_pain(data)
        
        # NOTE: For the image generator, we pass 0 if None to prevent a crash in 'int()'.
        # To see "N/A" on the image, we must update 'generate_pcr_dashboard' next.
        mp_display = max_pain if max_pain is not None else 0
        
        view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
        display_ticker = get_options_ticker(ticker)

        if view_setting == 'modern':
            vol_data = {'calls': c_vol, 'puts': p_vol}; oi_data = {'calls': c_oi, 'puts': p_oi}
            img_buf = generate_pcr_dashboard(display_ticker, spot, vol_pcr, oi_pcr, vol_data, oi_data, scope_label, target_expiry, mp_display, c_vol+p_vol)
            file = discord.File(img_buf, filename="beeks_pcr.png")
            embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://beeks_pcr.png")
            await ctx.respond(embed=embed, file=file, ephemeral=True)
        else:
            # BLOOMBERG VIEW (Enhanced)
            msg = f"> **{quote}**\n```yaml\n"
            msg += f"[{display_ticker} FLOW ANALYSIS]\n"
            msg += f"SCOPE  : {scope_label}\n"
            msg += f"SPOT   : {spot:.2f}\n"
            msg += "-" * 30 + "\n"
            msg += f"VOL PCR: {vol_pcr:.2f} ({'BEARISH' if vol_pcr > 1 else 'BULLISH'})\n"
            msg += f"OI PCR : {oi_pcr:.2f}\n"
            
            # Use N/A for text mode since we control the string here
            mp_str = f"{max_pain:.0f}" if max_pain is not None else "N/A"
            msg += f"MAX PN : {mp_str}\n"
            
            msg += f"AGGRESS: {'PUTS' if p_vol > c_vol else 'CALLS'}\n```"
            await ctx.respond(msg, ephemeral=True)

    except Exception as e:
        await ctx.respond(f"⚠️ **Beeks:** 'PCR Calculation Failed: {e}'", ephemeral=True)

@dom_group.command(name="vrp", description="Volatility Risk Premium (Edge Meter)")
async def dom_vrp(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True)
):
    await ctx.defer(ephemeral=True)
    yf_sym = resolve_yf_symbol(ticker)
    
    # 1. Fetch Data
    # Note: Relies on the FIXED fetch_market_data() that returns None on failure (no DB fallback)
    data = fetch_market_data(yf_sym) 
    
    if not data: 
        await ctx.respond(f"❌ **Beeks:** 'Feed Dark. No price history for {ticker}.'", ephemeral=True)
        return

    iv = data['iv']
    hv = data['hv']
    
    # 2. VALIDATION CHECK
    # If IV is exactly equal to HV, the Option Chain fetch failed and defaulted to baseline.
    # We reject this data to prevent false "Neutral" signals.
    if abs(iv - hv) < 0.00001:
        await ctx.respond(f"⚠️ **Beeks:** 'Option Chain Unstable. Cannot calculate VRP.'\n*Reason: IV data missing, system defaulted to HV baseline.*", ephemeral=True)
        return

    spread = (iv - hv) * 100 # Percentage points
    ratio = iv / hv if hv != 0 else 0
    
    view_setting = get_user_terminal_setting(ctx.author.id)
    quote = random.choice(MOVIE_QUOTES)
    
    if view_setting == 'modern':
        img_buf = generate_vrp_gauge(ticker.upper(), iv, hv, spread, ratio)
        file = discord.File(img_buf, filename="beeks_vrp.png")
        embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31)
        embed.set_image(url="attachment://beeks_vrp.png")
        await ctx.respond(embed=embed, file=file, ephemeral=True)
    else:
        # Bloomberg View
        msg = f"**{ticker.upper()} RISK PREMIUM (VRP)**\n"
        msg += "```yaml\n"
        msg += f"IMPLIED VOL (IV):  {iv*100:.2f}%\n"
        msg += f"REALIZED VOL (HV): {hv*100:.2f}%\n"
        msg += "-"*30 + "\n"
        msg += f"SPREAD:            {spread:+.2f}%\n"
        msg += f"RATIO:             {ratio:.2f}x\n"
        msg += f"EDGE:              {'SELL PREMIUM' if spread > 0 else 'BUY OPTIONS'}\n```"
        await ctx.respond(msg, ephemeral=True)

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
        
        # MAPPING: Price Source -> Option Source
        targets = {"^GSPC": "^SPX"} 
        
        for price_sym, opt_sym in targets.items():
            try:
                # 1. Get Price from Index (^GSPC)
                price_tkr = yf.Ticker(price_sym)
                hist = price_tkr.history(period="1d")
                if hist.empty: 
                    print(f"   ⚠️ No price data for {price_sym}"); continue
                
                # 2. Get Chain from Option Ticker (^SPX)
                opt_tkr = yf.Ticker(opt_sym)
                if not opt_tkr.options: 
                    print(f"   ⚠️ No options data for {opt_sym}"); continue

                # 3. Validate Data Quality
                anchor_price = hist['Close'].iloc[-1]
                if not validate_atm_data(opt_tkr, anchor_price): 
                    print(f"   ⚠️ Skipping {opt_sym}: ATM Data Sparse"); continue

                # 4. Build Snapshot
                full_chain = {"symbol": opt_sym, "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"), "expirations": {}}
                exps = opt_tkr.options
                for e in exps:
                    try:
                        opt = opt_tkr.option_chain(e)
                        full_chain["expirations"][e] = {
                            "calls": opt.calls.to_dict(orient='records'), 
                            "puts": opt.puts.to_dict(orient='records')
                        }
                    except: pass
                
                save_snapshot(opt_sym, full_chain, anchor_price, get_current_yield(price_sym), tag=session_tag, custom_timestamp=now.strftime("%Y-%m-%d %H:%M:%S"))
                print(f"   ✅ Snapshot Saved: {opt_sym} [{session_tag}] @ {anchor_price:.2f}")

            except Exception as e: print(f"   ❌ Failed {opt_sym}: {e}")
    except Exception as e: print(f"❌ CRITICAL SCHEDULER ERROR: {e}")

@auto_fetch_heavy_chains.before_loop
async def before_scheduler(): await bot.wait_until_ready(); print("⏰ Scheduler armed (NY Time).")

@auto_fetch_heavy_chains.error
async def scheduler_error(error): print(f"💀 SCHEDULER CRASHED: {error}"); auto_fetch_heavy_chains.restart()

@bot.event
async def on_ready():
    init_db()
    
    # --- PROXY CHECK ---
    try:
        ip_resp = requests.get("https://httpbin.org/ip", timeout=5).json()
        print(f"🔒 PROXY CHECK: Bot is surfing as {ip_resp['origin']}")
    except:
        print("⚠️ PROXY CHECK FAILED: Could not reach external IP.")
    # -------------------

    if not auto_fetch_heavy_chains.is_running():
        auto_fetch_heavy_chains.start()
    
    print(f"🍊 Duke & Duke: Clarence Beeks is Online. Logged in as {bot.user}")
    await bot.sync_commands()

if TOKEN: bot.run(TOKEN)
else: bot.run("YOUR_TOKEN_HERE")