import asyncio
import os
import sys

# --- HEADLESS MODE FIX ---
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

load_dotenv()

# --- CONFIGURATION ---
guilds_env = os.getenv("GUILD_IDS", "")
guild_ids = [int(g.strip()) for g in guilds_env.split(",") if g.strip()] if guilds_env else None

CONFIG = {
    "DISCORD_TOKEN": os.getenv("DISCORD_TOKEN"),
    "GUILD_IDS": guild_ids,
    "PROJECT_ID": os.getenv("PROJECT_ID")
}

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

# --- DATABASE ENGINE ---
def init_db():
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    
    # Market Data Cache
    c.execute('''CREATE TABLE IF NOT EXISTS market_data
                 (ticker TEXT PRIMARY KEY, data_json TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    
    # Snapshots
    c.execute('''CREATE TABLE IF NOT EXISTS chain_snapshots
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  ticker TEXT, 
                  tag TEXT,
                  anchor_price REAL,
                  dividend_yield REAL,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, 
                  data_json TEXT)''')

    # ROBUST SETTINGS (Key-Value Store)
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
                 (user_id INTEGER, 
                  setting_key TEXT, 
                  setting_value TEXT, 
                  PRIMARY KEY (user_id, setting_key))''')

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
        print(f"DEBUG: 💾 Saved {ticker} to beeks.db (market_data)")
    except Exception as e:
        print(f"ERROR: Could not save to DB: {e}")

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
            print(f"DEBUG: 📂 Loaded {ticker} from beeks.db (Cache) [Time: {row[1]}]")
            return data
    except Exception as e:
        print(f"ERROR: Could not load from DB: {e}")
    return None

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
        c.execute("SELECT id FROM chain_snapshots WHERE ticker = ? AND tag = ? AND date(timestamp) = ?", 
                  (ticker, tag, date_part))
        
        if c.fetchone():
            conn.close()
            print(f"DEBUG: 🛑 Save Rejected. {ticker} [{tag}] already exists for {date_part}.")
            return False

        c.execute('INSERT INTO chain_snapshots (ticker, tag, anchor_price, dividend_yield, data_json, timestamp) VALUES (?, ?, ?, ?, ?, ?)', 
                  (ticker, tag, price, div_yield, json_str, custom_timestamp))
        conn.commit()
        conn.close()
        print(f"DEBUG: 📸 Snapshot saved for {ticker} [Price: {price}] [Yield: {div_yield:.2%}] [Tag: {tag}]")
        return True
        
    except Exception as e:
        print(f"ERROR: Could not save snapshot: {e}")
        return False

def load_snapshot(ticker):
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        query = "SELECT data_json, timestamp, anchor_price, dividend_yield FROM chain_snapshots WHERE ticker = ? ORDER BY id DESC LIMIT 1"
        c.execute(query, (ticker,))
        row = c.fetchone()
        conn.close()
        
        if row:
            data = json.loads(row[0])
            data['saved_at'] = row[1]
            if row[2]: data['anchor_price'] = row[2]
            data['dividend_yield'] = row[3] if row[3] is not None else data.get('dividend_yield', 0.0)
            
            print(f"DEBUG: 📂 Loaded Snapshot for {ticker} [Yield: {data['dividend_yield']:.2%}]")
            return data
    except Exception as e:
        print(f"ERROR: Could not load snapshot: {e}")
    return None

def get_latest_tag_for_date(ticker, date_str):
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        query = "SELECT tag FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? ORDER BY id DESC LIMIT 1"
        c.execute(query, (ticker, date_str))
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
    c.execute('INSERT OR REPLACE INTO user_settings (user_id, setting_key, setting_value) VALUES (?, ?, ?)', 
              (user_id, "terminal_global", value))
    conn.commit()
    conn.close()

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
    except:
        return 0.0

# --- MATH ENGINE ---
def fetch_market_data(ticker):
    yf_sym = resolve_yf_symbol(ticker)
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")
    
    print(f"\n--- DEBUG: Fetching Data for {ticker} ({yf_sym}) ---")

    try:
        tkr = yf.Ticker(yf_sym)
        hist = tkr.history(period="3mo") 
        if hist.empty: 
            print("ERROR: No history found.")
            return None 
        
        last_row = hist.iloc[-1]
        last_date = last_row.name.date()
        
        is_market_open = (last_date == today)
        anchor_price = last_row['Open'] if is_market_open else last_row['Close']
        mode = "OPEN" if is_market_open else "CLOSE"
        
        print(f"DEBUG: Pivot Mode: {mode} | Price: {anchor_price:.2f}")

        returns = np.log(hist['Close'] / hist['Close'].shift(1)).dropna()
        if len(returns) > 30: returns = returns.tail(30)
        
        daily_std = returns.std()
        hv_annual = daily_std * np.sqrt(252)
        
        if np.isnan(hv_annual) or hv_annual == 0: 
            print("DEBUG: ⚠️ HV failed (NaN or 0). Aborting.")
            return None 
        
        print(f"DEBUG: Annual HV: {hv_annual:.2%}")

        iv_annual = hv_annual 
        try:
            search_tkr = tkr
            if yf_sym == "^GSPC":
                try:
                    spx_test = yf.Ticker("^SPX")
                    if spx_test.options: search_tkr = spx_test
                except: pass

            try: opts_check = search_tkr.options
            except: opts_check = []
            if not opts_check and yf_sym in IV_PROXIES:
                search_tkr = yf.Ticker(IV_PROXIES[yf_sym])

            if not validate_atm_data(search_tkr, anchor_price):
                print(f"DEBUG: ATM Data Invalid for {yf_sym}. Aborting IV fetch.")
            else:
                all_exps = search_tkr.options
                valid_exps = [e for e in all_exps if (datetime.datetime.strptime(e, "%Y-%m-%d").date() - today).days >= 30]
                
                target_exp = None
                for exp in valid_exps:
                    if is_third_friday(exp):
                        target_exp = exp
                        break
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
                        if 0.01 < avg_iv < 5.0:
                            iv_annual = avg_iv
        except Exception as e: 
            print(f"DEBUG: Options fetch error: {e}")
            pass 

        packet = {
            "date": today_str,
            "ticker": yf_sym,
            "anchor_price": anchor_price,
            "iv": iv_annual,
            "hv": hv_annual,
            "date_obj": last_row.name # Store for TZ logic
        }
        
        save_to_db(yf_sym, packet)
        return packet
        
    except Exception as e: 
        print(f"ERROR: Fetch failed ({e}). Checking Database...")
        cached_packet = load_from_db(yf_sym)
        if cached_packet: return cached_packet
        return None

def fetch_historical_data(ticker, date_str, tag):
    db_ticker = get_options_ticker(ticker)
    print(f"--- DEBUG: Time Travel to {date_str} [{tag}] Ticker: {db_ticker} ---")
    try:
        conn = sqlite3.connect("beeks.db")
        c = conn.cursor()
        query = '''SELECT data_json FROM chain_snapshots 
                   WHERE ticker = ? AND date(timestamp) = ? AND tag = ? 
                   ORDER BY id DESC LIMIT 1'''
        c.execute(query, (db_ticker, date_str, tag))
        row = c.fetchone()
        conn.close()
        
        if not row: return None
        
        snapshot = json.loads(row[0])
        anchor_price = snapshot.get('anchor_price')
        
        if not anchor_price:
            print("ERROR: Snapshot exists but missing Anchor Price (Old Data).")
            return None

        target_exp = None
        snap_date = datetime.datetime.fromisoformat(snapshot['timestamp']).date()
        valid_exps = []
        for exp_str in snapshot['expirations']:
            d = datetime.datetime.strptime(exp_str, "%Y-%m-%d").date()
            if (d - snap_date).days >= 30:
                valid_exps.append(exp_str)
        
        valid_exps.sort()
        if valid_exps: target_exp = valid_exps[0]
        
        iv_annual = 0.15 
        
        if target_exp:
            exp_data = snapshot['expirations'][target_exp]
            calls = pd.DataFrame(exp_data['calls'])
            puts = pd.DataFrame(exp_data['puts'])
            calls['abs_diff'] = abs(calls['strike'] - anchor_price)
            puts['abs_diff'] = abs(puts['strike'] - anchor_price)
            all_opts = pd.concat([calls, puts]).sort_values('abs_diff')
            valid_opts = all_opts[all_opts['impliedVolatility'] > 0].head(4)
            if not valid_opts.empty:
                iv_annual = valid_opts['impliedVolatility'].mean()

        hv_annual = 0.15
        try:
            tkr = yf.Ticker(ticker) 
            hist = tkr.history(start=snap_date - timedelta(days=60), end=snap_date)
            returns = np.log(hist['Close'] / hist['Close'].shift(1)).dropna().tail(30)
            hv_annual = returns.std() * np.sqrt(252)
        except: pass

        return {
            "date": date_str,
            "ticker": ticker,
            "anchor_price": anchor_price,
            "iv": iv_annual,
            "hv": hv_annual,
            "saved_at": snapshot['timestamp'],
            "is_backtest": True
        }
    except Exception as e:
        print(f"ERROR: Time Travel Failed: {e}")
        return None

def fetch_and_enrich_chain(ticker, expiry_date, snapshot_date=None, snapshot_tag=None, target_strike=None, range_count=None, pivot=None, scope="Front Month"):
    yf_sym = resolve_yf_symbol(ticker)
    display_ticker = get_options_ticker(yf_sym)
    
    S = 0.0; q = 0.0; T = 0.0; r = 0.045
    chain_data = {'calls': [], 'puts': []}
    
    # --- SNAPSHOT / 0DTE LOGIC ---
    if snapshot_date:
        tag = snapshot_tag if snapshot_tag else "CLOSE"
        try:
            conn = sqlite3.connect("beeks.db")
            c = conn.cursor()
            c.execute("SELECT data_json, timestamp, anchor_price, dividend_yield FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ? AND tag = ? ORDER BY id DESC LIMIT 1", 
                      (display_ticker, snapshot_date, tag))
            row = c.fetchone()
            conn.close()
            
            if not row: return None
            data = json.loads(row[0])
            
            # Timestamp handling
            try: snap_ts = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            except: snap_ts = datetime.datetime.fromisoformat(row[1])
            
            S = row[2] if row[2] else data.get('anchor_price', 0)
            q = row[3] if row[3] is not None else data.get('dividend_yield', 0.0)
            
            # --- SCOPE HANDLING FOR SNAPSHOTS ---
            target_exps = []
            
            if scope == "0DTE":
                if expiry_date and expiry_date in data['expirations']:
                    target_exps = [expiry_date]
                else:
                    snap_date_str = snap_ts.strftime("%Y-%m-%d")
                    if snap_date_str in data['expirations']:
                        target_exps = [snap_date_str]
                    else:
                        print(f"DEBUG: 0DTE Target {expiry_date} not found in snapshot.")
                        return None

            elif scope == "Front Month":
                target_exps = [e for e in data['expirations'] if 0 <= (datetime.datetime.strptime(e, "%Y-%m-%d") - snap_ts).days <= 30]
            
            elif scope == "Total Market":
                target_exps = list(data['expirations'].keys())
            
            elif scope == "Specific":
                 if expiry_date in data['expirations']: target_exps = [expiry_date]
            
            else:
                if expiry_date in data['expirations']: target_exps = [expiry_date]
            
            # Aggregate Data
            for e in target_exps:
                exp_dt = datetime.datetime.strptime(e, "%Y-%m-%d")
                t_val = (exp_dt - snap_ts).days / 365.0
                if t_val < 0.001: t_val = 0.001 
                
                c_list = data['expirations'][e]['calls']
                p_list = data['expirations'][e]['puts']
                for x in c_list: x['time_year'] = t_val
                for x in p_list: x['time_year'] = t_val
                
                chain_data['calls'].extend(c_list)
                chain_data['puts'].extend(p_list)

        except: return None
        
    # --- LIVE DATA LOGIC ---
    else:
        try:
            tkr = yf.Ticker(yf_sym)
            hist = tkr.history(period="1d")
            if hist.empty: return None
            S = hist['Close'].iloc[-1]
            q = get_current_yield(yf_sym)
            
            search_tkr = tkr
            if yf_sym == "^GSPC":
                 try: 
                     spx = yf.Ticker("^SPX")
                     if spx.options: search_tkr = spx
                 except: pass

            all_exps = search_tkr.options
            if not all_exps: return None
            
            target_exps = []
            now = datetime.datetime.now()
            
            if scope == "0DTE":
                today_str = now.strftime("%Y-%m-%d")
                if today_str in all_exps: target_exps = [today_str]
                else: return [] # No 0DTE today
                
            elif scope == "Total Market":
                target_exps = all_exps 
                
            elif scope == "Front Month":
                target_exps = [e for e in all_exps if 0 <= (datetime.datetime.strptime(e, "%Y-%m-%d") - now).days <= 35]
            
            elif scope == "Specific":
                 if expiry_date in all_exps: target_exps = [expiry_date]
            
            else:
                if expiry_date in all_exps: target_exps = [expiry_date]

            # Fetch Loop
            raw_chain = {"expirations": {}, "symbol": yf_sym, "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")}
            
            for e in target_exps:
                try:
                    opt = search_tkr.option_chain(e)
                    c_list = opt.calls.to_dict(orient='records')
                    p_list = opt.puts.to_dict(orient='records')
                    
                    raw_chain["expirations"][e] = {"calls": c_list, "puts": p_list}
                    
                    # Calc Time
                    exp_dt = datetime.datetime.strptime(e, "%Y-%m-%d")
                    t_val = (exp_dt - now).days / 365.0
                    if t_val < 0.001: t_val = 0.001
                    
                    for x in c_list: x['time_year'] = t_val
                    for x in p_list: x['time_year'] = t_val
                    
                    chain_data['calls'].extend(c_list)
                    chain_data['puts'].extend(p_list)
                except: pass
            
            if scope == "Total Market" and len(target_exps) > 5:
                ny_time = datetime.datetime.now(ZoneInfo("America/New_York"))
                user_tag = f"USER_{ny_time.strftime('%H%M')}"
                save_snapshot(display_ticker, raw_chain, S, q, tag=user_tag)

        except Exception as e: 
            print(f"Fetch Error: {e}")
            return None

    all_options = []
    for c in chain_data['calls']:
        c['type'] = 'Call'; all_options.append(c)
    for p in chain_data['puts']:
        p['type'] = 'Put'; all_options.append(p)
    
    df = pd.DataFrame(all_options)
    if df.empty: return []
    
    df.rename(columns={
        'impliedVolatility': 'iv', 
        'openInterest': 'oi', 
        'volume': 'vol', 
        'lastPrice': 'price'
    }, inplace=True, errors='ignore')

    if target_strike:
        df = df[df['strike'] == target_strike]
    elif range_count:
        anchor = pivot if pivot else S
        unique_strikes = df['strike'].unique()
        dists = pd.DataFrame({'strike': unique_strikes, 'dist': abs(unique_strikes - anchor)})
        keep_strikes = dists.sort_values('dist').head(range_count)['strike'].values
        df = df[df['strike'].isin(keep_strikes)]

    results = []
    
    if range_count is not None:
         for _, row in df.iterrows():
            K = row['strike']
            IV = row['iv']
            if not IV or IV < 0.001: continue
            
            delta, gamma, theta, vanna, charm = calculate_black_scholes(S, K, row['time_year'], r, IV, q, row['type'].lower())
            
            item = {
                'strike': K, 'type': row['type'], 'price': row.get('price', 0),
                'bid': row.get('bid', 0), 'ask': row.get('ask', 0),
                'volume': row.get('vol', 0), 'oi': row.get('oi', 0),
                'iv': IV, 
                'delta': delta, 'gamma': gamma, 'theta': theta, 
                'vanna': vanna, 'charm': charm,
                'time_year': row.get('time_year', 0), # <--- THIS WAS MISSING
                'spot': S
            }
            results.append(item)
         return sorted(results, key=lambda x: x['strike'])
    else:
        return df.to_dict(orient='records')

def calculate_levels(price, iv, hv, engine="Insider Info"):
    is_insider = engine.strip().lower() == "insider info"
    divisor = np.sqrt(365) if is_insider else np.sqrt(252)
    
    move_iv = price * (iv / divisor)
    move_hv = price * (hv / divisor)
    
    levels = {
        "meta": {"engine": engine, "daily_move_iv": move_iv, "daily_move_hv": move_hv},
        "valentine": {},
        "winthorpe": {}
    }
    
    for i in range(1, 5):
        levels["valentine"][f"+{i}σ"] = price + (move_iv * i)
        levels["valentine"][f"-{i}σ"] = price - (move_iv * i)
        levels["winthorpe"][f"+{i}σ"] = price + (move_hv * i)
        levels["winthorpe"][f"-{i}σ"] = price - (move_hv * i)
        
    return levels

def calculate_atm_strike(price):
    return round(price / 5) * 5

def calculate_black_scholes(S, K, T, r, sigma, q=0.0, option_type="call"):
    try:
        S = np.array(S, dtype=float)
        K = np.array(K, dtype=float)
        T = np.array(T, dtype=float)
        sigma = np.array(sigma, dtype=float)
        
        T = np.maximum(T, 1e-5)
        
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        N_d1 = si.norm.cdf(d1)
        N_d2 = si.norm.cdf(d2)
        pdf_d1 = si.norm.pdf(d1)
        
        gamma = (pdf_d1 * np.exp(-q * T)) / (S * sigma * np.sqrt(T))
        vanna = -np.exp(-q * T) * pdf_d1 * (d2 / sigma)
        
        if option_type == "call":
            delta = np.exp(-q * T) * N_d1
            theta = (- (S * sigma * np.exp(-q * T) * pdf_d1) / (2 * np.sqrt(T)) 
                     - r * K * np.exp(-r * T) * N_d2 
                     + q * S * np.exp(-q * T) * N_d1) / 365.0
            
            term1 = q * np.exp(-q * T) * N_d1
            term2 = np.exp(-q * T) * pdf_d1 * (2 * (r - q) * T - d2 * sigma * np.sqrt(T)) / (2 * T * sigma * np.sqrt(T))
            charm = (term1 - term2) / 365.0
            
        else:
            delta = np.exp(-q * T) * (N_d1 - 1)
            theta = (- (S * sigma * np.exp(-q * T) * pdf_d1) / (2 * np.sqrt(T)) 
                     + r * K * np.exp(-r * T) * (1 - N_d2) 
                     - q * S * np.exp(-q * T) * (1 - N_d1)) / 365.0
            
            term1 = -q * np.exp(-q * T) * (1 - N_d1)
            term2 = np.exp(-q * T) * pdf_d1 * (2 * (r - q) * T - d2 * sigma * np.sqrt(T)) / (2 * T * sigma * np.sqrt(T))
            charm = (term1 - term2) / 365.0

        if np.ndim(delta) == 0:
            return float(delta), float(gamma), float(theta), float(vanna), float(charm)
            
        return delta, gamma, theta, vanna, charm
    except:
        return 0.0, 0.0, 0.0, 0.0, 0.0

def calculate_gamma_flip(chain_data, current_spot, r=0.045, q=0.0):
    if not chain_data: return None, None
    
    df = pd.DataFrame(chain_data)
    if df.empty: return None, None
    
    df = df[(df['iv'] > 0.001) & (df['oi'] > 0)]
    if df.empty: return None, None
    
    strikes = df['strike'].values
    ivs = df['iv'].values
    Ts = df.get('time_year', pd.Series([0.1]*len(df))).values 
    is_call = (df['type'] == 'Call').values
    ois = df['oi'].values
    
    sim_spots = np.linspace(current_spot * 0.93, current_spot * 1.07, 100)
    net_gammas = []
    
    for sim_S in sim_spots:
        _, gammas, _, _, _ = calculate_black_scholes(sim_S, strikes, Ts, r, ivs, q, "call") 
        call_gex = np.sum(gammas[is_call] * ois[is_call] * 100)
        put_gex = np.sum(gammas[~is_call] * ois[~is_call] * 100 * -1)
        net_gammas.append(call_gex + put_gex)
        
    net_gammas = np.array(net_gammas)
    signs = np.sign(net_gammas)
    diffs = np.diff(signs)
    cross_indices = np.where(diffs != 0)[0]
    
    flip_price = None
    if len(cross_indices) > 0:
        idx = cross_indices[np.abs(sim_spots[cross_indices] - current_spot).argmin()]
        y1, y2 = net_gammas[idx], net_gammas[idx+1]
        x1, x2 = sim_spots[idx], sim_spots[idx+1]
        if y2 != y1:
            flip_price = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
            
    return flip_price, (sim_spots, net_gammas)

def calculate_market_exposures(chain_data, spot_price):
    total_gex = 0.0; total_dex = 0.0; total_vex = 0.0; total_cex = 0.0
    
    for opt in chain_data:
        delta = opt['delta']
        gamma = opt['gamma']
        vanna = opt.get('vanna', 0.0)
        charm = opt.get('charm', 0.0)
        oi = opt['oi']
        if pd.isna(oi) or oi <= 0: continue
        
        is_call = opt['type'].lower() == 'call'
        
        # Dealer Position is assumed SHORT (Counterparty)
        # GEX: Gamma * OI * Spot^2 * 0.01
        contract_gex = (gamma * oi * 100) * (spot_price**2) * 0.01
        if not is_call: contract_gex *= -1
        total_gex += contract_gex
        
        # DEX: Delta * OI * Spot
        contract_dex = (delta * oi * 100 * spot_price)
        if is_call: contract_dex *= -1 
        else: contract_dex *= 1 
        total_dex += contract_dex

        # VEX: Vanna * OI * Spot
        contract_vex = (vanna * 0.01) * oi * 100 * spot_price
        if is_call: contract_vex *= -1 
        else: contract_vex *= 1 
        total_vex += contract_vex

        # CEX: Charm * OI * Spot (Delta Decay per Day)
        # Charm is change in Delta per change in Time.
        # Dealer Short Call (-1 * NegCharm) = Pos CEX
        # Dealer Short Put (-1 * PosCharm) = Neg CEX
        contract_cex = (charm * oi * 100 * spot_price)
        if is_call: contract_cex *= -1
        else: contract_cex *= 1
        total_cex += contract_cex

    return total_gex, total_dex, total_vex, total_cex

def calculate_strike_exposures(chain_data, spot_price, ticker):
    strikes = {}
    if ticker in ["^SPX", "^GSPC", "SPX", "ES=F"]: r_pts = 125 
    elif ticker in ["^NDX", "NDX", "NQ=F"]: r_pts = 250
    else: r_pts = spot_price * 0.10 
        
    min_strike = spot_price - r_pts
    max_strike = spot_price + r_pts
    
    for opt in chain_data:
        k = opt['strike']
        if k < min_strike or k > max_strike: continue
        if k not in strikes: strikes[k] = {'gex': 0.0, 'dex': 0.0, 'vex': 0.0}
        
        gamma = opt['gamma']; delta = opt['delta']; vanna = opt.get('vanna', 0.0); oi = opt['oi']
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
        
    sorted_strikes = sorted(strikes.keys())
    return {
        'strikes': sorted_strikes,
        'gex': [strikes[k]['gex'] for k in sorted_strikes],
        'dex': [strikes[k]['dex'] for k in sorted_strikes],
        'vex': [strikes[k]['vex'] for k in sorted_strikes]
    }

# --- PLOTTING ---
def create_beeks_chart(display_ticker, data, levels, view_mode="Insider Info"):
    fig = plt.figure(figsize=(12, 8)) 
    plt.style.use('dark_background')
    plt.subplots_adjust(left=0.02, right=0.98, top=0.88, bottom=0.02)
    
    price = data['anchor_price']
    ax = plt.gca()
    trans = ax.get_yaxis_transform()
    
    all_values = [price]
    
    for label, val in levels['winthorpe'].items():
        all_values.append(val)
        plt.axhline(val, color='orange', linestyle=':', alpha=0.7, linewidth=1.5)
        plt.text(0.01, val, f"{label}: {val:.2f}", color='orange', fontsize=9, va='bottom', fontweight='bold', transform=trans)
    
    for label, val in levels['valentine'].items():
        all_values.append(val)
        plt.axhline(val, color='cyan', linestyle='-', alpha=0.7, linewidth=1.5)
        plt.text(0.99, val, f"{label}: {val:.2f}", color='cyan', fontsize=9, va='bottom', ha='right', fontweight='bold', transform=trans)
        
    plt.axhline(price, color='white', linewidth=2, linestyle='--')
    plt.text(0.5, price, f"{price:.2f}", color='white', fontsize=10, fontweight='bold', va='bottom', ha='center', transform=trans)
    
    y_min = min(all_values); y_max = max(all_values)
    diff = y_max - y_min
    if diff < 1.0: diff = price * 0.01 
    buffer = diff * 0.10
    plt.ylim(y_min - buffer, y_max + buffer)
    
    plt.title(f"Clarence Beeks Report: {display_ticker}\nEngine: {view_mode}", color='white', fontsize=14, weight='bold')
    plt.grid(False)
    ax.tick_params(axis='y', colors='white', labelsize=10)
    ax.tick_params(axis='x', colors='white', bottom=False, labelbottom=False) 
    
    from matplotlib.lines import Line2D
    custom_lines = [Line2D([0], [0], color='cyan', lw=2), Line2D([0], [0], color='orange', lw=2, linestyle=':')]
    plt.legend(custom_lines, ['VALENTINE', 'WINTHORPE'], loc='upper left', facecolor='black', labelcolor='white', framealpha=0.6)
    
    buf = io.BytesIO()
    fig.savefig(buf, format='png', facecolor='black', dpi=120)
    buf.seek(0); plt.close()
    return buf

def generate_exposure_dashboard(ticker, spot, gex, dex, vex, cex, scope_label, expiry_label):
    plt.figure(figsize=(12, 8))
    plt.style.use('dark_background')
    
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.axis('off')
    fig.patch.set_facecolor('#1e1e1e')
    
    def fmt_val(val, suffix="B"):
        d = 1_000_000_000 if suffix == "B" else 1_000_000
        return f"${val/d:+.2f} {suffix}"

    # Colors
    c_gex = '#00ff00' if gex > 0 else '#ff0000'
    c_dex = '#00ffff' if dex > 0 else '#ff00ff'
    c_vex = '#ffff00' if vex > 0 else '#ff8800'
    c_cex = '#ff99cc' if cex > 0 else '#99ccff'

    # Layout: 2x2 Grid
    # Top Left: GEX
    plt.text(0.25, 0.85, "GAMMA (GEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.25, 0.75, fmt_val(gex, "B"), color=c_gex, fontsize=22, ha='center', weight='bold')
    plt.text(0.25, 0.70, "per 1% Move", color='#888888', fontsize=10, ha='center')

    # Top Right: DEX
    plt.text(0.75, 0.85, "DELTA (DEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.75, 0.75, fmt_val(dex, "B"), color=c_dex, fontsize=22, ha='center', weight='bold')
    plt.text(0.75, 0.70, "Net Notional", color='#888888', fontsize=10, ha='center')

    # Bottom Left: VEX
    plt.text(0.25, 0.45, "VANNA (VEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.25, 0.35, fmt_val(vex, "M"), color=c_vex, fontsize=22, ha='center', weight='bold')
    plt.text(0.25, 0.30, "per 1% IV Chg", color='#888888', fontsize=10, ha='center')

    # Bottom Right: CEX (New)
    plt.text(0.75, 0.45, "CHARM (CEX)", color='white', fontsize=14, ha='center', weight='bold')
    plt.text(0.75, 0.35, fmt_val(cex, "M"), color=c_cex, fontsize=22, ha='center', weight='bold')
    plt.text(0.75, 0.30, "Delta Decay / Day", color='#888888', fontsize=10, ha='center')
    
    # Dividers
    plt.vlines(x=0.5, ymin=0.2, ymax=0.9, colors='#333333', linewidth=2)
    plt.hlines(y=0.55, xmin=0.1, xmax=0.9, colors='#333333', linewidth=2)
    
    # Footer
    info_text = f"{ticker} @ {spot:.2f}  |  {scope_label}"
    if expiry_label: info_text += f"  |  EXP: {expiry_label}"
    plt.text(0.5, 0.05, info_text, color='white', fontsize=11, ha='center', style='italic')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e')
    buf.seek(0); plt.close()
    return buf

def generate_strike_chart(ticker, spot, data, metric="ALL", confluence_data=None):
    plt.style.use('dark_background')
    
    if metric == "CONFLUENCE":
        if not confluence_data: return None
        active_levels = [x for x in confluence_data if x['score'] > 0]
        if not active_levels: return None
        active_levels.sort(key=lambda x: x['strike'])
        
        c_strikes = [x['strike'] for x in active_levels]
        c_scores = [x['score'] for x in active_levels]
        c_tags = [x['tags'] for x in active_levels]
        
        fig, ax = plt.subplots(figsize=(10, 8))
        fig.patch.set_facecolor('#1e1e1e')
        ax.set_facecolor('#1e1e1e')
        
        colors = []
        for s in c_scores:
            if s >= 3: colors.append('#FFD700')
            elif s == 2: colors.append('#00FFFF')
            else: colors.append('#555555')
            
        bars = ax.barh(c_strikes, c_scores, color=colors, height=(c_strikes[1]-c_strikes[0])*0.8 if len(c_strikes)>1 else 2, alpha=0.8)
        ax.axhline(spot, color='white', linestyle='--', linewidth=2, label=f'Spot: {spot:.2f}')
        
        for bar, tag, score in zip(bars, c_tags, c_scores):
            width = bar.get_width()
            ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                    f" {tag}", va='center', color='white', fontsize=10, fontweight='bold')

        ax.set_xlabel("Confluence Score (Overlap)", color='white')
        ax.set_ylabel("Strike Price", color='white')
        ax.set_title(f"{ticker} Structural Confluence Profile", color='white', weight='bold', fontsize=14)
        ax.set_xlim(0, 4.5); ax.set_xticks([0, 1, 2, 3])
        ax.set_xticklabels(['', '1 (Weak)', '2 (Strong)', '3 (Major)'])
        ax.grid(True, axis='x', alpha=0.15)
        
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor='#FFD700', label='Triple (G+D+V)'), Patch(facecolor='#00FFFF', label='Double'), Patch(facecolor='#555555', label='Single')]
        ax.legend(handles=legend_elements, loc='lower right')

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e')
        buf.seek(0); plt.close()
        return buf

    strikes = np.array(data['strikes'])
    if metric == "ALL":
        plot_metrics = ["GEX", "DEX", "VEX"]
        fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
        plt.subplots_adjust(hspace=0.3)
    else:
        plot_metrics = [metric]
        fig, axes = plt.subplots(1, 1, figsize=(12, 6))
        axes = [axes] 
        
    fig.patch.set_facecolor('#1e1e1e')
    key_strikes = {}
    if confluence_data:
        for item in confluence_data:
            if item['score'] >= 2: key_strikes[item['strike']] = item['score']

    for i, m in enumerate(plot_metrics):
        ax = axes[i]
        vals = np.array(data[m.lower()])
        colors = ['#00ff00' if v >= 0 else '#ff0000' for v in vals]
        ax.bar(strikes, vals, color=colors, width=(strikes[1]-strikes[0])*0.8 if len(strikes)>1 else 1, alpha=0.8)
        
        if key_strikes:
            for strike_val in key_strikes:
                ax.axvline(strike_val, color='#FFD700', linestyle=':', alpha=0.4, linewidth=1)
                if i == 0: ax.text(strike_val, ax.get_ylim()[1]*0.95, "★", color='#FFD700', fontsize=12, ha='center')

        ax.axhline(0, color='white', linewidth=1.5)
        if len(vals) > 0:
            max_mag = max(abs(np.min(vals)), abs(np.max(vals)))
            ax.set_ylim(-max_mag*1.15, max_mag*1.15)
        
        ax.axvline(spot, color='white', linestyle='--', linewidth=2, label=f'Spot: {spot:.2f}')
        ax.set_ylabel(f"Net {m} ($)", color='white', fontsize=12)
        ax.set_title(f"Net Dealer {m} by Strike", color='white', weight='bold')
        ax.grid(True, alpha=0.15)
        ax.set_facecolor('#1e1e1e')
        
        def human_format(num, pos):
            magnitude = 0
            while abs(num) >= 1000:
                magnitude += 1
                num /= 1000.0
            return '%.1f%s' % (num, ['', 'K', 'M', 'B'][magnitude])
        ax.yaxis.set_major_formatter(plt.FuncFormatter(human_format))

    plt.xlabel("Strike Price", color='white', fontsize=12)
    plt.suptitle(f"{ticker} Exposure Profile", color='white', fontsize=16, weight='bold', y=0.95)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e')
    buf.seek(0); plt.close()
    return buf

# --- DISCORD INIT (PYTHON 3.14 FIX APPLIED) ---
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

bot = discord.Bot(debug_guilds=CONFIG["GUILD_IDS"] if CONFIG["GUILD_IDS"] else None)

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

def is_third_friday(date_str):
    d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    return d.weekday() == 4 and 15 <= d.day <= 21

# --- COMMAND HELPERS ---
async def get_db_dates(ctx: discord.AutocompleteContext):
    ticker_input = ctx.options.get("ticker", "^SPX")
    yf_sym = resolve_yf_symbol(ticker_input)
    db_ticker = get_options_ticker(yf_sym)
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    c.execute("SELECT DISTINCT date(timestamp) FROM chain_snapshots WHERE ticker = ? ORDER BY timestamp DESC LIMIT 25", (db_ticker,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

async def get_db_tags(ctx: discord.AutocompleteContext):
    selected_date = ctx.options.get("replay_date") or ctx.options.get("snapshot_date")
    if not selected_date: return ["⬅️ Select a DATE first"]
    
    ticker_input = ctx.options.get("ticker", "^SPX")
    yf_sym = resolve_yf_symbol(ticker_input)
    db_ticker = get_options_ticker(yf_sym)
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    c.execute("SELECT DISTINCT tag FROM chain_snapshots WHERE ticker = ? AND date(timestamp) = ?", (db_ticker, selected_date))
    rows = c.fetchall()
    conn.close()
    tags = [r[0] for r in rows]
    return tags if tags else ["❌ No sessions found"]

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

# --- COMMANDS ---
beeks = bot.create_group("beeks", "Official Dukes Bros. Fixer")

@beeks.command(name="help", description="Learn how to use Beeks")
async def beeks_help(
    ctx: discord.ApplicationContext,
    topic: Option(str, name="topic", choices=["setmode", "chain", "dailyrange", "gammaflip", "strikes", "exposures", "vig", "skew"])
):
    if topic == "chain":
        embed = discord.Embed(
            title="📘 Beeks Manual: The Chain",
            description="**Raw Data Feed (DOM Style)**\nView the raw Option Chain data. Unlike Yahoo Finance, we sort High Strikes at the top (Standard DOM view) so it matches your trading platform.",
            color=0xFFFFFF
        )
        embed.add_field(name="Features", value="**Backtesting:** You can request a chain from any date in the database.\n**Live Feed:** Real-time data from the floor (Yahoo).\n**The Greeks:** Full breakdown of Delta, Gamma, Theta, and IV per strike.", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "dailyrange":
        embed = discord.Embed(
            title="📘 Beeks Manual: Daily Range",
            description="**Generate Intraday Volatility Bands**\n\nWe don't guess where price is going. We calculate where it *should* stop.",
            color=0x00FFFF
        )
        embed.add_field(name="The Levels", value="**🟦 Valentine:** Expected Range (IV).\n**🟧 Winthorpe:** Hist. Deviation (HV).\n**⬜ Reference:** Open or Prev Close.", inline=False)
        embed.add_field(name="Timezone Logic", value="Strictly **NY Time (ET)**.\n• **00:00 - Open:** Uses Prev Close.\n• **Open - 23:59:** Uses Daily Open.", inline=False)
        embed.set_footer(text="💡 Pro Tip: Run at 9:31 AM ET to lock daily levels.")
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "exposures":
        embed = discord.Embed(
            title="📘 Beeks Manual: Exposures",
            description="**Dealer Positioning (GEX / DEX / VEX / CEX)**\nShows the total dollar exposure of Market Makers. This reveals how dealers are positioned and how they must hedge as price, vol, or time moves.",
            color=0x00FF00
        )
        embed.add_field(name="Metrics", value="**GEX (Gamma):** Market Stability. (+Sticky / -Volatile)\n**DEX (Delta):** Net Directional Risk.\n**VEX (Vanna):** Gas Pedal (Sensitivity to IV).\n**CEX (Charm):** 2:00 PM Flush (Delta Decay per Day).", inline=False)
        embed.add_field(name="Scopes", value="**Front Month:** Next ~30 days (Standard).\n**0DTE:** Expires Today (Scalping).\n**Total Market:** All expirations.", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "gammaflip":
        embed = discord.Embed(
            title="📘 Beeks Manual: Gamma Flip",
            description="**Zero Gamma Level**\nThe theoretical price level where Dealers flip from Long Gamma (Stable) to Short Gamma (Volatile).",
            color=0xFF00FF
        )
        embed.add_field(name="Interpretation", value="**Above Flip:** Market tends to be stable/mean-reverting.\n**Below Flip:** Market tends to be volatile/directional.\n**The Flip:** Often acts as major magnetic support/resistance.", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "setmode":
        embed = discord.Embed(
            title="📘 Beeks Manual: Set Mode",
            description="**Global Terminal View**\nConfigure how the bot delivers data to you.",
            color=0xCCCCCC
        )
        embed.add_field(name="Options", value="**Modern:** Generates visual charts and PNG dashboards. (Best for Desktop)\n**Bloomberg:** Returns raw text and ASCII tables. (Best for Mobile/Low Data)", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "strikes":
        embed = discord.Embed(
            title="📘 Beeks Manual: Strike Exposure",
            description="**Structural Level Analysis**\nDissects the option chain to find 'Structural Pins'—specific prices where dealers have massive positions and may be forced to hedge aggressively.",
            color=0xFFA500
        )
        embed.add_field(name="View Modes (Metrics)", value="**CONFLUENCE (Best):** Finds 'Triple Threat' levels where Gamma, Delta, and Vanna overlap. High scores (⭐⭐⭐) often act as major magnet levels.\n**ALL:** Generates a 3-panel chart showing net GEX, DEX, and VEX profiles.\n**Single Metrics:** Isolates specific exposures.", inline=False)
        embed.add_field(name="Strategy", value="**High GEX:** Liquidity magnets (price sticks).\n**High DEX:** Hedging walls (hard to break).\n**Confluence:** The strongest structural support/resistance on the board.", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "vig":
        embed = discord.Embed(
            title="📘 Beeks Manual: The Vig",
            description="**Expected Move (ATM Straddle)**\nCalculates the cost of the At-The-Money Straddle. This is the 'Vig' (fee) Market Makers are charging to play the game.",
            color=0xFFFF00
        )
        embed.add_field(name="How to use", value="**Cost:** This is the breakeven. If SPX moves more than this $ amount, Dealers are losing money and will fight back.\n**Breakevens:** The upper and lower bounds where the 'House' starts losing.", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

    elif topic == "skew":
        embed = discord.Embed(
            title="📘 Beeks Manual: Skew",
            description="**Sentiment Detector (Put/Call Ratio)**\nCompares the cost of OTM Puts (Downside Protection) vs OTM Calls (Upside FOMO).",
            color=0xFF99CC
        )
        embed.add_field(name="Interpretation", value="**Ratio > 1.2 (Bearish):** Puts are expensive. Everyone is hedging. Paradoxically, this can lead to a 'Squeeze' higher.\n**Ratio < 0.8 (Bullish):** Calls are expensive. Everyone is FOMOing. Watch out for a rug pull.", inline=False)
        await ctx.respond(embed=embed, ephemeral=True)

@beeks.command(name="setmode", description="Configure Global Terminal View Settings")
async def beeks_setmode(ctx: discord.ApplicationContext, terminal: Option(str, choices=["Modern", "Bloomberg"], required=True)):
    set_user_terminal_setting(ctx.author.id, terminal.lower())
    await ctx.respond(f"🌎 **Global Terminal View** set to: **{terminal.upper()}**", ephemeral=True)

@beeks.command(name="snapshot", description="ADMIN: Force Download Full Chain")
@commands.has_permissions(administrator=True)
async def beeks_snapshot(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True),
    session: Option(str, required=True),
    force_date: Option(str, required=False)
):
    await ctx.defer(ephemeral=True)
    yf_sym = get_options_ticker(resolve_yf_symbol(ticker))
    try:
        tkr = yf.Ticker(yf_sym); hist = tkr.history(period="1d")
        if hist.empty and not tkr.options: await ctx.respond(f"❌ **Beeks:** 'Never heard of **{ticker.upper()}**.'", ephemeral=True); return
        current_price = hist['Close'].iloc[-1] if not hist.empty else 0.0
        if not validate_atm_data(tkr, current_price): await ctx.respond(f"❌ **Beeks:** 'Bad Exchange Data.'", ephemeral=True); return
    except Exception as e: await ctx.respond(f"❌ **Beeks:** 'Connection failed: {e}'", ephemeral=True); return

    await ctx.respond(f"⏳ **Beeks:** 'Acquiring full chain for **{yf_sym}**...'", ephemeral=True)
    try:
        start_time = time.time(); exps = tkr.options; div_yield = get_current_yield(yf_sym)
        ts_str = f"{force_date} 23:59:59" if force_date else datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
        full_chain = {"symbol": yf_sym, "timestamp": ts_str, "expirations": {}}
        contract_count = 0
        for e in exps:
            try:
                opt = tkr.option_chain(e)
                c_list = opt.calls.to_dict(orient='records'); p_list = opt.puts.to_dict(orient='records')
                if not c_list and not p_list: continue
                full_chain["expirations"][e] = {"calls": c_list, "puts": p_list}
                contract_count += (len(c_list) + len(p_list))
            except: pass
        
        save_status = "✅ **Asset Secured.**" if contract_count >= 100 else f"⚠️ **WARNING: DATA THIN.** ({contract_count})"
        success = save_snapshot(yf_sym, full_chain, current_price, div_yield, tag=session.upper(), custom_timestamp=ts_str)
        duration = time.time() - start_time
        if success: await ctx.respond(f"{save_status}\n**{yf_sym}** @ {current_price:.2f}\nYield: `{div_yield:.2%}`\n🏷️ Session: `{session.upper()}`\n📅 Date: `{ts_str}`\nContracts: **{contract_count}**\n⏱️ {duration:.2f}s", ephemeral=True)
        else: await ctx.respond(f"❌ **Beeks:** 'Duplicate Tag.'", ephemeral=True)
    except Exception as e: await ctx.respond(f"⚠️ **Beeks:** 'Snapshot failed: {e}'", ephemeral=True)

@beeks.command(name="delete_snapshot", description="ADMIN: Permanently remove a snapshot")
@commands.has_permissions(administrator=True)
async def beeks_delete_snapshot(ctx: discord.ApplicationContext, ticker: str, replay_date: str, session: str):
    await ctx.defer(ephemeral=True)
    yf_sym = get_options_ticker(resolve_yf_symbol(ticker))
    if delete_snapshot_from_db(yf_sym, replay_date, session):
        await ctx.respond(f"🗑️ **Beeks:** 'Shredded file for **{yf_sym}** on **{replay_date}** [{session}].'", ephemeral=True)
    else: await ctx.respond(f"❌ **Beeks:** 'File not found.'", ephemeral=True)

@beeks.command(name="dailyrange", description="Get Volatility Ranges")
async def beeks_dailyrange(
    ctx: discord.ApplicationContext,
    ticker: Option(str, required=True),
    engine: Option(str, choices=["Insider Info", "Market Floor"], default="Insider Info"),
    replay_date: Option(str, autocomplete=get_db_dates, required=False),
    session: Option(str, autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    raw_ticker = ticker.upper(); yf_sym = resolve_yf_symbol(raw_ticker)
    FUTURES_MAP = {"ES": "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F"}
    is_future = raw_ticker in FUTURES_MAP
    futures_ticker = FUTURES_MAP.get(raw_ticker)

    if replay_date:
        tag_to_use = session if session and "Select" not in session else get_latest_tag_for_date(get_options_ticker(yf_sym), replay_date)
        data = fetch_historical_data(yf_sym, replay_date, tag_to_use)
        if not data: await ctx.respond(f"❌ **Beeks:** 'File not found.'", ephemeral=True); return
        clean_footer = f"📼 REPLAY: {replay_date} [{tag_to_use}]"; movie_quote = random.choice(MOVIE_QUOTES); anchor_label = "SNAPSHOT"
    else:
        is_supported = yf_sym in ["^GSPC", "^NDX"] or is_future or (not yf_sym.startswith("^") and not yf_sym.endswith("=F"))
        if not is_supported: await ctx.respond(f"❌ **Beeks:** 'Incompatible Ticker.'", ephemeral=True); return
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
            ft = yf.Ticker(futures_ticker); spx = yf.Ticker(yf_sym)
            f_hist = ft.history(period="5d"); s_hist = spx.history(period="5d")
            if len(f_hist) >= 2 and len(s_hist) >= 2:
                offset = f_hist['Close'].iloc[-2] - s_hist['Close'].iloc[-2]
                display_price += offset
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

@beeks.command(name="gammaflip", description="Calculate Zero Gamma Level")
async def beeks_gammaflip(
    ctx: discord.ApplicationContext, ticker: Option(str, required=True), scope: Option(str, choices=["Front Month", "Total Market", "0DTE"], default="Front Month"),
    replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False), target_expiry: Option(str, required=False)
):
    await ctx.defer(ephemeral=True)
    yf_sym = resolve_yf_symbol(ticker); display_ticker = get_options_ticker(yf_sym)
    calc_date = None; calc_tag = session; market_time = datetime.datetime.now(ZoneInfo("America/New_York"))
    
    if target_expiry:
        target_date = target_expiry; status_msg = f"⏳ **Beeks:** 'Targeting **{target_date}**...'"
        if replay_date: calc_date = replay_date; status_msg += f" (using {replay_date})"
    elif scope == "0DTE" and not replay_date:
        target_date = market_time.strftime("%Y-%m-%d") if market_time.weekday() < 4 and market_time.hour < 16 else get_next_market_date(market_time)
        conn = sqlite3.connect("beeks.db"); c = conn.cursor(); c.execute("SELECT date(timestamp) FROM chain_snapshots WHERE ticker = ? AND tag = 'CLOSE' ORDER BY timestamp DESC LIMIT 1", (display_ticker,)); row = c.fetchone(); conn.close()
        if not row: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'No recent CLOSE snapshot for 0DTE.'"); return
        calc_date = row[0]; calc_tag = "CLOSE"; status_msg = f"⏳ **Beeks:** 'Loading 0DTE from {calc_date} [CLOSE]...'"
    elif replay_date:
        calc_date = replay_date; status_msg = f"⏳ **Beeks:** 'Simulation on {replay_date}...'"; target_date = replay_date 
    else:
        status_msg = f"⏳ **Beeks:** 'Scanning {scope}...'"; target_date = None

    await ctx.interaction.edit_original_response(content=status_msg)
    raw_data = fetch_and_enrich_chain(ticker=ticker, expiry_date=target_date, snapshot_date=calc_date, snapshot_tag=calc_tag, scope=scope)
    if not raw_data: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'No data found.'"); return
    
    spot_price = raw_data[0].get('spot') or yf.Ticker(yf_sym).history(period="1d")['Close'].iloc[-1]
    flip_price, plot_data = calculate_gamma_flip(raw_data, spot_price)
    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
    
    if view_setting == 'modern' and plot_data:
        sim_spots, net_gammas = plot_data
        plt.figure(figsize=(10, 6)); plt.style.use('dark_background')
        plt.plot(sim_spots, net_gammas, color='cyan', linewidth=2)
        plt.fill_between(sim_spots, net_gammas, 0, where=(net_gammas >= 0), color='green', alpha=0.3)
        plt.fill_between(sim_spots, net_gammas, 0, where=(net_gammas < 0), color='red', alpha=0.3)
        plt.axhline(0, color='white', linestyle='--', linewidth=1); plt.axvline(spot_price, color='yellow', linestyle=':', label='Spot')
        if flip_price: plt.axvline(flip_price, color='magenta', linewidth=2, label='Flip'); plt.text(flip_price, max(net_gammas)*0.1, f"{flip_price:.0f}", color='magenta', fontweight='bold')
        plt.title(f"Gamma Profile: {display_ticker}", color='white', fontweight='bold'); plt.ylabel("Net Gamma ($)", color='white'); plt.grid(True, alpha=0.2)
        buf = io.BytesIO(); plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e'); buf.seek(0); plt.close()
        file = discord.File(buf, filename="gamma_flip.png")
        embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://gamma_flip.png")
        await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
    else:
        res_str = f"**{quote}**\n\n**GAMMA FLIP REPORT: {display_ticker}**\nSpot Price: {spot_price:.2f}\n" + ("-" * 30) + "\n"
        if flip_price: res_str += f"**FLIP LEVEL: {flip_price:.2f}**\nDistance: {flip_price - spot_price:+.2f}\nRegime: {'BULLISH' if spot_price > flip_price else 'BEARISH'}\n"
        else: res_str += "⚠️ **NO FLIP DETECTED**\n"
        await ctx.interaction.edit_original_response(content=res_str)

@beeks.command(name="exposures", description="Show Dealer Dollar Exposure")
async def beeks_exposures(
    ctx: discord.ApplicationContext, ticker: Option(str, required=True), scope: Option(str, choices=["Front Month", "Total Market", "0DTE"], default="Front Month"),
    replay_date: Option(str, autocomplete=get_db_dates, required=False), session: Option(str, autocomplete=get_db_tags, required=False), target_expiry: Option(str, required=False)
):
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
    if not raw_data: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'No data found.'"); return
    
    # --- UPDATED UNPACKING ---
    spot_price = raw_data[0]['spot']
    gex, dex, vex, cex = calculate_market_exposures(raw_data, spot_price)
    
    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)

    if view_setting == 'modern':
        img_buf = generate_exposure_dashboard(display_ticker, spot_price, gex, dex, vex, cex, scope, target_date)
        file = discord.File(img_buf, filename="exposures.png")
        embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31); embed.set_image(url="attachment://exposures.png")
        await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
    else:
        def fmt(v, s="B"): d=1_000_000_000 if s=="B" else 1_000_000; return f"${v/d:>7.2f} {s}"
        msg = f"> **{quote}**\n```yaml\n+--------------------------------------------------+\n| CLARENCE BEEKS TERMINAL           [EXPOSURE]     |\n+--------------------------------------------------+\n| TICKER: {display_ticker:<16} SPOT: {spot_price:<15.2f} |\n| SCOPE:  {scope:<16} DATE: {target_date if target_date else 'LIVE':<15} |\n+--------------------------------------------------+\n| GEX (GAMMA)   : {fmt(gex, 'B'):<12} / 1% Move      |\n| DEX (DELTA)   : {fmt(dex, 'B'):<12} Net Notional   |\n| VEX (VANNA)   : {fmt(vex, 'M'):<12} / 1% IV Change |\n| CEX (CHARM)   : {fmt(cex, 'M'):<12} Delta Decay/Day|\n+--------------------------------------------------+\n| REGIME: {'DAMPENED VOL' if gex > 0 else 'ACCELERATED VOL':<32} |\n+--------------------------------------------------+\n```"
        await ctx.interaction.edit_original_response(content=msg)

@beeks.command(name="strikes", description="Visualize Net Exposure per Strike")
async def beeks_strikes(
    ctx: discord.ApplicationContext, ticker: Option(str, required=True), scope: Option(str, choices=["Front Month", "Total Market", "0DTE"], required=True),
    metric: Option(str, choices=["CONFLUENCE", "ALL", "GEX", "DEX", "VEX"], default="ALL"), target_expiry: Option(str, required=False),
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
    if not raw_data: await ctx.interaction.edit_original_response(content=f"❌ **Beeks:** 'No data found.'"); return
    
    spot_price = raw_data[0]['spot']; strike_data = calculate_strike_exposures(raw_data, spot_price, display_ticker)
    
    def get_top(vals): return {x[0] for x in sorted([p for p in zip(strike_data['strikes'], vals) if abs(p[1])>0], key=lambda x: abs(x[1]), reverse=True)[:5]}
    all_sig = get_top(strike_data['gex']) | get_top(strike_data['dex']) | get_top(strike_data['vex'])
    confluence_map = [{'strike': k, 'score': (1 if k in get_top(strike_data['gex']) else 0) + (1 if k in get_top(strike_data['dex']) else 0) + (1 if k in get_top(strike_data['vex']) else 0), 'tags': ("G" if k in get_top(strike_data['gex']) else "")+("D" if k in get_top(strike_data['dex']) else "")+("V" if k in get_top(strike_data['vex']) else "")} for k in all_sig]
    confluence_map.sort(key=lambda x: (x['score'], x['strike']), reverse=True)

    view_setting = get_user_terminal_setting(ctx.author.id); quote = random.choice(MOVIE_QUOTES)
    if view_setting == 'modern':
        img_buf = generate_strike_chart(display_ticker, spot_price, strike_data, metric, confluence_map)
        file = discord.File(img_buf, filename="strikes.png")
        desc = f"**{quote}**\n" + (f"\n**🎯 CONFLUENCE (Top 5)**\n" + "\n".join([f"`{int(i['strike']):<5}` {'⭐'*i['score']} ({i['tags']})" for i in confluence_map[:5]]) if metric == "CONFLUENCE" else "")
        embed = discord.Embed(color=0x2b2d31, description=desc); embed.set_image(url="attachment://strikes.png"); embed.set_footer(text=f"Spot: {spot_price:.2f} | {scope}")
        await ctx.interaction.edit_original_response(content="", embed=embed, file=file)
    else:
        # (Condensed Bloomberg logic for Strikes)
        lines = [f"> **{quote}**"]
        if metric == "CONFLUENCE":
            lines += ["```yaml", "+---------------------------------------+", "| KEY LEVELS (HIGHEST OVERLAP)          |", "+---------------------------------------+"]
            for i in confluence_map: lines.append(f"| {int(i['strike']):<7} | {'*'*i['score']:<3} | {i['tags']:<5} |")
            lines.append("```")
        else:
             # Just show table logic
             pass
        await ctx.interaction.edit_original_response(content="\n".join(lines))

@beeks.command(name="chain", description="View Raw Chain (Live or Backtest)")
async def beeks_chain(
    ctx: discord.ApplicationContext,
    ticker: Option(str, description="Ticker Symbol", required=True),
    expiry: Option(str, name="expiry", description="YYYY-MM-DD (Optional)", required=False),
    center: Option(float, name="center", description="Center Strike (Default: Spot)", required=False),
    rows: Option(int, name="rows", description="Rows to show", choices=[1, 3, 5, 10], default=10),
    replay_date: Option(str, name="replay_date", description="Backtest Date", autocomplete=get_db_dates, required=False),
    session: Option(str, name="session", description="Session Tag", autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)

    # --- 1. SETUP & FETCH ---
    yf_sym = resolve_yf_symbol(ticker)
    display_ticker = get_options_ticker(yf_sym)
    
    actual_rows = 11 if rows == 10 else rows

    # Backtest Logic
    calc_date = replay_date if replay_date else None
    calc_tag = session
    if calc_date and not calc_tag:
        calc_tag = get_latest_tag_for_date(display_ticker, calc_date)

    # Fetch Data
    data = fetch_and_enrich_chain(
        ticker=ticker,
        expiry_date=expiry,
        snapshot_date=calc_date,
        snapshot_tag=calc_tag,
        scope="Specific" if expiry else "Front Month",
        range_count=actual_rows,
        pivot=center
    )
    
    if not data:
        mode = f"REPLAY {calc_date}" if calc_date else "LIVE"
        await ctx.respond(f"❌ **Beeks:** 'No data found for {ticker} ({mode}). Check your dates.'", ephemeral=True)
        return

    # Meta Data
    spot = data[0]['spot']
    target_date = expiry if expiry else "FRONT MONTH"
    view_setting = get_user_terminal_setting(ctx.author.id)
    quote = random.choice(MOVIE_QUOTES)
    
    # Source Label
    if calc_date:
        source_label = f"DB: {calc_date} [{calc_tag}]"
    else:
        source_label = "LIVE"

    closest_strike = min([d['strike'] for d in data], key=lambda x: abs(x - spot))

    # Process Data
    table_rows = []
    
    grouped = {}
    for row in data:
        k = row['strike']
        if k not in grouped: grouped[k] = {'C': None, 'P': None}
        if row['type'].lower().startswith('c'): grouped[k]['C'] = row
        else: grouped[k]['P'] = row
    
    # --- FIX: REVERSE SORT (DOM STYLE / HIGH TO LOW) ---
    sorted_strikes = sorted(grouped.keys(), reverse=True)

    for k in sorted_strikes:
        c = grouped[k]['C']
        p = grouped[k]['P']
        
        # Safe Unpacking
        c_iv = c.get('iv', 0) if c else 0
        c_delta = c.get('delta', 0) if c else 0
        c_gamma = c.get('gamma', 0) if c else 0
        c_theta = c.get('theta', 0) if c else 0
        c_vol = c.get('volume', 0) if c else 0
        c_oi = c.get('oi', 0) if c else 0
        
        p_iv = p.get('iv', 0) if p else 0
        p_delta = p.get('delta', 0) if p else 0
        p_gamma = p.get('gamma', 0) if p else 0
        p_theta = p.get('theta', 0) if p else 0
        p_vol = p.get('volume', 0) if p else 0
        p_oi = p.get('oi', 0) if p else 0

        table_rows.append({
            'strike': k,
            'c_iv': c_iv, 'c_delta': c_delta, 'c_gamma': c_gamma, 'c_theta': c_theta, 'c_vol': c_vol, 'c_oi': c_oi,
            'p_iv': p_iv, 'p_delta': p_delta, 'p_gamma': p_gamma, 'p_theta': p_theta, 'p_vol': p_vol, 'p_oi': p_oi
        })

    # --- 2. RENDER: MODERN MODE (IMAGE) ---
    if view_setting == 'modern':
        # Create Table Plot
        plt.figure(figsize=(16, len(table_rows) * 0.5 + 3))
        plt.style.use('dark_background')
        ax = plt.gca()
        ax.axis('off')
        
        # Vol -> OI -> IV -> Theta -> Gamma -> Delta -> STRIKE -> Delta -> Gamma -> Theta -> IV -> OI -> Vol
        cols = ['VOL', 'OI', 'IV', 'THETA', 'GAMMA', 'DELTA', 'STRIKE', 'DELTA', 'GAMMA', 'THETA', 'IV', 'OI', 'VOL']
        cell_text = []
        cell_colors = []
        
        for row in table_rows:
            c_bg = '#003300' if row['strike'] < spot else '#222222'
            p_bg = '#330000' if row['strike'] > spot else '#222222'
            s_bg = '#444444' 
            if row['strike'] == closest_strike: s_bg = '#AA8800' 

            r_colors = [c_bg]*6 + [s_bg] + [p_bg]*6

            r_data = [
                f"{int(row['c_vol'])}", f"{int(row['c_oi'])}", f"{row['c_iv']:.1%}", f"{row['c_theta']:.2f}", f"{row['c_gamma']:.3f}", f"{row['c_delta']:.2f}",
                f"{row['strike']:.0f}",
                f"{row['p_delta']:.2f}", f"{row['p_gamma']:.3f}", f"{row['p_theta']:.2f}", f"{row['p_iv']:.1%}", f"{int(row['p_oi'])}", f"{int(row['p_vol'])}"
            ]
            cell_text.append(r_data)
            cell_colors.append(r_colors)

        table = plt.table(cellText=cell_text, colLabels=cols, cellColours=cell_colors, loc='center', cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.8) 
        
        for (i, j), cell in table.get_celld().items():
            if i == 0: 
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#111111') 
                cell.set_edgecolor('white')
                cell.set_linewidth(1)
            else:
                cell.set_edgecolor('#555555')
                cell.set_linewidth(0.5)

        header = f"{display_ticker} CHAIN  |  EXPIRY: {target_date}  |  SPOT: {spot:.2f}\nFEED: {source_label}"
        plt.title(header, color='white', pad=20, fontsize=14, weight='bold')

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e', dpi=120)
        buf.seek(0); plt.close()
        
        file = discord.File(buf, filename="chain_modern.png")
        embed = discord.Embed(description=f"**{quote}**", color=0x2b2d31)
        embed.set_image(url="attachment://chain_modern.png")
        await ctx.respond(embed=embed, file=file, ephemeral=True)

    # --- 3. RENDER: BLOOMBERG MODE (ASCII) ---
    else:
        def fmt_5(val):
            val = int(val)
            if val >= 1_000_000: s = f"{val/1_000_000:.1f}m"
            elif val >= 1_000: s = f"{val/1_000:.0f}k"
            else: s = str(val)
            return f"{s:>5}"

        lines = [
            f"> **{quote}**",
            f"```yaml",
            f"TICKER: {display_ticker}  SPOT: {spot:.2f}",
            f"EXP: {target_date}  SRC: {source_label}",
            f"-"*74,
            f"|{'CALLS':^32}|{'':^6}|{'PUTS':^32}|",
            f"|{'V':>5}{'OI':>5}{'IV':>4}{'TH':>6}{'GM':>6}{'DL':>6}|{'STRK':^6}|{'DL':>6}{'GM':>6}{'TH':>6}{'IV':>4}{'OI':>5}{'V':>5}|",
            f"-"*74
        ]
        
        for row in table_rows:
            s_str = f"{row['strike']:.0f}"
            strike_display = f">{s_str}<" if row['strike'] == closest_strike else s_str
            
            line = (
                f"|{fmt_5(row['c_vol'])}{fmt_5(row['c_oi'])}{row['c_iv']:>4.0%}"
                f"{row['c_theta']:>6.2f}{row['c_gamma']:>6.3f}{row['c_delta']:>6.2f}"
                f"|{strike_display:^6}|"
                f"{row['p_delta']:>6.2f}{row['p_gamma']:>6.3f}{row['p_theta']:>6.2f}"
                f"{row['p_iv']:>4.0%}{fmt_5(row['p_oi'])}{fmt_5(row['p_vol'])}|"
            )
            lines.append(line)
            
        lines.append(f"-"*74)
        lines.append(f"```")
        
        final_text = "\n".join(lines)
        if len(final_text) > 1950:
            await ctx.respond(final_text[:1950] + "\n```... (Truncated)", ephemeral=True)
        else:
            await ctx.respond(final_text, ephemeral=True)

@beeks.command(name="vig", description="Get ATM Straddle Cost (Expected Move)")
async def beeks_vig(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True), 
    expiry: Option(str, description="YYYY-MM-DD (Optional)", required=False),
    replay_date: Option(str, description="Backtest Date", autocomplete=get_db_dates, required=False), 
    session: Option(str, description="Session Tag", autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    
    # Setup
    calc_date = replay_date if replay_date else None
    calc_tag = session
    if calc_date and not calc_tag:
        yf_sym = resolve_yf_symbol(ticker)
        db_ticker = get_options_ticker(yf_sym)
        calc_tag = get_latest_tag_for_date(db_ticker, calc_date)

    # Fetch
    scope = "Specific" if expiry else "Front Month"
    data = fetch_and_enrich_chain(
        ticker=ticker, expiry_date=expiry, 
        snapshot_date=calc_date, snapshot_tag=calc_tag, 
        scope=scope, range_count=9999
    )

    if not data:
        await ctx.respond(f"❌ **Beeks:** 'No data found for {ticker}.'", ephemeral=True)
        return

    # Process
    spot = data[0]['spot']
    df = pd.DataFrame(data)
    
    # --- FIX IS HERE (Wrap in np.array) ---
    unique_strikes = np.array(sorted(df['strike'].unique()))
    atm_strike = unique_strikes[np.abs(unique_strikes - spot).argmin()]

    # Filter for ATM
    atm_opts = df[df['strike'] == atm_strike]
    call = atm_opts[atm_opts['type'].str.lower() == 'call']
    put = atm_opts[atm_opts['type'].str.lower() == 'put']

    if call.empty or put.empty:
        await ctx.respond(f"❌ **Beeks:** 'ATM Straddle incomplete for {atm_strike}.'", ephemeral=True)
        return

    # Price Calculation
    c_price = (call.iloc[0]['bid'] + call.iloc[0]['ask']) / 2
    if c_price == 0: c_price = call.iloc[0]['price']
    p_price = (put.iloc[0]['bid'] + put.iloc[0]['ask']) / 2
    if p_price == 0: p_price = put.iloc[0]['price']

    vig = c_price + p_price
    breakeven_up = atm_strike + vig
    breakeven_down = atm_strike - vig
    
    # Render
    view_setting = get_user_terminal_setting(ctx.author.id)
    quote = random.choice(MOVIE_QUOTES)
    source = f"DB: {calc_date} [{calc_tag}]" if calc_date else "LIVE"

    if view_setting == "modern":
        plt.figure(figsize=(6, 9)) 
        plt.style.use('dark_background')
        ax = plt.gca()
        ax.axis('off')
        
        # Scaling
        margin = vig * 1.4
        y_min = breakeven_down - margin
        y_max = breakeven_up + margin
        plt.ylim(y_min, y_max)
        plt.xlim(0, 10) 
        
        # Header
        header_y = y_max - (margin * 0.1)
        plt.text(5, header_y, "ATM EXPECTED MOVE", color='#888888', ha='center', fontsize=11, weight='bold')
        plt.text(5, header_y - (margin * 0.15), f"${vig:.2f}", color='white', ha='center', fontsize=32, weight='bold')

        # The Bar
        from matplotlib.patches import Rectangle
        rect_border = Rectangle((3.5, breakeven_down), 3, (breakeven_up - breakeven_down), 
                                linewidth=2, edgecolor='#00e5ff', facecolor='none', zorder=2)
        ax.add_patch(rect_border)
        rect_fill = Rectangle((3.5, breakeven_down), 3, (breakeven_up - breakeven_down), 
                              linewidth=0, facecolor='#00e5ff', alpha=0.15, zorder=1)
        ax.add_patch(rect_fill)
        plt.hlines(atm_strike, 3.5, 6.5, color='#00e5ff', linestyle=':', linewidth=1, alpha=0.5)

        # Labels
        plt.text(7, breakeven_up, f"{breakeven_up:.2f}", color='#00e5ff', va='center', ha='left', fontsize=12, weight='bold')
        plt.text(3, breakeven_up, "UPPER", color='#00e5ff', va='center', ha='right', fontsize=9)
        plt.text(7, breakeven_down, f"{breakeven_down:.2f}", color='#00e5ff', va='center', ha='left', fontsize=12, weight='bold')
        plt.text(3, breakeven_down, "LOWER", color='#00e5ff', va='center', ha='right', fontsize=9)

        # Spot Marker
        c_spot = 'white'
        if spot > breakeven_up: c_spot = '#00ff00'
        elif spot < breakeven_down: c_spot = '#ff0000'
        
        plt.hlines(spot, 2.5, 7.5, color=c_spot, linewidth=2, zorder=3)
        plt.plot(2.5, spot, marker='D', markersize=8, color=c_spot, zorder=4)
        plt.text(2, spot, f"{spot:.2f}", color=c_spot, ha='right', va='center', fontsize=14, weight='bold')

        plt.text(5, y_min, f"{ticker.upper()} | {source}", color='#444444', ha='center', fontsize=9)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e')
        buf.seek(0); plt.close()
        
        file = discord.File(buf, filename="vig_holo.png")
        embed = discord.Embed(description=f"**{quote}**", color=0x00e5ff); embed.set_image(url="attachment://vig_holo.png")
        await ctx.respond(embed=embed, file=file, ephemeral=True)
    else:
        msg = f"> **{quote}**\n**{ticker.upper()} THE VIG (ATM STRADDLE)**\nSource: `{source}`\nSpot: `{spot:.2f}` | ATM: `{atm_strike}`\n"
        msg += f"```yaml\nCOST (EXPECTED MOVE): ${vig:.2f}\n--------------------------------\nUPPER BREAKEVEN:      {breakeven_up:.2f}\nLOWER BREAKEVEN:      {breakeven_down:.2f}\n```"
        await ctx.respond(msg, ephemeral=True)

@beeks.command(name="skew", description="Check Put/Call Volatility Ratio")
async def beeks_skew(
    ctx: discord.ApplicationContext, 
    ticker: Option(str, required=True),
    expiry: Option(str, description="YYYY-MM-DD (Optional)", required=False),
    replay_date: Option(str, description="Backtest Date", autocomplete=get_db_dates, required=False), 
    session: Option(str, description="Session Tag", autocomplete=get_db_tags, required=False)
):
    await ctx.defer(ephemeral=True)
    
    # Backtest Setup
    calc_date = replay_date if replay_date else None
    calc_tag = session
    if calc_date and not calc_tag:
        yf_sym = resolve_yf_symbol(ticker)
        db_ticker = get_options_ticker(yf_sym)
        calc_tag = get_latest_tag_for_date(db_ticker, calc_date)

    # Fetch
    scope_req = "Specific" if expiry else "Front Month"
    data = fetch_and_enrich_chain(
        ticker=ticker, expiry_date=expiry, snapshot_date=calc_date, snapshot_tag=calc_tag, 
        scope=scope_req, range_count=9999
    )
    
    if not data:
        await ctx.respond(f"❌ **Beeks:** 'No data available.'", ephemeral=True)
        return

    spot = data[0]['spot']
    df = pd.DataFrame(data)

    # Filter Time
    if expiry:
        skew_chain = df 
        target_time = df['time_year'].iloc[0]
    else:
        unique_times = sorted(df['time_year'].unique())
        if not unique_times:
             await ctx.respond(f"❌ **Beeks:** 'Bad data structure.'", ephemeral=True); return
        target_time = min(unique_times, key=lambda x: abs(x - 0.082))
        skew_chain = df[df['time_year'] == target_time]
    
    # Calculate Skew
    put_strike_target = spot * 0.95
    call_strike_target = spot * 1.05
    
    # --- FIX IS HERE (.str.lower()) ---
    puts = skew_chain[skew_chain['type'].str.lower() == 'put']
    calls = skew_chain[skew_chain['type'].str.lower() == 'call']
    
    if puts.empty or calls.empty:
         await ctx.respond(f"❌ **Beeks:** 'Chain too thin for Skew.'", ephemeral=True); return

    put_row = puts.iloc[np.abs(puts['strike'] - put_strike_target).argmin()]
    call_row = calls.iloc[np.abs(calls['strike'] - call_strike_target).argmin()]
    
    put_iv = put_row['iv'] * 100
    call_iv = call_row['iv'] * 100
    
    if call_iv == 0: ratio = 0
    else: ratio = put_iv / call_iv
    
    sentiment = "BEARISH (HEDGING)" if ratio > 1.2 else "BULLISH (FOMO)" if ratio < 0.8 else "NEUTRAL"
    view_setting = get_user_terminal_setting(ctx.author.id)
    quote = random.choice(MOVIE_QUOTES)
    source = f"DB: {calc_date} [{calc_tag}]" if calc_date else "LIVE"
    dte_days = int(target_time * 365)

    if view_setting == "modern":
        plt.figure(figsize=(10, 5))
        plt.style.use('dark_background')
        ax = plt.gca(); ax.axis('off')

        c_sent = '#ff5555' if ratio > 1.2 else '#55ff55' if ratio < 0.8 else '#ffff55'
        
        plt.text(0.5, 0.85, f"VOLATILITY SKEW ({dte_days} DTE)", color='white', fontsize=16, weight='bold', ha='center')
        plt.text(0.5, 0.65, f"{ratio:.2f}", color=c_sent, fontsize=36, weight='bold', ha='center')
        plt.text(0.5, 0.55, sentiment, color=c_sent, fontsize=12, weight='bold', ha='center', bbox=dict(facecolor='#222222', edgecolor=c_sent, pad=5))
        
        plt.text(0.25, 0.35, f"PUT IV (95%)", color='#ff99cc', fontsize=10, ha='center')
        plt.text(0.25, 0.20, f"{put_iv:.1f}%", color='white', fontsize=16, weight='bold', ha='center')

        plt.text(0.75, 0.35, f"CALL IV (105%)", color='#99ccff', fontsize=10, ha='center')
        plt.text(0.75, 0.20, f"{call_iv:.1f}%", color='white', fontsize=16, weight='bold', ha='center')

        plt.text(0.5, 0.05, f"Ref: {spot:.2f} | {source}", color='#666666', fontsize=9, ha='center')

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#1e1e1e')
        buf.seek(0); plt.close()
        file = discord.File(buf, filename="skew.png")
        embed = discord.Embed(description=f"**{quote}**", color=0xFF99CC); embed.set_image(url="attachment://skew.png")
        await ctx.respond(embed=embed, file=file, ephemeral=True)
    else:
        msg = f"**{ticker.upper()} SKEW REPORT ({dte_days} DTE)**\nSource: `{source}`\n"
        msg += f"```yaml\nPUT IV (95%):  {put_iv:.1f}%\nCALL IV (105%): {call_iv:.1f}%\nRATIO:         {ratio:.2f}\nSENTIMENT:     {sentiment}\n```"
        await ctx.respond(msg, ephemeral=True)

@beeks.command(name="inspect", description="ADMIN: View Database Tree")
@commands.has_permissions(administrator=True)
async def beeks_inspect(ctx: discord.ApplicationContext, ticker: Option(str, required=True)):
    await ctx.defer(ephemeral=True)
    
    yf_sym = resolve_yf_symbol(ticker)
    db_ticker = get_options_ticker(yf_sym)
    
    conn = sqlite3.connect("beeks.db")
    c = conn.cursor()
    
    # Get all snapshots for this ticker
    c.execute("""
        SELECT date(timestamp) as date_part, tag, time(timestamp) as time_part 
        FROM chain_snapshots 
        WHERE ticker = ? 
        ORDER BY timestamp DESC
    """, (db_ticker,))
    
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        await ctx.respond(f"❌ **Beeks:** 'No records found for **{db_ticker}**.'", ephemeral=True)
        return

    # Build the Tree
    tree = {}
    for r in rows:
        d, tag, t = r
        if d not in tree: tree[d] = []
        tree[d].append(f"{tag} ({t})")
    
    # Format the Output
    lines = [f"**📂 DATABASE MANIFEST: {db_ticker}**", "```yaml"]
    
    for date_key in sorted(tree.keys(), reverse=True):
        lines.append(f"{date_key}")
        entries = tree[date_key]
        for i, entry in enumerate(entries):
            connector = "└─" if i == len(entries) - 1 else "├─"
            lines.append(f"  {connector} {entry}")
            
    lines.append("```")
    
    final_msg = "\n".join(lines)
    if len(final_msg) > 1900:
        final_msg = final_msg[:1900] + "\n```... (Truncated)"
        
    await ctx.respond(final_msg, ephemeral=True)

# --- ENSURE SCHED_TIMES IS DEFINED ---
sched_times = [datetime.time(hour=9, minute=45), datetime.time(hour=15, minute=45)]

@tasks.loop(time=sched_times)
async def auto_fetch_heavy_chains():
    now = datetime.datetime.now(ZoneInfo("America/New_York"))
    if now.weekday() > 4: return
    session_tag = "OPEN" if now.hour < 11 else "MID" if now.hour < 14 else "CLOSE"
    print(f"\n⏰ AUTO-FETCH [{session_tag}]")
    for symbol in ["^SPX", "^NDX"]:
        try:
            tkr = yf.Ticker(symbol); hist = tkr.history(period="1d")
            if hist.empty: continue
            if not validate_atm_data(tkr, hist['Close'].iloc[-1]): continue
            full_chain = {"symbol": symbol, "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"), "expirations": {}}
            for e in tkr.options:
                try:
                    opt = tkr.option_chain(e)
                    full_chain["expirations"][e] = {"calls": opt.calls.to_dict(orient='records'), "puts": opt.puts.to_dict(orient='records')}
                except: pass
            save_snapshot(symbol, full_chain, hist['Close'].iloc[-1], get_current_yield(symbol), tag=session_tag, custom_timestamp=now.strftime("%Y-%m-%d %H:%M:%S"))
        except: pass

@bot.event
async def on_ready():
    init_db()
    if not auto_fetch_heavy_chains.is_running(): auto_fetch_heavy_chains.start()
    print(f"🍊 Duke & Duke: Clarence Beeks is Online. Logged in as {bot.user}")
    await bot.sync_commands()

bot.run(CONFIG["DISCORD_TOKEN"])