import streamlit as st
import json

# --- ANTI-SLEEP PING HANDLER ---
try:
    if "ping" in st.query_params:
        st.write("🟢 App is awake and Firebase quota is protected!")
        st.stop()
except:
    pass

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import io
import os
import re
import textwrap
import firebase_admin
from firebase_admin import credentials, firestore

try:
    import PyPDF2
    PDF_ENABLED = True
except ImportError:
    PDF_ENABLED = False

# --- UI CONFIGURATION ---
st.set_page_config(page_title="Logistics AI Planner", layout="wide")

# --- AGGRESSIVE TEXT UNIFICATION ENGINE (NO 'NAN', NO DUPLICATES) ---
def unify_text(val):
    if pd.isna(val) or val is None: return ""
    val = str(val).strip()
    if val.lower() in ["nan", "none", "nat", "null", ""]: return ""
    
    if re.search(r'2\s*-\s*8', val, flags=re.IGNORECASE): return '2-8 VAN'
    if val.upper() == 'PHARMA': return 'Pharma'
    if val.upper() == 'CONSUMER': return 'Consumer'
    if val.upper() == 'BUS': return 'BUS'
    if val.upper() == 'PICK-UP' or val.upper() == 'PICK UP': return 'PICK-UP'
    if val.upper() == 'VAN': return 'VAN'
    return val

def unify_dataframe(df):
    if df.empty: return df
    df = df.fillna("") 
    target_cols = ['sector', 'division', 'type', 'veh_type', 'div_cat', 'person_type', 'area', 'anchor_area', 'anchor_vehicle', 'permitted_areas', 'restriction', 'start_date', 'end_date', 'region']
    for col in target_cols:
        if col in df.columns:
            df[col] = df[col].apply(unify_text)
    return df

def parse_date_safe(d_str):
    if pd.isna(d_str) or d_str is None: return ""
    if isinstance(d_str, (datetime, pd.Timestamp)): return d_str.strftime("%Y-%m-%d")
    d_str = str(d_str).strip().split(" ")[0]
    if d_str.lower() in ["none", "nan", "nat", ""]: return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try: return datetime.strptime(d_str, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return d_str

# --- FIREBASE INITIALIZATION & DB ADAPTER ---
FIREBASE_READY = False
conn = None

SYNC_TABLES = ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations', 'active_routes', 'draft_routes', 'route_plan_reasons', 'vacation_predictions']

try:
    if "firebase" in st.secrets:
        if not firebase_admin._apps:
            cert_dict = dict(st.secrets["firebase"])
            raw_key = str(cert_dict.get("private_key", ""))
            clean_key = raw_key.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
            clean_key = clean_key.replace("\\n", "").replace("\n", "").replace("\r", "").replace(" ", "").replace('"', "").replace("'", "").strip()
            wrapped_key = '\n'.join(textwrap.wrap(clean_key, 64))
            cert_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{wrapped_key}\n-----END PRIVATE KEY-----\n"
            cred = credentials.Certificate(cert_dict)
            firebase_admin.initialize_app(cred)
        db_fs = firestore.client()
        FIREBASE_READY = True
    elif os.path.exists("firebase-key.json"):
        if not firebase_admin._apps:
            cred = credentials.Certificate("firebase-key.json")
            firebase_admin.initialize_app(cred)
        db_fs = firestore.client()
        FIREBASE_READY = True
    else:
        FIREBASE_READY = False
except Exception as e:
    st.sidebar.error(f"Firebase Config Error: {str(e)}")
    FIREBASE_READY = False

if FIREBASE_READY:
    try:
        list(db_fs.collection("_system_ping").limit(1).stream())
    except Exception as ping_error:
        FIREBASE_READY = False

# --- HYBRID SQLITE ---
def init_sqlite_db():
    local_conn = sqlite3.connect('logistics.db', check_same_thread=False)
    c = local_conn.cursor()
    
    c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='areas'")
    row_areas = c.fetchone()
    if row_areas and 'name TEXT UNIQUE' in row_areas[0]:
        c.execute("CREATE TABLE areas_v2 (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT, sector TEXT, needs_helper TEXT, sort_order INTEGER, region TEXT)")
        c.execute("INSERT OR IGNORE INTO areas_v2 (code, name, sector, needs_helper, sort_order, region) SELECT code, name, sector, needs_helper, sort_order, region FROM areas")
        c.execute("DROP TABLE areas")
        c.execute("ALTER TABLE areas_v2 RENAME TO areas")
        
    c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='default_areas'")
    row_def_areas = c.fetchone()
    if row_def_areas and 'name TEXT UNIQUE' in row_def_areas[0]:
        c.execute("CREATE TABLE default_areas_v2 (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT, sector TEXT, needs_helper TEXT, sort_order INTEGER, region TEXT)")
        c.execute("INSERT OR IGNORE INTO default_areas_v2 (code, name, sector, needs_helper, sort_order, region) SELECT code, name, sector, needs_helper, sort_order, region FROM default_areas")
        c.execute("DROP TABLE default_areas")
        c.execute("ALTER TABLE default_areas_v2 RENAME TO default_areas")

    c.execute('''CREATE TABLE IF NOT EXISTS _sync_log (id INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT, action TEXT, doc_id TEXT, payload TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE, veh_type TEXT, sector TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT, sector TEXT, needs_helper TEXT, sort_order INTEGER, region TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY, number TEXT UNIQUE, type TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS default_drivers (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE, veh_type TEXT, sector TEXT, restriction TEXT, anchor_area TEXT, needs_helper TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS default_helpers (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE, restriction TEXT, anchor_area TEXT, health_card TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS default_areas (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT, sector TEXT, needs_helper TEXT, sort_order INTEGER, region TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS default_vehicles (id INTEGER PRIMARY KEY, number TEXT UNIQUE, type TEXT, anchor_area TEXT, status TEXT, permitted_areas TEXT, division TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, area TEXT, date TEXT, end_date TEXT, sector TEXT)''')
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_history ON history(person_code, area, sector, date)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, start_date DATE, end_date DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT, start_date TEXT, end_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS draft_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT, start_date TEXT, end_date TEXT, div_cat TEXT, sector TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS route_plan_reasons (id INTEGER PRIMARY KEY, plan_date TEXT, area TEXT, role TEXT, selected_person TEXT, score REAL, reasons TEXT, generated_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacation_predictions (id INTEGER PRIMARY KEY, person_code TEXT, person_name TEXT, role TEXT, suggested_start TEXT, suggested_end TEXT, reason TEXT, replacement_person TEXT, replacement_date TEXT)''')
    
    for query in [
        "ALTER TABLE drivers ADD COLUMN needs_helper TEXT DEFAULT 'Yes'",
        "ALTER TABLE drivers ADD COLUMN anchor_vehicle TEXT DEFAULT ''",
        "ALTER TABLE drivers ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE default_drivers ADD COLUMN anchor_vehicle TEXT DEFAULT ''",
        "ALTER TABLE helpers ADD COLUMN health_card TEXT DEFAULT 'No'",
        "ALTER TABLE helpers ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE areas ADD COLUMN sector TEXT DEFAULT 'Pharma'",
        "ALTER TABLE areas ADD COLUMN needs_driver TEXT DEFAULT 'Yes'",
        "ALTER TABLE areas ADD COLUMN needs_helper TEXT DEFAULT 'Yes'",
        "ALTER TABLE areas ADD COLUMN anchor_vehicle TEXT DEFAULT ''",
        "ALTER TABLE areas ADD COLUMN sort_order INTEGER DEFAULT 99",
        "ALTER TABLE areas ADD COLUMN region TEXT DEFAULT 'Dubai'",
        "ALTER TABLE areas ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE default_areas ADD COLUMN needs_driver TEXT DEFAULT 'Yes'",
        "ALTER TABLE default_areas ADD COLUMN anchor_vehicle TEXT DEFAULT ''",
        "ALTER TABLE vehicles ADD COLUMN anchor_area TEXT DEFAULT ''",
        "ALTER TABLE vehicles ADD COLUMN status TEXT DEFAULT 'Active'",
        "ALTER TABLE vehicles ADD COLUMN permitted_areas TEXT DEFAULT 'All'",
        "ALTER TABLE vehicles ADD COLUMN division TEXT DEFAULT 'Pharma'",
        "ALTER TABLE vehicles ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE active_routes ADD COLUMN veh_perm TEXT DEFAULT ''",
        "ALTER TABLE draft_routes ADD COLUMN veh_perm TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE vacations ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE active_routes ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE draft_routes ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE route_plan_reasons ADD COLUMN fb_id TEXT DEFAULT ''",
        "ALTER TABLE vacation_predictions ADD COLUMN fb_id TEXT DEFAULT ''"
    ]:
        try: c.execute(query)
        except sqlite3.OperationalError: pass
    local_conn.commit()
    return local_conn

conn = init_sqlite_db()

# --- HIGH-EFFICIENCY DELTA SYNC ENGINE ---
def process_sync_log():
    global FIREBASE_READY
    if not FIREBASE_READY: return
    try:
        c = conn.cursor()
        c.execute("SELECT id, table_name, action, doc_id, payload FROM _sync_log ORDER BY id ASC")
        rows = c.fetchall()
        if not rows: return
        
        for row in rows:
            log_id, t_name, act, d_id, payload_str = row
            data = json.loads(payload_str) if payload_str else None
            ref = db_fs.collection(t_name)
            
            try:
                if act == "INSERT":
                    clean_data = {k:v for k,v in data.items() if k != 'fb_id'} if data else {}
                    if d_id and str(d_id) != "None" and str(d_id).strip() != "": ref.document(str(d_id)).set(clean_data)
                    else: ref.add(clean_data)
                elif act == "UPDATE":
                    clean_data = {k:v for k,v in data.items() if k != 'fb_id'} if data else {}
                    if d_id: ref.document(str(d_id)).set(clean_data, merge=True)
                elif act == "DELETE_DOC":
                    if d_id: ref.document(str(d_id)).delete()
                elif act == "CLEAR_TABLE":
                    while True:
                        docs = list(ref.limit(200).stream())
                        if not docs: break
                        batch = db_fs.batch()
                        for d in docs: batch.delete(d.reference)
                        batch.commit()
                elif act == "INSERT_MANY":
                    batch = db_fs.batch()
                    for i, item in enumerate(data):
                        item_id = item.get('fb_id') or item.get('id') or item.get('code') or db_fs.collection(t_name).document().id
                        clean_item = {k:v for k,v in item.items() if k != 'fb_id'}
                        batch.set(ref.document(str(item_id)), clean_item)
                        if (i + 1) % 400 == 0:
                            batch.commit()
                            batch = db_fs.batch()
                    batch.commit()
                    
                c.execute("DELETE FROM _sync_log WHERE id=?", (log_id,))
                conn.commit()
            except Exception as e:
                if "429" in str(e) or "Quota" in str(e) or "ResourceExhausted" in str(e):
                    FIREBASE_READY = False
                    break 
    except Exception as e:
        pass

def sync_down_from_cloud(merge=False):
    global FIREBASE_READY
    if not FIREBASE_READY: return False
    try:
        c = conn.cursor()
        for t in SYNC_TABLES:
            try:
                docs = list(db_fs.collection(t).stream())
                if not docs: continue
                
                df_cloud = pd.DataFrame([{**doc.to_dict(), 'fb_id': doc.id} for doc in docs])
                if df_cloud.empty: continue
                df_cloud = unify_dataframe(df_cloud)
                
                c.execute(f"PRAGMA table_info({t})")
                local_cols = [row[1] for row in c.fetchall() if row[1] != 'id']
                
                for col in local_cols:
                    if col not in df_cloud.columns: df_cloud[col] = ""
                        
                df_export = df_cloud[local_cols].copy()
                
                if not merge:
                    c.execute(f"DELETE FROM {t}")
                    
                cols_str = ', '.join(local_cols)
                qmarks = ', '.join(['?'] * len(local_cols))
                vals = [tuple(x) for x in df_export.to_numpy()]
                
                if merge and t in ['history']:
                    c.executemany(f"INSERT OR IGNORE INTO {t} ({cols_str}) VALUES ({qmarks})", vals)
                elif merge and t in ['drivers', 'helpers', 'areas', 'vehicles']:
                    c.executemany(f"INSERT OR IGNORE INTO {t} ({cols_str}) VALUES ({qmarks})", vals)
                else:
                    c.executemany(f"INSERT INTO {t} ({cols_str}) VALUES ({qmarks})", vals)
                    
                conn.commit()
                
                if merge and t == 'vacations':
                    c.execute(f"DELETE FROM {t} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {t} GROUP BY person_code, start_date, end_date)")
                    conn.commit()
                    
            except Exception as table_e:
                print(f"Error syncing {t}: {table_e}") 
        return True
    except Exception as e:
        st.error(f"Sync failed: {e}")
        return False

c_check = conn.cursor()
c_check.execute("SELECT COUNT(*) FROM areas")
if c_check.fetchone()[0] == 0 and FIREBASE_READY:
    sync_down_from_cloud(merge=False)

sync_count = pd.read_sql("SELECT COUNT(*) FROM _sync_log", conn).iloc[0,0]
if FIREBASE_READY and sync_count == 0:
    st.sidebar.markdown("<div style='text-align: right; font-size: 15px; margin-top: -15px;' title='Connected to Secure Cloud'>🟢 Cloud Sync Active</div>", unsafe_allow_html=True)
elif FIREBASE_READY and sync_count > 0:
    st.sidebar.markdown(f"<div style='text-align: right; font-size: 15px; margin-top: -15px; color: #2e8b57;' title='Syncing'>🔄 Syncing {sync_count} items...</div>", unsafe_allow_html=True)
    process_sync_log() 
else:
    st.sidebar.markdown(f"<div style='text-align: right; font-size: 15px; margin-top: -15px; color: #FFA500;' title='Offline'>🟡 Offline Queue: {sync_count}</div>", unsafe_allow_html=True)
    st.sidebar.caption("App is running perfectly offline. Changes save locally and auto-sync when quota resets.")

@st.cache_data(show_spinner=False, ttl=86400) 
def load_table(table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
    if df.empty: return df
    df = unify_dataframe(df)
    
    if table_name == 'helpers' and 'health_card' not in df.columns: df['health_card'] = 'No'
    if table_name == 'drivers':
        if 'needs_helper' not in df.columns: df['needs_helper'] = 'Yes'
        if 'anchor_vehicle' not in df.columns: df['anchor_vehicle'] = ''
    if table_name == 'areas':
        if 'sector' not in df.columns: df['sector'] = 'Pharma'
        if 'needs_driver' not in df.columns: df['needs_driver'] = 'Yes'
        if 'needs_helper' not in df.columns: df['needs_helper'] = 'Yes'
        if 'anchor_vehicle' not in df.columns: df['anchor_vehicle'] = ''
        if 'sort_order' not in df.columns: df['sort_order'] = 99
        if 'region' not in df.columns: df['region'] = 'Dubai'
        df['sort_order'] = pd.to_numeric(df['sort_order'], errors='coerce').fillna(99)
        df = df.sort_values(by='sort_order')
    if table_name == 'history' and 'sector' not in df.columns: df['sector'] = 'Pharma'
    if table_name == 'vehicles':
        if 'anchor_area' not in df.columns: df['anchor_area'] = ''
        if 'status' not in df.columns: df['status'] = 'Active'
        if 'permitted_areas' not in df.columns: df['permitted_areas'] = 'All'
        if 'division' not in df.columns: df['division'] = 'Pharma'
    if table_name == 'active_routes' and 'start_date' not in df.columns: df['start_date'] = ''
    if table_name == 'draft_routes':
        if 'start_date' not in df.columns: df['start_date'] = ''
        if 'end_date' not in df.columns: df['end_date'] = ''
    if table_name == 'vacations' and 'person_code' not in df.columns: df['person_code'] = 'UNKNOWN'
    
    if table_name in ['active_routes', 'draft_routes'] and 'order_num' in df.columns: 
        df['order_num'] = pd.to_numeric(df['order_num'], errors='coerce').fillna(99)
        df = df.sort_values(by='order_num')
    
    if table_name in ['drivers', 'helpers']: df = df.drop_duplicates(subset=['code'], keep='first')
    if table_name == 'vehicles': df = df.drop_duplicates(subset=['number'], keep='first')
    if table_name == 'areas': df = df.drop_duplicates(subset=['code'], keep='first') 
    if table_name == 'history': df = drop_duplicates_safe(df, ['person_code', 'area', 'date'])
    
    return df

def drop_duplicates_safe(df, subset):
    if df.empty: return df
    return df.drop_duplicates(subset=subset, keep='first')

def run_query(query, params=(), table_name=None, action=None, doc_id=None, data=None, bypass_queue=False):
    try:
        c = conn.cursor()
        if query:
            if isinstance(data, list) and (action == "INSERT_MANY" or action == "INSERT"): 
                c.executemany(query, params)
            elif action == "INSERT_MANY":
                c.executemany(query, params)
            else: 
                c.execute(query, params)
                if action == "INSERT" and not doc_id: doc_id = str(c.lastrowid)
        elif action == "CLEAR_TABLE" and table_name:
            c.execute(f"DELETE FROM {table_name}")
        conn.commit()

        if not bypass_queue and table_name and action and table_name in SYNC_TABLES:
            if action == "INSERT_MANY" and isinstance(data, list):
                c.execute("INSERT INTO _sync_log (table_name, action, doc_id, payload) VALUES (?, ?, ?, ?)", (table_name, action, "", json.dumps(data)))
            elif action == "CLEAR_TABLE":
                c.execute("INSERT INTO _sync_log (table_name, action, doc_id, payload) VALUES (?, ?, ?, ?)", (table_name, action, "", ""))
            else:
                c.execute("INSERT INTO _sync_log (table_name, action, doc_id, payload) VALUES (?, ?, ?, ?)", (table_name, action, str(doc_id) if doc_id else "", json.dumps(data) if data else ""))
            
            conn.commit()
            process_sync_log()

        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Database Edit Failed: {str(e)}")
        return False

def generate_excel_with_sn(df_list, sheet_names):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for df, sheet in zip(df_list, sheet_names):
            export_df = df.copy()
            if 'id' in export_df.columns: export_df = export_df.drop(columns=['id'])
            if 'fb_id' in export_df.columns: export_df = export_df.drop(columns=['fb_id'])
            if 'sort_order' in export_df.columns: export_df = export_df.drop(columns=['sort_order'])
            if 'S/N' in export_df.columns: export_df = export_df.drop(columns=['S/N'])
            if 'vacation_status' in export_df.columns: export_df = export_df.drop(columns=['vacation_status'])
            if 'Days' in export_df.columns: export_df = export_df.drop(columns=['Days'])
            export_df.insert(0, 'S/N', range(1, 1 + len(export_df)))
            export_df.to_excel(writer, sheet_name=sheet, index=False)
    output.seek(0)
    return output

def get_vac_status(code, vac_cache, today_date):
    today_str = today_date.strftime("%Y-%m-%d")
    vacs = vac_cache.get(code, [])
    if not vacs: return "Never"
    
    for s, e in vacs:
        if s <= today_str <= e:
            days_left = (datetime.strptime(e, "%Y-%m-%d").date() - today_date).days
            return f"On leave ({days_left} days left)"
            
    past_vacs = sorted([e for s, e in vacs if e < today_str], reverse=True)
    if past_vacs:
        last_e = past_vacs[0]
        days_since = (today_date - datetime.strptime(last_e, "%Y-%m-%d").date()).days
        return f"Back ({days_since} days ago)"
        
    future_vacs = sorted([s for s, e in vacs if s > today_str])
    if future_vacs:
        next_s = future_vacs[0]
        days_until = (datetime.strptime(next_s, "%Y-%m-%d").date() - today_date).days
        return f"Upcoming (in {days_until} days)"
        
    return "Never"

# --- OPTIONS & HARDCODED TEMPLATES ---
VEHICLE_OPTIONS = ["", "VAN", "PICK-UP", "VAN / PICK-UP", "BUS", "2-8 VAN"]
SECTOR_OPTIONS = ["", "Pharma", "Consumer", "Bulk / Pick-Up", "2-8", "Govt / Urgent", "Substitute", "Fleet", "Bus"]
NEEDS_OPTIONS = ["Yes", "No", "Optional"]
ROUTE_COLUMN_ORDER = ["S/N", "Area Code", "AREA", "Sector", "Driver Code", "Drivers Name", "Helper Code", "Helpers Name", "VEH NO", "Permitted Areas", "Division Category"]

KEEP_HELPERS = ["H116", "H131", "H121", "H119", "H046", "H070", "H129", "H113", "H132", "H118", "H115", "H122", "H114", "H066", "H011", "H005", "H023", "H050", "H062", "H051", "H104", "H130", "H034", "H013", "H109", "H024", "H026", "H049", "H099", "H082", "H017", "H126"]
KEEP_DRIVERS = ["D085", "D034", "D101", "D038", "D107", "D048", "D104", "D040", "D019", "D064", "D029", "D036", "D011", "D050", "D094", "D109", "D010", "D102", "D027", "D024", "D023", "D026", "D032", "D047", "D061", "D044", "D052", "D099", "D042", "D103", "D037", "D046", "D049", "D089", "D054", "D088", "D098", "D033"]

SEED_AREAS_IMAGE = [
    ("PH-FUJ", "FUJAIRAH", "Pharma", "Yes", 1, "Fujairah"), 
    ("PH-RAK", "RAK / UAQ", "Pharma", "Yes", 2, "RAK"),
    ("PH-JAB", "JABEL ALI", "Pharma", "Yes", 3, "Dubai"),
    ("PH-ALQ1", "ALQOUZ-1", "Pharma", "Yes", 4, "Dubai"), 
    ("PH-ALQ2", "ALQOUZ-2", "Pharma", "Yes", 5, "Dubai"),
    ("PH-JUM", "JUMAIRAH", "Pharma", "Yes", 6, "Dubai"), 
    ("PH-BUR", "BUR DUBAI", "Pharma", "Yes", 7, "Dubai"),
    ("PH-MIR", "MIRDIFF", "Pharma", "Yes", 8, "Dubai"), 
    ("PH-QUS", "QUSAIS", "Pharma", "Yes", 9, "Dubai"),
    ("PH-DEI", "DEIRA", "Pharma", "Yes", 10, "Dubai"), 
    ("PH-AJM", "AJMAN", "Pharma", "Yes", 11, "Ajman"),
    ("PH-SHJS", "SHARJAH SANAYA", "Pharma", "Yes", 12, "Sharjah"),
    ("PH-SHJ", "SHARJAH ( BUHAIRA & ROLLA)", "Pharma", "Yes", 13, "Sharjah"), 
    ("28-CC1", "COLD CHAIN/URGENT ORDERS", "2-8", "No", 14, "Dubai, Sharjah, Ajman"), 
    ("28-CC2", "COLD CHAIN/URGENT ORDERS", "2-8", "No", 15, "Dubai, Sharjah, Ajman"), 
    ("PH-SAMP", "Sample Driver", "Pharma", "Yes", 16, "Dubai"), 
    ("PH-2ND1", "SECOND TRIP", "Pharma", "Yes", 17, "Dubai"), 
    ("PH-2ND2", "SECOND TRIP", "Pharma", "Yes", 18, "Dubai"), 
    ("GOV-1", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 19, "Dubai"), 
    ("GOV-2", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 20, "Dubai"),
    ("GOV-3", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 21, "Dubai"), 
    ("FLE-1", "FLEET SERVICE/RTA WORK", "Fleet", "No", 22, "Dubai"),
    ("PU-SUB", "SUBTITUTE / PICK UP", "Substitute", "No", 23, "Dubai"), 
    ("PU-1", "PICK UP", "Bulk / Pick-Up", "Yes", 24, "Dubai"), 
    ("PU-2", "PICK UP (SHARJAH)", "Bulk / Pick-Up", "Yes", 25, "Sharjah"), 
    ("PU-3", "PICK UP", "Bulk / Pick-Up", "Yes", 26, "Dubai"), 
    ("PU-4", "PICK UP (SHARJAH)", "Bulk / Pick-Up", "Yes", 27, "Sharjah"), 
    ("PU-5", "PICK UP", "Bulk / Pick-Up", "Yes", 28, "Dubai"), 
    ("PU-6", "PICK UP", "Bulk / Pick-Up", "Yes", 29, "Dubai"), 
    ("CON-ALQ", "ALQOUZ-1 & ALQOUZ-2", "Consumer", "Yes", 30, "Dubai"), 
    ("CON-JAB", "JABEL ALI", "Consumer", "Yes", 31, "Dubai"), 
    ("CON-MIR", "MIRDIFF", "Consumer", "Yes", 32, "Dubai"), 
    ("CON-BUR", "BUR DUBAI", "Consumer", "Yes", 33, "Dubai"), 
    ("CON-RAK", "RAK / UAQ", "Consumer", "Yes", 34, "RAK"), 
    ("CON-PU1", "PICK UP (SHARJAH)", "Consumer", "Yes", 35, "Sharjah"), 
    ("CON-PU2", "PICK UP", "Consumer", "Yes", 36, "Dubai"), 
    ("CON-AJM", "AJMAN", "Consumer", "Yes", 37, "Ajman"), 
    ("CON-SHJS", "SHARJAH SANAYA", "Consumer", "Yes", 38, "Sharjah"), 
    ("CON-SUB", "SUBTITUTE / URGENT ORDERS", "Substitute", "No", 39, "Dubai")
]

if "db_initialized" not in st.session_state:
    def execute_global_init(force=False, load_default=False):
        try:
            bq = not force
            if load_default:
                for t in ['areas', 'drivers', 'helpers', 'vehicles']:
                    def_df = load_table(f"default_{t}")
                    if not def_df.empty:
                        run_query(f"DELETE FROM {t}", table_name=t, action="CLEAR_TABLE", bypass_queue=bq)
                        dicts = def_df.drop(columns=[c for c in ['id', 'S/N', 'sort_order', 'vacation_status', 'fb_id'] if c in def_df.columns]).to_dict('records')
                        cols = ', '.join(dicts[0].keys())
                        qmarks = ', '.join(['?'] * len(dicts[0]))
                        vals = [tuple(d.values()) for d in dicts]
                        run_query(f"INSERT OR REPLACE INTO {t} ({cols}) VALUES ({qmarks})", vals, table_name=t, action="INSERT_MANY", data=dicts, bypass_queue=bq)
                        continue
            
            current_areas = load_table("areas")
            if force or len(current_areas) == 0:
                run_query("DELETE FROM areas", table_name="areas", action="CLEAR_TABLE", bypass_queue=bq)
                areas_data = [{"code": c, "name": n, "sector": unify_text(s), "needs_driver": "Yes", "needs_helper": nh, "sort_order": o, "region": r} for c, n, s, nh, o, r in SEED_AREAS_IMAGE]
                run_query("INSERT OR IGNORE INTO areas (code, name, sector, needs_driver, needs_helper, sort_order, region) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                          [(c, n, unify_text(s), "Yes", nh, o, r) for c, n, s, nh, o, r in SEED_AREAS_IMAGE], table_name="areas", action="INSERT_MANY", data=areas_data, bypass_queue=bq)
            
            d_df = load_table('drivers')
            if len(d_df) == 0:
                d_seed = [(RAW_NAME_MAP.get(code, "Unknown"), code, "VAN", "", "", "", "", "") for code in KEEP_DRIVERS]
                d_data = [{"name": RAW_NAME_MAP.get(code, "Unknown"), "code": code, "veh_type": "VAN", "sector": "", "needs_helper": "", "restriction": "", "anchor_area": "", "anchor_vehicle": ""} for code in KEEP_DRIVERS]
                run_query("INSERT OR IGNORE INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area, anchor_vehicle) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", d_seed, table_name="drivers", action="INSERT_MANY", data=d_data, bypass_queue=bq)
            
            h_df = load_table('helpers')
            if len(h_df) == 0:
                h_seed = [(RAW_NAME_MAP.get(code, "Unknown"), code, "", "No", "") for code in KEEP_HELPERS]
                h_data = [{"name": RAW_NAME_MAP.get(code, "Unknown"), "code": code, "restriction": "", "health_card": "No", "anchor_area": ""} for code in KEEP_HELPERS]
                run_query("INSERT OR IGNORE INTO helpers (name, code, restriction, health_card, anchor_area) VALUES (?, ?, ?, ?, ?)", h_seed, table_name="helpers", action="INSERT_MANY", data=h_data, bypass_queue=bq)

            v_df = load_table('vehicles')
            if len(v_df) == 0:
                v_seed = [(v_num, unify_text(v_type), permitted, unify_text(division), "", "Active") for v_num, v_type, permitted, division in SEED_VEHICLES]
                v_data = [{"number": v_num, "type": unify_text(v_type), "permitted_areas": permitted, "division": unify_text(division), "anchor_area": "", "status": "Active"} for v_num, v_type, permitted, division in SEED_VEHICLES]
                run_query("INSERT OR IGNORE INTO vehicles (number, type, permitted_areas, division, anchor_area, status) VALUES (?, ?, ?, ?, ?, ?)", v_seed, table_name="vehicles", action="INSERT_MANY", data=v_data, bypass_queue=bq)
            
            st.cache_data.clear()
        except Exception as e:
            st.error(f"Initialization Error: {e}")
            
    execute_global_init()
    st.session_state.db_initialized = True

# --- HIGH PERFORMANCE SCORING HELPERS ---
def build_experience_cache():
    history_df = load_table('history')
    exp_cache = {}
    if not history_df.empty:
        for _, r in history_df.iterrows():
            code = r['person_code']
            area = unify_text(r['area'])
            sector = unify_text(r.get('sector', 'Pharma'))
            end_date = parse_date_safe(r['end_date'] if pd.notna(r.get('end_date')) and str(r['end_date']).strip() else r['date'])
            
            if not end_date: continue

            if code not in exp_cache: exp_cache[code] = {'areas': {}, 'sectors': {}}
            if area not in exp_cache[code]['areas'] or end_date > exp_cache[code]['areas'][area]:
                exp_cache[code]['areas'][area] = end_date
            if sector not in exp_cache[code]['sectors'] or end_date > exp_cache[code]['sectors'][sector]:
                exp_cache[code]['sectors'][sector] = end_date
    return exp_cache

def build_vacation_cache():
    vacs_df = load_table('vacations')
    vac_cache = {}
    if not vacs_df.empty and 'person_code' in vacs_df.columns:
        for _, r in vacs_df.iterrows():
            code = r['person_code']
            if code not in vac_cache: vac_cache[code] = []
            s_val = parse_date_safe(r['start_date'])
            e_val = parse_date_safe(r['end_date'])
            if s_val and e_val:
                vac_cache[code].append((s_val, e_val))
    return vac_cache

def is_on_vacation(person_code, target_date, vac_cache):
    target_str = target_date.strftime("%Y-%m-%d")
    for start, end in vac_cache.get(person_code, []):
        if start <= target_str <= end: return True
    return False

def vacation_within_3_months(person_code, target_date, vac_cache):
    limit_date = (target_date + timedelta(days=90)).strftime("%Y-%m-%d")
    target_str = target_date.strftime("%Y-%m-%d")
    for start, end in vac_cache.get(person_code, []):
        if target_str < start <= limit_date: return parse_date_safe(start)
    return None

def months_until_next_vacation(person_code, vac_cache, target_date):
    target_str = target_date.strftime("%Y-%m-%d")
    past_vacs = [end for start, end in vac_cache.get(person_code, []) if end < target_str]
    if not past_vacs: return 0 
    last_vac = max(past_vacs)
    days_since = (target_date - datetime.strptime(last_vac, "%Y-%m-%d").date()).days
    return max(0, 365 - days_since) / 30.0

# --- WEIGHTED AI SCORING ALGORITHM (WITH MULTI-ANCHOR SUBSTRING SEARCH) ---
NEVER_WORKED_BONUS = 10000
NEVER_WORKED_SECTOR_BONUS = 8000
ANCHOR_MATCH_BONUS = 50000 
MONTHS_WEIGHT = 100
SECTOR_MONTHS_WEIGHT = 50
RECENT_AREA_PENALTY = -3000
VACATION_SOON_PENALTY = -1500

def calculate_candidate_score(candidate, area, req_veh, req_sector, target_date, exp_cache, vac_cache, role="Driver", hc_assigned=0):
    code = candidate['code']
    score = 0
    reasons = []
    target_str = target_date.strftime("%Y-%m-%d")

    if is_on_vacation(code, target_date, vac_cache):
        return None, "Excluded: On Vacation"
        
    if role == "Driver":
        p_veh = unify_text(candidate.get('veh_type', ''))
        if p_veh and p_veh not in [req_veh, ""] and not (p_veh == "VAN / PICK-UP" and req_veh in ["VAN", "PICK-UP"]):
            return None, f"Excluded: Vehicle Mismatch ({p_veh} != {req_veh})"

    anchors = [unify_text(a).upper() for a in str(candidate.get('anchor_area', '')).split(',') if a.strip()]
    if "NONE" in anchors: anchors.remove("NONE")
    if "" in anchors: anchors.remove("")

    if anchors:
        check_list = [unify_text(area['name']).upper(), unify_text(req_sector).upper(), unify_text(req_veh).upper()]
        matched = False
        for anc in anchors:
            if any(anc in chk or chk in anc for chk in check_list):
                matched = True
                break
            
        if matched:
            score += ANCHOR_MATCH_BONUS
            reasons.append(f"Anchor Match (+{ANCHOR_MATCH_BONUS})")
        else:
            return None, f"Excluded: Anchored strictly to {', '.join(anchors)}"

    last_worked_area = exp_cache.get(code, {}).get('areas', {}).get(unify_text(area['name']))
    if not last_worked_area:
        score += NEVER_WORKED_BONUS
        reasons.append(f"Never worked Area (+{NEVER_WORKED_BONUS})")
    else:
        months_since = (datetime.strptime(target_str, "%Y-%m-%d") - datetime.strptime(last_worked_area, "%Y-%m-%d")).days / 30.0
        if months_since < 3:
            score += RECENT_AREA_PENALTY
            reasons.append(f"Recent Area Visit <3m ({RECENT_AREA_PENALTY})")
        else:
            time_score = int(months_since * MONTHS_WEIGHT)
            score += time_score
            reasons.append(f"{months_since:.1f}m since area (+{time_score})")

    last_worked_sector = exp_cache.get(code, {}).get('sectors', {}).get(req_sector)
    if not last_worked_sector:
        score += NEVER_WORKED_SECTOR_BONUS
        reasons.append(f"Never worked {req_sector} Sector (+{NEVER_WORKED_SECTOR_BONUS})")
    else:
        months_since_sec = (datetime.strptime(target_str, "%Y-%m-%d") - datetime.strptime(last_worked_sector, "%Y-%m-%d")).days / 30.0
        time_score_sec = int(months_since_sec * SECTOR_MONTHS_WEIGHT)
        score += time_score_sec
        reasons.append(f"{months_since_sec:.1f}m since {req_sector} Sector (+{time_score_sec})")

    vac_start = vacation_within_3_months(code, target_date, vac_cache)
    if vac_start:
        score += VACATION_SOON_PENALTY
        reasons.append(f"Vacation soon ({VACATION_SOON_PENALTY})")

    if role == "Helper":
        if "Consumer" in unify_text(req_sector):
            if candidate.get('health_card') == 'Yes':
                if hc_assigned < 3:
                    score += 5000; reasons.append("Required HC for Consumer (+5000)")
                else:
                    score += 500; reasons.append("HC in Consumer (+500)")
            else:
                if hc_assigned < 3:
                    score -= 2000; reasons.append("Non-HC Penalty (-2000)")
        else:
            if candidate.get('health_card') == 'Yes':
                if hc_assigned < 3:
                    score -= 3000; reasons.append("Reserved HC for Consumer (-3000)")
                else:
                    score -= 200; reasons.append("Saved HC (-200)")

    return score, " | ".join(reasons)

def check_route_requirements(areas_df, drivers_df, helpers_df, vehicles_df, vac_cache, today_date):
    errors = []
    req_veh = {"VAN": 0, "PICK-UP": 0, "BUS": 0, "2-8 VAN": 0}
    for _, area in areas_df.iterrows():
        if area.get('needs_driver', 'Yes') == 'No': continue
        sec = unify_text(area.get('sector', ''))
        name = unify_text(area.get('name', ''))
        if "2-8" in sec or "COLD CHAIN" in name.upper(): req_veh["2-8 VAN"] += 1
        elif "Govt" in sec or "GOVT" in name.upper(): req_veh["BUS"] += 1
        elif "Pick-Up" in sec or "PICK UP" in name.upper(): req_veh["PICK-UP"] += 1
        else: req_veh["VAN"] += 1
            
    avail_veh = {"VAN": 0, "PICK-UP": 0, "BUS": 0, "2-8 VAN": 0}
    active_vehicles_df = vehicles_df[~vehicles_df.get('status', 'Active').str.contains('Under Service|In for Service', case=False, na=False)]
    for _, v in active_vehicles_df.iterrows():
        vtype = unify_text(v.get('type', 'VAN'))
        if vtype in avail_veh: avail_veh[vtype] += 1
        elif vtype == "VAN / PICK-UP":
            avail_veh["VAN"] += 1
            avail_veh["PICK-UP"] += 1
        
    for vtype, required in req_veh.items():
        if avail_veh[vtype] < required:
            errors.append(f"🚗 Missing **{vtype}** Vehicles: Route strictly needs **{required}**, but you only have **{avail_veh[vtype]}** active.")

    strict_d_req = len(areas_df[areas_df['needs_driver'] == 'Yes'])
    avail_d = len([1 for _, r in drivers_df.iterrows() if not is_on_vacation(r['code'], today_date, vac_cache)])
    if avail_d < strict_d_req:
        errors.append(f"🚛 Missing Drivers: Route strictly needs **{strict_d_req}** active drivers, but you only have **{avail_d}**.")
        
    return errors


# --- GLOBAL SHARED VARIABLES ---
areas_df_global = load_table('areas')
area_list_global = [""] + (areas_df_global['name'].drop_duplicates().tolist() if not areas_df_global.empty else [])
multi_anchor_opts = list(set([a for a in area_list_global + SECTOR_OPTIONS + VEHICLE_OPTIONS if a != ""]))
multi_anchor_opts.sort()

all_d = load_table('drivers')
all_h = load_table('helpers')
vehicles_global = load_table('vehicles')

drv_codes_opts = ["UNASSIGNED", ""] + all_d['code'].dropna().unique().tolist() if not all_d.empty else ["UNASSIGNED", ""]
drv_names_opts = ["UNASSIGNED", ""] + all_d['name'].dropna().unique().tolist() if not all_d.empty else ["UNASSIGNED", ""]
hlp_codes_opts = ["UNASSIGNED", "N/A", ""] + all_h['code'].dropna().unique().tolist() if not all_h.empty else ["UNASSIGNED", "N/A", ""]
hlp_names_opts = ["UNASSIGNED", "NO HELPER REQUIRED", ""] + all_h['name'].dropna().unique().tolist() if not all_h.empty else ["UNASSIGNED", "NO HELPER REQUIRED", ""]

v_num_opts = ["UNASSIGNED", "N/A", ""] + vehicles_global['number'].dropna().unique().tolist() if not vehicles_global.empty else ["UNASSIGNED", "N/A", ""]

# DYNAMIC DROPDOWN HELPERS
def get_dynamic_opts(df, col_name, standard_opts):
    opts = list(set(standard_opts + (df[col_name].dropna().tolist() if not df.empty and col_name in df.columns else [])))
    return sorted([str(x) for x in opts if pd.notna(x) and str(x).strip() != ""]) + [""]

# --- APP ROUTING ---
menu = ["1. AI Route Planner", "2. Database Management", "3. Past Experience Builder", "4. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)

# ==========================================
# SCREEN 1: AI ROUTE PLANNER
# ==========================================
if choice == "1. AI Route Planner":
    
    st.subheader("📊 Today's Availability Dashboard")
    today = date.today()
    vac_cache = build_vacation_cache()
    
    vac_d_names = [f"[{r['code']}] {r['name']}" for _, r in all_d.iterrows() if is_on_vacation(r['code'], today, vac_cache)] if not all_d.empty else []
    avail_d_names = [f"[{r['code']}] {r['name']}" for _, r in all_d.iterrows() if not is_on_vacation(r['code'], today, vac_cache)] if not all_d.empty else []
    solo_d_names = [f"[{r['code']}] {r['name']}" for _, r in all_d.iterrows() if (not is_on_vacation(r['code'], today, vac_cache)) and ((r.get('needs_helper', 'Yes') == 'No') or (unify_text(r.get('veh_type', '')) in ['BUS', '2-8 VAN']))] if not all_d.empty else []
    
    vac_h_names = [f"[{r['code']}] {r['name']}" for _, r in all_h.iterrows() if is_on_vacation(r['code'], today, vac_cache)] if not all_h.empty else []
    avail_h_names = [f"[{r['code']}] {r['name']}" for _, r in all_h.iterrows() if not is_on_vacation(r['code'], today, vac_cache)] if not all_h.empty else []
    
    draft_routes = load_table('draft_routes')
    active_routes = load_table('active_routes')
    active_draft_df = draft_routes if not draft_routes.empty else active_routes
    assigned_d = active_draft_df['driver_code'].dropna().tolist() if not active_draft_df.empty else []
    assigned_h = active_draft_df['helper_code'].dropna().tolist() if not active_draft_df.empty else []
    
    extra_d_names = [n for n in avail_d_names if n.split("]")[0][1:] not in assigned_d]
    extra_h_names = [n for n in avail_h_names if n.split("]")[0][1:] not in assigned_h]

    req_helpers = len(avail_d_names) - len(solo_d_names)
    shortage = req_helpers - len(avail_h_names)

    col_a, col_e1, col_b, col_e2, col_c = st.columns(5)
    
    with col_a:
        st.metric("🚛 Total Drivers Available", f"{len(avail_d_names)} / {len(all_d)}")
        with st.popover("🔍 View Drivers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            if avail_d_names: st.markdown("**✅ Available:**<ol>" + "".join([f"<li>{n}</li>" for n in avail_d_names]) + "</ol>", unsafe_allow_html=True)
            if vac_d_names: st.markdown(f"**🌴 On Vacation ({len(vac_d_names)}):**<ol>" + "".join([f"<li>{n}</li>" for n in vac_d_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_e1:
        st.metric("🚛 Extra Drivers (Surplus)", f"{len(extra_d_names)}")
        with st.popover("🔍 View Extra Drivers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            st.caption("Drivers available but not currently assigned to a route.")
            if extra_d_names: st.markdown("<ol>" + "".join([f"<li>{n}</li>" for n in extra_d_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_b:
        st.metric("👤 Total Helpers Available", f"{len(avail_h_names)} / {len(all_h)}")
        with st.popover("🔍 View Helpers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            if avail_h_names: st.markdown("**✅ Available:**<ol>" + "".join([f"<li>{n}</li>" for n in avail_h_names]) + "</ol>", unsafe_allow_html=True)
            if vac_h_names: st.markdown(f"**🌴 On Vacation ({len(vac_h_names)}):**<ol>" + "".join([f"<li>{n}</li>" for n in vac_h_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            
    with col_e2:
        st.metric("👤 Extra Helpers (Surplus)", f"{len(extra_h_names)}")
        with st.popover("🔍 View Extra Helpers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            st.caption("Helpers available but not currently assigned to a route.")
            if extra_h_names: st.markdown("<ol>" + "".join([f"<li>{n}</li>" for n in extra_h_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_c:
        if shortage > 0: 
            st.metric("⚠️ Helper Shortage", f"-{shortage}", delta_color="inverse")
            with st.popover("🚨 View Shortage Details"):
                st.error(f"**Shortage of {shortage} Helpers!**")
                st.write(f"Active Drivers: **{len(avail_d_names)}**")
                st.write(f"Minus Solo Drivers: **{len(solo_d_names)}**")
                st.write(f"Helpers Needed: **{req_helpers}**")
                st.write(f"Helpers Available: **{len(avail_h_names)}**")
        else: 
            st.metric("✅ Route Status", "Sufficient Staff", delta_color="normal")

    st.divider()

    # --- ROUTE PLANS ---
    if not draft_routes.empty:
        st.warning("✨ **DRAFT MODE**: This plan is NOT saved to History yet! You can manually edit any cell below, then save the draft or Confirm to log experiences.")
        
        d_start_str = draft_routes.iloc[0].get('start_date') if 'start_date' in draft_routes.columns and str(draft_routes.iloc[0].get('start_date')) else None
        d_end_str = draft_routes.iloc[0].get('end_date') if 'end_date' in draft_routes.columns and str(draft_routes.iloc[0].get('end_date')) else None
        
        plan_start_val = datetime.strptime(d_start_str, "%Y-%m-%d").date() if d_start_str and d_start_str != "None" and d_start_str != "" else today
        plan_end_val = datetime.strptime(d_end_str, "%Y-%m-%d").date() if d_end_str and d_end_str != "None" and d_end_str != "" else today + timedelta(days=90)
        
        c_d1, c_d2 = st.columns(2)
        plan_start = c_d1.date_input("Plan Start Date", value=plan_start_val)
        plan_end = c_d2.date_input("Plan End Date", value=plan_end_val)
        
        disp_draft = draft_routes.copy()
        
        if 'S/N' not in disp_draft.columns:
            disp_draft.insert(0, 'S/N', disp_draft.get('order_num', range(1, 1 + len(disp_draft))))
            
        if 'veh_perm' in disp_draft.columns:
            disp_draft = disp_draft.rename(columns={"veh_perm": "Permitted Areas"})
        else:
            disp_draft['Permitted Areas'] = "All"
            
        disp_draft = disp_draft.rename(columns={"area_code": "Area Code", "area_name": "AREA", "veh_num": "VEH NO", "sector": "Sector", "div_cat": "Division Category"})
        if "driver_name" in disp_draft.columns: disp_draft["Drivers Name"] = disp_draft["driver_name"]
        if "driver_code" in disp_draft.columns: disp_draft["Driver Code"] = disp_draft["driver_code"]
        if "helper_name" in disp_draft.columns: disp_draft["Helpers Name"] = disp_draft["helper_name"]
        if "helper_code" in disp_draft.columns: disp_draft["Helper Code"] = disp_draft["helper_code"]
        
        disp_draft = disp_draft[[c for c in ROUTE_COLUMN_ORDER if c in disp_draft.columns]]
        
        edited_df = st.data_editor(
            disp_draft, 
            use_container_width=True, hide_index=True, key="route_editor", 
            column_order=ROUTE_COLUMN_ORDER,
            column_config={
                "Area Code": st.column_config.TextColumn(disabled=True),
                "Driver Code": st.column_config.SelectboxColumn("CODE", options=drv_codes_opts),
                "Drivers Name": st.column_config.SelectboxColumn("Drivers Name", options=drv_names_opts),
                "Helper Code": st.column_config.SelectboxColumn("CODE", options=hlp_codes_opts),
                "Helpers Name": st.column_config.SelectboxColumn("Helpers Name", options=hlp_names_opts),
                "VEH NO": st.column_config.SelectboxColumn("VEH NO", options=v_num_opts)
            }
        )
        
        col_down, col_save, col_app, col_can = st.columns([1, 1, 1.2, 1])
        output = generate_excel_with_sn([edited_df], ['Draft Route Plan'])
        col_down.download_button("📥 Download Draft Excel", data=output, file_name=f"Draft_Plan_{today}.xlsx")
        
        if col_save.button("💾 Save Draft Plan", type="secondary"):
            if "route_editor" in st.session_state:
                changes = st.session_state["route_editor"].get("edited_rows", {})
                for row_idx, col_changes in changes.items():
                    for col_name, new_val in col_changes.items():
                        edited_df.iat[row_idx, edited_df.columns.get_loc(col_name)] = new_val
                        
            run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE") 
            p_s = plan_start.strftime("%Y-%m-%d")
            p_e = plan_end.strftime("%Y-%m-%d")
            
            insert_data = []
            new_dicts = []
            for index, r in edited_df.iterrows():
                sn_val = r.get('S/N', index + 1)
                orig_row = disp_draft.iloc[index] if index < len(disp_draft) else {}
                
                d_code = r.get('Driver Code', '')
                d_name = r.get('Drivers Name', '')
                if d_name != orig_row.get('Drivers Name'):
                    match = all_d[all_d['name'] == d_name]
                    if not match.empty: d_code = match.iloc[0]['code']
                elif d_code != orig_row.get('Driver Code'):
                    match = all_d[all_d['code'] == d_code]
                    if not match.empty: d_name = match.iloc[0]['name']
                    
                h_code = r.get('Helper Code', '')
                h_name = r.get('Helpers Name', '')
                if h_name != orig_row.get('Helpers Name'):
                    match = all_h[all_h['name'] == h_name]
                    if not match.empty: h_code = match.iloc[0]['code']
                elif h_code != orig_row.get('Helper Code'):
                    match = all_h[all_h['code'] == h_code]
                    if not match.empty: h_name = match.iloc[0]['name']

                a_code_val = r.get('Area Code', '')
                insert_data.append((sn_val, a_code_val, r.get('AREA', ''), unify_text(r.get('Sector', '')), d_code, d_name, h_code, h_name, r.get('VEH NO', ''), unify_text(r.get('Division Category', '')), p_s, p_e, r.get('Permitted Areas', '')))
                new_dicts.append({"order_num":sn_val, "area_code":a_code_val, "area_name":r.get('AREA', ''), "sector":unify_text(r.get('Sector', '')), "driver_code":d_code, "driver_name":d_name, "helper_code":h_code, "helper_name":h_name, "veh_num":r.get('VEH NO', ''), "div_cat":unify_text(r.get('Division Category', '')), "start_date":p_s, "end_date":p_e, "veh_perm":r.get('Permitted Areas', '')})
                
            q_dr = "INSERT INTO draft_routes (order_num, area_code, area_name, sector, driver_code, driver_name, helper_code, helper_name, veh_num, div_cat, start_date, end_date, veh_perm) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            run_query(q_dr, insert_data, table_name="draft_routes", action="INSERT_MANY", data=new_dicts)
            st.success("Draft Saved Successfully!")
            st.rerun()

        if col_app.button("✅ Confirm Plan & Save Experiences", type="primary"):
            if "route_editor" in st.session_state:
                changes = st.session_state["route_editor"].get("edited_rows", {})
                for row_idx, col_changes in changes.items():
                    for col_name, new_val in col_changes.items():
                        edited_df.iat[row_idx, edited_df.columns.get_loc(col_name)] = new_val
                        
            run_query("DELETE FROM active_routes", table_name="active_routes", action="CLEAR_TABLE") 
            p_s = plan_start.strftime("%Y-%m-%d")
            p_e = plan_end.strftime("%Y-%m-%d")
            
            active_data = []
            active_dicts = []
            hist_data = []
            hist_dicts = []
            for index, r in edited_df.iterrows():
                sn_val = r.get('S/N', index + 1)
                orig_row = disp_draft.iloc[index] if index < len(disp_draft) else {}
                
                d_code = r.get('Driver Code', '')
                d_name = r.get('Drivers Name', '')
                if d_name != orig_row.get('Drivers Name'):
                    match = all_d[all_d['name'] == d_name]
                    if not match.empty: d_code = match.iloc[0]['code']
                elif d_code != orig_row.get('Driver Code'):
                    match = all_d[all_d['code'] == d_code]
                    if not match.empty: d_name = match.iloc[0]['name']
                    
                h_code = r.get('Helper Code', '')
                h_name = r.get('Helpers Name', '')
                if h_name != orig_row.get('Helpers Name'):
                    match = all_h[all_h['name'] == h_name]
                    if not match.empty: h_code = match.iloc[0]['code']
                elif h_code != orig_row.get('Helper Code'):
                    match = all_h[all_h['code'] == h_code]
                    if not match.empty: h_name = match.iloc[0]['name']

                a_code_val = r.get('Area Code', '')
                active_data.append((sn_val, a_code_val, r.get('AREA', ''), d_code, d_name, h_code, h_name, r.get('VEH NO', ''), p_s, p_e, r.get('Permitted Areas', '')))
                active_dicts.append({"order_num":sn_val, "area_code":a_code_val, "area_name":r.get('AREA', ''), "driver_code":d_code, "driver_name":d_name, "helper_code":h_code, "helper_name":h_name, "veh_num":r.get('VEH NO', ''), "start_date":p_s, "end_date":p_e, "veh_perm":r.get('Permitted Areas', '')})
                
                for code, name, ptype in [(d_code, d_name, "Driver"), (h_code, h_name, "Helper")]:
                    if pd.notna(code) and str(code).strip() not in ["UNASSIGNED", "N/A", "", "None"]:
                        hist_data.append((ptype, str(code).strip(), str(name).strip(), r.get('AREA', ''), unify_text(r.get('Sector', '')), p_s, p_e))
                        hist_dicts.append({"person_type":ptype, "person_code":str(code).strip(), "person_name":str(name).strip(), "area":r.get('AREA', ''), "sector":unify_text(r.get('Sector', '')), "date":p_s, "end_date":p_e})
            
            q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num, start_date, end_date, veh_perm) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            run_query(q_ar, active_data, table_name="active_routes", action="INSERT_MANY", data=active_dicts)
            
            q_hist = "INSERT OR IGNORE INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)"
            run_query(q_hist, hist_data, table_name="history", action="INSERT_MANY", data=hist_dicts)
            
            run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
            st.success(f"Plan Approved! System logged these experiences from {p_s} to {p_e}.")
            st.rerun()
            
        if col_can.button("🗑️ Discard Draft", type="secondary"):
            run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
            st.rerun()

    elif not active_routes.empty:
        start_dt = active_routes.iloc[0].get('start_date', 'Unknown')
        end_dt = active_routes.iloc[0].get('end_date', 'Unknown')
        st.subheader(f"📋 Current Active Route Plan ({start_dt} to {end_dt})")
        
        areas = load_table('areas')
        active_with_sector = active_routes.copy()
        if not areas.empty:
            sector_map = dict(zip(areas['name'], areas['sector']))
            active_with_sector['Sector'] = active_with_sector['area_name'].map(sector_map).fillna("Pharma")
            
        if 'veh_perm' in active_with_sector.columns:
            active_with_sector = active_with_sector.rename(columns={"veh_perm": "Permitted Areas"})
        else:
            active_with_sector['Permitted Areas'] = "All"
            
        disp_active = active_with_sector.rename(columns={"area_code": "Area Code", "area_name": "AREA", "veh_num": "VEH NO", "driver_name": "Drivers Name", "driver_code": "Driver Code", "helper_name": "Helpers Name", "helper_code": "Helper Code", "div_cat": "Division Category"})
        disp_active = disp_active[[c for c in ROUTE_COLUMN_ORDER if c in disp_active.columns]]
        if 'S/N' not in disp_active.columns: disp_active.insert(0, 'S/N', range(1, 1 + len(disp_active)))
        
        st.dataframe(
            disp_active, use_container_width=True, hide_index=True, column_order=ROUTE_COLUMN_ORDER,
            column_config={"Area Code": st.column_config.TextColumn(disabled=True), "Driver Code": st.column_config.TextColumn("CODE"), "Helper Code": st.column_config.TextColumn("CODE")}
        )
        col_dl, col_rm = st.columns(2)
        output = generate_excel_with_sn([disp_active], ['Active Route Plan'])
        col_dl.download_button("📥 Download Active Plan Excel", data=output, file_name=f"Active_Plan_{start_dt}.xlsx")
        
        if col_rm.button("🗑️ Remove Current Plan", type="secondary"):
            run_query("DELETE FROM active_routes", table_name="active_routes", action="CLEAR_TABLE")
            st.rerun()

    else:
        st.subheader("📋 Route Plan Dashboard")
        st.info("No Active or Draft routes exist. Generate an AI route below.")
        empty_df = pd.DataFrame(columns=ROUTE_COLUMN_ORDER)
        st.dataframe(empty_df, use_container_width=True, hide_index=True, column_config={"Area Code": st.column_config.TextColumn(disabled=True), "Driver Code": "CODE", "Helper Code": "CODE"})

    st.divider()

    # --- GENERATOR ENGINE ---
    st.header("⚙️ Generate Smart AI Route Plan")
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=today)
    rot_type = col2.radio("Who is rotating this month?", ["Drivers", "Helpers"])
    
    if "attempt_generate" not in st.session_state: st.session_state.attempt_generate = False
    if "force_bypass" not in st.session_state: st.session_state.force_bypass = False

    if st.button("Generate Smart AI Route Plan", type="primary"):
        st.session_state.attempt_generate = True
        st.session_state.force_bypass = False

    if st.session_state.attempt_generate:
        areas = load_table('areas')
        vehicles = load_table('vehicles')
        val_errors = check_route_requirements(areas, all_d, all_h, vehicles, vac_cache, month_target)
        
        if val_errors and not st.session_state.force_bypass:
            st.error("🚨 **ROUTE GENERATION HALTED: DATABASE SHORTAGE DETECTED**")
            for err in val_errors: st.warning(err)
            st.markdown("Cannot fulfill the Route Plan with current database. Please add the missing vehicles/drivers, or bypass this warning to assign what you have.")
            
            c_byp, c_can = st.columns(2)
            if c_byp.button("⚠️ Bypass Warnings & Force Generate"):
                st.session_state.force_bypass = True
                st.rerun()
            if c_can.button("❌ Cancel Generation"):
                st.session_state.attempt_generate = False
                st.session_state.force_bypass = False
                st.rerun()
        else:
            with st.spinner("Applying Weighted Scoring & predicting Future Replacements..."):
                exp_cache = build_experience_cache()
                vac_cache = build_vacation_cache()
                
                run_query("DELETE FROM route_plan_reasons", table_name="route_plan_reasons", action="CLEAR_TABLE")
                run_query("DELETE FROM vacation_predictions", table_name="vacation_predictions", action="CLEAR_TABLE")
                
                route_plan = []
                used_drivers, used_helpers, used_vehicles = set(), set(), set()
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                p_s_gen = month_target.strftime("%Y-%m-%d")
                p_e_gen = (month_target + timedelta(days=90)).strftime("%Y-%m-%d")
                
                reason_data = []
                reason_dicts = []
                predict_data = []
                predict_dicts = []
                
                consumer_hc_assigned = 0
                
                # Calculate Surplus for Optional Assignments
                strict_d_req = len(areas[areas['needs_driver'] == 'Yes'])
                avail_d_count = len([1 for _, r in all_d.iterrows() if not is_on_vacation(r['code'], month_target, vac_cache)])
                surplus_drivers = avail_d_count - strict_d_req

                strict_h_req = len(areas[areas['needs_helper'] == 'Yes'])
                avail_h_count = len([1 for _, r in all_h.iterrows() if not is_on_vacation(r['code'], month_target, vac_cache)])
                surplus_helpers = avail_h_count - strict_h_req

                for _, area in areas.iterrows():
                    area_code = area.get('code', '')
                    area_name = unify_text(area['name'])
                    req_sector = unify_text(area.get('sector', 'Pharma'))
                    needs_helper = area.get('needs_helper', 'Yes') in ['Yes', 'Optional']
                    
                    div_cat = "PHARMA DIVISION"
                    if "2-8" in req_sector or "GOVT" in req_sector.upper() or "FLEET" in req_sector.upper(): div_cat = "2-8 / URGENT ORDERS"
                    elif "PICK-UP" in req_sector.upper() or "SUBSTITUTE" in req_sector.upper() or "BULK" in req_sector.upper(): div_cat = "PICK-UPS / URGENT COLD CHAIN"
                    elif "CONSUMER" in req_sector.upper(): div_cat = "CONSUMER DIVISION"

                    req_veh = "VAN"
                    if "2-8" in req_sector or "COLD CHAIN" in area_name.upper(): req_veh = "2-8 VAN"
                    elif "GOVT" in req_sector.upper() or "GOVT" in area_name.upper(): req_veh = "BUS"
                    elif "PICK-UP" in req_sector.upper() or "PICK UP" in area_name.upper(): req_veh = "PICK-UP"

                    if not active_routes.empty and 'area_code' in active_routes.columns and area_code:
                        prev_assignment = active_routes[active_routes['area_code'] == area_code]
                    else:
                        prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                        
                    a_d_code, a_d_name, a_h_code, a_h_name, a_v_num, a_v_perm = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "All"

                    # 1. ASSIGN DRIVER
                    nd = str(area.get('needs_driver', 'Yes')).strip()
                    if nd == 'No':
                        a_d_code, a_d_name = "N/A", "NO DRIVER REQUIRED"
                    elif rot_type == "Drivers" or prev_assignment.empty or prev_assignment.iloc[0].get('driver_code') in ["N/A", "UNASSIGNED", None, ""]:
                        if nd == 'Optional' and surplus_drivers <= 0:
                            a_d_code, a_d_name = "N/A", "NO DRIVER REQUIRED"
                        else:
                            best_d, best_d_score, d_reason = None, -999999, "No valid drivers"
                            avail_dr = all_d[~all_d['code'].isin(used_drivers)]
                            
                            for _, p in avail_dr.iterrows():
                                score, rsn = calculate_candidate_score(p, area, req_veh, req_sector, month_target, exp_cache, vac_cache, role="Driver")
                                if score is not None and score > best_d_score:
                                    best_d_score, best_d, d_reason = score, p, rsn
                                    
                            if best_d is not None:
                                a_d_code, a_d_name = best_d['code'], best_d['name']
                                used_drivers.add(a_d_code)
                                if nd == 'Optional': surplus_drivers -= 1
                                
                                reason_data.append((p_s_gen, area_name, "Driver", a_d_name, best_d_score, d_reason, timestamp))
                                reason_dicts.append({"plan_date":p_s_gen, "area":area_name, "role":"Driver", "selected_person":a_d_name, "score":best_d_score, "reasons":d_reason, "generated_at":timestamp})
                                
                                vac_start = vacation_within_3_months(a_d_code, month_target, vac_cache)
                                if vac_start:
                                    repl_d, best_r_score, _ = None, -999999, ""
                                    for _, rp in all_d[~all_d['code'].isin([a_d_code])].iterrows():
                                        r_score, _ = calculate_candidate_score(rp, area, req_veh, req_sector, datetime.strptime(vac_start, "%Y-%m-%d").date(), exp_cache, vac_cache, role="Driver")
                                        if r_score is not None and r_score > best_r_score:
                                            best_r_score, repl_d = r_score, rp
                                    repl_name = repl_d['name'] if repl_d is not None else "CRITICAL SHORTAGE"
                                    predict_data.append((a_d_code, a_d_name, "Driver", vac_start, "Scheduled Vacation", repl_name, vac_start))
                                    predict_dicts.append({"person_code":a_d_code, "person_name":a_d_name, "role":"Driver", "suggested_start":vac_start, "reason":"Scheduled Vacation", "replacement_person":repl_name, "replacement_date":vac_start})
                    else:
                        a_d_code, a_d_name = prev_assignment.iloc[0]['driver_code'], prev_assignment.iloc[0]['driver_name']
                        used_drivers.add(a_d_code)

                    # 2. ASSIGN HELPER
                    nh = str(area.get('needs_helper', 'Yes')).strip()
                    driver_needs_h = True
                    if a_d_code not in ["N/A", "UNASSIGNED"]:
                        d_row = all_d[all_d['code'] == a_d_code]
                        if not d_row.empty and str(d_row.iloc[0].get('needs_helper')).strip() == 'No':
                            driver_needs_h = False

                    if nh == 'No' or not driver_needs_h:
                        a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"
                    elif rot_type == "Helpers" or prev_assignment.empty or prev_assignment.iloc[0].get('helper_code') in ["N/A", "UNASSIGNED", None, ""]:
                        if nh == 'Optional' and surplus_helpers <= 0:
                            a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"
                        else:
                            best_h, best_h_score, h_reason = None, -999999, "No valid helpers"
                            avail_hl = all_h[~all_h['code'].isin(used_helpers)]
                            
                            for _, p in avail_hl.iterrows():
                                score, rsn = calculate_candidate_score(p, area, req_veh, req_sector, month_target, exp_cache, vac_cache, role="Helper", hc_assigned=consumer_hc_assigned)
                                if score is not None and score > best_h_score:
                                    best_h_score, best_h, h_reason = score, p, rsn
                                    
                            if best_h is not None:
                                a_h_code, a_h_name = best_h['code'], best_h['name']
                                used_helpers.add(a_h_code)
                                
                                if "Consumer" in unify_text(req_sector) and best_h.get('health_card') == 'Yes':
                                    consumer_hc_assigned += 1
                                    
                                if nh == 'Optional': surplus_helpers -= 1
                                    
                                reason_data.append((p_s_gen, area_name, "Helper", a_h_name, best_h_score, h_reason, timestamp))
                                reason_dicts.append({"plan_date":p_s_gen, "area":area_name, "role":"Helper", "selected_person":a_h_name, "score":best_h_score, "reasons":h_reason, "generated_at":timestamp})
                                
                                vac_start = vacation_within_3_months(a_h_code, month_target, vac_cache)
                                if vac_start:
                                    repl_h, best_r_score, _ = None, -999999, ""
                                    for _, rp in all_h[~all_h['code'].isin([a_h_code])].iterrows():
                                        r_score, _ = calculate_candidate_score(rp, area, req_veh, req_sector, datetime.strptime(vac_start, "%Y-%m-%d").date(), exp_cache, vac_cache, role="Helper", hc_assigned=consumer_hc_assigned)
                                        if r_score is not None and r_score > best_r_score:
                                            best_r_score, repl_h = r_score, rp
                                    repl_name = repl_h['name'] if repl_h is not None else "CRITICAL SHORTAGE"
                                    predict_data.append((a_h_code, a_h_name, "Helper", vac_start, "Scheduled Vacation", repl_name, vac_start))
                                    predict_dicts.append({"person_code":a_h_code, "person_name":a_h_name, "role":"Helper", "suggested_start":vac_start, "reason":"Scheduled Vacation", "replacement_person":repl_name, "replacement_date":vac_start})
                    else:
                        a_h_code, a_h_name = prev_assignment.iloc[0]['helper_code'], prev_assignment.iloc[0]['helper_name']
                        used_helpers.add(a_h_code)

                    # 3. ASSIGN VEHICLE
                    if a_d_code == "N/A":
                        a_v_num, a_v_perm = "N/A", "N/A"
                    elif a_d_code != "UNASSIGNED" and a_v_num == "UNASSIGNED":
                        d_type = all_d[all_d['code'] == a_d_code]['veh_type'].values[0] if not all_d[all_d['code'] == a_d_code].empty else "VAN"
                        tvt = req_veh if req_veh != "VAN" else unify_text(d_type)
                        
                        drv_row = all_d[all_d['code'] == a_d_code]
                        d_anch_veh_str = drv_row.iloc[0].get('anchor_vehicle', '') if 'anchor_vehicle' in drv_row.columns and not drv_row.empty else ""
                        d_anch_vehs = [v.strip().upper() for v in str(d_anch_veh_str).split(',') if v.strip()]
                        if "NONE" in d_anch_vehs: d_anch_vehs.remove("NONE")
                        if "" in d_anch_vehs: d_anch_vehs.remove("")
                        
                        a_anch_veh_str = area.get('anchor_vehicle', '')
                        a_anch_vehs = [v.strip().upper() for v in str(a_anch_veh_str).split(',') if v.strip()]
                        if "NONE" in a_anch_vehs: a_anch_vehs.remove("NONE")
                        if "" in a_anch_vehs: a_anch_vehs.remove("")
                        
                        potential_vs = []
                        active_vehicles_df = vehicles[~vehicles.get('status', 'Active').str.contains('Under Service|In for Service', case=False, na=False)]
                        for _, v in active_vehicles_df[~active_vehicles_df['number'].isin(used_vehicles)].iterrows():
                            v_num_chk = unify_text(v.get('number', '')).upper()
                            
                            is_area_anchored = v_num_chk in a_anch_vehs
                            is_drv_anchored = v_num_chk in d_anch_vehs
                            
                            v_anchors = [a.strip().upper() for a in str(v.get('anchor_area', '')).split(',') if a.strip()]
                            if "NONE" in v_anchors: v_anchors.remove("NONE")
                            if "" in v_anchors: v_anchors.remove("")
                            
                            is_veh_area_anchored = False
                            if v_anchors:
                                check_list = [unify_text(area_name).upper(), unify_text(req_sector).upper(), unify_text(tvt).upper()]
                                if any(any(anc in chk or chk in anc for chk in check_list) for anc in v_anchors):
                                    is_veh_area_anchored = True
                            
                            if is_area_anchored:
                                potential_vs.append((v, 2000))
                                continue
                            if is_drv_anchored:
                                potential_vs.append((v, 1000))
                                continue
                            if is_veh_area_anchored:
                                potential_vs.append((v, 100))
                                continue
                                
                            v_type = unify_text(v.get('type', 'VAN'))
                            type_match = False
                            if v_type == tvt: type_match = True
                            elif tvt in ["VAN", "PICK-UP"] and v_type == "VAN / PICK-UP": type_match = True
                            
                            if not type_match: continue
                            
                            v_perm_str = unify_text(v.get('permitted_areas', 'All'))
                            v_perm_chk = v_perm_str.upper()
                            a_reg = unify_text(area.get('region', 'Dubai')).upper()
                            
                            if v_perm_chk != "ALL" and a_reg not in v_perm_chk and v_perm_chk not in a_reg:
                                continue 
                            
                            potential_vs.append((v, 0)) # Normal Priority

                        potential_vs.sort(key=lambda x: x[1], reverse=True)

                        if potential_vs:
                            a_v_num = potential_vs[0][0]['number']
                            a_v_perm = potential_vs[0][0]['permitted_areas']
                            used_vehicles.add(a_v_num)

                    route_plan.append({
                        "Driver Code": a_d_code, "Drivers Name": a_d_name, 
                        "Area Code": area_code, "AREA": area_name, "Sector": req_sector, "Helper Code": a_h_code, "Helpers Name": a_h_name, 
                        "VEH NO": a_v_num, "Permitted Areas": a_v_perm, "Division Category": div_cat
                    })

                run_query("INSERT INTO route_plan_reasons (plan_date, area, role, selected_person, score, reasons, generated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", reason_data, table_name="route_plan_reasons", action="INSERT_MANY", data=reason_dicts)
                run_query("INSERT INTO vacation_predictions (person_code, person_name, role, suggested_start, reason, replacement_person, replacement_date) VALUES (?, ?, ?, ?, ?, ?, ?)", predict_data, table_name="vacation_predictions", action="INSERT_MANY", data=predict_dicts)

                run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
                draft_inserts = []
                draft_dicts = []
                for index, r in enumerate(route_plan):
                    draft_inserts.append((index+1, r['Area Code'], r['AREA'], r['Sector'], r['Driver Code'], r['Drivers Name'], r['Helper Code'], r['Helpers Name'], r['VEH NO'], r['Division Category'], p_s_gen, p_e_gen, r['Permitted Areas']))
                    draft_dicts.append({"order_num":index+1, "area_code":r['Area Code'], "area_name":r['AREA'], "sector":r['Sector'], "driver_code":r['Driver Code'], "driver_name":r['Drivers Name'], "helper_code":r['Helper Code'], "helper_name":r['Helpers Name'], "veh_num":r['VEH NO'], "div_cat":r['Division Category'], "start_date":p_s_gen, "end_date":p_e_gen, "veh_perm": r['Permitted Areas']})
                run_query("INSERT INTO draft_routes (order_num, area_code, area_name, sector, driver_code, driver_name, helper_code, helper_name, veh_num, div_cat, start_date, end_date, veh_perm) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", draft_inserts, table_name="draft_routes", action="INSERT_MANY", data=draft_dicts)
                
                st.session_state.attempt_generate = False
                st.session_state.force_bypass = False
                st.rerun()

    # --- AI REASONING EXPLANATION TABLE ---
    reasons_df = load_table('route_plan_reasons')
    predict_df = load_table('vacation_predictions')
    areas_exp_df = load_table('areas')
    if not reasons_df.empty and not areas_exp_df.empty and 'name' in areas_exp_df.columns:
        with st.expander("🤖 View AI Reasoning & Future Replacement Logs", expanded=False):
            st.caption("Detailed breakdown of why the AI selected each candidate based on the weighted scoring logic.")
            
            explain_list = []
            for area in areas_exp_df['name'].unique():
                d_rsn = reasons_df[(reasons_df['area'] == area) & (reasons_df['role'] == 'Driver')]
                h_rsn = reasons_df[(reasons_df['area'] == area) & (reasons_df['role'] == 'Helper')]
                
                d_name = d_rsn.iloc[0]['selected_person'] if not d_rsn.empty else "Kept Previous"
                d_text = f"[{d_rsn.iloc[0]['score']}] {d_rsn.iloc[0]['reasons']}" if not d_rsn.empty else "N/A"
                
                h_name = h_rsn.iloc[0]['selected_person'] if not h_rsn.empty else "Kept Previous"
                h_text = f"[{h_rsn.iloc[0]['score']}] {h_rsn.iloc[0]['reasons']}" if not h_rsn.empty else "N/A"
                
                repl_info, repl_date = "Safe", "-"
                d_repl = predict_df[predict_df['person_name'] == d_name] if not predict_df.empty else pd.DataFrame()
                h_repl = predict_df[predict_df['person_name'] == h_name] if not predict_df.empty else pd.DataFrame()
                
                if not d_repl.empty:
                    repl_info = f"DRIVER Replace: {d_repl.iloc[0].get('replacement_person', 'UNK')}"
                    repl_date = d_repl.iloc[0].get('replacement_date', '-')
                elif not h_repl.empty:
                    repl_info = f"HELPER Replace: {h_repl.iloc[0].get('replacement_person', 'UNK')}"
                    repl_date = h_repl.iloc[0].get('replacement_date', '-')

                explain_list.append({
                    "Area": area, "Driver": d_name, "Driver Reason": d_text, "Helper": h_name, "Helper Reason": h_text,
                    "Vacation Risk": "⚠️ Yes" if repl_info != "Safe" else "✅ None", "Future Replacement": repl_info, "Replacement Date": repl_date
                })
                
            st.dataframe(pd.DataFrame(explain_list), use_container_width=True, hide_index=True)


# ==========================================
# SCREEN 2: DATABASE MANAGEMENT
# ==========================================
elif choice == "2. Database Management":
    st.header("🗄️ Manage Database")
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles", "📥 Cloud & File Sync"])

    d_col_order = ["S/N", "code", "name", "veh_type", "sector", "needs_helper", "anchor_area", "anchor_vehicle", "vacation_status"]
    h_col_order = ["S/N", "code", "name", "health_card", "anchor_area", "vacation_status"]
    a_col_order = ["S/N", "code", "name", "sector", "region", "needs_driver", "needs_helper", "anchor_vehicle", "sort_order"]
    v_col_order = ["S/N", "number", "type", "division", "status", "anchor_area", "permitted_areas"]

    today = date.today()
    vac_cache = build_vacation_cache()

    # DRIVERS TAB
    with tab1:
        st.subheader("📋 Full Drivers List")
        drivers_df = load_table('drivers')
        disp_df = drivers_df.drop(columns=['restriction'], errors='ignore').copy()
        
        if not disp_df.empty:
            disp_df['vacation_status'] = disp_df['code'].apply(lambda x: get_vac_status(x, vac_cache, today))
        
        search_d = st.text_input("🔍 Search Drivers by Code, Name, Area, etc.", key="search_drivers")
        if search_d and not disp_df.empty:
            disp_df = disp_df[disp_df.astype(str).apply(lambda x: x.str.contains(search_d, case=False, na=False)).any(axis=1)]
        if not disp_df.empty: disp_df.insert(0, 'S/N', range(1, 1 + len(disp_df)))
        
        edited_d = st.data_editor(
            disp_df, 
            column_order=[c for c in d_col_order if c in disp_df.columns],
            column_config={
                "id": None, "fb_id": None, 
                "S/N": st.column_config.NumberColumn(disabled=True),
                "vacation_status": st.column_config.TextColumn("Vacation Status", disabled=True),
                "veh_type": st.column_config.SelectboxColumn("Veh Type", options=get_dynamic_opts(drivers_df, 'veh_type', VEHICLE_OPTIONS)),
                "sector": st.column_config.SelectboxColumn("Sector", options=get_dynamic_opts(drivers_df, 'sector', SECTOR_OPTIONS)),
                "needs_helper": st.column_config.SelectboxColumn("Needs Helper", options=NEEDS_OPTIONS)
            }, use_container_width=True, height=250, hide_index=True, key="ed_drivers"
        )
        if st.button("💾 Save Table Edits", key="save_table_drivers"):
            changes_saved = 0
            for idx in disp_df.index:
                row_id = int(disp_df.loc[idx, 'id'])
                if not disp_df.loc[idx].equals(edited_d.loc[idx]):
                    fb_id = str(disp_df.loc[idx, 'fb_id']) if 'fb_id' in disp_df.columns and pd.notna(disp_df.loc[idx, 'fb_id']) and str(disp_df.loc[idx, 'fb_id']).strip() else str(row_id)
                    update_dict = edited_d.loc[idx].drop(labels=['id', 'S/N', 'vacation_status'], errors='ignore').to_dict()
                    update_dict = {k: ("" if pd.isna(v) else str(v).strip()) for k, v in update_dict.items()}
                    for col in ['sector', 'veh_type', 'division', 'type']:
                        if col in update_dict: update_dict[col] = unify_text(update_dict[col])
                        
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    params = tuple(list(update_dict.values()) + [row_id])
                    run_query(f"UPDATE drivers SET {sql_sets} WHERE id=?", params, table_name="drivers", action="UPDATE", doc_id=fb_id, data=update_dict)
                    changes_saved += 1
            if changes_saved > 0:
                st.success(f"Saved {changes_saved} updates locally & queued for cloud sync!")
                st.rerun()
            
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Driver")
            d_name = st.text_input("New Driver Name", key="add_d_name")
            d_code = st.text_input("New Driver Code", key="add_d_code").strip()
            
            c1, c2 = st.columns(2)
            d_type_sel = c1.selectbox("Veh Type (Select)", get_dynamic_opts(drivers_df, 'veh_type', VEHICLE_OPTIONS), key="add_d_type_s")
            d_type_man = c2.text_input("Or manual Veh Type", key="add_d_type_m")
            d_type = d_type_man.strip() if d_type_man.strip() else d_type_sel

            c3, c4 = st.columns(2)
            d_sec_sel = c3.selectbox("Sector (Select)", get_dynamic_opts(drivers_df, 'sector', SECTOR_OPTIONS), key="add_d_sec_s")
            d_sec_man = c4.text_input("Or manual Sector", key="add_d_sec_m")
            d_sec = d_sec_man.strip() if d_sec_man.strip() else d_sec_sel

            d_needs_h = st.selectbox("New Driver Needs Helper?", NEEDS_OPTIONS, index=0, key="add_d_nh")
            
            d_anchor_opts = st.multiselect("Anchor Area(s)", multi_anchor_opts, key="add_d_anchor")
            d_anchor_man = st.text_input("Or manual Anchor Areas (comma-separated)", key="add_d_anchor_m")
            d_anchor_list = d_anchor_opts + [x.strip() for x in d_anchor_man.split(',') if x.strip()]
            d_anchor_str = ", ".join(list(set(d_anchor_list)))
            
            d_anchor_v_opts = st.multiselect("Anchor Vehicle(s)", v_num_opts, key="add_d_anchor_v")
            d_anchor_v_man = st.text_input("Or manual Anchor Vehicles (comma-separated)", key="add_d_anchor_v_m")
            d_anchor_v_list = d_anchor_v_opts + [x.strip() for x in d_anchor_v_man.split(',') if x.strip()]
            d_anchor_v_str = ", ".join(list(set(d_anchor_v_list)))
            
            if st.button("➕ Add Driver", use_container_width=True):
                if drivers_df['code'].isin([d_code]).any():
                    st.error(f"Driver Code {d_code} already exists! Cannot duplicate.")
                else:
                    if run_query("INSERT INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area, anchor_vehicle) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                              (d_name, d_code, unify_text(d_type), unify_text(d_sec), d_needs_h, "", d_anchor_str, d_anchor_v_str), table_name="drivers", action="INSERT", data={"name":d_name, "code":d_code, "veh_type":unify_text(d_type), "sector":unify_text(d_sec), "needs_helper":d_needs_h, "restriction":"", "anchor_area":d_anchor_str, "anchor_vehicle":d_anchor_v_str}):
                        st.success("Driver Added!")
                        st.rerun()

        with c_edit:
            st.subheader("🗑️ Delete Driver")
            d_del_opts = [f"[{r['code']}] {r['name']}" for _, r in drivers_df.iterrows()] if not drivers_df.empty else []
            sel_d_str = st.selectbox("Select Driver to Delete", d_del_opts)
            sel_d_code = sel_d_str.split("] ")[0].replace("[", "") if sel_d_str else None
            
            if sel_d_code:
                d_data = drivers_df[drivers_df['code'] == sel_d_code].iloc[0]
                if st.button("🗑️ Delete Driver", use_container_width=True, key=f"btn_d_del_{d_data['id']}"):
                    fb_id = str(d_data.get('fb_id', ''))
                    if not fb_id.strip() or fb_id == 'nan': fb_id = str(d_data['id'])
                    if run_query("DELETE FROM drivers WHERE code=?", (sel_d_code,), table_name="drivers", action="DELETE_DOC", doc_id=fb_id):
                        st.success("Driver Deleted!")
                        st.rerun()

    # HELPERS TAB
    with tab2:
        st.subheader("📋 Full Helpers List")
        helpers_df = load_table('helpers')
        disp_h = helpers_df.drop(columns=['restriction'], errors='ignore').copy()
        
        if not disp_h.empty:
            disp_h['vacation_status'] = disp_h['code'].apply(lambda x: get_vac_status(x, vac_cache, today))

        search_h = st.text_input("🔍 Search Helpers", key="search_helpers")
        if search_h and not disp_h.empty: disp_h = disp_h[disp_h.astype(str).apply(lambda x: x.str.contains(search_h, case=False, na=False)).any(axis=1)]
        if not disp_h.empty: disp_h.insert(0, 'S/N', range(1, 1 + len(disp_h)))
        
        edited_h = st.data_editor(
            disp_h, 
            column_order=[c for c in h_col_order if c in disp_h.columns],
            column_config={
                "id": None, "fb_id": None,
                "S/N": st.column_config.NumberColumn(disabled=True),
                "vacation_status": st.column_config.TextColumn("Vacation Status", disabled=True),
                "health_card": st.column_config.SelectboxColumn("Health Card", options=["Yes", "No", ""])
            }, use_container_width=True, height=250, hide_index=True, key="ed_helpers"
        )
        if st.button("💾 Save Table Edits", key="save_table_helpers"):
            changes_saved = 0
            for idx in disp_h.index:
                row_id = int(disp_h.loc[idx, 'id'])
                if not disp_h.loc[idx].equals(edited_h.loc[idx]):
                    fb_id = str(disp_h.loc[idx, 'fb_id']) if 'fb_id' in disp_h.columns and pd.notna(disp_h.loc[idx, 'fb_id']) and str(disp_h.loc[idx, 'fb_id']).strip() else str(row_id)
                    update_dict = edited_h.loc[idx].drop(labels=['id', 'S/N', 'vacation_status'], errors='ignore').to_dict()
                    update_dict = {k: ("" if pd.isna(v) else str(v).strip()) for k, v in update_dict.items()}
                    for col in ['sector', 'veh_type', 'division', 'type']:
                        if col in update_dict: update_dict[col] = unify_text(update_dict[col])
                        
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    params = tuple(list(update_dict.values()) + [row_id])
                    run_query(f"UPDATE helpers SET {sql_sets} WHERE id=?", params, table_name="helpers", action="UPDATE", doc_id=fb_id, data=update_dict)
                    changes_saved += 1
            if changes_saved > 0:
                st.success(f"Saved {changes_saved} updates locally & queued for cloud sync!")
                st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Helper")
            h_name = st.text_input("New Helper Name", key="add_h_name")
            h_code = st.text_input("New Helper Code", key="add_h_code").strip()
            h_health = st.selectbox("New Helper Health Card?", ["No", "Yes"], key="add_h_hc")
            
            h_anchor_opts = st.multiselect("Anchor Area(s)", multi_anchor_opts, key="add_h_anc")
            h_anchor_man = st.text_input("Or manual Anchor Areas (comma-separated)", key="add_h_anc_m")
            h_anchor_list = h_anchor_opts + [x.strip() for x in h_anchor_man.split(',') if x.strip()]
            h_anchor_str = ", ".join(list(set(h_anchor_list)))
            
            if st.button("➕ Add Helper", use_container_width=True):
                if helpers_df['code'].isin([h_code]).any():
                    st.error(f"Helper Code {h_code} already exists! Cannot duplicate.")
                else:
                    if run_query("INSERT INTO helpers (name, code, health_card, restriction, anchor_area) VALUES (?, ?, ?, ?, ?)", (h_name, h_code, h_health, "", h_anchor_str), table_name="helpers", action="INSERT", data={"name":h_name, "code":h_code, "health_card":h_health, "restriction":"", "anchor_area":h_anchor_str}):
                        st.success("Helper Added!")
                        st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Helper")
            h_del_opts = [f"[{r['code']}] {r['name']}" for _, r in helpers_df.iterrows()] if not helpers_df.empty else []
            sel_h_str = st.selectbox("Select Helper to Delete", h_del_opts)
            sel_h_code = sel_h_str.split("] ")[0].replace("[", "") if sel_h_str else None
            
            if sel_h_code:
                h_data = helpers_df[helpers_df['code'] == sel_h_code].iloc[0]
                if st.button("🗑️ Delete Helper", use_container_width=True, key=f"btn_h_del_{h_data['id']}"):
                    fb_id = str(h_data.get('fb_id', ''))
                    if not fb_id.strip() or fb_id == 'nan': fb_id = str(h_data['id'])
                    if run_query("DELETE FROM helpers WHERE code=?", (sel_h_code,), table_name="helpers", action="DELETE_DOC", doc_id=fb_id):
                        st.success("Helper Deleted!")
                        st.rerun()

    # AREAS TAB
    with tab3:
        st.subheader("📋 Route Template Areas")
        a_df = load_table('areas')
        disp_a = a_df.copy()
        
        search_a = st.text_input("🔍 Search Areas", key="search_areas")
        if search_a and not disp_a.empty: disp_a = disp_a[disp_a.astype(str).apply(lambda x: x.str.contains(search_a, case=False, na=False)).any(axis=1)]
        if not disp_a.empty: disp_a.insert(0, 'S/N', range(1, 1 + len(disp_a)))
        
        edited_a = st.data_editor(
            disp_a, 
            column_order=[c for c in a_col_order if c in disp_a.columns],
            column_config={
                "id": None, "fb_id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "sector": st.column_config.SelectboxColumn("Sector", options=get_dynamic_opts(a_df, 'sector', SECTOR_OPTIONS)),
                "needs_driver": st.column_config.SelectboxColumn("Needs Driver", options=NEEDS_OPTIONS),
                "needs_helper": st.column_config.SelectboxColumn("Needs Helper", options=NEEDS_OPTIONS),
                "anchor_vehicle": st.column_config.SelectboxColumn("Anchor Vehicle", options=v_num_opts),
                "region": st.column_config.SelectboxColumn("Region", options=get_dynamic_opts(a_df, 'region', ["Dubai", "Sharjah", "Ajman", "RAK", "Fujairah"]))
            }, use_container_width=True, height=250, hide_index=True, key="ed_areas"
        )
        if st.button("💾 Save Table Edits", key="save_table_areas"):
            changes_saved = 0
            for idx in disp_a.index:
                row_id = int(disp_a.loc[idx, 'id'])
                if not disp_a.loc[idx].equals(edited_a.loc[idx]):
                    fb_id = str(disp_a.loc[idx, 'fb_id']) if 'fb_id' in disp_a.columns and pd.notna(disp_a.loc[idx, 'fb_id']) and str(disp_a.loc[idx, 'fb_id']).strip() else str(row_id)
                    update_dict = edited_a.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                    update_dict = {k: ("" if pd.isna(v) else str(v).strip()) for k, v in update_dict.items()}
                    for col in ['sector', 'veh_type', 'division', 'type']:
                        if col in update_dict: update_dict[col] = unify_text(update_dict[col])
                        
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    params = tuple(list(update_dict.values()) + [row_id])
                    run_query(f"UPDATE areas SET {sql_sets} WHERE id=?", params, table_name="areas", action="UPDATE", doc_id=fb_id, data=update_dict)
                    changes_saved += 1
            if changes_saved > 0:
                st.success(f"Saved {changes_saved} updates locally & queued for cloud sync!")
                st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Area")
            a_name = st.text_input("New Area Name", key="add_a_name").strip()
            a_code = st.text_input("New Area Code", key="add_a_code")
            
            c1, c2 = st.columns(2)
            a_sec_sel = c1.selectbox("Sector (Select)", get_dynamic_opts(a_df, 'sector', SECTOR_OPTIONS), key="add_a_sec_s")
            a_sec_man = c2.text_input("Or manual Sector", key="add_a_sec_m")
            a_sec = a_sec_man.strip() if a_sec_man.strip() else a_sec_sel
            
            c3, c4, c5 = st.columns(3)
            a_needs_d = c3.selectbox("Needs Driver?", NEEDS_OPTIONS, index=0, key="add_a_nd")
            a_needs_h = c4.selectbox("Needs Helper?", NEEDS_OPTIONS, index=0, key="add_a_nh")
            a_reg_sel = c5.selectbox("Region (Select)", get_dynamic_opts(a_df, 'region', ["Dubai", "Sharjah"]), key="add_a_reg_s")
            a_reg_man = c5.text_input("Or manual Region", key="add_a_reg_m")
            a_reg = a_reg_man.strip() if a_reg_man.strip() else a_reg_sel
            
            c6, c7 = st.columns(2)
            a_anch_veh_sel = c6.selectbox("Anchor Vehicle (Select)", v_num_opts, key="add_a_anch_v_s")
            a_anch_veh_man = c7.text_input("Or manual Vehicle", key="add_a_anch_v_m")
            a_anch_veh = a_anch_veh_man.strip() if a_anch_veh_man.strip() else a_anch_veh_sel
            
            if st.button("➕ Add Area", use_container_width=True):
                if a_df['code'].isin([a_code]).any():
                    st.error(f"Area Code {a_code} already exists! Cannot duplicate.")
                else:
                    new_order = len(a_df) + 1
                    if run_query("INSERT INTO areas (name, code, sector, needs_driver, needs_helper, anchor_vehicle, sort_order, region) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (a_name, a_code, unify_text(a_sec), a_needs_d, a_needs_h, a_anch_veh, new_order, a_reg), table_name="areas", action="INSERT", data={"name":a_name, "code":a_code, "sector":unify_text(a_sec), "needs_driver":a_needs_d, "needs_helper":a_needs_h, "anchor_vehicle":a_anch_veh, "sort_order":new_order, "region":a_reg}):
                        st.success("Area Added!")
                        st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Area")
            area_opts = [f"[{r['code']}] {r['name']}" for _, r in a_df.iterrows()] if not a_df.empty else []
            sel_a_str = st.selectbox("Select Area to Delete", area_opts)
            if sel_a_str:
                sel_a_code = sel_a_str.split("] ")[0].replace("[", "")
                a_data = a_df[a_df['code'] == sel_a_code].iloc[0]
                if st.button("🗑️ Delete Area", use_container_width=True, key=f"btn_a_del_{a_data['id']}"):
                    fb_id = str(a_data.get('fb_id', ''))
                    if not fb_id.strip() or fb_id == 'nan': fb_id = str(a_data['id'])
                    if run_query("DELETE FROM areas WHERE code=?", (sel_a_code,), table_name="areas", action="DELETE_DOC", doc_id=fb_id):
                        st.success("Area Deleted!")
                        st.rerun()

    # VEHICLES TAB
    with tab4:
        st.subheader("📋 Full Vehicles List")
        v_df = load_table('vehicles')
        disp_v = v_df.copy()
        
        search_v = st.text_input("🔍 Search Vehicles", key="search_vehicles")
        if search_v and not disp_v.empty: disp_v = disp_v[disp_v.astype(str).apply(lambda x: x.str.contains(search_v, case=False, na=False)).any(axis=1)]
        if not disp_v.empty: disp_v.insert(0, 'S/N', range(1, 1 + len(disp_v)))
        
        edited_v = st.data_editor(
            disp_v, 
            column_order=[c for c in v_col_order if c in disp_v.columns],
            column_config={
                "id": None, "fb_id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "type": st.column_config.SelectboxColumn("Veh Type", options=get_dynamic_opts(v_df, 'type', VEHICLE_OPTIONS)),
                "division": st.column_config.SelectboxColumn("Division", options=get_dynamic_opts(v_df, 'division', SECTOR_OPTIONS)),
                "status": st.column_config.SelectboxColumn("Status", options=["Active", "Under Service", "In for Service"])
            }, use_container_width=True, height=250, hide_index=True, key="ed_vehicles"
        )
        if st.button("💾 Save Table Edits", key="save_table_vehicles"):
            changes_saved = 0
            for idx in disp_v.index:
                row_id = int(disp_v.loc[idx, 'id'])
                if not disp_v.loc[idx].equals(edited_v.loc[idx]):
                    fb_id = str(disp_v.loc[idx, 'fb_id']) if 'fb_id' in disp_v.columns and pd.notna(disp_v.loc[idx, 'fb_id']) and str(disp_v.loc[idx, 'fb_id']).strip() else str(row_id)
                    update_dict = edited_v.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                    update_dict = {k: ("" if pd.isna(v) else str(v).strip()) for k, v in update_dict.items()}
                    for col in ['sector', 'veh_type', 'division', 'type']:
                        if col in update_dict: update_dict[col] = unify_text(update_dict[col])
                        
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    params = tuple(list(update_dict.values()) + [row_id])
                    run_query(f"UPDATE vehicles SET {sql_sets} WHERE id=?", params, table_name="vehicles", action="UPDATE", doc_id=fb_id, data=update_dict)
                    changes_saved += 1
            if changes_saved > 0:
                st.success(f"Saved {changes_saved} updates locally & queued for cloud sync!")
                st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Vehicle")
            v_num = st.text_input("New Vehicle Number", key="add_v_num").strip()
            
            c1, c2 = st.columns(2)
            v_type_sel = c1.selectbox("Veh Type (Select)", get_dynamic_opts(v_df, 'type', VEHICLE_OPTIONS), key="add_v_type_s")
            v_type_man = c2.text_input("Or manual Veh Type", key="add_v_type_m")
            v_type = v_type_man.strip() if v_type_man.strip() else v_type_sel

            c3, c4 = st.columns(2)
            v_div_sel = c3.selectbox("Division (Select)", get_dynamic_opts(v_df, 'division', SECTOR_OPTIONS), key="add_v_div_s")
            v_div_man = c4.text_input("Or manual Division", value="Pharma", key="add_v_div_m")
            v_div = v_div_man.strip() if v_div_man.strip() else v_div_sel

            c5, c6 = st.columns(2)
            v_perm = c5.text_input("Permitted Areas", value="All", key="add_v_perm")
            v_stat = c6.selectbox("Status", ["Active", "Under Service", "In for Service"], key="add_v_stat")
            
            v_anchor_opts = st.multiselect("Anchor Area(s)", multi_anchor_opts, key="add_v_anc")
            v_anchor_man = st.text_input("Or manual Anchor Areas (comma-separated)", key="add_v_anc_m")
            v_anchor_list = v_anchor_opts + [x.strip() for x in v_anchor_man.split(',') if x.strip()]
            v_anchor_str = ", ".join(list(set(v_anchor_list)))
            
            if st.button("➕ Add Vehicle", use_container_width=True):
                if v_df['number'].isin([v_num]).any():
                    st.error(f"Vehicle Number {v_num} already exists! Cannot duplicate.")
                else:
                    if run_query("INSERT INTO vehicles (number, type, permitted_areas, division, anchor_area, status) VALUES (?, ?, ?, ?, ?, ?)", (v_num, unify_text(v_type), v_perm, unify_text(v_div), v_anchor_str, v_stat), table_name="vehicles", action="INSERT", data={"number":v_num, "type":unify_text(v_type), "permitted_areas":v_perm, "division":unify_text(v_div), "anchor_area":v_anchor_str, "status":v_stat}):
                        st.success("Vehicle Added!")
                        st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Vehicle")
            v_del_opts = [f"[{r['number']}] {r.get('type', 'VAN')} - {r.get('division', 'Pharma')}" for _, r in v_df.iterrows()] if not v_df.empty else []
            sel_v_str = st.selectbox("Select Vehicle to Delete", v_del_opts)
            sel_v = sel_v_str.split("] ")[0].replace("[", "") if sel_v_str else None
            
            if sel_v:
                v_data = v_df[v_df['number'] == sel_v].iloc[0]
                if st.button("🗑️ Delete Veh", use_container_width=True, key=f"btn_v_del_{v_data['id']}"):
                    fb_id = str(v_data.get('fb_id', ''))
                    if not fb_id.strip() or fb_id == 'nan': fb_id = str(v_data['id'])
                    if run_query("DELETE FROM vehicles WHERE number=?", (sel_v,), table_name="vehicles", action="DELETE_DOC", doc_id=fb_id):
                        st.success("Vehicle Deleted!")
                        st.rerun()

    with tab5:
        st.subheader("📥 Export Database")
        dfs_to_export = [load_table(t) for t in ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations']]
        output = generate_excel_with_sn(dfs_to_export, ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations'])
        st.download_button("📥 Download Master Database (Excel)", data=output, file_name="Master_Database.xlsx", type="primary")

        st.divider()
        st.subheader("📤 Import Database via Excel")
        st.caption("Did the cloud quota crash delete your cloud data yesterday? Upload the Master_Database.xlsx file you downloaded yesterday to instantly restore everything.")
        uploaded_file = st.file_uploader("Upload Master Database Excel", type=['xlsx'])
        if uploaded_file and st.button("Sync Data to System", type="primary"):
            xls = pd.ExcelFile(uploaded_file)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                run_query(None, table_name=sheet, action="CLEAR_TABLE")

                insert_data = []
                insert_dicts = []
                for _, row in df.iterrows():
                    data_dict = {k: unify_text(v) if k in ['sector', 'veh_type', 'division', 'type'] else v for k, v in row.to_dict().items() if pd.notna(v) and k not in ['id', 'S/N', 'vacation_status', 'fb_id', 'Days']}
                    
                    if sheet in ['history', 'vacations']:
                        if 'start_date' in data_dict: data_dict['start_date'] = parse_date_safe(data_dict['start_date'])
                        if 'end_date' in data_dict: data_dict['end_date'] = parse_date_safe(data_dict['end_date'])
                        if 'date' in data_dict: data_dict['date'] = parse_date_safe(data_dict['date'])
                            
                    cols = ', '.join(data_dict.keys())
                    vals = tuple(data_dict.values())
                    qmarks = ', '.join(['?'] * len(data_dict))
                    query = f"INSERT OR IGNORE INTO {sheet} ({cols}) VALUES ({qmarks})"
                    insert_data.append(vals)
                    insert_dicts.append(data_dict)
                
                if insert_data:
                    run_query(query, insert_data, table_name=sheet, action="INSERT_MANY", data=insert_dicts)
            st.success("Database restored completely from Excel!")

        st.divider()
        st.subheader("🚨 Cloud Diagnostic Scanner & Data Recovery")
        if FIREBASE_READY:
            if st.button("🔍 Deep Scan Firebase Cloud"):
                with st.spinner("Scanning all Firebase tables..."):
                    counts = {}
                    for t in SYNC_TABLES:
                        try: counts[t] = len(list(db_fs.collection(t).stream()))
                        except: counts[t] = 0
                    st.json(counts)
                    
                    if counts.get('history', 0) == 0 and counts.get('vacations', 0) == 0:
                        st.error("🚨 **Your Firebase cloud is completely empty for History and Vacations.** The data was wiped by the quota crash yesterday. **You MUST use the Excel Upload tool above** to restore your data from your `Master_Database.xlsx` backup.")
                    else:
                        st.success("✅ Found data in Firebase! Click the button below to merge it back into your app.")
        else:
            st.warning("Firebase is currently offline. Connect to check cloud status.")
        
        c_r1, c_r2, c_r3 = st.columns(3)
        if c_r1.button("♻️ Restore Default Template", type="secondary"):
            with st.spinner("Restoring layout..."):
                execute_global_init(force=True, load_default=True)
            st.success("Layout restored successfully!")
            st.rerun()
            
        if c_r2.button("🚑 Safe Recover Data from Cloud", type="primary"):
            with st.spinner("Deep scanning Firebase for legacy data and safely merging..."):
                if sync_down_from_cloud(merge=True):
                    st.success("Successfully recovered and merged all available data from the cloud!")
                    st.rerun()
                else:
                    st.error("Failed to connect to Firebase or quota exceeded.")

        if c_r3.button("💥 WIPE ENTIRE DATABASE", type="primary"):
            with st.spinner("Wiping everything..."):
                for t in ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations', 'active_routes', 'draft_routes', 'route_plan_reasons', 'vacation_predictions']:
                    run_query(f"DELETE FROM {t}", table_name=t, action="CLEAR_TABLE")
            st.success("Entire database wiped successfully!")
            st.rerun()


# ==========================================
# SCREEN 3: PAST EXPERIENCE BUILDER
# ==========================================
elif choice == "3. Past Experience Builder":
    st.header("🕰️ Manage Past Experience")
    history_df = load_table('history')
    areas_df = load_table('areas')
    area_list = [""] + (areas_df['name'].drop_duplicates().tolist() if not areas_df.empty else [])
    
    search_hist = st.text_input("🔍 Search History by Exact Date, Month, Year, Code, Name, or Area", "")
    
    disp_hist = history_df.sort_values(by="date", ascending=False).copy() if not history_df.empty and 'date' in history_df.columns else history_df.copy()
    
    if search_hist and not disp_hist.empty:
        disp_hist = disp_hist[disp_hist.astype(str).apply(lambda x: x.str.contains(search_hist, case=False, na=False)).any(axis=1)]
        
    if not disp_hist.empty:
        def calc_days(row):
            s = parse_date_safe(row.get('date', ''))
            e = parse_date_safe(row.get('end_date', ''))
            if s and e:
                try: return (datetime.strptime(e, "%Y-%m-%d") - datetime.strptime(s, "%Y-%m-%d")).days + 1
                except: return 0
            return 0
        disp_hist['Days'] = disp_hist.apply(calc_days, axis=1)
        disp_hist.insert(0, 'S/N', range(1, 1 + len(disp_hist)))
    
    edited_hist = st.data_editor(
        disp_hist, column_config={
            "id": None, "fb_id": None, 
            "S/N": st.column_config.NumberColumn(disabled=True),
            "Days": st.column_config.NumberColumn("Days", disabled=True)
        }, use_container_width=True, height=350, hide_index=True, key="ed_hist"
    )
    
    if st.button("💾 Save Table Edits", key="save_table_hist"):
        changes_saved = 0
        for idx in disp_hist.index:
            row_id = int(disp_hist.loc[idx, 'id'])
            if not disp_hist.loc[idx].equals(edited_hist.loc[idx]):
                fb_id = str(disp_hist.loc[idx, 'fb_id']) if 'fb_id' in disp_hist.columns and pd.notna(disp_hist.loc[idx, 'fb_id']) and str(disp_hist.loc[idx, 'fb_id']).strip() else str(row_id)
                update_dict = edited_hist.loc[idx].drop(labels=['id', 'S/N', 'Days'], errors='ignore').to_dict()
                update_dict = {k: ("" if pd.isna(v) else str(v).strip()) for k, v in update_dict.items()}
                for col in ['sector', 'veh_type', 'division', 'type']:
                    if col in update_dict: update_dict[col] = unify_text(update_dict[col])
                    
                sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                params = tuple(list(update_dict.values()) + [row_id])
                run_query(f"UPDATE history SET {sql_sets} WHERE id=?", params, table_name="history", action="UPDATE", doc_id=fb_id, data=update_dict)
                changes_saved += 1
        if changes_saved > 0:
            st.success(f"Saved {changes_saved} updates locally & queued for cloud sync!")
            st.rerun()

    with st.expander("🚨 Emergency Data Restore"):
        st.warning("Clicking this will wipe out ALL current Past Experience data from the system. (It defaults to empty).")
        if st.button("♻️ Wipe All Past Experience Data", type="primary"):
            with st.spinner("Wiping old history..."):
                run_query(None, table_name="history", action="CLEAR_TABLE")
                st.cache_data.clear()
            st.success("Past Experience data fully wiped and reset!")
            st.rerun()

    st.divider()
    
    # --- SMART BULK EXCEL/PDF SYNC ---
    st.subheader("📥 Smart Bulk Sync (Excel or PDF)")
    if not PDF_ENABLED:
        st.info("Upload an Excel (.xlsx) file containing history data. To upload PDFs, create a file named `requirements.txt` in your repository and type `PyPDF2` inside it.")
    else:
        st.info("Upload an Excel (.xlsx) or PDF ('Dispatch Summary') file. The AI will parse it, unify formatting, and strictly prevent duplicates.")
        
    bulk_file = st.file_uploader("Upload Experience Data", type=['xlsx', 'pdf'] if PDF_ENABLED else ['xlsx'])
    
    if bulk_file and st.button("Sync Uploaded Data", type="primary"):
        with st.spinner("Processing file intelligently..."):
            new_records = []
            new_dicts = []
            
            if bulk_file.name.endswith('.xlsx'):
                df_up = pd.read_excel(bulk_file)
                
                col_map = {}
                for c in df_up.columns:
                    cl = str(c).lower().strip()
                    if 'code' in cl: col_map[c] = 'person_code'
                    elif 'name' in cl: col_map[c] = 'person_name'
                    elif 'type' in cl or 'role' in cl: col_map[c] = 'person_type'
                    elif 'area' in cl: col_map[c] = 'area'
                    elif 'div' in cl: col_map[c] = 'sector'
                    elif 'start' in cl or 'from' in cl or 'date' in cl: col_map[c] = 'date'
                    elif 'end' in cl or 'to' in cl: col_map[c] = 'end_date'
                df_up = df_up.rename(columns=col_map)
                
                for _, row in df_up.iterrows():
                    c_val = str(row.get('person_code', '')).strip()
                    if pd.isna(c_val) or c_val.lower() in ["nan", "none", ""]: continue
                    
                    n_val = str(row.get('person_name', '')).strip()
                    a_val = str(row.get('area', '')).strip()
                    d_val = unify_text(str(row.get('sector', '')).strip()) if 'sector' in row else "Pharma"
                    
                    f_val = parse_date_safe(row.get('date'))
                    t_val = parse_date_safe(row.get('end_date')) if 'end_date' in row else f_val
                    
                    if f_val and t_val:
                        ptype = "Helper" if c_val.startswith('H') else "Driver"
                        new_records.append((ptype, c_val, n_val, a_val, d_val, f_val, t_val))
                        new_dicts.append({"person_type": ptype, "person_code": c_val, "person_name": n_val, "area": a_val, "sector": d_val, "date": f_val, "end_date": t_val})
                            
            elif bulk_file.name.endswith('.pdf') and PDF_ENABLED:
                pdf_reader = PyPDF2.PdfReader(bulk_file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                    
                date_match = re.search(r"Date\s*:\s*(\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
                global_date = parse_date_safe(date_match.group(1)) if date_match else None
                
                d_list = load_table('drivers').to_dict('records')
                h_list = load_table('helpers').to_dict('records')
                a_list = load_table('areas').to_dict('records')
                
                for line in text.split('\n'):
                    if not line.strip(): continue
                    
                    line_dates = re.findall(r'\d{2}/\d{2}/\d{4}', line)
                    f_val = parse_date_safe(line_dates[0]) if len(line_dates) > 0 else global_date
                    t_val = parse_date_safe(line_dates[1]) if len(line_dates) > 1 else f_val
                    
                    if not f_val: continue
                        
                    found_area = next((a for a in a_list if str(a['name']).lower() in line.lower()), None)
                    if found_area:
                        found_driver = next((d for d in d_list if str(d['name']).lower() in line.lower()), None)
                        if found_driver:
                            new_records.append(("Driver", found_driver['code'], found_driver['name'], found_area['name'], found_area.get('sector', 'Pharma'), f_val, t_val))
                            new_dicts.append({"person_type": "Driver", "person_code": found_driver['code'], "person_name": found_driver['name'], "area": found_area['name'], "sector": found_area.get('sector', 'Pharma'), "date": f_val, "end_date": t_val})
                        
                        found_helper = next((h for h in h_list if str(h['name']).lower() in line.lower()), None)
                        if found_helper:
                            new_records.append(("Helper", found_helper['code'], found_helper['name'], found_area['name'], found_area.get('sector', 'Pharma'), f_val, t_val))
                            new_dicts.append({"person_type": "Helper", "person_code": found_helper['code'], "person_name": found_helper['name'], "area": found_area['name'], "sector": found_area.get('sector', 'Pharma'), "date": f_val, "end_date": t_val})

            if new_records:
                q_hist = "INSERT OR IGNORE INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)"
                run_query(q_hist, new_records, table_name="history", action="INSERT_MANY", data=new_dicts)
                st.success(f"Successfully imported {len(new_records)} records while preventing duplicates!")
                st.rerun()
            else:
                st.warning("Could not extract valid records from the file. Ensure it has CODE, AREA, and DATES.")

    st.divider()

    c_add, c_edit = st.columns(2)
    with c_add:
        st.subheader("➕ Add Single Experience Manually")
        p_type = st.selectbox("Role", ["Driver", "Helper"])
        df_names = load_table('drivers') if p_type == "Driver" else load_table('helpers')
        person_list = [f"[{row.get('code', '')}] {row['name']}" for idx, row in df_names.iterrows()] if not df_names.empty else []
        
        if person_list:
            p_person = st.selectbox("Select Person", person_list)
            p_area = st.selectbox("Area Experienced In", area_list)
            p_sec = st.text_input("Which Sector was this in?", value="Pharma")
            d1, d2 = st.columns(2)
            p_start_date = d1.date_input("From Date (Start)")
            p_end_date = d2.date_input("To Date (End)")
            
            if st.button("➕ Add Past Experience", use_container_width=True):
                p_code = p_person.split("] ")[0].replace("[", "")
                p_name = p_person.split("] ")[1]
                
                overlap = history_df[(history_df['person_code']==p_code) & (history_df['area']==p_area) & (history_df['date']==p_start_date.strftime("%Y-%m-%d"))]
                if p_start_date > p_end_date: 
                    st.error("Start Date cannot be after End Date.")
                elif not overlap.empty:
                    st.error("⚠️ This person already has an experience log for this Area on this exact Start Date!")
                else:
                    if run_query("INSERT OR IGNORE INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (p_type, p_code, p_name, p_area, unify_text(p_sec), p_start_date.strftime("%Y-%m-%d"), p_end_date.strftime("%Y-%m-%d")), 
                              table_name="history", action="INSERT", data={"person_type":p_type, "person_code":p_code, "person_name":p_name, "area":p_area, "sector":unify_text(p_sec), "date":p_start_date.strftime("%Y-%m-%d"), "end_date":p_end_date.strftime("%Y-%m-%d")}):
                        st.success("Experience Added!")
                        st.rerun()

    with c_edit:
        st.subheader("🗑️ Remove Experience manually")
        if not history_df.empty:
            hist_options = []
            hist_map = {}
            for idx, row in history_df.iterrows():
                sec = unify_text(row.get('sector', 'Pharma'))
                label = f"[{row.get('person_code', 'UNK')}] {row['person_name']} - {row['area']} ({sec})"
                hist_options.append(label)
                hist_map[label] = str(row['id'])

            sel_hist_str = st.selectbox("Select Record to Delete", hist_options)
            if sel_hist_str:
                hist_id = int(hist_map[sel_hist_str])
                if st.button("🗑️ Delete Experience", use_container_width=True, key=f"he_del_{hist_id}"):
                    row_data = history_df[history_df['id'] == hist_id].iloc[0]
                    fb_id = str(row_data.get('fb_id', ''))
                    if not fb_id.strip() or fb_id == 'nan': fb_id = str(hist_id)
                    if run_query("DELETE FROM history WHERE id=?", (hist_id,), table_name="history", action="DELETE_DOC", doc_id=fb_id):
                        st.success("Experience Deleted!")
                        st.rerun()


# ==========================================
# SCREEN 4: VACATION SCHEDULE
# ==========================================
elif choice == "4. Vacation Schedule":
    st.header("🌴 Manage Vacation Schedule")
    vacs_df = load_table('vacations')
    vac_cache = build_vacation_cache()
    today = date.today()

    st.subheader("📊 Active Vacations Overview")
    active_vacs = []
    if not vacs_df.empty and 'person_name' in vacs_df.columns:
        for _, row in vacs_df.iterrows():
            sd = parse_date_safe(row['start_date'])
            ed = parse_date_safe(row['end_date'])
            if sd and ed and sd <= today.strftime("%Y-%m-%d") <= ed:
                active_vacs.append({
                    "Role": row['person_type'],
                    "Code": row.get('person_code', 'UNK'),
                    "Name": row['person_name'],
                    "Return Date": ed,
                    "Days Left": (datetime.strptime(ed, "%Y-%m-%d").date() - today).days
                })
    if active_vacs:
        dash_df = pd.DataFrame(active_vacs).sort_values(by="Days Left")
        dash_df.insert(0, 'S/N', range(1, 1 + len(dash_df)))
        st.dataframe(dash_df, use_container_width=True, hide_index=True)
    else:
        st.info("No personnel currently on vacation today.")
        
    with st.expander("🤖 Show AI Vacation Suggestions (Staff Who Need a Break)"):
        due_list = []
        for df, role in [(load_table('drivers'), "Driver"), (load_table('helpers'), "Helper")]:
            for _, p in df.iterrows():
                code = p.get('code', '')
                past_vacs = [end for start, end in vac_cache.get(code, [])]
                if not past_vacs:
                    due_list.append({"Code": code, "Name": p['name'], "Role": role, "Status": "NEVER Taken a Vacation!"})
                else:
                    last_vac = max(past_vacs)
                    days_since = (today - datetime.strptime(last_vac, "%Y-%m-%d").date()).days
                    if days_since > 300:
                        due_list.append({"Code": code, "Name": p['name'], "Role": role, "Status": f"Overdue by {days_since - 300} days (Last: {last_vac})"})
        if due_list: st.dataframe(pd.DataFrame(due_list), use_container_width=True, hide_index=True)
        else: st.success("Everyone seems well rested!")

    st.divider()

    st.subheader("📋 Vacation Database")
    search_vac = st.text_input("🔍 Search Vacations by Date, Code, Name...", "")
    disp_vac = vacs_df.copy()
    
    if search_vac and not disp_vac.empty:
        disp_vac = disp_vac[disp_vac.astype(str).apply(lambda x: x.str.contains(search_vac, case=False, na=False)).any(axis=1)]
    if not disp_vac.empty: disp_vac.insert(0, 'S/N', range(1, 1 + len(disp_vac)))
    
    edited_vac = st.data_editor(
        disp_vac, column_config={
            "id": None, "fb_id": None, "S/N": st.column_config.NumberColumn(disabled=True)
        }, use_container_width=True, height=250, hide_index=True, key="ed_vac"
    )
    
    if st.button("💾 Save Table Edits", key="save_table_vacs"):
        changes_saved = 0
        for idx in disp_vac.index:
            row_id = int(disp_vac.loc[idx, 'id'])
            if not disp_vac.loc[idx].equals(edited_vac.loc[idx]):
                fb_id = str(disp_vac.loc[idx, 'fb_id']) if 'fb_id' in disp_vac.columns and pd.notna(disp_vac.loc[idx, 'fb_id']) and str(disp_vac.loc[idx, 'fb_id']).strip() else str(row_id)
                update_dict = edited_vac.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                update_dict = {k: ("" if pd.isna(v) else str(v).strip()) for k, v in update_dict.items()}
                for col in ['sector', 'veh_type', 'division', 'type']:
                    if col in update_dict: update_dict[col] = unify_text(update_dict[col])
                    
                sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                params = tuple(list(update_dict.values()) + [row_id])
                run_query(f"UPDATE vacations SET {sql_sets} WHERE id=?", params, table_name="vacations", action="UPDATE", doc_id=fb_id, data=update_dict)
                changes_saved += 1
        if changes_saved > 0:
            st.success(f"Saved {changes_saved} updates locally & queued for cloud sync!")
            st.rerun()

    with st.expander("📥 Export / 📤 Import Vacation Data"):
        output = generate_excel_with_sn([vacs_df], ['vacations'])
        st.download_button("📥 Download Vacation Data", data=output, file_name="Vacation_Data.xlsx")
        
        up_vac = st.file_uploader("Upload Vacation Excel", type=['xlsx'], key="up_vac")
        if up_vac and st.button("Sync Vacation Database"):
            df = pd.read_excel(up_vac)
            
            # Fuzzy match columns
            col_map = {}
            for c in df.columns:
                cl = str(c).lower().strip()
                if 'code' in cl: col_map[c] = 'person_code'
                elif 'name' in cl: col_map[c] = 'person_name'
                elif 'type' in cl or 'role' in cl: col_map[c] = 'person_type'
                elif 'start' in cl or 'from' in cl: col_map[c] = 'start_date'
                elif 'end' in cl or 'to' in cl: col_map[c] = 'end_date'
            df = df.rename(columns=col_map)
            
            run_query(None, table_name="vacations", action="CLEAR_TABLE")
            
            insert_data = []
            insert_dicts = []
            for _, row in df.iterrows():
                sd_raw = row.get('start_date')
                ed_raw = row.get('end_date')
                sd = parse_date_safe(sd_raw)
                ed = parse_date_safe(ed_raw)
                if not sd or not ed: continue 
                
                ptype = str(row.get('person_type', '')).strip()
                pcode = str(row.get('person_code', '')).strip()
                pname = str(row.get('person_name', '')).strip()
                if not pcode or pcode.lower() == 'nan': continue
                
                data_dict = {
                    "person_type": ptype,
                    "person_code": pcode,
                    "person_name": pname,
                    "start_date": sd,
                    "end_date": ed
                }
                
                cols = ', '.join(data_dict.keys())
                vals = tuple(data_dict.values())
                qmarks = ', '.join(['?'] * len(data_dict))
                query = f"INSERT OR IGNORE INTO vacations ({cols}) VALUES ({qmarks})"
                insert_data.append(vals)
                insert_dicts.append(data_dict)
                
            if insert_data:
                run_query(query, insert_data, table_name="vacations", action="INSERT_MANY", data=insert_dicts)
            st.rerun()

    st.divider()
    c_add, c_edit = st.columns(2)
    
    with c_add:
        st.subheader("➕ Add Vacation")
        v_type = st.selectbox("Role", ["Driver", "Helper"])
        df_names = load_table('drivers') if v_type == "Driver" else load_table('helpers')
        name_list = [f"[{row.get('code', '')}] {row['name']}" for idx, row in df_names.iterrows()] if not df_names.empty else []
        
        if name_list:
            v_person = st.selectbox("Select Person Name", name_list)
            d1, d2 = st.columns(2)
            v_start = d1.date_input("Start Date (Leave)")
            v_end = d2.date_input("End Date (Return)", value=date.today() + timedelta(days=30))
            
            if st.button("➕ Add Vacation", use_container_width=True):
                v_code = v_person.split("] ")[0].replace("[", "")
                v_name = v_person.split("] ")[1]
                
                overlap = vacs_df[(vacs_df['person_code'] == v_code) & (vacs_df['start_date'] == v_start.strftime("%Y-%m-%d"))] if not vacs_df.empty else pd.DataFrame()
                    
                if v_start > v_end: 
                    st.error("Start Date cannot be after End Date.")
                elif not overlap.empty:
                    st.error(f"⚠️ {v_name} already has a vacation logged starting exactly on {v_start.strftime('%Y-%m-%d')}!")
                else:
                    if run_query("INSERT OR IGNORE INTO vacations (person_type, person_code, person_name, start_date, end_date) VALUES (?, ?, ?, ?, ?)", (v_type, v_code, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")), table_name="vacations", action="INSERT", data={"person_type":v_type, "person_code":v_code, "person_name":v_name, "start_date":v_start.strftime("%Y-%m-%d"), "end_date":v_end.strftime("%Y-%m-%d")}):
                        st.success("Vacation Added!")
                        st.rerun()

    with c_edit:
        st.subheader("🗑️ Delete Vacation manually")
        if not vacs_df.empty:
            vac_options = []
            vac_map = {}
            for idx, row in vacs_df.iterrows():
                label = f"[{row.get('person_code', 'UNK')}] {row['person_name']} ({row['start_date']} to {row['end_date']})"
                vac_options.append(label)
                vac_map[label] = str(row['id'])

            sel_vac_str = st.selectbox("Select Vacation to Delete", vac_options)
            if sel_vac_str:
                vac_id = int(vac_map[sel_vac_str])
                if st.button("🗑️ Delete Vacation", use_container_width=True, key=f"vac_del_{vac_id}"):
                    row_data = vacs_df[vacs_df['id'] == vac_id].iloc[0]
                    fb_id = str(row_data.get('fb_id', ''))
                    if not fb_id.strip() or fb_id == 'nan': fb_id = str(vac_id)
                    if run_query("DELETE FROM vacations WHERE id=?", (vac_id,), table_name="vacations", action="DELETE_DOC", doc_id=fb_id):
                        st.success("Vacation Deleted!")
                        st.rerun()
