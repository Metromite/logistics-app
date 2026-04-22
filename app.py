import streamlit as st

# --- ANTI-SLEEP PING HANDLER ---
if "ping" in st.query_params:
    st.write("🟢 App is awake and Firebase quota is protected!")
    st.stop()

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

# --- TEXT UNIFICATION ENGINE ---
def unify_text(val):
    if pd.isna(val) or val is None: return "None"
    val = str(val).strip()
    # Unify 2-8 VAN variations
    val = re.sub(r'2\s*-\s*8\s*VAN', '2-8 VAN', val, flags=re.IGNORECASE)
    if val.upper() == 'PHARMA': val = 'Pharma'
    if val.upper() == 'CONSUMER': val = 'Consumer'
    return val

def unify_dataframe(df):
    if df.empty: return df
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(unify_text)
    return df

# --- FIREBASE INITIALIZATION & DB ADAPTER ---
FIREBASE_READY = False
conn = None

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

    if FIREBASE_READY:
        try:
            list(db_fs.collection("_system_ping").limit(1).stream())
        except Exception as ping_error:
            if "429" in str(ping_error) or "Quota" in str(ping_error) or "ResourceExhausted" in str(ping_error):
                st.sidebar.error("🚨 Firebase Quota Exceeded! Using lightning-fast local cache.")
            else:
                st.sidebar.error(f"⚠️ Firebase Error: {ping_error}")
            FIREBASE_READY = False
except Exception as e:
    st.sidebar.error(f"Firebase Config Error: {str(e)}")
    FIREBASE_READY = False

if FIREBASE_READY:
    st.sidebar.markdown("<div style='text-align: right; font-size: 20px; margin-top: -15px;' title='Connected to Secure Cloud'>🟢 Firebase Connected</div>", unsafe_allow_html=True)
else:
    st.sidebar.markdown("<div style='text-align: right; font-size: 20px; margin-top: -15px;' title='Local Database Mode'>🔴 Firebase Disconnected (Offline Mode)</div>", unsafe_allow_html=True)

# --- HYBRID SQLITE (SOLVES QUOTA EXCEEDED PERMANENTLY) ---
def init_sqlite_db():
    local_conn = sqlite3.connect('logistics.db', check_same_thread=False)
    c = local_conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE, veh_type TEXT, sector TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY, name TEXT UNIQUE, code TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY, number TEXT UNIQUE, type TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, area TEXT, date TEXT, end_date TEXT, sector TEXT)''')
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_history ON history(person_code, area, sector, date)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, start_date DATE, end_date DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT, start_date TEXT, end_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS draft_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT, start_date TEXT, end_date TEXT, div_cat TEXT, sector TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS route_plan_reasons (id INTEGER PRIMARY KEY, plan_date TEXT, area TEXT, role TEXT, selected_person TEXT, score REAL, reasons TEXT, generated_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacation_predictions (id INTEGER PRIMARY KEY, person_code TEXT, person_name TEXT, role TEXT, suggested_start TEXT, suggested_end TEXT, reason TEXT, replacement_person TEXT, replacement_date TEXT)''')
    
    for query in [
        "ALTER TABLE drivers ADD COLUMN needs_helper TEXT DEFAULT 'Yes'",
        "ALTER TABLE helpers ADD COLUMN health_card TEXT DEFAULT 'No'",
        "ALTER TABLE areas ADD COLUMN sector TEXT DEFAULT 'Pharma'",
        "ALTER TABLE areas ADD COLUMN needs_helper TEXT DEFAULT 'Yes'",
        "ALTER TABLE areas ADD COLUMN sort_order INTEGER DEFAULT 99",
        "ALTER TABLE vehicles ADD COLUMN anchor_area TEXT DEFAULT 'None'",
        "ALTER TABLE vehicles ADD COLUMN status TEXT DEFAULT 'Active'",
        "ALTER TABLE vehicles ADD COLUMN permitted_areas TEXT DEFAULT 'All'",
        "ALTER TABLE vehicles ADD COLUMN division TEXT DEFAULT 'Pharma'"
    ]:
        try: c.execute(query)
        except sqlite3.OperationalError: pass
    local_conn.commit()
    return local_conn

conn = init_sqlite_db()

# --- SMART QUOTA-SAVING DB HANDLER ---
def clear_cache():
    st.cache_data.clear()

@st.cache_data(show_spinner=False, ttl=86400) 
def load_table(table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    
    if df.empty: return df
    df = unify_dataframe(df)
    
    if table_name == 'helpers' and 'health_card' not in df.columns: df['health_card'] = 'No'
    if table_name == 'drivers' and 'needs_helper' not in df.columns: df['needs_helper'] = 'Yes'
    if table_name == 'areas' and 'sector' not in df.columns: df['sector'] = 'Pharma'
    if table_name == 'areas' and 'needs_helper' not in df.columns: df['needs_helper'] = 'Yes'
    if table_name == 'areas' and 'sort_order' not in df.columns: df['sort_order'] = 99
    if table_name == 'history' and 'sector' not in df.columns: df['sector'] = 'Pharma'
    if table_name == 'vehicles':
        if 'anchor_area' not in df.columns: df['anchor_area'] = 'None'
        if 'status' not in df.columns: df['status'] = 'Active'
        if 'permitted_areas' not in df.columns: df['permitted_areas'] = 'All'
        if 'division' not in df.columns: df['division'] = 'Pharma'
    if table_name == 'active_routes' and 'start_date' not in df.columns: df['start_date'] = 'None'
    if table_name == 'draft_routes':
        if 'start_date' not in df.columns: df['start_date'] = 'None'
        if 'end_date' not in df.columns: df['end_date'] = 'None'
    if table_name == 'vacations' and 'person_code' not in df.columns: df['person_code'] = 'UNKNOWN'
    
    if table_name == 'areas': df['sort_order'] = pd.to_numeric(df['sort_order'], errors='coerce').fillna(99); df = df.sort_values(by='sort_order')
    if table_name in ['active_routes', 'draft_routes'] and 'order_num' in df.columns: df['order_num'] = pd.to_numeric(df['order_num'], errors='coerce').fillna(99); df = df.sort_values(by='order_num')
    
    if table_name in ['drivers', 'helpers']: df = df.drop_duplicates(subset=['code'], keep='first')
    if table_name == 'vehicles': df = df.drop_duplicates(subset=['number'], keep='first')
    if table_name == 'areas': df = df.drop_duplicates(subset=['name'], keep='first')
    if table_name == 'history': df = df.drop_duplicates(subset=['person_code', 'area', 'date'], keep='first')
    
    return df

def run_query(query, params=(), table_name=None, action=None, doc_id=None, data=None):
    try:
        c = conn.cursor()
        if query:
            if isinstance(data, list) and action == "INSERT_MANY": c.executemany(query, params)
            else: c.execute(query, params)
        elif action == "CLEAR_TABLE" and table_name:
            c.execute(f"DELETE FROM {table_name}")
        conn.commit()

        if FIREBASE_READY and table_name and action:
            if action == "INSERT" and data:
                db_fs.collection(table_name).add(data)
            elif action == "UPDATE" and doc_id and data:
                db_fs.collection(table_name).document(str(doc_id)).update(data)
            elif action == "DELETE_DOC" and doc_id:
                db_fs.collection(table_name).document(str(doc_id)).delete()
            elif action == "CLEAR_TABLE":
                docs = db_fs.collection(table_name).select([]).stream() 
                batch = db_fs.batch()
                for count, doc in enumerate(docs, 1):
                    batch.delete(doc.reference)
                    if count % 400 == 0:
                        batch.commit()
                        batch = db_fs.batch()
                batch.commit()
                
        if table_name: load_table.clear(table_name)
        else: st.cache_data.clear()
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
            if 'sort_order' in export_df.columns: export_df = export_df.drop(columns=['sort_order'])
            if 'S/N' in export_df.columns: export_df = export_df.drop(columns=['S/N'])
            export_df.insert(0, 'S/N', range(1, 1 + len(export_df)))
            export_df.to_excel(writer, sheet_name=sheet, index=False)
    output.seek(0)
    return output

# --- OPTIONS ---
VEHICLE_OPTIONS = ["None", "VAN", "PICK-UP", "VAN / PICK-UP", "BUS", "2-8 VAN"]
SECTOR_OPTIONS = ["None", "Pharma", "Consumer", "Bulk / Pick-Up", "2-8", "Govt / Urgent", "Substitute", "Fleet", "Bus"]
NEEDS_HELPER_OPTIONS = ["Yes", "No", "None"]
ROUTE_COLUMN_ORDER = ["S/N", "Driver Code", "Drivers Name", "AREA", "Sector", "Helper Code", "Helpers Name", "VEH NO", "Division Category"]

# --- STRICT HARDCODED ALLOWLISTS ---
KEEP_HELPERS = ["H116", "H131", "H121", "H119", "H046", "H070", "H129", "H113", "H132", "H118", "H115", "H122", "H114", "H066", "H011", "H005", "H023", "H050", "H062", "H051", "H104", "H130", "H034", "H013", "H109", "H024", "H026", "H049", "H099", "H082", "H017", "H126"]
KEEP_DRIVERS = ["D085", "D034", "D101", "D038", "D107", "D048", "D104", "D040", "D019", "D064", "D029", "D036", "D011", "D050", "D094", "D109", "D010", "D102", "D027", "D024", "D023", "D026", "D032", "D047", "D061", "D044", "D052", "D099", "D042", "D103", "D037", "D046", "D049", "D089", "D054", "D088", "D098", "D033"]

SEED_AREAS_IMAGE = [
    ("PH-FUJ", "FUJAIRAH", "Pharma", "Yes", 1), ("PH-RAK", "RAK / UAQ", "Pharma", "Yes", 2),
    ("PH-ALQ1", "ALQOUZ-1", "Pharma", "Yes", 3), ("PH-ALQ2", "ALQOUZ-2", "Pharma", "Yes", 4),
    ("PH-JUM", "JUMAIRAH", "Pharma", "Yes", 5), ("PH-BUR", "BURDUBAI", "Pharma", "Yes", 6),
    ("PH-MIR", "MIRDIFF", "Pharma", "Yes", 7), ("PH-QUS", "QUSAIS", "Pharma", "Yes", 8),
    ("PH-DEI", "DEIRA", "Pharma", "Yes", 9), ("PH-AJM", "AJMAN", "Pharma", "Yes", 10),
    ("PH-BUH", "BUHAIRAH", "Pharma", "Yes", 11), ("PH-SHJS", "SHJ - SANAYYA", "Pharma", "Yes", 12),
    ("PH-JAB", "JABEL ALI", "Pharma", "Yes", 13), ("28-CC1", "COLD CHAIN/URGENT ORDERS", "2-8", "No", 14), 
    ("28-CC2", "COLD CHAIN/URGENT ORDERS", "2-8", "No", 15), ("PH-SAMP", "Sample Driver", "Pharma", "Yes", 16), 
    ("PH-2ND1", "2ND TRIP", "Pharma", "Yes", 17), ("PH-2ND2", "2ND TRIP", "Pharma", "Yes", 18), 
    ("GOV-1", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 19), ("GOV-2", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 20),
    ("GOV-3", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 21), ("FLE-1", "FLEET SERVICE/RTA WORK", "Fleet", "No", 22),
    ("PU-SUB", "SUBTITUTE/PICK UP", "Substitute", "No", 23), ("PU-1", "PICK UP", "Bulk / Pick-Up", "Yes", 24), 
    ("PU-2", "PICK UP/SHJ", "Bulk / Pick-Up", "Yes", 25), ("PU-3", "PICK UP", "Bulk / Pick-Up", "Yes", 26), 
    ("PU-4", "PICK UP/SHJ", "Bulk / Pick-Up", "Yes", 27), ("PU-5", "PICK UP", "Bulk / Pick-Up", "Yes", 28), 
    ("PU-6", "PICK UP", "Bulk / Pick-Up", "Yes", 29), ("CON-ALQ", "ALQ", "Consumer", "Yes", 30), 
    ("CON-JAB", "JA", "Consumer", "Yes", 31), ("CON-DXBO", "DXBO", "Consumer", "Yes", 32), 
    ("CON-BUR", "BUR", "Consumer", "Yes", 33), ("CON-RAK", "RAK", "Consumer", "Yes", 34), 
    ("CON-PU1", "PICK UP/SHJ (C)", "Consumer", "Yes", 35), ("CON-PU2", "PICK UP (C)", "Consumer", "Yes", 36), 
    ("CON-AJM", "AJM", "Consumer", "Yes", 37), ("CON-SHJS", "SHJS", "Consumer", "Yes", 38), 
    ("CON-SUB", "SUBTITUTE/URGENT ORDERS", "Substitute", "No", 39)
]

SEED_VEHICLES = [
    ("M 95321", "PICK-UP", "DUBAI", "Pharma"), ("C 47055", "PICK-UP", "SHARJAH", "Pharma"),
    ("B 14813", "BUS", "DUBAI", "Pharma"), ("C 58107", "PICK-UP", "SHARJAH", "Pharma"),
    ("C 58801", "VAN", "DUBAI", "Pharma"), ("16 47645", "BUS", "DUBAI", "Pharma"),
    ("I 85664", "PICK-UP", "DUBAI", "Pharma"), ("I 86488", "VAN", "DUBAI", "Pharma"),
    ("R 96871", "BUS", "DUBAI", "Pharma"), ("U 65986", "PICK-UP", "SHARJAH", "Consumer"),
    ("U 65988", "VAN", "SHARJAH", "Pharma"), ("U 65990", "VAN", "DUBAI", "Pharma"),
    ("V-83576", "VAN", "DUBAI", "Pharma"), ("V-84049", "VAN", "SHARJAH", "Pharma"),
    ("V-84050", "VAN", "SHARJAH", "Pharma"), ("W 49535", "PICK-UP", "DUBAI", "Consumer"),
    ("W 49536", "VAN", "DUBAI", "Pharma"), ("W 49539", "VAN", "DUBAI", "Pharma"),
    ("W 49540", "VAN", "DUBAI", "Consumer"), ("O 72506", "VAN", "AJMAN AND SHARJAH", "Consumer"),
    ("O 72533", "PICK-UP", "DUBAI", "Pharma"), ("O 72548", "PICK-UP", "DUBAI", "2-8 VAN"),
    ("O 72567", "VAN", "DUBAI", "Pharma"), ("O 72578", "VAN", "DUBAI", "Pharma"),
    ("O 72579", "VAN", "DUBAI", "Pharma"), ("O 72581", "VAN", "SHARJAH", "Consumer"),
    ("D 85038", "VAN", "DUBAI", "Consumer"), ("D 85076", "VAN", "DUBAI", "Pharma"),
    ("D 85823", "VAN", "DUBAI", "Pharma"), ("C 26596", "BUS", "DUBAI", "Pharma"),
    ("V 60857", "VAN", "AJMAN", "Pharma"), ("N 31329", "VAN", "DUBAI", "Pharma"),
    ("N 32094", "VAN", "DUBAI", "Pharma"), ("N 32119", "VAN", "DUBAI", "Pharma"),
    ("N 32126", "VAN", "RAK", "Pharma"), ("N 33680", "VAN", "RAK", "Consumer"),
    ("E 18104", "VAN", "DUBAI", "Pharma"), ("E 18316", "VAN", "DUBAI", "Consumer"),
    ("BB 72473", "BUS", "DUBAI", "Pharma"), ("I 71528", "PICK-UP", "DUBAI", "2-8 VAN"),
    ("W 11792", "VAN", "FUJAIRAH", "Pharma"), ("T 26701", "VAN", "SHARJAH", "2-8 VAN"),
    ("CC 98174", "VAN", "DUBAI", "Pharma"), ("CC 98175", "VAN", "DUBAI", "Pharma"),
    ("CC 98176", "VAN", "DUBAI", "2-8 VAN")
]

RAW_NAME_MAP = {
    "D085": "Rahul R.P", "D034": "Adil Hassan", "D101": "Tintu V Joseph", "D038": "Ismail Korokkaran", "D107": "Muneeb Hussain", 
    "D048": "Moideen Azeez", "D104": "Mahammed Ansar", "D040": "Hussain Mohammed", "D019": "Muhammed Kunji", "D064": "Shabeer Ali A.Rahman", 
    "D029": "Baderudheen", "D036": "Rashid Baderzaman", "D011": "Imran Khan", "D050": "Abdul Mansoor", "D094": "Shuhaib Mullantakath", 
    "D109": "Yousuf Nobi Shakib", "D010": "Nasar", "D102": "Mohammed Nasiruddeen", "D027": "Sultan", "D024": "Sadiq Shah", 
    "D023": "Sabir Shah", "D026": "Jahaberudheen", "D032": "Sayd Mubarak", "D047": "Ahmed Faraj", "D061": "Said Alavy", 
    "D044": "Zainul Abid", "D052": "Noushad Ali", "D099": "Muhammed Noushad P", "D042": "Gulam Khan Mohammad", "D103": "Jamseer PV Ibrahim", 
    "D037": "Nijavudeen", "D046": "Azeez Abdulla", "D049": "Abdul Jabbar", "D089": "Jisam K Saleem", "D054": "Sameer Zakariyah", 
    "D088": "Saheer Ali V Z", "D098": "Muhammed Aslam K", "D033": "Naeem Fazal",
    "H116": "Munawir P Kabeer", "H131": "Said Ahmed Ibrahim", "H121": "Afreen Salam", "H119": "Muhammed Janees P", "H046": "Shihabudeen", 
    "H070": "A. Harshad", "H129": "Pratik Bista", "H113": "Chadi Otmani", "H132": "Ahmed Younis", "H118": "Muhammed Shamil P", 
    "H115": "Omar AlSaeed", "H122": "Mohamed Arsath", "H114": "Abdul Khader", "H066": "Christopherlov Brian", "H011": "Sudhakaran", 
    "H005": "Aboobacker Aliyar", "H023": "Adil", "H050": "Ranjith. P", "H062": "Rakshith.p", "H051": "Shar Bahadar", 
    "H104": "Mohammed Shakeer", "H130": "Javed Akhtar", "H034": "Riyasudheen Khuthubudheen", "H013": "Haris K", "H109": "AL Ameen", 
    "H024": "Mohd Musthafa", "H026": "Riyas Ahmed", "H049": "Shobith", "H099": "Muhammed Rajas", "H082": "Hassan Mohammed", 
    "H017": "Mujammal", "H126": "Subin Kovammal"
}

def parse_date_safe(d_str):
    if pd.isna(d_str) or not str(d_str).strip() or str(d_str) == "None": return None
    d_str = str(d_str).strip().split(" ")[0]
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try: return datetime.strptime(d_str, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return d_str

# SAFELY CLOSED RAW STRING SO IT DOES NOT BREAK THE CODE
RAW_HISTORY_DATA = """"""

if "db_initialized" not in st.session_state:
    def execute_global_init(force=False):
        try:
            current_areas = load_table("areas")
            if force or len(current_areas) != 39:
                c = conn.cursor()
                c.execute("DELETE FROM areas")
                c.executemany("INSERT OR IGNORE INTO areas (code, name, sector, needs_helper, sort_order) VALUES (?, ?, ?, ?, ?)", SEED_AREAS_IMAGE)
                conn.commit()
                if FIREBASE_READY: run_query(None, table_name="areas", action="CLEAR_TABLE")
            
            d_df = load_table('drivers')
            if len(d_df) == 0:
                c = conn.cursor()
                d_seed = [(RAW_NAME_MAP.get(code, "Unknown"), code, "VAN", "None", "None", "None", "None") for code in KEEP_DRIVERS]
                c.executemany("INSERT OR IGNORE INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?, ?)", d_seed)
                conn.commit()
            
            h_df = load_table('helpers')
            if len(h_df) == 0:
                c = conn.cursor()
                h_seed = [(RAW_NAME_MAP.get(code, "Unknown"), code, "None", "No", "None") for code in KEEP_HELPERS]
                c.executemany("INSERT OR IGNORE INTO helpers (name, code, restriction, health_card, anchor_area) VALUES (?, ?, ?, ?, ?)", h_seed)
                conn.commit()

            v_df = load_table('vehicles')
            if len(v_df) == 0:
                c = conn.cursor()
                v_seed = [(v_num, unify_text(v_type), permitted, unify_text(division), "None", "Active") for v_num, v_type, permitted, division in SEED_VEHICLES]
                c.executemany("INSERT OR IGNORE INTO vehicles (number, type, permitted_areas, division, anchor_area, status) VALUES (?, ?, ?, ?, ?, ?)", v_seed)
                conn.commit()
                    
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
            code, area, sector = r['person_code'], r['area'], r.get('sector', 'Pharma')
            if pd.isna(sector) or sector == "nan": sector = "Pharma"
            end_date = safe_parse_date(r['end_date'] if pd.notna(r.get('end_date')) and r['end_date'] != "None" else r['date'])
            
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
            vac_cache[code].append((safe_parse_date(r['start_date']), safe_parse_date(r['end_date'])))
    return vac_cache

def is_on_vacation(person_code, target_date, vac_cache):
    for start, end in vac_cache.get(person_code, []):
        if start <= target_date <= end: return True
    return False

def vacation_within_3_months(person_code, target_date, vac_cache):
    limit_date = target_date + timedelta(days=90)
    for start, end in vac_cache.get(person_code, []):
        if target_date < start <= limit_date: return start
    return None

def months_until_next_vacation(person_code, vac_cache, target_date):
    past_vacs = [end for start, end in vac_cache.get(person_code, []) if end < target_date]
    if not past_vacs: return 0 
    last_vac = max(past_vacs)
    days_since = (target_date - last_vac).days
    return max(0, 365 - days_since) / 30.0


# --- WEIGHTED AI SCORING ALGORITHM (WITH MULTI-ANCHOR) ---
NEVER_WORKED_BONUS = 10000
NEVER_WORKED_SECTOR_BONUS = 8000
ANCHOR_MATCH_BONUS = 5000
MONTHS_WEIGHT = 100
SECTOR_MONTHS_WEIGHT = 50
RECENT_AREA_PENALTY = -3000
VACATION_SOON_PENALTY = -1500

def calculate_candidate_score(candidate, area, req_veh, req_sector, target_date, exp_cache, vac_cache, role="Driver"):
    code = candidate['code']
    score = 0
    reasons = []

    if is_on_vacation(code, target_date, vac_cache):
        return None, "Excluded: On Vacation"
        
    if role == "Driver":
        p_veh = candidate.get('veh_type', 'None')
        if p_veh not in [req_veh, "None"] and not (p_veh == "VAN / PICK-UP" and req_veh in ["VAN", "PICK-UP"]):
            return None, f"Excluded: Vehicle Mismatch ({p_veh} != {req_veh})"

    anchors = [a.strip() for a in str(candidate.get('anchor_area', 'None')).split(',') if a.strip()]
    if "None" in anchors and len(anchors) == 1: anchors = []

    if anchors:
        if any(a in [area['name'], req_sector, req_veh] for a in anchors):
            score += ANCHOR_MATCH_BONUS
            reasons.append(f"Anchor Match (+{ANCHOR_MATCH_BONUS})")
        else:
            return None, f"Excluded: Anchored strictly to {', '.join(anchors)}"

    last_worked_area = exp_cache.get(code, {}).get('areas', {}).get(area['name'])
    if not last_worked_area:
        score += NEVER_WORKED_BONUS
        reasons.append(f"Never worked Area (+{NEVER_WORKED_BONUS})")
    else:
        months_since = (target_date - last_worked_area).days / 30.0
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
        months_since_sec = (target_date - last_worked_sector).days / 30.0
        time_score_sec = int(months_since_sec * SECTOR_MONTHS_WEIGHT)
        score += time_score_sec
        reasons.append(f"{months_since_sec:.1f}m since {req_sector} Sector (+{time_score_sec})")

    vac_start = vacation_within_3_months(code, target_date, vac_cache)
    if vac_start:
        score += VACATION_SOON_PENALTY
        reasons.append(f"Vacation soon ({VACATION_SOON_PENALTY})")

    if role == "Helper":
        if "Consumer" in req_sector:
            if candidate.get('health_card') == 'Yes':
                score += 1500; reasons.append("Health Card (+1500)")
            else:
                score -= 1500; reasons.append("No Health Card (-1500)")
        elif candidate.get('health_card') == 'Yes':
            score -= 1000; reasons.append("Waste Health Card (-1000)")

    return score, " | ".join(reasons)


def check_route_requirements(areas_df, drivers_df, helpers_df, vehicles_df, vac_cache, today_date):
    errors = []
    req_veh = {"VAN": 0, "PICK-UP": 0, "BUS": 0, "2-8 VAN": 0}
    for _, area in areas_df.iterrows():
        sec = unify_text(area.get('sector', ''))
        name = unify_text(area.get('name', ''))
        if "2-8" in sec or "COLD CHAIN" in name: req_veh["2-8 VAN"] += 1
        elif "Govt" in sec or "GOVT" in name: req_veh["BUS"] += 1
        elif "Pick-Up" in sec or "PICK UP" in name: req_veh["PICK-UP"] += 1
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
            errors.append(f"🚗 Missing **{vtype}** Vehicles: Route needs **{required}**, but you only have **{avail_veh[vtype]}** active.")

    avail_d = len([1 for _, r in drivers_df.iterrows() if not is_on_vacation(r['code'], today_date, vac_cache)])
    if avail_d < len(areas_df):
        errors.append(f"🚛 Missing Drivers: Route needs **{len(areas_df)}** active drivers, but you only have **{avail_d}**.")
        
    return errors


# --- GLOBAL SHARED VARIABLES ---
areas_df_global = load_table('areas')
area_list_global = ["None"] + (areas_df_global['name'].tolist() if not areas_df_global.empty else [])
multi_anchor_opts = list(set([a for a in area_list_global + SECTOR_OPTIONS + VEHICLE_OPTIONS if a != "None"]))
multi_anchor_opts.sort()

# --- APP ROUTING ---
menu = ["1. AI Route Planner", "2. Database Management", "3. Past Experience Builder", "4. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)


# ==========================================
# SCREEN 1: AI ROUTE PLANNER
# ==========================================
if choice == "1. AI Route Planner":
    
    st.subheader("📊 Today's Availability Dashboard")
    today = date.today()
    all_d = load_table('drivers')
    all_h = load_table('helpers')
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
        
        d_start_str = draft_routes.iloc[0].get('start_date') if 'start_date' in draft_routes.columns and pd.notna(draft_routes.iloc[0].get('start_date')) else None
        d_end_str = draft_routes.iloc[0].get('end_date') if 'end_date' in draft_routes.columns and pd.notna(draft_routes.iloc[0].get('end_date')) else None
        
        plan_start_val = safe_parse_date(d_start_str) if d_start_str and d_start_str != "None" else today
        plan_end_val = safe_parse_date(d_end_str) if d_end_str and d_end_str != "None" else today + timedelta(days=90)
        
        c_d1, c_d2 = st.columns(2)
        plan_start = c_d1.date_input("Plan Start Date", value=plan_start_val)
        plan_end = c_d2.date_input("Plan End Date", value=plan_end_val)
        
        disp_draft = draft_routes.copy()
        
        if 'S/N' not in disp_draft.columns:
            disp_draft.insert(0, 'S/N', disp_draft.get('order_num', range(1, 1 + len(disp_draft))))
            
        disp_draft = disp_draft.rename(columns={"area_name": "AREA", "veh_num": "VEH NO", "sector": "Sector"})
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
                "Driver Code": st.column_config.TextColumn("CODE"),
                "Helper Code": st.column_config.TextColumn("CODE")
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
            for index, r in edited_df.iterrows():
                sn_val = r.get('S/N', index + 1)
                insert_data.append((sn_val, "", r.get('AREA', ''), unify_text(r.get('Sector', '')), r.get('Driver Code', ''), r.get('Drivers Name', ''), r.get('Helper Code', ''), r.get('Helpers Name', ''), r.get('VEH NO', ''), unify_text(r.get('Division Category', '')), p_s, p_e))
                
            q_dr = "INSERT INTO draft_routes (order_num, area_code, area_name, sector, driver_code, driver_name, helper_code, helper_name, veh_num, div_cat, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            run_query(q_dr, insert_data, table_name="draft_routes", action="INSERT_MANY")
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
            hist_data = []
            for index, r in edited_df.iterrows():
                sn_val = r.get('S/N', index + 1)
                active_data.append((sn_val, "", r.get('AREA', ''), r.get('Driver Code', ''), r.get('Drivers Name', ''), r.get('Helper Code', ''), r.get('Helpers Name', ''), r.get('VEH NO', ''), p_s, p_e))
                
                for code, name, ptype in [(r.get('Driver Code', ''), r.get('Drivers Name', ''), "Driver"), (r.get('Helper Code', ''), r.get('Helpers Name', ''), "Helper")]:
                    if pd.notna(code) and str(code).strip() not in ["UNASSIGNED", "N/A", "", "None"]:
                        hist_data.append((ptype, str(code).strip(), str(name).strip(), r.get('AREA', ''), unify_text(r.get('Sector', '')), p_s, p_e))
            
            q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            run_query(q_ar, active_data, table_name="active_routes", action="INSERT_MANY")
            
            q_hist = "INSERT OR IGNORE INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)"
            run_query(q_hist, hist_data, table_name="history", action="INSERT_MANY")
            
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
            
        disp_active = active_with_sector.rename(columns={"area_name": "AREA", "veh_num": "VEH NO", "driver_name": "Drivers Name", "driver_code": "Driver Code", "helper_name": "Helpers Name", "helper_code": "Helper Code"})
        disp_active = disp_active[[c for c in ROUTE_COLUMN_ORDER if c in disp_active.columns]]
        if 'S/N' not in disp_active.columns: disp_active.insert(0, 'S/N', range(1, 1 + len(disp_active)))
        
        st.dataframe(
            disp_active, use_container_width=True, hide_index=True, column_order=ROUTE_COLUMN_ORDER,
            column_config={"Driver Code": st.column_config.TextColumn("CODE"), "Helper Code": st.column_config.TextColumn("CODE")}
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
        st.dataframe(empty_df, use_container_width=True, hide_index=True, column_config={"Driver Code": "CODE", "Helper Code": "CODE"})

    st.divider()

    # --- GENERATOR ENGINE ---
    st.header("⚙️ Generate Smart AI Route Plan")
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=today)
    rot_type = col2.radio("Who is rotating this month?", ["Drivers", "Helpers"])
    
    if "force_bypass" not in st.session_state: st.session_state.force_bypass = False

    if st.button("Generate Smart AI Route Plan", type="primary") or st.session_state.force_bypass:
        areas = load_table('areas')
        vehicles = load_table('vehicles')
        val_errors = check_route_requirements(areas, all_d, all_h, vehicles, vac_cache, month_target)
        
        if val_errors and not st.session_state.force_bypass:
            st.error("🚨 **ROUTE GENERATION HALTED: DATABASE SHORTAGE DETECTED**")
            for err in val_errors: st.warning(err)
            st.markdown("Cannot fulfill the 39-Route Plan with current database. Please add the missing vehicles/drivers, or bypass this warning to assign what you have.")
            if st.button("⚠️ Bypass Warnings & Force Generate"):
                st.session_state.force_bypass = True
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
                predict_data = []

                for _, area in areas.iterrows():
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

                    prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                    a_d_code, a_d_name, a_h_code, a_h_name, a_v_num = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED"

                    if rot_type == "Drivers" or prev_assignment.empty or prev_assignment.iloc[0].get('driver_code') in ["N/A", "UNASSIGNED", None]:
                        best_d, best_d_score, d_reason = None, -999999, "No valid drivers"
                        avail_dr = all_d[~all_d['code'].isin(used_drivers)]
                        
                        for _, p in avail_dr.iterrows():
                            score, rsn = calculate_candidate_score(p, area, req_veh, req_sector, month_target, exp_cache, vac_cache, role="Driver")
                            if score is not None and score > best_d_score:
                                best_d_score, best_d, d_reason = score, p, rsn
                                
                        if best_d is not None:
                            a_d_code, a_d_name = best_d['code'], best_d['name']
                            used_drivers.add(a_d_code)
                            if best_d.get('needs_helper') == 'No': needs_helper = False
                            reason_data.append((p_s_gen, area_name, "Driver", a_d_name, best_d_score, d_reason, timestamp))
                            
                            vac_start = vacation_within_3_months(a_d_code, month_target, vac_cache)
                            if vac_start:
                                repl_d, best_r_score, _ = None, -999999, ""
                                for _, rp in all_d[~all_d['code'].isin([a_d_code])].iterrows():
                                    r_score, _ = calculate_candidate_score(rp, area, req_veh, req_sector, vac_start, exp_cache, vac_cache, role="Driver")
                                    if r_score is not None and r_score > best_r_score:
                                        best_r_score, repl_d = r_score, rp
                                repl_name = repl_d['name'] if repl_d is not None else "CRITICAL SHORTAGE"
                                predict_data.append((a_d_code, a_d_name, "Driver", vac_start.strftime("%Y-%m-%d"), "Scheduled Vacation", repl_name, vac_start.strftime("%Y-%m-%d")))
                    else:
                        a_d_code, a_d_name = prev_assignment.iloc[0]['driver_code'], prev_assignment.iloc[0]['driver_name']
                        used_drivers.add(a_d_code)

                    if not needs_helper:
                        a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"
                    elif rot_type == "Helpers" or prev_assignment.empty or prev_assignment.iloc[0].get('helper_code') in ["N/A", "UNASSIGNED", None]:
                        best_h, best_h_score, h_reason = None, -999999, "No valid helpers"
                        avail_hl = all_h[~all_h['code'].isin(used_helpers)]
                        
                        for _, p in avail_hl.iterrows():
                            score, rsn = calculate_candidate_score(p, area, req_veh, req_sector, month_target, exp_cache, vac_cache, role="Helper")
                            if score is not None and score > best_h_score:
                                best_h_score, best_h, h_reason = score, p, rsn
                                
                        if best_h is not None:
                            a_h_code, a_h_name = best_h['code'], best_h['name']
                            used_helpers.add(a_h_code)
                            reason_data.append((p_s_gen, area_name, "Helper", a_h_name, best_h_score, h_reason, timestamp))
                            
                            vac_start = vacation_within_3_months(a_h_code, month_target, vac_cache)
                            if vac_start:
                                repl_h, best_r_score, _ = None, -999999, ""
                                for _, rp in all_h[~all_h['code'].isin([a_h_code])].iterrows():
                                    r_score, _ = calculate_candidate_score(rp, area, req_veh, req_sector, vac_start, exp_cache, vac_cache, role="Helper")
                                    if r_score is not None and r_score > best_r_score:
                                        best_r_score, repl_h = r_score, rp
                                repl_name = repl_h['name'] if repl_h is not None else "CRITICAL SHORTAGE"
                                predict_data.append((a_h_code, a_h_name, "Helper", vac_start.strftime("%Y-%m-%d"), "Scheduled Vacation", repl_name, vac_start.strftime("%Y-%m-%d")))
                    else:
                        a_h_code, a_h_name = prev_assignment.iloc[0]['helper_code'], prev_assignment.iloc[0]['helper_name']
                        used_helpers.add(a_h_code)

                    if a_d_code != "UNASSIGNED" and a_v_num == "UNASSIGNED":
                        d_type = all_d[all_d['code'] == a_d_code]['veh_type'].values[0] if not all_d[all_d['code'] == a_d_code].empty else "VAN"
                        tvt = req_veh if req_veh != "VAN" else unify_text(d_type)
                        
                        potential_vs = []
                        active_vehicles_df = vehicles[~vehicles.get('status', 'Active').str.contains('Under Service|In for Service', case=False, na=False)]
                        for _, v in active_vehicles_df[~active_vehicles_df['number'].isin(used_vehicles)].iterrows():
                            v_type = unify_text(v.get('type', 'VAN'))
                            type_match = False
                            if v_type == tvt: type_match = True
                            elif tvt in ["VAN", "PICK-UP"] and v_type == "VAN / PICK-UP": type_match = True
                            
                            if not type_match: continue
                            
                            v_anchors = [a.strip() for a in str(v.get('anchor_area', 'None')).split(',') if a.strip()]
                            if "None" in v_anchors and len(v_anchors) == 1: v_anchors = []
                            
                            if v_anchors:
                                if any(unify_text(a) in [area_name, req_sector, tvt] for a in v_anchors):
                                    potential_vs.append((v, True))
                            else:
                                potential_vs.append((v, False))

                        potential_vs.sort(key=lambda x: x[1], reverse=True)

                        if potential_vs:
                            a_v_num = potential_vs[0][0]['number']
                            used_vehicles.add(a_v_num)

                    route_plan.append({
                        "Driver Code": a_d_code, "Drivers Name": a_d_name, 
                        "AREA": area_name, "Sector": req_sector, "Helper Code": a_h_code, "Helpers Name": a_h_name, 
                        "VEH NO": a_v_num, "Division Category": div_cat, "Area Code": area.get('code', '')
                    })

                run_query("INSERT INTO route_plan_reasons (plan_date, area, role, selected_person, score, reasons, generated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", reason_data, table_name="route_plan_reasons", action="INSERT_MANY")
                run_query("INSERT INTO vacation_predictions (person_code, person_name, role, suggested_start, reason, replacement_person, replacement_date) VALUES (?, ?, ?, ?, ?, ?, ?)", predict_data, table_name="vacation_predictions", action="INSERT_MANY")

                run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
                draft_inserts = []
                for index, r in enumerate(route_plan):
                    draft_inserts.append((index+1, r['Area Code'], r['AREA'], r['Sector'], r['Driver Code'], r['Drivers Name'], r['Helper Code'], r['Helpers Name'], r['VEH NO'], r['Division Category'], p_s_gen, p_e_gen))
                run_query("INSERT INTO draft_routes (order_num, area_code, area_name, sector, driver_code, driver_name, helper_code, helper_name, veh_num, div_cat, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", draft_inserts, table_name="draft_routes", action="INSERT_MANY")
                
                st.session_state.force_bypass = False
                st.rerun()

    # --- AI REASONING EXPLANATION TABLE ---
    reasons_df = load_table('route_plan_reasons')
    predict_df = load_table('vacation_predictions')
    if not reasons_df.empty:
        with st.expander("🤖 View AI Reasoning & Future Replacement Logs", expanded=False):
            st.caption("Detailed breakdown of why the AI selected each candidate based on the weighted scoring logic.")
            
            explain_list = []
            for area in load_table('areas')['name'].unique():
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
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles", "📥 Bulk Excel Sync"])

    # DRIVERS TAB
    with tab1:
        st.subheader("📋 Full Drivers List")
        drivers_df = load_table('drivers')
        disp_df = drivers_df.drop(columns=['restriction'], errors='ignore').copy()
        
        search_d = st.text_input("🔍 Search Drivers by Code, Name, Area, etc.", key="search_drivers")
        if search_d and not disp_df.empty:
            disp_df = disp_df[disp_df.astype(str).apply(lambda x: x.str.contains(search_d, case=False, na=False)).any(axis=1)]
        if not disp_df.empty: disp_df.insert(0, 'S/N', range(1, 1 + len(disp_df)))
        
        veh_opts = list(set(VEHICLE_OPTIONS + disp_df.get('veh_type', pd.Series()).dropna().unique().tolist()))
        sec_opts = list(set(SECTOR_OPTIONS + disp_df.get('sector', pd.Series()).dropna().unique().tolist()))
        
        edited_d = st.data_editor(
            disp_df, 
            column_config={
                "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "veh_type": st.column_config.SelectboxColumn("Vehicle Type", options=veh_opts),
                "sector": st.column_config.SelectboxColumn("Sector", options=sec_opts),
                "needs_helper": st.column_config.SelectboxColumn("Needs Helper", options=NEEDS_HELPER_OPTIONS),
                "anchor_area": st.column_config.TextColumn("Anchor(s) (comma-separated)", help="Type Areas, Sectors, or Veh Types separated by commas")
            }, use_container_width=True, height=250, hide_index=True, key="ed_drivers"
        )
        if st.button("💾 Save Table Edits", key="save_table_drivers"):
            if "ed_drivers" in st.session_state:
                changes = st.session_state["ed_drivers"].get("edited_rows", {})
                for row_idx, col_changes in changes.items():
                    row_id = disp_df.iloc[row_idx]['id']
                    sql_sets = ", ".join([f"{k}=?" for k in col_changes.keys()])
                    run_query(f"UPDATE drivers SET {sql_sets} WHERE id=?", tuple(list(col_changes.values()) + [row_id]), table_name="drivers", action="UPDATE", doc_id=row_id, data=col_changes)
            st.success("Drivers saved successfully!")
            st.rerun()
            
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Driver")
            d_name = st.text_input("New Driver Name", key="add_d_name")
            d_code = st.text_input("New Driver Code", key="add_d_code").strip()
            col_t, col_s, col_h = st.columns(3)
            d_type = col_t.selectbox("New Driver Veh Type", VEHICLE_OPTIONS, key="add_d_type")
            d_sec = col_s.selectbox("New Driver Sector", SECTOR_OPTIONS, key="add_d_sec")
            d_needs_h = col_h.selectbox("New Driver Needs Helper?", NEEDS_HELPER_OPTIONS, index=2, key="add_d_nh")
            
            d_anchor_opts = st.multiselect("New Driver Anchor(s)", multi_anchor_opts, key="add_d_anchor")
            d_anchor_str = ", ".join(d_anchor_opts) if d_anchor_opts else "None"
            
            if st.button("➕ Add Driver", use_container_width=True):
                if drivers_df['code'].isin([d_code]).any():
                    st.error(f"Driver Code {d_code} already exists! Cannot duplicate.")
                else:
                    if run_query("INSERT INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                              (d_name, d_code, unify_text(d_type), unify_text(d_sec), d_needs_h, "None", d_anchor_str), table_name="drivers", action="INSERT", data={"name":d_name, "code":d_code, "veh_type":unify_text(d_type), "sector":unify_text(d_sec), "needs_helper":d_needs_h, "restriction":"None", "anchor_area":d_anchor_str}):
                        st.success("Driver Added!")
                        st.rerun()

        with c_edit:
            st.subheader("🗑️ Delete Driver")
            sel_d_code = st.selectbox("Select Driver to Delete", drivers_df['code'].tolist() if not drivers_df.empty else [])
            if sel_d_code:
                d_data = drivers_df[drivers_df['code'] == sel_d_code].iloc[0]
                if st.button("🗑️ Delete Driver", use_container_width=True, key=f"btn_d_del_{d_data['id']}"):
                    if run_query("DELETE FROM drivers WHERE code=?", (sel_d_code,), table_name="drivers", action="DELETE_DOC", doc_id=d_data['id']):
                        st.success("Driver Deleted!")
                        st.rerun()

    # HELPERS TAB
    with tab2:
        st.subheader("📋 Full Helpers List")
        helpers_df = load_table('helpers')
        disp_h = helpers_df.drop(columns=['restriction'], errors='ignore').copy()
        
        search_h = st.text_input("🔍 Search Helpers", key="search_helpers")
        if search_h and not disp_h.empty: disp_h = disp_h[disp_h.astype(str).apply(lambda x: x.str.contains(search_h, case=False, na=False)).any(axis=1)]
        if not disp_h.empty: disp_h.insert(0, 'S/N', range(1, 1 + len(disp_h)))
        
        edited_h = st.data_editor(
            disp_h, column_config={
                "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "health_card": st.column_config.SelectboxColumn("Health Card", options=["Yes", "No"]),
                "anchor_area": st.column_config.TextColumn("Anchor(s) (comma-separated)", help="Type Areas, Sectors, or Veh Types separated by commas")
            }, use_container_width=True, height=250, hide_index=True, key="ed_helpers"
        )
        if st.button("💾 Save Table Edits", key="save_table_helpers"):
            if "ed_helpers" in st.session_state:
                changes = st.session_state["ed_helpers"].get("edited_rows", {})
                for row_idx, col_changes in changes.items():
                    row_id = disp_h.iloc[row_idx]['id']
                    sql_sets = ", ".join([f"{k}=?" for k in col_changes.keys()])
                    run_query(f"UPDATE helpers SET {sql_sets} WHERE id=?", tuple(list(col_changes.values()) + [row_id]), table_name="helpers", action="UPDATE", doc_id=row_id, data=col_changes)
            st.success("Helpers saved successfully!")
            st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Helper")
            h_name = st.text_input("New Helper Name", key="add_h_name")
            h_code = st.text_input("New Helper Code", key="add_h_code").strip()
            h_health = st.selectbox("New Helper Health Card?", ["No", "Yes"], key="add_h_hc")
            
            h_anchor_opts = st.multiselect("New Helper Anchor(s)", multi_anchor_opts, key="add_h_anc")
            h_anchor_str = ", ".join(h_anchor_opts) if h_anchor_opts else "None"
            
            if st.button("➕ Add Helper", use_container_width=True):
                if helpers_df['code'].isin([h_code]).any():
                    st.error(f"Helper Code {h_code} already exists! Cannot duplicate.")
                else:
                    if run_query("INSERT INTO helpers (name, code, health_card, restriction, anchor_area) VALUES (?, ?, ?, ?, ?)", (h_name, h_code, h_health, "None", h_anchor_str), table_name="helpers", action="INSERT", data={"name":h_name, "code":h_code, "health_card":h_health, "restriction":"None", "anchor_area":h_anchor_str}):
                        st.success("Helper Added!")
                        st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Helper")
            sel_h_code = st.selectbox("Select Helper to Delete", helpers_df['code'].tolist() if not helpers_df.empty else [])
            if sel_h_code:
                h_data = helpers_df[helpers_df['code'] == sel_h_code].iloc[0]
                if st.button("🗑️ Delete Helper", use_container_width=True, key=f"btn_h_del_{h_data['id']}"):
                    if run_query("DELETE FROM helpers WHERE code=?", (sel_h_code,), table_name="helpers", action="DELETE_DOC", doc_id=h_data['id']):
                        st.success("Helper Deleted!")
                        st.rerun()

    # AREAS TAB
    with tab3:
        st.subheader("📋 Route Template Areas")
        a_df = load_table('areas')
        disp_a = a_df.drop(columns=['sort_order'], errors='ignore').copy()
        
        search_a = st.text_input("🔍 Search Areas", key="search_areas")
        if search_a and not disp_a.empty: disp_a = disp_a[disp_a.astype(str).apply(lambda x: x.str.contains(search_a, case=False, na=False)).any(axis=1)]
        if not disp_a.empty: disp_a.insert(0, 'S/N', range(1, 1 + len(disp_a)))
        
        a_sec_opts = list(set(SECTOR_OPTIONS + disp_a.get('sector', pd.Series()).dropna().unique().tolist()))
        
        edited_a = st.data_editor(
            disp_a, column_config={
                "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "sector": st.column_config.SelectboxColumn("Sector", options=a_sec_opts),
                "needs_helper": st.column_config.SelectboxColumn("Needs Helper", options=NEEDS_HELPER_OPTIONS)
            }, use_container_width=True, height=250, hide_index=True, key="ed_areas"
        )
        if st.button("💾 Save Table Edits", key="save_table_areas"):
            if "ed_areas" in st.session_state:
                changes = st.session_state["ed_areas"].get("edited_rows", {})
                for row_idx, col_changes in changes.items():
                    row_id = disp_a.iloc[row_idx]['id']
                    sql_sets = ", ".join([f"{k}=?" for k in col_changes.keys()])
                    run_query(f"UPDATE areas SET {sql_sets} WHERE id=?", tuple(list(col_changes.values()) + [row_id]), table_name="areas", action="UPDATE", doc_id=row_id, data=col_changes)
            st.success("Areas saved successfully!")
            st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Area")
            a_name = st.text_input("New Area Name", key="add_a_name").strip()
            a_code = st.text_input("New Area Code", key="add_a_code")
            col_s, col_n = st.columns(2)
            a_sec = col_s.selectbox("New Area Sector", SECTOR_OPTIONS, key="add_a_sec")
            a_needs = col_n.selectbox("New Area Needs Helper?", NEEDS_HELPER_OPTIONS, key="add_a_nh")
            if st.button("➕ Add Area", use_container_width=True):
                if a_df['name'].isin([a_name]).any():
                    st.error(f"Area {a_name} already exists! Cannot duplicate.")
                else:
                    new_order = len(a_df) + 1
                    if run_query("INSERT INTO areas (name, code, sector, needs_helper, sort_order) VALUES (?, ?, ?, ?, ?)", (a_name, a_code, unify_text(a_sec), a_needs, new_order), table_name="areas", action="INSERT", data={"name":a_name, "code":a_code, "sector":unify_text(a_sec), "needs_helper":a_needs, "sort_order":new_order}):
                        st.success("Area Added!")
                        st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Area")
            sel_a = st.selectbox("Select Area to Delete", a_df['name'].tolist() if not a_df.empty else [])
            if sel_a:
                a_data = a_df[a_df['name'] == sel_a].iloc[0]
                if st.button("🗑️ Delete Area", use_container_width=True, key=f"btn_a_del_{a_data['id']}"):
                    if run_query("DELETE FROM areas WHERE name=?", (sel_a,), table_name="areas", action="DELETE_DOC", doc_id=a_data['id']):
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
        
        v_type_opts = list(set(VEHICLE_OPTIONS + disp_v.get('type', pd.Series()).dropna().unique().tolist()))
        v_div_opts = list(set(["Pharma", "Consumer", "2-8 VAN"] + disp_v.get('division', pd.Series()).dropna().unique().tolist()))
        
        edited_v = st.data_editor(
            disp_v, column_config={
                "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "type": st.column_config.SelectboxColumn("Type", options=v_type_opts),
                "division": st.column_config.SelectboxColumn("Division", options=v_div_opts),
                "status": st.column_config.SelectboxColumn("Status", options=["Active", "Under Service", "In for Service"]),
                "anchor_area": st.column_config.TextColumn("Anchor(s) (comma-separated)", help="Type Areas, Sectors, or Veh Types separated by commas"),
                "permitted_areas": st.column_config.TextColumn("Permitted Areas", help="Type Permitted Regions (e.g. Dubai, Sharjah)")
            }, use_container_width=True, height=250, hide_index=True, key="ed_vehicles"
        )
        if st.button("💾 Save Table Edits", key="save_table_vehicles"):
            if "ed_vehicles" in st.session_state:
                changes = st.session_state["ed_vehicles"].get("edited_rows", {})
                for row_idx, col_changes in changes.items():
                    row_id = disp_v.iloc[row_idx]['id']
                    sql_sets = ", ".join([f"{k}=?" for k in col_changes.keys()])
                    run_query(f"UPDATE vehicles SET {sql_sets} WHERE id=?", tuple(list(col_changes.values()) + [row_id]), table_name="vehicles", action="UPDATE", doc_id=row_id, data=col_changes)
            st.success("Vehicles saved successfully!")
            st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Vehicle")
            v_num = st.text_input("New Vehicle Number", key="add_v_num").strip()
            v_type = st.selectbox("New Vehicle Type", VEHICLE_OPTIONS, key="add_v_type")
            v_div = st.selectbox("New Vehicle Division", v_div_opts, key="add_v_div")
            v_perm = st.text_input("Permitted Areas (e.g. Dubai, Sharjah)", value="All", key="add_v_perm")
            v_stat = st.selectbox("Status", ["Active", "Under Service", "In for Service"], key="add_v_stat")
            
            v_anchor_opts = st.multiselect("New Vehicle Anchor(s)", multi_anchor_opts, key="add_v_anc")
            v_anchor_str = ", ".join(v_anchor_opts) if v_anchor_opts else "None"
            
            if st.button("➕ Add Vehicle", use_container_width=True):
                if v_df['number'].isin([v_num]).any():
                    st.error(f"Vehicle Number {v_num} already exists! Cannot duplicate.")
                else:
                    if run_query("INSERT INTO vehicles (number, type, permitted_areas, division, anchor_area, status) VALUES (?, ?, ?, ?, ?, ?)", (v_num, unify_text(v_type), v_perm, unify_text(v_div), v_anchor_str, v_stat), table_name="vehicles", action="INSERT", data={"number":v_num, "type":unify_text(v_type), "permitted_areas":v_perm, "division":unify_text(v_div), "anchor_area":v_anchor_str, "status":v_stat}):
                        st.success("Vehicle Added!")
                        st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Vehicle")
            sel_v = st.selectbox("Select Vehicle to Delete", v_df['number'].tolist() if not v_df.empty else [])
            if sel_v:
                v_data = v_df[v_df['number'] == sel_v].iloc[0]
                if st.button("🗑️ Delete Veh", use_container_width=True, key=f"btn_v_del_{v_data['id']}"):
                    if run_query("DELETE FROM vehicles WHERE number=?", (sel_v,), table_name="vehicles", action="DELETE_DOC", doc_id=v_data['id']):
                        st.success("Vehicle Deleted!")
                        st.rerun()

    with tab5:
        st.subheader("📥 Export Database")
        dfs_to_export = [load_table(t) for t in ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations']]
        output = generate_excel_with_sn(dfs_to_export, ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations'])
        st.download_button("📥 Download Master Database (Excel)", data=output, file_name="Master_Database.xlsx", type="primary")

        st.divider()
        uploaded_file = st.file_uploader("Upload your modified Excel to Sync", type=['xlsx'])
        if uploaded_file and st.button("Sync Data to System", type="primary"):
            xls = pd.ExcelFile(uploaded_file)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                run_query(None, table_name=sheet, action="CLEAR_TABLE")

                for _, row in df.iterrows():
                    data_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v) and k not in ['id', 'S/N']}
                    cols, vals = ', '.join(data_dict.keys()), tuple(data_dict.values())
                    qmarks = ', '.join(['?'] * len(data_dict))
                    run_query(f"INSERT OR IGNORE INTO {sheet} ({cols}) VALUES ({qmarks})", vals, table_name=sheet, action="INSERT", data=data_dict)
            st.success("Database synchronized successfully!")

        st.divider()
        st.subheader("🚨 Emergency Route Template Restore")
        st.warning("If your Areas or Vehicles got messed up, click this to reset the Route Template exactly to your Image Layout.")
        if st.button("♻️ Restore 39-Row Route Layout & Vehicles", type="primary"):
            with st.spinner("Restoring layout..."):
                execute_global_init(force=True)
            st.success("Layout restored successfully!")
            st.rerun()


# ==========================================
# SCREEN 3: PAST EXPERIENCE BUILDER
# ==========================================
elif choice == "3. Past Experience Builder":
    st.header("🕰️ Manage Past Experience")
    history_df = load_table('history')
    areas_df = load_table('areas')
    area_list = areas_df['name'].tolist() if not areas_df.empty else []
    
    search_hist = st.text_input("🔍 Search History by Exact Date, Month, Year, Code, Name, or Area", "")
    disp_hist = history_df.sort_values(by="date", ascending=False).copy()
    
    if search_hist and not disp_hist.empty:
        disp_hist = disp_hist[disp_hist.astype(str).apply(lambda x: x.str.contains(search_hist, case=False, na=False)).any(axis=1)]
    if not disp_hist.empty: disp_hist.insert(0, 'S/N', range(1, 1 + len(disp_hist)))
    
    h_sec_opts = list(set(SECTOR_OPTIONS + disp_hist.get('sector', pd.Series()).dropna().unique().tolist()))
    h_area_opts = list(set(area_list + disp_hist.get('area', pd.Series()).dropna().unique().tolist()))
    
    edited_hist = st.data_editor(
        disp_hist, column_config={
            "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
            "person_type": st.column_config.SelectboxColumn("Role", options=["Driver", "Helper"]),
            "sector": st.column_config.SelectboxColumn("Sector", options=h_sec_opts),
            "area": st.column_config.SelectboxColumn("Area", options=h_area_opts)
        }, use_container_width=True, height=350, hide_index=True, key="ed_hist"
    )
    
    if st.button("💾 Save Table Edits", key="save_table_hist"):
        if "ed_hist" in st.session_state:
            changes = st.session_state["ed_hist"].get("edited_rows", {})
            for row_idx, col_changes in changes.items():
                row_id = disp_hist.iloc[row_idx]['id']
                sql_sets = ", ".join([f"{k}=?" for k in col_changes.keys()])
                run_query(f"UPDATE history SET {sql_sets} WHERE id=?", tuple(list(col_changes.values()) + [row_id]), table_name="history", action="UPDATE", doc_id=row_id, data=col_changes)
        st.success("Experience saved successfully!")
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
    st.info("Upload an Excel (.xlsx) or PDF file containing history data. The AI will parse it, unify formatting (e.g. 2-8 VAN), and strictly prevent duplicates.")
    bulk_file = st.file_uploader("Upload Experience Data", type=['xlsx', 'pdf'])
    
    if bulk_file and st.button("Sync Uploaded Data", type="primary"):
        with st.spinner("Processing file intelligently..."):
            new_records = []
            
            if bulk_file.name.endswith('.xlsx'):
                df_up = pd.read_excel(bulk_file)
                
                code_col = next((c for c in df_up.columns if 'code' in c.lower()), None)
                name_col = next((c for c in df_up.columns if 'name' in c.lower()), None)
                area_col = next((c for c in df_up.columns if 'area' in c.lower()), None)
                div_col = next((c for c in df_up.columns if 'div' in c.lower()), None)
                from_col = next((c for c in df_up.columns if 'from' in c.lower() or 'start' in c.lower()), None)
                to_col = next((c for c in df_up.columns if 'to' in c.lower() or 'end' in c.lower()), None)
                
                if code_col and area_col and from_col:
                    for _, row in df_up.iterrows():
                        c_val = str(row[code_col]).strip()
                        if pd.isna(c_val) or c_val == "nan": continue
                        
                        n_val = str(row[name_col]).strip() if name_col else "Unknown"
                        a_val = str(row[area_col]).strip()
                        d_val = unify_text(str(row[div_col]).strip() if div_col else "Pharma")
                        f_val = parse_date_safe(row[from_col])
                        t_val = parse_date_safe(row[to_col]) if to_col else f_val
                        
                        if f_val and t_val:
                            ptype = "Helper" if c_val.startswith('H') else "Driver"
                            new_records.append((ptype, c_val, n_val, a_val, d_val, f_val, t_val))
                            
            elif bulk_file.name.endswith('.pdf'):
                if PDF_ENABLED:
                    pdf_reader = PyPDF2.PdfReader(bulk_file)
                    text = ""
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n"
                        
                    for line in text.split('\n'):
                        match = re.search(r'([A-Z]{1,2}\d{3})\s+(.*?)\s+(.*?)\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})', line)
                        if match:
                            c_val = match.group(1).strip()
                            n_val = match.group(2).strip()
                            a_val = match.group(3).strip()
                            d_val = "Pharma"
                            f_val = parse_date_safe(match.group(4))
                            t_val = parse_date_safe(match.group(5))
                            ptype = "Helper" if c_val.startswith('H') else "Driver"
                            new_records.append((ptype, c_val, n_val, a_val, d_val, f_val, t_val))
                else:
                    st.error("PyPDF2 library not found. Please add 'PyPDF2' to requirements.txt")

            if new_records:
                q_hist = "INSERT OR IGNORE INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)"
                run_query(q_hist, new_records, table_name="history", action="INSERT_MANY")
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
            p_sec = st.selectbox("Which Sector was this in?", SECTOR_OPTIONS)
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
                hist_id = hist_map[sel_hist_str]
                if st.button("🗑️ Delete Experience", use_container_width=True, key=f"he_del_{hist_id}"):
                    if run_query("DELETE FROM history WHERE id=?", (hist_id,), table_name="history", action="DELETE_DOC", doc_id=hist_id):
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
            sd = safe_parse_date(row['start_date'])
            ed = safe_parse_date(row['end_date'])
            if sd and ed and sd <= today <= ed:
                active_vacs.append({
                    "Role": row['person_type'],
                    "Code": row.get('person_code', 'UNK'),
                    "Name": row['person_name'],
                    "Return Date": ed.strftime("%b %d, %Y"),
                    "Days Left": (ed - today).days
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
                    days_since = (today - last_vac).days
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
            "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
            "person_type": st.column_config.SelectboxColumn("Role", options=["Driver", "Helper"])
        }, use_container_width=True, height=250, hide_index=True, key="ed_vac"
    )
    
    if st.button("💾 Save Table Edits", key="save_table_vacs"):
        if "ed_vac" in st.session_state:
            changes = st.session_state["ed_vac"].get("edited_rows", {})
            for row_idx, col_changes in changes.items():
                row_id = disp_vac.iloc[row_idx]['id']
                sql_sets = ", ".join([f"{k}=?" for k in col_changes.keys()])
                run_query(f"UPDATE vacations SET {sql_sets} WHERE id=?", tuple(list(col_changes.values()) + [row_id]), table_name="vacations", action="UPDATE", doc_id=row_id, data=col_changes)
        st.success("Vacations saved successfully!")
        st.rerun()

    with st.expander("📥 Export / 📤 Import Vacation Data"):
        output = generate_excel_with_sn([vacs_df], ['vacations'])
        st.download_button("📥 Download Vacation Data", data=output, file_name="Vacation_Data.xlsx")
        
        up_vac = st.file_uploader("Upload Vacation Excel", type=['xlsx'], key="up_vac")
        if up_vac and st.button("Sync Vacation Database"):
            df = pd.read_excel(up_vac)
            run_query(None, table_name="vacations", action="CLEAR_TABLE")
            
            for _, row in df.iterrows():
                data_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v) and k not in ['id', 'S/N']}
                cols, vals = ', '.join(data_dict.keys()), tuple(data_dict.values())
                qmarks = ', '.join(['?'] * len(data_dict))
                run_query(f"INSERT OR IGNORE INTO vacations ({cols}) VALUES ({qmarks})", vals, table_name="vacations", action="INSERT", data=data_dict)
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
                vac_id = vac_map[sel_vac_str]
                if st.button("🗑️ Delete Vacation", use_container_width=True, key=f"vac_del_{vac_id}"):
                    if run_query("DELETE FROM vacations WHERE id=?", (vac_id,), table_name="vacations", action="DELETE_DOC", doc_id=vac_id):
                        st.success("Vacation Deleted!")
                        st.rerun()
