import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import io
import os
import textwrap
import firebase_admin
from firebase_admin import credentials, firestore

# --- UI CONFIGURATION ---
st.set_page_config(page_title="Logistics AI Planner", layout="wide")

# --- FIREBASE INITIALIZATION & DB ADAPTER ---
FIREBASE_READY = False
conn = None

try:
    if "firebase" in st.secrets:
        if not firebase_admin._apps:
            # Securely parse Streamlit secrets into standard dictionary
            cert_dict = dict(st.secrets["firebase"])
            
            # Bulletproof PEM reconstruction to prevent any formatting crashes
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
        st.sidebar.error("⚠️ No Firebase configuration found in Streamlit Secrets.")
        FIREBASE_READY = False

    # Connection Ping Test
    if FIREBASE_READY:
        try:
            list(db_fs.collection("_system_ping").limit(1).stream())
        except Exception as ping_error:
            if "429" in str(ping_error) or "Quota" in str(ping_error) or "ResourceExhausted" in str(ping_error):
                st.sidebar.error("🚨 Firebase Daily Free Quota Exceeded! Firebase is temporarily paused. Please check Google Cloud Console.")
            else:
                st.sidebar.error(f"⚠️ Firebase Database is unreachable: {ping_error}")
            FIREBASE_READY = False

except Exception as e:
    st.sidebar.error(f"Firebase Config Error: {str(e)}")
    FIREBASE_READY = False

if FIREBASE_READY:
    st.sidebar.markdown("<div style='text-align: right; font-size: 20px; margin-top: -15px;' title='Connected to Secure Cloud'>🟢 Firebase Connected</div>", unsafe_allow_html=True)
else:
    st.sidebar.markdown("<div style='text-align: right; font-size: 20px; margin-top: -15px;' title='Local Database Mode'>🔴 Firebase Disconnected</div>", unsafe_allow_html=True)

# SQLite Fallback Initialization
def init_sqlite_db():
    local_conn = sqlite3.connect('logistics.db', check_same_thread=False)
    c = local_conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY, name TEXT, code TEXT, veh_type TEXT, sector TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (id INTEGER PRIMARY KEY, name TEXT, code TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY, name TEXT, code TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY, number TEXT, type TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, area TEXT, date TEXT, end_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, start_date DATE, end_date DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT, start_date TEXT, end_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS draft_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT, start_date TEXT, end_date TEXT, div_cat TEXT, sector TEXT)''')
    
    # NEW AI REASONING TABLES
    c.execute('''CREATE TABLE IF NOT EXISTS route_plan_reasons (id INTEGER PRIMARY KEY, plan_date TEXT, area TEXT, role TEXT, selected_person TEXT, score REAL, reasons TEXT, generated_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacation_predictions (id INTEGER PRIMARY KEY, person_code TEXT, person_name TEXT, role TEXT, suggested_start TEXT, suggested_end TEXT, reason TEXT, replacement_person TEXT, replacement_date TEXT)''')
    
    for query in [
        "ALTER TABLE drivers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE drivers ADD COLUMN needs_helper TEXT DEFAULT 'Yes'",
        "ALTER TABLE helpers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE helpers ADD COLUMN health_card TEXT DEFAULT 'No'",
        "ALTER TABLE history ADD COLUMN end_date TEXT",
        "ALTER TABLE history ADD COLUMN person_code TEXT DEFAULT 'UNKNOWN'",
        "ALTER TABLE history ADD COLUMN sector TEXT DEFAULT 'Pharma'",
        "ALTER TABLE areas ADD COLUMN sector TEXT DEFAULT 'Pharma'",
        "ALTER TABLE areas ADD COLUMN needs_helper TEXT DEFAULT 'Yes'",
        "ALTER TABLE areas ADD COLUMN sort_order INTEGER DEFAULT 99",
        "ALTER TABLE vehicles ADD COLUMN anchor_area TEXT DEFAULT 'None'",
        "ALTER TABLE active_routes ADD COLUMN start_date TEXT DEFAULT 'None'",
        "ALTER TABLE active_routes ADD COLUMN end_date TEXT DEFAULT 'None'",
        "ALTER TABLE vacations ADD COLUMN person_code TEXT DEFAULT 'UNKNOWN'",
        "ALTER TABLE vacation_predictions ADD COLUMN replacement_date TEXT DEFAULT 'None'"
    ]:
        try: c.execute(query)
        except sqlite3.OperationalError: pass
    local_conn.commit()
    return local_conn

if not FIREBASE_READY:
    conn = init_sqlite_db()


# --- SMART DB QUERY HANDLER (CACHE PROTECTS FIREBASE QUOTA) ---
def clear_cache():
    st.cache_data.clear()

@st.cache_data(show_spinner=False, ttl=600)
def load_table(table_name):
    if FIREBASE_READY:
        try:
            docs = db_fs.collection(table_name).stream()
            data = [{**doc.to_dict(), 'id': doc.id} for doc in docs]
            df = pd.DataFrame(data)
            if df.empty: return df
            if table_name == 'helpers' and 'health_card' not in df.columns: df['health_card'] = 'No'
            if table_name == 'drivers' and 'needs_helper' not in df.columns: df['needs_helper'] = 'Yes'
            if table_name == 'areas' and 'sector' not in df.columns: df['sector'] = 'Pharma'
            if table_name == 'areas' and 'needs_helper' not in df.columns: df['needs_helper'] = 'Yes'
            if table_name == 'areas' and 'sort_order' not in df.columns: df['sort_order'] = 99
            if table_name == 'history' and 'sector' not in df.columns: df['sector'] = 'Pharma'
            if table_name == 'vehicles' and 'anchor_area' not in df.columns: df['anchor_area'] = 'None'
            if table_name == 'active_routes' and 'start_date' not in df.columns: df['start_date'] = 'None'
            if table_name == 'vacations' and 'person_code' not in df.columns: df['person_code'] = 'UNKNOWN'
            
            if table_name == 'areas':
                df['sort_order'] = pd.to_numeric(df['sort_order'], errors='coerce').fillna(99)
                df = df.sort_values(by='sort_order')
            if table_name in ['active_routes', 'draft_routes'] and 'order_num' in df.columns:
                df['order_num'] = pd.to_numeric(df['order_num'], errors='coerce').fillna(99)
                df = df.sort_values(by='order_num')
                
            if table_name == 'history': df['sector'] = df['sector'].fillna('Pharma')
            return df
        except Exception as e:
            st.error(f"Error reading from Firebase: {e}")
            return pd.DataFrame()
    else:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        if df.empty: return df
        if table_name == 'areas': 
            df['sort_order'] = pd.to_numeric(df['sort_order'], errors='coerce').fillna(99)
            df = df.sort_values(by='sort_order')
        if table_name in ['active_routes', 'draft_routes'] and 'order_num' in df.columns:
            df['order_num'] = pd.to_numeric(df['order_num'], errors='coerce').fillna(99)
            df = df.sort_values(by='order_num')
        if table_name == 'history': df['sector'] = df['sector'].fillna('Pharma')
        if table_name == 'vacations' and 'person_code' not in df.columns: df['person_code'] = 'UNKNOWN'
        return df

def run_query(query, params=(), table_name=None, action=None, doc_id=None, data=None):
    try:
        if FIREBASE_READY and table_name and action:
            if action == "INSERT": db_fs.collection(table_name).add(data)
            elif action == "UPDATE" and doc_id: db_fs.collection(table_name).document(str(doc_id)).update(data)
            elif action == "DELETE_DOC" and doc_id: db_fs.collection(table_name).document(str(doc_id)).delete()
            elif action == "CLEAR_TABLE":
                docs = db_fs.collection(table_name).stream()
                for doc in docs: doc.reference.delete()
        else:
            if query:
                c = conn.cursor()
                c.execute(query, params)
                conn.commit()
                
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

# Adjusted to match the Image while keeping Pandas unique names
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
    ("PH-JAB", "JABEL ALI", "Pharma", "Yes", 13), 
    ("28-CC1", "COLD CHAIN/URGENT ORDERS", "2-8", "No", 14), ("28-CC2", "COLD CHAIN/URGENT ORDERS", "2-8", "No", 15),
    ("PH-SAMP", "Sample Driver", "Pharma", "Yes", 16), ("PH-2ND1", "2ND TRIP", "Pharma", "Yes", 17),
    ("PH-2ND2", "2ND TRIP", "Pharma", "Yes", 18), 
    ("GOV-1", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 19), ("GOV-2", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 20),
    ("GOV-3", "GOVT/URGENT ORDERS", "Govt / Urgent", "No", 21), ("FLE-1", "FLEET SERVICE/RTA WORK", "Fleet", "No", 22),
    ("PU-SUB", "SUBTITUTE/PICK UP", "Substitute", "No", 23), 
    ("PU-1", "PICK UP", "Bulk / Pick-Up", "Yes", 24), ("PU-2", "PICK UP/SHJ", "Bulk / Pick-Up", "Yes", 25),
    ("PU-3", "PICK UP", "Bulk / Pick-Up", "Yes", 26), ("PU-4", "PICK UP/SHJ", "Bulk / Pick-Up", "Yes", 27),
    ("PU-5", "PICK UP", "Bulk / Pick-Up", "Yes", 28), ("PU-6", "PICK UP", "Bulk / Pick-Up", "Yes", 29),
    ("CON-ALQ", "ALQ", "Consumer", "Yes", 30), ("CON-JAB", "JA", "Consumer", "Yes", 31),
    ("CON-DXBO", "DXBO", "Consumer", "Yes", 32), ("CON-BUR", "BUR", "Consumer", "Yes", 33),
    ("CON-RAK", "RAK", "Consumer", "Yes", 34), ("CON-PU1", "PICK UP/SHJ (C)", "Consumer", "Yes", 35),
    ("CON-PU2", "PICK UP (C)", "Consumer", "Yes", 36), ("CON-AJM", "AJM", "Consumer", "Yes", 37),
    ("CON-SHJS", "SHJS", "Consumer", "Yes", 38), ("CON-SUB", "SUBTITUTE/URGENT ORDERS", "Substitute", "No", 39)
]

SEED_VEHICLES = [
    ("M 95321", "PICK-UP"), ("C 47055", "PICK-UP"), ("B 14813", "BUS"), ("C 58107", "PICK-UP"), ("C 58801", "VAN"),
    ("16 47645", "BUS"), ("I 85664", "PICK-UP"), ("I 86488", "VAN"), ("R 96871", "BUS"), ("U 65986", "PICK-UP"),
    ("U 65988", "VAN"), ("U 65990", "VAN"), ("V-83576", "VAN"), ("V-84049", "VAN"), ("V-84050", "VAN"),
    ("W 49535", "PICK-UP"), ("W 49536", "VAN"), ("W 49539", "VAN"), ("W 49540", "VAN"), ("O 72506", "VAN"),
    ("O 72533", "PICK-UP"), ("O 72548", "PICK-UP"), ("O 72567", "VAN"), ("O 72578", "VAN"), ("O 72579", "VAN"),
    ("O 72581", "VAN"), ("D 85038", "VAN"), ("D 85076", "VAN"), ("D 85823", "VAN"), ("C 26596", "BUS"),
    ("V 60857", "VAN"), ("N 31329", "VAN"), ("N 32094", "VAN"), ("N 32119", "VAN"), ("N 32126", "VAN"),
    ("N 33680", "VAN"), ("E 18104", "VAN"), ("E 18316", "VAN"), ("BB 72473", "BUS"), ("I 71528", "PICK-UP"),
    ("W 11792", "VAN"), ("T 26701", "VAN"), ("CC 98174", "VAN"), ("CC 98175", "VAN"), ("CC 98176", "VAN")
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

PRELOAD_HISTORY = [
    ("Helper", "H116", "2024-08-01", "2024-10-31", "MIRDIFF", "Pharma"), ("Helper", "H116", "2024-11-01", "2025-01-31", "2ND TRIP", "Pharma"),
    ("Helper", "H116", "2025-02-01", "2025-04-30", "ALQOUZ-2", "Pharma"), ("Helper", "H116", "2025-05-01", "2025-07-31", "ALQOUZ-1", "Pharma"),
    ("Helper", "H116", "2025-08-01", "2025-10-31", "DEIRA", "Pharma"), ("Helper", "H121", "2024-11-01", "2025-01-31", "BURDUBAI", "Pharma"),
    ("Helper", "H121", "2025-02-01", "2025-04-30", "JUMAIRAH", "Pharma"), ("Helper", "H121", "2025-05-01", "2025-07-31", "ALQ", "Consumer"),
    ("Helper", "H121", "2025-08-01", "2025-10-31", "RAK / UAQ", "Pharma"), ("Helper", "H119", "2024-08-01", "2024-10-31", "DEIRA", "Pharma"),
    ("Helper", "H119", "2024-11-01", "2025-01-31", "JABEL ALI", "Pharma"), ("Helper", "H119", "2025-02-01", "2025-04-30", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Helper", "H119", "2025-05-01", "2025-07-31", "MIRDIFF", "Pharma"), ("Helper", "H119", "2025-08-01", "2025-10-31", "JABEL ALI", "Pharma"),
    ("Helper", "H046", "2024-05-01", "2024-07-31", "ALQOUZ-1", "Pharma"), ("Helper", "H046", "2024-08-01", "2024-10-31", "BURDUBAI", "Pharma"),
    ("Helper", "H046", "2024-11-01", "2025-01-31", "FUJAIRAH", "Pharma"), ("Helper", "H046", "2025-02-01", "2025-04-30", "FUJAIRAH", "Pharma"),
    ("Helper", "H046", "2025-05-01", "2025-07-31", "FUJAIRAH", "Pharma"), ("Helper", "H046", "2025-08-01", "2025-10-31", "ALQ", "Consumer"),
    ("Helper", "H070", "2024-05-01", "2024-07-31", "PHARMA", "Pharma"), ("Helper", "H070", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H070", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H070", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H070", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H070", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H129", "2025-08-01", "2025-10-31", "DXBO", "Consumer"), ("Helper", "H113", "2024-05-01", "2024-07-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Helper", "H113", "2024-08-01", "2024-10-31", "AJMAN", "Pharma"), ("Helper", "H113", "2024-11-01", "2025-01-31", "AJMAN", "Pharma"),
    ("Helper", "H113", "2025-02-01", "2025-04-30", "SHJS", "Consumer"), ("Helper", "H113", "2025-05-01", "2025-07-31", "DEIRA", "Pharma"),
    ("Helper", "H113", "2025-08-01", "2025-10-31", "AJMAN", "Pharma"), ("Helper", "H118", "2024-08-01", "2024-10-31", "SHJ - SANAYYA", "Pharma"),
    ("Helper", "H118", "2024-11-01", "2025-01-31", "SHJ - SANAYYA", "Pharma"), ("Helper", "H118", "2025-02-01", "2025-04-30", "AJMAN", "Pharma"),
    ("Helper", "H118", "2025-05-01", "2025-07-31", "AJMAN", "Pharma"), ("Helper", "H118", "2025-08-01", "2025-10-31", "JABEL ALI", "Pharma"),
    ("Helper", "H115", "2024-05-01", "2024-07-31", "SHJ - SANAYYA", "Pharma"), ("Helper", "H115", "2025-02-01", "2025-04-30", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Helper", "H122", "2025-02-01", "2025-04-30", "QUSAIS", "Pharma"), ("Helper", "H122", "2025-05-01", "2025-07-31", "JABEL ALI", "Pharma"),
    ("Helper", "H122", "2025-08-01", "2025-10-31", "JABEL ALI", "Pharma"), ("Helper", "H114", "2024-05-01", "2024-07-31", "DEIRA", "Pharma"),
    ("Helper", "H114", "2024-08-01", "2024-10-31", "SHJ- BUH/ROLLA", "Pharma"), ("Helper", "H114", "2024-11-01", "2025-01-31", "DEIRA", "Pharma"),
    ("Helper", "H114", "2025-02-01", "2025-04-30", "JABEL ALI", "Pharma"), ("Helper", "H114", "2025-05-01", "2025-07-31", "SHJ - SANAYYA", "Pharma"),
    ("Helper", "H114", "2025-08-01", "2025-10-31", "BURDUBAI", "Pharma"), ("Helper", "H066", "2024-05-01", "2024-07-31", "SHJ - SANAYYA", "Pharma"),
    ("Helper", "H066", "2024-08-01", "2024-10-31", "DXBO", "Consumer"), ("Helper", "H066", "2024-11-01", "2025-01-31", "BURDUBAI", "Pharma"),
    ("Helper", "H066", "2025-02-01", "2025-04-30", "JA", "Consumer"), ("Helper", "H066", "2025-05-01", "2025-07-31", "JA", "Consumer"),
    ("Helper", "H066", "2025-08-01", "2025-10-31", "AJM", "Consumer"), ("Helper", "H011", "2024-05-01", "2024-07-31", "RAK / UAQ", "Pharma"),
    ("Helper", "H011", "2024-08-01", "2024-10-31", "QUSAIS", "Pharma"), ("Helper", "H011", "2024-11-01", "2025-01-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Helper", "H011", "2025-05-01", "2025-07-31", "JUMAIRAH", "Pharma"), ("Helper", "H011", "2025-08-01", "2025-10-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Helper", "H005", "2024-05-01", "2024-07-31", "BURDUBAI", "Pharma"), ("Helper", "H005", "2024-08-01", "2024-10-31", "ALQOUZ-2", "Pharma"),
    ("Helper", "H005", "2024-11-01", "2025-01-31", "RAK / UAQ", "Pharma"), ("Helper", "H005", "2025-05-01", "2025-07-31", "BURDUBAI", "Pharma"),
    ("Helper", "H005", "2025-08-01", "2025-10-31", "JUMAIRAH", "Pharma"), ("Helper", "H023", "2024-05-01", "2024-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Helper", "H023", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H023", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H023", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H023", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H023", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H050", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H050", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H050", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H050", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H050", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H062", "2024-05-01", "2024-07-31", "BURDUBAI", "Pharma"), ("Helper", "H062", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H062", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H062", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H062", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H062", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H024", "2024-05-01", "2024-07-31", "JABEL ALI", "Pharma"), ("Helper", "H024", "2024-08-01", "2024-10-31", "AJMAN", "Pharma"),
    ("Helper", "H024", "2024-11-01", "2025-01-31", "JA", "Consumer"), ("Helper", "H024", "2025-05-01", "2025-07-31", "SHJS", "Consumer"),
    ("Helper", "H024", "2025-08-01", "2025-10-31", "RAK", "Consumer"), ("Helper", "H082", "2024-05-01", "2024-07-31", "DXBO", "Consumer"),
    ("Helper", "H082", "2024-08-01", "2024-10-31", "ALQ", "Consumer"), ("Helper", "H082", "2024-11-01", "2025-01-31", "DXBO", "Consumer"),
    ("Helper", "H082", "2025-05-01", "2025-07-31", "DXBO", "Consumer"), ("Helper", "H082", "2025-08-01", "2025-10-31", "ALQ", "Consumer"),
    ("Helper", "H026", "2024-05-01", "2024-07-31", "ALQOUZ-1", "Pharma"), ("Helper", "H026", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H026", "2024-11-01", "2025-01-31", "ALQ", "Consumer"), ("Helper", "H026", "2025-02-01", "2025-04-30", "AJMAN", "Pharma"),
    ("Helper", "H026", "2025-05-01", "2025-07-31", "DXBO", "Consumer"), ("Helper", "H026", "2025-08-01", "2025-10-31", "SHJS", "Consumer"),
    ("Helper", "H109", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H109", "2024-08-01", "2024-10-31", "JA", "Consumer"),
    ("Helper", "H109", "2024-11-01", "2025-01-31", "AJM", "Consumer"), ("Helper", "H109", "2025-02-01", "2025-04-30", "DXBO", "Consumer"),
    ("Helper", "H109", "2025-05-01", "2025-07-31", "RAK", "Consumer"), ("Helper", "H109", "2025-08-01", "2025-10-31", "BUR", "Consumer"),
    ("Helper", "H013", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H013", "2024-08-01", "2024-10-31", "RAK", "Consumer"),
    ("Helper", "H013", "2024-11-01", "2025-01-31", "SHJS", "Consumer"), ("Helper", "H013", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H013", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H013", "2025-08-01", "2025-10-31", "AJM", "Consumer"),
    ("Helper", "H034", "2024-05-01", "2024-07-31", "AJMAN", "Pharma"), ("Helper", "H034", "2024-08-01", "2024-10-31", "FUJAIRAH", "Pharma"),
    ("Helper", "H034", "2024-11-01", "2025-01-31", "AJM", "Consumer"), ("Helper", "H034", "2025-05-01", "2025-07-31", "AJM", "Consumer"),
    ("Helper", "H034", "2025-08-01", "2025-10-31", "JA", "Consumer"), ("Helper", "H099", "2024-05-01", "2024-07-31", "ALQOUZ-1", "Pharma"),
    ("Helper", "H099", "2024-08-01", "2024-10-31", "ALQOUZ-1", "Pharma"), ("Helper", "H099", "2024-11-01", "2025-01-31", "MIRDIFF", "Pharma"),
    ("Helper", "H099", "2025-05-01", "2025-07-31", "QUSAIS", "Pharma"), ("Helper", "H099", "2025-08-01", "2025-10-31", "SHJ - SANAYYA", "Pharma"),
    ("Helper", "H017", "2024-05-01", "2024-07-31", "ALQOUZ-1", "Pharma"), ("Helper", "H017", "2024-08-01", "2024-10-31", "JUMAIRAH", "Pharma"),
    ("Helper", "H017", "2024-11-01", "2025-01-31", "ALQOUZ-1", "Pharma"), ("Helper", "H017", "2025-05-01", "2025-07-31", "RAK / UAQ", "Pharma"),
    ("Helper", "H017", "2025-08-01", "2025-10-31", "FUJAIRAH", "Pharma"), ("Helper", "H051", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H051", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H051", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H051", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H051", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H104", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H104", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H104", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H104", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H112", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H112", "2025-02-01", "2025-04-30", "PICK UP", "Bulk / Pick-Up"),
    ("Helper", "H112", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Helper", "H112", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),

    ("Driver", "D085", "2024-05-01", "2024-07-31", "COLD CHAIN/URGENT ORDERS", "2-8"), ("Driver", "D085", "2024-08-01", "2024-10-31", "COLD CHAIN/URGENT ORDERS", "2-8"),
    ("Driver", "D085", "2024-11-01", "2025-01-31", "COLD CHAIN/URGENT ORDERS", "2-8"), ("Driver", "D085", "2025-05-01", "2025-07-31", "ALQOUZ-2", "Pharma"),
    ("Driver", "D085", "2025-08-01", "2025-10-31", "SUBTITUTE/URGENT ORDERS", "Substitute"), ("Driver", "D034", "2024-05-01", "2024-07-31", "JABEL ALI", "Pharma"),
    ("Driver", "D034", "2024-08-01", "2024-10-31", "SHJ - SANAYYA", "Pharma"), ("Driver", "D034", "2024-11-01", "2025-01-31", "ALQOUZ-2", "Pharma"),
    ("Driver", "D034", "2025-02-01", "2025-04-30", "AJMAN", "Pharma"), ("Driver", "D034", "2025-05-01", "2025-07-31", "QUSAIS", "Pharma"),
    ("Driver", "D034", "2025-08-01", "2025-10-31", "ALQOUZ-1", "Pharma"), ("Driver", "D101", "2024-08-01", "2024-10-31", "ALQOUZ-2", "Pharma"),
    ("Driver", "D101", "2024-11-01", "2025-01-31", "QUSAIS", "Pharma"), ("Driver", "D101", "2025-02-01", "2025-04-30", "DXBO", "Consumer"),
    ("Driver", "D101", "2025-05-01", "2025-07-31", "SHJ - SANAYYA", "Pharma"), ("Driver", "D101", "2025-08-01", "2025-10-31", "DEIRA", "Pharma"),
    ("Driver", "D038", "2024-05-01", "2024-07-31", "AJMAN", "Consumer"), ("Driver", "D038", "2024-08-01", "2024-10-31", "QUSAIS", "Pharma"),
    ("Driver", "D038", "2024-11-01", "2025-01-31", "ALQ", "Consumer"), ("Driver", "D038", "2025-05-01", "2025-07-31", "JA", "Consumer"),
    ("Driver", "D038", "2025-08-01", "2025-10-31", "BUR", "Consumer"), ("Driver", "D048", "2024-08-01", "2024-10-31", "AJMAN", "Pharma"),
    ("Driver", "D048", "2024-11-01", "2025-01-31", "2ND TRIP", "Pharma"), ("Driver", "D048", "2025-05-01", "2025-07-31", "BURDUBAI", "Pharma"),
    ("Driver", "D048", "2025-08-01", "2025-10-31", "MIRDIFF", "Pharma"), ("Driver", "D019", "2024-05-01", "2024-07-31", "RAK / UAQ", "Pharma"),
    ("Driver", "D019", "2024-08-01", "2024-10-31", "MIRDIFF", "Pharma"), ("Driver", "D019", "2024-11-01", "2025-01-31", "DEIRA", "Pharma"),
    ("Driver", "D019", "2025-05-01", "2025-07-31", "ALQOUZ-1", "Pharma"), ("Driver", "D019", "2025-08-01", "2025-10-31", "FUJAIRAH", "Pharma"),
    ("Driver", "D064", "2024-05-01", "2024-07-31", "SHJ- BUH/ROLLA", "Pharma"), ("Driver", "D064", "2024-08-01", "2024-10-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Driver", "D064", "2024-11-01", "2025-01-31", "RAK / UAQ", "Pharma"), ("Driver", "D064", "2025-05-01", "2025-07-31", "RAK / UAQ", "Pharma"),
    ("Driver", "D064", "2025-08-01", "2025-10-31", "AJMAN", "Pharma"), ("Driver", "D029", "2024-05-01", "2024-07-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Driver", "D029", "2024-08-01", "2024-10-31", "SUBTITUTE/URGENT ORDERS", "Substitute"), ("Driver", "D029", "2024-11-01", "2025-01-31", "ALQOUZ-1", "Pharma"),
    ("Driver", "D029", "2025-05-01", "2025-07-31", "JUMAIRAH", "Pharma"), ("Driver", "D029", "2025-08-01", "2025-10-31", "RAK / UAQ", "Pharma"),
    ("Driver", "D011", "2024-05-01", "2024-07-31", "SHJ- BUH/ROLLA", "Pharma"), ("Driver", "D011", "2024-08-01", "2024-10-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Driver", "D011", "2025-05-01", "2025-07-31", "SHJ- BUH/ROLLA", "Pharma"), ("Driver", "D011", "2025-08-01", "2025-10-31", "SHJ- BUH/ROLLA", "Pharma"),
    ("Driver", "D050", "2024-05-01", "2024-07-31", "FUJAIRAH", "Pharma"), ("Driver", "D050", "2024-08-01", "2024-10-31", "ALQ", "Consumer"),
    ("Driver", "D050", "2024-11-01", "2025-01-31", "DXBO", "Consumer"), ("Driver", "D050", "2025-02-01", "2025-04-30", "RAK", "Consumer"),
    ("Driver", "D050", "2025-05-01", "2025-07-31", "DXBO", "Consumer"), ("Driver", "D050", "2025-08-01", "2025-10-31", "AJM", "Consumer"),
    ("Driver", "D094", "2024-05-01", "2024-07-31", "ALQOUZ-1", "Pharma"), ("Driver", "D094", "2024-08-01", "2024-10-31", "SUBTITUTE/URGENT ORDERS", "Substitute"),
    ("Driver", "D094", "2024-11-01", "2025-01-31", "MIRDIFF", "Pharma"), ("Driver", "D094", "2025-05-01", "2025-07-31", "DEIRA", "Pharma"),
    ("Driver", "D109", "2024-08-01", "2024-10-31", "JABEL ALI", "Pharma"), ("Driver", "D010", "2024-05-01", "2024-07-31", "QUSAIS", "Pharma"),
    ("Driver", "D010", "2024-11-01", "2025-01-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D010", "2025-05-01", "2025-07-31", "COLD CHAIN/URGENT ORDERS", "2-8"),
    ("Driver", "D098", "2024-05-01", "2024-07-31", "JUMAIRAH", "Pharma"), ("Driver", "D098", "2024-08-01", "2024-10-31", "RAK / UAQ", "Pharma"),
    ("Driver", "D098", "2024-11-01", "2025-01-31", "JABEL ALI", "Pharma"), ("Driver", "D098", "2025-05-01", "2025-07-31", "MIRDIFF", "Pharma"),
    ("Driver", "D098", "2025-08-01", "2025-10-31", "JABEL ALI", "Pharma"), ("Driver", "D049", "2024-05-01", "2024-07-31", "BURDUBAI", "Pharma"),
    ("Driver", "D049", "2024-08-01", "2024-10-31", "JABEL ALI", "Pharma"), ("Driver", "D049", "2024-11-01", "2025-01-31", "BURDUBAI", "Pharma"),
    ("Driver", "D049", "2025-05-01", "2025-07-31", "COLD CHAIN/URGENT ORDERS", "2-8"), ("Driver", "D049", "2025-08-01", "2025-10-31", "QUSAIS", "Pharma"),
    ("Driver", "D046", "2024-08-01", "2024-10-31", "BURDUBAI", "Pharma"), ("Driver", "D046", "2024-11-01", "2025-01-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D046", "2025-05-01", "2025-07-31", "AJMAN", "Pharma"), ("Driver", "D046", "2025-08-01", "2025-10-31", "JUMAIRAH", "Pharma"),
    ("Driver", "D040", "2024-05-01", "2024-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D040", "2024-08-01", "2024-10-31", "ALQOUZ-1", "Pharma"),
    ("Driver", "D040", "2024-11-01", "2025-01-31", "JUMAIRAH", "Pharma"), ("Driver", "D040", "2025-02-01", "2025-04-30", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D040", "2025-05-01", "2025-07-31", "SUBTITUTE/URGENT ORDERS", "Substitute"), ("Driver", "D040", "2025-08-01", "2025-10-31", "COLD CHAIN/URGENT ORDERS", "2-8"),
    ("Driver", "D037", "2024-05-01", "2024-07-31", "RAK", "Consumer"), ("Driver", "D037", "2024-08-01", "2024-10-31", "SUBTITUTE/URGENT ORDERS", "Substitute"),
    ("Driver", "D037", "2024-11-01", "2025-01-31", "AJM", "Consumer"), ("Driver", "D037", "2025-02-01", "2025-04-30", "SHJS", "Consumer"),
    ("Driver", "D037", "2025-05-01", "2025-07-31", "FUJAIRAH", "Pharma"), ("Driver", "D037", "2025-08-01", "2025-10-31", "AJM", "Consumer"),
    ("Driver", "D026", "2024-05-01", "2024-07-31", "FLEET SERVICE/RTA WORK", "Fleet"), ("Driver", "D026", "2024-08-01", "2024-10-31", "FLEET SERVICE/RTA WORK", "Fleet"),
    ("Driver", "D026", "2024-11-01", "2025-01-31", "FLEET SERVICE/RTA WORK", "Fleet"), ("Driver", "D026", "2025-05-01", "2025-07-31", "FLEET SERVICE/RTA WORK", "Fleet"),
    ("Driver", "D026", "2025-08-01", "2025-10-31", "FLEET SERVICE/RTA WORK", "Fleet"), ("Driver", "D024", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D024", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D024", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D024", "2025-05-01", "2025-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D024", "2025-08-01", "2025-10-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D047", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D047", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D047", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D047", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D047", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D061", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D061", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D061", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D061", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D061", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D044", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D044", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D044", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D044", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D044", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D052", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D052", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D052", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D052", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D052", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D089", "2024-05-01", "2024-07-31", "DXBO", "Consumer"), ("Driver", "D089", "2024-08-01", "2024-10-31", "RAK", "Consumer"),
    ("Driver", "D089", "2025-05-01", "2025-07-31", "ALQ", "Consumer"), ("Driver", "D089", "2025-08-01", "2025-10-31", "SHJS", "Consumer"),
    ("Driver", "D036", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D036", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D036", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D036", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D036", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D054", "2024-05-01", "2024-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D054", "2024-08-01", "2024-10-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D054", "2024-11-01", "2025-01-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D054", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"), ("Driver", "D054", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D088", "2024-05-01", "2024-07-31", "ALQ", "Consumer"), ("Driver", "D088", "2024-08-01", "2024-10-31", "SHJS", "Consumer"),
    ("Driver", "D088", "2024-11-01", "2025-01-31", "JA", "Consumer"), ("Driver", "D088", "2025-05-01", "2025-07-31", "BUR", "Consumer"),
    ("Driver", "D088", "2025-08-01", "2025-10-31", "ALQ", "Consumer"), ("Driver", "D023", "2024-05-01", "2024-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D023", "2024-08-01", "2024-10-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D023", "2025-05-01", "2025-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D023", "2025-08-01", "2025-10-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D104", "2024-11-01", "2025-01-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D104", "2025-02-01", "2025-04-30", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D104", "2025-05-01", "2025-07-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D104", "2025-08-01", "2025-10-31", "SUBTITUTE/URGENT ORDERS", "Substitute"), ("Driver", "D107", "2025-08-01", "2025-10-31", "SUBTITUTE/URGENT ORDERS", "Substitute"),
    ("Driver", "D027", "2024-05-01", "2024-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D027", "2024-08-01", "2024-10-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D027", "2025-05-01", "2025-07-31", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D027", "2025-08-01", "2025-10-31", "GOVT/URGENT ORDERS", "Govt / Urgent"),
    ("Driver", "D103", "2024-08-01", "2024-10-31", "DXBO", "Consumer"), ("Driver", "D103", "2024-11-01", "2025-01-31", "RAK", "Consumer"),
    ("Driver", "D103", "2025-02-01", "2025-04-30", "GOVT/URGENT ORDERS", "Govt / Urgent"), ("Driver", "D103", "2025-05-01", "2025-07-31", "SHJS", "Consumer"),
    ("Driver", "D103", "2025-08-01", "2025-10-31", "DXBO", "Consumer"), ("Driver", "D042", "2024-05-01", "2024-07-31", "COLD CHAIN/URGENT ORDERS", "2-8"),
    ("Driver", "D042", "2024-08-01", "2024-10-31", "COLD CHAIN/URGENT ORDERS", "2-8"), ("Driver", "D042", "2024-11-01", "2025-01-31", "COLD CHAIN/URGENT ORDERS", "2-8"),
    ("Driver", "D042", "2025-05-01", "2025-07-31", "SUBTITUTE/URGENT ORDERS", "Substitute"), ("Driver", "D042", "2025-08-01", "2025-10-31", "PICK UP", "Bulk / Pick-Up"),
    ("Driver", "D033", "2024-05-01", "2024-07-31", "AJMAN", "Pharma"), ("Driver", "D033", "2024-08-01", "2024-10-31", "JA", "Consumer"),
    ("Driver", "D033", "2024-11-01", "2025-01-31", "SHJS", "Consumer"), ("Driver", "D033", "2025-02-01", "2025-04-30", "DXBO", "Consumer"),
    ("Driver", "D033", "2025-05-01", "2025-07-31", "RAK", "Consumer"), ("Driver", "D033", "2025-08-01", "2025-10-31", "JA", "Consumer")
]


if "db_initialized" not in st.session_state:
    def execute_global_init(force=False):
        try:
            current_areas = load_table("areas")
            if force or len(current_areas) != 39:
                if FIREBASE_READY:
                    run_query(None, table_name="areas", action="CLEAR_TABLE")
                    for code, name, sector, nh, order in SEED_AREAS_IMAGE: 
                        db_fs.collection("areas").add({"code": code, "name": name, "sector": sector, "needs_helper": nh, "sort_order": order})
                else:
                    c = conn.cursor()
                    c.execute("DELETE FROM areas")
                    c.executemany("INSERT INTO areas (code, name, sector, needs_helper, sort_order) VALUES (?, ?, ?, ?, ?)", SEED_AREAS_IMAGE)
                    conn.commit()
            
            d_df = load_table('drivers')
            if len(d_df) == 0:
                if FIREBASE_READY:
                    for code in KEEP_DRIVERS: db_fs.collection("drivers").add({"name": RAW_NAME_MAP.get(code, "Unknown"), "code": code, "veh_type": "VAN", "sector": "None", "needs_helper": "None", "restriction": "None", "anchor_area": "None"})
                else:
                    c = conn.cursor()
                    d_seed = [(RAW_NAME_MAP.get(code, "Unknown"), code, "VAN", "None", "None", "None", "None") for code in KEEP_DRIVERS]
                    c.executemany("INSERT INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?, ?)", d_seed)
                    conn.commit()
            
            h_df = load_table('helpers')
            if len(h_df) == 0:
                if FIREBASE_READY:
                    for code in KEEP_HELPERS: db_fs.collection("helpers").add({"name": RAW_NAME_MAP.get(code, "Unknown"), "code": code, "restriction": "None", "health_card": "No", "anchor_area": "None"})
                else:
                    c = conn.cursor()
                    h_seed = [(RAW_NAME_MAP.get(code, "Unknown"), code, "None", "No", "None") for code in KEEP_HELPERS]
                    c.executemany("INSERT INTO helpers (name, code, restriction, health_card, anchor_area) VALUES (?, ?, ?, ?, ?)", h_seed)
                    conn.commit()

            v_df = load_table('vehicles')
            if len(v_df) == 0:
                if FIREBASE_READY:
                    for v_num, v_type in SEED_VEHICLES: db_fs.collection("vehicles").add({"number": v_num, "type": v_type, "anchor_area": "None"})
                else:
                    c = conn.cursor()
                    v_seed = [(v_num, v_type, "None") for v_num, v_type in SEED_VEHICLES]
                    c.executemany("INSERT INTO vehicles (number, type, anchor_area) VALUES (?, ?, ?)", v_seed)
                    conn.commit()
            
            st.cache_data.clear()
        except Exception:
            pass
            
    execute_global_init()
    st.session_state.db_initialized = True

def safe_parse_date(date_str):
    try: return datetime.strptime(str(date_str).split(" ")[0], "%Y-%m-%d").date()
    except: return date.today()

# --- HIGH PERFORMANCE SCORING HELPERS (WITH CROSS TRAINING SECTOR CACHE) ---
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

    # 1. Hard Constraints (Exclusions)
    if is_on_vacation(code, target_date, vac_cache):
        return None, "Excluded: On Vacation"
        
    if role == "Driver":
        p_veh = candidate.get('veh_type', 'None')
        if p_veh not in [req_veh, "None"] and not (p_veh == "VAN / PICK-UP" and req_veh in ["VAN", "PICK-UP"]):
            return None, f"Excluded: Vehicle Mismatch ({p_veh} != {req_veh})"

    # 2. MULTI-ANCHOR Logic (Strict Exclusion & Bonuses)
    anchors = [a.strip() for a in str(candidate.get('anchor_area', 'None')).split(',') if a.strip()]
    if "None" in anchors and len(anchors) == 1: anchors = []

    if anchors:
        # If the candidate has ANY anchors, the route MUST match at least one of them
        if any(a in [area['name'], req_sector, req_veh] for a in anchors):
            score += ANCHOR_MATCH_BONUS
            reasons.append(f"Anchor Match (+{ANCHOR_MATCH_BONUS})")
        else:
            return None, f"Excluded: Anchored strictly to {', '.join(anchors)}"

    # 3. Area Rotation Logic
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

    # 4. Sector Cross-Training Logic
    last_worked_sector = exp_cache.get(code, {}).get('sectors', {}).get(req_sector)
    if not last_worked_sector:
        score += NEVER_WORKED_SECTOR_BONUS
        reasons.append(f"Never worked {req_sector} Sector (+{NEVER_WORKED_SECTOR_BONUS})")
    else:
        months_since_sec = (target_date - last_worked_sector).days / 30.0
        time_score_sec = int(months_since_sec * SECTOR_MONTHS_WEIGHT)
        score += time_score_sec
        reasons.append(f"{months_since_sec:.1f}m since {req_sector} Sector (+{time_score_sec})")

    # 5. Vacation Predictor Logic
    vac_start = vacation_within_3_months(code, target_date, vac_cache)
    if vac_start:
        score += VACATION_SOON_PENALTY
        reasons.append(f"Vacation soon ({VACATION_SOON_PENALTY})")

    # 6. Role Specific Additions
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
        sec = area.get('sector', '')
        name = area.get('name', '')
        if "2-8" in sec or "COLD CHAIN" in name: req_veh["2-8 VAN"] += 1
        elif "Govt" in sec or "GOVT" in name: req_veh["BUS"] += 1
        elif "Pick-Up" in sec or "PICK UP" in name: req_veh["PICK-UP"] += 1
        else: req_veh["VAN"] += 1
            
    avail_veh = {"VAN": 0, "PICK-UP": 0, "BUS": 0, "2-8 VAN": 0}
    for _, v in vehicles_df.iterrows():
        vtype = v.get('type', 'VAN')
        if vtype in avail_veh: avail_veh[vtype] += 1
        elif vtype == "VAN / PICK-UP":
            avail_veh["VAN"] += 1
            avail_veh["PICK-UP"] += 1
        
    for vtype, required in req_veh.items():
        if avail_veh[vtype] < required:
            errors.append(f"🚗 Missing **{vtype}** Vehicles: Route needs **{required}**, but you only have **{avail_veh[vtype]}**.")

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
    solo_d_names = [f"[{r['code']}] {r['name']}" for _, r in all_d.iterrows() if (not is_on_vacation(r['code'], today, vac_cache)) and ((r.get('needs_helper', 'Yes') == 'No') or (r.get('veh_type', '') in ['BUS', '2-8 VAN']))] if not all_d.empty else []
    
    vac_h_names = [f"[{r['code']}] {r['name']}" for _, r in all_h.iterrows() if is_on_vacation(r['code'], today, vac_cache)] if not all_h.empty else []
    avail_h_names = [f"[{r['code']}] {r['name']}" for _, r in all_h.iterrows() if not is_on_vacation(r['code'], today, vac_cache)] if not all_h.empty else []
    
    req_helpers = len(avail_d_names) - len(solo_d_names)
    shortage = req_helpers - len(avail_h_names)

    col_a, col_b, col_c, col_d = st.columns(4)
    
    with col_a:
        st.metric("🚛 Total Drivers Available", f"{len(avail_d_names)} / {len(all_d)}")
        with st.popover("🔍 View Drivers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            if avail_d_names: st.markdown("**✅ Available:**<ol>" + "".join([f"<li>{n}</li>" for n in avail_d_names]) + "</ol>", unsafe_allow_html=True)
            if vac_d_names: st.markdown(f"**🌴 On Vacation ({len(vac_d_names)}):**<ol>" + "".join([f"<li>{n}</li>" for n in vac_d_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_b:
        st.metric("👤 Total Helpers Available", f"{len(avail_h_names)} / {len(all_h)}")
        with st.popover("🔍 View Helpers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            if avail_h_names: st.markdown("**✅ Available:**<ol>" + "".join([f"<li>{n}</li>" for n in avail_h_names]) + "</ol>", unsafe_allow_html=True)
            if vac_h_names: st.markdown(f"**🌴 On Vacation ({len(vac_h_names)}):**<ol>" + "".join([f"<li>{n}</li>" for n in vac_h_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_c:
        st.metric("⚡ Solo Drivers (No Helper)", len(solo_d_names))
        with st.popover("🔍 View Solo Drivers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            st.caption("Drivers active today who do not need a helper.")
            if solo_d_names: st.markdown("<ol>" + "".join([f"<li>{n}</li>" for n in solo_d_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_d:
        if shortage > 0: 
            st.metric("⚠️ Helper Shortage", f"-{shortage}", delta_color="inverse")
            with st.popover("🚨 View Shortage Details"):
                st.error(f"**Shortage of {shortage} Helpers!**")
                st.write(f"Active Drivers: **{len(avail_d_names)}**")
                st.write(f"Minus Solo Drivers: **{len(solo_d_names)}**")
                st.write(f"Helpers Needed: **{req_helpers}**")
                st.write(f"Helpers Available: **{len(avail_h_names)}**")
                st.info("AI will assign available helpers to priority routes. Remaining will be 'UNASSIGNED'.")
        else: 
            st.metric("✅ Helper Status", "Sufficient Surplus", delta_color="normal")

    st.divider()

    # --- ROUTE PLANS ---
    draft_routes = load_table('draft_routes')
    active_routes = load_table('active_routes')
    
    if not draft_routes.empty:
        st.warning("✨ **DRAFT MODE**: This plan is NOT saved to History yet! You can manually edit any cell below, then click Approve.")
        disp_draft = draft_routes.copy()
        
        # Mapping DB to Requested Visual Layout exactly
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
        
        col_down, col_app, col_can = st.columns([1, 1, 1])
        output = generate_excel_with_sn([edited_df], ['Draft Route Plan'])
        col_down.download_button("📥 Download Draft Excel", data=output, file_name=f"Draft_Plan_{today}.xlsx")
        
        if col_app.button("✅ Confirm Plan & Save Experiences", type="primary"):
            run_query("DELETE FROM active_routes", table_name="active_routes", action="CLEAR_TABLE") 
            p_s = today.strftime("%Y-%m-%d")
            p_e = (today + timedelta(days=30)).strftime("%Y-%m-%d")
            
            for index, r in edited_df.iterrows():
                q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                data_dict = {"order_num":r['S/N'], "area_code":"", "area_name":r.get('AREA', ''), "driver_code":r.get('Driver Code', ''), "driver_name":r.get('Drivers Name', ''), "helper_code":r.get('Helper Code', ''), "helper_name":r.get('Helpers Name', ''), "veh_num":r.get('VEH NO', ''), "start_date":p_s, "end_date":p_e}
                run_query(q_ar, (r['S/N'], "", r.get('AREA', ''), r.get('Driver Code', ''), r.get('Drivers Name', ''), r.get('Helper Code', ''), r.get('Helpers Name', ''), r.get('VEH NO', ''), p_s, p_e), table_name="active_routes", action="INSERT", data=data_dict)
                
                for code, name, ptype in [(r.get('Driver Code', ''), r.get('Drivers Name', ''), "Driver"), (r.get('Helper Code', ''), r.get('Helpers Name', ''), "Helper")]:
                    if code not in ["UNASSIGNED", "N/A", ""]:
                        run_query("INSERT INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)", (ptype, code, name, r.get('AREA', ''), r.get('Sector', ''), p_s, p_e), table_name="history", action="INSERT", data={"person_type":ptype, "person_code":code, "person_name":name, "area":r.get('AREA', ''), "sector":r.get('Sector', ''), "date":p_s, "end_date":p_e})
            
            run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
            st.success("Plan Approved! System will remember these experiences.")
            st.rerun()
            
        if col_can.button("❌ Cancel Draft", type="secondary"):
            run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
            st.rerun()

    elif not active_routes.empty:
        start_dt = active_routes.iloc[0].get('start_date', 'Unknown')
        st.subheader(f"📋 Current Active Route Plan (Started: {start_dt})")
        
        # Merge Sector info to display seamlessly on active routes
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

                for _, area in areas.iterrows():
                    area_name = area['name']
                    req_sector = area.get('sector', 'Pharma')
                    needs_helper = area.get('needs_helper', 'Yes') in ['Yes', 'Optional']
                    
                    div_cat = "PHARMA DIVISION"
                    if "2-8" in req_sector or "Govt" in req_sector or "Fleet" in req_sector: div_cat = "2-8 / URGENT ORDERS"
                    elif "Pick-Up" in req_sector or "Substitute" in req_sector or "Bulk" in req_sector: div_cat = "PICK-UPS / URGENT COLD CHAIN"
                    elif "Consumer" in req_sector: div_cat = "CONSUMER DIVISION"

                    # Vehicle Requirement logic
                    req_veh = "VAN"
                    if "2-8" in req_sector or "COLD CHAIN" in area_name: req_veh = "2-8 VAN"
                    elif "Govt" in req_sector or "GOVT" in area_name: req_veh = "BUS"
                    elif "Pick-Up" in req_sector or "PICK UP" in area_name: req_veh = "PICK-UP"

                    prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                    a_d_code, a_d_name, a_h_code, a_h_name, a_v_num = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED"

                    # 1. ASSIGN DRIVER (Only generates new ones if "Drivers" is selected, otherwise keeps old one)
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
                            run_query("INSERT INTO route_plan_reasons (plan_date, area, role, selected_person, score, reasons, generated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                      (month_target.strftime("%Y-%m-%d"), area_name, "Driver", a_d_name, best_d_score, d_reason, timestamp), table_name="route_plan_reasons", action="INSERT", data={"plan_date":month_target.strftime("%Y-%m-%d"), "area":area_name, "role":"Driver", "selected_person":a_d_name, "score":best_d_score, "reasons":d_reason, "generated_at":timestamp})
                            
                            # Future Replacement Check
                            vac_start = vacation_within_3_months(a_d_code, month_target, vac_cache)
                            if vac_start:
                                repl_d, best_r_score, _ = None, -999999, ""
                                for _, rp in all_d[~all_d['code'].isin([a_d_code])].iterrows():
                                    r_score, _ = calculate_candidate_score(rp, area, req_veh, req_sector, vac_start, exp_cache, vac_cache, role="Driver")
                                    if r_score is not None and r_score > best_r_score:
                                        best_r_score, repl_d = r_score, rp
                                repl_name = repl_d['name'] if repl_d is not None else "CRITICAL SHORTAGE"
                                run_query("INSERT INTO vacation_predictions (person_code, person_name, role, suggested_start, reason, replacement_person, replacement_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                          (a_d_code, a_d_name, "Driver", vac_start.strftime("%Y-%m-%d"), "Scheduled Vacation", repl_name, vac_start.strftime("%Y-%m-%d")), table_name="vacation_predictions", action="INSERT", data={"person_code":a_d_code, "person_name":a_d_name, "role":"Driver", "suggested_start":vac_start.strftime("%Y-%m-%d"), "reason":"Scheduled Vacation", "replacement_person":repl_name, "replacement_date":vac_start.strftime("%Y-%m-%d")})
                    else:
                        a_d_code, a_d_name = prev_assignment.iloc[0]['driver_code'], prev_assignment.iloc[0]['driver_name']
                        used_drivers.add(a_d_code)

                    # 2. ASSIGN HELPER (Only generates new ones if "Helpers" is selected, otherwise keeps old one)
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
                            run_query("INSERT INTO route_plan_reasons (plan_date, area, role, selected_person, score, reasons, generated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                      (month_target.strftime("%Y-%m-%d"), area_name, "Helper", a_h_name, best_h_score, h_reason, timestamp), table_name="route_plan_reasons", action="INSERT", data={"plan_date":month_target.strftime("%Y-%m-%d"), "area":area_name, "role":"Helper", "selected_person":a_h_name, "score":best_h_score, "reasons":h_reason, "generated_at":timestamp})
                            
                            # Future Replacement Check
                            vac_start = vacation_within_3_months(a_h_code, month_target, vac_cache)
                            if vac_start:
                                repl_h, best_r_score, _ = None, -999999, ""
                                for _, rp in all_h[~all_h['code'].isin([a_h_code])].iterrows():
                                    r_score, _ = calculate_candidate_score(rp, area, req_veh, req_sector, vac_start, exp_cache, vac_cache, role="Helper")
                                    if r_score is not None and r_score > best_r_score:
                                        best_r_score, repl_h = r_score, rp
                                repl_name = repl_h['name'] if repl_h is not None else "CRITICAL SHORTAGE"
                                run_query("INSERT INTO vacation_predictions (person_code, person_name, role, suggested_start, reason, replacement_person, replacement_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                          (a_h_code, a_h_name, "Helper", vac_start.strftime("%Y-%m-%d"), "Scheduled Vacation", repl_name, vac_start.strftime("%Y-%m-%d")), table_name="vacation_predictions", action="INSERT", data={"person_code":a_h_code, "person_name":a_h_name, "role":"Helper", "suggested_start":vac_start.strftime("%Y-%m-%d"), "reason":"Scheduled Vacation", "replacement_person":repl_name, "replacement_date":vac_start.strftime("%Y-%m-%d")})
                    else:
                        a_h_code, a_h_name = prev_assignment.iloc[0]['helper_code'], prev_assignment.iloc[0]['helper_name']
                        used_helpers.add(a_h_code)

                    # 3. ASSIGN VEHICLE (Multi-Anchor Logic Included)
                    if a_d_code != "UNASSIGNED" and a_v_num == "UNASSIGNED":
                        d_type = all_d[all_d['code'] == a_d_code]['veh_type'].values[0] if not all_d[all_d['code'] == a_d_code].empty else "VAN"
                        tvt = req_veh if req_veh != "VAN" else d_type
                        
                        potential_vs = []
                        for _, v in vehicles[~vehicles['number'].isin(used_vehicles)].iterrows():
                            v_type = v.get('type', 'VAN')
                            type_match = False
                            if v_type == tvt: type_match = True
                            elif tvt in ["VAN", "PICK-UP"] and v_type == "VAN / PICK-UP": type_match = True
                            
                            if not type_match: continue
                            
                            v_anchors = [a.strip() for a in str(v.get('anchor_area', 'None')).split(',') if a.strip()]
                            if "None" in v_anchors and len(v_anchors) == 1: v_anchors = []
                            
                            if v_anchors:
                                if any(a in [area_name, req_sector, tvt] for a in v_anchors):
                                    potential_vs.append((v, True)) # Matched Anchor
                            else:
                                potential_vs.append((v, False)) # Unanchored Vehicle

                        # Sort: Anchored matches go first, then normal unanchored ones
                        potential_vs.sort(key=lambda x: x[1], reverse=True)

                        if potential_vs:
                            a_v_num = potential_vs[0][0]['number']
                            used_vehicles.add(a_v_num)

                    route_plan.append({
                        "Driver Code": a_d_code, "Drivers Name": a_d_name, 
                        "AREA": area_name, "Sector": req_sector, "Helper Code": a_h_code, "Helpers Name": a_h_name, 
                        "VEH NO": a_v_num, "Division Category": div_cat, "Area Code": area['code']
                    })

                run_query("DELETE FROM draft_routes", table_name="draft_routes", action="CLEAR_TABLE")
                for index, r in enumerate(route_plan):
                    q_dr = "INSERT INTO draft_routes (order_num, area_code, area_name, sector, driver_code, driver_name, helper_code, helper_name, veh_num, div_cat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    run_query(q_dr, (index+1, r['Area Code'], r['AREA'], r['Sector'], r['Driver Code'], r['Drivers Name'], r['Helper Code'], r['Helpers Name'], r['VEH NO'], r['Division Category']), table_name="draft_routes", action="INSERT", data={"order_num":index+1, "area_code":r['Area Code'], "area_name":r['AREA'], "sector":r['Sector'], "driver_code":r['Driver Code'], "driver_name":r['Drivers Name'], "helper_code":r['Helper Code'], "helper_name":r['Helpers Name'], "veh_num":r['VEH NO'], "div_cat":r['Division Category']})
                
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
                
                # Check for replacements
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
            for idx in disp_df.index:
                row_id = disp_df.loc[idx, 'id']
                if not disp_df.loc[idx].equals(edited_d.loc[idx]):
                    update_dict = edited_d.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    run_query(f"UPDATE drivers SET {sql_sets} WHERE id=?", tuple(list(update_dict.values()) + [row_id]), table_name="drivers", action="UPDATE", doc_id=row_id, data=update_dict)
            st.rerun()
            
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Driver")
            st.caption("Don't see your custom Sector or Vehicle Type in the dropdown? Add a new driver here and the dropdown will automatically learn it.")
            d_name = st.text_input("New Driver Name", key="add_d_name")
            d_code = st.text_input("New Driver Code", key="add_d_code")
            col_t, col_s, col_h = st.columns(3)
            d_type = col_t.selectbox("New Driver Veh Type", VEHICLE_OPTIONS, key="add_d_type")
            d_sec = col_s.selectbox("New Driver Sector", SECTOR_OPTIONS, key="add_d_sec")
            d_needs_h = col_h.selectbox("New Driver Needs Helper?", NEEDS_HELPER_OPTIONS, index=2, key="add_d_nh")
            
            d_anchor_opts = st.multiselect("New Driver Anchor(s)", multi_anchor_opts, key="add_d_anchor")
            d_anchor_str = ", ".join(d_anchor_opts) if d_anchor_opts else "None"
            
            if st.button("➕ Add Driver", use_container_width=True):
                if run_query("INSERT INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                          (d_name, d_code, d_type, d_sec, d_needs_h, "None", d_anchor_str), table_name="drivers", action="INSERT", data={"name":d_name, "code":d_code, "veh_type":d_type, "sector":d_sec, "needs_helper":d_needs_h, "restriction":"None", "anchor_area":d_anchor_str}):
                    st.success("Driver Added!")
                    st.rerun()

        with c_edit:
            st.subheader("🗑️ Delete Driver")
            st.caption("Manually Remove a Driver from the Database")
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
            for idx in disp_h.index:
                row_id = disp_h.loc[idx, 'id']
                if not disp_h.loc[idx].equals(edited_h.loc[idx]):
                    update_dict = edited_h.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    run_query(f"UPDATE helpers SET {sql_sets} WHERE id=?", tuple(list(update_dict.values()) + [row_id]), table_name="helpers", action="UPDATE", doc_id=row_id, data=update_dict)
            st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Helper")
            h_name = st.text_input("New Helper Name", key="add_h_name")
            h_code = st.text_input("New Helper Code", key="add_h_code")
            h_health = st.selectbox("New Helper Health Card?", ["No", "Yes"], key="add_h_hc")
            
            h_anchor_opts = st.multiselect("New Helper Anchor(s)", multi_anchor_opts, key="add_h_anc")
            h_anchor_str = ", ".join(h_anchor_opts) if h_anchor_opts else "None"
            
            if st.button("➕ Add Helper", use_container_width=True):
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
            for idx in disp_a.index:
                row_id = disp_a.loc[idx, 'id']
                if not disp_a.loc[idx].equals(edited_a.loc[idx]):
                    update_dict = edited_a.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    run_query(f"UPDATE areas SET {sql_sets} WHERE id=?", tuple(list(update_dict.values()) + [row_id]), table_name="areas", action="UPDATE", doc_id=row_id, data=update_dict)
            st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Area")
            a_name = st.text_input("New Area Name", key="add_a_name")
            a_code = st.text_input("New Area Code", key="add_a_code")
            col_s, col_n = st.columns(2)
            a_sec = col_s.selectbox("New Area Sector", SECTOR_OPTIONS, key="add_a_sec")
            a_needs = col_n.selectbox("New Area Needs Helper?", NEEDS_HELPER_OPTIONS, key="add_a_nh")
            if st.button("➕ Add Area", use_container_width=True):
                new_order = len(a_df) + 1
                if run_query("INSERT INTO areas (name, code, sector, needs_helper, sort_order) VALUES (?, ?, ?, ?, ?)", (a_name, a_code, a_sec, a_needs, new_order), table_name="areas", action="INSERT", data={"name":a_name, "code":a_code, "sector":a_sec, "needs_helper":a_needs, "sort_order":new_order}):
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
        
        edited_v = st.data_editor(
            disp_v, column_config={
                "id": None, "S/N": st.column_config.NumberColumn(disabled=True),
                "type": st.column_config.SelectboxColumn("Type", options=v_type_opts),
                "anchor_area": st.column_config.TextColumn("Anchor(s) (comma-separated)", help="Type Areas, Sectors, or Veh Types separated by commas")
            }, use_container_width=True, height=250, hide_index=True, key="ed_vehicles"
        )
        if st.button("💾 Save Table Edits", key="save_table_vehicles"):
            for idx in disp_v.index:
                row_id = disp_v.loc[idx, 'id']
                if not disp_v.loc[idx].equals(edited_v.loc[idx]):
                    update_dict = edited_v.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                    sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                    run_query(f"UPDATE vehicles SET {sql_sets} WHERE id=?", tuple(list(update_dict.values()) + [row_id]), table_name="vehicles", action="UPDATE", doc_id=row_id, data=update_dict)
            st.rerun()

        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Vehicle")
            v_num = st.text_input("New Vehicle Number", key="add_v_num")
            v_type = st.selectbox("New Vehicle Type", VEHICLE_OPTIONS, key="add_v_type")
            
            v_anchor_opts = st.multiselect("New Vehicle Anchor(s)", multi_anchor_opts, key="add_v_anc")
            v_anchor_str = ", ".join(v_anchor_opts) if v_anchor_opts else "None"
            
            if st.button("➕ Add Vehicle", use_container_width=True):
                if run_query("INSERT INTO vehicles (number, type, anchor_area) VALUES (?, ?, ?)", (v_num, v_type, v_anchor_str), table_name="vehicles", action="INSERT", data={"number":v_num, "type":v_type, "anchor_area":v_anchor_str}):
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
                if not FIREBASE_READY: run_query(f"DELETE FROM {sheet}")
                else: run_query(None, table_name=sheet, action="CLEAR_TABLE")

                for _, row in df.iterrows():
                    data_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v) and k not in ['id', 'S/N']}
                    if not FIREBASE_READY:
                        cols, vals = ', '.join(data_dict.keys()), tuple(data_dict.values())
                        qmarks = ', '.join(['?'] * len(data_dict))
                        run_query(f"INSERT INTO {sheet} ({cols}) VALUES ({qmarks})", vals)
                    else:
                        run_query(None, table_name=sheet, action="INSERT", data=data_dict)
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
        for idx in disp_hist.index:
            row_id = disp_hist.loc[idx, 'id']
            if not disp_hist.loc[idx].equals(edited_hist.loc[idx]):
                update_dict = edited_hist.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                run_query(f"UPDATE history SET {sql_sets} WHERE id=?", tuple(list(update_dict.values()) + [row_id]), table_name="history", action="UPDATE", doc_id=row_id, data=update_dict)
        st.rerun()

    with st.expander("🚨 Emergency Data Restore"):
        st.warning("If your Past Experience data is empty or incorrect, click below to wipe current records and load the exact PDF data.")
        if st.button("♻️ Restore PDF Past Experience Data", type="primary"):
            with st.spinner("Wiping old history and loading PDF data..."):
                if FIREBASE_READY:
                    run_query(None, table_name="history", action="CLEAR_TABLE")
                    for ptype, pcode, pstart, pend, parea, psec in PRELOAD_HISTORY:
                        db_fs.collection("history").add({"person_type": ptype, "person_code": pcode, "person_name": RAW_NAME_MAP.get(pcode, "Unknown"), "area": parea, "sector": psec, "date": pstart, "end_date": pend})
                else:
                    c = conn.cursor()
                    c.execute("DELETE FROM history")
                    h_seed_full = [(ptype, pcode, RAW_NAME_MAP.get(pcode, "Unknown"), parea, psec, pstart, pend) for ptype, pcode, pstart, pend, parea, psec in PRELOAD_HISTORY]
                    c.executemany("INSERT INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)", h_seed_full)
                    conn.commit()
                st.cache_data.clear()
            st.success("Past Experience data fully restored to PDF specifications!")
            st.rerun()

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
                    if run_query("INSERT INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (p_type, p_code, p_name, p_area, p_sec, p_start_date.strftime("%Y-%m-%d"), p_end_date.strftime("%Y-%m-%d")), 
                              table_name="history", action="INSERT", data={"person_type":p_type, "person_code":p_code, "person_name":p_name, "area":p_area, "sector":p_sec, "date":p_start_date.strftime("%Y-%m-%d"), "end_date":p_end_date.strftime("%Y-%m-%d")}):
                        st.success("Experience Added!")
                        st.rerun()

    with c_edit:
        st.subheader("🗑️ Remove Experience manually")
        if not history_df.empty:
            hist_options = []
            hist_map = {}
            for idx, row in history_df.iterrows():
                sec = row.get('sector', 'Pharma')
                if pd.isna(sec) or sec == "nan": sec = "Pharma"
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
            if sd <= today <= ed:
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
        for idx in disp_vac.index:
            row_id = disp_vac.loc[idx, 'id']
            if not disp_vac.loc[idx].equals(edited_vac.loc[idx]):
                update_dict = edited_vac.loc[idx].drop(labels=['id', 'S/N'], errors='ignore').to_dict()
                sql_sets = ", ".join([f"{k}=?" for k in update_dict.keys()])
                run_query(f"UPDATE vacations SET {sql_sets} WHERE id=?", tuple(list(update_dict.values()) + [row_id]), table_name="vacations", action="UPDATE", doc_id=row_id, data=update_dict)
        st.rerun()

    with st.expander("📥 Export / 📤 Import Vacation Data"):
        output = generate_excel_with_sn([vacs_df], ['vacations'])
        st.download_button("📥 Download Vacation Data", data=output, file_name="Vacation_Data.xlsx")
        
        up_vac = st.file_uploader("Upload Vacation Excel", type=['xlsx'], key="up_vac")
        if up_vac and st.button("Sync Vacation Database"):
            df = pd.read_excel(up_vac)
            if not FIREBASE_READY: run_query("DELETE FROM vacations")
            else: run_query(None, table_name="vacations", action="CLEAR_TABLE")
            
            for _, row in df.iterrows():
                data_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v) and k not in ['id', 'S/N']}
                if not FIREBASE_READY:
                    cols, vals = ', '.join(data_dict.keys()), tuple(data_dict.values())
                    qmarks = ', '.join(['?'] * len(data_dict))
                    run_query(f"INSERT INTO vacations ({cols}) VALUES ({qmarks})", vals)
                else:
                    run_query(None, table_name="vacations", action="INSERT", data=data_dict)
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
                
                if not vacs_df.empty and 'person_name' in vacs_df.columns:
                    overlap = vacs_df[(vacs_df['person_name'] == v_name) & (vacs_df['start_date'] == v_start.strftime("%Y-%m-%d"))]
                else:
                    overlap = pd.DataFrame()
                    
                if v_start > v_end: 
                    st.error("Start Date cannot be after End Date.")
                elif not overlap.empty:
                    st.error(f"⚠️ {v_name} already has a vacation logged starting exactly on {v_start.strftime('%Y-%m-%d')}!")
                else:
                    if run_query("INSERT INTO vacations (person_type, person_code, person_name, start_date, end_date) VALUES (?, ?, ?, ?, ?)", (v_type, v_code, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")), table_name="vacations", action="INSERT", data={"person_type":v_type, "person_code":v_code, "person_name":v_name, "start_date":v_start.strftime("%Y-%m-%d"), "end_date":v_end.strftime("%Y-%m-%d")}):
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
