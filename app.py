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
        except Exception:
            FIREBASE_READY = False

except Exception as e:
    FIREBASE_READY = False

if FIREBASE_READY:
    st.sidebar.markdown("<div style='text-align: right; font-size: 20px; margin-top: -15px;' title='Connected to Secure Cloud'>🟢</div>", unsafe_allow_html=True)
else:
    st.sidebar.markdown("<div style='text-align: right; font-size: 20px; margin-top: -15px;' title='Local Database Mode'>🔴</div>", unsafe_allow_html=True)

# SQLite Fallback Initialization
def init_sqlite_db():
    local_conn = sqlite3.connect('logistics.db', check_same_thread=False)
    c = local_conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY, name TEXT, code TEXT, veh_type TEXT, sector TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (id INTEGER PRIMARY KEY, name TEXT, code TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY, name TEXT, code TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY, number TEXT, type TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, area TEXT, date TEXT, end_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (id INTEGER PRIMARY KEY, person_type TEXT, person_name TEXT, start_date DATE, end_date DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS performance (id INTEGER PRIMARY KEY, person_code TEXT, area TEXT, success_rate REAL, delay_count INTEGER)''')
    
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
        "ALTER TABLE vehicles ADD COLUMN anchor_area TEXT DEFAULT 'None'"
    ]:
        try: c.execute(query)
        except sqlite3.OperationalError: pass
    local_conn.commit()
    return local_conn

if not FIREBASE_READY:
    conn = init_sqlite_db()


# --- SAFE DB QUERY HANDLER ---
def clear_cache():
    st.cache_data.clear()

@st.cache_data
def load_table(table_name):
    if FIREBASE_READY:
        docs = db_fs.collection(table_name).stream()
        data = [{**doc.to_dict(), 'id': doc.id} for doc in docs]
        df = pd.DataFrame(data)
        if table_name == 'helpers' and 'health_card' not in df.columns and not df.empty: df['health_card'] = 'No'
        if table_name == 'drivers' and 'needs_helper' not in df.columns and not df.empty: df['needs_helper'] = 'Yes'
        if table_name == 'areas' and 'sector' not in df.columns and not df.empty: df['sector'] = 'Pharma'
        if table_name == 'areas' and 'needs_helper' not in df.columns and not df.empty: df['needs_helper'] = 'Yes'
        if table_name == 'areas' and 'sort_order' not in df.columns and not df.empty: df['sort_order'] = 99
        if table_name == 'history' and 'sector' not in df.columns and not df.empty: df['sector'] = 'Pharma'
        if table_name == 'vehicles' and 'anchor_area' not in df.columns and not df.empty: df['anchor_area'] = 'None'
        
        # Enforce strict sort order for areas
        if table_name == 'areas' and not df.empty:
            df = df.sort_values(by='sort_order')
            
        # Clean potential NaN values from history
        if table_name == 'history' and not df.empty:
            df['sector'] = df['sector'].fillna('Pharma')
            
        return df
    else:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        if table_name == 'areas' and not df.empty: df = df.sort_values(by='sort_order')
        if table_name == 'history' and not df.empty: df['sector'] = df['sector'].fillna('Pharma')
        return df

def run_query(query, params=(), table_name=None, action=None, doc_id=None, data=None):
    clear_cache()
    if FIREBASE_READY and table_name and action:
        if action == "INSERT": db_fs.collection(table_name).add(data)
        elif action == "UPDATE" and doc_id: db_fs.collection(table_name).document(str(doc_id)).update(data)
        elif action == "DELETE_DOC" and doc_id: db_fs.collection(table_name).document(str(doc_id)).delete()
        elif action == "CLEAR_TABLE":
            docs = db_fs.collection(table_name).stream()
            for doc in docs: doc.reference.delete()
    else:
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()

def generate_excel_with_sn(df_list, sheet_names):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for df, sheet in zip(df_list, sheet_names):
            export_df = df.copy()
            if 'id' in export_df.columns: export_df = export_df.drop(columns=['id'])
            if 'S/N' in export_df.columns: export_df = export_df.drop(columns=['S/N'])
            export_df.insert(0, 'S/N', range(1, 1 + len(export_df)))
            export_df.to_excel(writer, sheet_name=sheet, index=False)
    output.seek(0)
    return output


# --- OPTIONS ---
VEHICLE_OPTIONS = ["None", "VAN", "PICK-UP", "BUS", "2-8 VAN"]
SECTOR_OPTIONS = ["None", "Pharma", "Consumer", "Bulk / Pick-Up", "2-8", "Govt / Urgent", "Substitute", "Fleet", "Bus"]


# --- STRICT IMAGE-BASED ROUTE LAYOUT SEED ---
SEED_AREAS_IMAGE = [
    ("PH-FUJ", "FUJAIRAH", "Pharma", "Yes", 1),
    ("PH-RAK", "RAK", "Pharma", "Yes", 2),
    ("PH-ALQ1", "ALQOUZ-1", "Pharma", "Yes", 3),
    ("PH-ALQ2", "ALQOUZ-2", "Pharma", "Yes", 4),
    ("PH-JUM", "JUMAIRAH", "Pharma", "Yes", 5),
    ("PH-BUR", "BURDUBAI", "Pharma", "Yes", 6),
    ("PH-MIR", "MIRDIFF", "Pharma", "Yes", 7),
    ("PH-QUS", "QUSAIS", "Pharma", "Yes", 8),
    ("PH-DEI", "DEIRA", "Pharma", "Yes", 9),
    ("PH-AJM", "AJMAN", "Pharma", "Yes", 10),
    ("PH-BUH", "BUHAIRAH", "Pharma", "Yes", 11),
    ("PH-SHJS", "SHJ - SANAYYA", "Pharma", "Yes", 12),
    ("PH-JAB", "JABEL ALI", "Pharma", "Yes", 13),
    ("28-CC1", "COLD CHAIN/URGENT 1", "2-8", "No", 14),
    ("28-CC2", "COLD CHAIN/URGENT 2", "2-8", "No", 15),
    ("PH-2ND1", "2ND TRIP 1", "Pharma", "Yes", 16),
    ("PH-2ND2", "2ND TRIP 2", "Pharma", "Yes", 17),
    ("PH-SAMP", "SAMPLE DRIVER", "Pharma", "Yes", 18),
    ("GOV-1", "GOVT/URGENT 1", "Govt", "No", 19),
    ("GOV-2", "GOVT/URGENT 2", "Govt", "No", 20),
    ("GOV-3", "GOVT/URGENT 3", "Govt", "No", 21),
    ("FLE-1", "FLEET SERVICE", "Fleet", "No", 22),
    ("PU-1", "PHARMA PICK UP 1", "Pick-Up", "Yes", 23),
    ("PU-2", "PHARMA PICK UP 2", "Pick-Up", "Yes", 24),
    ("PU-3", "PHARMA PICK UP 3", "Pick-Up", "Yes", 25),
    ("PU-4", "PHARMA PICK UP 4", "Pick-Up", "Yes", 26),
    ("PU-5", "PHARMA PICK UP 5", "Pick-Up", "Yes", 27),
    ("PU-6", "PHARMA PICK UP 6", "Pick-Up", "Yes", 28),
    ("PU-SUB", "PHARMA PICK UP SUB", "Pick-Up", "No", 29),
    ("CON-ALQ", "ALQOUZ (C)", "Consumer", "Yes", 30),
    ("CON-JAB", "JABAL ALI (C)", "Consumer", "Yes", 31),
    ("CON-MIR", "MIRDIFF (C)", "Consumer", "Yes", 32),
    ("CON-BUR", "BURDUBAI (C)", "Consumer", "Yes", 33),
    ("CON-RAK", "RAK (C)", "Consumer", "Yes", 34),
    ("CON-AJM", "AJMAN (C)", "Consumer", "Yes", 35),
    ("CON-SHJS", "SHJ & SANAIYA (C)", "Consumer", "Yes", 36),
    ("CON-PU1", "CONSUMER PICK UP 1", "Consumer", "Yes", 37),
    ("CON-PU2", "CONSUMER PICK UP 2", "Consumer", "Yes", 38),
    ("CON-SUB", "CONSUMER SUB", "Consumer", "No", 39)
]

def auto_seed_database(force=False):
    seeded = False
    if FIREBASE_READY:
        if force or len(list(db_fs.collection("areas").limit(1).stream())) == 0:
            if force: run_query(None, table_name="areas", action="CLEAR_TABLE")
            for code, name, sector, nh, order in SEED_AREAS_IMAGE: 
                db_fs.collection("areas").add({"code": code, "name": name, "sector": sector, "needs_helper": nh, "sort_order": order})
            seeded = True
    else:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM areas")
        if force or c.fetchone()[0] == 0:
            if force: c.execute("DELETE FROM areas")
            c.executemany("INSERT INTO areas (code, name, sector, needs_helper, sort_order) VALUES (?, ?, ?, ?, ?)", SEED_AREAS_IMAGE)
            conn.commit()
            seeded = True
    if seeded: clear_cache()
auto_seed_database()


# --- SMART SCORING LOGIC ---
def safe_parse_date(date_str):
    try: return datetime.strptime(date_str, "%Y-%m-%d").date()
    except: return date.today()

@st.cache_data
def get_experience_months(history_df, person_code, area_name, sector_name):
    if history_df.empty or 'person_code' not in history_df.columns: return 0
    records = history_df[(history_df['person_code'] == person_code) & (history_df['area'] == area_name) & (history_df['sector'] == sector_name)]
    total_days = sum(max(0, ((safe_parse_date(r['end_date']) if pd.notna(r.get('end_date')) and r['end_date'] != "None" else safe_parse_date(r['date']) + timedelta(days=30)) - safe_parse_date(r['date'])).days) for _, r in records.iterrows())
    return total_days / 30.0

def get_last_assignment(history_df, person_code, area_name):
    if history_df.empty: return None
    records = history_df[(history_df['person_code'] == person_code) & (history_df['area'] == area_name)]
    if records.empty: return None
    return safe_parse_date(records.iloc[-1]['date'])

@st.cache_data
def is_on_vacation(vacations_df, person_name, target_date):
    if vacations_df.empty or 'person_name' not in vacations_df.columns: return False
    for _, row in vacations_df.iterrows():
        if row['person_name'] == person_name:
            if safe_parse_date(row['start_date']) <= target_date <= safe_parse_date(row['end_date']):
                return True
    return False

def select_best_candidate(candidates_df, area_name, req_sector, target_date, history_df, vacations_df, role="Driver"):
    best_candidate, best_score, best_reason = None, -99999, "No valid candidates left"
    
    # Strictly define required vehicles based on Sector / Area Name
    req_veh = "VAN"
    if "2-8" in req_sector or "2-8" in area_name: req_veh = "2-8 VAN"
    elif "Govt" in req_sector or "GOVT" in area_name: req_veh = "BUS"
    elif "Pick-Up" in req_sector or "PICK UP" in area_name: req_veh = "PICK-UP"

    for _, person in candidates_df.iterrows():
        if is_on_vacation(vacations_df, person['name'], target_date): continue
        score = 0
        reasons = []

        if role == "Driver":
            p_veh = person.get('veh_type', 'None')
            p_sec = person.get('sector', 'None')
            
            if p_veh in [req_veh, "None"]:
                score += 300
                reasons.append(f"Veh Match ({p_veh}) (+300)")
            else:
                score -= 600
                reasons.append(f"Wrong Veh (-600)")

            if p_sec in [req_sector, "None"]:
                score += 200
                reasons.append(f"Sector Match ({p_sec}) (+200)")
            else:
                score -= 400
                reasons.append(f"Wrong Sector (-400)")

        if role == "Helper":
            # If area is Consumer, heavily favor Health Card
            if "Consumer" in req_sector:
                if person.get('health_card') == 'Yes':
                    score += 1500
                    reasons.append("Health Card for Consumer (+1500)")
                else:
                    score -= 1500
                    reasons.append("No Health Card for Consumer (-1500)")
            # If area is Pharma, penalize wasting a Health Card
            elif person.get('health_card') == 'Yes':
                score -= 1000
                reasons.append("Waste of Health Card in Pharma (-1000)")

        # Anchors
        anchor = person.get('anchor_area')
        if anchor == area_name:
            score += 2000
            reasons.append("Anchored Area (+2000)")
        elif anchor not in ["None", "", None] and type(anchor) == str:
            score -= 1000
            reasons.append(f"Anchored elsewhere (-1000)")

        # CORE RULE: LEARN NEW AREAS
        exp_months = get_experience_months(history_df, person['code'], area_name, req_sector)
        if exp_months == 0:
            score += 5000
            reasons.append("Learning Priority: 0 Exp (+5000)")
        else:
            score += int(exp_months * 5)
            reasons.append(f"Has {exp_months:.1f}m Exp (+{int(exp_months*5)})")
            
        # CORE RULE: STRICT 6-MONTH PENALTY
        last_date = get_last_assignment(history_df, person['code'], area_name)
        if last_date:
            months_since = (target_date - last_date).days / 30.0
            if months_since < 6:
                penalty = 10000 + int((6 - months_since) * 100)
                score -= penalty
                reasons.append(f"Recent Visit <6m (-{penalty})")

        if score > best_score:
            best_score = score
            best_candidate = person
            best_reason = f"Score [{score}]: " + ", ".join(reasons)

    return best_candidate, best_reason


# --- APP ROUTING ---
menu = ["1. AI Route Planner", "2. Database Management", "3. Past Experience Builder", "4. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)


# ==========================================
# SCREEN 1: AI ROUTE PLANNER
# ==========================================
if choice == "1. AI Route Planner":
    
    # --- SCROLLABLE DASHBOARD ---
    st.subheader("📊 Today's Availability Dashboard")
    today = date.today()
    all_d = load_table('drivers')
    all_h = load_table('helpers')
    vacs = load_table('vacations')
    
    vac_d_names = [r['name'] for _, r in all_d.iterrows() if is_on_vacation(vacs, r['name'], today)] if not all_d.empty else []
    avail_d_names = [r['name'] for _, r in all_d.iterrows() if not is_on_vacation(vacs, r['name'], today)] if not all_d.empty else []
    solo_d_names = [r['name'] for _, r in all_d.iterrows() if (not is_on_vacation(vacs, r['name'], today)) and ((r.get('needs_helper', 'Yes') == 'No') or (r.get('veh_type', '') in ['BUS', '2-8 VAN']))] if not all_d.empty else []
    
    vac_h_names = [r['name'] for _, r in all_h.iterrows() if is_on_vacation(vacs, r['name'], today)] if not all_h.empty else []
    avail_h_names = [r['name'] for _, r in all_h.iterrows() if not is_on_vacation(vacs, r['name'], today)] if not all_h.empty else []
    
    req_helpers = len(avail_d_names) - len(solo_d_names)
    shortage = req_helpers - len(avail_h_names)

    col_a, col_b, col_c, col_d = st.columns(4)
    
    with col_a:
        st.metric("🚛 Total Drivers Available", f"{len(avail_d_names)} / {len(all_d)}")
        with st.popover("🔍 View Drivers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            if avail_d_names: st.markdown("**Available:**<ol>" + "".join([f"<li>{n}</li>" for n in avail_d_names]) + "</ol>", unsafe_allow_html=True)
            if vac_d_names: st.markdown("**On Vacation:**<ol>" + "".join([f"<li>{n}</li>" for n in vac_d_names]) + "</ol>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_b:
        st.metric("👤 Total Helpers Available", f"{len(avail_h_names)} / {len(all_h)}")
        with st.popover("🔍 View Helpers"):
            st.markdown('<div style="max-height: 250px; overflow-y: auto;">', unsafe_allow_html=True)
            if avail_h_names: st.markdown("**Available:**<ol>" + "".join([f"<li>{n}</li>" for n in avail_h_names]) + "</ol>", unsafe_allow_html=True)
            if vac_h_names: st.markdown("**On Vacation:**<ol>" + "".join([f"<li>{n}</li>" for n in vac_h_names]) + "</ol>", unsafe_allow_html=True)
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

    st.header("🧠 AI Route Generation")
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=today)
    rot_type = col2.radio("Who is rotating this month?", ["Drivers", "Helpers"])
    st.info(f"💡 Note: You are rotating **{rot_type}**. The system will KEEP the currently assigned **{'Helpers' if rot_type == 'Drivers' else 'Drivers'}** so they can train the incoming staff for one month.")
    
    if st.button("Generate Smart AI Route Plan", type="primary"):
        with st.spinner("Calculating 0-Experience priorities, health cards, and strict 6-month penalties..."):
            drivers = load_table('drivers')
            helpers = load_table('helpers')
            areas = load_table('areas')
            vehicles = load_table('vehicles')
            history = load_table('history')
            active_routes = load_table('active_routes')
            
            route_targets = areas.to_dict('records') if not areas.empty else []
            route_plan, report_log = [], []
            used_drivers, used_helpers, used_vehicles = set(), set(), set()

            for area in route_targets:
                area_name = area['name']
                req_sector = area.get('sector', 'Pharma')
                needs_helper = area.get('needs_helper', 'Yes') == 'Yes'
                    
                prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                a_d_code, a_d_name, a_h_code, a_h_name, a_v_num, log_reason = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "No pool left"

                if rot_type == "Drivers":
                    if needs_helper and not prev_assignment.empty and prev_assignment.iloc[0]['helper_code'] != "N/A":
                        a_h_code, a_h_name = prev_assignment.iloc[0]['helper_code'], prev_assignment.iloc[0]['helper_name']
                        used_helpers.add(a_h_code)
                    
                    avail_dr = drivers[~drivers['code'].isin(used_drivers)]
                    best_d, d_reason = select_best_candidate(avail_dr, area_name, req_sector, month_target, history, vacs, role="Driver")
                    if best_d is not None:
                        a_d_code, a_d_name, log_reason = best_d['code'], best_d['name'], f"DRIVER -> {d_reason}"
                        used_drivers.add(a_d_code)
                        if best_d.get('needs_helper') == 'No': needs_helper = False
                else:
                    if not prev_assignment.empty:
                        a_d_code, a_d_name = prev_assignment.iloc[0]['driver_code'], prev_assignment.iloc[0]['driver_name']
                        a_v_num = prev_assignment.iloc[0]['veh_num']
                        used_drivers.add(a_d_code)
                        used_vehicles.add(a_v_num)
                        
                        d_type_chk = drivers[drivers['code'] == a_d_code]['needs_helper'].values if not drivers.empty else []
                        if len(d_type_chk) > 0 and d_type_chk[0] == 'No': needs_helper = False
                        
                    if needs_helper:
                        avail_hl = helpers[~helpers['code'].isin(used_helpers)]
                        best_h, h_reason = select_best_candidate(avail_hl, area_name, req_sector, month_target, history, vacs, role="Helper")
                        if best_h is not None:
                            a_h_code, a_h_name, log_reason = best_h['code'], best_h['name'], f"HELPER -> {h_reason}"
                            used_helpers.add(a_h_code)

                if not needs_helper: a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"

                if a_d_code != "UNASSIGNED" and a_v_num == "UNASSIGNED":
                    d_type = drivers[drivers['code'] == a_d_code]['veh_type'].values[0] if not drivers[drivers['code'] == a_d_code].empty else "VAN"
                    tvt = "2-8 VAN" if "2-8" in req_sector else ("BUS" if "Govt" in req_sector else ("PICK-UP" if "Pick-Up" in req_sector or "PICK UP" in area_name else d_type))
                    
                    avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['anchor_area'] == area_name)]
                    if avail_v.empty: avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['type'] == tvt) & (vehicles['anchor_area'] == "None")]
                    if avail_v.empty: avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['anchor_area'] == "None")]
                    if not avail_v.empty:
                        a_v_num = avail_v.iloc[0]['number']
                        used_vehicles.add(a_v_num)

                route_plan.append({"Area Code": area['code'], "Area Full Name": area_name, "Sector": req_sector, "Driver Code": a_d_code, "Driver Name": a_d_name, "Helper Code": a_h_code, "Helper Name": a_h_name, "Vehicle Number": a_v_num})
                report_log.append({"Area": area_name, "Driver": a_d_name, "Helper": a_h_name, "AI Logic Reason": log_reason})

            st.session_state.generated_plan = route_plan
            st.session_state.generated_report = report_log
            st.session_state.plan_date = month_target

    if 'generated_plan' in st.session_state:
        st.success("✨ AI Plan Generated! Double-click any cell below to MANUALLY EDIT the route before saving.")
        
        df_r = pd.DataFrame(st.session_state.generated_plan)
        df_log = pd.DataFrame(st.session_state.generated_report)
        
        if 'S/N' not in df_r.columns: df_r.insert(0, 'S/N', range(1, 1 + len(df_r)))
        if 'S/N' not in df_log.columns: df_log.insert(0, 'S/N', range(1, 1 + len(df_log)))
        
        edited_df = st.data_editor(df_r, use_container_width=True, hide_index=True, key="route_editor")
        
        with st.expander("Show AI Logic Report (Why the AI chose these people)"):
            st.dataframe(df_log, use_container_width=True, hide_index=True)
        
        output = generate_excel_with_sn([edited_df, df_log], ['Route Plan', 'AI Logic Report'])
        col_down, col_app = st.columns(2)
        col_down.download_button("📥 Download Excel Route Plan", data=output, file_name=f"Smart_Plan_{st.session_state.plan_date}.xlsx")
        
        if col_app.button("✅ Approve Plan & Save Experiences", type="primary"):
            run_query("DELETE FROM active_routes", table_name="active_routes", action="CLEAR_TABLE") 
            p_s = st.session_state.plan_date.strftime("%Y-%m-%d")
            p_e = (st.session_state.plan_date + timedelta(days=30)).strftime("%Y-%m-%d")
            
            for index, r in edited_df.iterrows():
                q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                run_query(q_ar, (r['S/N'], r['Area Code'], r['Area Full Name'], r['Driver Code'], r['Driver Name'], r['Helper Code'], r['Helper Name'], r['Vehicle Number']), table_name="active_routes", action="INSERT", data={"order_num":r['S/N'], "area_code":r['Area Code'], "area_name":r['Area Full Name'], "driver_code":r['Driver Code'], "driver_name":r['Driver Name'], "helper_code":r['Helper Code'], "helper_name":r['Helper Name'], "veh_num":r['Vehicle Number']})
                
                for code, name, ptype in [(r['Driver Code'], r['Driver Name'], "Driver"), (r['Helper Code'], r['Helper Name'], "Helper")]:
                    if code not in ["UNASSIGNED", "N/A"]:
                        hist_chk = load_table('history')
                        if hist_chk.empty or len(hist_chk[(hist_chk['person_code']==code) & (hist_chk['area']==r['Area Full Name']) & (hist_chk['date']==p_s)]) == 0:
                            run_query("INSERT INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)", (ptype, code, name, r['Area Full Name'], r['Sector'], p_s, p_e), table_name="history", action="INSERT", data={"person_type":ptype, "person_code":code, "person_name":name, "area":r['Area Full Name'], "sector":r['Sector'], "date":p_s, "end_date":p_e})
            
            st.success("Plan Approved & Saved! Your manual edits were secured. System will remember these sector experiences for next month.")
            del st.session_state.generated_plan


# ==========================================
# SCREEN 2: DATABASE MANAGEMENT
# ==========================================
elif choice == "2. Database Management":
    st.header("🗄️ Manage Database")
    
    areas_df = load_table('areas')
    area_list = ["None"] + (areas_df['name'].tolist() if not areas_df.empty else [])
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles", "📥 Bulk Excel Sync"])

    with tab1:
        st.subheader("📋 Full Drivers List")
        drivers_df = load_table('drivers')
        disp_df = drivers_df.drop(columns=['id', 'restriction'], errors='ignore').copy()
        if not disp_df.empty: disp_df.insert(0, 'S/N', range(1, 1 + len(disp_df)))
        st.dataframe(disp_df, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            d_name = st.text_input("Name")
            d_code = st.text_input("Code")
            col_t, col_s, col_h = st.columns(3)
            d_type = col_t.selectbox("Vehicle Type", VEHICLE_OPTIONS)
            d_sec = col_s.selectbox("Category / Sector", SECTOR_OPTIONS)
            d_needs_h = col_h.selectbox("Needs Helper?", ["Yes", "No"])
            d_anchor = st.selectbox("Anchor Area", area_list, key="d_add_anchor")
            if st.button("➕ Add Driver", use_container_width=True):
                run_query("INSERT INTO drivers (name, code, veh_type, sector, needs_helper, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                          (d_name, d_code, d_type, d_sec, d_needs_h, "None", d_anchor), table_name="drivers", action="INSERT", data={"name":d_name, "code":d_code, "veh_type":d_type, "sector":d_sec, "needs_helper":d_needs_h, "restriction":"None", "anchor_area":d_anchor})
                st.rerun()

        with c_edit:
            sel_d_code = st.selectbox("Select Driver to Edit/Delete", drivers_df['code'].tolist() if not drivers_df.empty else [])
            if sel_d_code:
                d_data = drivers_df[drivers_df['code'] == sel_d_code].iloc[0]
                e_name = st.text_input("Edit Name", d_data['name'], key=f"d_name_{d_data['id']}")
                ct, cs, ch = st.columns(3)
                e_type = ct.selectbox("Edit Veh Type", VEHICLE_OPTIONS, index=VEHICLE_OPTIONS.index(d_data['veh_type']) if d_data['veh_type'] in VEHICLE_OPTIONS else 0, key=f"d_veh_{d_data['id']}")
                e_sec = cs.selectbox("Edit Category / Sector", SECTOR_OPTIONS, index=SECTOR_OPTIONS.index(d_data.get('sector', 'None')) if d_data.get('sector') in SECTOR_OPTIONS else 0, key=f"d_sec_{d_data['id']}")
                e_needs_h = ch.selectbox("Edit Needs Helper", ["Yes", "No"], index=1 if d_data.get('needs_helper') == "No" else 0, key=f"d_nh_{d_data['id']}")
                
                a_idx = area_list.index(d_data.get('anchor_area', 'None')) if d_data.get('anchor_area', 'None') in area_list else 0
                e_anchor = st.selectbox("Edit Anchor", area_list, index=a_idx, key=f"d_anc_{d_data['id']}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Driver", use_container_width=True, key=f"d_upd_{d_data['id']}"):
                    run_query("UPDATE drivers SET name=?, veh_type=?, sector=?, needs_helper=?, anchor_area=? WHERE code=?", (e_name, e_type, e_sec, e_needs_h, e_anchor, sel_d_code), table_name="drivers", action="UPDATE", doc_id=d_data['id'], data={"name":e_name, "veh_type":e_type, "sector":e_sec, "needs_helper":e_needs_h, "anchor_area":e_anchor})
                    st.rerun()
                if c_del.button("🗑️ Delete Driver", use_container_width=True, key=f"d_del_{d_data['id']}"):
                    run_query("DELETE FROM drivers WHERE code=?", (sel_d_code,), table_name="drivers", action="DELETE_DOC", doc_id=d_data['id'])
                    st.rerun()

    with tab2:
        st.subheader("📋 Full Helpers List")
        helpers_df = load_table('helpers')
        disp_h = helpers_df.drop(columns=['id', 'restriction'], errors='ignore').copy()
        if not disp_h.empty: disp_h.insert(0, 'S/N', range(1, 1 + len(disp_h)))
        st.dataframe(disp_h, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            h_name = st.text_input("Helper Name")
            h_code = st.text_input("Helper Code")
            h_health = st.selectbox("Has Health Card?", ["No", "Yes"])
            h_anchor = st.selectbox("Anchor Area", area_list, key="h_anchor_add")
            if st.button("➕ Add Helper", use_container_width=True):
                run_query("INSERT INTO helpers (name, code, health_card, restriction, anchor_area) VALUES (?, ?, ?, ?, ?)", (h_name, h_code, h_health, "None", h_anchor), table_name="helpers", action="INSERT", data={"name":h_name, "code":h_code, "health_card":h_health, "restriction":"None", "anchor_area":h_anchor})
                st.rerun()
        with c_edit:
            sel_h_code = st.selectbox("Select Helper to Edit/Delete", helpers_df['code'].tolist() if not helpers_df.empty else [])
            if sel_h_code:
                h_data = helpers_df[helpers_df['code'] == sel_h_code].iloc[0]
                e_hname = st.text_input("Edit Name", h_data['name'], key=f"h_name_{h_data['id']}")
                e_hhealth = st.selectbox("Edit Health Card", ["No", "Yes"], index=1 if h_data.get('health_card') == "Yes" else 0, key=f"h_hc_{h_data['id']}")
                ha_idx = area_list.index(h_data.get('anchor_area', 'None')) if h_data.get('anchor_area', 'None') in area_list else 0
                e_hanchor = st.selectbox("Edit Anchor", area_list, index=ha_idx, key=f"h_anc_{h_data['id']}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Helper", use_container_width=True, key=f"h_upd_{h_data['id']}"):
                    run_query("UPDATE helpers SET name=?, health_card=?, anchor_area=? WHERE code=?", (e_hname, e_hhealth, e_hanchor, sel_h_code), table_name="helpers", action="UPDATE", doc_id=h_data['id'], data={"name":e_hname, "health_card":e_hhealth, "anchor_area":e_hanchor})
                    st.rerun()
                if c_del.button("🗑️ Delete Helper", use_container_width=True, key=f"h_del_{h_data['id']}"):
                    run_query("DELETE FROM helpers WHERE code=?", (sel_h_code,), table_name="helpers", action="DELETE_DOC", doc_id=h_data['id'])
                    st.rerun()

    with tab3:
        st.subheader("📋 Full Areas Route Template")
        a_df = load_table('areas')
        disp_a = a_df.drop(columns=['id'], errors='ignore').copy()
        if not disp_a.empty: disp_a.insert(0, 'S/N', range(1, 1 + len(disp_a)))
        st.dataframe(disp_a, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            a_name = st.text_input("Area Full Name")
            a_code = st.text_input("Area Code")
            col_s, col_n = st.columns(2)
            a_sec = col_s.selectbox("Area Sector", SECTOR_OPTIONS)
            a_needs = col_n.selectbox("Needs Helper for this Route?", ["Yes", "No"])
            if st.button("➕ Add Area", use_container_width=True):
                new_order = len(a_df) + 1
                run_query("INSERT INTO areas (name, code, sector, needs_helper, sort_order) VALUES (?, ?, ?, ?, ?)", (a_name, a_code, a_sec, a_needs, new_order), table_name="areas", action="INSERT", data={"name":a_name, "code":a_code, "sector":a_sec, "needs_helper":a_needs, "sort_order":new_order})
                st.rerun()
        with c_edit:
            sel_a = st.selectbox("Select Area to Edit/Delete", a_df['name'].tolist() if not a_df.empty else [])
            if sel_a:
                a_data = a_df[a_df['name'] == sel_a].iloc[0]
                ea_name = st.text_input("Edit Name", a_data['name'], key=f"a_name_{a_data['id']}")
                ea_code = st.text_input("Edit Code", a_data['code'], key=f"a_code_{a_data['id']}")
                ecs, ecn = st.columns(2)
                ea_sec = ecs.selectbox("Edit Sector", SECTOR_OPTIONS, index=SECTOR_OPTIONS.index(a_data.get('sector', 'Pharma')) if a_data.get('sector') in SECTOR_OPTIONS else 0, key=f"a_sec_{a_data['id']}")
                ea_needs = ecn.selectbox("Edit Needs Helper", ["Yes", "No"], index=1 if a_data.get('needs_helper') == "No" else 0, key=f"a_nh_{a_data['id']}")
                
                cu, cd = st.columns(2)
                if cu.button("💾 Update Area", use_container_width=True, key=f"a_upd_{a_data['id']}"):
                    run_query("UPDATE areas SET name=?, code=?, sector=?, needs_helper=? WHERE name=?", (ea_name, ea_code, ea_sec, ea_needs, sel_a), table_name="areas", action="UPDATE", doc_id=a_data['id'], data={"name":ea_name, "code":ea_code, "sector":ea_sec, "needs_helper":ea_needs})
                    st.rerun()
                if cd.button("🗑️ Delete Area", use_container_width=True, key=f"a_del_{a_data['id']}"):
                    run_query("DELETE FROM areas WHERE name=?", (sel_a,), table_name="areas", action="DELETE_DOC", doc_id=a_data['id'])
                    st.rerun()

    with tab4:
        st.subheader("📋 Full Vehicles List")
        v_df = load_table('vehicles')
        disp_v = v_df.drop(columns=['id'], errors='ignore').copy()
        if not disp_v.empty: disp_v.insert(0, 'S/N', range(1, 1 + len(disp_v)))
        st.dataframe(disp_v, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            v_num = st.text_input("Vehicle Number")
            v_type = st.selectbox("Type", VEHICLE_OPTIONS)
            v_anchor = st.selectbox("Anchor to Specific Area", area_list, key="v_anchor")
            if st.button("➕ Add Vehicle", use_container_width=True):
                run_query("INSERT INTO vehicles (number, type, anchor_area) VALUES (?, ?, ?)", (v_num, v_type, v_anchor), table_name="vehicles", action="INSERT", data={"number":v_num, "type":v_type, "anchor_area":v_anchor})
                st.rerun()
        with c_edit:
            sel_v = st.selectbox("Select Vehicle to Edit/Delete", v_df['number'].tolist() if not v_df.empty else [])
            if sel_v:
                v_data = v_df[v_df['number'] == sel_v].iloc[0]
                ev_type = st.selectbox("Edit Type", VEHICLE_OPTIONS, index=VEHICLE_OPTIONS.index(v_data['type']) if v_data['type'] in VEHICLE_OPTIONS else 0, key=f"v_type_{v_data['id']}")
                va_idx = area_list.index(v_data.get('anchor_area', 'None')) if v_data.get('anchor_area', 'None') in area_list else 0
                ev_anchor = st.selectbox("Edit Anchor Area", area_list, index=va_idx, key=f"v_anc_{v_data['id']}")
                
                cu, cd = st.columns(2)
                if cu.button("💾 Update Veh", use_container_width=True, key=f"v_upd_{v_data['id']}"):
                    run_query("UPDATE vehicles SET type=?, anchor_area=? WHERE number=?", (ev_type, ev_anchor, sel_v), table_name="vehicles", action="UPDATE", doc_id=v_data['id'], data={"type":ev_type, "anchor_area":ev_anchor})
                    st.rerun()
                if cd.button("🗑️ Delete Veh", use_container_width=True, key=f"v_del_{v_data['id']}"):
                    run_query("DELETE FROM vehicles WHERE number=?", (sel_v,), table_name="vehicles", action="DELETE_DOC", doc_id=v_data['id'])
                    st.rerun()
                
    with tab5:
        st.subheader("📥 Export / 📤 Import Database")
        dfs_to_export = [load_table(t) for t in ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations']]
        output = generate_excel_with_sn(dfs_to_export, ['drivers', 'helpers', 'areas', 'vehicles', 'history', 'vacations'])
        st.download_button("📥 Download Master Database (Excel)", data=output, file_name="Master_Database.xlsx", type="primary")
        
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
        st.warning("If your Areas got messed up, click this to reset the Route Template exactly to your Image Layout.")
        if st.button("♻️ Restore 39-Row Route Layout", type="primary"):
            with st.spinner("Restoring layout..."):
                auto_seed_database(force=True)
            st.success("Layout restored successfully!")
            st.rerun()


# ==========================================
# SCREEN 3: PAST EXPERIENCE BUILDER
# ==========================================
elif choice == "3. Past Experience Builder":
    st.header("🕰️ Manage Past Experience")
    history_df = load_table('history')
    
    disp_hist = history_df.drop(columns=['id'], errors='ignore').sort_values(by="date", ascending=False).copy()
    if not disp_hist.empty: disp_hist.insert(0, 'S/N', range(1, 1 + len(disp_hist)))
    st.dataframe(disp_hist, use_container_width=True, height=250, hide_index=True)
    
    with st.expander("📥 Export / 📤 Import History Data"):
        output = generate_excel_with_sn([history_df], ['history'])
        st.download_button("📥 Download History Data", data=output, file_name="History_Data.xlsx")
        
        up_hist = st.file_uploader("Upload History Excel", type=['xlsx'], key="up_hist")
        if up_hist and st.button("Sync History Database"):
            df = pd.read_excel(up_hist)
            if not FIREBASE_READY: run_query("DELETE FROM history")
            else: run_query(None, table_name="history", action="CLEAR_TABLE")
            
            for _, row in df.iterrows():
                data_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v) and k not in ['id', 'S/N']}
                if not FIREBASE_READY:
                    cols, vals = ', '.join(data_dict.keys()), tuple(data_dict.values())
                    qmarks = ', '.join(['?'] * len(data_dict))
                    run_query(f"INSERT INTO history ({cols}) VALUES ({qmarks})", vals)
                else:
                    run_query(None, table_name="history", action="INSERT", data=data_dict)
            st.rerun()

    st.divider()
    c_add, c_edit = st.columns(2)
    areas_df = load_table('areas')
    area_list = areas_df['name'].tolist() if not areas_df.empty else []
    
    with c_add:
        st.subheader("➕ Add Experience")
        p_type = st.selectbox("Role", ["Driver", "Helper"])
        df_names = load_table('drivers') if p_type == "Driver" else load_table('helpers')
        person_list = [f"{row['code']} - {row['name']}" for _, row in df_names.iterrows()] if not df_names.empty else []
        if person_list:
            p_person = st.selectbox("Select Person", person_list)
            p_area = st.selectbox("Area Experienced In", area_list)
            p_sec = st.selectbox("Which Sector was this in?", SECTOR_OPTIONS)
            d1, d2 = st.columns(2)
            p_start_date = d1.date_input("From Date (Start)")
            p_end_date = d2.date_input("To Date (End)")
            
            if st.button("➕ Add Past Experience", use_container_width=True):
                p_code, p_name = p_person.split(" - ")[0], p_person.split(" - ")[1]
                overlap = history_df[(history_df['person_code']==p_code) & (history_df['area']==p_area) & (history_df['date']==p_start_date.strftime("%Y-%m-%d"))]
                if p_start_date > p_end_date: 
                    st.error("Start Date cannot be after End Date.")
                elif not overlap.empty:
                    st.error("⚠️ This person already has an experience log for this Area on this exact Start Date!")
                else:
                    run_query("INSERT INTO history (person_type, person_code, person_name, area, sector, date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (p_type, p_code, p_name, p_area, p_sec, p_start_date.strftime("%Y-%m-%d"), p_end_date.strftime("%Y-%m-%d")), 
                              table_name="history", action="INSERT", data={"person_type":p_type, "person_code":p_code, "person_name":p_name, "area":p_area, "sector":p_sec, "date":p_start_date.strftime("%Y-%m-%d"), "end_date":p_end_date.strftime("%Y-%m-%d")})
                    st.rerun()

    with c_edit:
        st.subheader("✏️ Edit / Delete Experience")
        if not history_df.empty:
            hist_options = []
            hist_map = {}
            for idx, row in history_df.iterrows():
                sec = row.get('sector', 'Pharma')
                if pd.isna(sec) or sec == "nan": sec = "Pharma"
                label = f"[{idx+1}] {row['person_name']} - {row['area']} ({sec})"
                hist_options.append(label)
                hist_map[label] = str(row['id'])

            sel_hist_str = st.selectbox("Select Record to Edit/Delete", hist_options)
            if sel_hist_str:
                hist_id = hist_map[sel_hist_str]
                hist_data = history_df[history_df['id'].astype(str) == hist_id].iloc[0]
                a_idx = area_list.index(hist_data['area']) if hist_data['area'] in area_list else 0
                e_area = st.selectbox("Edit Area", area_list, index=a_idx, key=f"he_area_{hist_id}")
                e_sec = st.selectbox("Edit Sector", SECTOR_OPTIONS, index=SECTOR_OPTIONS.index(hist_data.get('sector', 'Pharma')) if hist_data.get('sector') in SECTOR_OPTIONS else 0, key=f"he_sec_{hist_id}")
                ed1, ed2 = st.columns(2)
                e_start_val, e_end_val = safe_parse_date(hist_data['date']), safe_parse_date(hist_data['end_date'])
                new_start = ed1.date_input("Edit Start Date", value=e_start_val, key=f"he_es_{hist_id}")
                new_end = ed2.date_input("Edit End Date", value=e_end_val, key=f"he_ee_{hist_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Experience", use_container_width=True, key=f"he_upd_{hist_id}"):
                    run_query("UPDATE history SET area=?, sector=?, date=?, end_date=? WHERE id=?", (e_area, e_sec, new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d"), hist_id), table_name="history", action="UPDATE", doc_id=hist_id, data={"area":e_area, "sector":e_sec, "date":new_start.strftime("%Y-%m-%d"), "end_date":new_end.strftime("%Y-%m-%d")})
                    st.rerun()
                if c_del.button("🗑️ Delete Experience", use_container_width=True, key=f"he_del_{hist_id}"):
                    run_query("DELETE FROM history WHERE id=?", (hist_id,), table_name="history", action="DELETE_DOC", doc_id=hist_id)
                    st.rerun()


# ==========================================
# SCREEN 4: VACATION SCHEDULE
# ==========================================
elif choice == "4. Vacation Schedule":
    st.header("🌴 Manage Vacation Schedule")
    vacs_df = load_table('vacations')

    st.subheader("📊 Active Vacations Overview")
    today = date.today()
    active_vacs = []
    if not vacs_df.empty and 'person_name' in vacs_df.columns:
        for _, row in vacs_df.iterrows():
            sd = safe_parse_date(row['start_date'])
            ed = safe_parse_date(row['end_date'])
            if sd <= today <= ed:
                active_vacs.append({
                    "Role": row['person_type'],
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
                if vacs_df.empty or 'person_name' not in vacs_df.columns:
                    last_vac = pd.DataFrame()
                else:
                    last_vac = vacs_df[vacs_df['person_name'] == p['name']]
                    
                if last_vac.empty:
                    due_list.append({"Name": p['name'], "Role": role, "Status": "NEVER Taken a Vacation!"})
                else:
                    lv_date = safe_parse_date(last_vac.iloc[-1]['end_date'])
                    days_since = (today - lv_date).days
                    if days_since > 300:
                        due_list.append({"Name": p['name'], "Role": role, "Status": f"Overdue by {days_since - 300} days (Last: {lv_date})"})
        if due_list:
            st.dataframe(pd.DataFrame(due_list), use_container_width=True, hide_index=True)
        else:
            st.success("Everyone seems well rested!")

    st.divider()

    st.subheader("📋 Full Vacation Database")
    disp_vac = vacs_df.drop(columns=['id'], errors='ignore').copy()
    if not disp_vac.empty: disp_vac.insert(0, 'S/N', range(1, 1 + len(disp_vac)))
    st.dataframe(disp_vac, use_container_width=True, height=250, hide_index=True)

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
        name_list = df_names['name'].tolist() if not df_names.empty else []
        if name_list:
            v_name = st.selectbox("Name", name_list)
            d1, d2 = st.columns(2)
            v_start = d1.date_input("Start Date (Leave)")
            v_end = d2.date_input("End Date (Return)", value=date.today() + timedelta(days=30))
            
            if st.button("➕ Add Vacation", use_container_width=True):
                if not vacs_df.empty and 'person_name' in vacs_df.columns:
                    overlap = vacs_df[(vacs_df['person_name'] == v_name) & (vacs_df['start_date'] == v_start.strftime("%Y-%m-%d"))]
                else:
                    overlap = pd.DataFrame()
                    
                if v_start > v_end: 
                    st.error("Start Date cannot be after End Date.")
                elif not overlap.empty:
                    st.error(f"⚠️ {v_name} already has a vacation logged starting exactly on {v_start.strftime('%Y-%m-%d')}!")
                else:
                    run_query("INSERT INTO vacations (person_type, person_name, start_date, end_date) VALUES (?, ?, ?, ?)", (v_type, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")), table_name="vacations", action="INSERT", data={"person_type":v_type, "person_name":v_name, "start_date":v_start.strftime("%Y-%m-%d"), "end_date":v_end.strftime("%Y-%m-%d")})
                    st.rerun()

    with c_edit:
        st.subheader("✏️ Edit / Delete Vacation")
        if not vacs_df.empty:
            vac_list = [f"{row['id']} - {row['person_name']} ({row['start_date']} to {row['end_date']})" for _, row in vacs_df.iterrows()]
            sel_vac_str = st.selectbox("Select Vacation to Edit/Delete", vac_list)
            if sel_vac_str:
                vac_id = sel_vac_str.split(" - ")[0]
                vac_data = vacs_df[vacs_df['id'].astype(str) == vac_id].iloc[0]
                ed1, ed2 = st.columns(2)
                e_vstart_val, e_vend_val = safe_parse_date(vac_data['start_date']), safe_parse_date(vac_data['end_date'])
                new_vstart = ed1.date_input("Edit Start Date", value=e_vstart_val, key=f"vac_es_{vac_id}")
                new_vend = ed2.date_input("Edit End Date", value=e_vend_val, key=f"vac_ee_{vac_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Vacation", use_container_width=True, key=f"vac_upd_{vac_id}"):
                    if new_vstart > new_vend: st.error("Start Date cannot be after End Date.")
                    else:
                        run_query("UPDATE vacations SET start_date=?, end_date=? WHERE id=?", (new_vstart.strftime("%Y-%m-%d"), new_vend.strftime("%Y-%m-%d"), vac_id), table_name="vacations", action="UPDATE", doc_id=vac_id, data={"start_date":new_vstart.strftime("%Y-%m-%d"), "end_date":new_vend.strftime("%Y-%m-%d")})
                        st.rerun()
                        
                if c_del.button("🗑️ Delete Vacation", use_container_width=True, key=f"vac_del_{vac_id}"):
                    run_query("DELETE FROM vacations WHERE id=?", (vac_id,), table_name="vacations", action="DELETE_DOC", doc_id=vac_id)
                    st.rerun()
