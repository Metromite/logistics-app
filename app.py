import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import io
import os

# --- FIREBASE / SQLITE FALLBACK SETUP ---
FIREBASE_READY = False
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    if os.path.exists("firebase-key.json"):
        if not firebase_admin._apps:
            cred = credentials.Certificate("firebase-key.json")
            firebase_admin.initialize_app(cred)
        db_fs = firestore.client()
        FIREBASE_READY = True
except ImportError:
    pass

if not FIREBASE_READY:
    import sqlite3

# --- 1. SEED DATA ---
SEED_AREAS = [
    ("BAN", "BANIYAS AREA"), ("DXB", "Dubai Area"), ("MUS", "MUSAFFAH AREA"), ("KLF", "KHALIFA CITY AREA"),
    ("TCA", "TOURIST CLUB AREA"), ("KLD", "KHALIDIYA AREA"), ("ARP", "AIRPORT AREA"), ("ALNJ", "AL AIN JIMMY AREA"),
    ("ALNC", "AL AIN CITY"), ("WRG", "WESTERN REGION"), ("AUH1", "ABU DHABI WH-1"), ("AUH", "Abu Dhabi"),
    ("KIZ", "KIZAD, CPC"), ("BUH", "BUHAIRAH"), ("ALQ-2", "Dubai Al Quoz - Rasal Khoor , Business B"),
    ("JFZ", "JAFZA City Pharmacy Stores"), ("SHJ", "Sharjah Stores"), ("QUS", "Al Qusais & Mamzar Area Dubai"),
    ("RAK", "Ras Al Khaimah & Umm Al Quwain"), ("FUJ", "Fujairah & Kalba & Dhaid"), ("AJM", "Ajman"),
    ("SHJS", "Sharjah & Sanaiya & Al Nahda"), ("SHJK", "Sharjah & King Faizal & Zahrah St"),
    ("SHJR", "Sharjah & Rolla & Taawun Area"), ("DXBI", "Deira In & Rigga & Naif St"),
    ("DXBO", "Deira Out & Mirdif & Rashidiya Area"), ("BUR", "Bur Dubai & Karama & Mankhol Area"),
    ("JUM", "Dubai Jumairah & Satwa Area"), ("ALQ", "Dubai Al Quoz & Awir & Barsha Area"),
    ("JA", "Dubai Jebal Ali & DIP Area"), ("DIC", "DIC City Pharmacy Store"), ("AD", "AUH City Pharmacy Store"),
    ("ALN", "Al Ain City Pharmacy Store"), ("HAT", "Hatta & Al Madam Area")
]

# --- 2. UNIVERSAL DB ADAPTER (Handles Firebase or SQLite transparently) ---
def init_db():
    if FIREBASE_READY: return None
    conn = sqlite3.connect('logistics.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY, name TEXT, code TEXT, veh_type TEXT, sector TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (id INTEGER PRIMARY KEY, name TEXT, code TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY, name TEXT, code TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY, number TEXT, type TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, area TEXT, date TEXT, end_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (id INTEGER PRIMARY KEY, person_type TEXT, person_name TEXT, start_date DATE, end_date DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS performance (id INTEGER PRIMARY KEY, person_code TEXT, area TEXT, success_rate REAL, delay_count INTEGER)''')
    
    try: c.execute("ALTER TABLE drivers ADD COLUMN restriction TEXT DEFAULT 'None'")
    except: pass
    try: c.execute("ALTER TABLE helpers ADD COLUMN restriction TEXT DEFAULT 'None'")
    except: pass
    try: c.execute("ALTER TABLE history ADD COLUMN end_date TEXT")
    except: pass

    # Seed areas if empty
    c.execute("SELECT COUNT(*) FROM areas")
    if c.fetchone()[0] == 0:
        c.executemany("INSERT INTO areas (code, name) VALUES (?, ?)", SEED_AREAS)
    conn.commit()
    return conn

conn = init_db()

def clear_cache():
    st.cache_data.clear()

@st.cache_data
def load_table(table_name):
    if FIREBASE_READY:
        docs = db_fs.collection(table_name).stream()
        data = [{**doc.to_dict(), 'id': doc.id} for doc in docs]
        return pd.DataFrame(data)
    else:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def run_query(query, params=(), collection=None, action=None, doc_id=None, data=None):
    clear_cache() # Invalidate cache on any DB write
    if FIREBASE_READY and collection:
        if action == "INSERT": db_fs.collection(collection).add(data)
        elif action == "UPDATE": db_fs.collection(collection).document(doc_id).update(data)
        elif action == "DELETE": db_fs.collection(collection).document(doc_id).delete()
    else:
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()

# --- 3. LOGIC & SMART SCORING SYSTEM ---
def safe_parse_date(date_str):
    try: return datetime.strptime(date_str, "%Y-%m-%d").date()
    except: return date.today()

@st.cache_data
def get_experience_months(history_df, person_code, area_name):
    if history_df.empty or 'person_code' not in history_df.columns: return 0
    records = history_df[(history_df['person_code'] == person_code) & (history_df['area'] == area_name)]
    total_days = 0
    for _, r in records.iterrows():
        s = safe_parse_date(r['date'])
        e = safe_parse_date(r['end_date']) if pd.notna(r.get('end_date')) and r['end_date'] != "None" else s + timedelta(days=30)
        total_days += max(0, (e - s).days)
    return total_days / 30.0

def get_last_assignment(history_df, person_code, area_name):
    if history_df.empty: return None
    records = history_df[(history_df['person_code'] == person_code) & (history_df['area'] == area_name)]
    if records.empty: return None
    return safe_parse_date(records.iloc[-1]['date'])

def is_on_vacation(vacations_df, person_name, target_date):
    if vacations_df.empty: return False
    for _, row in vacations_df.iterrows():
        if row['person_name'] == person_name:
            if safe_parse_date(row['start_date']) <= target_date <= safe_parse_date(row['end_date']):
                return True
    return False

def check_restriction(restriction, area_name):
    if restriction == "None" or pd.isna(restriction): return True
    if restriction == "Consumer Only" and "Consumer" not in area_name: return False
    if restriction == "2-8 Cars Only" and "2-8" not in area_name: return False
    mapping = {"Sharjah": ["Sharjah", "SHJ"], "Dubai": ["Dubai", "DXB", "JUM", "BUR", "QUS", "DIC", "ALQ", "JA", "JFZ"], "Ajman": ["Ajman", "AJM"], "Fujairah": ["Fujairah", "FUJ"], "Ras Al Khaimah": ["Ras Al Khaimah", "RAK"], "Abu Dhabi": ["Abu Dhabi", "AUH", "MUS", "BAN", "KLF", "TCA", "KLD", "ARP", "KIZ"], "Al Ain": ["Al Ain", "ALN"]}
    if restriction in mapping: return any(k.lower() in area_name.lower() for k in mapping[restriction])
    return restriction.lower() in area_name.lower()

def select_best_candidate(candidates_df, area_name, target_date, history_df, vacations_df, perf_df, partner_exp_months):
    best_candidate, best_score, best_reason = None, -9999, "No valid candidates"
    
    for _, person in candidates_df.iterrows():
        if is_on_vacation(vacations_df, person['name'], target_date): continue
        if not check_restriction(person.get('restriction', 'None'), area_name): continue
        if person.get('anchor_area') not in ["None", None, ""] and person.get('anchor_area') != area_name: continue
        
        exp_months = get_experience_months(history_df, person['code'], area_name)
        
        # 3 Month rule: At least one person must have 3m exp unless anchored
        if partner_exp_months < 3 and exp_months < 3 and person.get('anchor_area') != area_name:
            continue
            
        score = 0
        reasons = []

        # PRIORITY: Never Visited
        if exp_months == 0:
            score += 100
            reasons.append("Never Visited (+100)")
        else:
            score += (exp_months * 2)
            reasons.append(f"Exp {exp_months:.1f}m (+{int(exp_months*2)})")

        # PENALTY: Recent Assignment (last 6 months)
        last_date = get_last_assignment(history_df, person['code'], area_name)
        if last_date:
            months_since = (target_date - last_date).days / 30.0
            if months_since < 6:
                penalty = int((6 - months_since) * 10)
                score -= penalty
                reasons.append(f"Recent Penalty (-{penalty})")

        # BONUS: Anchor
        if person.get('anchor_area') == area_name:
            score += 200
            reasons.append("Anchored (+200)")

        # BONUS/PENALTY: Performance
        if not perf_df.empty and 'person_code' in perf_df.columns:
            p_data = perf_df[(perf_df['person_code'] == person['code']) & (perf_df['area'] == area_name)]
            if not p_data.empty:
                sr = p_data.iloc[0].get('success_rate', 100)
                dc = p_data.iloc[0].get('delay_count', 0)
                score += (sr * 0.1) - (dc * 2)
                reasons.append(f"Perf Add ({(sr*0.1)-(dc*2):.1f})")

        if score > best_score:
            best_score = score
            best_candidate = person
            best_reason = " | ".join(reasons) + f" = Score: {score:.1f}"

    return best_candidate, best_reason


# --- 4. UI SETUP ---
st.set_page_config(page_title="Smart Logistics AI", layout="wide")
st.title("🚛 Smart Logistics Route & Vacation AI")

if not FIREBASE_READY:
    st.warning("⚠️ Running in Local SQLite Mode. To enable Cloud Sharing, add 'firebase-key.json' to your repository.")

menu = ["1. AI Route Planner", "2. Master Database & Excel", "3. Vacation AI Planner"]
choice = st.sidebar.radio("Navigate", menu)

# ==========================================
# SCREEN 1: AI ROUTE PLANNER
# ==========================================
if choice == "1. AI Route Planner":
    st.header("🧠 AI Route Generation (Scoring System)")
    
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=date.today())
    rot_type = col2.radio("Who is rotating?", ["Drivers", "Helpers"])
    
    if st.button("Generate Smart AI Route Plan"):
        with st.spinner("Calculating billions of possibilities..."):
            drivers = load_table('drivers')
            helpers = load_table('helpers')
            areas = load_table('areas')
            vehicles = load_table('vehicles')
            history = load_table('history')
            vacations = load_table('vacations')
            active_routes = load_table('active_routes')
            
            # Create performance table if doesn't exist
            try: perf_df = load_table('performance')
            except: perf_df = pd.DataFrame()
            
            fallback_areas = ["2-8 Cars", "Urgent/Gov Order", "Pickup", "Substitute", "Second Trip", "Novo Sample"]
            all_area_names = areas['name'].tolist() + fallback_areas
            route_targets = areas.to_dict('records') + [{"code": "N/A", "name": fb} for fb in fallback_areas]
            
            route_plan, report_log = [], []
            used_drivers, used_helpers, used_vehicles = set(), set(), set()
            order_counter = 1

            for area in route_targets:
                area_name = area['name']
                needs_helper = not any(kw in area_name.lower() for kw in ["2-8", "urgent", "gov", "substitute"])
                prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                
                a_d_code, a_d_name, a_h_code, a_h_name, a_v_num, log_reason = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "Fallback / No Match"

                # Driver Rotation Logic
                if rot_type == "Drivers":
                    if needs_helper and not prev_assignment.empty and prev_assignment.iloc[0]['helper_code'] != "N/A":
                        a_h_code, a_h_name = prev_assignment.iloc[0]['helper_code'], prev_assignment.iloc[0]['helper_name']
                    
                    h_exp = get_experience_months(history, a_h_code, area_name) if a_h_code != "UNASSIGNED" else 0
                    avail_d = drivers[~drivers['code'].isin(used_drivers)]
                    
                    best_d, d_reason = select_best_candidate(avail_d, area_name, month_target, history, vacations, perf_df, h_exp)
                    if best_d is not None:
                        a_d_code, a_d_name, log_reason = best_d['code'], best_d['name'], f"DRIVER: {d_reason}"
                        used_drivers.add(a_d_code)
                        if best_d['veh_type'] == "BUS": needs_helper = False
                
                # Helper Rotation Logic
                else:
                    if not prev_assignment.empty:
                        a_d_code, a_d_name = prev_assignment.iloc[0]['driver_code'], prev_assignment.iloc[0]['driver_name']
                        d_type_chk = drivers[drivers['code'] == a_d_code]['veh_type'].values if not drivers.empty else []
                        if len(d_type_chk) > 0 and d_type_chk[0] == "BUS": needs_helper = False
                        
                    if needs_helper:
                        d_exp = get_experience_months(history, a_d_code, area_name) if a_d_code != "UNASSIGNED" else 0
                        avail_h = helpers[~helpers['code'].isin(used_helpers)]
                        best_h, h_reason = select_best_candidate(avail_h, area_name, month_target, history, vacations, perf_df, d_exp)
                        if best_h is not None:
                            a_h_code, a_h_name, log_reason = best_h['code'], best_h['name'], f"HELPER: {h_reason}"
                            used_helpers.add(a_h_code)

                if not needs_helper:
                    a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"

                # Assign Vehicle
                if a_d_code != "UNASSIGNED":
                    d_type = drivers[drivers['code'] == a_d_code]['veh_type'].values[0] if not drivers[drivers['code'] == a_d_code].empty else "VAN"
                    tvt = "PICK-UP" if "Pickup" in area_name else ("BUS" if "2-8" in area_name else d_type)
                    avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['type'] == tvt)]
                    if avail_v.empty: avail_v = vehicles[~vehicles['number'].isin(used_vehicles)]
                    if not avail_v.empty:
                        a_v_num = avail_v.iloc[0]['number']
                        used_vehicles.add(a_v_num)

                route_plan.append({"Order Num": order_counter, "Area Code": area['code'], "Area Name": area_name, "Driver Code": a_d_code, "Driver Name": a_d_name, "Helper Code": a_h_code, "Helper Name": a_h_name, "Vehicle": a_v_num})
                report_log.append({"Area": area_name, "Driver": a_d_name, "Helper": a_h_name, "AI Reason": log_reason})
                order_counter += 1

            st.session_state.generated_plan, st.session_state.generated_report, st.session_state.plan_date = route_plan, report_log, month_target
            st.success("✅ Smart Plan Generated! Review reasons and approve below.")

    if 'generated_plan' in st.session_state:
        df_r = pd.DataFrame(st.session_state.generated_plan)
        df_log = pd.DataFrame(st.session_state.generated_report)
        st.dataframe(df_r, use_container_width=True)
        st.dataframe(df_log, use_container_width=True)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_r.to_excel(writer, sheet_name='Route Plan', index=False)
            df_log.to_excel(writer, sheet_name='AI Logic Report', index=False)
        output.seek(0)
        
        col_down, col_app = st.columns(2)
        col_down.download_button("📥 Download Excel", data=output, file_name=f"Smart_Plan_{st.session_state.plan_date}.xlsx")
        
        if col_app.button("✅ Approve Plan & Save Performance History", type="primary"):
            run_query("DELETE FROM active_routes", collection="active_routes", action="DELETE") # Simplified for SQLite
            
            p_s = st.session_state.plan_date.strftime("%Y-%m-%d")
            p_e = (st.session_state.plan_date + timedelta(days=90)).strftime("%Y-%m-%d")
            
            for r in st.session_state.generated_plan:
                q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                run_query(q_ar, (r['Order Num'], r['Area Code'], r['Area Name'], r['Driver Code'], r['Driver Name'], r['Helper Code'], r['Helper Name'], r['Vehicle']), collection="active_routes", action="INSERT", data=r)
                
                # History & Performance Init
                for code, name, ptype in [(r['Driver Code'], r['Driver Name'], "Driver"), (r['Helper Code'], r['Helper Name'], "Helper")]:
                    if code not in ["UNASSIGNED", "N/A"]:
                        run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)", (ptype, code, name, r['Area Name'], p_s, p_e), collection="history", action="INSERT", data={"person_type":ptype, "person_code":code, "area":r['Area Name'], "date":p_s, "end_date":p_e})
                        # Init Perf
                        run_query("INSERT INTO performance (person_code, area, success_rate, delay_count) VALUES (?, ?, ?, ?)", (code, r['Area Name'], 100.0, 0), collection="performance", action="INSERT", data={"person_code":code, "area":r['Area Name'], "success_rate":100.0, "delay_count":0})
            
            st.success("Saved perfectly! 3 months experience and performance tracking initialized.")
            del st.session_state.generated_plan


# ==========================================
# SCREEN 2: MASTER DATABASE & EXCEL
# ==========================================
elif choice == "2. Master Database & Excel":
    st.header("🗄️ Master Database & Bulk Excel Center")
    
    tab1, tab2 = st.tabs(["📊 Bulk Excel Import/Export", "✏️ Manual Add/Edit"])
    
    with tab1:
        st.info("Download the entire database to your computer, edit it in Excel, and upload it back here to update the system instantly.")
        # EXPORT
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            load_table('drivers').to_excel(writer, sheet_name='drivers', index=False)
            load_table('helpers').to_excel(writer, sheet_name='helpers', index=False)
            load_table('areas').to_excel(writer, sheet_name='areas', index=False)
            load_table('vehicles').to_excel(writer, sheet_name='vehicles', index=False)
        output.seek(0)
        st.download_button("📥 Download Master Database (Excel)", data=output, file_name="Master_Database.xlsx", type="primary")
        
        st.divider()
        # IMPORT
        st.subheader("📤 Upload Updated Excel")
        uploaded_file = st.file_uploader("Upload your modified Master_Database.xlsx", type=['xlsx'])
        if uploaded_file and st.button("Sync Data to System"):
            xls = pd.ExcelFile(uploaded_file)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                run_query(f"DELETE FROM {sheet}") # Clear old
                for _, row in df.iterrows():
                    cols = ', '.join(row.index)
                    vals = tuple(row.values)
                    qmarks = ', '.join(['?'] * len(row))
                    run_query(f"INSERT INTO {sheet} ({cols}) VALUES ({qmarks})", vals)
            st.success("Database fully synchronized from Excel!")
            
    with tab2:
        st.write("Use the 'Download' feature above to manage massive lists easily.")
        st.dataframe(load_table('drivers'), use_container_width=True)

# ==========================================
# SCREEN 3: SMART VACATION AI
# ==========================================
elif choice == "3. Vacation AI Planner":
    st.header("🌴 Smart Vacation AI Planner")
    
    v_df = load_table('vacations')
    history_df = load_table('history')
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("⚠️ Due for Vacation (1-Year Rule)")
        st.write("These personnel haven't had a recorded vacation in over a year:")
        # Logic for finding due people
        due_list = []
        for df, role in [(load_table('drivers'), "Driver"), (load_table('helpers'), "Helper")]:
            for _, p in df.iterrows():
                last_vac = v_df[v_df['person_name'] == p['name']]
                if last_vac.empty: 
                    due_list.append({"Name": p['name'], "Role": role, "Status": "Never taken"})
                else:
                    lv_date = safe_parse_date(last_vac.iloc[-1]['end_date'])
                    if (date.today() - lv_date).days > 365:
                        due_list.append({"Name": p['name'], "Role": role, "Status": f"Last was {lv_date}"})
        st.dataframe(pd.DataFrame(due_list), use_container_width=True)

    with col2:
        st.subheader("➕ Schedule Gapless Vacation")
        v_type = st.selectbox("Role", ["Driver", "Helper"])
        name_list = load_table('drivers' if v_type == "Driver" else 'helpers')['name'].tolist()
        v_name = st.selectbox("Select Person", name_list)
        
        start_d = st.date_input("Leave Date")
        # Ensure minimum 1 month
        end_d = st.date_input("Return Date (Replacement must come EXACTLY on this day)", value=start_d + timedelta(days=30))
        
        if st.button("Approve Vacation & Secure Gap"):
            run_query("INSERT INTO vacations (person_type, person_name, start_date, end_date) VALUES (?, ?, ?, ?)",
                      (v_type, v_name, start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d")))
            st.success(f"Scheduled! System will automatically block {v_name} from routes during these dates.")
            st.rerun()

    st.divider()
    st.subheader("📋 Current Vacations List")
    st.dataframe(v_df, use_container_width=True)
