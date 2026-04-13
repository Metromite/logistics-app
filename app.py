import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import io
import os
import firebase_admin
from firebase_admin import credentials, firestore

# --- FIREBASE INITIALIZATION & DB ADAPTER ---
# The app tries to use Firebase first. If 'firebase-key.json' is missing, it automatically falls back to local SQLite to prevent crashing.
FIREBASE_READY = False
try:
    if os.path.exists("firebase-key.json"):
        if not firebase_admin._apps:
            cred = credentials.Certificate("firebase-key.json")
            firebase_admin.initialize_app(cred)
        db_fs = firestore.client()
        FIREBASE_READY = True
except Exception as e:
    st.sidebar.error(f"Firebase Init Error: {e}")

# SQLite Fallback Initialization
def init_sqlite_db():
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
    
    # Migrations
    for query in [
        "ALTER TABLE drivers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE helpers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE history ADD COLUMN end_date TEXT",
        "ALTER TABLE history ADD COLUMN person_code TEXT DEFAULT 'UNKNOWN'"
    ]:
        try: c.execute(query)
        except sqlite3.OperationalError: pass
    conn.commit()
    return conn

conn = None if FIREBASE_READY else init_sqlite_db()

# --- CACHED DATA LOADERS (Performance Optimization) ---
def clear_cache():
    st.cache_data.clear()

@st.cache_data
def load_table(table_name):
    """Loads a collection from Firestore OR a table from SQLite into a Pandas DataFrame."""
    if FIREBASE_READY:
        docs = db_fs.collection(table_name).stream()
        data = [{**doc.to_dict(), 'id': doc.id} for doc in docs]
        return pd.DataFrame(data)
    else:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def run_query(query, params=(), table_name=None, action=None, doc_id=None, data=None):
    """Universal function to write to SQLite OR Firebase."""
    clear_cache() # Invalidate cache on any database write
    if FIREBASE_READY and table_name and action:
        if action == "INSERT": db_fs.collection(table_name).add(data)
        elif action == "UPDATE": db_fs.collection(table_name).document(str(doc_id)).update(data)
        elif action == "DELETE": db_fs.collection(table_name).document(str(doc_id)).delete()
    else:
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()


# --- SMART SCORING & ASSIGNMENT LOGIC ---
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

@st.cache_data
def is_on_vacation(vacations_df, person_name, target_date):
    """Checks if a person is on vacation during the target date."""
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
    mapping = {
        "Sharjah": ["Sharjah", "SHJ"], "Dubai": ["Dubai", "DXB", "JUM", "BUR", "QUS", "DIC", "ALQ", "JA", "JFZ"],
        "Ajman": ["Ajman", "AJM"], "Fujairah": ["Fujairah", "FUJ"], "Ras Al Khaimah": ["Ras Al Khaimah", "RAK"],
        "Abu Dhabi": ["Abu Dhabi", "AUH", "MUS", "BAN", "KLF", "TCA", "KLD", "ARP", "KIZ"], "Al Ain": ["Al Ain", "ALN"]
    }
    if restriction in mapping: return any(k.lower() in area_name.lower() for k in mapping[restriction])
    return restriction.lower() in area_name.lower()

def select_best_candidate(candidates_df, area_name, target_date, history_df, vacations_df, perf_df, partner_exp_months):
    """Scoring System instead of first-match assignment."""
    best_candidate, best_score, best_reason = None, -9999, "No valid candidates found"
    
    for _, person in candidates_df.iterrows():
        # 1. Vacation Blocker
        if is_on_vacation(vacations_df, person['name'], target_date): continue
        # 2. Restriction & Anchor Validation
        if not check_restriction(person.get('restriction', 'None'), area_name): continue
        if person.get('anchor_area') not in ["None", None, ""] and person.get('anchor_area') != area_name: continue
        
        exp_months = get_experience_months(history_df, person['code'], area_name)
        
        # 3. Minimum Experience Rule (Partner must have 3 months exp if candidate has < 3)
        if partner_exp_months < 3 and exp_months < 3 and person.get('anchor_area') != area_name:
            continue
            
        # --- SCORING FORMULA ---
        score = (exp_months * 2)
        reasons = []

        if exp_months == 0:
            score += 100 # Huge priority for areas never visited
            reasons.append("Never Visited (+100)")
        
        last_date = get_last_assignment(history_df, person['code'], area_name)
        if last_date:
            months_since = (target_date - last_date).days / 30.0
            if months_since < 6:
                penalty = int(max(0, 6 - months_since) * 10 * 3) # Penalty * 3 as requested
                score -= penalty
                reasons.append(f"Recent Penalty (-{penalty})")
                
        if person.get('anchor_area') == area_name:
            score += 200 # Anchor bonus * 2
            reasons.append("Anchor Bonus (+200)")

        # Performance Scoring
        if not perf_df.empty and 'person_code' in perf_df.columns:
            p_data = perf_df[(perf_df['person_code'] == person['code']) & (perf_df['area'] == area_name)]
            if not p_data.empty:
                sr = p_data.iloc[0].get('success_rate', 100)
                dc = p_data.iloc[0].get('delay_count', 0)
                perf_score = (sr * 0.1) - (dc * 2)
                score += perf_score
                reasons.append(f"Performance ({perf_score:.1f})")

        if score > best_score:
            best_score = score
            best_candidate = person
            best_reason = f"Score: {score:.1f} | " + ", ".join(reasons)

    return best_candidate, best_reason


# --- UI SETUP & NAVIGATION ---
st.set_page_config(page_title="Logistics AI Planner", layout="wide")
st.title("🚛 Smart Logistics Route & Vacation Planner")
if FIREBASE_READY: st.success("🟢 Connected to Firebase Cloud")
else: st.warning("🟡 Running on Local SQLite Database (firebase-key.json not found)")

menu = ["1. AI Route Planner", "2. Database Management", "3. Past Experience Builder", "4. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)

RESTRICTION_OPTIONS = ["None", "Consumer Only", "2-8 Cars Only", "Sharjah", "Dubai", "Ajman", "Fujairah", "Ras Al Khaimah", "Abu Dhabi", "Al Ain"]


# ==========================================
# SCREEN 1: AI ROUTE PLANNER
# ==========================================
if choice == "1. AI Route Planner":
    st.header("🧠 AI Route Generation (Scoring System)")
    
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=date.today())
    rot_type = col2.radio("Who is rotating?", ["Drivers", "Helpers"])
    
    if st.button("Generate Smart AI Route Plan", type="primary"):
        with st.spinner("Calculating optimal routes based on experience, penalties, and performance..."):
            drivers = load_table('drivers')
            helpers = load_table('helpers')
            areas = load_table('areas')
            vehicles = load_table('vehicles')
            history = load_table('history')
            vacations = load_table('vacations')
            active_routes = load_table('active_routes')
            try: perf_df = load_table('performance')
            except: perf_df = pd.DataFrame()
            
            fallback_areas = ["2-8 Cars", "Urgent/Gov Order", "Pickup", "Substitute", "Second Trip", "Novo Sample"]
            route_targets = areas.to_dict('records') + [{"code": "N/A", "name": fb} for fb in fallback_areas]
            
            route_plan, report_log = [], []
            used_drivers, used_helpers, used_vehicles = set(), set(), set()
            order_counter = 1

            for area in route_targets:
                area_name = area['name']
                needs_helper = not any(kw in area_name.lower() for kw in ["2-8", "urgent", "gov", "substitute"])
                prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                
                a_d_code, a_d_name, a_h_code, a_h_name, a_v_num, log_reason = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "System Fallback"

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

                if not needs_helper: a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"

                # Assign Vehicle
                if a_d_code != "UNASSIGNED":
                    d_type = drivers[drivers['code'] == a_d_code]['veh_type'].values[0] if not drivers[drivers['code'] == a_d_code].empty else "VAN"
                    tvt = "PICK-UP" if "Pickup" in area_name else ("BUS" if "2-8" in area_name else d_type)
                    avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['type'] == tvt)]
                    if avail_v.empty: avail_v = vehicles[~vehicles['number'].isin(used_vehicles)]
                    if not avail_v.empty:
                        a_v_num = avail_v.iloc[0]['number']
                        used_vehicles.add(a_v_num)

                route_plan.append({"Order Number": order_counter, "Area Code": area['code'], "Area Full Name": area_name, "Driver Code": a_d_code, "Driver Name": a_d_name, "Helper Code": a_h_code, "Helper Name": a_h_name, "Vehicle Number": a_v_num})
                report_log.append({"Area": area_name, "Driver": a_d_name, "Helper": a_h_name, "AI Reason": log_reason})
                order_counter += 1

            st.session_state.generated_plan = route_plan
            st.session_state.generated_report = report_log
            st.session_state.plan_date = month_target

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
        
        if col_app.button("✅ Approve Plan & Save 3-Month Exp/Performance", type="primary"):
            run_query("DELETE FROM active_routes", table_name="active_routes", action="DELETE") 
            p_s = st.session_state.plan_date.strftime("%Y-%m-%d")
            p_e = (st.session_state.plan_date + timedelta(days=90)).strftime("%Y-%m-%d")
            
            for r in st.session_state.generated_plan:
                q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                run_query(q_ar, (r['Order Number'], r['Area Code'], r['Area Full Name'], r['Driver Code'], r['Driver Name'], r['Helper Code'], r['Helper Name'], r['Vehicle Number']), table_name="active_routes", action="INSERT", data=r)
                
                for code, name, ptype in [(r['Driver Code'], r['Driver Name'], "Driver"), (r['Helper Code'], r['Helper Name'], "Helper")]:
                    if code not in ["UNASSIGNED", "N/A"]:
                        run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)", (ptype, code, name, r['Area Full Name'], p_s, p_e), table_name="history", action="INSERT", data={"person_type":ptype, "person_code":code, "person_name":name, "area":r['Area Full Name'], "date":p_s, "end_date":p_e})
                        run_query("INSERT INTO performance (person_code, area, success_rate, delay_count) VALUES (?, ?, ?, ?)", (code, r['Area Full Name'], 100.0, 0), table_name="performance", action="INSERT", data={"person_code":code, "area":r['Area Full Name'], "success_rate":100.0, "delay_count":0})
            
            st.success("Saved perfectly! Experience and performance tracking updated.")
            del st.session_state.generated_plan


# ==========================================
# SCREEN 2: DATABASE MANAGEMENT (All Original Tabs Restored + Excel Tab)
# ==========================================
elif choice == "2. Database Management":
    st.header("🗄️ Manage Database")
    
    areas_df = load_table('areas')
    area_list = ["None"] + (areas_df['name'].tolist() if not areas_df.empty else [])
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles", "📥 Bulk Excel Sync"])

    # DRIVERS TAB (RESTORED)
    with tab1:
        st.subheader("📋 Full Drivers List")
        drivers_df = load_table('drivers')
        st.dataframe(drivers_df, use_container_width=True, height=250)
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Driver")
            d_name = st.text_input("Name")
            d_code = st.text_input("Code")
            d_type = st.selectbox("Vehicle Type", ["VAN", "PICK-UP", "BUS"])
            d_restr = st.selectbox("Restriction/Permit", RESTRICTION_OPTIONS)
            d_anchor = st.selectbox("Anchor Area", area_list, key="d_add_anchor")
            if st.button("Add New Driver", use_container_width=True):
                run_query("INSERT INTO drivers (name, code, veh_type, sector, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?)", 
                          (d_name, d_code, d_type, "Pharma", d_restr, d_anchor), table_name="drivers", action="INSERT", data={"name":d_name, "code":d_code, "veh_type":d_type, "sector":"Pharma", "restriction":d_restr, "anchor_area":d_anchor})
                st.success("Added!")
                st.rerun()

        with c_edit:
            st.subheader("✏️ Edit / Delete Driver")
            sel_d_code = st.selectbox("Select Driver to Edit/Delete", drivers_df['code'].tolist() if not drivers_df.empty else [])
            if sel_d_code:
                d_data = drivers_df[drivers_df['code'] == sel_d_code].iloc[0]
                e_name = st.text_input("Edit Name", d_data['name'])
                e_type = st.selectbox("Edit Veh Type", ["VAN", "PICK-UP", "BUS"], index=["VAN", "PICK-UP", "BUS"].index(d_data['veh_type']) if d_data['veh_type'] in ["VAN", "PICK-UP", "BUS"] else 0)
                r_idx = RESTRICTION_OPTIONS.index(d_data['restriction']) if d_data['restriction'] in RESTRICTION_OPTIONS else 0
                e_restr = st.selectbox("Edit Restriction", RESTRICTION_OPTIONS, index=r_idx)
                a_idx = area_list.index(d_data['anchor_area']) if d_data['anchor_area'] in area_list else 0
                e_anchor = st.selectbox("Edit Anchor", area_list, index=a_idx, key="d_edit_anchor")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Driver", use_container_width=True):
                    run_query("UPDATE drivers SET name=?, veh_type=?, restriction=?, anchor_area=? WHERE code=?", (e_name, e_type, e_restr, e_anchor, sel_d_code), table_name="drivers", action="UPDATE", doc_id=d_data['id'], data={"name":e_name, "veh_type":e_type, "restriction":e_restr, "anchor_area":e_anchor})
                    st.success("Updated!")
                    st.rerun()
                if c_del.button("🗑️ Delete Driver", use_container_width=True):
                    run_query("DELETE FROM drivers WHERE code=?", (sel_d_code,), table_name="drivers", action="DELETE", doc_id=d_data['id'])
                    st.warning("Deleted!")
                    st.rerun()

    # HELPERS TAB (RESTORED)
    with tab2:
        st.subheader("📋 Full Helpers List")
        helpers_df = load_table('helpers')
        st.dataframe(helpers_df, use_container_width=True, height=250)
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Helper")
            h_name = st.text_input("Helper Name")
            h_code = st.text_input("Helper Code")
            h_restr = st.selectbox("Restriction", RESTRICTION_OPTIONS, key="h_restr_add")
            h_anchor = st.selectbox("Anchor Area", area_list, key="h_anchor_add")
            if st.button("Add New Helper", use_container_width=True):
                run_query("INSERT INTO helpers (name, code, restriction, anchor_area) VALUES (?, ?, ?, ?)", (h_name, h_code, h_restr, h_anchor), table_name="helpers", action="INSERT", data={"name":h_name, "code":h_code, "restriction":h_restr, "anchor_area":h_anchor})
                st.success("Added!")
                st.rerun()
        with c_edit:
            st.subheader("✏️ Edit / Delete Helper")
            sel_h_code = st.selectbox("Select Helper to Edit/Delete", helpers_df['code'].tolist() if not helpers_df.empty else [])
            if sel_h_code:
                h_data = helpers_df[helpers_df['code'] == sel_h_code].iloc[0]
                e_hname = st.text_input("Edit Name", h_data['name'], key="eh_name")
                hr_idx = RESTRICTION_OPTIONS.index(h_data['restriction']) if h_data['restriction'] in RESTRICTION_OPTIONS else 0
                e_hrestr = st.selectbox("Edit Restriction", RESTRICTION_OPTIONS, index=hr_idx, key="eh_restr")
                ha_idx = area_list.index(h_data['anchor_area']) if h_data['anchor_area'] in area_list else 0
                e_hanchor = st.selectbox("Edit Anchor", area_list, index=ha_idx, key="eh_anchor")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Helper", use_container_width=True):
                    run_query("UPDATE helpers SET name=?, restriction=?, anchor_area=? WHERE code=?", (e_hname, e_hrestr, e_hanchor, sel_h_code), table_name="helpers", action="UPDATE", doc_id=h_data['id'], data={"name":e_hname, "restriction":e_hrestr, "anchor_area":e_hanchor})
                    st.success("Updated!")
                    st.rerun()
                if c_del.button("🗑️ Delete Helper", use_container_width=True):
                    run_query("DELETE FROM helpers WHERE code=?", (sel_h_code,), table_name="helpers", action="DELETE", doc_id=h_data['id'])
                    st.warning("Deleted!")
                    st.rerun()

    # AREAS TAB (RESTORED)
    with tab3:
        st.subheader("📋 Full Areas List")
        a_df = load_table('areas')
        st.dataframe(a_df, use_container_width=True, height=250)
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Area")
            a_name = st.text_input("Area Full Name")
            a_code = st.text_input("Area Code")
            if st.button("Add New Area", use_container_width=True):
                run_query("INSERT INTO areas (name, code) VALUES (?, ?)", (a_name, a_code), table_name="areas", action="INSERT", data={"name":a_name, "code":a_code})
                st.success("Added!")
                st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Area")
            sel_a = st.selectbox("Select Area to Delete", a_df['name'].tolist() if not a_df.empty else [])
            if st.button("Delete Selected Area", use_container_width=True) and sel_a:
                a_id = a_df[a_df['name'] == sel_a].iloc[0]['id']
                run_query("DELETE FROM areas WHERE name=?", (sel_a,), table_name="areas", action="DELETE", doc_id=a_id)
                st.warning("Deleted!")
                st.rerun()

    # VEHICLES TAB (RESTORED)
    with tab4:
        st.subheader("📋 Full Vehicles List")
        v_df = load_table('vehicles')
        st.dataframe(v_df, use_container_width=True, height=250)
        st.divider()
        c_add, c_edit = st.columns(2)
        with c_add:
            st.subheader("➕ Add Vehicle")
            v_num = st.text_input("Vehicle Number")
            v_type = st.selectbox("Type", ["VAN", "PICK-UP", "BUS"])
            if st.button("Add New Vehicle", use_container_width=True):
                run_query("INSERT INTO vehicles (number, type) VALUES (?, ?)", (v_num, v_type), table_name="vehicles", action="INSERT", data={"number":v_num, "type":v_type})
                st.success("Added!")
                st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Vehicle")
            sel_v = st.selectbox("Select Vehicle to Delete", v_df['number'].tolist() if not v_df.empty else [])
            if st.button("Delete Selected Vehicle", use_container_width=True) and sel_v:
                v_id = v_df[v_df['number'] == sel_v].iloc[0]['id']
                run_query("DELETE FROM vehicles WHERE number=?", (sel_v,), table_name="vehicles", action="DELETE", doc_id=v_id)
                st.warning("Deleted!")
                st.rerun()
                
    # NEW BULK EXCEL SYNC TAB
    with tab5:
        st.subheader("📥 Export / 📤 Import Database")
        st.write("You can download the full database, make changes in Excel, and upload it back here.")
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            load_table('drivers').to_excel(writer, sheet_name='drivers', index=False)
            load_table('helpers').to_excel(writer, sheet_name='helpers', index=False)
            load_table('areas').to_excel(writer, sheet_name='areas', index=False)
            load_table('vehicles').to_excel(writer, sheet_name='vehicles', index=False)
            load_table('history').to_excel(writer, sheet_name='history', index=False)
            load_table('vacations').to_excel(writer, sheet_name='vacations', index=False)
        output.seek(0)
        st.download_button("📥 Download Master Database (Excel)", data=output, file_name="Master_Database.xlsx", type="primary")
        
        st.divider()
        uploaded_file = st.file_uploader("Upload your modified Excel to Sync", type=['xlsx'])
        if uploaded_file and st.button("Sync Data to System", type="primary"):
            xls = pd.ExcelFile(uploaded_file)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                if not FIREBASE_READY: run_query(f"DELETE FROM {sheet}")
                for _, row in df.iterrows():
                    data_dict = row.to_dict()
                    if not FIREBASE_READY:
                        cols, vals = ', '.join(row.index), tuple(row.values)
                        qmarks = ', '.join(['?'] * len(row))
                        run_query(f"INSERT INTO {sheet} ({cols}) VALUES ({qmarks})", vals)
                    else:
                        run_query(None, table_name=sheet, action="INSERT", data=data_dict)
            st.success("Database fully synchronized from Excel!")


# ==========================================
# SCREEN 3: PAST EXPERIENCE BUILDER (RESTORED)
# ==========================================
elif choice == "3. Past Experience Builder":
    st.header("🕰️ Manage Past Experience")
    
    st.subheader("📋 Current Experience Log")
    history_df = load_table('history')
    st.dataframe(history_df.sort_values(by="id", ascending=False) if not history_df.empty else history_df, use_container_width=True, height=250)
    st.divider()
    
    c_add, c_edit = st.columns(2)
    
    areas_df = load_table('areas')
    area_list = areas_df['name'].tolist() if not areas_df.empty else []
    
    with c_add:
        st.subheader("➕ Add Experience")
        p_type = st.selectbox("Role", ["Driver", "Helper"])
        df_names = load_table('drivers') if p_type == "Driver" else load_table('helpers')
        person_list = [f"{row['code']} - {row['name']}" for _, row in df_names.iterrows()] if not df_names.empty else []
        
        p_person = st.selectbox("Select Person", person_list)
        p_area = st.selectbox("Area Experienced In", area_list)
        
        d1, d2 = st.columns(2)
        p_start_date = d1.date_input("From Date (Start)")
        p_end_date = d2.date_input("To Date (End)")
        
        if st.button("➕ Add Past Experience", use_container_width=True):
            if p_start_date > p_end_date: st.error("Start Date cannot be after End Date.")
            else:
                p_code, p_name = p_person.split(" - ")[0], p_person.split(" - ")[1]
                run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)",
                          (p_type, p_code, p_name, p_area, p_start_date.strftime("%Y-%m-%d"), p_end_date.strftime("%Y-%m-%d")), 
                          table_name="history", action="INSERT", data={"person_type":p_type, "person_code":p_code, "person_name":p_name, "area":p_area, "date":p_start_date.strftime("%Y-%m-%d"), "end_date":p_end_date.strftime("%Y-%m-%d")})
                st.success("Experience logically calculated and added!")
                st.rerun()

    with c_edit:
        st.subheader("✏️ Edit / Delete Experience")
        if not history_df.empty:
            history_list = [f"{row['id']} - {row['person_name']} ({row['area']})" for _, row in history_df.iterrows()]
            sel_hist_str = st.selectbox("Select Record to Edit/Delete", history_list)
            
            if sel_hist_str:
                hist_id = sel_hist_str.split(" - ")[0]
                hist_data = history_df[history_df['id'].astype(str) == hist_id].iloc[0]
                
                a_idx = area_list.index(hist_data['area']) if hist_data['area'] in area_list else 0
                e_area = st.selectbox("Edit Area", area_list, index=a_idx, key=f"e_area_{hist_id}")
                
                ed1, ed2 = st.columns(2)
                e_start_val, e_end_val = safe_parse_date(hist_data['date']), safe_parse_date(hist_data['end_date'])
                new_start = ed1.date_input("Edit Start Date", value=e_start_val, key=f"es_{hist_id}")
                new_end = ed2.date_input("Edit End Date", value=e_end_val, key=f"ee_{hist_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Experience", use_container_width=True):
                    run_query("UPDATE history SET area=?, date=?, end_date=? WHERE id=?", (e_area, new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d"), hist_id), table_name="history", action="UPDATE", doc_id=hist_id, data={"area":e_area, "date":new_start.strftime("%Y-%m-%d"), "end_date":new_end.strftime("%Y-%m-%d")})
                    st.success("Updated Successfully!")
                    st.rerun()
                    
                if c_del.button("🗑️ Delete Experience", use_container_width=True):
                    run_query("DELETE FROM history WHERE id=?", (hist_id,), table_name="history", action="DELETE", doc_id=hist_id)
                    st.warning("Deleted!")
                    st.rerun()


# ==========================================
# SCREEN 4: VACATION SCHEDULE (RESTORED + AI)
# ==========================================
elif choice == "4. Vacation Schedule":
    st.header("🌴 Manage Vacation Schedule & AI Tracking")
    
    vacs_df = load_table('vacations')
    
    # NEW SMART SUGGESTIONS FEATURE
    with st.expander("🤖 Show AI Vacation Suggestions (Overdue Personnel)"):
        due_list = []
        for df, role in [(load_table('drivers'), "Driver"), (load_table('helpers'), "Helper")]:
            for _, p in df.iterrows():
                last_vac = vacs_df[vacs_df['person_name'] == p['name']]
                if last_vac.empty: due_list.append({"Name": p['name'], "Role": role, "Status": "Never taken"})
                else:
                    lv_date = safe_parse_date(last_vac.iloc[-1]['end_date'])
                    if (date.today() - lv_date).days > 365:
                        due_list.append({"Name": p['name'], "Role": role, "Status": f"Last was {lv_date}"})
        st.dataframe(pd.DataFrame(due_list), use_container_width=True)

    st.subheader("📋 Current Vacations List")
    st.dataframe(vacs_df, use_container_width=True, height=250)
    st.divider()
    
    c_add, c_edit = st.columns(2)
    
    with c_add:
        st.subheader("➕ Add Gapless Vacation")
        v_type = st.selectbox("Role", ["Driver", "Helper"])
        df_names = load_table('drivers') if v_type == "Driver" else load_table('helpers')
        name_list = df_names['name'].tolist() if not df_names.empty else []
        v_name = st.selectbox("Name", name_list)
        
        d1, d2 = st.columns(2)
        v_start = d1.date_input("Start Date (Leave)")
        v_end = d2.date_input("End Date (Replacement comes exactly on this day)", value=date.today() + timedelta(days=30))
        
        if st.button("➕ Add Vacation", use_container_width=True):
            if v_start > v_end: st.error("Start Date cannot be after End Date.")
            else:
                overlapping = 0
                for _, row in vacs_df.iterrows():
                    if row['person_type'] == v_type:
                        if max(v_start, safe_parse_date(row['start_date'])) <= min(v_end, safe_parse_date(row['end_date'])):
                            overlapping += 1
                if overlapping >= 3: st.error(f"Cannot add! Already {overlapping} {v_type}s on vacation.")
                else:
                    run_query("INSERT INTO vacations (person_type, person_name, start_date, end_date) VALUES (?, ?, ?, ?)", (v_type, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")), table_name="vacations", action="INSERT", data={"person_type":v_type, "person_name":v_name, "start_date":v_start.strftime("%Y-%m-%d"), "end_date":v_end.strftime("%Y-%m-%d")})
                    st.success("Vacation scheduled successfully! System will auto-block them from route selection.")
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
                new_vstart = ed1.date_input("Edit Start Date", value=e_vstart_val, key=f"vs_{vac_id}")
                new_vend = ed2.date_input("Edit End Date", value=e_vend_val, key=f"ve_{vac_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Vacation", use_container_width=True):
                    if new_vstart > new_vend: st.error("Start Date cannot be after End Date.")
                    else:
                        run_query("UPDATE vacations SET start_date=?, end_date=? WHERE id=?", (new_vstart.strftime("%Y-%m-%d"), new_vend.strftime("%Y-%m-%d"), vac_id), table_name="vacations", action="UPDATE", doc_id=vac_id, data={"start_date":new_vstart.strftime("%Y-%m-%d"), "end_date":new_vend.strftime("%Y-%m-%d")})
                        st.success("Updated Successfully!")
                        st.rerun()
                        
                if c_del.button("🗑️ Delete Vacation", use_container_width=True):
                    run_query("DELETE FROM vacations WHERE id=?", (vac_id,), table_name="vacations", action="DELETE", doc_id=vac_id)
                    st.warning("Deleted!")
                    st.rerun()
