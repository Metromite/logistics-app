import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import io
import os
import textwrap
import firebase_admin
from firebase_admin import credentials, firestore

# --- UI CONFIGURATION (Native Streamlit Light/Dark Mode) ---
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

# Sleek Native Dot Indicator for Connection
if FIREBASE_READY:
    st.sidebar.markdown("<div style='text-align: center; font-size: 20px; margin-top: -15px;' title='Connected to Secure Cloud'>🟢</div>", unsafe_allow_html=True)
else:
    st.sidebar.markdown("<div style='text-align: center; font-size: 20px; margin-top: -15px;' title='Local Database Mode'>🔴</div>", unsafe_allow_html=True)

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
    
    # Migrations for new features (Health Cards, Vehicle Anchors)
    for query in [
        "ALTER TABLE drivers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE drivers ADD COLUMN health_card TEXT DEFAULT 'No'",
        "ALTER TABLE helpers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE history ADD COLUMN end_date TEXT",
        "ALTER TABLE history ADD COLUMN person_code TEXT DEFAULT 'UNKNOWN'",
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
        # Ensure new columns exist even on fresh pull
        if table_name == 'drivers' and 'health_card' not in df.columns and not df.empty: df['health_card'] = 'No'
        if table_name == 'vehicles' and 'anchor_area' not in df.columns and not df.empty: df['anchor_area'] = 'None'
        return df
    else:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def run_query(query, params=(), table_name=None, action=None, doc_id=None, data=None):
    clear_cache()
    if FIREBASE_READY and table_name and action:
        if action == "INSERT": 
            db_fs.collection(table_name).add(data)
        elif action == "UPDATE": 
            if doc_id: db_fs.collection(table_name).document(str(doc_id)).update(data)
        elif action == "DELETE_DOC": 
            if doc_id: db_fs.collection(table_name).document(str(doc_id)).delete()
        elif action == "CLEAR_TABLE":
            docs = db_fs.collection(table_name).stream()
            for doc in docs: doc.reference.delete()
    else:
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()

# --- EXCEL UTILITY WITH S/N COLUMN ---
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


# --- SMART SCORING LOGIC (NO MORE "UNASSIGNED") ---
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
    if vacations_df.empty: return False
    for _, row in vacations_df.iterrows():
        if row['person_name'] == person_name:
            if safe_parse_date(row['start_date']) <= target_date <= safe_parse_date(row['end_date']):
                return True
    return False

def select_best_candidate(candidates_df, area_name, target_date, history_df, vacations_df, role="Driver"):
    best_candidate, best_score, best_reason = None, -99999, "No valid candidates left"
    
    # Determine requirements based on Area name keywords
    req_sector = "Consumer" if "consumer" in area_name.lower() or "dxb" in area_name.lower() else "Pharma"
    req_veh = "BUS" if "2-8" in area_name or "govt" in area_name.lower() else ("PICK-UP" if "pick up" in area_name.lower() or "pick-up" in area_name.lower() else "VAN")

    for _, person in candidates_df.iterrows():
        # Hard Filter: Vacation
        if is_on_vacation(vacations_df, person['name'], target_date): continue
        
        score = 0
        reasons = []

        # Point System for Drivers
        if role == "Driver":
            # Health Card Logic
            if req_sector == "Consumer":
                if person.get('health_card') == 'Yes':
                    score += 500
                    reasons.append("Health Card Match (+500)")
                else:
                    score -= 800
                    reasons.append("No Health Card (-800)")
            elif person.get('health_card') == 'Yes':
                score -= 300
                reasons.append("Waste of Health Card here (-300)")

            # Vehicle Logic
            if person.get('veh_type') == req_veh:
                score += 300
                reasons.append(f"Vehicle Match: {req_veh} (+300)")
            else:
                score -= 500
                reasons.append(f"Wrong Vehicle Type (-500)")

            # Sector Logic
            if person.get('sector') == req_sector:
                score += 100
                reasons.append(f"Sector Match: {req_sector} (+100)")
            else:
                score -= 200
                reasons.append(f"Wrong Sector (-200)")

        # Common Point System (Driver & Helper)
        # Anchor Logic
        anchor = person.get('anchor_area')
        if anchor == area_name:
            score += 1000
            reasons.append("Anchored specifically to this Area (+1000)")
        elif anchor not in ["None", "", None] and type(anchor) == str:
            score -= 900
            reasons.append(f"Anchored elsewhere (-900)")

        # Experience Logic
        exp_months = get_experience_months(history_df, person['code'], area_name)
        if exp_months == 0:
            score += 150
            reasons.append("Fresh Rotation (+150)")
        else:
            score += int(exp_months * 10)
            reasons.append(f"Has {exp_months:.1f}m Exp (+{int(exp_months*10)})")
            
        # Recent Assignment Penalty Logic
        last_date = get_last_assignment(history_df, person['code'], area_name)
        if last_date:
            months_since = (target_date - last_date).days / 30.0
            if months_since < 6:
                penalty = int((6 - months_since) * 30)
                score -= penalty
                reasons.append(f"Recent Penalty (-{penalty})")

        # Select Highest Score
        if score > best_score:
            best_score = score
            best_candidate = person
            best_reason = f"Score [{score}]: " + ", ".join(reasons)

    return best_candidate, best_reason


# --- NAVIGATION ---
st.title("🚛 Smart Logistics Route Planner")

menu = ["1. AI Route Planner", "2. Database Management", "3. Past Experience Builder", "4. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)
RESTRICTION_OPTIONS = ["None", "Consumer Only", "2-8 Cars Only", "Sharjah", "Dubai", "Ajman", "Fujairah", "Ras Al Khaimah", "Abu Dhabi", "Al Ain"]


# ==========================================
# SCREEN 1: AI ROUTE PLANNER
# ==========================================
if choice == "1. AI Route Planner":
    st.header("🧠 AI Route Generation")
    
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=date.today())
    rot_type = col2.radio("Who is rotating this month?", ["Drivers", "Helpers"])
    
    st.info(f"💡 Note: Because you are rotating **{rot_type}**, the system will permanently KEEP the currently assigned **{'Helpers' if rot_type == 'Drivers' else 'Drivers'}** and Vehicles attached to their routes.")
    
    if st.button("Generate Smart AI Route Plan", type="primary"):
        with st.spinner("Calculating optimal routes based on experience, vacations, and penalties..."):
            drivers = load_table('drivers')
            helpers = load_table('helpers')
            areas = load_table('areas')
            vehicles = load_table('vehicles')
            history = load_table('history')
            vacations = load_table('vacations')
            active_routes = load_table('active_routes')
            
            # Organize Target Template based on City Pharmacy Layout
            route_targets = areas.to_dict('records')
            
            route_plan, report_log = [], []
            used_drivers, used_helpers, used_vehicles = set(), set(), set()
            order_counter = 1

            for area in route_targets:
                area_name = area['name']
                # Helper Rules based on Layout
                needs_helper = True
                if any(kw in area_name.lower() for kw in ["2-8", "urgent", "govt", "fleet", "substitute"]):
                    needs_helper = False
                    
                prev_assignment = active_routes[active_routes['area_name'] == area_name] if not active_routes.empty else pd.DataFrame()
                
                a_d_code, a_d_name, a_h_code, a_h_name, a_v_num, log_reason = "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "UNASSIGNED", "No pool left"

                if rot_type == "Drivers":
                    # Keep existing Helper & Vehicle if they exist
                    if needs_helper and not prev_assignment.empty and prev_assignment.iloc[0]['helper_code'] != "N/A":
                        a_h_code, a_h_name = prev_assignment.iloc[0]['helper_code'], prev_assignment.iloc[0]['helper_name']
                        used_helpers.add(a_h_code)
                    
                    avail_d = drivers[~drivers['code'].isin(used_drivers)]
                    best_d, d_reason = select_best_candidate(avail_d, area_name, month_target, history, vacations, role="Driver")
                    if best_d is not None:
                        a_d_code, a_d_name, log_reason = best_d['code'], best_d['name'], f"DRIVER -> {d_reason}"
                        used_drivers.add(a_d_code)
                else:
                    # Keep existing Driver & Vehicle if they exist
                    if not prev_assignment.empty:
                        a_d_code, a_d_name = prev_assignment.iloc[0]['driver_code'], prev_assignment.iloc[0]['driver_name']
                        a_v_num = prev_assignment.iloc[0]['veh_num']
                        used_drivers.add(a_d_code)
                        used_vehicles.add(a_v_num)
                        
                    if needs_helper:
                        avail_h = helpers[~helpers['code'].isin(used_helpers)]
                        best_h, h_reason = select_best_candidate(avail_h, area_name, month_target, history, vacations, role="Helper")
                        if best_h is not None:
                            a_h_code, a_h_name, log_reason = best_h['code'], best_h['name'], f"HELPER -> {h_reason}"
                            used_helpers.add(a_h_code)

                if not needs_helper: a_h_code, a_h_name = "N/A", "NO HELPER REQUIRED"

                # Assign Vehicle if not kept from previous driver
                if a_d_code != "UNASSIGNED" and a_v_num == "UNASSIGNED":
                    d_type = drivers[drivers['code'] == a_d_code]['veh_type'].values[0] if not drivers[drivers['code'] == a_d_code].empty else "VAN"
                    tvt = "BUS" if "2-8" in area_name else ("PICK-UP" if "pick up" in area_name.lower() else d_type)
                    
                    # 1. Try to find a vehicle explicitly anchored to this area
                    avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['anchor_area'] == area_name)]
                    # 2. Try to find matching type
                    if avail_v.empty: avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['type'] == tvt) & (vehicles['anchor_area'] == "None")]
                    # 3. Fallback
                    if avail_v.empty: avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['anchor_area'] == "None")]
                    
                    if not avail_v.empty:
                        a_v_num = avail_v.iloc[0]['number']
                        used_vehicles.add(a_v_num)

                route_plan.append({"Area Code": area['code'], "Area Full Name": area_name, "Driver Code": a_d_code, "Driver Name": a_d_name, "Helper Code": a_h_code, "Helper Name": a_h_name, "Vehicle Number": a_v_num})
                report_log.append({"Area": area_name, "Driver": a_d_name, "Helper": a_h_name, "AI Logic Reason": log_reason})

            st.session_state.generated_plan = route_plan
            st.session_state.generated_report = report_log
            st.session_state.plan_date = month_target

    if 'generated_plan' in st.session_state:
        df_r = pd.DataFrame(st.session_state.generated_plan)
        df_log = pd.DataFrame(st.session_state.generated_report)
        
        df_r.insert(0, 'S/N', range(1, 1 + len(df_r)))
        df_log.insert(0, 'S/N', range(1, 1 + len(df_log)))
        
        st.dataframe(df_r, use_container_width=True, hide_index=True)
        st.dataframe(df_log, use_container_width=True, hide_index=True)
        
        output = generate_excel_with_sn([df_r, df_log], ['Route Plan', 'AI Logic Report'])
        
        col_down, col_app = st.columns(2)
        col_down.download_button("📥 Download Excel Route Plan", data=output, file_name=f"Smart_Plan_{st.session_state.plan_date}.xlsx")
        
        if col_app.button("✅ Approve Plan & Save Experiences", type="primary"):
            run_query("DELETE FROM active_routes", table_name="active_routes", action="CLEAR_TABLE") 
            p_s = st.session_state.plan_date.strftime("%Y-%m-%d")
            p_e = (st.session_state.plan_date + timedelta(days=90)).strftime("%Y-%m-%d")
            
            for index, r in df_r.iterrows():
                q_ar = "INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                run_query(q_ar, (r['S/N'], r['Area Code'], r['Area Full Name'], r['Driver Code'], r['Driver Name'], r['Helper Code'], r['Helper Name'], r['Vehicle Number']), table_name="active_routes", action="INSERT", data={"order_num":r['S/N'], "area_code":r['Area Code'], "area_name":r['Area Full Name'], "driver_code":r['Driver Code'], "driver_name":r['Driver Name'], "helper_code":r['Helper Code'], "helper_name":r['Helper Name'], "veh_num":r['Vehicle Number']})
                
                for code, name, ptype in [(r['Driver Code'], r['Driver Name'], "Driver"), (r['Helper Code'], r['Helper Name'], "Helper")]:
                    if code not in ["UNASSIGNED", "N/A"]:
                        # Anti-duplicate check
                        hist_chk = load_table('history')
                        if hist_chk.empty or len(hist_chk[(hist_chk['person_code']==code) & (hist_chk['area']==r['Area Full Name']) & (hist_chk['date']==p_s)]) == 0:
                            run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)", (ptype, code, name, r['Area Full Name'], p_s, p_e), table_name="history", action="INSERT", data={"person_type":ptype, "person_code":code, "person_name":name, "area":r['Area Full Name'], "date":p_s, "end_date":p_e})
            
            st.success("Plan Approved! System will remember these assignments for next month.")
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
        disp_df = drivers_df.drop(columns=['id'], errors='ignore').copy()
        if not disp_df.empty: disp_df.insert(0, 'S/N', range(1, 1 + len(disp_df)))
        st.dataframe(disp_df, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            d_name = st.text_input("Name")
            d_code = st.text_input("Code")
            col_t, col_s, col_h = st.columns(3)
            d_type = col_t.selectbox("Vehicle Type", ["VAN", "PICK-UP", "BUS"])
            d_sec = col_s.selectbox("Sector", ["Pharma", "Consumer"])
            d_health = col_h.selectbox("Health Card?", ["No", "Yes"])
            d_restr = st.selectbox("Restriction/Permit", RESTRICTION_OPTIONS)
            d_anchor = st.selectbox("Anchor Area", area_list, key="d_add_anchor")
            if st.button("➕ Add Driver", use_container_width=True):
                run_query("INSERT INTO drivers (name, code, veh_type, sector, restriction, anchor_area, health_card) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                          (d_name, d_code, d_type, d_sec, d_restr, d_anchor, d_health), table_name="drivers", action="INSERT", data={"name":d_name, "code":d_code, "veh_type":d_type, "sector":d_sec, "restriction":d_restr, "anchor_area":d_anchor, "health_card":d_health})
                st.rerun()

        with c_edit:
            sel_d_code = st.selectbox("Select Driver to Edit/Delete", drivers_df['code'].tolist() if not drivers_df.empty else [])
            if sel_d_code:
                d_data = drivers_df[drivers_df['code'] == sel_d_code].iloc[0]
                e_name = st.text_input("Edit Name", d_data['name'])
                ct, cs, ch = st.columns(3)
                e_type = ct.selectbox("Edit Veh Type", ["VAN", "PICK-UP", "BUS"], index=["VAN", "PICK-UP", "BUS"].index(d_data['veh_type']) if d_data['veh_type'] in ["VAN", "PICK-UP", "BUS"] else 0)
                e_sec = cs.selectbox("Edit Sector", ["Pharma", "Consumer"], index=0 if d_data.get('sector') == "Pharma" else 1)
                e_health = ch.selectbox("Edit Health Card", ["No", "Yes"], index=1 if d_data.get('health_card') == "Yes" else 0)
                
                r_idx = RESTRICTION_OPTIONS.index(d_data['restriction']) if d_data['restriction'] in RESTRICTION_OPTIONS else 0
                e_restr = st.selectbox("Edit Restriction", RESTRICTION_OPTIONS, index=r_idx)
                a_idx = area_list.index(d_data['anchor_area']) if d_data['anchor_area'] in area_list else 0
                e_anchor = st.selectbox("Edit Anchor", area_list, index=a_idx, key="d_edit_anchor")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Driver", use_container_width=True):
                    run_query("UPDATE drivers SET name=?, veh_type=?, sector=?, health_card=?, restriction=?, anchor_area=? WHERE code=?", (e_name, e_type, e_sec, e_health, e_restr, e_anchor, sel_d_code), table_name="drivers", action="UPDATE", doc_id=d_data['id'], data={"name":e_name, "veh_type":e_type, "sector":e_sec, "health_card":e_health, "restriction":e_restr, "anchor_area":e_anchor})
                    st.rerun()
                if c_del.button("🗑️ Delete Driver", use_container_width=True):
                    run_query("DELETE FROM drivers WHERE code=?", (sel_d_code,), table_name="drivers", action="DELETE_DOC", doc_id=d_data['id'])
                    st.rerun()

    with tab2:
        st.subheader("📋 Full Helpers List")
        helpers_df = load_table('helpers')
        disp_h = helpers_df.drop(columns=['id'], errors='ignore').copy()
        if not disp_h.empty: disp_h.insert(0, 'S/N', range(1, 1 + len(disp_h)))
        st.dataframe(disp_h, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            h_name = st.text_input("Helper Name")
            h_code = st.text_input("Helper Code")
            h_restr = st.selectbox("Restriction", RESTRICTION_OPTIONS, key="h_restr_add")
            h_anchor = st.selectbox("Anchor Area", area_list, key="h_anchor_add")
            if st.button("➕ Add Helper", use_container_width=True):
                run_query("INSERT INTO helpers (name, code, restriction, anchor_area) VALUES (?, ?, ?, ?)", (h_name, h_code, h_restr, h_anchor), table_name="helpers", action="INSERT", data={"name":h_name, "code":h_code, "restriction":h_restr, "anchor_area":h_anchor})
                st.rerun()
        with c_edit:
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
                    st.rerun()
                if c_del.button("🗑️ Delete Helper", use_container_width=True):
                    run_query("DELETE FROM helpers WHERE code=?", (sel_h_code,), table_name="helpers", action="DELETE_DOC", doc_id=h_data['id'])
                    st.rerun()

    with tab3:
        st.subheader("📋 Full Areas List")
        a_df = load_table('areas')
        disp_a = a_df.drop(columns=['id'], errors='ignore').copy()
        if not disp_a.empty: disp_a.insert(0, 'S/N', range(1, 1 + len(disp_a)))
        st.dataframe(disp_a, use_container_width=True, height=250, hide_index=True)
        
        c_add, c_edit = st.columns(2)
        with c_add:
            a_name = st.text_input("Area Full Name")
            a_code = st.text_input("Area Code")
            if st.button("➕ Add Area", use_container_width=True):
                run_query("INSERT INTO areas (name, code) VALUES (?, ?)", (a_name, a_code), table_name="areas", action="INSERT", data={"name":a_name, "code":a_code})
                st.rerun()
        with c_edit:
            sel_a = st.selectbox("Select Area to Delete", a_df['name'].tolist() if not a_df.empty else [])
            if st.button("🗑️ Delete Selected Area", use_container_width=True) and sel_a:
                a_id = a_df[a_df['name'] == sel_a].iloc[0]['id']
                run_query("DELETE FROM areas WHERE name=?", (sel_a,), table_name="areas", action="DELETE_DOC", doc_id=a_id)
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
            v_type = st.selectbox("Type", ["VAN", "PICK-UP", "BUS"])
            v_anchor = st.selectbox("Anchor to Specific Area", area_list, key="v_anchor")
            if st.button("➕ Add Vehicle", use_container_width=True):
                run_query("INSERT INTO vehicles (number, type, anchor_area) VALUES (?, ?, ?)", (v_num, v_type, v_anchor), table_name="vehicles", action="INSERT", data={"number":v_num, "type":v_type, "anchor_area":v_anchor})
                st.rerun()
        with c_edit:
            sel_v = st.selectbox("Select Vehicle to Edit/Delete", v_df['number'].tolist() if not v_df.empty else [])
            if sel_v:
                v_data = v_df[v_df['number'] == sel_v].iloc[0]
                ev_type = st.selectbox("Edit Type", ["VAN", "PICK-UP", "BUS"], index=["VAN", "PICK-UP", "BUS"].index(v_data['type']) if v_data['type'] in ["VAN", "PICK-UP", "BUS"] else 0)
                va_idx = area_list.index(v_data.get('anchor_area', 'None')) if v_data.get('anchor_area', 'None') in area_list else 0
                ev_anchor = st.selectbox("Edit Anchor Area", area_list, index=va_idx)
                
                cu, cd = st.columns(2)
                if cu.button("💾 Update Veh", use_container_width=True):
                    run_query("UPDATE vehicles SET type=?, anchor_area=? WHERE number=?", (ev_type, ev_anchor, sel_v), table_name="vehicles", action="UPDATE", doc_id=v_data['id'], data={"type":ev_type, "anchor_area":ev_anchor})
                    st.rerun()
                if cd.button("🗑️ Delete Veh", use_container_width=True):
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
        p_person = st.selectbox("Select Person", person_list)
        p_area = st.selectbox("Area Experienced In", area_list)
        d1, d2 = st.columns(2)
        p_start_date = d1.date_input("From Date (Start)")
        p_end_date = d2.date_input("To Date (End)")
        
        if st.button("➕ Add Past Experience", use_container_width=True):
            p_code, p_name = p_person.split(" - ")[0], p_person.split(" - ")[1]
            # Anti-Duplicate Validation
            overlap = history_df[(history_df['person_code']==p_code) & (history_df['area']==p_area) & (history_df['date']==p_start_date.strftime("%Y-%m-%d"))]
            if p_start_date > p_end_date: 
                st.error("Start Date cannot be after End Date.")
            elif not overlap.empty:
                st.error("⚠️ This person already has an experience log for this Area on this exact Start Date!")
            else:
                run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)",
                          (p_type, p_code, p_name, p_area, p_start_date.strftime("%Y-%m-%d"), p_end_date.strftime("%Y-%m-%d")), 
                          table_name="history", action="INSERT", data={"person_type":p_type, "person_code":p_code, "person_name":p_name, "area":p_area, "date":p_start_date.strftime("%Y-%m-%d"), "end_date":p_end_date.strftime("%Y-%m-%d")})
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
                    st.rerun()
                if c_del.button("🗑️ Delete Experience", use_container_width=True):
                    run_query("DELETE FROM history WHERE id=?", (hist_id,), table_name="history", action="DELETE_DOC", doc_id=hist_id)
                    st.rerun()


# ==========================================
# SCREEN 4: VACATION SCHEDULE
# ==========================================
elif choice == "4. Vacation Schedule":
    st.header("🌴 Manage Vacation Schedule")
    vacs_df = load_table('vacations')

    # --- REAL-TIME VACATION DASHBOARD ---
    st.subheader("📊 Active Vacations Overview")
    today = date.today()
    active_vacs = []
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
        v_name = st.selectbox("Name", name_list)
        d1, d2 = st.columns(2)
        v_start = d1.date_input("Start Date (Leave)")
        v_end = d2.date_input("End Date (Return)", value=date.today() + timedelta(days=30))
        
        if st.button("➕ Add Vacation", use_container_width=True):
            # Duplicate Validation
            overlap = vacs_df[(vacs_df['person_name'] == v_name) & (vacs_df['start_date'] == v_start.strftime("%Y-%m-%d"))]
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
                new_vstart = ed1.date_input("Edit Start Date", value=e_vstart_val, key=f"vs_{vac_id}")
                new_vend = ed2.date_input("Edit End Date", value=e_vend_val, key=f"ve_{vac_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Vacation", use_container_width=True):
                    if new_vstart > new_vend: st.error("Start Date cannot be after End Date.")
                    else:
                        run_query("UPDATE vacations SET start_date=?, end_date=? WHERE id=?", (new_vstart.strftime("%Y-%m-%d"), new_vend.strftime("%Y-%m-%d"), vac_id), table_name="vacations", action="UPDATE", doc_id=vac_id, data={"start_date":new_vstart.strftime("%Y-%m-%d"), "end_date":new_vend.strftime("%Y-%m-%d")})
                        st.rerun()
                        
                if c_del.button("🗑️ Delete Vacation", use_container_width=True):
                    run_query("DELETE FROM vacations WHERE id=?", (vac_id,), table_name="vacations", action="DELETE_DOC", doc_id=vac_id)
                    st.rerun()
