import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import io

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

SEED_DRIVERS = [
    ("AD056", "NAUSHAD ALI"), ("AD060", "TINTU JOSEPH"), ("AD054", "VIJU NAIR"), ("AD067", "SREEKANTH"),
    ("AD038", "RAHAMATULLAH"), ("AD063", "MOHAMMED SHEREEF"), ("AD055", "AHAMAD JAN"), ("AD068", "MOHAMED ANSAR"),
    ("AD047", "Michael De Torres"), ("AD066", "ANSARI"), ("AD062", "NIJAVUDEEN"), ("AD065", "SABIR SHAH"),
    ("AD039", "H. SAGUL AMEED"), ("AD053", "Mohamed Shefeeq"), ("AD046", "MUSTHAFA . KA"), ("AD061", "YASAR ARAFATH"),
    ("SH003", "RAHUL RAJENDRAN"), ("AD045", "ESLAM SABRI"), ("AD050", "SHAHID ALI"), ("AD041", "MOHD. MUSTHAFA"),
    ("AD051", "ABDUL JALEEL"), ("AD064", "FAYAZ KHAN"), ("AD057", "SHEKKEER PH"), ("AD021", "CUSTOMER"),
    ("AD004", "ADIL SHAH GULAM"), ("AD006", "ABDUL NAEEM"), ("AD010", "RAHMATH BUNAIRY"), ("AD013", "FAZAL NAEM"),
    ("AD014", "NISAAR AHMED"), ("AD023", "FAISAL"), ("AD025", "ALI AHAMAD"), ("AD026", "NADAR SHAH"),
    ("AD022", "MOHAMMED GHANI"), ("AD029", "ABDUL AZIZ"), ("AD033", "RASHID"), ("D106", "Mohammed Shereef K V"),
    ("D081", "Musthafa Kadangod"), ("D094", "Shuhaib Mullantakath"), ("D083", "Asaf Khan Fazal"), ("SBP1", "SEBA MED PROMOTION"),
    ("D076", "Rahmatullah Mohammed"), ("D067", "Shebin Kabeer"), ("D096", "Ahmad Jan Mughal"), ("SRV", "Service Dept BUH"),
    ("D100", "Adil Ghulam"), ("D104", "Mahammed Ansar"), ("D086", "Abdul Jaleel Nadakkavu"), ("D105", "Fayaz Khan RS"),
    ("D087", "Abdul Naeem"), ("D108", "Ansari Ithayathullah"), ("D103", "Jamseer PV Ibrahim"), ("D080", "Shahid Ali khan"),
    ("D107", "Muneeb Hussain"), ("D085", "Rahul R.P"), ("D095", "Islam Sabri"), ("D089", "Jisam K Saleem"),
    ("SLM", "SALAMA PACKING"), ("D072", "Rahmat Bunairy"), ("D073", "Mohamed Mustafa Kummalil"), ("D063", "Ajmal Ali Akbar"),
    ("D097", "Shekkeer P Hamza"), ("D059", "Jeham Sher AUH"), ("D099", "Muhammed Noushad P"), ("CPC001", "Danish Mohammed"),
    ("BUH002", "Mohamed Khaled"), ("D078", "Mahammad Arif"), ("D075", "Michael DE Torres"), ("D061", "Said Alavy"),
    ("D082", "Rashid Meethal"), ("D088", "Saheer Ali V Z"), ("D079", "Nadar Shah Shah"), ("D102", "Mohammed Nasiruddeen"),
    ("D098", "Muhammed Aslam K"), ("D071", "Fazal Naeem Abdur Rahim"), ("D101", "Tintu V Joseph"), ("D058", "Nazeer Khan AUH"),
    ("D074", "Nisar Ahamed Shah"), ("D090", "Faisal Mahmood"), ("MED1", "Kathleen Grace"), ("D070", "Mohamed Ghani"),
    ("D092", "Mohamed Shefeeq.MP"), ("D093", "Viju Nair"), ("D068", "Ali Ahamed"), ("D049", "Abdul Jabbar"),
    ("D051", "Mohammed Ibrahim"), ("D052", "Noushad Ali"), ("D050", "Abdul Mansoor"), ("D047", "Ahmed Faraj"),
    ("D048", "Moideen Azeez"), ("C001", "Collected By Customer"), ("D010", "Nasar"), ("D011", "Imran Khan"),
    ("D019", "Muhammed Kunji"), ("D023", "Sabir Shah"), ("D024", "Sadiq Shah"), ("D026", "Jahaberudheen"),
    ("D029", "Baderudheen"), ("SLPO", "Delivered by LPO Dept"), ("D033", "Naeem Fazal"), ("D037", "Nijavudeen"),
    ("D038", "Ismail Korokkaran"), ("D035", "Shaul Hameed"), ("D036", "Rashid Baderzaman"), ("D040", "Hussain Mohammed"),
    ("D042", "Gulam Khan Mohammad"), ("D044", "Zainul Abid"), ("D032", "Sayd Mubarak"), ("D034", "Adil Hassan"),
    ("D046", "Azeez Abdulla"), ("D054", "Sameer Zakariyah"), ("SH002", "GULAM KHAN"), ("SH001", "SADIQ SHA"),
    ("SH004", "Zainulabid"), ("AD069", "MOH KUNHI"), ("C002", "OTHERS"), ("D027", "Sultan"), ("D064", "Shabeer Ali A.Rahman"),
    ("D109", "Yousuf Nobi Shakib")
]

SEED_HELPERS = [
    ("AH039", "RABIYA"), ("AH031", "HAJA MOIDEEN"), ("AH036", "SYED MAISAM"), ("AH037", "RASHEED"),
    ("AH038", "AUREL"), ("AH033", "JOSEPH"), ("AH034", "MUBARAK"), ("AH042", "LORLDWINE"), ("AH044", "ISMAIL"),
    ("AH041", "ABBAS"), ("AH040", "AMANULLHA"), ("AH043", "CHARL-VINCE"), ("AH002", "RAFEEQ"), ("AH012", "SAYED ALI"),
    ("AH020", "LATHIF"), ("AH021", "RAMSHEED"), ("AH022", "NAIK"), ("AH025", "FAZAL HADI"), ("AH027", "REJITHA"),
    ("AH030", "SHAHID"), ("AH40", "RAJESH"), ("H117", "Mahammad Nawaz PM"), ("H115", "Omar AlSaeed"),
    ("H111", "Muhammed Saif VS"), ("H130", "Javed Akhtar"), ("H077", "Ihab Mohamad Kaddour"), ("H107", "Abdul Razak"),
    ("H123", "Sheik Abdul Malik"), ("H097", "Mahdi"), ("H125", "Kadar Moideen"), ("H082", "Hassan Mohammed"),
    ("H083", "Shakeer"), ("H091", "Mohammed Sameem"), ("H095", "Noushad Ali"), ("H104", "Mohammed Shakeer"),
    ("H069", "Muhammed Shajahan"), ("H070", "A. Harshad"), ("H075", "Mohamed Hassan"), ("H116", "Munawir P Kabeer"),
    ("H119", "Muhammed Janees P"), ("H118", "Muhammed Shamil P"), ("H127", "Rafsal Rafeek"), ("H112", "Abbas Mohamed MA"),
    ("H113", "Chadi Otmani"), ("H073", "Jose John"), ("H066", "Christopherlov Brian"), ("H124", "Nethaniel Fernandez M"),
    ("H067", "Jansher"), ("H100", "Mohammed Sajeer"), ("H120", "Abdul Karim AS"), ("H080", "Muhammed Saleh"),
    ("H084", "Housine EL Amri"), ("H129", "Pratik Bista"), ("H121", "Afreen Salam"), ("H122", "Mohamed Arsath"),
    ("H126", "Subin Kovammal"), ("H078", "Abdul Rahman Ayyuob"), ("H079", "Dilip Siwakoti"), ("H101", "Jeffrey Gumban"),
    ("H109", "AL Ameen"), ("H110", "Mohammed Fameem"), ("H102", "Shameer Manikkunnummal"), ("H128", "Manil Dilusha"),
    ("H074", "Abdullah Leppa"), ("H098", "Caleb Manaois"), ("H099", "Muhammed Rajas"), ("H103", "Ali Akbar Hassan"),
    ("H087", "Elmer Flores"), ("H114", "Abdul Khader"), ("H054", "Siddik Abdulla"), ("H062", "Rakshith.p"),
    ("H059", "Rekhil Kodakka"), ("H063", "Janeesh mullappally"), ("H023", "Adil"), ("H024", "Mohd Musthafa"),
    ("H005", "Aboobacker Aliyar"), ("H011", "Sudhakaran"), ("H013", "Haris K"), ("H017", "Mujammal"),
    ("H021", "Saifudheen"), ("H022", "Jose Patibo"), ("H026", "Riyas Ahmed"), ("H034", "Riyasudheen Khuthubudheen"),
    ("H055", "Shaji"), ("H046", "Shihabudeen"), ("H049", "Shobith"), ("H050", "Ranjith. P"), ("H051", "Shar Bahadar"),
    ("H053", "Sabeer Ali Chemban"), ("H039", "Ranjith"), ("H041", "Axel Flores"), ("H058", "Sheik Kareem"),
    ("H064", "Shahul C"), ("H065", "Juancho"), ("H068", "Muhammed Shafiq"), ("H072", "Mohammed Haseeb"),
    ("H081", "M. Ziaudeen"), ("H085", "Afreed Mahmood"), ("H088", "Shafneed Nazar"), ("H089", "Mohamed Saleh Ibrahem Saleh"),
    ("H092", "Mohammed Saddam"), ("H131", "Said Ahmed Ibrahim"), ("H105", "Yousaf Nobi"), ("H132", "Ahmed Younis")
]

RESTRICTION_OPTIONS = ["None", "Consumer Only", "2-8 Cars Only", "Sharjah", "Dubai", "Ajman", "Fujairah", "Ras Al Khaimah", "Abu Dhabi", "Al Ain"]

# --- 2. DATABASE SETUP & UPGRADE ---
def init_db():
    conn = sqlite3.connect('logistics.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT, veh_type TEXT, 
                    sector TEXT, restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT, 
                    restriction TEXT, anchor_area TEXT, last_vacation DATE)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS areas (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (
                    id INTEGER PRIMARY KEY, number TEXT, type TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, 
                    area TEXT, date TEXT, end_date TEXT)''')
                    
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (
                    id INTEGER PRIMARY KEY, person_type TEXT, person_name TEXT, 
                    start_date DATE, end_date DATE)''')
                    
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (
                    id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, 
                    driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT)''')
    
    # DATABASE UPGRADE / MIGRATION FIX
    migrations = [
        "ALTER TABLE drivers ADD COLUMN sector TEXT DEFAULT 'Pharma'",
        "ALTER TABLE drivers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE helpers ADD COLUMN restriction TEXT DEFAULT 'None'",
        "ALTER TABLE history ADD COLUMN person_code TEXT DEFAULT 'UNKNOWN'",
        "ALTER TABLE history ADD COLUMN end_date TEXT DEFAULT 'None'"
    ]
    for query in migrations:
        try:
            c.execute(query)
        except sqlite3.OperationalError:
            pass
    
    # Auto-Seed Data if empty
    c.execute("SELECT COUNT(*) FROM areas")
    if c.fetchone()[0] == 0:
        c.executemany("INSERT INTO areas (code, name) VALUES (?, ?)", SEED_AREAS)

    c.execute("SELECT COUNT(*) FROM vehicles")
    if c.fetchone()[0] == 0:
        c.executemany("INSERT INTO vehicles (number, type) VALUES (?, ?)", SEED_VEHICLES)
        
    c.execute("SELECT COUNT(*) FROM drivers")
    if c.fetchone()[0] == 0:
        d_seed = [(name, code, "VAN", "Pharma", "None", "None") for code, name in SEED_DRIVERS]
        c.executemany("INSERT INTO drivers (name, code, veh_type, sector, restriction, anchor_area) VALUES (?, ?, ?, ?, ?, ?)", d_seed)
        
    c.execute("SELECT COUNT(*) FROM helpers")
    if c.fetchone()[0] == 0:
        h_seed = [(name, code, "None", "None") for code, name in SEED_HELPERS]
        c.executemany("INSERT INTO helpers (name, code, restriction, anchor_area) VALUES (?, ?, ?, ?)", h_seed)

    conn.commit()
    return conn

conn = init_db()

def run_query(query, params=()):
    c = conn.cursor()
    c.execute(query, params)
    conn.commit()
    return c.fetchall()

def load_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def check_restriction(restriction, area_name):
    if restriction == "None": return True
    if restriction == "Consumer Only" and "Consumer" not in area_name: return False
    if restriction == "2-8 Cars Only" and "2-8" not in area_name: return False
    
    mapping = {
        "Sharjah": ["Sharjah", "SHJ"],
        "Dubai": ["Dubai", "DXB", "JUM", "BUR", "QUS", "DIC", "ALQ", "JA", "JFZ"],
        "Ajman": ["Ajman", "AJM"],
        "Fujairah": ["Fujairah", "FUJ"],
        "Ras Al Khaimah": ["Ras Al Khaimah", "RAK"],
        "Abu Dhabi": ["Abu Dhabi", "AUH", "MUS", "BAN", "KLF", "TCA", "KLD", "ARP", "KIZ"],
        "Al Ain": ["Al Ain", "ALN"]
    }
    if restriction in mapping:
        return any(k.lower() in area_name.lower() for k in mapping[restriction])
    return restriction.lower() in area_name.lower()

# Function to calculate exactly how many months of experience someone has in an area
def get_experience_months(person_code, area_name):
    records = run_query("SELECT date, end_date FROM history WHERE person_code=? AND area=?", (person_code, area_name))
    total_days = 0
    for start_str, end_str in records:
        try:
            start = datetime.strptime(start_str, "%Y-%m-%d").date()
            if end_str and end_str != "None":
                end = datetime.strptime(end_str, "%Y-%m-%d").date()
            else:
                end = start + timedelta(days=30)
            total_days += max(0, (end - start).days)
        except:
            pass
    return total_days / 30.0

# Helper to safely parse dates for editing
def safe_parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except:
        return date.today()

# --- 3. UI SETUP ---
st.set_page_config(page_title="Logistics Route Planner", layout="wide")
st.title("🚛 Logistics Route & Rotation Planner")

menu = ["1. Create Route Plan", "2. Database Management", "3. Past Experience Builder", "4. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)

# ==========================================
# SCREEN 1: CREATE ROUTE PLAN
# ==========================================
if choice == "1. Create Route Plan":
    st.header("🗺️ Generate Rotation & Route Plan")
    
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=date.today())
    rot_type = col2.radio("Who is rotating?", ["Drivers", "Helpers"])
    
    if st.button("Generate Temporary Route Plan"):
        drivers = load_table('drivers')
        helpers = load_table('helpers')
        areas = load_table('areas')
        vehicles = load_table('vehicles')
        history = load_table('history')
        active_routes = load_table('active_routes')
        
        fallback_areas = ["2-8 Cars", "Urgent/Gov Order", "Pickup", "Substitute", "Second Trip", "Novo Sample"]
        all_area_names = areas['name'].tolist() + fallback_areas
        
        route_plan = []
        report_log = []
        order_counter = 1
        
        route_targets = areas.to_dict('records')
        for fb in fallback_areas:
            route_targets.append({"code": "N/A", "name": fb})
            
        used_drivers = set()
        used_helpers = set()
        used_vehicles = set()

        for idx, area in enumerate(route_targets):
            area_name = area['name']
            area_code = area['code']
            
            assigned_d_code, assigned_d_name = "UNASSIGNED", "UNASSIGNED"
            assigned_h_code, assigned_h_name = "UNASSIGNED", "UNASSIGNED"
            assigned_v_num = "UNASSIGNED"
            log_reason = ""
            
            prev_assignment = active_routes[active_routes['area_name'] == area_name]
            
            needs_helper = True
            area_lower = area_name.lower()
            if any(kw in area_lower for kw in ["2-8", "urgent", "gov", "substitute"]):
                needs_helper = False
            
            if rot_type == "Drivers":
                # KEEP HELPER
                if needs_helper:
                    if not prev_assignment.empty and prev_assignment.iloc[0]['helper_code'] != "N/A":
                        assigned_h_code = prev_assignment.iloc[0]['helper_code']
                        assigned_h_name = prev_assignment.iloc[0]['helper_name']
                    else:
                        avail_h = helpers[~helpers['code'].isin(used_helpers)]
                        for _, h in avail_h.iterrows():
                            if check_restriction(h['restriction'], area_name):
                                assigned_h_code, assigned_h_name = h['code'], h['name']
                                break
                
                # ROTATE DRIVER
                avail_d = drivers[~drivers['code'].isin(used_drivers)]
                for _, d in avail_d.iterrows():
                    if d['anchor_area'] != "None" and d['anchor_area'] != area_name: continue
                    if not check_restriction(d['restriction'], area_name): continue
                    
                    past_6m = (month_target - timedelta(days=180)).strftime("%Y-%m-%d")
                    if not history.empty and 'person_code' in history.columns:
                        recent = history[(history['person_code'] == d['code']) & (history['date'] >= past_6m)]['area'].tolist()
                        if area_name in recent and d['anchor_area'] == "None": continue
                    
                    d_exp = get_experience_months(d['code'], area_name)
                    if d_exp < 1: 
                        h_exp = get_experience_months(assigned_h_code, area_name)
                        if h_exp < 2 and assigned_h_code != "UNASSIGNED" and needs_helper: 
                            continue 
                        
                    assigned_d_code, assigned_d_name = d['code'], d['name']
                    log_reason = "Driver rotated. Helper kept."
                    used_drivers.add(d['code'])
                    
                    if d['veh_type'] == "BUS": needs_helper = False
                    break
                    
            elif rot_type == "Helpers":
                # KEEP DRIVER
                if not prev_assignment.empty:
                    assigned_d_code = prev_assignment.iloc[0]['driver_code']
                    assigned_d_name = prev_assignment.iloc[0]['driver_name']
                    d_type_check = drivers[drivers['code'] == assigned_d_code]['veh_type'].values
                    if len(d_type_check) > 0 and d_type_check[0] == "BUS":
                        needs_helper = False
                else:
                    avail_d = drivers[~drivers['code'].isin(used_drivers)]
                    for _, d in avail_d.iterrows():
                        if check_restriction(d['restriction'], area_name):
                            assigned_d_code, assigned_d_name = d['code'], d['name']
                            if d['veh_type'] == "BUS": needs_helper = False
                            break
                
                # ROTATE HELPER
                if needs_helper:
                    avail_h = helpers[~helpers['code'].isin(used_helpers)]
                    for _, h in avail_h.iterrows():
                        if h['anchor_area'] != "None" and h['anchor_area'] != area_name: continue
                        if not check_restriction(h['restriction'], area_name): continue
                        
                        d_exp = get_experience_months(assigned_d_code, area_name)
                        if d_exp < 1 and h['anchor_area'] == "None" and assigned_d_code != "UNASSIGNED": continue
                        
                        assigned_h_code, assigned_h_name = h['code'], h['name']
                        log_reason = "Helper rotated. Driver kept."
                        used_helpers.add(h['code'])
                        break

            if not needs_helper:
                assigned_h_code = "N/A"
                assigned_h_name = "NO HELPER REQUIRED"

            # ASSIGN VEHICLE
            if assigned_d_code != "UNASSIGNED":
                d_type = drivers[drivers['code'] == assigned_d_code]['veh_type'].values
                target_v_type = d_type[0] if len(d_type) > 0 else "VAN"
                if "Pickup" in area_name: target_v_type = "PICK-UP"
                if "2-8 Cars" in area_name: target_v_type = "BUS"
                
                avail_v = vehicles[(~vehicles['number'].isin(used_vehicles)) & (vehicles['type'] == target_v_type)]
                if not avail_v.empty:
                    assigned_v_num = avail_v.iloc[0]['number']
                    used_vehicles.add(assigned_v_num)
                else:
                    avail_any = vehicles[~vehicles['number'].isin(used_vehicles)]
                    if not avail_any.empty:
                        assigned_v_num = avail_any.iloc[0]['number']
                        used_vehicles.add(assigned_v_num)
            
            route_plan.append({
                "Order Number": order_counter,
                "Area Code": area_code,
                "Area Full Name": area_name,
                "Driver Code": assigned_d_code,
                "Driver Name": assigned_d_name,
                "Helper Code": assigned_h_code,
                "Helper Name": assigned_h_name,
                "Vehicle Number": assigned_v_num
            })
            
            report_log.append({
                "Area": area_name,
                "Driver": assigned_d_name,
                "Helper": assigned_h_name,
                "Reason": log_reason if log_reason else ("No Helper Needed" if not needs_helper else "System Fallback/Missed Constraints")
            })
            order_counter += 1

        st.session_state.generated_plan = route_plan
        st.session_state.generated_report = report_log
        st.session_state.plan_date = month_target
        st.success("Plan Generated! Please review below and click the green button to finalize and add as 3 months experience.")

    if 'generated_plan' in st.session_state:
        st.info("📊 Excel Sheet Preview:")
        df_route = pd.DataFrame(st.session_state.generated_plan)
        st.dataframe(df_route, use_container_width=True)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_route.to_excel(writer, sheet_name='Route Plan', index=False)
            pd.DataFrame(st.session_state.generated_report).to_excel(writer, sheet_name='Report Log', index=False)
        output.seek(0)
        
        col_down, col_app = st.columns(2)
        col_down.download_button("📥 Download Excel", data=output, file_name=f"Generated_Plan_{st.session_state.plan_date}.xlsx")
        
        if col_app.button("✅ Approve Plan & Add as Next 3 Months Experience", type="primary"):
            run_query("DELETE FROM active_routes")
            plan_start_str = st.session_state.plan_date.strftime("%Y-%m-%d")
            plan_end_str = (st.session_state.plan_date + timedelta(days=90)).strftime("%Y-%m-%d") 
            
            for r in st.session_state.generated_plan:
                run_query("INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                          (r['Order Number'], r['Area Code'], r['Area Full Name'], r['Driver Code'], r['Driver Name'], r['Helper Code'], r['Helper Name'], r['Vehicle Number']))
                
                if r['Driver Code'] != "UNASSIGNED":
                    run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)",
                              ("Driver", r['Driver Code'], r['Driver Name'], r['Area Full Name'], plan_start_str, plan_end_str))
                if r['Helper Code'] not in ["UNASSIGNED", "N/A"]:
                    run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)",
                              ("Helper", r['Helper Code'], r['Helper Name'], r['Area Full Name'], plan_start_str, plan_end_str))
            
            st.success("Plan Approved! The next 3 months of experience have been saved into the database.")
            del st.session_state.generated_plan


# ==========================================
# SCREEN 2: DATABASE MANAGEMENT (Add/Edit/Delete)
# ==========================================
elif choice == "2. Database Management":
    st.header("🗄️ Manage Database (Add, Edit, Delete)")
    
    areas_df = load_table('areas')
    area_list = ["None"] + (areas_df['name'].tolist() if not areas_df.empty else [])
    
    tab1, tab2, tab3, tab4 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles"])

    # DRIVERS
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
                          (d_name, d_code, d_type, "Pharma", d_restr, d_anchor))
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
                    run_query("UPDATE drivers SET name=?, veh_type=?, restriction=?, anchor_area=? WHERE code=?", (e_name, e_type, e_restr, e_anchor, sel_d_code))
                    st.success("Updated!")
                    st.rerun()
                if c_del.button("🗑️ Delete Driver", use_container_width=True):
                    run_query("DELETE FROM drivers WHERE code=?", (sel_d_code,))
                    st.warning("Deleted!")
                    st.rerun()

    # HELPERS
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
                run_query("INSERT INTO helpers (name, code, restriction, anchor_area) VALUES (?, ?, ?, ?)", (h_name, h_code, h_restr, h_anchor))
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
                    run_query("UPDATE helpers SET name=?, restriction=?, anchor_area=? WHERE code=?", (e_hname, e_hrestr, e_hanchor, sel_h_code))
                    st.success("Updated!")
                    st.rerun()
                if c_del.button("🗑️ Delete Helper", use_container_width=True):
                    run_query("DELETE FROM helpers WHERE code=?", (sel_h_code,))
                    st.warning("Deleted!")
                    st.rerun()

    # AREAS
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
                run_query("INSERT INTO areas (name, code) VALUES (?, ?)", (a_name, a_code))
                st.success("Added!")
                st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Area")
            sel_a = st.selectbox("Select Area to Delete", a_df['name'].tolist() if not a_df.empty else [])
            if st.button("Delete Selected Area", use_container_width=True) and sel_a:
                run_query("DELETE FROM areas WHERE name=?", (sel_a,))
                st.warning("Deleted!")
                st.rerun()

    # VEHICLES
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
                run_query("INSERT INTO vehicles (number, type) VALUES (?, ?)", (v_num, v_type))
                st.success("Added!")
                st.rerun()
        with c_edit:
            st.subheader("🗑️ Delete Vehicle")
            sel_v = st.selectbox("Select Vehicle to Delete", v_df['number'].tolist() if not v_df.empty else [])
            if st.button("Delete Selected Vehicle", use_container_width=True) and sel_v:
                run_query("DELETE FROM vehicles WHERE number=?", (sel_v,))
                st.warning("Deleted!")
                st.rerun()


# ==========================================
# SCREEN 3: PAST EXPERIENCE BUILDER (Add/Edit/Delete)
# ==========================================
elif choice == "3. Past Experience Builder":
    st.header("🕰️ Manage Past Experience")
    
    st.subheader("📋 Current Experience Log")
    history_df = load_table('history')
    st.dataframe(history_df.sort_values(by="id", ascending=False), use_container_width=True, height=250)
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
            if p_start_date > p_end_date:
                st.error("Start Date cannot be after End Date.")
            else:
                p_code = p_person.split(" - ")[0]
                p_name = p_person.split(" - ")[1]
                run_query("INSERT INTO history (person_type, person_code, person_name, area, date, end_date) VALUES (?, ?, ?, ?, ?, ?)",
                          (p_type, p_code, p_name, p_area, p_start_date.strftime("%Y-%m-%d"), p_end_date.strftime("%Y-%m-%d")))
                st.success("Experience logically calculated and added!")
                st.rerun()

    with c_edit:
        st.subheader("✏️ Edit / Delete Experience")
        if not history_df.empty:
            history_list = [f"{row['id']} - {row['person_name']} ({row['area']})" for _, row in history_df.iterrows()]
            sel_hist_str = st.selectbox("Select Record to Edit/Delete", history_list)
            
            if sel_hist_str:
                hist_id = int(sel_hist_str.split(" - ")[0])
                hist_data = history_df[history_df['id'] == hist_id].iloc[0]
                
                a_idx = area_list.index(hist_data['area']) if hist_data['area'] in area_list else 0
                e_area = st.selectbox("Edit Area", area_list, index=a_idx, key=f"e_area_{hist_id}")
                
                ed1, ed2 = st.columns(2)
                e_start_val = safe_parse_date(hist_data['date'])
                e_end_val = safe_parse_date(hist_data['end_date'])
                
                new_start = ed1.date_input("Edit Start Date", value=e_start_val, key=f"es_{hist_id}")
                new_end = ed2.date_input("Edit End Date", value=e_end_val, key=f"ee_{hist_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Experience", use_container_width=True):
                    run_query("UPDATE history SET area=?, date=?, end_date=? WHERE id=?", 
                              (e_area, new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d"), hist_id))
                    st.success("Updated Successfully!")
                    st.rerun()
                    
                if c_del.button("🗑️ Delete Experience", use_container_width=True):
                    run_query("DELETE FROM history WHERE id=?", (hist_id,))
                    st.warning("Deleted!")
                    st.rerun()
        else:
            st.info("No history available to edit.")


# ==========================================
# SCREEN 4: VACATION SCHEDULE (Add/Edit/Delete)
# ==========================================
elif choice == "4. Vacation Schedule":
    st.header("🌴 Manage Vacation Schedule")
    
    st.subheader("📋 Current Vacations List")
    vacs_df = load_table('vacations')
    st.dataframe(vacs_df, use_container_width=True, height=250)
    st.divider()
    
    c_add, c_edit = st.columns(2)
    
    with c_add:
        st.subheader("➕ Add Vacation")
        v_type = st.selectbox("Role", ["Driver", "Helper"])
        df_names = load_table('drivers') if v_type == "Driver" else load_table('helpers')
        name_list = df_names['name'].tolist() if not df_names.empty else []
        v_name = st.selectbox("Name", name_list)
        
        d1, d2 = st.columns(2)
        v_start = d1.date_input("Start Date")
        v_end = d2.date_input("End Date")
        
        if st.button("➕ Add Vacation", use_container_width=True):
            if v_start > v_end:
                st.error("Start Date cannot be after End Date.")
            else:
                # Check overlaps (Max 3 allowed)
                overlapping = 0
                for _, row in vacs_df.iterrows():
                    if row['person_type'] == v_type:
                        e_start = safe_parse_date(row['start_date'])
                        e_end = safe_parse_date(row['end_date'])
                        if max(v_start, e_start) <= min(v_end, e_end):
                            overlapping += 1
                
                if overlapping >= 3:
                    st.error(f"Cannot add! Already {overlapping} {v_type}s on vacation during these dates.")
                else:
                    run_query("INSERT INTO vacations (person_type, person_name, start_date, end_date) VALUES (?, ?, ?, ?)",
                              (v_type, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")))
                    st.success("Vacation scheduled successfully!")
                    st.rerun()

    with c_edit:
        st.subheader("✏️ Edit / Delete Vacation")
        if not vacs_df.empty:
            vac_list = [f"{row['id']} - {row['person_name']} ({row['start_date']} to {row['end_date']})" for _, row in vacs_df.iterrows()]
            sel_vac_str = st.selectbox("Select Vacation to Edit/Delete", vac_list)
            
            if sel_vac_str:
                vac_id = int(sel_vac_str.split(" - ")[0])
                vac_data = vacs_df[vacs_df['id'] == vac_id].iloc[0]
                
                ed1, ed2 = st.columns(2)
                e_vstart_val = safe_parse_date(vac_data['start_date'])
                e_vend_val = safe_parse_date(vac_data['end_date'])
                
                new_vstart = ed1.date_input("Edit Start Date", value=e_vstart_val, key=f"vs_{vac_id}")
                new_vend = ed2.date_input("Edit End Date", value=e_vend_val, key=f"ve_{vac_id}")
                
                c_upd, c_del = st.columns(2)
                if c_upd.button("💾 Update Vacation", use_container_width=True):
                    if new_vstart > new_vend:
                        st.error("Start Date cannot be after End Date.")
                    else:
                        run_query("UPDATE vacations SET start_date=?, end_date=? WHERE id=?", 
                                  (new_vstart.strftime("%Y-%m-%d"), new_vend.strftime("%Y-%m-%d"), vac_id))
                        st.success("Updated Successfully!")
                        st.rerun()
                        
                if c_del.button("🗑️ Delete Vacation", use_container_width=True):
                    run_query("DELETE FROM vacations WHERE id=?", (vac_id,))
                    st.warning("Deleted!")
                    st.rerun()
        else:
            st.info("No vacations available to edit.")
