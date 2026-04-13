import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import io

# --- 1. SEED DATA (Provided by User) ---
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

# --- 2. DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect('logistics.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT, veh_type TEXT, 
                    sector TEXT, anchor_area TEXT, last_vacation DATE)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT, 
                    anchor_area TEXT, last_vacation DATE)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS areas (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (
                    id INTEGER PRIMARY KEY, number TEXT, type TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY, person_type TEXT, person_code TEXT, person_name TEXT, 
                    area TEXT, date TEXT)''')
                    
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (
                    id INTEGER PRIMARY KEY, person_type TEXT, person_name TEXT, 
                    start_date DATE, end_date DATE)''')
                    
    c.execute('''CREATE TABLE IF NOT EXISTS active_routes (
                    id INTEGER PRIMARY KEY, order_num INTEGER, area_code TEXT, area_name TEXT, 
                    driver_code TEXT, driver_name TEXT, helper_code TEXT, helper_name TEXT, veh_num TEXT)''')
    
    # Auto-Seed Data if empty
    c.execute("SELECT COUNT(*) FROM vehicles")
    if c.fetchone()[0] == 0:
        c.executemany("INSERT INTO vehicles (number, type) VALUES (?, ?)", SEED_VEHICLES)
        
    c.execute("SELECT COUNT(*) FROM drivers")
    if c.fetchone()[0] == 0:
        d_seed = [(name, code, "VAN", "Pharma", "None") for code, name in SEED_DRIVERS]
        c.executemany("INSERT INTO drivers (name, code, veh_type, sector, anchor_area) VALUES (?, ?, ?, ?, ?)", d_seed)
        
    c.execute("SELECT COUNT(*) FROM helpers")
    if c.fetchone()[0] == 0:
        h_seed = [(name, code, "None") for code, name in SEED_HELPERS]
        c.executemany("INSERT INTO helpers (name, code, anchor_area) VALUES (?, ?, ?)", h_seed)

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

# --- 3. UI SETUP ---
st.set_page_config(page_title="Logistics Route Planner", layout="wide")
st.title("🚛 Logistics Route & Rotation Planner")

menu = ["1. Create Route Plan", "2. Database Management", "3. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)

# ==========================================
# SCREEN 1: CREATE ROUTE PLAN
# ==========================================
if choice == "1. Create Route Plan":
    st.header("🗺️ Generate Rotation & Route Plan")
    st.info("Generates an Excel sheet with constraints: Rotation gaps, Experience rules, Anchors, and Vehicle/Sector matches.")
    
    col1, col2 = st.columns(2)
    month_target = col1.date_input("Target Rotation Date", value=date.today())
    rot_type = col2.radio("Who is rotating?", ["Drivers", "Helpers"])
    
    if st.button("Generate Excel Route Plan"):
        drivers = load_table('drivers')
        helpers = load_table('helpers')
        areas = load_table('areas')
        vehicles = load_table('vehicles')
        history = load_table('history')
        vacations = load_table('vacations')
        active_routes = load_table('active_routes')
        
        # Fallback pseudo-areas if no specific area
        fallback_areas = ["2-8 Cars", "Urgent/Gov Order", "Pickup", "Substitute", "Second Trip", "Sample Driver"]
        all_area_names = areas['name'].tolist() + fallback_areas
        
        route_plan = []
        report_log = []
        order_counter = 1
        
        # We assign an area to each available vehicle/route slot
        # For simulation, we generate routes based on all existing areas + fallbacks
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
            
            # Find previous month's assignment for this area to keep the non-rotating person
            prev_assignment = active_routes[active_routes['area_name'] == area_name]
            
            if rot_type == "Drivers":
                # KEEP HELPER
                if not prev_assignment.empty:
                    assigned_h_code = prev_assignment.iloc[0]['helper_code']
                    assigned_h_name = prev_assignment.iloc[0]['helper_name']
                else:
                    # Pick a random available helper
                    avail_h = helpers[~helpers['code'].isin(used_helpers)]
                    if not avail_h.empty:
                        assigned_h_code = avail_h.iloc[0]['code']
                        assigned_h_name = avail_h.iloc[0]['name']
                
                # ROTATE DRIVER
                avail_d = drivers[~drivers['code'].isin(used_drivers)]
                for _, d in avail_d.iterrows():
                    # Check anchor
                    if d['anchor_area'] != "None" and d['anchor_area'] != area_name: continue
                    # Check 6 month history
                    past_6m = (month_target - timedelta(days=180)).strftime("%Y-%m-%d")
                    recent = history[(history['person_code'] == d['code']) & (history['date'] >= past_6m)]['area'].tolist()
                    if area_name in recent and d['anchor_area'] == "None": continue
                    
                    # Helper experience rule (if driver is 1st time, helper needs 2 months)
                    d_history = history[(history['person_code'] == d['code']) & (history['area'] == area_name)]
                    if len(d_history) == 0: 
                        h_history = history[(history['person_code'] == assigned_h_code) & (history['area'] == area_name)]
                        if len(h_history) < 2: continue # Helper doesn't have 2 months exp
                        
                    # Assigned!
                    assigned_d_code, assigned_d_name = d['code'], d['name']
                    log_reason = "Driver rotated. Helper kept."
                    used_drivers.add(d['code'])
                    break
                    
            elif rot_type == "Helpers":
                # KEEP DRIVER
                if not prev_assignment.empty:
                    assigned_d_code = prev_assignment.iloc[0]['driver_code']
                    assigned_d_name = prev_assignment.iloc[0]['driver_name']
                else:
                    avail_d = drivers[~drivers['code'].isin(used_drivers)]
                    if not avail_d.empty:
                        assigned_d_code = avail_d.iloc[0]['code']
                        assigned_d_name = avail_d.iloc[0]['name']
                
                # ROTATE HELPER
                avail_h = helpers[~helpers['code'].isin(used_helpers)]
                for _, h in avail_h.iterrows():
                    # Check anchor
                    if h['anchor_area'] != "None" and h['anchor_area'] != area_name: continue
                    
                    # Driver exp rule: Give new helper if driver has >= 1 month exp
                    d_history = history[(history['person_code'] == assigned_d_code) & (history['area'] == area_name)]
                    if len(d_history) < 1 and h['anchor_area'] == "None": continue
                    
                    assigned_h_code, assigned_h_name = h['code'], h['name']
                    log_reason = "Helper rotated. Driver kept."
                    used_helpers.add(h['code'])
                    break

            # Assign Vehicle (Match Driver vehicle type if possible)
            if assigned_d_code != "UNASSIGNED":
                d_type = drivers[drivers['code'] == assigned_d_code]['veh_type'].values
                target_v_type = d_type[0] if len(d_type) > 0 else "VAN"
                
                # Handling fallbacks
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
            
            # Record Route
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
            
            # Record Report
            report_log.append({
                "Area": area_name,
                "Driver": assigned_d_name,
                "Driver Exp (Months)": len(history[(history['person_code'] == assigned_d_code) & (history['area'] == area_name)]),
                "Helper": assigned_h_name,
                "Helper Exp (Months)": len(history[(history['person_code'] == assigned_h_code) & (history['area'] == area_name)]),
                "Reason for Assignment": log_reason if log_reason else "System Fallback/Constraints missed"
            })
            
            order_counter += 1

        df_route = pd.DataFrame(route_plan)
        df_report = pd.DataFrame(report_log)
        
        # Save to active routes for next month's reference
        run_query("DELETE FROM active_routes")
        for r in route_plan:
            run_query("INSERT INTO active_routes (order_num, area_code, area_name, driver_code, driver_name, helper_code, helper_name, veh_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                      (r['Order Number'], r['Area Code'], r['Area Full Name'], r['Driver Code'], r['Driver Name'], r['Helper Code'], r['Helper Name'], r['Vehicle Number']))
            
            # Append to History
            if r['Driver Code'] != "UNASSIGNED":
                run_query("INSERT INTO history (person_type, person_code, person_name, area, date) VALUES (?, ?, ?, ?, ?)",
                          ("Driver", r['Driver Code'], r['Driver Name'], r['Area Full Name'], month_target.strftime("%Y-%m-%d")))
            if r['Helper Code'] != "UNASSIGNED":
                run_query("INSERT INTO history (person_type, person_code, person_name, area, date) VALUES (?, ?, ?, ?, ?)",
                          ("Helper", r['Helper Code'], r['Helper Name'], r['Area Full Name'], month_target.strftime("%Y-%m-%d")))

        st.success("Route Plan Generated Successfully!")
        st.dataframe(df_route)
        
        # Create Excel File in Memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_route.to_excel(writer, sheet_name='Route Plan', index=False)
            df_report.to_excel(writer, sheet_name='Rotation Report', index=False)
        output.seek(0)
        
        st.download_button(
            label="📥 Download Route Plan (Excel)",
            data=output,
            file_name=f"Route_Plan_{month_target.strftime('%b_%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


# ==========================================
# SCREEN 2: DATABASE MANAGEMENT
# ==========================================
elif choice == "2. Database Management":
    st.header("🗄️ Manage Database")
    tab1, tab2, tab3, tab4 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles"])

    areas_df = load_table('areas')
    area_list = ["None", "Consumer", "Pickup", "2-8 Cars"] + (areas_df['name'].tolist() if not areas_df.empty else [])

    with tab1:
        st.subheader("Drivers")
        col1, col2, col3, col4, col5 = st.columns(5)
        d_name = col1.text_input("Name (Driver)")
        d_code = col2.text_input("Code (Driver)")
        d_type = col3.selectbox("Vehicle Type", ["VAN", "PICK-UP", "BUS"])
        d_sector = col4.selectbox("Sector", ["Pharma", "Consumer"])
        d_anchor = col5.selectbox("Anchor to", area_list, key="d_anchor")
        
        if st.button("Add Driver"):
            run_query("INSERT INTO drivers (name, code, veh_type, sector, anchor_area) VALUES (?, ?, ?, ?, ?)", 
                      (d_name, d_code, d_type, d_sector, d_anchor))
            st.success("Added!")
            
        st.dataframe(load_table('drivers'))
        
        del_d = st.text_input("Enter Driver Code to Remove")
        if st.button("Remove Driver"):
            run_query("DELETE FROM drivers WHERE code = ?", (del_d,))
            st.warning("Deleted!")

    with tab2:
        st.subheader("Helpers")
        col1, col2, col3 = st.columns(3)
        h_name = col1.text_input("Name (Helper)")
        h_code = col2.text_input("Code (Helper)")
        h_anchor = col3.selectbox("Anchor to", area_list, key="h_anchor")
        
        if st.button("Add Helper"):
            run_query("INSERT INTO helpers (name, code, anchor_area) VALUES (?, ?, ?)", 
                      (h_name, h_code, h_anchor))
            st.success("Added!")
            
        st.dataframe(load_table('helpers'))
        
        del_h = st.text_input("Enter Helper Code to Remove")
        if st.button("Remove Helper"):
            run_query("DELETE FROM helpers WHERE code = ?", (del_h,))
            st.warning("Deleted!")

    with tab3:
        st.subheader("Areas")
        a_name = st.text_input("Area Full Name")
        a_code = st.text_input("Area Code")
        if st.button("Add Area"):
            run_query("INSERT INTO areas (name, code) VALUES (?, ?)", (a_name, a_code))
            st.success("Added!")
        st.dataframe(load_table('areas'))

    with tab4:
        st.subheader("Vehicles")
        v_num = st.text_input("Vehicle Number")
        v_type = st.selectbox("Type", ["VAN", "PICK-UP", "BUS"])
        if st.button("Add Vehicle"):
            run_query("INSERT INTO vehicles (number, type) VALUES (?, ?)", (v_num, v_type))
            st.success("Added!")
            
        st.dataframe(load_table('vehicles'))
        
        del_v = st.text_input("Enter Vehicle Number to Remove")
        if st.button("Remove Vehicle"):
            run_query("DELETE FROM vehicles WHERE number = ?", (del_v,))
            st.warning("Deleted!")

# ==========================================
# SCREEN 3: VACATION SCHEDULE
# ==========================================
elif choice == "3. Vacation Schedule":
    st.header("🌴 Vacation Management")
    
    col1, col2, col3, col4 = st.columns(4)
    v_type = col1.selectbox("Role", ["Driver", "Helper"])
    
    df_names = load_table('drivers') if v_type == "Driver" else load_table('helpers')
    name_list = df_names['name'].tolist() if not df_names.empty else []
    v_name = col2.selectbox("Name", name_list)
    
    v_start = col3.date_input("Start Date")
    v_end = col4.date_input("End Date")
    
    if st.button("Add Vacation"):
        vacs = load_table('vacations')
        overlapping = 0
        for _, row in vacs.iterrows():
            if row['person_type'] == v_type:
                exist_start = datetime.strptime(row['start_date'], "%Y-%m-%d").date()
                exist_end = datetime.strptime(row['end_date'], "%Y-%m-%d").date()
                if max(v_start, exist_start) <= min(v_end, exist_end):
                    overlapping += 1
        
        if overlapping >= 3:
            st.error(f"Cannot add! Already {overlapping} {v_type}s on vacation during these dates.")
        else:
            run_query("INSERT INTO vacations (person_type, person_name, start_date, end_date) VALUES (?, ?, ?, ?)",
                      (v_type, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")))
            st.success("Vacation scheduled successfully!")
            
    st.subheader("Current Vacations")
    st.dataframe(load_table('vacations'))
