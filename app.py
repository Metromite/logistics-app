import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date

# --- 1. DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect('logistics.db')
    c = conn.cursor()
    # Create Tables
    c.execute('''CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT, veh_type TEXT, 
                    healthcard BOOLEAN, anchor_area TEXT, last_vacation DATE)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS helpers (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT, consumer_only BOOLEAN, 
                    anchor_area TEXT, last_vacation DATE)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS areas (
                    id INTEGER PRIMARY KEY, name TEXT, code TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (
                    id INTEGER PRIMARY KEY, number TEXT, type TEXT, restricted_area TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY, person_type TEXT, person_name TEXT, 
                    area TEXT, date TEXT)''')
                    
    c.execute('''CREATE TABLE IF NOT EXISTS vacations (
                    id INTEGER PRIMARY KEY, person_type TEXT, person_name TEXT, 
                    start_date DATE, end_date DATE)''')
    conn.commit()
    return conn

conn = init_db()

# --- HELPER FUNCTIONS ---
def run_query(query, params=()):
    c = conn.cursor()
    c.execute(query, params)
    conn.commit()
    return c.fetchall()

def load_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", conn)

# --- UI SETUP ---
st.set_page_config(page_title="Logistics Rotation App", layout="wide")
st.title("🚛 Logistics Rotation & Vacation Manager")

# Navigation
menu = ["1. Database Management", "2. Rotation Planner (3-Month)", "3. Vacation Schedule"]
choice = st.sidebar.radio("Navigate", menu)

# ==========================================
# SCREEN 1: DATABASE MANAGEMENT
# ==========================================
if choice == "1. Database Management":
    st.header("Manage Database (Add/Edit/Remove)")
    tab1, tab2, tab3, tab4 = st.tabs(["Drivers", "Helpers", "Areas", "Vehicles"])

    # DRIVERS
    with tab1:
        st.subheader("Add New Driver")
        col1, col2, col3, col4 = st.columns(4)
        d_name = col1.text_input("Driver Name")
        d_code = col2.text_input("Driver Code")
        d_type = col3.selectbox("Vehicle Type", ["Both", "Pickup Only", "2-8 Cars Only"])
        d_hc = col4.checkbox("Has Healthcard?")
        
        areas_df = load_table('areas')
        area_list = ["None"] + areas_df['name'].tolist() if not areas_df.empty else ["None"]
        d_anchor = st.selectbox("Anchor to Area (Optional)", area_list, key="d_anchor")
        
        if st.button("Add Driver"):
            run_query("INSERT INTO drivers (name, code, veh_type, healthcard, anchor_area) VALUES (?, ?, ?, ?, ?)", 
                      (d_name, d_code, d_type, d_hc, d_anchor))
            st.success("Driver Added!")
        st.dataframe(load_table('drivers'))

    # HELPERS
    with tab2:
        st.subheader("Add New Helper")
        col1, col2, col3 = st.columns(3)
        h_name = col1.text_input("Helper Name")
        h_code = col2.text_input("Helper Code")
        h_cons = col3.checkbox("Consumer Only?")
        h_anchor = st.selectbox("Anchor to Area (Optional)", area_list, key="h_anchor")
        
        if st.button("Add Helper"):
            run_query("INSERT INTO helpers (name, code, consumer_only, anchor_area) VALUES (?, ?, ?, ?)", 
                      (h_name, h_code, h_cons, h_anchor))
            st.success("Helper Added!")
        st.dataframe(load_table('helpers'))

    # AREAS
    with tab3:
        st.subheader("Add Area")
        a_name = st.text_input("Area Full Name")
        a_code = st.text_input("Area Code")
        if st.button("Add Area"):
            run_query("INSERT INTO areas (name, code) VALUES (?, ?)", (a_name, a_code))
            st.success("Area Added!")
        st.dataframe(load_table('areas'))

    # VEHICLES
    with tab4:
        st.subheader("Add Vehicle")
        v_num = st.text_input("Vehicle Number")
        v_type = st.selectbox("Type", ["Pickup", "2-8 Cars", "Van"])
        v_restr = st.selectbox("Restricted to Area (Optional)", area_list)
        if st.button("Add Vehicle"):
            run_query("INSERT INTO vehicles (number, type, restricted_area) VALUES (?, ?, ?)", 
                      (v_num, v_type, v_restr))
            st.success("Vehicle Added!")
        st.dataframe(load_table('vehicles'))


# ==========================================
# SCREEN 2: ROTATION PLANNER
# ==========================================
elif choice == "2. Rotation Planner (3-Month)":
    st.header("🔄 Generate Rotation")
    st.info("Logic: Drivers and Helpers rotate every 3 months. Driver and Helper rotations have a 1-month gap. Personnel will not return to an area they visited in the last 6 months unless anchored.")
    
    month_target = st.date_input("Select Rotation Start Date", value=date.today())
    rot_type = st.radio("Who is rotating this month?", ["Drivers", "Helpers"])
    
    if st.button("Generate Rotation Schedule"):
        # Fetch Data
        drivers = load_table('drivers')
        helpers = load_table('helpers')
        areas = load_table('areas')
        vacations = load_table('vacations')
        history = load_table('history')
        
        st.write("### New Assignments Generated:")
        
        assignments = []
        
        # Determine who is rotating
        df_rotating = drivers if rot_type == "Drivers" else helpers
        
        for index, person in df_rotating.iterrows():
            # 1. Check if on Vacation
            on_vacation = False
            for _, vac in vacations.iterrows():
                if vac['person_name'] == person['name']:
                    v_start = datetime.strptime(vac['start_date'], "%Y-%m-%d").date()
                    v_end = datetime.strptime(vac['end_date'], "%Y-%m-%d").date()
                    if v_start <= month_target <= v_end:
                        on_vacation = True
                        assignments.append({"Name": person['name'], "Assigned Area": f"ON VACATION (Until {vac['end_date']})", "Notes": "Requires temp cover"})
                        break
            if on_vacation: continue
            
            # 2. Check Anchors
            if person['anchor_area'] != "None" and pd.notna(person['anchor_area']):
                assignments.append({"Name": person['name'], "Assigned Area": person['anchor_area'], "Notes": "Anchored"})
                continue
                
            # 3. Apply constraints & History (6 months)
            available_areas = areas['name'].tolist()
            
            # Filter history
            past_6_months = (month_target - timedelta(days=180)).strftime("%Y-%m-%d")
            recent_areas = history[(history['person_name'] == person['name']) & (history['date'] >= past_6_months)]['area'].tolist()
            
            valid_areas = [a for a in available_areas if a not in recent_areas]
            
            # Apply specific constraints (e.g. Consumer Only, Healthcard)
            if rot_type == "Helpers" and person['consumer_only']:
                valid_areas = [a for a in valid_areas if "Consumer" in a] # Assuming area name contains 'Consumer'
                
            # Assign Area
            if valid_areas:
                chosen_area = valid_areas[0] # Simple greedy assignment
                assignments.append({"Name": person['name'], "Assigned Area": chosen_area, "Notes": "Rotated successfully"})
                # Save to history
                run_query("INSERT INTO history (person_type, person_name, area, date) VALUES (?, ?, ?, ?)",
                          (rot_type, person['name'], chosen_area, month_target.strftime("%Y-%m-%d")))
            else:
                assignments.append({"Name": person['name'], "Assigned Area": "MANUAL INTERVENTION NEEDED", "Notes": "No valid areas found based on 6-month rule."})
                
        st.table(pd.DataFrame(assignments))
        
    st.divider()
    st.subheader("Manual Override / Custom Assignment")
    col1, col2 = st.columns(2)
    person_override = col1.selectbox("Select Person", load_table('drivers')['name'].tolist() + load_table('helpers')['name'].tolist())
    area_override = col2.selectbox("Select Area", load_table('areas')['name'].tolist())
    if st.button("Force Assign & Save to History"):
        run_query("INSERT INTO history (person_type, person_name, area, date) VALUES (?, ?, ?, ?)",
                  ("Override", person_override, area_override, month_target.strftime("%Y-%m-%d")))
        st.success("Saved to history!")


# ==========================================
# SCREEN 3: VACATION SCHEDULE
# ==========================================
elif choice == "3. Vacation Schedule":
    st.header("🌴 Vacation Management")
    st.info("Rules: 1 vacation per year. Max 3 drivers and 3 helpers at the same time.")
    
    col1, col2, col3, col4 = st.columns(4)
    v_type = col1.selectbox("Role", ["Driver", "Helper"])
    
    # Dynamically load names based on role
    df_names = load_table('drivers') if v_type == "Driver" else load_table('helpers')
    name_list = df_names['name'].tolist() if not df_names.empty else []
    v_name = col2.selectbox("Name", name_list)
    
    v_start = col3.date_input("Start Date")
    v_end = col4.date_input("End Date")
    
    if st.button("Add Vacation"):
        # Constraint Check: Max 3 overlapping
        vacs = load_table('vacations')
        overlapping = 0
        for _, row in vacs.iterrows():
            if row['person_type'] == v_type:
                exist_start = datetime.strptime(row['start_date'], "%Y-%m-%d").date()
                exist_end = datetime.strptime(row['end_date'], "%Y-%m-%d").date()
                # Check date overlap
                if max(v_start, exist_start) <= min(v_end, exist_end):
                    overlapping += 1
        
        if overlapping >= 3:
            st.error(f"Cannot add! Already {overlapping} {v_type}s on vacation during these dates.")
        else:
            run_query("INSERT INTO vacations (person_type, person_name, start_date, end_date) VALUES (?, ?, ?, ?)",
                      (v_type, v_name, v_start.strftime("%Y-%m-%d"), v_end.strftime("%Y-%m-%d")))
            # Update last vacation date in main tables
            table_to_update = "drivers" if v_type == "Driver" else "helpers"
            run_query(f"UPDATE {table_to_update} SET last_vacation = ? WHERE name = ?", (v_start.strftime("%Y-%m-%d"), v_name))
            st.success("Vacation scheduled successfully!")
            
    st.subheader("Current Vacation Database")
    st.dataframe(load_table('vacations'))
    
    st.divider()
    st.subheader("Upcoming Vacation Suggestions")
    st.write("System suggests vacations based on whoever has gone the longest without one (1 year rule).")
    
    # Suggestion logic
    drivers = load_table('drivers')
    if 'last_vacation' in drivers.columns and not drivers.empty:
        drivers['last_vacation'] = pd.to_datetime(drivers['last_vacation']).fillna(pd.Timestamp('1900-01-01'))
        due_drivers = drivers.sort_values(by='last_vacation').head(3)
        st.write("**Drivers Due for Vacation:**")
        st.dataframe(due_drivers[['name', 'last_vacation']])