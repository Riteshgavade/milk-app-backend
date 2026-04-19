import streamlit as st
import pandas as pd
from pymongo import MongoClient
import certifi
import os
from dotenv import load_dotenv

st.set_page_config(page_title="Milk App Admin", page_icon="🛠️", layout="wide")

# ==========================================
# 🔒 LOGIN SYSTEM
# ==========================================
ADMIN_PASSWORD = "Ritesh"

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def verify_login():
    if st.session_state.password_input == ADMIN_PASSWORD:
        st.session_state.logged_in = True
        st.session_state.password_input = "" 
    else:
        st.error("❌ Incorrect password.")

if not st.session_state.logged_in:
    st.title("🔒 Developer Admin Login")
    st.markdown("Please enter the admin password to access the database controls.")
    st.text_input("Password", type="password", key="password_input", on_change=verify_login)
    st.button("Login", on_click=verify_login)
    st.stop() 

st.sidebar.button("Logout", on_click=lambda: st.session_state.update({"logged_in": False}))

# --- DATABASE CONNECTION ---
MONGO_URL = os.getenv("MONGO_URL")

@st.cache_resource
def init_connection():
    return MongoClient(MONGO_URL, tlsCAFile=certifi.where())

client = init_connection()
db = client.get_database("milk_collection_db")
collection = db["milk_records"]
page_configs = db["page_configs"] 

def clean_number(value):
    if pd.isna(value): return 0.0
    clean_str = str(value).replace('?', '').replace('₹', '').replace(',', '').strip()
    try: return float(clean_str)
    except ValueError: return 0.0

# UI Date Formatter
def format_ui_date(date_str):
    try:
        return pd.to_datetime(date_str).strftime('%d-%m-%Y')
    except:
        return date_str

st.title("🛠️ Developer Admin Control Center")
st.markdown("Manage your Milk Application database, upload new data, and configure app pages.")

tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload CSV Data", "🔍 View & Fix Data", "📊 Database Health", "📱 Billing Cycles (App UI)"])

# ==========================================
# TAB 1: UPLOAD CSV
# ==========================================
with tab1:
    st.header("Upload New Billing Cycle Data")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Preview of uploaded data:")
        st.dataframe(df)
        
        if st.button("🚀 Push Data to MongoDB"):
            with st.spinner('Uploading...'):
                records_to_upload = []
                for index, row in df.iterrows():
                    if pd.isna(row.get('Customer Number')): continue
                    
                    customer_num = str(int(row['Customer Number']))
                    customer_name = str(row['Name']).strip()
                    raw_date = row.get('Date')
                    dt_obj = pd.to_datetime(raw_date, errors='coerce')
                    if pd.isna(dt_obj): continue 
                    clean_date = dt_obj.strftime('%Y-%m-%d')
                    
                    # 👇 NEW: GRAB DAIRY CODE AND PHONE NUMBER 👇
                    dairy_code = str(row.get('Dairy_Code', '')).strip()
                    if pd.isna(row.get('Dairy_Code')) or dairy_code.lower() == 'nan':
                        dairy_code = ""

                    phone_number = str(row.get('Phone_Number', '')).strip()
                    if pd.isna(row.get('Phone_Number')) or phone_number.lower() == 'nan':
                        phone_number = ""
                    
                    kapat_name = str(row.get('Kapat_Name', '')).strip()
                    if pd.isna(row.get('Kapat_Name')) or kapat_name.lower() == 'nan':
                        kapat_name = ""
                        
                    kapat_amount = clean_number(row.get('Kapat_Amount', 0))
                    keleli_kapat = clean_number(row.get('Keleli_Kapat', 0))

                    m_liters = clean_number(row.get('Morning_Liter', 0))
                    n_liters = clean_number(row.get('Night_Liter', 0))
                    
                    if m_liters > 0:
                        records_to_upload.append({
                            "dairy_code": dairy_code, # 👈 ADDED HERE
                            "customer_number": customer_num, "customer_name": customer_name, "phone_number": phone_number,
                            "date": clean_date, "shift": "Morning", 
                            "liters": m_liters, "fat": clean_number(row.get('Morning_Fat', 0)), "snf": clean_number(row.get('Morning_SNF', 0)), 
                            "rate": clean_number(row.get('Rate', 0)), "total_amount": clean_number(row.get('Amount', 0)),
                            "kapat_name": kapat_name, "kapat_amount": kapat_amount, "keleli_kapat": keleli_kapat
                        })
                        kapat_name = ""
                        kapat_amount = 0.0
                        keleli_kapat = 0.0
                    
                    if n_liters > 0:
                        records_to_upload.append({
                            "dairy_code": dairy_code, # 👈 ADDED HERE
                            "customer_number": customer_num, "customer_name": customer_name, "phone_number": phone_number,
                            "date": clean_date, "shift": "Evening", 
                            "liters": n_liters, "fat": clean_number(row.get('Night_Fat', 0)), "snf": clean_number(row.get('Night_SNF', 0)), 
                            "rate": clean_number(row.get('Rate.1', 0)), "total_amount": clean_number(row.get('Amount.1', 0)),
                            "kapat_name": kapat_name, "kapat_amount": kapat_amount, "keleli_kapat": keleli_kapat
                        })
                        
                    if m_liters == 0 and n_liters == 0 and kapat_amount > 0:
                        records_to_upload.append({
                            "dairy_code": dairy_code, # 👈 ADDED HERE
                            "customer_number": customer_num, "customer_name": customer_name, "phone_number": phone_number,
                            "date": clean_date, "shift": "Deduction", 
                            "liters": 0.0, "fat": 0.0, "snf": 0.0, "rate": 0.0, "total_amount": 0.0,
                            "kapat_name": kapat_name, "kapat_amount": kapat_amount, "keleli_kapat": keleli_kapat
                        })

                if records_to_upload:
                    collection.insert_many(records_to_upload)
                    st.success(f"✅ Successfully uploaded {len(records_to_upload)} records to the cloud!")
                else:
                    st.error("No valid data found to upload.")
                    

# ==========================================
# TAB 2: VIEW & FIX DATA
# ==========================================
with tab2:
    st.header("Search & Delete Single Records")
    col1, col2 = st.columns(2)
    search_id = col1.text_input("Enter Customer ID to search:")
    
    if search_id:
        records = list(collection.find({"customer_number": search_id}).sort("date", -1).limit(50))
        if records:
            st.write(f"Found {len(records)} recent records for ID: {search_id}")
            for r in records:
                # Displays the date nicely to Admin
                with st.expander(f"{format_ui_date(r['date'])} | {r['shift']} | ₹{r['total_amount']}"):
                    st.json({"Liters": r['liters'], "Fat": r['fat'], "SNF": r['snf']})
                    if st.button(f"🗑️ Delete this record", key=str(r['_id'])):
                        collection.delete_one({"_id": r['_id']})
                        st.rerun() 
        else:
            st.info("No records found for this ID.")

    st.markdown("---")
    st.header("🚨 Bulk Delete by Date Range")
    st.error("Warning: This action will permanently erase data from the cloud. It cannot be undone.")
    
    col_del1, col_del2 = st.columns(2)
    bulk_start = col_del1.date_input("Start Date to Delete", key="bulk_start")
    bulk_end = col_del2.date_input("End Date to Delete", key="bulk_end")
    
    start_str = bulk_start.strftime('%Y-%m-%d')
    end_str = bulk_end.strftime('%Y-%m-%d')
    
    count_to_delete = collection.count_documents({"date": {"$gte": start_str, "$lte": end_str}})
    
    if count_to_delete > 0:
        st.write(f"**Found {count_to_delete} records** between {format_ui_date(start_str)} and {format_ui_date(end_str)}.")
        confirm_delete = st.checkbox(f"I am sure I want to permanently delete these {count_to_delete} records.")
        if confirm_delete:
            if st.button("🔥 Permanently Delete Records", type="primary"):
                collection.delete_many({"date": {"$gte": start_str, "$lte": end_str}})
                st.success(f"Successfully wiped {count_to_delete} records from the database!")
                st.rerun()
    else:
        st.info("No records found in this date range.")

# ==========================================
# TAB 3: DATABASE HEALTH
# ==========================================
with tab3:
    st.header("System Overview")
    if st.button("Refresh Stats"):
        total_records = collection.count_documents({})
        unique_customers = len(collection.distinct("customer_number"))
        st.metric(label="Total Database Records", value=total_records)
        st.metric(label="Total Registered Farmers", value=unique_customers)

# ==========================================
# TAB 4: MOBILE APP CONFIG
# ==========================================
with tab4:
    st.header("📱 Configure Mobile App Billing Cycles")
    
    st.subheader("➕ Create New Cycle")
    with st.form("new_cycle_form", clear_on_submit=True):
        cycle_name = st.text_input("Cycle Name (e.g., April Bill 1, Page 1)")
        col_start, col_end = st.columns(2)
        start_date = col_start.date_input("Start Date")
        end_date = col_end.date_input("End Date")
        
        submitted = st.form_submit_button("💾 Save Billing Cycle", type="primary")
        if submitted:
            if not cycle_name.strip():
                st.error("⚠️ Please enter a name for the billing cycle.")
            else:
                config_data = {
                    "page_id": cycle_name.strip(),
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "end_date": end_date.strftime('%Y-%m-%d')
                }
                page_configs.update_one({"page_id": cycle_name.strip()}, {"$set": config_data}, upsert=True)
                st.success(f"✅ '{cycle_name}' saved! It will now instantly appear on the mobile app.")
    
    st.markdown("---")
    st.subheader("📋 Active Billing Cycles")
    
    current_configs = list(page_configs.find({}))
    if current_configs:
        for c in current_configs:
            col_name, col_dates, col_del = st.columns([3, 3, 1])
            with col_name:
                st.markdown(f"**{c.get('page_id')}**")
            with col_dates:
                # Displays the active cycles in DD-MM-YYYY format
                st.write(f"📅 {format_ui_date(c.get('start_date'))}  to  {format_ui_date(c.get('end_date'))}")
            with col_del:
                if st.button("🗑️ Delete", key=f"del_cycle_{c['_id']}"):
                    page_configs.delete_one({"_id": c['_id']})
                    st.rerun() 
            st.divider()
    else:
        st.info("No billing cycles configured yet. Create one above!")