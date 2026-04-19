import pandas as pd
from pymongo import MongoClient
import certifi

# 1. CONNECT TO DATABASE
# Update the password below if it is not 'milk_admin'
MONGO_URL = "mongodb+srv://milk_admin:milk_admin@cluster0.p3vd1zt.mongodb.net/?appName=Cluster0"

client = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
db = client.get_database("milk_collection_db")

# We will put EVERYTHING into one collection to make the Morning/Evening toggle work easily!
collection = db["milk_records"] 

print("🔍 Reading and Cleaning CSV Data...")

# 2. LOAD DATA
df = pd.read_csv("Book1.csv")

# 3. CLEANING FUNCTION (Removes '?' and commas from numbers)
def clean_number(value):
    if pd.isna(value): 
        return 0.0
    clean_str = str(value).replace('?', '').replace('₹', '').replace(',', '').strip()
    try:
        return float(clean_str)
    except ValueError:
        return 0.0

# 4. PREPARE RECORDS
records_to_upload = []

for index, row in df.iterrows():
    if pd.isna(row['Customer Number']):
        continue
        
    customer_num = str(int(row['Customer Number']))
    customer_name = str(row['Name']).strip()
    
    # --- FIX: Format the Date safely without crashing on NaT ---
    raw_date = row.get('Date')
    dt_obj = pd.to_datetime(raw_date, errors='coerce')
    
    # If the date is invalid or completely missing, skip this row
    if pd.isna(dt_obj):
        continue 
        
    clean_date = dt_obj.strftime('%Y-%m-%d')
    
    # --- MORNING SHIFT ---
    morning_liters = clean_number(row.get('Morning_Liter', 0))
    if morning_liters > 0:
        records_to_upload.append({
            "customer_number": customer_num,
            "customer_name": customer_name,
            "date": clean_date,
            "shift": "Morning",
            "liters": morning_liters,
            "fat": clean_number(row.get('Morning_Fat', 0)),
            "snf": clean_number(row.get('Morning_SNF', 0)),
            "rate": clean_number(row.get('Rate', 0)),
            "total_amount": clean_number(row.get('Amount', 0))
        })

    # --- EVENING/NIGHT SHIFT ---
    night_liters = clean_number(row.get('Night_Liter', 0))
    if night_liters > 0:
        records_to_upload.append({
            "customer_number": customer_num,
            "customer_name": customer_name,
            "date": clean_date,
            "shift": "Evening",
            "liters": night_liters,
            "fat": clean_number(row.get('Night_Fat', 0)),
            "snf": clean_number(row.get('Night_SNF', 0)),
            "rate": clean_number(row.get('Rate.1', 0)),
            "total_amount": clean_number(row.get('Amount.1', 0))
        })

# 5. UPLOAD TO DATABASE
if records_to_upload:
    print(f"🚀 Uploading {len(records_to_upload)} total records (Morning + Evening) to MongoDB...")
    collection.insert_many(records_to_upload)
    print("✅ Upload Complete! All your historical data is now in the cloud.")
else:
    print("❌ No valid data found to upload.")