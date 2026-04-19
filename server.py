import os
import certifi
from dotenv import load_dotenv  
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from datetime import datetime

# Load hidden variables
load_dotenv()

app = FastAPI()

# 1. DATABASE CONNECTION
MONGO_URL = os.getenv("MONGO_URL")
print("🔥 Connecting to Cloud Database...")

client = AsyncIOMotorClient(
    MONGO_URL,
    tlsCAFile=certifi.where(),
    tlsAllowInvalidCertificates=True,
    tls=True
)
db = client.get_database("milk_collection_db")

# 2. DATA MODELS
class LoginRequest(BaseModel):
    dairy_code: str
    phone_number: str
    customer_number: str

class RecordsRequest(BaseModel):
    customer_number: str
    shift: str

class DeveloperPageRequest(BaseModel):
    customer_number: str
    shift: str
    page_name: str

class ShiftRequest(BaseModel):
    customer_number: str

class ShiftDetailsRequest(BaseModel):
    customer_number: str
    date: str
    shift: str

class DateRangeRequest(BaseModel):
    customer_number: str
    start_date: str
    end_date: str


# --- 🧠 SUPER BULLETPROOF DATE TRANSLATOR ---
def parse_smart_date(date_val):
    if not date_val: return None
    if isinstance(date_val, datetime): return date_val
        
    date_str = str(date_val).strip().split(" ")[0]
    formats = ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%y", "%d/%m/%y", "%Y/%m/%d", "%m/%d/%Y"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
            
    print(f"⚠️ DATE ERROR: Python couldn't read -> '{date_str}'")
    return None

# 👇 UI FORMATTER (Flips any date into DD-MM-YYYY)
def to_ddmmyyyy(date_val):
    dt = parse_smart_date(date_val)
    if dt:
        return dt.strftime("%d-%m-%Y")
    return str(date_val)


# 3. API ROUTES

# --- 🔒 SMART SECURE LOGIN ROUTE ---
@app.post("/api/login")
async def login(data: LoginRequest):
    try:
        # Check if all 3 fields are filled
        if not data.dairy_code or not data.phone_number or not data.customer_number:
            return {"success": False, "message": "Please fill in all details."}

        # Query MongoDB requiring exact matches for Dairy Code and Phone Number
        search_query = {
            "dairy_code": data.dairy_code.strip(),
            "phone_number": data.phone_number.strip()
        }

        # Add the Customer Number (checking for both String and Integer formats in DB)
        cust_query = [{"customer_number": data.customer_number}]
        if data.customer_number.isdigit():
            cust_query.append({"customer_number": int(data.customer_number)})
        search_query["$or"] = cust_query

        # Execute Search
        user = await db.milk_records.find_one(search_query)
        
        if user:
            return {
                "success": True,
                "customer_name": user.get("customer_name", "Farmer"), # Returns Name for Dashboard
                "customer_number": str(user.get("customer_number"))
            }
        else:
            return {"success": False, "message": "Invalid Dairy Code, Phone Number, or Customer Number."}
            
    except Exception as e:
        print(f"Login Error: {e}")
        return {"success": False, "message": "Server Error"}


# --- 🔄 GET BILLING CYCLES FROM STREAMLIT ---
@app.get("/api/get_billing_cycles")
async def get_billing_cycles():
    try:
        cursor = db.page_configs.find({})
        configs = await cursor.to_list(length=50)
        
        cycles = []
        for c in configs:
            cycles.append({
                "id": str(c.get("_id", "")),
                "label": c.get("page_id", "New Bill"),
                "start_date": to_ddmmyyyy(c.get("start_date", "")),
                "end_date": to_ddmmyyyy(c.get("end_date", ""))
            })
            
        return {"success": True, "cycles": cycles}
    except Exception as e:
        print(f"Error fetching cycles: {e}")
        return {"success": False, "cycles": []}


# --- 📅 GET RECENT SHIFTS FOR DASHBOARD (Purple Grid) ---
@app.post("/api/get_recent_shifts")
async def get_recent_shifts(data: ShiftRequest):
    try:
        cursor = db.milk_records.find({"customer_number": str(data.customer_number)}).sort("_id", -1).limit(200)
        records = await cursor.to_list(length=200)
        
        seen_shifts = set()
        recent_shifts = []
        
        for r in records:
            raw_date = r.get("date")
            shift = r.get("shift", "").capitalize() 
            
            pair = (raw_date, shift)
            if pair not in seen_shifts:
                seen_shifts.add(pair)
                label = "सकाळ" if shift.lower() == "morning" else "संध्याकाळ"
                
                recent_shifts.append({
                    "id": f"{raw_date}_{shift}",
                    "date": to_ddmmyyyy(raw_date),
                    "shift": shift,
                    "label": label
                })
                
                if len(recent_shifts) >= 14: break
                    
        return {"success": True, "shifts": recent_shifts}
    except Exception as e:
        return {"success": False, "shifts": []}


# --- 🔍 GET EXACT DETAILS FOR ONE SHIFT ---
@app.post("/api/get_shift_details")
async def get_shift_details(data: ShiftDetailsRequest):
    try:
        cursor = db.milk_records.find({
            "customer_number": str(data.customer_number),
            "date": data.date, 
            "shift": {"$regex": f"^{data.shift}$", "$options": "i"} 
        })
        records = await cursor.to_list(length=50)
        
        formatted_records = []
        for doc in records:
            formatted_records.append({
                "id": str(doc.get("_id", "")),
                "date": to_ddmmyyyy(doc.get("date", "")),
                "liters": doc.get("liters", 0.0),
                "fat": doc.get("fat", 0.0),
                "snf": doc.get("snf", 0.0),
                "rate": doc.get("rate", 0.0),
                "amount": doc.get("total_amount", 0.0)
            })
            
        return {"success": True, "records": formatted_records}
    except Exception as e:
        return {"success": False, "records": []}


# --- 📅 DATE RANGE ROUTE WITH KAPAT ---
@app.post("/api/get_date_range_records")
async def get_date_range_records(data: DateRangeRequest):
    try:
        start_dt = parse_smart_date(data.start_date)
        end_dt = parse_smart_date(data.end_date)
        
        if not start_dt or not end_dt: return {"success": False, "records": []}
        if start_dt > end_dt: start_dt, end_dt = end_dt, start_dt

        search_query = [{"customer_number": data.customer_number}]
        if str(data.customer_number).isdigit():
            search_query.append({"customer_number": int(data.customer_number)})

        cursor = db.milk_records.find({"$or": search_query})
        all_records = await cursor.to_list(length=1000)
        
        valid_records = []
        for doc in all_records:
            raw_db_date = doc.get("date", "")
            record_dt = parse_smart_date(raw_db_date)
            
            if record_dt and (start_dt <= record_dt <= end_dt):
                valid_records.append({
                    "id": str(doc.get("_id", "")),
                    "date": to_ddmmyyyy(raw_db_date),
                    "shift": doc.get("shift", ""),
                    "liters": doc.get("liters", 0.0),
                    "fat": doc.get("fat", 0.0),
                    "snf": doc.get("snf", 0.0),
                    "rate": doc.get("rate", 0.0),
                    "amount": doc.get("total_amount", 0.0),
                    "kapat_name": doc.get("kapat_name", ""),
                    "kapat_amount": doc.get("kapat_amount", 0.0),
                    "keleli_kapat": doc.get("keleli_kapat", 0.0)
                })
        
        valid_records.sort(key=lambda x: parse_smart_date(x["date"]), reverse=True)
        return {"success": True, "records": valid_records}
        
    except Exception as e:
        print(f"❌ Error fetching range: {e}")
        return {"success": False, "records": []}


# --- 📋 MAIN RECORDS ROUTE (Legacy fallback) ---
@app.post("/api/get_records")
async def get_records(data: RecordsRequest):
    cursor = db.milk_records.find({
        "customer_number": data.customer_number,
        "shift": data.shift
    }).sort("date", -1).limit(100)
    
    shift_records = await cursor.to_list(length=100)
    shift_amount = sum(doc.get("total_amount", 0) for doc in shift_records)
    
    all_cursor = db.milk_records.find({
        "customer_number": data.customer_number
    })
    all_records = await all_cursor.to_list(length=500)
    combined_amount = sum(doc.get("total_amount", 0) for doc in all_records)

    formatted_records = []
    for doc in shift_records:
        formatted_records.append({
            "date": to_ddmmyyyy(doc.get("date", "")),
            "liters": doc.get("liters", 0.0),
            "fat": doc.get("fat", 0.0),
            "snf": doc.get("snf", 0.0),
            "rate": doc.get("rate", 0.0),
            "amount": doc.get("total_amount", 0.0)
        })
        
    return {
        "total_records": len(formatted_records),
        "shift_amount": round(shift_amount, 2),
        "combined_amount": round(combined_amount, 2),
        "records": formatted_records
    }


# --- 🛠️ DEVELOPER CONTROLLED PAGES ROUTE (Legacy fallback) ---
@app.post("/api/get_developer_page")
async def get_developer_page(data: DeveloperPageRequest):
    config = await db.page_configs.find_one({"page_id": data.page_name})
    
    if not config:
        return {
            "total_records": 0, "shift_amount": 0, 
            "combined_amount": 0, "records": []
        }
        
    start_date = config.get("start_date")
    end_date = config.get("end_date")

    query = {
        "customer_number": data.customer_number,
        "shift": data.shift,
        "date": {"$gte": start_date, "$lte": end_date}
    }
    
    cursor = db.milk_records.find(query).sort("date", -1).limit(100)
    shift_records = await cursor.to_list(length=100)
    
    shift_amount = sum(doc.get("total_amount", 0) for doc in shift_records)
    
    all_cursor = db.milk_records.find({
        "customer_number": data.customer_number,
        "date": {"$gte": start_date, "$lte": end_date}
    })
    all_records = await all_cursor.to_list(length=500)
    combined_amount = sum(doc.get("total_amount", 0) for doc in all_records)

    formatted_records = []
    for doc in shift_records:
        formatted_records.append({
            "date": to_ddmmyyyy(doc.get("date", "")),
            "liters": doc.get("liters", 0.0),
            "fat": doc.get("fat", 0.0),
            "snf": doc.get("snf", 0.0),
            "rate": doc.get("rate", 0.0),
            "amount": doc.get("total_amount", 0.0)
        })
        
    return {
        "total_records": len(formatted_records),
        "shift_amount": round(shift_amount, 2),
        "combined_amount": round(combined_amount, 2),
        "records": formatted_records
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)