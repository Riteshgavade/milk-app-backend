import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta

# --- CONFIGURATION ---
# This connects to the database running on your laptop. No password needed.
MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "milk_collection_db"

async def reset_database():
    print(f"🔌 Connecting to local database at: {MONGO_URL}...")
    
    try:
        # Connect
        client = AsyncIOMotorClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        
        # Test connection
        await client.server_info()
        print("✅ Connection Successful!")

        # 1. CLEAR OLD DATA (The "Replace" part)
        print("🧹 Clearing old data...")
        await db.customers.delete_many({})
        await db.morning_collection.delete_many({})
        await db.evening_collection.delete_many({})
        print("   -> Old records deleted.")

        # 2. INSERT USER (Ramesh)
        print("👤 Creating user: Ramesh Kumar...")
        ramesh = {
            "customer_name": "Ramesh Kumar",
            "customer_number": "101",
            "mobile_number": "9876543210",
            "created_at": datetime.utcnow()
        }
        await db.customers.insert_one(ramesh)
        
        # 3. INSERT MILK RECORDS (Last 5 days)
        print("🥛 Adding milk records...")
        records = []
        for i in range(5):
            # Create a date for 'i' days ago
            record_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            
            records.append({
                "customer_name": "Ramesh Kumar",
                "customer_number": "101",
                "liters": 5.0 + i,       # 5.0, 6.0, etc.
                "fat": 4.0 + (i * 0.1),  # 4.0, 4.1, etc.
                "snf": 8.5,
                "rate": 32.0,
                "total_amount": (5.0 + i) * 32.0,
                "date": record_date,
                "created_at": datetime.utcnow()
            })
        
        await db.morning_collection.insert_many(records)
        print(f"   -> Added {len(records)} records for Ramesh.")
        
        print("\n🎉 SUCCESS! Your database is ready.")
        print("   Login with: Name='Ramesh Kumar', Number='101'")

    except Exception as e:
        print(f"\n❌ ERROR: Could not connect to MongoDB.")
        print(f"   Reason: {e}")
        print("   -> Did you install MongoDB Community Server? Is it running?")

if __name__ == "__main__":
    asyncio.run(reset_database())