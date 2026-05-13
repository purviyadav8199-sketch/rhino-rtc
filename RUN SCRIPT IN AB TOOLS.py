from opcua import Client
import time
from datetime import datetime
import os
import sqlite3
import socket
import csv
from collections import deque

# ============================================
# PLC OPC UA endpoint
# ============================================
PLC_URL = "opc.tcp://192.168.12.190:4840"

# ============================================
# OPC UA Node IDs (SINGLE SOURCE OF TRUTH)
# ============================================
NODES = {
    "Cycle_No":              'ns=3;s="RTC NEW DATALOG"."Cycle_No"',
    "Comp_Preset":           'ns=3;s="RTC NEW DATALOG"."Comp_Preset"',
    "Sand_Temp":             'ns=3;s="RTC NEW DATALOG"."Sand_Temp"',
    "Corr_Preset":           'ns=3;s="RTC NEW DATALOG"."Corr_Preset"',
    "Readj_Water":           'ns=3;s="RTC NEW DATALOG"."Readj_Water"',
    "As1_Measure":           'ns=3;s="RTC NEW DATALOG"."As1_Measure"',
    "Corr_Water_AS1":        'ns=3;s="RTC NEW DATALOG"."Corr_Water_AS1"',
    "As2_Measure":           'ns=3;s="RTC NEW DATALOG"."As2_Measure"',
    "Corr_Water_AS2":        'ns=3;s="RTC NEW DATALOG"."Corr_Water_AS2"',
    "As3_Measure":           'ns=3;s="RTC NEW DATALOG"."As3_Measure"',
    "Corr_Water_As3":        'ns=3;s="RTC NEW DATALOG"."Corr_Water_As3"',
    "DEVIATION":             'ns=3;s="RTC NEW DATALOG"."DEVIATION"',
    "TotalWater":            'ns=3;s="RTC NEW DATALOG"."TotalWater"',
    "Cycle_Time":            'ns=3;s="RTC NEW DATALOG"."Cycle_Time"',
    "Gcs_Preset":            'ns=3;s="RTC NEW DATALOG"."Gcs_Preset"',
    "GCS_Measure":           'ns=3;s="RTC NEW DATALOG"."GCS_Measure"',
    "Weight_Bentonite_A":    'ns=3;s="RTC NEW DATALOG"."Weight_Bentonite_A"',
    "Weight_CoalDust":       'ns=3;s="RTC NEW DATALOG"."Weight_CoalDust"',
    "Weight_Old_Sand":       'ns=3;s="RTC NEW DATALOG"."Weight_Old_Sand"',
    "Output_Sand_Temp":      'ns=3;s="RTC NEW DATALOG"."Output_Sand_Temp"',
    "Final_Measure":         'ns=3;s="RTC NEW DATALOG"."Final_Measure"',
    "Rtc_First_Cycle_Start": 'ns=3;s="RTC NEW DATALOG"."Rtc_First_Cycle_Start"',
    "Sand_Too_Wet_Fault":    'ns=3;s="RTC NEW DATALOG"."Sand_Too_Wet_Fault"',
    "newsand_Weight":        'ns=3;s="RTC NEW DATALOG"."newsand_Weight"',
}

# ============================================
# COLUMN ORDER (CSV = SQLITE)
# ============================================
COLUMNS = ["timestamp"] + list(NODES.keys())

# ============================================
# CSV SETUP
# ============================================
CSV_FILE = "plc_data.csv"

def recreate_csv():
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()

if not os.path.exists(CSV_FILE):
    recreate_csv()
else:
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        header = next(csv.reader(f), [])
        if header != COLUMNS:
            print("⚠️ CSV mismatch → recreating CSV")
            recreate_csv()

# ============================================
# SQLITE SETUP
# ============================================
DB_FILE = "plc_data.db"
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS plc_data (
    timestamp TEXT
)
""")
conn.commit()

# ============================================
# ENSURE ALL COLUMNS EXIST
# ============================================
def ensure_columns():
    cursor.execute("PRAGMA table_info(plc_data)")
    existing = [c[1] for c in cursor.fetchall()]

    for col in COLUMNS:
        if col not in existing:
            cursor.execute(f'ALTER TABLE plc_data ADD COLUMN "{col}" TEXT')
            print(f"➕ Added column: {col}")

    conn.commit()

ensure_columns()

# ============================================
# BUFFER (NO DATA LOSS)
# ============================================
buffer = deque()

# ============================================
# FORMAT VALUES
# ============================================
def fmt(val):
    try:
        return round(float(val), 2)
    except:
        return val

# ============================================
# CONNECT PLC
# ============================================
def connect_plc():
    while True:
        try:
            print("🔌 Connecting PLC...")
            c = Client(PLC_URL)
            c.connect()
            print("✅ PLC Connected")
            return c
        except:
            time.sleep(2)

client = connect_plc()

# ============================================
# SAFE READ
# ============================================
def safe_read(nodeid):
    global client
    try:
        return fmt(client.get_node(nodeid).get_value())
    except (socket.error, OSError, ConnectionResetError):
        print("⚠️ PLC disconnected → reconnecting")
        try:
            client.disconnect()
        except:
            pass
        time.sleep(2)
        client = connect_plc()
        try:
            return fmt(client.get_node(nodeid).get_value())
        except:
            return None
    except:
        return None

# ============================================
# WRITE DATA
# ============================================
def write_row(row):
    ordered = {c: row.get(c) for c in COLUMNS}
    try:
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=COLUMNS).writerow(ordered)

        cursor.execute(
            f'INSERT INTO plc_data ({",".join(COLUMNS)}) VALUES ({",".join(["?"]*len(COLUMNS))})',
            [ordered[c] for c in COLUMNS]
        )
        conn.commit()
    except Exception as e:
        print("⚠️ Write failed → buffering", e)
        buffer.append(ordered)

def flush_buffer():
    while buffer:
        write_row(buffer.popleft())

# ============================================
# MAIN LOOP (CYCLE BASED)
# ============================================
last_cycle = None
print("📡 Logging started")

try:
    while True:
        cycle = safe_read(NODES["Cycle_No"])
        if cycle is None:
            time.sleep(1)
            continue

        if cycle != last_cycle:
            last_cycle = cycle
            print(f"🔄 New Cycle: {cycle}")

            row = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            for k, v in NODES.items():
                row[k] = safe_read(v)

            write_row(row)
            flush_buffer()

        time.sleep(1)

except KeyboardInterrupt:
    print("🛑 Stopped")
    try:
        client.disconnect()
    except:
        pass
    conn.close()
