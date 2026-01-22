import minimalmodbus
import time
import json
import sqlite3
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import os
import sys

# --- Meter configuration ---
METER_IDS = [7, 8, 9, 10, 11]
DEVICE = '/dev/ttyUSB0'
BAUDRATE = 9600

register_map = {
    3009: "Avg VLN",
    3011: "Avg Current",
    3029: "kWh",
    3005: "Avg PF"
}
multipliers = {
    3009: 100,
    3011: 100,
    3029: 100,
    3041: 100,
    3005: 1000
}

# --- MQTT Configuration ---
MQTT_BROKER = "mqtt.sworks.co.in"
MQTT_PORT = 1883
MQTT_TOPIC = "smart-meter/"
MQTT_USER = "sworks"
MQTT_PASSWORD = "S@works@1231"

# --- Database ---
DB_FILE = "meter_data.db"

# --- Watchdog ping file ---
WATCHDOG_PING = "/tmp/watchdog-ping"

# --- MQTT Client State ---
mqtt_connected = False
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = (rc == 0)
    print("MQTT Connected!" if mqtt_connected else f"MQTT failed with code {rc}")

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    print("MQTT Disconnected")

client.on_connect = on_connect
client.on_disconnect = on_disconnect

# --- Initialize DB ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS meter_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        meter_id INTEGER,
        avg_vln REAL,
        avg_current REAL,
        kwh REAL,
        avg_pf REAL,
        source TEXT,
        sent_status INTEGER DEFAULT 0
    )''')
    conn.commit()
    conn.close()

# --- Clean DB older than 1 day ---
def clean_old_data():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    yesterday = datetime.now() - timedelta(days=1)
    cursor.execute("DELETE FROM meter_readings WHERE timestamp < ?", (yesterday.isoformat(),))
    conn.commit()
    conn.close()
    print("Old data cleaned.")

# --- Read data from one meter ---
def read_meter_data(meter_id):
    instrument = minimalmodbus.Instrument(DEVICE, meter_id)
    instrument.serial.baudrate = BAUDRATE
    instrument.serial.bytesize = 8
    instrument.serial.parity = minimalmodbus.serial.PARITY_NONE
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 1
    instrument.mode = minimalmodbus.MODE_RTU

    data = {"timestamp": datetime.now().isoformat(), "meter_id": meter_id}

    for reg, label in register_map.items():
        try:
            if reg == 3005:
                raw = instrument.read_long(reg, functioncode=3, signed=False)
                pf_value = raw / multipliers[reg]
                if pf_value > 1:
                    pf_value = (raw - multipliers[reg]) / multipliers[reg]
                value = round(pf_value, 3)
            else:
                raw = instrument.read_long(reg, functioncode=3, signed=True)
                value = round(raw / multipliers[reg], 2)
            data[label] = value
        except Exception as e:
            print(f"Error reading {label} from meter {meter_id}: {e}")
            data[label] = None
        time.sleep(0.1)
    return data

# --- Store to DB ---
def store_data_to_db(data, source="live"):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO meter_readings (
        timestamp, meter_id, avg_vln, avg_current, kwh, avg_pf, source, sent_status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)''', (
        data["timestamp"], data["meter_id"], data.get("Avg VLN"),
        data.get("Avg Current"), data.get("kWh"), data.get("Avg PF"), source
    ))
    conn.commit()
    conn.close()

# --- Resend unsent offline data ---
def resend_unsent_data():
    if not mqtt_connected:
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM meter_readings WHERE sent_status = 0')
    rows = cursor.fetchall()
    for row in rows:
        data = {
            "timestamp": row[1],
            "meter_id": row[2],
            "Avg VLN": row[3],
            "Avg Current": row[4],
            "kWh": row[5],
            "Avg PF": row[6],
            "source": row[7]
        }
        try:
            payload = json.dumps(data)
            client.publish(MQTT_TOPIC, payload)
            cursor.execute('UPDATE meter_readings SET sent_status = 1 WHERE id = ?', (row[0],))
            print(f"Resent offline data: {data}")
        except Exception as e:
            print(f"Failed to resend offline data: {e}")
    conn.commit()
    conn.close()

# --- Read & Send data batch ---
def batch_read_and_send():
    for meter_id in METER_IDS:
        data = read_meter_data(meter_id)
        print(f"Meter {meter_id} data: {data}")

        if mqtt_connected:
            try:
                data["source"] = "live"
                payload = json.dumps(data)
                client.publish(MQTT_TOPIC, payload)
                print(f"Published live data for meter {meter_id}")
                store_data_to_db(data, source="live")
            except Exception as e:
                print(f"MQTT publish failed: {e}")
                data["source"] = "offline"
                store_data_to_db(data, source="offline")
        else:
            data["source"] = "offline"
            store_data_to_db(data, source="offline")
            print("MQTT offline. Data stored for later.")

# --- Watchdog Ping File ---
def ping_watchdog():
    try:
        with open(WATCHDOG_PING, "w") as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        print(f"Watchdog ping failed: {e}")

# --- Main ---
if __name__ == "__main__":
    init_db()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    try:
        while True:
            print(f"\n=== Reading started at {datetime.now().isoformat()} ===")
            batch_read_and_send()
            resend_unsent_data()
            clean_old_data()
            ping_watchdog()
            time.sleep(300)  # 5 minutes
    except KeyboardInterrupt:
        print("Stopped manually.")
    finally:
        client.loop_stop()
        client.disconnect()
