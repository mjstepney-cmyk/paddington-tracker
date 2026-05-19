import os
import json
import threading
import time
import requests
import stomp
from flask import Flask, render_template_string
from datetime import datetime
from xml.etree import ElementTree as ET

app = Flask(__name__)

# --- Config ---
NR_USER = os.environ.get("NR_USER", "mjstepney@gmail.com")
NR_PASS = os.environ.get("NR_PASS", "Hobbes01!")
DARWIN_USER = os.environ.get("DARWIN_USER", "mjstepney@gmail.com")
DARWIN_PASS = os.environ.get("DARWIN_PASS", "Hobbes01!")

STOMP_HOST = "publicdatafeeds.networkrail.co.uk"
STOMP_PORT = 61618
TD_TOPIC = "/topic/TD_ALL_SIG_AREA"
DARWIN_URL = "https://lite.realtime.nationalrail.co.uk/OpenLDBWS/api/20220120"

TARGET_DESTINATIONS = ["PGN", "NTA"]
FROM_CRS = "PAD"
TD_AREA = "D3"  # Paddington TD area (NOT WY which is Euston/Camden)

# --- Berth map (from SMART database, D3 area) ---
# Format: berth_id -> (human label, is_platform, platform_number)
BERTH_MAP = {
    # Platform berths - R prefix (arrival/occupied)
    "R001": ("London Paddington", True, "1"),
    "R003": ("London Paddington", True, "2"),
    "R005": ("London Paddington", True, "3"),
    "R007": ("London Paddington", True, "4"),
    "R009": ("London Paddington", True, "5"),
    "R011": ("London Paddington", True, "6"),
    "R013": ("London Paddington", True, "7"),
    "R015": ("London Paddington", True, "8"),
    "R017": ("London Paddington", True, "9"),
    "R019": ("London Paddington", True, "10"),
    "R025": ("London Paddington", True, "11"),
    "R029": ("London Paddington", True, "12"),
    "R031": ("London Paddington", True, "14"),
    # Platform berths - A prefix (departure)
    "A001": ("London Paddington", True, "1"),
    "A003": ("London Paddington", True, "2"),
    "A005": ("London Paddington", True, "3"),
    "A007": ("London Paddington", True, "4"),
    "A009": ("London Paddington", True, "5"),
    "A011": ("London Paddington", True, "6"),
    "A013": ("London Paddington", True, "7"),
    "A015": ("London Paddington", True, "8"),
    "A017": ("London Paddington", True, "9"),
    "A019": ("London Paddington", True, "10"),
    "A025": ("London Paddington", True, "11"),
    "A029": ("London Paddington", True, "12"),
    "A031": ("London Paddington", True, "14"),
    # Numeric platform berths
    "0037": ("London Paddington", True, "1"),
    "0039": ("London Paddington", True, "1"),
    "0041": ("London Paddington", True, "11"),
    "0043": ("London Paddington", True, "11"),
    "0045": ("London Paddington", True, "11"),
    "0047": ("London Paddington", True, "11"),
    "6003": ("London Paddington", True, "1"),
    "6004": ("London Paddington", True, "1"),
    # Old Oak Common sidings/depot
    "0128": ("Old Oak Common Depot", False, None),
    "0130": ("Old Oak Common Depot", False, None),
    "0133": ("Old Oak Common", False, None),
    "0136": ("Old Oak Common Depot", False, None),
    "0138": ("Old Oak Common Depot", False, None),
    "0139": ("Old Oak Common Depot", False, None),
    "0140": ("Old Oak Common Depot", False, None),
    "0141": ("Old Oak Common", False, None),
    "0142": ("Old Oak Common Depot", False, None),
    "0143": ("Old Oak Common Depot", False, None),
    "0145": ("Old Oak Common", False, None),
    "0158": ("Old Oak Common Depot", False, None),
    "C311": ("Old Oak Common", False, None),
    "C313": ("Old Oak Common", False, None),
    "C315": ("Old Oak Common", False, None),
    "C410": ("Old Oak Common", False, None),
    "C412": ("Old Oak Common", False, None),
    # North Pole IEP depot (GWR IET stabling)
    "0098": ("North Pole Depot", False, None),
    "0100": ("North Pole Depot", False, None),
    "0101": ("North Pole Depot", False, None),
    "0103": ("North Pole Depot", False, None),
    "0119": ("North Pole Depot", False, None),
    "0125": ("North Pole Depot", False, None),
    "6010": ("North Pole Depot", False, None),
    "6012": ("North Pole Depot", False, None),
    "6014": ("North Pole Depot", False, None),
    "6016": ("North Pole Depot", False, None),
    "6018": ("North Pole Depot", False, None),
    "6020": ("North Pole Depot", False, None),
    # Approach berths
    "0026": ("Paddington Approach", False, None),
    "0028": ("Paddington Approach", False, None),
    "0030": ("Paddington Approach", False, None),
    "0032": ("Paddington Approach", False, None),
    "0034": ("Paddington Approach", False, None),
    "0036": ("Paddington Approach", False, None),
    "0052": ("Royal Oak", False, None),
    "0053": ("Royal Oak", False, None),
    "0054": ("Royal Oak", False, None),
    "0055": ("Royal Oak", False, None),
    "0057": ("Royal Oak Junction", False, None),
    "0059": ("Royal Oak Junction", False, None),
    "0070": ("Portobello Junction", False, None),
    "0072": ("Portobello Junction", False, None),
    "0074": ("Portobello Junction", False, None),
    "0076": ("Portobello Junction", False, None),
    "0078": ("Portobello Junction", False, None),
}

# --- Shared state ---
state = {
    "berths": {},
    "services": [],
    "last_td_update": None,
    "last_darwin_update": None,
    "td_connected": False,
    "raw_berths": {},
}
state_lock = threading.Lock()


# --- TD STOMP Listener ---
class TDListener(stomp.ConnectionListener):
    def on_connected(self, frame):
        with state_lock:
            state["td_connected"] = True
        print("TD feed connected")

    def on_disconnected(self):
        with state_lock:
            state["td_connected"] = False
        print("TD feed disconnected - will reconnect")

    def on_message(self, frame):
        try:
            messages = json.loads(frame.body)
            with state_lock:
                for msg in messages:
                    if "CA_MSG" in msg:
                        d = msg["CA_MSG"]
                        if d.get("area_id") != TD_AREA:
                            continue
                        headcode = d.get("descr", "").strip()
                        from_b = d.get("from", "").strip()
                        to_b = d.get("to", "").strip()
                        if headcode:
                            state["berths"][headcode] = to_b
                            state["raw_berths"][to_b] = headcode
                            if from_b in state["raw_berths"]:
                                del state["raw_berths"][from_b]
                        state["last_td_update"] = datetime.now()
                    elif "CB_MSG" in msg:
                        d = msg["CB_MSG"]
                        if d.get("area_id") != TD_AREA:
                            continue
                        headcode = d.get("descr", "").strip()
                        if headcode and headcode in state["berths"]:
                            del state["berths"][headcode]
                    elif "CC_MSG" in msg:
                        d = msg["CC_MSG"]
                        if d.get("area_id") != TD_AREA:
                            continue
                        headcode = d.get("descr", "").strip()
                        to_b = d.get("to", "").strip()
                        if headcode:
                            state["berths"][headcode] = to_b
                            state["raw_berths"][to_b] = headcode
                            state["last_td_update"] = datetime.now()
        except Exception as e:
            print(f"TD parse error: {e}")


def td_connect():
    conn = stomp.Connection(
        host_and_ports=[(STOMP_HOST, STOMP_PORT)],
        keepalive=True,
        heartbeats=(10000, 10000),
    )
    conn.set_listener("", TDListener())
    while True:
        try:
            conn.connect(NR_USER, NR_PASS, wait=True)
            conn.subscribe(destination=TD_TOPIC, id=1, ack="auto")
            print("Subscribed to TD feed")
            while conn.is_connected():
                time.sleep(5)
        except Exception as e:
            print(f"TD connection error: {e}")
        time.sleep(15)


# --- Darwin poller ---
def darwin_query():
    services = []
    for dest in TARGET_DESTINATIONS:
        soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:ns1="http://thalesgroup.com/RTTI/2016-02-16/ldb/"
  xmlns:ns2="http://thalesgroup.com/RTTI/2013-11-28/Token/types">
  <SOAP-ENV:Header>
    <ns2:AccessToken>
      <ns2:TokenValue>{DARWIN_USER}:{DARWIN_PASS}</ns2:TokenValue>
    </ns2:AccessToken>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <ns1:GetDepartureBoardRequest>
      <ns1:numRows>5</ns1:numRows>
      <ns1:crs>{FROM_CRS}</ns1:crs>
      <ns1:filterCrs>{dest}</ns1:filterCrs>
      <ns1:filterType>to</ns1:filterType>
      <ns1:timeWindow>240</ns1:timeWindow>
    </ns1:GetDepartureBoardRequest>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"""
        try:
            r = requests.post(
                DARWIN_URL,
                data=soap,
                headers={"Content-Type": "text/xml"},
                timeout=10,
            )
            root = ET.fromstring(r.text)
            ns = "http://thalesgroup.com/RTTI/2017-10-01/ldb/types"
            for svc in root.iter(f"{{{ns}}}service"):
                std = svc.findtext(f"{{{ns}}}std", "")
                etd = svc.findtext(f"{{{ns}}}etd", "")
                headcode = svc.findtext(f"{{{ns}}}trainid", "")
                dest_name = svc.findtext(f"{{{ns}}}destination/{{{ns}}}location/{{{ns}}}locationName", dest)
                platform = svc.findtext(f"{{{ns}}}platform", "")
                services.append({
                    "departs": std,
                    "etd": etd,
                    "headcode": headcode,
                    "destination": dest_name,
                    "darwin_platform": platform,
                })
        except Exception as e:
            print(f"Darwin query error for {dest}: {e}")
    return services


def darwin_poll():
    while True:
        svcs = darwin_query()
        with state_lock:
            state["services"] = svcs
            state["last_darwin_update"] = datetime.now()
        time.sleep(300)


# --- Location resolution ---
def resolve_location(headcode):
    berth = state["berths"].get(headcode)
    if not berth:
        return None, False, None
    mapped = BERTH_MAP.get(berth)
    if mapped:
        return mapped
    return f"Unknown berth: {berth}", False, None


# --- HTML ---
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="30">
<title>Paddington Tracker</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;500&display=swap');
  :root {
    --bg: #0a0a0f; --surface: #13131a; --border: #1e1e2e;
    --green: #00e676; --amber: #ffab00; --grey: #546e7a;
    --text: #e0e0e0; --dim: #607d8b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'DM Sans', sans-serif;
    min-height: 100vh; padding: 1.5rem 1rem 3rem; max-width: 480px; margin: 0 auto; }
  header { display: flex; align-items: baseline; gap: 0.5rem; margin-bottom: 2rem;
    border-bottom: 1px solid var(--border); padding-bottom: 1rem; }
  header h1 { font-family: 'Space Mono', monospace; font-size: 1rem; color: var(--dim);
    letter-spacing: 0.1em; text-transform: uppercase; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green);
    animation: pulse 2s infinite; flex-shrink: 0; margin-left: auto; }
  .dot.offline { background: var(--grey); animation: none; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .card { background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem; }
  .card-route { font-family: 'Space Mono', monospace; font-size: 0.7rem; color: var(--dim);
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.35rem; }
  .card-time { font-family: 'Space Mono', monospace; font-size: 2rem; font-weight: 700;
    line-height: 1; margin-bottom: 0.75rem; }
  .status-badge { display: inline-flex; align-items: center; gap: 0.4rem; font-size: 0.8rem;
    font-weight: 500; padding: 0.3rem 0.75rem; border-radius: 999px; margin-bottom: 0.75rem; }
  .status-green { background: rgba(0,230,118,0.12); color: var(--green); }
  .status-amber { background: rgba(255,171,0,0.12); color: var(--amber); }
  .status-grey  { background: rgba(84,110,122,0.12); color: var(--grey); }
  .platform-big { font-family: 'Space Mono', monospace; font-size: 3.5rem; font-weight: 700;
    color: var(--green); line-height: 1; margin: 0.5rem 0; }
  .location-label { font-size: 1rem; color: var(--amber); font-weight: 500; margin: 0.25rem 0; }
  .meta { font-size: 0.75rem; color: var(--dim); margin-top: 0.5rem; font-family: 'Space Mono', monospace; }
  .no-services { text-align: center; color: var(--dim); padding: 2rem 0; font-size: 0.9rem; }
  .system-row { display: flex; justify-content: space-between; font-size: 0.7rem;
    font-family: 'Space Mono', monospace; color: var(--dim); margin-top: 2rem;
    padding-top: 1rem; border-top: 1px solid var(--border); }
  .ok { color: var(--green); } .warn { color: var(--amber); }
</style>
</head>
<body>
<header>
  <h1>Paddington Tracker</h1>
  <div class="dot {{ 'ok' if td_connected else 'offline' }}"></div>
</header>
{% if not services %}
  <div class="no-services">No Paignton / Newton Abbot services in the next 4 hours.</div>
{% else %}
  {% for svc in services %}
  <div class="card">
    <div class="card-route">London Paddington → {{ svc.destination }}</div>
    <div class="card-time">{{ svc.departs }}{% if svc.etd and svc.etd != 'On time' %} <span style="font-size:1rem;color:var(--amber)">{{ svc.etd }}</span>{% endif %}</div>
    <div style="font-size:0.75rem;color:var(--dim);font-family:'Space Mono',monospace;margin-bottom:0.75rem">{{ svc.headcode }}</div>
    {% if svc.platform %}
      <div><span class="status-badge status-green">✓ Platform confirmed</span></div>
      <div class="platform-big">{{ svc.platform }}</div>
    {% elif svc.location %}
      <div><span class="status-badge status-amber">⬤ Located</span></div>
      <div class="location-label">{{ svc.location }}</div>
    {% else %}
      <div><span class="status-badge status-grey">◌ Not yet located</span></div>
    {% endif %}
    {% if svc.last_seen %}
    <div class="meta">TD last seen: {{ svc.last_seen }}</div>
    {% endif %}
  </div>
  {% endfor %}
{% endif %}
<div class="system-row">
  <span>TD: <span class="{{ 'ok' if td_connected else 'warn' }}">{{ 'live' if td_connected else 'offline' }}</span></span>
  <span>Darwin: {{ darwin_update }}</span>
  <span>{{ now }}</span>
</div>
</body>
</html>
"""


@app.route("/")
def index():
    with state_lock:
        svcs = state["services"]
        td_connected = state["td_connected"]
        darwin_update = state["last_darwin_update"].strftime("%H:%M") if state["last_darwin_update"] else "-"
        now = datetime.now().strftime("%H:%M:%S")
        enriched = []
        for svc in svcs:
            hc = svc.get("headcode", "")
            darwin_plat = svc.get("darwin_platform", "")
            location, is_platform, plat_num = resolve_location(hc)
            platform = darwin_plat or (plat_num if is_platform else "")
            last_seen = ""
            if hc in state["berths"] and state["last_td_update"]:
                last_seen = state["last_td_update"].strftime("%H:%M")
            enriched.append({
                **svc,
                "platform": platform,
                "location": location if not is_platform else "",
                "last_seen": last_seen,
            })
    return render_template_string(HTML, services=enriched, td_connected=td_connected,
                                  darwin_update=darwin_update, now=now)


@app.route("/berths")
def berths():
    with state_lock:
        return {"berths": state["berths"], "raw_berths": state["raw_berths"]}


@app.route("/smart")
def smart_debug():
    sample = {k: v for k, v in list(BERTH_MAP.items())[:10]}
    return {"total": len(BERTH_MAP), "td_area": TD_AREA, "sample": sample}


if __name__ == "__main__":
    threading.Thread(target=td_connect, daemon=True).start()
    threading.Thread(target=darwin_poll, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
