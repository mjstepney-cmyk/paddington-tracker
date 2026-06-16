import os
import json
import threading
import time
import requests
import stomp
from flask import Flask, render_template_string
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

# --- Config ---
NR_USER   = os.environ.get("NR_USER",      "mjstepney@gmail.com")
NR_PASS   = os.environ.get("NR_PASS",      "Hobbes01!")
DARWIN_KEY= os.environ.get("DARWIN_APIKEY","qgZNj5JTagKo1hKzcGpRhYgGImlSSsMiA1uHW5LKcOmgaRGH")
RTT_USER  = os.environ.get("RTT_USER",     "")
RTT_PASS  = os.environ.get("RTT_PASS",     "")

STOMP_HOST = "publicdatafeeds.networkrail.co.uk"
STOMP_PORT = 61618
TD_TOPIC   = "/topic/TD_ALL_SIG_AREA"
TD_AREA    = "D3"
LONDON_TZ  = ZoneInfo("Europe/London")
FROM_CRS   = "PAD"

DARWIN_BASE = "https://api1.raildata.org.uk/1010-live-departure-board-dep1_2/LDBWS/api/20220120/GetDepBoardWithDetails"
RTT_BASE    = "https://api.rtt.io/api/v1/json/search"

# Browser-like User-Agent — RDM's gateway 403s bare python-requests UA
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# Verified GWR westbound destinations from Paddington
WESTBOUND_CRS = {
    "PGN","NTA","PLY","PNZ",           # Devon/Cornwall
    "EXD","TAU",                        # Exeter/Taunton
    "BRI","NWP","WSM",                  # Bristol/Newport/Weston
    "CDF","SWA",                        # Cardiff/Swansea
    "OXF","CHM","CBN",                  # Oxford/Cheltenham/Chippenham
    "BPW",                              # Bridgwater
}
LOCAL_CRS = {"RDG","SLO","MAI","TWY","IVR","HAY","HWV","THA","PAD"}
EXCLUDE_OPERATORS = {"HX","XR","CC"}

# --- Berth map (D3 area - Paddington + approaches) ---
BERTH_MAP = {
    "R001":("London Paddington",True,"1"),  "R003":("London Paddington",True,"2"),
    "R005":("London Paddington",True,"3"),  "R007":("London Paddington",True,"4"),
    "R009":("London Paddington",True,"5"),  "R011":("London Paddington",True,"6"),
    "R013":("London Paddington",True,"7"),  "R015":("London Paddington",True,"8"),
    "R017":("London Paddington",True,"9"),  "R019":("London Paddington",True,"10"),
    "R025":("London Paddington",True,"11"), "R029":("London Paddington",True,"12"),
    "R031":("London Paddington",True,"14"),
    "A001":("London Paddington",True,"1"),  "A003":("London Paddington",True,"2"),
    "A005":("London Paddington",True,"3"),  "A007":("London Paddington",True,"4"),
    "A009":("London Paddington",True,"5"),  "A011":("London Paddington",True,"6"),
    "A013":("London Paddington",True,"7"),  "A015":("London Paddington",True,"8"),
    "A017":("London Paddington",True,"9"),  "A019":("London Paddington",True,"10"),
    "A025":("London Paddington",True,"11"), "A029":("London Paddington",True,"12"),
    "A031":("London Paddington",True,"14"),
    "0037":("London Paddington",True,"1"),  "0039":("London Paddington",True,"1"),
    "0041":("London Paddington",True,"11"), "0043":("London Paddington",True,"11"),
    "0045":("London Paddington",True,"11"), "0047":("London Paddington",True,"11"),
    "6003":("London Paddington",True,"1"),  "6004":("London Paddington",True,"1"),
    "0128":("Old Oak Common Depot",False,None), "0130":("Old Oak Common Depot",False,None),
    "0133":("Old Oak Common",False,None),   "0136":("Old Oak Common Depot",False,None),
    "0138":("Old Oak Common Depot",False,None),"0139":("Old Oak Common Depot",False,None),
    "0140":("Old Oak Common Depot",False,None),"0141":("Old Oak Common",False,None),
    "0142":("Old Oak Common Depot",False,None),"0143":("Old Oak Common Depot",False,None),
    "0145":("Old Oak Common",False,None),   "0158":("Old Oak Common Depot",False,None),
    "C311":("Old Oak Common",False,None),   "C313":("Old Oak Common",False,None),
    "C315":("Old Oak Common",False,None),   "C410":("Old Oak Common",False,None),
    "C412":("Old Oak Common",False,None),
    "0098":("North Pole Depot",False,None), "0100":("North Pole Depot",False,None),
    "0101":("North Pole Depot",False,None), "0103":("North Pole Depot",False,None),
    "0119":("North Pole Depot",False,None), "0125":("North Pole Depot",False,None),
    "6010":("North Pole Depot",False,None), "6012":("North Pole Depot",False,None),
    "6014":("North Pole Depot",False,None), "6016":("North Pole Depot",False,None),
    "6018":("North Pole Depot",False,None), "6020":("North Pole Depot",False,None),
    "0026":("Paddington Approach",False,None),"0028":("Paddington Approach",False,None),
    "0030":("Paddington Approach",False,None),"0032":("Paddington Approach",False,None),
    "0034":("Paddington Approach",False,None),"0036":("Paddington Approach",False,None),
    "0052":("Royal Oak",False,None),        "0053":("Royal Oak",False,None),
    "0054":("Royal Oak",False,None),        "0055":("Royal Oak",False,None),
    "0057":("Royal Oak Junction",False,None),"0059":("Royal Oak Junction",False,None),
    "0070":("Portobello Junction",False,None),"0072":("Portobello Junction",False,None),
    "0074":("Portobello Junction",False,None),"0076":("Portobello Junction",False,None),
    "0078":("Portobello Junction",False,None),
}

# --- Shared state ---
state = {
    "berths": {},
    "raw_berths": {},
    "darwin": [],
    "rtt": [],
    "last_td": None,
    "last_darwin": None,
    "last_rtt": None,
    "td_connected": False,
    "darwin_error": "",
    "rtt_error": "",
}
lock = threading.Lock()


def now_london():
    return datetime.now(LONDON_TZ)


def fmt_time(dt):
    return dt.strftime("%H:%M") if dt else "-"


# --- TD STOMP ---
class TDListener(stomp.ConnectionListener):
    def on_connected(self, frame):
        with lock: state["td_connected"] = True
        print("TD connected")

    def on_disconnected(self):
        with lock: state["td_connected"] = False
        print("TD disconnected")

    def on_message(self, frame):
        try:
            messages = json.loads(frame.body)
        except Exception:
            return
        with lock:
            for msg in messages:
                for mtype, d in msg.items():
                    if d.get("area_id") != TD_AREA:
                        continue
                    hc     = d.get("descr","").strip()
                    from_b = d.get("from","").strip()
                    to_b   = d.get("to","").strip()
                    if mtype == "CA_MSG":
                        if hc:
                            state["berths"][hc] = to_b
                            state["raw_berths"][to_b] = hc
                            if from_b in state["raw_berths"]:
                                del state["raw_berths"][from_b]
                        state["last_td"] = now_london()
                    elif mtype == "CB_MSG":
                        if hc:
                            state["berths"].pop(hc, None)
                    elif mtype == "CC_MSG":
                        if hc:
                            state["berths"][hc] = to_b
                            state["raw_berths"][to_b] = hc
                        state["last_td"] = now_london()


def td_thread():
    conn = stomp.Connection(
        host_and_ports=[(STOMP_HOST, STOMP_PORT)],
        keepalive=True, heartbeats=(10000, 10000),
    )
    conn.set_listener("", TDListener())
    while True:
        try:
            conn.connect(NR_USER, NR_PASS, wait=True)
            conn.subscribe(destination=TD_TOPIC, id=1, ack="auto")
            print("TD subscribed")
            while conn.is_connected():
                time.sleep(5)
        except Exception as e:
            print(f"TD error: {e}")
        time.sleep(15)


# --- Darwin poller via RDM LDBWS (GetDepBoardWithDetails) ---
# Single call returns full PAD board WITH calling points, so we filter in code.
# Browser User-Agent avoids the gateway 403 that bare python-requests triggers.
def darwin_poll():
    while True:
        svcs = []
        error = ""
        try:
            url = f"{DARWIN_BASE}/{FROM_CRS}"
            r = requests.get(
                url,
                headers={"x-apikey": DARWIN_KEY, "User-Agent": BROWSER_UA},
                params={"numRows": 50, "timeWindow": 120},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            raw = data.get("trainServices") or []
            for s in raw:
                op_code = s.get("operatorCode","")
                if op_code in EXCLUDE_OPERATORS:
                    continue

                dest_list = s.get("destination") or []
                dest_crs  = dest_list[0].get("crs","") if dest_list else ""
                dest_name = dest_list[0].get("locationName","?") if dest_list else "?"

                # Collect this service's calling-point CRS codes
                call_crs = set()
                for cpl in (s.get("subsequentCallingPoints") or []):
                    for cp in (cpl.get("callingPoint") or []):
                        c = cp.get("crs")
                        if c:
                            call_crs.add(c)

                # Keep if final destination OR any calling point is a westbound target
                if dest_crs in WESTBOUND_CRS or (call_crs & WESTBOUND_CRS):
                    pass
                elif dest_crs in LOCAL_CRS:
                    continue
                else:
                    continue

                hc = s.get("trainid","") or s.get("serviceID","")
                svcs.append({
                    "std":      s.get("std",""),
                    "etd":      s.get("etd",""),
                    "platform": s.get("platform","") or "",
                    "headcode": hc,
                    "operator": s.get("operator",""),
                    "dest":     dest_name,
                    "dest_crs": dest_crs,
                    "source":   "darwin",
                })
        except Exception as e:
            error = str(e)
            print(f"Darwin error: {e}")
        svcs.sort(key=lambda x: x["std"])
        with lock:
            state["darwin"] = svcs
            state["last_darwin"] = now_london()
            state["darwin_error"] = error if not svcs else ""
        time.sleep(60)


# --- RTT poller (full day, scheduled only) ---
def rtt_poll():
    while True:
        if not (RTT_USER and RTT_PASS):
            time.sleep(60)
            continue
        try:
            today = now_london().strftime("%Y/%m/%d")
            r = requests.get(
                f"{RTT_BASE}/{FROM_CRS}",
                auth=(RTT_USER, RTT_PASS),
                params={"date": today},
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            svcs = []
            for s in (data.get("services") or []):
                loc = s.get("locationDetail",{})
                if loc.get("isCall") and not loc.get("isPublicDeparture"):
                    continue
                dest_list = s.get("destination",[])
                if not dest_list:
                    continue
                dest_crs  = dest_list[-1].get("crs","")
                dest_name = dest_list[-1].get("description","?")
                op = s.get("atocCode","")
                if op in EXCLUDE_OPERATORS:
                    continue
                if dest_crs in LOCAL_CRS:
                    continue
                if dest_crs not in WESTBOUND_CRS:
                    continue
                std = loc.get("gbttBookedDeparture","") or loc.get("publicDeparture","")
                if len(std) == 4:
                    std = std[:2]+":"+std[2:]
                svcs.append({
                    "std":      std,
                    "etd":      "",
                    "platform": "",
                    "headcode": s.get("trainIdentity",""),
                    "operator": s.get("atocName",""),
                    "dest":     dest_name,
                    "dest_crs": dest_crs,
                    "source":   "rtt",
                })
            svcs.sort(key=lambda x: x["std"])
            with lock:
                state["rtt"] = svcs
                state["last_rtt"] = now_london()
                state["rtt_error"] = ""
        except Exception as e:
            with lock: state["rtt_error"] = str(e)
            print(f"RTT error: {e}")
        time.sleep(600)


# --- Location resolution ---
def resolve(headcode):
    berth = state["berths"].get(headcode)
    if not berth:
        return None, False, None
    mapped = BERTH_MAP.get(berth)
    if mapped:
        return mapped
    return f"Berth {berth}", False, None


# --- Build merged board ---
def build_board():
    now = now_london()
    darwin_hcs = set()
    darwin_rows = []

    for s in state["darwin"]:
        hc = s["headcode"]
        darwin_hcs.add(hc)
        location, is_platform, plat_num = resolve(hc)
        platform = s["platform"] or (plat_num if is_platform else "")
        if platform or is_platform:
            status = "green"
        elif location:
            status = "amber"
        else:
            status = "grey"
        delay = ""
        if s["etd"] and s["etd"] not in ("On time",""):
            delay = s["etd"]
        darwin_rows.append({**s,
            "platform": platform,
            "siding": location if not is_platform else "",
            "status": status,
            "delay": delay,
        })

    rtt_rows = []
    for s in state["rtt"]:
        if s["headcode"] in darwin_hcs:
            continue
        try:
            h,m = s["std"].split(":")
            dep = now.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
            if dep < now - timedelta(minutes=5):
                continue
        except Exception:
            pass
        rtt_rows.append(s)

    return darwin_rows, rtt_rows


# --- HTML ---
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Paddington Board</title>
<style>
:root{--bg:#09090f;--s2:#18181f;--bdr:#1f1f2e;
  --green:#00e676;--amber:#ffc107;--grey:#4a5568;--red:#ff5252;
  --text:#e2e8f0;--dim:#64748b;--mono:'Space Mono',monospace;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:-apple-system,system-ui,sans-serif;
  max-width:520px;margin:0 auto;padding:0 0 3rem;}
.hdr{display:flex;align-items:center;padding:1rem;border-bottom:1px solid var(--bdr);gap:.75rem;}
.hdr-title{font-family:var(--mono);font-size:.75rem;letter-spacing:.12em;text-transform:uppercase;color:var(--dim);}
.hdr-time{font-family:var(--mono);font-size:.85rem;margin-left:auto;}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);flex-shrink:0;}
.dot.off{background:var(--red);}
.section-label{font-family:var(--mono);font-size:.65rem;letter-spacing:.15em;text-transform:uppercase;
  color:var(--dim);padding:.75rem 1rem .4rem;border-top:1px solid var(--bdr);margin-top:.5rem;}
.row{display:grid;grid-template-columns:52px 1fr auto;gap:0 .75rem;align-items:center;
  padding:.7rem 1rem;border-bottom:1px solid var(--bdr);}
.row:hover{background:var(--s2);}
.col-time{font-family:var(--mono);font-size:1.2rem;font-weight:700;line-height:1;}
.col-time .delay{font-size:.65rem;color:var(--amber);display:block;font-weight:400;}
.col-dest .dest{font-size:.95rem;font-weight:500;}
.col-dest .sub{font-size:.7rem;color:var(--dim);font-family:var(--mono);margin-top:.15rem;
  display:flex;gap:.5rem;flex-wrap:wrap;}
.col-dest .loc-amber{color:var(--amber);}
.col-dest .loc-grey{color:var(--grey);}
.col-plat{font-family:var(--mono);font-size:1.5rem;font-weight:700;text-align:right;min-width:2.5rem;}
.col-plat.green{color:var(--green);}
.col-plat.amber{color:var(--amber);}
.col-plat.grey{color:var(--grey);}
.col-plat.sched{color:var(--grey);font-size:.7rem;padding-top:.3rem;}
.toggle-row{display:flex;align-items:center;justify-content:space-between;
  padding:.6rem 1rem;cursor:pointer;user-select:none;}
.toggle-row:hover{background:var(--s2);}
.toggle-label{font-family:var(--mono);font-size:.65rem;letter-spacing:.12em;text-transform:uppercase;color:var(--dim);}
.toggle-btn{font-family:var(--mono);font-size:.65rem;color:var(--dim);border:1px solid var(--bdr);
  border-radius:4px;padding:.15rem .4rem;background:none;}
#rtt-section{display:none;}
.footer{font-family:var(--mono);font-size:.6rem;color:var(--grey);
  display:flex;justify-content:space-between;padding:.75rem 1rem;border-top:1px solid var(--bdr);margin-top:1rem;}
.no-svcs{text-align:center;color:var(--dim);padding:2.5rem 1rem;font-size:.85rem;}
</style>
</head>
<body>
<div class="hdr">
  <div class="dot {{ 'ok' if td_connected else 'off' }}"></div>
  <span class="hdr-title">London Paddington · Westbound</span>
  <span class="hdr-time">{{ now }}</span>
</div>

{% if not darwin_rows %}
<div class="no-svcs">No westbound departures in the next 2 hours.</div>
{% else %}
<div class="section-label">Next 2 hours · live</div>
{% for r in darwin_rows %}
<div class="row">
  <div class="col-time">
    {{ r.std }}
    {% if r.delay %}<span class="delay">{{ r.delay }}</span>{% endif %}
  </div>
  <div class="col-dest">
    <div class="dest">{{ r.dest }}</div>
    <div class="sub">
      <span>{{ r.headcode }}</span>
      {% if r.siding %}<span class="loc-amber">{{ r.siding }}</span>
      {% elif r.status == 'grey' %}<span class="loc-grey">not located</span>{% endif %}
    </div>
  </div>
  <div class="col-plat {{ r.status }}">
    {% if r.platform %}{{ r.platform }}{% elif r.status == 'amber' %}~{% else %}–{% endif %}
  </div>
</div>
{% endfor %}
{% endif %}

{% if rtt_rows %}
<div class="toggle-row" onclick="toggleRTT()">
  <span class="toggle-label">Later today ({{ rtt_rows|length }} services)</span>
  <button class="toggle-btn" id="toggle-btn">show ▾</button>
</div>
<div id="rtt-section">
  {% for r in rtt_rows %}
  <div class="row">
    <div class="col-time">{{ r.std }}</div>
    <div class="col-dest">
      <div class="dest">{{ r.dest }}</div>
      <div class="sub"><span>{{ r.headcode }}</span><span>{{ r.operator }}</span></div>
    </div>
    <div class="col-plat sched">sched</div>
  </div>
  {% endfor %}
</div>
{% elif not rtt_active %}
<div class="toggle-row" style="cursor:default;">
  <span class="toggle-label" style="color:var(--grey)">Later today · RTT not configured</span>
</div>
{% endif %}

<div class="footer">
  <span>TD {{ 'live' if td_connected else 'OFFLINE' }}{% if last_td %} · {{ last_td }}{% endif %}</span>
  <span>Darwin {% if darwin_error %}<span style="color:var(--red)">ERR</span>{% else %}{{ last_darwin }}{% endif %}</span>
  <span>RTT {% if rtt_active %}{{ last_rtt or '…' }}{% else %}off{% endif %}</span>
</div>

<script>
function toggleRTT(){
  const s=document.getElementById('rtt-section');
  const b=document.getElementById('toggle-btn');
  const vis=s.style.display==='block';
  s.style.display=vis?'none':'block';
  b.textContent=vis?'show ▾':'hide ▴';
}
setTimeout(()=>location.reload(), 20000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    with lock:
        td_connected = state["td_connected"]
        last_td      = fmt_time(state["last_td"])
        last_darwin  = fmt_time(state["last_darwin"])
        last_rtt     = fmt_time(state["last_rtt"])
        darwin_error = state["darwin_error"]
        rtt_active   = bool(RTT_USER and RTT_PASS)
        darwin_rows, rtt_rows = build_board()

    return render_template_string(HTML,
        darwin_rows=darwin_rows,
        rtt_rows=rtt_rows,
        rtt_active=rtt_active,
        td_connected=td_connected,
        last_td=last_td,
        last_darwin=last_darwin,
        last_rtt=last_rtt,
        darwin_error=darwin_error,
        now=fmt_time(now_london()),
    )


@app.route("/berths")
def berths_debug():
    with lock:
        return {"berths": state["berths"], "raw_berths": state["raw_berths"]}


@app.route("/darwin_debug")
def darwin_debug():
    with lock:
        return {"services": state["darwin"], "error": state["darwin_error"],
                "last": fmt_time(state["last_darwin"])}


@app.route("/rtt_debug")
def rtt_debug():
    with lock:
        return {"services": state["rtt"][:20], "error": state["rtt_error"],
                "last": fmt_time(state["last_rtt"]), "total": len(state["rtt"])}


@app.route("/health")
def health():
    return "ok", 200


if __name__ == "__main__":
    threading.Thread(target=td_thread,   daemon=True).start()
    threading.Thread(target=darwin_poll, daemon=True).start()
    threading.Thread(target=rtt_poll,    daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
