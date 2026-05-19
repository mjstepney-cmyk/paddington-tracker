# Paddington Platform Predictor

Tracks GWR trains to Paignton / Newton Abbot and predicts platform allocation at London Paddington before it's announced on the boards.

## How it works
1. **TD feed** (Network Rail STOMP) — tracks physical train position at berth level in the WY (Paddington) area
2. **Darwin LDBWS** — polls every 5 min for scheduled departures and any confirmed platform
3. App resolves headcode → berth → human location (siding or platform number)

## Endpoints
- `/` — mobile-friendly status page (auto-refreshes every 30s)
- `/berths` — JSON debug view of all observed WY berths (use this for berth mapping)

## Berth mapping
The `BERTH_MAP` dict in `app.py` needs to be built empirically. To do this:
1. Deploy the app
2. Visit `/berths` while a known service is moving through Paddington
3. Correlate berth IDs with the timetabled train's position
4. Add entries to `BERTH_MAP` in `app.py`

Key locations to map:
- Ranelagh Bridge sidings (GWR IET stabling point, ~1km west of Paddington)
- Old Oak Common sidings
- Platform approach berths
- Platform 1–8 berths (long-distance)

## Deployment (Railway.app)
1. Push this repo to GitHub
2. New project on railway.app → Deploy from GitHub repo
3. Set environment variables (optional — defaults are in app.py):
   - `NR_USER`, `NR_PASS` — Network Rail credentials
   - `DARWIN_USER`, `DARWIN_PASS` — Darwin credentials
4. Railway auto-detects Procfile and deploys

## Environment variables
For security, set credentials as Railway env vars rather than leaving them hardcoded:
- `NR_USER=mjstepney@gmail.com`
- `NR_PASS=<your NR password>`
- `DARWIN_USER=mjstepney@gmail.com`
- `DARWIN_PASS=<your Darwin password>`
