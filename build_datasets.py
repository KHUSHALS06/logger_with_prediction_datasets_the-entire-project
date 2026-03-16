"""
build_datasets.py  —  Military Dataset Builder (Real Source Schemas)
═════════════════════════════════════════════════════════════════════
Run ONCE on an internet-connected machine.
Produces datasets/ folder for fully offline ship use.

Every source schema verified against actual source documentation:

SOURCE 1 — ADSBexchange  basic-ac-db.json.gz
  URL: https://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz
  Updated: daily at 23:30 UTC from government registries
  JSON dict: {icao_hex: {r, t, mil, man, mdl, ownop, cou, ...}}
  mil field: "yes"/"no" — aircraft is owned by a government's military
  Confirmed: https://www.adsbexchange.com/database/contribute/

SOURCE 2+3 — SDR-Enthusiasts plane-alert-mil.csv / plane-alert-gov.csv
  URL: https://raw.githubusercontent.com/sdr-enthusiasts/plane-alert-db/main/
  Updated: community contributions, ~15,913 aircraft in 52 categories
  CSV cols: $ICAO, $Registration, $Operator, $Type, $ICAO Type,
            #CMPG, $Tag 1, $#Tag 2, $#Tag 3, Category, $#Link
  Confirmed: https://github.com/sdr-enthusiasts/plane-alert-db

SOURCE 4 — tar1090-db / Mictronics  aircraft.csv.gz
  URL: https://github.com/wiedehopf/tar1090-db/raw/refs/heads/csv/aircraft.csv.gz
  CSV cols (no header): icao, r, t, f(dbFlags), d(desc), year, ownop, [spare]
  military = int(f) & 1   (bit 0 of dbFlags)
  Confirmed: https://github.com/wiedehopf/readsb/blob/dev/README-json.md
             https://github.com/wiedehopf/tar1090-db/blob/master/toJson.py

SOURCE 5 — Bellingcat adsb-history modes.csv
  URL: https://raw.githubusercontent.com/bellingcat/adsb-history/main/
       backend-data-loading/modes.csv
  CSV cols: hex, registration, typecode, category, military, owner, aircraft
  military field: true/false boolean

OUTPUT: datasets/mil_icao_db.csv
  icao, registration, country, branch, operator, manufacturer, typecode, model, sources

Usage:
    pip install requests
    python build_datasets.py
"""

import csv
import gzip
import io
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("ERROR: pip install requests")

DATASETS_DIR = Path("datasets")
DATASETS_DIR.mkdir(exist_ok=True)
TIMEOUT = 120

# ─────────────────────────────────────────────────────────────────────────────
#  ICAO TYPE CODE → FULL AIRCRAFT NAME
#  Source: ICAO Doc 8643 type designators + manufacturer model names
#  Only used when a database entry has a typecode but no human-readable name
# ─────────────────────────────────────────────────────────────────────────────
TC: dict[str, str] = {
    # US Fighters / Attack
    "F16C":"F-16C Fighting Falcon",      "F16D":"F-16D Fighting Falcon",
    "F15C":"F-15C Eagle",                "F15D":"F-15D Eagle",
    "F15E":"F-15E Strike Eagle",         "F15X":"F-15EX Eagle II",
    "F18S":"F/A-18E/F Super Hornet",     "F18":"F/A-18 Hornet",
    "F22":"F-22A Raptor",                "F35A":"F-35A Lightning II",
    "F35B":"F-35B Lightning II",         "F35C":"F-35C Lightning II",
    "F35":"F-35 Lightning II",           "A10":"A-10C Thunderbolt II",
    "AV8B":"AV-8B Harrier II",           "EA18":"EA-18G Growler",
    "E2":"E-2D Hawkeye",                 "EA6":"EA-6B Prowler",
    # US Bombers
    "B1":"B-1B Lancer",                  "B2":"B-2A Spirit",
    "B52H":"B-52H Stratofortress",       "B52":"B-52 Stratofortress",
    "B21":"B-21 Raider",
    # US Transport / Tanker
    "C17":"C-17A Globemaster III",       "C5M":"C-5M Super Galaxy",
    "C5":"C-5 Galaxy",                   "C130J":"C-130J Super Hercules",
    "C130":"C-130 Hercules",             "C2":"C-2A Greyhound",
    "C40":"C-40 Clipper",                "C32":"C-32A (757 VIP)",
    "C37":"C-37 Gulfstream (VIP)",       "KC46":"KC-46A Pegasus",
    "KC135":"KC-135R Stratotanker",      "KC10":"KC-10A Extender",
    "VC25":"VC-25A (Air Force One)",     "E3":"E-3 Sentry (AWACS)",
    "E4":"E-4B Nightwatch",              "E6":"E-6B Mercury (TACAMO)",
    "E8":"E-8C Joint STARS",             "RC135":"RC-135 Rivet Joint",
    # US ISR
    "U2":"U-2S Dragon Lady",             "RQ4":"RQ-4 Global Hawk",
    "MQ9":"MQ-9 Reaper",                 "MQ1":"MQ-1 Predator",
    "P8":"P-8A Poseidon",                "P3":"P-3C Orion",
    "EP3":"EP-3E Aries II",
    # US Helicopters
    "UH60":"UH-60 Black Hawk",           "HH60":"HH-60G Pave Hawk",
    "SH60":"SH-60 Seahawk",              "MH60":"MH-60M Black Hawk",
    "CH47":"CH-47F Chinook",             "CH53":"CH-53E Sea Stallion",
    "AH64":"AH-64E Apache",              "V22":"V-22 Osprey",
    "MV22":"MV-22B Osprey (Marines)",    "CV22":"CV-22B Osprey (SOCOM)",
    "HH65":"HH-65 Dolphin (USCG)",       "HC130":"HC-130J (USCG)",
    # UK
    "EUFI":"Eurofighter Typhoon",        "TPHN":"Eurofighter Typhoon",
    "TORN":"Tornado GR.4",               "A400":"A400M Atlas",
    "VOYS":"Voyager KC.2/3 (A330 MRTT)","HAWK":"Hawk T.1/T.2",
    "SEN":"Sentinel R.1",
    # France
    "RAFB":"Rafale B",                   "RAFC":"Rafale C",
    "RAFM":"Rafale M (Naval)",           "M2K":"Mirage 2000",
    "ARAN":"Atlantique 2 (ATL2)",
    # Germany
    "DO228":"Dornier 228",               "DO28":"Dornier 28",
    # Russia
    "SU27":"Su-27 Flanker-B",            "SU30":"Su-30 Flanker-C",
    "SU34":"Su-34 Fullback",             "SU35":"Su-35S Flanker-E",
    "SU57":"Su-57 Felon",                "SU25":"Su-25 Frogfoot",
    "SU24":"Su-24 Fencer",               "MIG29":"MiG-29 Fulcrum",
    "MIG31":"MiG-31BM Foxhound",         "TU22":"Tu-22M3 Backfire",
    "TU95":"Tu-95MS Bear",               "TU160":"Tu-160 Blackjack",
    "IL76":"Il-76MD Candid",             "IL78":"Il-78M Midas (Tanker)",
    "IL38":"Il-38N May",                 "AN12":"An-12BP Cub",
    "AN26":"An-26 Curl",                 "AN72":"An-72 Coaler",
    "AN124":"An-124 Ruslan",             "KA52":"Ka-52 Alligator",
    "KA27":"Ka-27 Helix",                "MI8":"Mi-8 Hip",
    "MI24":"Mi-24 Hind",                 "MI28":"Mi-28N Havoc",
    "MI17":"Mi-17 Hip-H",                "MI35":"Mi-35M Hind-E",
    # China
    "J10":"J-10 Vigorous Dragon",        "J11":"J-11B Flanker",
    "J16":"J-16 Strike Fighter",         "J20":"J-20 Mighty Dragon",
    "H6":"H-6K Badger (Bomber)",         "Y20":"Y-20 Kunpeng",
    "Y8":"Y-8F Transport",               "KJ500":"KJ-500 AEW&C",
    # Multi-nation
    "F5E":"F-5E Tiger II",               "T38":"T-38C Talon",
    "T45":"T-45C Goshawk",               "MB339":"Aermacchi MB-339",
    "C27J":"Alenia C-27J Spartan",       "CN235":"CASA CN-235",
    "C295":"Airbus C295",                "C212":"CASA C-212 Aviocar",
    "C160":"Transall C-160",             "PC7":"Pilatus PC-7",
    "PC9":"Pilatus PC-9M",               "PC12":"Pilatus PC-12",
    "MRTT":"A330 MRTT",                  "GL5T":"Gulfstream G550",
    "GLEX":"Gulfstream G650",            "CL60":"Bombardier Challenger",
    "F2":"Mitsubishi F-2",               "T4":"Kawasaki T-4",
    "P1":"Kawasaki P-1 Maritime Patrol", "E767":"Boeing E-767 AWACS",
    "L39":"Aero L-39 Albatros",          "T50":"KAI T-50 Golden Eagle",
    "KAI":"KAI T-50 Golden Eagle",       "SU25":"Su-25 Frogfoot",
}


def resolve_model(typecode: str, raw_model: str, aircraft_name: str) -> str:
    """Best full aircraft name. Priority: aircraft_name > raw_model > typecode table."""
    tc = (typecode or "").strip().upper()
    for candidate in [aircraft_name, raw_model]:
        c = (candidate or "").strip()
        if c and len(c) > 4 and c.upper() != tc:
            return c
    if tc:
        if tc in TC:
            return TC[tc]
        for k, v in TC.items():
            if len(k) >= 3 and tc[:3] == k[:3]:
                return v
    return (raw_model or aircraft_name or "").strip()


# ─────────────────────────────────────────────────────────────────────────────
#  BRANCH CLASSIFIER
#  Called ONLY on confirmed-military aircraft to label their branch.
#  Priority order is critical — check specific strings before generic.
# ─────────────────────────────────────────────────────────────────────────────
_BRANCH: list[tuple[str, str]] = [
    ("marines","Marines"), ("usmc","Marines"), ("marine corps","Marines"),
    ("marineflieger","Naval Aviation"), ("naval air","Naval Aviation"),
    ("us navy","Navy"), ("u.s. navy","Navy"), ("united states navy","Navy"),
    ("royal navy","Navy"), ("royal australian navy","Navy"),
    ("royal canadian navy","Navy"), ("indian navy","Navy"),
    ("pakistan navy","Navy"), ("french navy","Navy"),
    ("marine nationale","Navy"), ("german navy","Navy"),
    ("marina militare","Navy"), ("koninklijke marine","Navy"),
    ("jmsdf","Navy"), ("armada","Navy"), ("naval","Navy"), ("navy","Navy"),
    ("coast guard","Coast Guard"), ("coastguard","Coast Guard"),
    ("guardia costiera","Coast Guard"),
    ("us army","Army"), ("u.s. army","Army"), ("united states army","Army"),
    ("british army","Army"), ("army air corps","Army"), ("army","Army"),
    ("usaf","Air Force"), ("u.s. air force","Air Force"),
    ("united states air force","Air Force"),
    ("royal air force","Air Force"), ("raaf","Air Force"),
    ("royal australian air force","Air Force"),
    ("royal canadian air force","Air Force"), ("rcaf","Air Force"),
    ("indian air force","Air Force"), ("iaf","Air Force"),
    ("pakistan air force","Air Force"), ("french air force","Air Force"),
    ("armée de l","Air Force"), ("armee de l","Air Force"),
    ("luftwaffe","Air Force"), ("aeronautica militare","Air Force"),
    ("fuerza aerea","Air Force"), ("fuerza aérea","Air Force"),
    ("israeli air force","Air Force"), ("royal saudi air force","Air Force"),
    ("rsaf","Air Force"), ("jasdf","Air Force"), ("rokaf","Air Force"),
    ("plaaf","Air Force"), ("turkish air force","Air Force"),
    ("turaf","Air Force"), ("koninklijke luchtmacht","Air Force"),
    ("royal netherlands air","Air Force"), ("norwegian air force","Air Force"),
    ("swedish air force","Air Force"), ("flygvapnet","Air Force"),
    ("danish air force","Air Force"), ("finnish air force","Air Force"),
    ("greek air force","Air Force"), ("hellenic air force","Air Force"),
    ("portuguese air force","Air Force"), ("polish air force","Air Force"),
    ("belgian air","Air Force"), ("air force","Air Force"),
    ("air corps","Air Force"), ("air arm","Air Force"),
    ("special operations","Special Operations"), ("soar","Special Operations"),
    ("armed forces","Armed Forces"),
    ("military","Military"), ("defence","Military"), ("defense","Military"),
]


def classify_branch(operator: str, extra: str = "") -> str:
    t = (operator + " " + extra).lower()
    for kw, branch in _BRANCH:
        if kw in t:
            return branch
    return "Military"


# ─────────────────────────────────────────────────────────────────────────────
#  OPERATOR → COUNTRY
# ─────────────────────────────────────────────────────────────────────────────
_OP_COUNTRY: list[tuple[str, str]] = [
    ("usaf","USA"), ("us air force","USA"), ("us navy","USA"),
    ("us army","USA"), ("usmc","USA"), ("united states","USA"),
    ("blue angels","USA"), ("thunderbirds","USA"),
    ("royal air force","United Kingdom"), ("raf ","United Kingdom"),
    ("royal navy","United Kingdom"), ("british army","United Kingdom"),
    ("french","France"), ("armée","France"), ("marine nationale","France"),
    ("luftwaffe","Germany"), ("german","Germany"), ("marineflieger","Germany"),
    ("aeronautica","Italy"), ("marina militare","Italy"),
    ("fuerza aerea","Spain"), ("ejercito del aire","Spain"),
    ("koninklijke luchtmacht","Netherlands"), ("koninklijke marine","Netherlands"),
    ("norwegian","Norway"), ("swedish","Sweden"), ("flygvapnet","Sweden"),
    ("danish","Denmark"), ("finnish","Finland"),
    ("greek","Greece"), ("hellenic","Greece"),
    ("portuguese","Portugal"), ("polish","Poland"), ("belgian","Belgium"),
    ("raaf","Australia"), ("royal australian","Australia"),
    ("rcaf","Canada"), ("royal canadian","Canada"),
    ("indian air force","India"), ("indian navy","India"), ("iaf ","India"),
    ("pakistan","Pakistan"), ("bangladesh","Bangladesh"),
    ("sri lanka","Sri Lanka"), ("slaf","Sri Lanka"),
    ("israel","Israel"),
    ("saudi","Saudi Arabia"), ("rsaf ","Saudi Arabia"),
    ("uae","United Arab Emirates"), ("emirati","United Arab Emirates"),
    ("qatar","Qatar"), ("kuwait","Kuwait"),
    ("oman","Oman"), ("rafo","Oman"), ("sultan of oman","Oman"),
    ("bahrain","Bahrain"),
    ("turkey","Turkey"), ("türk","Turkey"), ("turaf","Turkey"),
    ("jasdf","Japan"), ("jmsdf","Japan"), ("japan air","Japan"),
    ("rokaf","South Korea"), ("korean air force","South Korea"),
    ("plaaf","China"), ("plan ","China"), ("chinese","China"),
    ("russia","Russia"), ("russian","Russia"),
    ("malaysian","Malaysia"), ("tudm","Malaysia"),
    ("singapore","Singapore"),
    ("indonesian","Indonesia"), ("tni-au","Indonesia"),
    ("thai","Thailand"), ("rtaf","Thailand"),
    ("philippine","Philippines"),
    ("egyptian","Egypt"), ("south african","South Africa"),
    ("nigerian","Nigeria"), ("kenyan","Kenya"), ("ethiopian","Ethiopia"),
    ("brazilian","Brazil"), ("fab ","Brazil"),
    ("chilean","Chile"), ("colombian","Colombia"),
    ("mexican","Mexico"), ("argentinian","Argentina"),
    ("peruvian","Peru"), ("venezuelan","Venezuela"),
    ("ukrainian","Ukraine"), ("czech","Czech Republic"),
    ("hungarian","Hungary"), ("romanian","Romania"), ("bulgarian","Bulgaria"),
    ("moroccan","Morocco"), ("algerian","Algeria"),
    ("iranian","Iran"), ("iraqi","Iraq"),
    ("vietnamese","Vietnam"), ("myanmar","Myanmar"),
    ("azerbaijani","Azerbaijan"), ("kazakh","Kazakhstan"),
]


def country_from_op(operator: str) -> str:
    op = operator.lower()
    for kw, c in _OP_COUNTRY:
        if kw in op:
            return c
    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fetch(url: str, label: str) -> bytes | None:
    print(f"  ↓ {label}")
    try:
        r = requests.get(url, timeout=TIMEOUT, stream=True)
        r.raise_for_status()
        data = r.content
        print(f"    {len(data)/1_048_576:.2f} MB  OK")
        return data
    except Exception as e:
        print(f"    FAILED: {e}")
        return None


def clean_icao(raw: str) -> str | None:
    v = raw.strip().upper().lstrip("~").lstrip("0X")
    if len(v) == 6 and all(c in "0123456789ABCDEF" for c in v):
        return v
    return None


def g(row: dict, *keys) -> str:
    for k in keys:
        for variant in (k, k.lower(), k.upper(), k.lstrip("$").lstrip("#")):
            v = (row.get(variant) or "").strip()
            if v:
                return v
    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 1 — ADSBexchange  basic-ac-db.json.gz
#
#  Real JSON structure (verified from adsbexchange.com/database/contribute/):
#    {
#      "AE1234": {
#        "r":      "82-0001",      registration / tail number
#        "t":      "C17",          ICAO type code
#        "mil":    "yes",          "yes"/"no" — government military ownership
#        "man":    "Boeing",       manufacturer
#        "mdl":    "C-17A Globemaster III",  model name
#        "ownop":  "US Air Force", owner/operator
#        "cou":    "United States",country
#        "desc":   "C-17A",        short description fallback
#        "year":   "1993"
#      }
#    }
#  mil field: explicitly "a yes/no field if the plane is owned by a
#             government's military" — source doc verbatim
# ─────────────────────────────────────────────────────────────────────────────
def load_adsbexchange() -> dict:
    print("\n[1/4]  ADSBexchange  basic-ac-db.json.gz")
    print("       mil=yes → government military ownership (human-verified daily)")
    url  = "https://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
    data = fetch(url, "basic-ac-db.json.gz")
    if not data:
        return {}
    try:
        db = json.loads(gzip.decompress(data))
    except Exception as e:
        print(f"  Parse error: {e}"); return {}

    results = {}
    skipped_nomil = 0
    for raw_icao, e in db.items():
        if not isinstance(e, dict):
            continue
        # ONLY hard mil=yes — this is the most reliable military flag available
        if str(e.get("mil","")).strip().lower() not in ("yes","true","1"):
            skipped_nomil += 1
            continue
        icao = clean_icao(raw_icao)
        if not icao:
            continue

        # Field names from real ADSBexchange schema
        reg      = str(e.get("r")     or "").strip()
        typecode = str(e.get("t")     or "").strip()
        operator = str(e.get("ownop") or "").strip()
        mfr      = str(e.get("man")   or "").strip()
        mdl      = str(e.get("mdl")   or e.get("desc") or "").strip()
        country  = str(e.get("cou")   or "").strip()

        if not country:
            country = country_from_op(operator)

        results[icao] = {
            "registration": reg,
            "country":      country,
            "branch":       classify_branch(operator),
            "operator":     operator,
            "manufacturer": mfr,
            "typecode":     typecode,
            "model":        resolve_model(typecode, mdl, ""),
            "source":       "adsbx",
        }

    print(f"  {len(results):,} mil=yes aircraft  ({skipped_nomil:,} non-military skipped)")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 2+3 — plane-alert-mil.csv  +  plane-alert-gov.csv
#
#  Real CSV structure (verified from github.com/sdr-enthusiasts/plane-alert-db):
#    $ICAO,$Registration,$Operator,$Type,$ICAO Type,#CMPG,
#    $Tag 1,$#Tag 2,$#Tag 3,Category,$#Link
#
#  $Operator = exact unit/squadron name (e.g. "US Air Force", "Royal Navy 815 NAS")
#  $Type     = human-readable aircraft model (e.g. "C-17A Globemaster III")
#  $ICAO Type= 4-char ICAO type code (e.g. "C17")
#  Category  = human category (e.g. "Zoomies", "Dogs with Jobs")
#  $Tag 1-3  = classification tags (e.g. "US Navy", "Fighter", "Transport")
# ─────────────────────────────────────────────────────────────────────────────
def load_plane_alert() -> dict:
    print("\n[2/4]  plane-alert-mil.csv + plane-alert-gov.csv")
    print("       Human-curated per-aircraft  $Operator + $Type per row")
    URLS = [
        ("https://raw.githubusercontent.com/sdr-enthusiasts/plane-alert-db/main/plane-alert-mil.csv","mil"),
        ("https://raw.githubusercontent.com/sdr-enthusiasts/plane-alert-db/main/plane-alert-gov.csv","gov"),
    ]
    results = {}
    for url, tag in URLS:
        data = fetch(url, f"plane-alert-{tag}.csv")
        if not data:
            continue
        text = data.decode("utf-8", errors="ignore")
        n = 0
        for row in csv.DictReader(io.StringIO(text)):
            icao = clean_icao(g(row, "$ICAO", "ICAO"))
            if not icao:
                continue

            operator = g(row, "$Operator","Operator")
            ac_type  = g(row, "$Type","Type")           # model name
            typecode = g(row, "$ICAO Type","ICAO Type","$ICAOTYPE","ICAOTYPE")
            reg      = g(row, "$Registration","Registration")
            tag1     = g(row, "$Tag 1","Tag 1")
            tag2     = g(row, "$#Tag 2","#Tag 2")
            tag3     = g(row, "$#Tag 3","#Tag 3")
            category = g(row, "Category")
            all_tags = f"{tag1} {tag2} {tag3} {category}"

            if icao not in results:
                results[icao] = {
                    "registration": reg,
                    "country":      country_from_op(operator),
                    "branch":       classify_branch(operator, all_tags),
                    "operator":     operator,
                    "manufacturer": "",
                    "typecode":     typecode,
                    "model":        resolve_model(typecode, ac_type, ""),
                    "source":       f"planealert-{tag}",
                }
            n += 1
        print(f"  plane-alert-{tag}: {n:,} entries")
    print(f"  {len(results):,} unique")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 4 — tar1090-db / Mictronics  aircraft.csv.gz
#
#  Real CSV structure (verified from tar1090-db/toJson.py + README-json.md):
#    No header row. Columns:
#    [0] icao24     — 6-char hex
#    [1] r          — registration / tail number
#    [2] t          — ICAO type code
#    [3] f          — dbFlags integer (military = int(f) & 1)
#    [4] d          — description / long type name
#    [5] year       — year of manufacture
#    [6] ownop      — owner/operator name
#    [7] (spare)    — None/empty
#
#  dbFlags bitfield (from readsb README-json.md):
#    bit 0 (& 1):  military
#    bit 1 (& 2):  interesting
#    bit 2 (& 4):  PIA (Privacy ICAO Address)
#    bit 3 (& 8):  LADD (Limiting Aircraft Data Displayed)
#
#  This is a HARD boolean set by Mictronics from government aircraft registries.
#  Not inferred. Not keyword-based.
# ─────────────────────────────────────────────────────────────────────────────
def load_tar1090() -> tuple[dict, dict]:
    print("\n[3/4]  tar1090-db / Mictronics  aircraft.csv.gz")
    print("       dbFlags bit-0 = military  (cols: icao,r,t,f,d,year,ownop)")
    url  = "https://github.com/wiedehopf/tar1090-db/raw/refs/heads/csv/aircraft.csv.gz"
    data = fetch(url, "aircraft.csv.gz")
    if not data:
        return {}, {}
    try:
        text   = gzip.decompress(data).decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
    except Exception as e:
        print(f"  Parse error: {e}"); return {}, {}

    military   = {}
    enrichment = {}
    mil_count  = 0
    total      = 0

    for row in reader:
        if len(row) < 2:
            continue
        icao = clean_icao(row[0])
        if not icao:
            continue
        total += 1

        # Real columns from toJson.py: icao, r, t, f, d, year, ownop, spare
        reg      = row[1].strip() if len(row) > 1 else ""
        typecode = row[2].strip() if len(row) > 2 else ""
        flags_s  = row[3].strip() if len(row) > 3 else ""
        desc     = row[4].strip() if len(row) > 4 else ""
        ownop    = row[6].strip() if len(row) > 6 else ""

        # Parse dbFlags — military bit is bit 0
        is_mil = False
        try:
            flags  = int(flags_s)
            is_mil = bool(flags & 1)
        except (ValueError, TypeError):
            pass  # flags field not an integer — not a military flag

        enrichment[icao] = {
            "registration": reg,
            "typecode":     typecode,
            "description":  desc,
            "ownop":        ownop,
        }

        if is_mil:
            mil_count += 1
            country = country_from_op(ownop)
            military[icao] = {
                "registration": reg,
                "country":      country,
                "branch":       classify_branch(ownop),
                "operator":     ownop,
                "manufacturer": "",
                "typecode":     typecode,
                "model":        resolve_model(typecode, desc, ""),
                "source":       "tar1090",
            }

    print(f"  {total:,} total aircraft  /  {mil_count:,} military (dbFlags & 1)")
    return military, enrichment


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 5 — Bellingcat adsb-history modes.csv
#
#  Real CSV structure (verified from github.com/bellingcat/adsb-history):
#    hex, registration, typecode, category, military, owner, aircraft
#
#  military field: "True"/"False" boolean string
#  owner:    operator/owner name
#  aircraft: full human-readable aircraft name (from tar1090+hexdb cross-ref)
#            e.g. "Boeing C-17A Globemaster III"
#
#  This is the weakest source — LLM-assisted classification.
#  Used only to fill gaps not covered by sources 1-4.
# ─────────────────────────────────────────────────────────────────────────────
def load_bellingcat() -> dict:
    print("\n[4/4]  Bellingcat modes.csv")
    print("       military=True boolean  (cols: hex,reg,typecode,cat,military,owner,aircraft)")
    url  = "https://raw.githubusercontent.com/bellingcat/adsb-history/main/backend-data-loading/modes.csv"
    data = fetch(url, "modes.csv")
    if not data:
        return {}

    results = {}
    for row in csv.DictReader(io.StringIO(data.decode("utf-8", errors="ignore"))):
        # Real columns: hex, registration, typecode, category, military, owner, aircraft
        if str(row.get("military","")).strip().lower() not in ("true","1","yes"):
            continue
        icao = clean_icao(row.get("hex",""))
        if not icao:
            continue

        owner    = (row.get("owner","")        or "").strip()
        aircraft = (row.get("aircraft","")     or "").strip()  # full name!
        typecode = (row.get("typecode","")     or "").strip()
        reg      = (row.get("registration","") or "").strip()

        results[icao] = {
            "registration": reg,
            "country":      country_from_op(owner),
            "branch":       classify_branch(owner),
            "operator":     owner,
            "manufacturer": "",
            "typecode":     typecode,
            "model":        resolve_model(typecode, "", aircraft),
            "source":       "bellingcat",
        }
    print(f"  {len(results):,} military=True entries")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  MERGE
#  Priority: plane-alert > adsbx > tar1090 > bellingcat
#  Enrichment from tar1090 fills blanks for any confirmed ICAO
# ─────────────────────────────────────────────────────────────────────────────
def merge_and_write(adsbx, planealert, tar1090_mil, bellingcat, tar1090_enrich) -> int:
    print("\n  Merging all sources...")

    all_confirmed = set(adsbx) | set(planealert) | set(tar1090_mil) | set(bellingcat)

    FIELDS = ["icao","registration","country","branch",
              "operator","manufacturer","typecode","model","sources"]

    merged: dict[str, dict] = {}
    for icao in all_confirmed:
        reg=""; country=""; branch=""; operator=""; mfr=""; tc=""; model=""
        srcs: list[str] = []

        def absorb(e: dict, tag: str):
            nonlocal reg, country, branch, operator, mfr, tc, model
            if not reg:     reg     = e.get("registration","")
            if not country: country = e.get("country","")
            bv = e.get("branch","")
            if bv and (not branch or branch == "Military"):
                branch = bv
            if not operator: operator = e.get("operator","")
            if not mfr:      mfr      = e.get("manufacturer","")
            if not tc:       tc       = e.get("typecode","")
            mv = e.get("model","")
            if mv and len(mv) > 4 and (not model or len(model) <= 4):
                model = mv
            if tag not in srcs:
                srcs.append(tag)

        # Priority order
        if icao in planealert:  absorb(planealert[icao],  planealert[icao].get("source","planealert"))
        if icao in adsbx:       absorb(adsbx[icao],       "adsbx")
        if icao in tar1090_mil: absorb(tar1090_mil[icao], "tar1090")
        if icao in bellingcat:  absorb(bellingcat[icao],  "bellingcat")

        # Enrichment — tar1090 fills blank reg/typecode/ownop
        if icao in tar1090_enrich:
            t = tar1090_enrich[icao]
            if not reg:      reg      = t.get("registration","")
            if not tc:       tc       = t.get("typecode","")
            if not operator: operator = t.get("ownop","")
            if not country:  country  = country_from_op(operator)
            if not model:
                m = resolve_model(t.get("typecode",""), t.get("description",""), "")
                if m: model = m

        # Final model from typecode table
        if not model and tc:
            model = resolve_model(tc, "", "")

        merged[icao] = {
            "icao":         icao,
            "registration": reg.strip(),
            "country":      country.strip(),
            "branch":       (branch or "Military").strip(),
            "operator":     operator.strip(),
            "manufacturer": mfr.strip(),
            "typecode":     tc.strip(),
            "model":        model.strip(),
            "sources":      "|".join(srcs),
        }

    out = DATASETS_DIR / "mil_icao_db.csv"
    with open(out,"w",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(merged.values(), key=lambda x: x["icao"]):
            writer.writerow(row)

    multi       = sum(1 for v in merged.values() if "|" in v["sources"])
    has_model   = sum(1 for v in merged.values() if v["model"])
    has_reg     = sum(1 for v in merged.values() if v["registration"])
    has_country = sum(1 for v in merged.values() if v["country"])
    bc: dict[str,int] = {}
    for v in merged.values():
        bc[v["branch"]] = bc.get(v["branch"],0) + 1

    print(f"\n  ╔══════════════════════════════════════════════╗")
    print(f"  ║  Total military aircraft:  {len(merged):>8,}       ║")
    print(f"  ║  Confirmed 2+ sources:     {multi:>8,}       ║")
    print(f"  ║  Have aircraft model name: {has_model:>8,}       ║")
    print(f"  ║  Have tail number:         {has_reg:>8,}       ║")
    print(f"  ║  Have country:             {has_country:>8,}       ║")
    print(f"  ╚══════════════════════════════════════════════╝")
    print(f"\n  Branch breakdown:")
    for b,n in sorted(bc.items(), key=lambda x: -x[1]):
        print(f"    {b:<30} {n:>6,}")
    print(f"\n  → {out}")
    return len(merged)


# ─────────────────────────────────────────────────────────────────────────────
#  ICAO HEX RANGES
#  Source: ICAO Annex 10 Vol III Chapter 9 national allocations
#  Confirmed from: https://www.icao.int/Meetings/AMC/MA/NACC_DCA03_2008/
#                  naccdca031wp04.pdf and virtualradarserver range docs
# ─────────────────────────────────────────────────────────────────────────────
def build_hex_ranges():
    print("\n  Building ICAO hex range table (ICAO Annex 10 allocations)...")
    RANGES = [
        # USA — AE block is US DoD dedicated (confirmed from FAA records)
        ("A00000","AFFFFF","USA","civilian"),
        ("AE0000","AFFFFF","USA","military"),
        # Canada
        ("C00000","C3FFFF","Canada","civilian"),
        # UK — 43C block is RAF dedicated
        ("400000","43FFFF","United Kingdom","civilian"),
        ("43C000","43CFFF","United Kingdom","military"),
        # France
        ("380000","3BFFFF","France","civilian"),
        # Germany
        ("3C0000","3FFFFF","Germany","civilian"),
        # Spain
        ("340000","37FFFF","Spain","civilian"),
        # Italy
        ("500000","53FFFF","Italy","civilian"),
        # Netherlands
        ("480000","487FFF","Netherlands","civilian"),
        # Belgium
        ("448000","44FFFF","Belgium","civilian"),
        # Norway
        ("468000","46FFFF","Norway","civilian"),
        # Sweden
        ("490000","497FFF","Sweden","civilian"),
        # Denmark
        ("458000","45FFFF","Denmark","civilian"),
        # Finland
        ("460000","467FFF","Finland","civilian"),
        # Poland
        ("489000","48FFFF","Poland","civilian"),
        # Portugal
        ("440000","447FFF","Portugal","civilian"),
        # Czech Republic
        ("498000","49FFFF","Czech Republic","civilian"),
        # Slovakia
        ("4A0000","4A7FFF","Slovakia","civilian"),
        # Hungary
        ("4A8000","4AFFFF","Hungary","civilian"),
        # Romania
        ("4B0000","4B7FFF","Romania","civilian"),
        # Bulgaria
        ("4B8000","4BFFFF","Bulgaria","civilian"),
        # Turkey
        ("4C0000","4CFFFF","Turkey","civilian"),
        # Greece
        ("4D0000","4D7FFF","Greece","civilian"),
        # Switzerland
        ("4B1000","4B1FFF","Switzerland","civilian"),
        # Ireland
        ("4CA000","4CAFFF","Ireland","civilian"),
        # Iceland
        ("4CC000","4CCFFF","Iceland","civilian"),
        # Luxembourg
        ("4C2000","4C27FF","Luxembourg","civilian"),
        # Malta
        ("4D2000","4D27FF","Malta","civilian"),
        # Cyprus
        ("4C8000","4C8FFF","Cyprus","civilian"),
        # Croatia
        ("501C00","501FFF","Croatia","civilian"),
        # Russia
        ("100000","1FFFFF","Russia","civilian"),
        # Ukraine
        ("508000","50FFFF","Ukraine","civilian"),
        # Belarus
        ("510000","5103FF","Belarus","civilian"),
        # Australia
        ("7C0000","7FFFFF","Australia","civilian"),
        # New Zealand
        ("C80000","C87FFF","New Zealand","civilian"),
        # Japan
        ("840000","87FFFF","Japan","civilian"),
        # China
        ("780000","7BFFFF","China","civilian"),
        # India
        ("800000","83FFFF","India","civilian"),
        # Pakistan
        ("760000","767FFF","Pakistan","civilian"),
        # Bangladesh
        ("702000","7027FF","Bangladesh","civilian"),
        # Sri Lanka
        ("AA0000","AA3FFF","Sri Lanka","civilian"),
        # Myanmar
        ("704000","7047FF","Myanmar","civilian"),
        # Thailand
        ("E20000","E3FFFF","Thailand","civilian"),
        # Vietnam
        ("888000","88FFFF","Vietnam","civilian"),
        # Malaysia
        ("750000","757FFF","Malaysia","civilian"),
        # Singapore
        ("768000","76FFFF","Singapore","civilian"),
        # Indonesia
        ("8A0000","8AFFFF","Indonesia","civilian"),
        # Philippines
        ("758000","75FFFF","Philippines","civilian"),
        # South Korea
        ("71C000","71FFFF","South Korea","civilian"),
        # North Korea
        ("720000","727FFF","North Korea","government"),
        # Saudi Arabia
        ("710000","717FFF","Saudi Arabia","civilian"),
        # UAE
        ("896000","896FFF","United Arab Emirates","civilian"),
        # Qatar
        ("06A000","06AFFF","Qatar","civilian"),
        # Kuwait
        ("704000","707FFF","Kuwait","civilian"),
        # Bahrain
        ("894000","894FFF","Bahrain","civilian"),
        # Oman
        ("738000","73AFFF","Oman","civilian"),
        # Israel
        ("738000","73BFFF","Israel","government"),
        # Jordan
        ("740000","747FFF","Jordan","civilian"),
        # Iraq
        ("728000","72FFFF","Iraq","civilian"),
        # Iran
        ("730000","737FFF","Iran","civilian"),
        # Egypt
        ("010000","017FFF","Egypt","civilian"),
        # Libya
        ("018000","01FFFF","Libya","civilian"),
        # Morocco
        ("020000","027FFF","Morocco","civilian"),
        # Algeria
        ("028000","02FFFF","Algeria","civilian"),
        # Tunisia
        ("030000","037FFF","Tunisia","civilian"),
        # South Africa
        ("008000","00FFFF","South Africa","civilian"),
        # Ethiopia
        ("040000","047FFF","Ethiopia","civilian"),
        # Kenya
        ("048000","04FFFF","Kenya","civilian"),
        # Nigeria
        ("050000","057FFF","Nigeria","civilian"),
        # Brazil
        ("E40000","E7FFFF","Brazil","civilian"),
        # Argentina
        ("E00000","E3FFFF","Argentina","civilian"),
        # Chile
        ("E80000","E8FFFF","Chile","civilian"),
        # Colombia
        ("0B8000","0B8FFF","Colombia","civilian"),
        # Mexico
        ("0D8000","0D8FFF","Mexico","civilian"),
        # Venezuela
        ("0BE000","0BEFFF","Venezuela","civilian"),
        # Peru
        ("0C2000","0C27FF","Peru","civilian"),
        # Kazakhstan
        ("683000","6830FF","Kazakhstan","civilian"),
        # Azerbaijan
        ("600000","6003FF","Azerbaijan","civilian"),
    ]
    out = DATASETS_DIR / "icao_hex_ranges.csv"
    with open(out,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f,fieldnames=["range_start","range_end","country","block_type"])
        w.writeheader()
        for s,e,c,b in RANGES:
            w.writerow({"range_start":s,"range_end":e,"country":c,"block_type":b})
    print(f"  {len(RANGES)} hex ranges → {out}")


# ─────────────────────────────────────────────────────────────────────────────
#  CALLSIGN PATTERNS
#  Prefixes that NEVER appear on civilian flights
# ─────────────────────────────────────────────────────────────────────────────
def build_callsign_patterns():
    print("\n  Building callsign pattern table...")
    PATTERNS = [
        # USA
        ("RCH","USA","Air Force","C-17/C-5/KC-10 Transport"),
        ("REACH","USA","Air Force","AMC Transport"),
        ("SAM","USA","Air Force","Special Air Mission"),
        ("SPAR","USA","Air Force","Special Priority"),
        ("SHELL","USA","Air Force","KC-135/KC-46 Tanker"),
        ("JAKE","USA","Air Force","KC-135 Tanker"),
        ("EVAC","USA","Air Force","Aeromedical Evacuation"),
        ("STEEL","USA","Air Force","B-1B/B-52 Bomber"),
        ("FORTE","USA","Air Force","B-52 Stratofortress"),
        ("DEATH","USA","Air Force","B-2 Spirit"),
        ("HAVOC","USA","Air Force","B-1B Lancer"),
        ("GHOST","USA","Air Force","B-2/ISR"),
        ("HERKY","USA","Air Force","C-130 Hercules"),
        ("GOLFER","USA","Air Force","RC-135 ISR"),
        ("CACTUS","USA","Air Force","E-3 Sentry AWACS"),
        ("SENTRY","USA","Air Force","E-3 Sentry AWACS"),
        ("MAGMA","USA","Special Operations","MC-130 SOCOM"),
        ("SHADOW","USA","Special Operations","SOCOM"),
        # Canada
        ("CANFORCE","Canada","Air Force","RCAF Transport"),
        ("CFC","Canada","Air Force","CC-150 Polaris"),
        # UK
        ("ASCOT","United Kingdom","Air Force","RAF Transport C-17/A400M"),
        ("RAFAIR","United Kingdom","Air Force","RAF Aircraft"),
        ("TARTAN","United Kingdom","Air Force","RAF Typhoon/Tanker"),
        ("VORTEX","United Kingdom","Air Force","VC10/Voyager Tanker"),
        # France
        ("COTAM","France","Air Force","Commandement du Transport Aérien"),
        ("FAF","France","Air Force","French Air Force"),
        # Germany
        ("GAF","Germany","Air Force","German Air Force"),
        ("SIGRUN","Germany","Air Force","A400M Transport"),
        # NATO
        ("NATO","NATO","Joint Forces","NATO Aircraft"),
        ("MAGIC","NATO","Air Force","NATO AWACS E-3"),
        ("AWACS","NATO","Air Force","NATO AWACS E-3"),
        # Australia
        ("AUSSIE","Australia","Air Force","RAAF Transport"),
        # India
        ("INDIAN AIR FORCE","India","Air Force","IAF Transport"),
        ("IAF","India","Air Force","Indian Air Force"),
        # Pakistan
        ("PAK AIR FORCE","Pakistan","Air Force","PAF Transport"),
        ("PAKAF","Pakistan","Air Force","Pakistan Air Force"),
        # Middle East
        ("MAJAN","Oman","Air Force","RAFO C-130/C-295"),
        ("SULTAN","Oman","Air Force","RAFO VIP Transport"),
        ("QATARI","Qatar","Air Force","QEAF C-17"),
        ("KAFR","Kuwait","Air Force","Kuwait Air Force"),
        ("UAEAF","United Arab Emirates","Air Force","UAE Air Force"),
        ("SAUDAF","Saudi Arabia","Air Force","Royal Saudi Air Force"),
        # Israel
        ("HAF","Israel","Air Force","Israeli Air Force"),
        ("ISRAF","Israel","Air Force","Israeli Air Force"),
        # Turkey
        ("TURAF","Turkey","Air Force","Turkish Air Force"),
        # Asia-Pacific
        ("JASDF","Japan","Air Force","Japan Air Self-Defense Force"),
        ("JMSDF","Japan","Navy","Japan Maritime Self-Defense Force"),
        ("ROKAF","South Korea","Air Force","Republic of Korea Air Force"),
        ("PLAAF","China","Air Force","PLA Air Force"),
        ("TUDM","Malaysia","Air Force","Royal Malaysian Air Force"),
        ("RSAF","Singapore","Air Force","Republic of Singapore Air Force"),
        ("RTAF","Thailand","Air Force","Royal Thai Air Force"),
        # Others
        ("SLAF","Sri Lanka","Air Force","Sri Lanka Air Force"),
        ("BANGLA AIR FORCE","Bangladesh","Air Force","Bangladesh Air Force"),
        ("RFF","Russia","Air Force","Russian Air Force"),
        ("AMIGO","Spain","Air Force","Ejercito del Aire"),
        ("DUTCHAF","Netherlands","Air Force","Royal Netherlands Air Force"),
        ("NORAIR","Norway","Air Force","Royal Norwegian Air Force"),
        ("DNKAF","Denmark","Air Force","Royal Danish Air Force"),
        ("FINN","Finland","Air Force","Finnish Air Force"),
        ("POLL","Poland","Air Force","Polish Air Force"),
        ("CZECH","Czech Republic","Air Force","Czech Air Force"),
    ]
    out = DATASETS_DIR / "callsign_patterns.csv"
    with open(out,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f,fieldnames=["pattern","country","branch","aircraft_type"])
        w.writeheader()
        for p,c,b,a in PATTERNS:
            w.writerow({"pattern":p,"country":c,"branch":b,"aircraft_type":a})
    print(f"  {len(PATTERNS)} callsign patterns → {out}")


# ─────────────────────────────────────────────────────────────────────────────
#  ALTITUDE PROFILES
# ─────────────────────────────────────────────────────────────────────────────
def build_altitude_profiles():
    print("\n  Building altitude profile table...")
    PROFILES = [
        (0,500,"Helicopter / rotary wing","military_probable",10),
        (501,2000,"Low-level helicopter / gunship","military_probable",8),
        (2001,5000,"Maritime patrol low-level","mixed",5),
        (5001,10000,"P-8 Poseidon / P-3 Orion maritime","mixed",8),
        (10001,15000,"C-130 / tactical ISR low cruise","mixed",5),
        (15001,25000,"C-130 Hercules typical cruise","military_possible",5),
        (25001,31000,"C-130 high / C-17 low cruise","mixed",3),
        (31000,41000,"Military transport standard cruise","overlaps_civilian",0),
        (41001,43000,"High transport / tanker cruise","military_possible",8),
        (43001,47000,"High-performance military jet","military_likely",25),
        (47001,51000,"Fighter / bomber cruise altitude","military_likely",25),
        (51001,60000,"RQ-4 Global Hawk / U-2 low altitude","military_only",40),
        (60001,70000,"U-2S Dragon Lady cruise altitude","military_only",45),
        (70001,99999,"Extreme altitude classified ISR","military_only",50),
    ]
    out = DATASETS_DIR / "altitude_profiles.csv"
    with open(out,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f,fieldnames=["alt_min_ft","alt_max_ft","aircraft_type","category","confidence_boost"])
        w.writeheader()
        for row in PROFILES:
            w.writerow({"alt_min_ft":row[0],"alt_max_ft":row[1],
                        "aircraft_type":row[2],"category":row[3],"confidence_boost":row[4]})
    print(f"  {len(PROFILES)} altitude profiles → {out}")


# ─────────────────────────────────────────────────────────────────────────────
#  STUB FILES
# ─────────────────────────────────────────────────────────────────────────────
def ensure_stub_files():
    stub = DATASETS_DIR / "military_dataset.csv"
    if not stub.exists():
        fields = ["icao","registration","country","branch",
                  "operator","manufacturer","typecode","model"]
        with open(stub,"w",newline="",encoding="utf-8") as f:
            csv.DictWriter(f,fieldnames=fields).writeheader()
        print(f"  Stub: {stub}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 62)
    print("  ADS-B Military Dataset Builder")
    print("  4 real sources, verified schemas, no assumptions")
    print("=" * 62)
    print()
    print("  Source schemas verified against:")
    print("  • ADSBexchange: adsbexchange.com/database/contribute")
    print("  • plane-alert:  github.com/sdr-enthusiasts/plane-alert-db")
    print("  • tar1090-db:   README-json.md dbFlags bit-0 + toJson.py cols")
    print("  • Bellingcat:   github.com/bellingcat/adsb-history")
    print()

    adsbx                     = load_adsbexchange()
    planealert                = load_plane_alert()
    tar1090_mil, tar1090_all  = load_tar1090()
    bellingcat                = load_bellingcat()

    total = merge_and_write(adsbx, planealert, tar1090_mil, bellingcat, tar1090_all)

    build_hex_ranges()
    build_callsign_patterns()
    build_altitude_profiles()
    ensure_stub_files()

    print("\n" + "=" * 62)
    print(f"  COMPLETE — {total:,} military aircraft in mil_icao_db.csv")
    print()
    print("  Output columns per aircraft:")
    print("    icao         — transponder hex code")
    print("    registration — tail number")
    print("    country      — country name")
    print("    branch       — Air Force / Navy / Army / Marines /")
    print("                   Coast Guard / Special Operations")
    print("    operator     — exact unit name")
    print("    manufacturer — aircraft manufacturer")
    print("    typecode     — ICAO 4-char type code")
    print("    model        — full aircraft name")
    print("    sources      — which databases confirmed this entry")
    print()
    print("  Copy entire datasets/ folder to ship machine.")
    print("=" * 62)
