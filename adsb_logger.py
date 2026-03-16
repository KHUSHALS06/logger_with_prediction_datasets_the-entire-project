import tkinter as tk
from tkinter import messagebox
import threading
import queue
import socket
import json
import csv
import os
import time
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict


CONFIG_FILE     = "config.json"
DATA_DIR        = Path("data")
LOG_DIR         = Path("logs")
DATASETS_DIR    = Path("datasets")
BATCH_SIZE      = 500
BATCH_INTERVAL  = 2.0
QUEUE_MAXSIZE   = 20_000
RECONNECT_DELAY = 5.0

# ── All 22 raw SBS-1 fields ──────────────────────────────────────────────────
SBS_FIELDS = [
    "message_type", "transmission_type", "session_id", "aircraft_id",
    "icao", "flight_id",
    "date_generated", "time_generated", "date_logged", "time_logged",
    "callsign", "altitude", "ground_speed", "track",
    "latitude", "longitude", "vertical_rate", "squawk",
    "alert", "emergency", "spi", "on_ground",
]

EXTRA_FIELDS       = [
    "ship_name",
    "country",          # e.g. USA, UK, India
    "branch",           # Air Force / Navy / Army / Marines / Coast Guard / Special Operations
    "operator",         # e.g. "US Air Force", "Royal Navy 815 NAS"
    "manufacturer",     # e.g. "Boeing", "Lockheed Martin"
    "typecode",         # ICAO type code e.g. C17, F16C, P8
    "model",            # Full aircraft name e.g. "C-17 Globemaster III"
    "registration",     # Tail number e.g. "ZZ336", "168000"
    "military",         # YES / POSSIBLE / NO
    "confidence",       # 0-100
    "logged_at",
]
CSV_HEADER         = SBS_FIELDS + EXTRA_FIELDS
MILITARY_TRACK_HDR = CSV_HEADER   # same columns

LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)
DATASETS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "adsb_logger.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("adsb")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}

def save_config(data: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
#  MILITARY PREDICTOR
# ─────────────────────────────────────────────────────────────────────────────
class MilitaryPredictor:
    """
    Offline military aircraft predictor — six precision layers.

      Layer 1  Session cache       — O(1) reuse of previous result for same ICAO
      Layer 2  Local verified DB   — user-confirmed aircraft from past sessions
      Layer 3  mil_icao_db.csv     — merged from 4 open-source databases (16,000+ aircraft)
                                     Each entry has: country, branch, operator,
                                     manufacturer, typecode, model, registration
      Layer 4  Callsign patterns   — known military prefixes, never appear on civil flights
      Layer 5  ICAO hex range      — block-level country + military attribution
      Layer 6  Altitude profile    — confidence boost only, never triggers alone

    Adds these fields to every row:
        country, branch, operator, manufacturer, typecode,
        model, registration, military (YES/POSSIBLE/NO), confidence
    """

    # ── Hardcoded high-confidence callsign prefixes ───────────────────────────
    # These never appear on civilian flights.
    # Format: (prefix, country, branch, aircraft_type)
    _BUILTIN_CALLSIGNS = [
        # USA
        ("RCH",        "USA",        "USAF AMC",              "Transport (C-17/C-5/KC-10)"),
        ("REACH",      "USA",        "USAF AMC",              "Transport"),
        ("SAM",        "USA",        "USAF",                  "Special Air Mission"),
        ("SPAR",       "USA",        "USAF",                  "Special Priority"),
        ("SHELL",      "USA",        "USAF",                  "Tanker"),
        ("JAKE",       "USA",        "USAF",                  "Tanker"),
        ("TOPCAT",     "USA",        "USAF",                  "Fighter"),
        ("EVAC",       "USA",        "USAF AMC",              "Aeromedical"),
        ("STEEL",      "USA",        "USAF",                  "Bomber/Strike"),
        ("COBRA",      "USA",        "US Military",           "Fighter"),
        ("HERKY",      "USA",        "USAF",                  "C-130 Hercules"),
        ("GOLFER",     "USA",        "USAF",                  "ISR"),
        ("FORTE",      "USA",        "USAF",                  "B-52"),
        ("DEATH",      "USA",        "USAF",                  "B-2 Spirit"),
        ("GHOST",      "USA",        "USAF",                  "B-2 Spirit"),
        ("DARKSTAR",   "USA",        "USAF",                  "ISR"),
        ("SIGINT",     "USA",        "US Military",           "SIGINT"),
        # US Navy
        ("IRON",       "USA",        "US Navy",               "Fighter"),
        ("CONVOY",     "USA",        "US Navy",               "Maritime Patrol"),
        ("RANGER",     "USA",        "US Navy",               "P-8 Poseidon"),
        # UK
        ("ASCOT",      "UK",         "RAF",                   "Transport (C-17/A400M)"),
        ("RAFAIR",     "UK",         "RAF",                   "Transport"),
        ("TARTAN",     "UK",         "RAF",                   "Tanker/ISR"),
        ("VORTEX",     "UK",         "RAF",                   "Tanker"),
        # Canada
        ("CANFORCE",   "Canada",     "RCAF",                  "Transport (CC-130/CC-150)"),
        # France
        ("COTAM",      "France",     "French Air Force",      "Transport"),
        ("FROG",       "France",     "French Air Force",      "Fighter"),
        ("FAF",        "France",     "French Air Force",      "Any"),
        # Germany
        ("GAF",        "Germany",    "German Air Force",      "Any"),
        ("GERMAN AF",  "Germany",    "German Air Force",      "Transport"),
        # Italy
        ("IAM",        "Italy",      "Italian Air Force",     "Transport"),
        # Spain
        ("AMIGO",      "Spain",      "Spanish Air Force",     "Any"),
        # Netherlands
        ("DUTCHAF",    "Netherlands","Royal Netherlands AF",  "Any"),
        # Belgium
        ("Belgian",    "Belgium",    "Belgian Air Force",     "Any"),
        # Norway
        ("NORWEGIAN AIR FORCE", "Norway", "Royal Norwegian AF", "Any"),
        # Denmark
        ("DANISH AIR FORCE",    "Denmark","Royal Danish AF",  "Any"),
        # Australia
        ("AUSSIE",     "Australia",  "RAAF",                  "Any"),
        ("DRAGON",     "Australia",  "RAAF",                  "Any"),
        # India
        ("INDIAN AIR FORCE",    "India",  "IAF",              "Transport"),
        ("IAF",        "India",      "IAF",                   "Any"),
        # Pakistan
        ("PAK AIR FORCE",       "Pakistan","PAF",             "Transport"),
        ("PAKAF",      "Pakistan",   "PAF",                   "Any"),
        # Bangladesh
        ("BANGLA AIR FORCE",    "Bangladesh","BAF",           "Transport"),
        # Sri Lanka
        ("SLAF",       "Sri Lanka",  "SLAF",                  "Any"),
        # Middle East
        ("MAJAN",      "Oman",       "RAFO",                  "Transport"),
        ("SULTAN",     "Oman",       "RAFO",                  "VIP Transport"),
        ("OMAN AIR FORCE",      "Oman","RAFO",                "Any"),
        ("QATARI",     "Qatar",      "QEAF",                  "Transport (C-17)"),
        ("KAFR",       "Kuwait",     "KFAF",                  "Transport"),
        ("UAEAF",      "UAE",        "UAEAF",                 "Any"),
        ("SAUDAF",     "Saudi Arabia","RSAF",                 "Any"),
        ("BAHRAIN",    "Bahrain",    "RBAF",                  "Any"),
        # Israel
        ("HAF",        "Israel",     "IAF",                   "Any"),
        ("ISRAF",      "Israel",     "IAF",                   "Any"),
        # Turkey
        ("TURKISH AIR FORCE",   "Turkey","TurAF",             "Any"),
        ("TURAF",      "Turkey",     "TurAF",                 "Any"),
        # Malaysia
        ("TUDM",       "Malaysia",   "RMAF",                  "Any"),
        # Singapore
        ("RSAF",       "Singapore",  "RSAF",                  "Any"),
        # Japan
        ("JASDF",      "Japan",      "JASDF",                 "Any"),
        ("JMSDF",      "Japan",      "JMSDF",                 "Maritime Patrol"),
        # South Korea
        ("ROKAF",      "South Korea","ROKAF",                 "Any"),
        # China — rarely broadcast but include
        ("PLAAF",      "China",      "PLAAF",                 "Any"),
        # Russia — rarely broadcast
        ("RFF",        "Russia",     "Russian Air Force",     "Any"),
    ]

    # ── Altitude profiles ─────────────────────────────────────────────────────
    # (min_ft, max_ft, aircraft_type_hint, confidence_boost)
    # Boost only applied when confidence already > 0 from another layer.
    _ALTITUDE_PROFILES = [
        (0,      500,   "Helicopter / Low-level tactical",   10),
        (501,    2000,  "Helicopter / Gunship",               8),
        (2001,   10000, "Maritime Patrol / Coastal ISR",      8),
        (10001,  20000, "Tactical transport / ISR",           5),
        (20001,  30000, "C-130 / Tactical transport",         5),
        (43001,  51000, "High-performance military",         25),
        (51001,  65000, "ISR platform (U-2 / Global Hawk)",  40),
        (65001,  99999, "Classified / U-2 extreme altitude", 50),
    ]

    # ── ICAO hex ranges that are almost exclusively military ──────────────────
    # Stored as (start_int, end_int, country, block_type)
    # block_type: "military" → confidence 70 if no other signal
    #             "government" → confidence 50
    _BUILTIN_HEX_RANGES = [
        (0xAE0000, 0xAFFFFF, "USA",        "military"),   # US DoD block
        (0x43C000, 0x43CFFF, "UK",         "military"),   # RAF dedicated
        (0x7C0000, 0x7FFFFF, "Australia",  "government"),
        (0x800000, 0x87FFFF, "India",      "government"),
        (0xC80000, 0xC8FFFF, "Canada",     "government"),
        (0x738000, 0x73FFFF, "Germany",    "government"),
        (0x3C0000, 0x3FFFFF, "Germany",    "government"),
        (0x4B0000, 0x4BFFFF, "Netherlands","government"),
        (0x460000, 0x467FFF, "Norway",     "government"),
        (0x458000, 0x45FFFF, "Sweden",     "government"),
        (0x456000, 0x457FFF, "Denmark",    "government"),
        (0x4D0000, 0x4DFFFF, "Belgium",    "government"),
        (0x500000, 0x53FFFF, "Italy",      "government"),
        (0x340000, 0x37FFFF, "France",     "government"),
        (0x380000, 0x3BFFFF, "Spain",      "government"),
        (0x896000, 0x897FFF, "Japan",      "government"),
        (0x71C000, 0x71FFFF, "South Korea","government"),
        (0x760000, 0x76FFFF, "China",      "government"),
        (0x140000, 0x17FFFF, "Russia",     "government"),
    ]

    def __init__(self):
        # Runtime session memory
        self.prediction_cache: dict[str, dict]       = {}   # icao → result
        self.verification_status: dict[str, str]     = {}   # icao → pending/validated/modified/rejected
        self.predicted_military_rows: dict[str, list] = defaultdict(list)  # icao → [rows]

        # Static datasets loaded once at startup
        self.verified_locals:   dict[str, dict] = {}
        self.mil_icao_dict:     dict[str, dict] = {}
        self.callsign_patterns: list[dict]      = []
        self.hex_ranges:        list[tuple]     = []

        self._load_datasets()

    # ── Dataset loading ───────────────────────────────────────────────────────
    def _load_datasets(self):
        self._load_verified_locals()
        self._load_mil_icao_db()
        self._load_callsign_patterns()
        self._load_hex_ranges()
        log.info(
            f"Predictor ready  "
            f"verified={len(self.verified_locals)}  "
            f"mil_db={len(self.mil_icao_dict)}  "
            f"callsigns={len(self.callsign_patterns)}  "
            f"hex_ranges={len(self.hex_ranges)}"
        )

    def _load_verified_locals(self):
        path = DATASETS_DIR / "military_dataset.csv"
        if not path.exists():
            return
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                icao = row.get("icao", "").strip().upper()
                if icao:
                    self.verified_locals[icao] = {
                        "country":      row.get("country", ""),
                        "branch":       row.get("branch", "Military"),
                        "operator":     row.get("operator", ""),
                        "manufacturer": row.get("manufacturer", ""),
                        "typecode":     row.get("typecode", ""),
                        "model":        row.get("model", ""),
                        "registration": row.get("registration", ""),
                    }

    def _load_mil_icao_db(self):
        path = DATASETS_DIR / "mil_icao_db.csv"
        if not path.exists():
            log.warning("mil_icao_db.csv not found — Layer 3 disabled")
            return
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                icao = row.get("icao", "").strip().upper()
                if icao:
                    self.mil_icao_dict[icao] = {
                        "country":      row.get("country", ""),
                        "branch":       row.get("branch", "Military"),
                        "operator":     row.get("operator", ""),
                        "manufacturer": row.get("manufacturer", ""),
                        "typecode":     row.get("typecode", ""),
                        "model":        row.get("model", ""),
                        "registration": row.get("registration", ""),
                    }

    def _load_callsign_patterns(self):
        # Start with hardcoded builtins
        for prefix, country, branch, aircraft_type in self._BUILTIN_CALLSIGNS:
            self.callsign_patterns.append({
                "prefix":        prefix.upper(),
                "country":       country,
                "branch":        branch,
                "aircraft_type": aircraft_type,
            })
        # Extend with CSV if present
        path = DATASETS_DIR / "callsign_patterns.csv"
        if not path.exists():
            return
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                prefix = row.get("pattern", "").strip().upper()
                if prefix:
                    self.callsign_patterns.append({
                        "prefix":        prefix,
                        "country":       row.get("country", ""),
                        "branch":        row.get("branch", ""),
                        "aircraft_type": row.get("aircraft_type", ""),
                    })
        # Sort longest prefix first so "CANFORCE" matches before "CAN"
        self.callsign_patterns.sort(key=lambda x: len(x["prefix"]), reverse=True)

    def _load_hex_ranges(self):
        # Start with hardcoded builtins
        for start, end, country, block_type in self._BUILTIN_HEX_RANGES:
            self.hex_ranges.append((start, end, country, block_type))
        # Extend with CSV if present
        path = DATASETS_DIR / "icao_hex_ranges.csv"
        if not path.exists():
            return
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    start      = int(row["range_start"], 16)
                    end        = int(row["range_end"],   16)
                    country    = row.get("country", "")
                    block_type = row.get("block_type", "civilian")
                    self.hex_ranges.append((start, end, country, block_type))
                except (KeyError, ValueError):
                    continue

    # ── Core prediction ───────────────────────────────────────────────────────
    def predict(self, merged: dict) -> dict:
        """
        Evaluate one merged ADS-B row.
        Adds: country, branch, operator, manufacturer, typecode, model,
              registration, military, confidence
        """
        icao     = merged.get("icao", "").strip().upper()
        callsign = merged.get("callsign", "").strip().upper()
        altitude = merged.get("altitude", "").strip()

        country      = ""
        branch       = ""
        operator     = ""
        manufacturer = ""
        typecode     = ""
        model        = ""
        registration = ""
        military     = "NO"
        confidence   = 0

        # ── Layer 1: Session cache ────────────────────────────────────────────
        if icao and icao in self.prediction_cache:
            cached = self.prediction_cache[icao]
            merged.update(cached)
            if cached.get("military") in ("YES", "POSSIBLE"):
                self.predicted_military_rows[icao].append(dict(merged))
            return merged

        # ── Layer 2: Local verified dataset ───────────────────────────────────
        if icao and icao in self.verified_locals:
            v            = self.verified_locals[icao]
            country      = v["country"]
            branch       = v.get("branch", "Military")
            operator     = v.get("operator", "")
            manufacturer = v.get("manufacturer", "")
            typecode     = v.get("typecode", "")
            model        = v.get("model", "")
            registration = v.get("registration", "")
            military     = "YES"
            confidence   = 100

        # ── Layer 3: Military ICAO database ───────────────────────────────────
        elif icao and icao in self.mil_icao_dict:
            v            = self.mil_icao_dict[icao]
            country      = v["country"]
            branch       = v.get("branch", "Military")
            operator     = v.get("operator", "")
            manufacturer = v.get("manufacturer", "")
            typecode     = v.get("typecode", "")
            model        = v.get("model", "")
            registration = v.get("registration", "")
            military     = "YES"
            confidence   = 100

        else:
            # ── Layer 4: Callsign pattern match ───────────────────────────────
            if callsign:
                for pattern in self.callsign_patterns:
                    if callsign.startswith(pattern["prefix"]):
                        country      = pattern["country"]
                        branch       = pattern["branch"]
                        operator     = pattern["branch"]
                        model        = pattern.get("aircraft_type", "")
                        military     = "YES"
                        confidence   = 95
                        break

            # ── Layer 5: ICAO hex range ───────────────────────────────────────
            if icao:
                try:
                    icao_int = int(icao, 16)
                    for start, end, rng_country, block_type in self.hex_ranges:
                        if start <= icao_int <= end:
                            if not country:
                                country = rng_country
                            if block_type == "military" and confidence < 70:
                                military     = "POSSIBLE"
                                confidence   = 70
                                branch       = branch or "Military"
                                operator     = operator or f"{rng_country} Military"
                            elif block_type == "government" and confidence < 50:
                                military     = "POSSIBLE"
                                confidence   = 50
                                branch       = branch or "Military"
                                operator     = operator or f"{rng_country} Government"
                            break
                except ValueError:
                    pass

            # ── Layer 6: Altitude profile (boost only) ────────────────────────
            if confidence > 0 and altitude:
                try:
                    alt_ft = int(altitude)
                    for alt_min, alt_max, alt_hint, boost in self._ALTITUDE_PROFILES:
                        if alt_min <= alt_ft <= alt_max:
                            confidence += boost
                            if not model and confidence < 95:
                                model = alt_hint
                            break
                    confidence = min(confidence, 99)
                    if military == "POSSIBLE" and confidence >= 85:
                        military = "YES"
                except ValueError:
                    pass

        # ── Final classification ──────────────────────────────────────────────
        result = {
            "country":      country,
            "branch":       branch or ("Military" if military != "NO" else ""),
            "operator":     operator,
            "manufacturer": manufacturer,
            "typecode":     typecode,
            "model":        model,
            "registration": registration,
            "military":     military,
            "confidence":   str(confidence),
        }

        if icao:
            self.prediction_cache[icao] = result

        merged.update(result)

        if military in ("YES", "POSSIBLE") and icao:
            self.predicted_military_rows[icao].append(dict(merged))

        return merged

    # ── Verification (called from UI) ────────────────────────────────────────
    def set_verification(self, icao: str, status: str,
                         country: str = "", branch: str = "",
                         operator: str = "", model: str = "",
                         full_corrections: dict | None = None):
        """
        status: 'validated' | 'modified' | 'rejected'
        full_corrections: if provided, every key/value is applied to cache + rows.
        """
        icao = icao.upper()
        self.verification_status[icao] = status

        if status == "modified":
            corrections = full_corrections or {
                "country": country, "branch": branch,
                "operator": operator, "model": model,
            }
            if icao in self.prediction_cache:
                self.prediction_cache[icao].update(corrections)
            for row in self.predicted_military_rows.get(icao, []):
                row.update(corrections)

        elif status == "rejected":
            if icao in self.prediction_cache:
                self.prediction_cache[icao]["military"] = "NO"
            self.predicted_military_rows.pop(icao, None)

    # ── End-of-session writes ─────────────────────────────────────────────────
    def write_military_tracks(self):
        """Write all non-rejected military rows to data/military_tracks.csv"""
        path = DATA_DIR / "military_tracks.csv"
        rows_written = 0
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=MILITARY_TRACK_HDR, extrasaction="ignore")
            writer.writeheader()
            for icao, rows in self.predicted_military_rows.items():
                status = self.verification_status.get(icao, "pending")
                if status == "rejected":
                    continue
                writer.writerows(rows)
                rows_written += len(rows)
        log.info(f"Military tracks written  rows={rows_written}  path={path}")

    def update_training_dataset(self):
        """Append validated/modified aircraft to datasets/military_dataset.csv"""
        path = DATASETS_DIR / "military_dataset.csv"
        existing = set()
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    existing.add(row.get("icao", "").upper())

        fields   = ["icao", "country", "branch", "operator",
                    "manufacturer", "typecode", "model", "registration"]
        new_rows = []
        for icao, status in self.verification_status.items():
            if status not in ("validated", "modified"):
                continue
            if icao in existing:
                continue
            cached = self.prediction_cache.get(icao, {})
            new_rows.append({
                "icao":         icao,
                "country":      cached.get("country", ""),
                "branch":       cached.get("branch", "Military"),
                "operator":     cached.get("operator", ""),
                "manufacturer": cached.get("manufacturer", ""),
                "typecode":     cached.get("typecode", ""),
                "model":        cached.get("model", ""),
                "registration": cached.get("registration", ""),
            })

        if new_rows:
            write_header = not path.exists()
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                if write_header:
                    writer.writeheader()
                writer.writerows(new_rows)
            log.info(f"Training dataset updated  new_entries={len(new_rows)}")

    def clear_session(self):
        """Reset all runtime memory after files are written."""
        self.prediction_cache.clear()
        self.verification_status.clear()
        self.predicted_military_rows.clear()
        log.info("Predictor session memory cleared")


# ─────────────────────────────────────────────────────────────────────────────
#  MILITARY CONTACT TABLE ROW
#  One row per detected military aircraft, embedded in the main window.
#  MODIFY expands in-place showing every single SBS + extra field as editable.
# ─────────────────────────────────────────────────────────────────────────────

# All fields the edit panel exposes, grouped for layout.
# Format: (field_key, display_label, group)
_EDIT_GROUPS = [
    # ── SBS raw fields ────────────────────────────────────────────────────────
    ("SBS DATA",        None,                 "header"),
    ("icao",            "ICAO Hex",           "sbs"),
    ("callsign",        "Callsign",           "sbs"),
    ("altitude",        "Altitude (ft)",      "sbs"),
    ("ground_speed",    "Ground Speed (kts)", "sbs"),
    ("track",           "Track (°)",          "sbs"),
    ("latitude",        "Latitude",           "sbs"),
    ("longitude",       "Longitude",          "sbs"),
    ("vertical_rate",   "Vertical Rate",      "sbs"),
    ("squawk",          "Squawk",             "sbs"),
    ("alert",           "Alert",              "sbs"),
    ("emergency",       "Emergency",          "sbs"),
    ("spi",             "SPI",                "sbs"),
    ("on_ground",       "On Ground",          "sbs"),
    ("transmission_type","Transmission Type", "sbs"),
    ("session_id",      "Session ID",         "sbs"),
    ("aircraft_id",     "Aircraft ID",        "sbs"),
    ("flight_id",       "Flight ID",          "sbs"),
    ("date_generated",  "Date Generated",     "sbs"),
    ("time_generated",  "Time Generated",     "sbs"),
    ("date_logged",     "Date Logged",        "sbs"),
    ("time_logged",     "Time Logged",        "sbs"),
    # ── Extra / enriched fields ───────────────────────────────────────────────
    ("ENRICHED DATA",   None,                 "header"),
    ("country",         "Country",            "extra"),
    ("branch",          "Branch",             "extra"),
    ("operator",        "Operator",           "extra"),
    ("manufacturer",    "Manufacturer",       "extra"),
    ("typecode",        "Type Code",          "extra"),
    ("model",           "Aircraft Model",     "extra"),
    ("registration",    "Registration",       "extra"),
    ("military",        "Military",           "extra"),
    ("confidence",      "Confidence",         "extra"),
    ("ship_name",       "Ship Name",          "extra"),
    ("logged_at",       "Logged At",          "extra"),
]


class MilContactRow(tk.Frame):
    STATUS_COLORS = {
        "pending":   "#E0A030",
        "validated": "#2EC27E",
        "modified":  "#7EC8E3",
        "rejected":  "#C0392B",
    }

    def __init__(self, parent, predictor: MilitaryPredictor, row: dict, on_resize):
        super().__init__(parent, bg=C["surface"],
                         highlightthickness=1,
                         highlightbackground=C["border"])
        self.predictor = predictor
        self.data      = dict(row)           # mutable local copy
        self.icao      = row.get("icao","").upper()
        self.on_resize = on_resize
        self._status   = "pending"
        self._expanded = False
        self._entries: dict[str, tk.Entry] = {}   # field_key → Entry widget

        self._build_summary()
        self._build_edit_panel()

    # ── compact summary (always visible) ────────────────────────────────────
    def _build_summary(self):
        conf    = int(self.data.get("confidence", 0))
        bar_col = C["red"] if conf >= 95 else C["amber"]

        tk.Frame(self, width=4, bg=bar_col).pack(side="left", fill="y")

        body = tk.Frame(self, bg=C["surface"])
        body.pack(side="left", fill="both", expand=True, padx=8, pady=6)

        def lbl(parent, text, fg, bold=True):
            return tk.Label(parent, text=text, bg=C["surface"], fg=fg,
                            font=(C["mono"], 9, "bold" if bold else ""),
                            anchor="w")

        # Line 1 — ICAO · callsign · model · confidence
        top = tk.Frame(body, bg=C["surface"])
        top.pack(fill="x")
        lbl(top, self.icao, C["text_accent"]).pack(side="left")
        lbl(top, f"  {self.data.get('callsign','').strip():<10}", C["text"]).pack(side="left")
        lbl(top, self.data.get("model","") or self.data.get("typecode","") or "—",
            C["text"]).pack(side="left", padx=(8, 0))
        lbl(top, f" {conf}%", bar_col).pack(side="right")

        # Line 2 — country · branch · operator · reg · altitude
        mid = tk.Frame(body, bg=C["surface"])
        mid.pack(fill="x", pady=(2, 0))
        parts = [
            self.data.get("country","") or "—",
            self.data.get("branch","")  or "—",
            self.data.get("operator","")or "—",
        ]
        if self.data.get("registration"): parts.append(self.data["registration"])
        if self.data.get("altitude"):     parts.append(f"{self.data['altitude']} ft")
        lbl(mid, "  ·  ".join(parts), C["text_dim"], bold=False).pack(side="left")

        # Line 3 — status badge + buttons
        btn_f = tk.Frame(body, bg=C["surface"])
        btn_f.pack(fill="x", pady=(6, 0))

        self._status_lbl = tk.Label(btn_f, text="● PENDING",
                                    bg=C["surface"], fg=C["amber"],
                                    font=(C["mono"], 8, "bold"))
        self._status_lbl.pack(side="left")

        def abtn(text, cmd, bg, fg):
            return tk.Button(btn_f, text=text, command=cmd,
                             bg=bg, fg=fg,
                             font=(C["mono"], 8, "bold"),
                             relief="flat", cursor="hand2",
                             padx=10, pady=3)

        self._btn_reject   = abtn("✕ REJECT",   self._on_reject,    C["border"],    C["text_dim"])
        self._btn_modify   = abtn("✎ MODIFY",   self._toggle_edit,  C["border_hi"], C["text"])
        self._btn_validate = abtn("✓ VALIDATE", self._on_validate,  C["green"],     C["bg"])

        self._btn_reject.pack(side="right", padx=(4, 0))
        self._btn_modify.pack(side="right",  padx=(4, 0))
        self._btn_validate.pack(side="right", padx=(4, 0))

    # ── full edit panel (hidden until MODIFY pressed) ────────────────────────
    def _build_edit_panel(self):
        self._edit_frame = tk.Frame(self, bg=C["bg"],
                                    highlightthickness=1,
                                    highlightbackground=C["border_hi"])

        # Scrollable inner area for all fields
        canvas = tk.Canvas(self._edit_frame, bg=C["bg"],
                           highlightthickness=0, height=320)
        vsb = tk.Scrollbar(self._edit_frame, orient="vertical",
                           command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=C["bg"])
        win   = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _resize_inner(event):
            canvas.itemconfig(win, width=event.width)
        canvas.bind("<Configure>", _resize_inner)

        def _scroll(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind("<MouseWheel>", _scroll)

        # ── section header ────────────────────────────────────────────────────
        def section_hdr(parent, text):
            f = tk.Frame(parent, bg=C["border"])
            f.pack(fill="x", pady=(12, 4))
            tk.Label(f, text=f"  {text}",
                     bg=C["border"], fg=C["text_dim"],
                     font=(C["mono"], 8, "bold"),
                     anchor="w").pack(fill="x", ipady=3)

        # ── editable field row (label left, entry right) ──────────────────────
        def make_field(parent, key, label):
            row_f = tk.Frame(parent, bg=C["bg"])
            row_f.pack(fill="x", padx=10, pady=2)

            tk.Label(row_f, text=f"{label:<22}",
                     bg=C["bg"], fg=C["text_dim"],
                     font=(C["mono"], 8, "bold"),
                     anchor="w", width=22).pack(side="left")

            e = tk.Entry(row_f, font=(C["mono"], 9),
                         bg=C["surface"], fg=C["text"],
                         insertbackground=C["text_accent"],
                         relief="flat", highlightthickness=1,
                         highlightbackground=C["border_hi"])
            e.insert(0, self.data.get(key, "") or "")
            e.pack(side="left", fill="x", expand=True, ipady=3, padx=(6, 10))
            self._entries[key] = e

        # Build all groups
        for item in _EDIT_GROUPS:
            key, label, kind = item
            if kind == "header":
                section_hdr(inner, key)
            else:
                make_field(inner, key, label)

        # Update scroll region when inner frame resizes
        def _update_scroll(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
        inner.bind("<Configure>", _update_scroll)

        # ── save / cancel buttons ─────────────────────────────────────────────
        btn_bar = tk.Frame(self._edit_frame, bg=C["bg"])
        btn_bar.pack(fill="x", padx=10, pady=8)

        tk.Button(btn_bar, text="SAVE CHANGES",
                  command=self._on_modified,
                  bg=C["amber"], fg=C["bg"],
                  font=(C["mono"], 9, "bold"),
                  relief="flat", cursor="hand2",
                  padx=14, pady=5).pack(side="left")

        tk.Button(btn_bar, text="CANCEL",
                  command=self._toggle_edit,
                  bg=C["border"], fg=C["text_dim"],
                  font=(C["mono"], 9, "bold"),
                  relief="flat", cursor="hand2",
                  padx=14, pady=5).pack(side="left", padx=(8, 0))

    # ── toggle expand ────────────────────────────────────────────────────────
    def _toggle_edit(self):
        if self._expanded:
            self._edit_frame.pack_forget()
            self._expanded = False
        else:
            self._edit_frame.pack(fill="x", padx=4, pady=(0, 4))
            self._expanded = True
        self.on_resize()

    # ── status helper ────────────────────────────────────────────────────────
    def _set_status(self, status: str, label: str):
        self._status = status
        color = self.STATUS_COLORS.get(status, C["text_dim"])
        self._status_lbl.config(text=f"● {label}", fg=color)
        for b in (self._btn_validate, self._btn_modify, self._btn_reject):
            b.config(state="disabled", bg=C["border"], fg=C["text_dim"])
        if self._expanded:
            self._toggle_edit()

    # ── actions ──────────────────────────────────────────────────────────────
    def _on_validate(self):
        self.predictor.set_verification(self.icao, "validated")
        self._set_status("validated", "VALIDATED")

    def _on_modified(self):
        # Collect every entry widget value back into a full corrections dict
        corrections = {key: e.get().strip() for key, e in self._entries.items()}
        self.predictor.set_verification(
            self.icao, "modified",
            corrections.get("country",""),
            corrections.get("branch",""),
            corrections.get("operator",""),
            corrections.get("model",""),
            corrections,          # full row overrides
        )
        self._set_status("modified", "MODIFIED")

    def _on_reject(self):
        self.predictor.set_verification(self.icao, "rejected")
        self._set_status("rejected", "REJECTED")
        for w in self.winfo_children():
            try: w.config(bg=C["bg"])
            except Exception: pass


# ─────────────────────────────────────────────────────────────────────────────
#  SBS PARSER
# ─────────────────────────────────────────────────────────────────────────────
def parse_sbs_line(line: str, ship_name: str) -> dict | None:
    try:
        parts = line.strip().split(",")
        if not parts or parts[0] != "MSG":
            return None
        while len(parts) < 22:
            parts.append("")
        row = {field: parts[i].strip() for i, field in enumerate(SBS_FIELDS)}
        row["ship_name"] = ship_name
        row["logged_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return row
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  AIRCRAFT CACHE
# ─────────────────────────────────────────────────────────────────────────────
class AircraftCache:
    def __init__(self):
        self._state: dict[str, dict] = defaultdict(dict)

    def update_and_get(self, row: dict) -> dict:
        icao = row.get("icao", "")
        if not icao:
            return row
        cached = self._state[icao]
        merged = dict(cached)
        for k, v in row.items():
            if v not in ("", None):
                merged[k] = v
        for meta in ("message_type", "transmission_type", "session_id",
                     "aircraft_id", "flight_id", "date_generated",
                     "time_generated", "date_logged", "time_logged",
                     "ship_name", "logged_at"):
            merged[meta] = row.get(meta, "")
        self._state[icao] = merged
        return merged


# ─────────────────────────────────────────────────────────────────────────────
#  TCP LISTENER
# ─────────────────────────────────────────────────────────────────────────────
class TCPListener(threading.Thread):
    def __init__(self, host, port, ship_name, data_queue, stop_event,
                 on_stats, predictor: MilitaryPredictor, on_military_detected):
        super().__init__(daemon=True, name="TCPListener")
        self.host                = host
        self.port                = port
        self.ship_name           = ship_name
        self.data_queue          = data_queue
        self.stop_event          = stop_event
        self.on_stats            = on_stats
        self.predictor           = predictor
        self.on_military_detected = on_military_detected
        self.cache               = AircraftCache()

        # Track which ICAOs have already triggered a popup this session
        self._popup_shown: set[str] = set()

    def run(self):
        buf = ""
        while not self.stop_event.is_set():
            try:
                log.info(f"Connecting  {self.host}:{self.port}")
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(10.0)
                    s.connect((self.host, self.port))
                    s.settimeout(2.0)
                    log.info("TCP connected")
                    while not self.stop_event.is_set():
                        try:
                            chunk = s.recv(4096).decode("utf-8", errors="ignore")
                            if not chunk:
                                break
                            buf += chunk
                            lines = buf.split("\n")
                            buf = lines[-1]
                            for line in lines[:-1]:
                                self._process_line(line)
                        except socket.timeout:
                            continue
                        except Exception as exc:
                            log.error(f"Recv error: {exc}")
                            break
            except Exception as exc:
                if not self.stop_event.is_set():
                    log.error(f"TCP error: {exc}  — retry in {RECONNECT_DELAY}s")
                    time.sleep(RECONNECT_DELAY)

    def _process_line(self, line: str):
        row = parse_sbs_line(line, self.ship_name)
        if not row:
            return

        # Merge partial messages
        merged = self.cache.update_and_get(row)

        # Run military prediction
        merged = self.predictor.predict(merged)

        # Trigger popup once per ICAO if military detected
        icao = merged.get("icao", "").upper()
        if (
            merged.get("military") in ("YES", "POSSIBLE")
            and icao
            and icao not in self._popup_shown
            and icao not in self.predictor.verification_status
        ):
            self._popup_shown.add(icao)
            # Schedule popup on main thread — Tkinter is not thread-safe
            self.on_military_detected(dict(merged))

        # Queue enriched row for CSV writing
        try:
            self.data_queue.put_nowait(merged)
            self.on_stats("rx")
        except queue.Full:
            log.warning("Queue full — row dropped")


# ─────────────────────────────────────────────────────────────────────────────
#  BATCH CSV WRITER
# ─────────────────────────────────────────────────────────────────────────────
class BatchWriter(threading.Thread):
    def __init__(self, data_queue, stop_event, csv_path, on_stats):
        super().__init__(daemon=True, name="BatchWriter")
        self.data_queue   = data_queue
        self.stop_event   = stop_event
        self.csv_path     = csv_path
        self.on_stats     = on_stats
        self.rows_written = 0

    def _flush(self, writer, file_obj, batch):
        writer.writerows(batch)
        file_obj.flush()
        self.rows_written += len(batch)
        self.on_stats("write", len(batch))

    def run(self):
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer     = csv.DictWriter(f, fieldnames=CSV_HEADER, extrasaction="ignore")
            last_flush = time.monotonic()
            while not self.stop_event.is_set() or not self.data_queue.empty():
                batch = []
                try:
                    while len(batch) < BATCH_SIZE:
                        batch.append(self.data_queue.get_nowait())
                except queue.Empty:
                    pass
                now = time.monotonic()
                if batch and (len(batch) >= BATCH_SIZE or now - last_flush >= BATCH_INTERVAL):
                    self._flush(writer, f, batch)
                    last_flush = now
                elif not batch:
                    time.sleep(0.1)
            tail = []
            while not self.data_queue.empty():
                try:
                    tail.append(self.data_queue.get_nowait())
                except queue.Empty:
                    break
            if tail:
                self._flush(writer, f, tail)
        log.info(f"Writer closed  total_rows={self.rows_written:,}")


# ─────────────────────────────────────────────────────────────────────────────
#  SESSION
# ─────────────────────────────────────────────────────────────────────────────
class Session:
    def __init__(self, ship_name: str):
        self.ship_name = ship_name
        self.start_ts  = datetime.now()
        self.stop_ts   = None
        self._running  = DATA_DIR / f"{ship_name}_{self.start_ts.strftime('%Y%m%d_%H%M%S')}_RUNNING.csv"
        self._final    = None

    def init_csv(self):
        with open(self._running, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADER).writeheader()
        log.info(f"Session started  {self._running.name}")

    def finalize(self) -> Path:
        self.stop_ts = datetime.now()
        name = (
            f"{self.ship_name}"
            f"_{self.start_ts.strftime('%Y%m%d_%H%M%S')}"
            f"_TO_{self.stop_ts.strftime('%Y%m%d_%H%M%S')}.csv"
        )
        self._final = DATA_DIR / name
        if self._running.exists():
            self._running.rename(self._final)
        log.info(f"Session saved  {self._final.name}")
        return self._final

    def mark_incomplete(self):
        if self._running.exists():
            inc = Path(str(self._running).replace("_RUNNING.csv", "_INCOMPLETE.csv"))
            self._running.rename(inc)
            log.warning(f"Session incomplete  {inc.name}")

    @property
    def csv_path(self) -> Path:
        return self._running

    @property
    def final_path(self) -> Path | None:
        return self._final


# ─────────────────────────────────────────────────────────────────────────────
#  EXCEL EXPORT
# ─────────────────────────────────────────────────────────────────────────────
def export_to_excel(csv_path: Path) -> Path:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.cell import WriteOnlyCell
    except ImportError:
        raise RuntimeError("openpyxl not installed.  Run:  pip install openpyxl")

    xlsx_path = csv_path.with_suffix(".xlsx")
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("ADS-B Data")

    hdr_font  = Font(bold=True, color="F5F5F0", name="Courier New")
    hdr_fill  = PatternFill("solid", fgColor="0D0D0D")
    hdr_align = Alignment(horizontal="center")

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for i, row in enumerate(csv.reader(f)):
            cells = []
            for val in row:
                c = WriteOnlyCell(ws, value=val)
                if i == 0:
                    c.font      = hdr_font
                    c.fill      = hdr_fill
                    c.alignment = hdr_align
                cells.append(c)
            ws.append(cells)

    wb.save(xlsx_path)
    log.info(f"Excel exported  {xlsx_path.name}")
    return xlsx_path


# ─────────────────────────────────────────────────────────────────────────────
#  COLOUR PALETTE
# ─────────────────────────────────────────────────────────────────────────────
C = {
    "bg":          "#080C0F",
    "surface":     "#0E1419",
    "border":      "#1C2730",
    "border_hi":   "#2A3D50",
    "text":        "#D4D8DC",
    "text_dim":    "#4A5668",
    "text_accent": "#7EC8E3",
    "green":       "#2EC27E",
    "green_dim":   "#1A7A4A",
    "red":         "#C0392B",
    "amber":       "#E0A030",
    "mono":        "Courier New",
    "ui":          "Helvetica",
}


# ─────────────────────────────────────────────────────────────────────────────
#  WIDGETS
# ─────────────────────────────────────────────────────────────────────────────
class ShipNameDialog(tk.Toplevel):
    def __init__(self, parent, callback):
        super().__init__(parent)
        self.title("Ship Name")
        self.configure(bg=C["bg"])
        self.resizable(False, False)
        self.grab_set()
        self.callback = callback
        self._build()
        self.transient(parent)
        self.geometry("360x220")

    def _build(self):
        tk.Frame(self, height=8, bg=C["bg"]).pack()
        tk.Label(self, text="SHIP NAME", bg=C["bg"], fg=C["text_dim"],
                 font=(C["mono"], 8, "bold")).pack(pady=(14, 2))
        tk.Label(self, text="Enter Ship Name", bg=C["bg"], fg=C["text"],
                 font=(C["ui"], 11)).pack(pady=(0, 14))
        self._entry = tk.Entry(
            self, font=(C["mono"], 13), width=22,
            bg=C["surface"], fg=C["text"],
            insertbackground=C["text_accent"],
            relief="flat", bd=0,
            highlightthickness=1,
            highlightbackground=C["border_hi"],
            highlightcolor=C["text_accent"],
        )
        self._entry.pack(ipady=6, padx=40)
        self._entry.focus()
        tk.Frame(self, height=16, bg=C["bg"]).pack()
        tk.Button(
            self, text="CONFIRM", command=self._confirm,
            bg=C["green"], fg=C["bg"],
            font=(C["mono"], 10, "bold"),
            relief="flat", cursor="hand2",
            activebackground=C["green_dim"],
            activeforeground=C["text"],
            padx=20, pady=6,
        ).pack()
        self.bind("<Return>", lambda _: self._confirm())

    def _confirm(self):
        name = self._entry.get().strip()
        if not name:
            self._entry.config(highlightbackground=C["red"])
            return
        self.callback(name)
        self.destroy()


class StatTile(tk.Frame):
    def __init__(self, parent, label: str, **kw):
        super().__init__(parent, bg=C["surface"],
                         highlightthickness=1,
                         highlightbackground=C["border"], **kw)
        tk.Label(self, text=label.upper(), bg=C["surface"], fg=C["text_dim"],
                 font=(C["mono"], 7, "bold")).pack(pady=(10, 0))
        self._val = tk.Label(self, text="0", bg=C["surface"], fg=C["text"],
                             font=(C["mono"], 20, "bold"))
        self._val.pack(pady=(2, 10))

    def set(self, value: str):
        self._val.config(text=value)

    def set_color(self, color: str):
        self._val.config(fg=color)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN APP
# ─────────────────────────────────────────────────────────────────────────────
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ADS-B TCP LOGGER  //  MILITARY PREDICTION")
        self.resizable(False, False)
        self.configure(bg=C["bg"])

        self._cfg          = load_config()
        self._ship_name    = self._cfg.get("ship_name", "")
        self._session      = None
        self._listener     = None
        self._writer       = None
        self._stop_event   = None
        self._data_queue   = None
        self._rx_count     = 0
        self._write_count  = 0
        self._mil_count    = 0
        self._running      = False
        self._start_time   = None
        self._blink        = True

        # Predictor lives for the app lifetime, reset between sessions
        self._predictor = MilitaryPredictor()

        self._build_ui()

        if not self._ship_name:
            self.after(120, self._ask_ship_name)
        else:
            self._lbl_ship.config(text=self._ship_name.upper())
            self._check_orphans()

    def _ask_ship_name(self):
        def on_confirm(name):
            self._ship_name = name
            self._cfg["ship_name"] = name
            save_config(self._cfg)
            self._lbl_ship.config(text=name.upper())
            self._check_orphans()
        ShipNameDialog(self, on_confirm)

    def _check_orphans(self):
        for f in DATA_DIR.glob("*_RUNNING.csv"):
            target = Path(str(f).replace("_RUNNING.csv", "_INCOMPLETE.csv"))
            f.rename(target)
            log.warning(f"Orphan renamed: {target.name}")

    # ── UI ────────────────────────────────────────────────────────────────────
    def _build_ui(self):
        W = 760

        # ── Title bar ─────────────────────────────────────────────────────────
        title_bar = tk.Frame(self, bg=C["bg"])
        title_bar.pack(fill="x", padx=20, pady=(18, 0))

        tk.Label(title_bar, text="ADS-B TCP LOGGER",
                 bg=C["bg"], fg=C["text"],
                 font=(C["mono"], 15, "bold")).pack(side="left", expand=True)

        ship_col = tk.Frame(title_bar, bg=C["bg"])
        ship_col.pack(side="right")
        tk.Label(ship_col, text="SHIP NAME", bg=C["bg"], fg=C["text_dim"],
                 font=(C["mono"], 7, "bold")).pack(anchor="e")
        self._lbl_ship = tk.Label(ship_col, text="—",
                                   bg=C["bg"], fg=C["text_accent"],
                                   font=(C["mono"], 11, "bold"))
        self._lbl_ship.pack(anchor="e")

        tk.Frame(self, height=1, bg=C["border"]).pack(fill="x", pady=(12, 0))

        # ── Connection row ────────────────────────────────────────────────────
        conn = tk.Frame(self, bg=C["bg"])
        conn.pack(fill="x", padx=20, pady=(14, 0))

        def field(parent, label_text, default, width):
            col = tk.Frame(parent, bg=C["bg"])
            col.pack(side="left", padx=(0, 18))
            tk.Label(col, text=label_text, bg=C["bg"], fg=C["text_dim"],
                     font=(C["mono"], 7, "bold")).pack(anchor="w")
            e = tk.Entry(col, width=width, font=(C["mono"], 11),
                         bg=C["surface"], fg=C["text"],
                         insertbackground=C["text_accent"],
                         relief="flat", bd=0,
                         highlightthickness=1,
                         highlightbackground=C["border"],
                         highlightcolor=C["text_accent"])
            e.insert(0, default)
            e.pack(ipady=5, pady=(3, 0))
            return e

        self._ent_host = field(conn, "SOURCE HOST", "127.0.0.1", 18)
        self._ent_port = field(conn, "SOURCE PORT", "30003",      7)

        right = tk.Frame(conn, bg=C["bg"])
        right.pack(side="right", pady=(12, 0))

        ind_row = tk.Frame(right, bg=C["bg"])
        ind_row.pack(anchor="e")
        self._live_canvas = tk.Canvas(ind_row, width=10, height=10,
                                      bg=C["bg"], highlightthickness=0)
        self._live_canvas.pack(side="left", padx=(0, 5))
        self._live_dot = self._live_canvas.create_oval(1, 1, 9, 9,
                                                       fill=C["border"], outline="")
        self._lbl_live = tk.Label(ind_row, text="IDLE",
                                  bg=C["bg"], fg=C["text_dim"],
                                  font=(C["mono"], 8, "bold"))
        self._lbl_live.pack(side="left")

        tk.Frame(self, height=1, bg=C["border"]).pack(fill="x", pady=(14, 0))

        # ── Buttons ───────────────────────────────────────────────────────────
        ctrl = tk.Frame(self, bg=C["bg"])
        ctrl.pack(fill="x", padx=20, pady=14)

        def btn(parent, text, cmd, bg, fg, state="normal"):
            return tk.Button(parent, text=text, command=cmd,
                             bg=bg, fg=fg,
                             font=(C["mono"], 10, "bold"),
                             relief="flat", cursor="hand2",
                             activebackground=C["border_hi"],
                             activeforeground=C["text"],
                             padx=18, pady=7, state=state)

        self._btn_start     = btn(ctrl, "START",            self._start_session, C["green"],     C["bg"])
        self._btn_stop      = btn(ctrl, "STOP",             self._stop_session,  C["border"],    C["text_dim"], "disabled")
        self._btn_ship_name = btn(ctrl, "CHANGE SHIP NAME", self._ask_ship_name, C["border_hi"], C["text"])
        self._btn_export    = btn(ctrl, "EXPORT EXCEL",     self._export_excel,  C["border"],    C["text_dim"], "disabled")

        self._btn_start.pack(side="left", padx=(0, 8))
        self._btn_stop.pack(side="left",  padx=(0, 8))
        self._btn_export.pack(side="right")
        self._btn_ship_name.pack(side="right", padx=(0, 8))

        tk.Frame(self, height=1, bg=C["border"]).pack(fill="x")

        # ── Stats tiles ───────────────────────────────────────────────────────
        tiles = tk.Frame(self, bg=C["bg"])
        tiles.pack(fill="x", padx=20, pady=14)
        tiles.grid_columnconfigure((0, 1, 2), weight=1)

        def tile(label, col):
            t = StatTile(tiles, label)
            t.grid(row=0, column=col, sticky="ew",
                   padx=(0 if col == 0 else 5, 0))
            return t

        self._tile_wr  = tile("Written",  0)
        self._tile_mil = tile("Military", 1)
        self._tile_dur = tile("Duration", 2)

        tk.Frame(self, height=1, bg=C["border"]).pack(fill="x")

        # ── Military contacts panel ───────────────────────────────────────────
        contacts_hdr = tk.Frame(self, bg=C["bg"])
        contacts_hdr.pack(fill="x", padx=20, pady=(10, 4))

        tk.Label(contacts_hdr, text="MILITARY CONTACTS",
                 bg=C["bg"], fg=C["text_dim"],
                 font=(C["mono"], 8, "bold")).pack(side="left")

        self._contacts_count_lbl = tk.Label(
            contacts_hdr, text="0 contacts",
            bg=C["bg"], fg=C["text_dim"],
            font=(C["mono"], 8),
        )
        self._contacts_count_lbl.pack(side="right")

        # Scrollable canvas for contact rows
        scroll_outer = tk.Frame(self, bg=C["border"], bd=0)
        scroll_outer.pack(fill="both", expand=True, padx=20, pady=(0, 0))

        self._contacts_canvas = tk.Canvas(
            scroll_outer, bg=C["bg"],
            highlightthickness=0, bd=0,
        )
        scrollbar = tk.Scrollbar(scroll_outer, orient="vertical",
                                 command=self._contacts_canvas.yview)
        self._contacts_canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side="right", fill="y")
        self._contacts_canvas.pack(side="left", fill="both", expand=True)

        # Inner frame that holds the actual rows
        self._contacts_inner = tk.Frame(self._contacts_canvas, bg=C["bg"])
        self._canvas_window  = self._contacts_canvas.create_window(
            (0, 0), window=self._contacts_inner, anchor="nw"
        )

        # Resize inner frame width when canvas resizes
        def _on_canvas_resize(event):
            self._contacts_canvas.itemconfig(self._canvas_window, width=event.width)
        self._contacts_canvas.bind("<Configure>", _on_canvas_resize)

        # Mousewheel scrolling
        def _on_mousewheel(event):
            self._contacts_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self._contacts_canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # Placeholder shown when no contacts yet
        self._no_contacts_lbl = tk.Label(
            self._contacts_inner,
            text="No military contacts detected yet",
            bg=C["bg"], fg=C["text_dim"],
            font=(C["mono"], 9), pady=20,
        )
        self._no_contacts_lbl.pack()

        # ── Status bar ────────────────────────────────────────────────────────
        tk.Frame(self, height=1, bg=C["border"]).pack(fill="x")
        status_bar = tk.Frame(self, bg=C["bg"])
        status_bar.pack(fill="x")
        self._lbl_status = tk.Label(status_bar, text="  Ready",
                                    bg=C["bg"], fg=C["text_dim"],
                                    font=(C["mono"], 8), anchor="w")
        self._lbl_status.pack(side="left", fill="x", expand=True, ipady=4, padx=4)

        file_row = tk.Frame(self, bg=C["bg"])
        file_row.pack(fill="x", padx=20, pady=(6, 14))
        tk.Label(file_row, text="SAVED FILE", bg=C["bg"], fg=C["text_dim"],
                 font=(C["mono"], 7, "bold")).pack(anchor="w")
        self._lbl_file = tk.Label(file_row, text="—",
                                  bg=C["bg"], fg=C["text_accent"],
                                  font=(C["mono"], 8), anchor="w",
                                  wraplength=W - 40)
        self._lbl_file.pack(anchor="w")

        self.geometry(f"{W}x680")
        self._contact_widgets: dict[str, MilContactRow] = {}
        self._tick()

    # ── Start ─────────────────────────────────────────────────────────────────
    def _start_session(self):
        host     = self._ent_host.get().strip()
        port_str = self._ent_port.get().strip()

        if not port_str.isdigit():
            messagebox.showerror("Error", "Source port must be a number.")
            return
        if not self._ship_name:
            messagebox.showerror("Error", "No ship name configured.")
            return

        port = int(port_str)

        self._rx_count    = 0
        self._write_count = 0
        self._mil_count   = 0
        self._stop_event  = threading.Event()
        self._data_queue  = queue.Queue(maxsize=QUEUE_MAXSIZE)

        # Clear contacts table
        for w in self._contacts_inner.winfo_children():
            w.destroy()
        self._contact_widgets.clear()
        self._no_contacts_lbl = tk.Label(
            self._contacts_inner,
            text="No military contacts detected yet",
            bg=C["bg"], fg=C["text_dim"],
            font=(C["mono"], 9), pady=20,
        )
        self._no_contacts_lbl.pack()
        self._contacts_count_lbl.config(text="0 contacts", fg=C["text_dim"])

        self._predictor.clear_session()

        self._session = Session(self._ship_name)
        self._session.init_csv()

        self._listener = TCPListener(
            host, port, self._ship_name,
            self._data_queue, self._stop_event, self._on_stats,
            self._predictor, self._on_military_detected,
        )
        self._writer = BatchWriter(
            self._data_queue, self._stop_event,
            self._session.csv_path, self._on_stats,
        )

        self._listener.start()
        self._writer.start()
        self._start_time = datetime.now()
        self._running    = True

        self._btn_start.config(state="disabled",  bg=C["border"],  fg=C["text_dim"])
        self._btn_stop.config(state="normal",      bg=C["red"],     fg=C["text"])
        self._btn_export.config(state="disabled",  bg=C["border"],  fg=C["text_dim"])
        self._btn_ship_name.config(state="disabled")
        for e in (self._ent_host, self._ent_port):
            e.config(state="disabled")

        self._set_status(f"LIVE  {self._ship_name}  {host}:{port}")
        self._lbl_live.config(text="LIVE", fg=C["green"])

    # ── Stop ──────────────────────────────────────────────────────────────────
    def _stop_session(self):
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        self._set_status("Flushing buffers...")
        self.update()

        if self._writer:
            self._writer.join(timeout=12)

        final = self._session.finalize()

        # Write military tracks + update training dataset
        self._set_status("Writing military tracks...")
        self.update()
        self._predictor.write_military_tracks()
        self._predictor.update_training_dataset()
        self._predictor.clear_session()

        self._btn_start.config(state="normal",    bg=C["green"],     fg=C["bg"])
        self._btn_stop.config(state="disabled",   bg=C["border"],    fg=C["text_dim"])
        self._btn_export.config(state="normal",   bg=C["border_hi"], fg=C["text"])
        self._btn_ship_name.config(state="normal")
        for e in (self._ent_host, self._ent_port):
            e.config(state="normal")

        self._lbl_live.config(text="IDLE", fg=C["text_dim"])
        self._live_canvas.itemconfig(self._live_dot, fill=C["border"])
        self._set_status(f"Saved  {final.name}")
        self._lbl_file.config(text=str(final.resolve()))

    # ── Military detection callback ───────────────────────────────────────────
    def _on_military_detected(self, row: dict):
        """Called from TCPListener thread — schedules row addition on main thread."""
        self._mil_count += 1
        self.after(0, lambda r=dict(row): self._add_contact_row(r))

    def _add_contact_row(self, row: dict):
        """Always runs on Tkinter main thread. Adds one row to the contacts table."""
        icao = row.get("icao", "").upper()
        if not icao or icao in self._contact_widgets:
            return

        # Remove placeholder label on first contact
        if self._no_contacts_lbl.winfo_ismapped():
            self._no_contacts_lbl.pack_forget()

        # Separator between rows
        if self._contact_widgets:
            tk.Frame(self._contacts_inner, height=1,
                     bg=C["border"]).pack(fill="x")

        def on_resize():
            self._contacts_inner.update_idletasks()
            self._contacts_canvas.configure(
                scrollregion=self._contacts_canvas.bbox("all")
            )

        contact = MilContactRow(self._contacts_inner, self._predictor, row, on_resize)
        contact.pack(fill="x", pady=(0, 0))
        self._contact_widgets[icao] = contact

        # Update scroll region
        self._contacts_inner.update_idletasks()
        self._contacts_canvas.configure(
            scrollregion=self._contacts_canvas.bbox("all")
        )
        # Auto-scroll to latest
        self._contacts_canvas.yview_moveto(1.0)

        # Update contacts count label
        n = len(self._contact_widgets)
        self._contacts_count_lbl.config(
            text=f"{n} contact{'s' if n != 1 else ''}",
            fg=C["amber"] if n > 0 else C["text_dim"],
        )

    # ── Export ────────────────────────────────────────────────────────────────
    def _export_excel(self):
        if not self._session or not self._session.final_path:
            messagebox.showwarning("Warning", "No completed session available.")
            return
        self._set_status("Exporting to Excel...")
        self._btn_export.config(state="disabled")
        self.update()
        csv_path = self._session.final_path

        def run():
            try:
                xlsx = export_to_excel(csv_path)
                self.after(0, lambda: self._set_status(f"Exported  {xlsx.name}"))
                self.after(0, lambda: self._btn_export.config(state="normal"))
                self.after(0, lambda: messagebox.showinfo("Export Complete", str(xlsx.resolve())))
            except Exception as exc:
                self.after(0, lambda: messagebox.showerror("Export Error", str(exc)))
                self.after(0, lambda: self._set_status("Export failed"))
                self.after(0, lambda: self._btn_export.config(state="normal"))

        threading.Thread(target=run, daemon=True).start()

    # ── Stats ─────────────────────────────────────────────────────────────────
    def _on_stats(self, event: str, count: int = 1):
        if event == "rx":
            self._rx_count += count
        elif event == "write":
            self._write_count += count

    # ── 1 Hz tick ─────────────────────────────────────────────────────────────
    def _tick(self):
        self._tile_wr.set(f"{self._write_count:,}")
        self._tile_mil.set(f"{self._mil_count:,}")
        if self._mil_count > 0:
            self._tile_mil.set_color(C["amber"])

        if self._running and self._start_time:
            elapsed = datetime.now() - self._start_time
            h, rem  = divmod(int(elapsed.total_seconds()), 3600)
            m, s    = divmod(rem, 60)
            self._tile_dur.set(f"{h:02d}:{m:02d}:{s:02d}")
            self._blink = not self._blink
            self._live_canvas.itemconfig(
                self._live_dot,
                fill=C["green"] if self._blink else C["bg"]
            )

        self.after(1000, self._tick)

    def _set_status(self, msg: str):
        self._lbl_status.config(text=f"  {msg}")

    # ── Close ─────────────────────────────────────────────────────────────────
    def on_close(self):
        if self._running:
            if messagebox.askyesno("Quit", "Session is active. Stop and save before quitting?"):
                self._stop_session()
            else:
                return
        if self._session and not self._session.final_path:
            self._session.mark_incomplete()
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()