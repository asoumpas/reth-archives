#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
diavgeia_sync.py
================
Κατεβάζει πράξεις (αποφάσεις/ανακοινώσεις/δαπάνες) από τη Διαύγεια
(opendata.diavgeia.gov.gr) για τους 6 φορείς της Π.Ε. Ρεθύμνης και τις
αποθηκεύει σε τοπική βάση SQLite (diavgeia.db).

Χαρακτηριστικά:
  * Υποστήριξη ΠΟΛΛΑΠΛΩΝ φορέων σε ένα ενιαίο repo.
  * Αυτόματη ανίχνευση του σωστού organizationUid κάθε φορέα
    (δοκιμάζει latinName slugs + αριθμητικά uid μέχρι να βρει αποτελέσματα),
    με caching σε org_cache στη βάση.
  * Πλήρες πρώτο κατέβασμα (bootstrap) ΚΑΙ incremental sync (μόνο νέες πράξεις).
  * Ποσό (amount) κρατείται ΜΟΝΟ όταν το API το δίνει δομημένα.

Χρήση:
  python diavgeia_sync.py            # κανονικό incremental sync (default)
  python diavgeia_sync.py --full     # πλήρες κατέβασμα από την αρχή
  python diavgeia_sync.py --org dimosrethymnis   # μόνο έναν φορέα
  python diavgeia_sync.py --since 2024-01-01     # από συγκεκριμένη ημερομηνία
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone

import requests

# --------------------------------------------------------------------------
# ΡΥΘΜΙΣΕΙΣ
# --------------------------------------------------------------------------

OPENDATA_ROOT = "https://opendata.diavgeia.gov.gr/luminapi/opendata"
SEARCH_URL = OPENDATA_ROOT + "/search"
ORG_URL = OPENDATA_ROOT + "/organizations"

DB_PATH = "diavgeia.db"
PAGE_SIZE = 500           # μέγεθος σελίδας του API (max ~500)
REQUEST_TIMEOUT = 60      # δευτερόλεπτα
SLEEP_BETWEEN = 0.4       # ευγενική παύση μεταξύ κλήσεων (sec)
MAX_RETRIES = 5

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "diavgeia-rethymno-sync/1.0 (+github actions; open data)",
}

# Οι 6 φορείς της Π.Ε. Ρεθύμνης.
# Για κάθε φορέα δίνουμε ΠΟΛΛΑΠΛΕΣ υποψήφιες μορφές organizationUid
# (latinName slugs + γνωστά αριθμητικά uid). Ο scraper δοκιμάζει με τη σειρά
# μέχρι κάποια να επιστρέψει αποτελέσματα, και την αποθηκεύει στο cache.
#
# key            -> σταθερό αναγνωριστικό (χρησιμοποιείται στη βάση & στη σελίδα)
# label          -> εμφανιζόμενο όνομα φορέα
# candidates     -> λίστα υποψήφιων organizationUid προς δοκιμή (με σειρά προτίμησης)
ORGANIZATIONS = [
    {
        "key": "pe_rethymnis",
        "label": "Π.Ε. Ρεθύμνης (Περιφέρεια Κρήτης)",
        "candidates": [
            "99201018",        # ΕΠΙΒΕΒΑΙΩΜΕΝΟ uid (Περιφέρεια Κρήτης – Π.Ε. Ρεθύμνου)
            "perethymnis",
            "pe_rethymnis",
            "perifereiakienotitarethymnis",
        ],
        # ΠΡΟΣΟΧΗ: αν το πρώτο bootstrap δείξει ότι αυτός ο κωδικός επιστρέφει
        # πράξεις ΟΛΗΣ της Περιφέρειας Κρήτης (Ηράκλειο/Χανιά/Λασίθι), ξεσχολίασε
        # την παρακάτω γραμμή για να κρατάς μόνο όσες αφορούν Ρέθυμνο:
        # "unit_filter": ["ρεθύμν", "ρεθυμν"],
    },
    {
        "key": "dimos_rethymnis",
        "label": "Δήμος Ρεθύμνης",
        "candidates": [
            "99222402",        # ΕΠΙΒΕΒΑΙΩΜΕΝΟ uid
            "dimosrethymnis",
            "dimosrethymnou",
            "6263",
        ],
    },
    {
        "key": "dimos_amariou",
        "label": "Δήμος Αμαρίου",
        "candidates": [
            "99222404",        # ΕΠΙΒΕΒΑΙΩΜΕΝΟ uid
            "dimosamariou",
            "dimos_amariou",
        ],
    },
    {
        "key": "dimos_agiou_vasileiou",
        "label": "Δήμος Αγίου Βασιλείου",
        "candidates": [
            "99222403",        # ΕΠΙΒΕΒΑΙΩΜΕΝΟ uid
            "dimosagiouvasileiou",
            "dimoslampis",
        ],
    },
    {
        "key": "dimos_anogeion",
        "label": "Δήμος Ανωγείων",
        "candidates": [
            "99222406",        # ΕΠΙΒΕΒΑΙΩΜΕΝΟ uid
            "dimos_anogeia",   # επίσης γνωστό latinName (παλαιό uid 6039)
            "dimosanogeion",
        ],
    },
    {
        "key": "dimos_mylopotamou",
        "label": "Δήμος Μυλοποτάμου",
        "candidates": [
            "99222405",        # ΕΠΙΒΕΒΑΙΩΜΕΝΟ uid
            "dimosmylopotamou",  # επίσης γνωστό latinName
            "dimos_mylopotamou",
        ],
    },
]


# --------------------------------------------------------------------------
# ΒΑΣΗ ΔΕΔΟΜΕΝΩΝ
# --------------------------------------------------------------------------

def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            ada              TEXT PRIMARY KEY,
            org_key          TEXT NOT NULL,
            org_label        TEXT,
            organization_uid TEXT,
            subject          TEXT,
            decision_type    TEXT,
            decision_type_label TEXT,
            issue_date       TEXT,
            submission_ts    TEXT,
            protocol         TEXT,
            signer           TEXT,
            unit             TEXT,
            amount           REAL,
            amount_currency  TEXT,
            year             INTEGER,
            doc_url          TEXT,
            raw_json         TEXT,
            fetched_at       TEXT,
            -- Πεδία προσέγγισης ποσών (όπως στη Γαύδο). Γεμίζουν αργότερα
            -- από ξεχωριστό extract_amounts.py που διαβάζει τα PDF.
            doc_category        TEXT,    -- 'xep' | 'analipsi' | 'symvasi' | 'other'
            count_in_total      INTEGER DEFAULT 0,  -- 1 ΜΟΝΟ για ΧΕΠ (αποφυγή διπλομέτρησης)
            amount_confidence   TEXT,    -- 'high' | 'medium' | 'low' | NULL
            amount_method       TEXT,
            amount_processed_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_org      ON decisions(org_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_year     ON decisions(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_type     ON decisions(decision_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_issue    ON decisions(issue_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_category ON decisions(doc_category)")

    # Migration: αν η βάση προϋπάρχει χωρίς τις νέες στήλες, πρόσθεσέ τες.
    existing = {r[1] for r in cur.execute("PRAGMA table_info(decisions)")}
    for col, ddl in [
        ("doc_category",        "ALTER TABLE decisions ADD COLUMN doc_category TEXT"),
        ("count_in_total",      "ALTER TABLE decisions ADD COLUMN count_in_total INTEGER DEFAULT 0"),
        ("amount_confidence",   "ALTER TABLE decisions ADD COLUMN amount_confidence TEXT"),
        ("amount_method",       "ALTER TABLE decisions ADD COLUMN amount_method TEXT"),
        ("amount_processed_at", "ALTER TABLE decisions ADD COLUMN amount_processed_at TEXT"),
    ]:
        if col not in existing:
            cur.execute(ddl)

    # cache του ανιχνευμένου uid ανά φορέα
    cur.execute("""
        CREATE TABLE IF NOT EXISTS org_cache (
            org_key          TEXT PRIMARY KEY,
            organization_uid TEXT,
            resolved_at      TEXT,
            total_seen       INTEGER
        )
    """)

    # μεταδεδομένα συγχρονισμού (π.χ. τελευταίο sync ανά φορέα)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_meta (
            org_key       TEXT PRIMARY KEY,
            last_issue    TEXT,
            last_sync_at  TEXT
        )
    """)
    conn.commit()


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def http_get_json(url, params):
    """GET με retries -> dict (ή None αν 4xx εκτός 429)."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS,
                             timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                # το API επιστρέφει JSON όταν ζητάμε wt=json
                try:
                    return r.json()
                except ValueError:
                    # μερικές φορές το content-type είναι λάθος· δοκίμασε χειροκίνητα
                    return json.loads(r.text)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** attempt, 30)
                print(f"  [retry {attempt}] HTTP {r.status_code}, αναμονή {wait}s...")
                time.sleep(wait)
                continue
            # 400/404 κ.λπ. -> δεν δουλεύει αυτό το query
            return None
        except requests.RequestException as e:
            last_exc = e
            wait = min(2 ** attempt, 30)
            print(f"  [retry {attempt}] σφάλμα δικτύου: {e}; αναμονή {wait}s...")
            time.sleep(wait)
    if last_exc:
        print(f"  Αποτυχία μετά από {MAX_RETRIES} προσπάθειες: {last_exc}")
    return None


def search_page(org_uid, page, size, from_date=None, sort="recent"):
    """Μία σελίδα αποτελεσμάτων search για συγκεκριμένο organizationUid."""
    q = f'organizationUid:"{org_uid}"'
    params = {
        "q": q,
        "page": page,
        "size": size,
        "sort": sort,
        "wt": "json",
    }
    if from_date:
        # φίλτρο ημερομηνίας έκδοσης (issueDate) >= from_date
        params["from_issue"] = from_date
    return http_get_json(SEARCH_URL, params)


# --------------------------------------------------------------------------
# ΑΥΤΟ-ΑΝΙΧΝΕΥΣΗ organizationUid (όπως το find_working_query της Γαύδου)
# --------------------------------------------------------------------------

def find_working_query(org, conn):
    """
    Δοκιμάζει με τη σειρά τις υποψήφιες μορφές organizationUid του φορέα.
    Επιστρέφει το πρώτο uid που επιστρέφει >0 αποτελέσματα.
    Αποθηκεύει το αποτέλεσμα στο org_cache για επόμενες φορές.
    """
    cur = conn.cursor()
    cached = cur.execute(
        "SELECT organization_uid FROM org_cache WHERE org_key = ?",
        (org["key"],)
    ).fetchone()
    if cached and cached[0]:
        # Επαλήθευσε γρήγορα ότι ακόμη δουλεύει
        data = search_page(cached[0], page=0, size=1)
        if data and int(data.get("info", {}).get("total", 0) or 0) > 0:
            print(f"  ✓ (cache) {org['label']}: uid={cached[0]} "
                  f"[{data['info']['total']} πράξεις]")
            return cached[0]
        print(f"  ! cache uid={cached[0]} δεν επιστρέφει αποτελέσματα, "
              f"επαναπροσδιορισμός...")

    print(f"  Ανίχνευση organizationUid για: {org['label']}")
    for cand in org["candidates"]:
        time.sleep(SLEEP_BETWEEN)
        data = search_page(cand, page=0, size=1)
        if data is None:
            print(f"    - '{cand}': άκυρο/δεν απαντά")
            continue
        total = int(data.get("info", {}).get("total", 0) or 0)
        if total > 0:
            print(f"    ✓ '{cand}': {total} πράξεις -> ΕΠΙΛΕΧΘΗΚΕ")
            cur.execute(
                "INSERT OR REPLACE INTO org_cache "
                "(org_key, organization_uid, resolved_at, total_seen) "
                "VALUES (?, ?, ?, ?)",
                (org["key"], cand, datetime.now(timezone.utc).isoformat(), total)
            )
            conn.commit()
            return cand
        print(f"    - '{cand}': 0 αποτελέσματα")

    # Τελευταίο fallback: ρώτα το ίδιο το μητρώο της Διαύγειας με το όνομα,
    # βρες το αριθμητικό uid και επαλήθευσέ το.
    uid = resolve_uid_via_directory(org["label"])
    if uid:
        data = search_page(uid, page=0, size=1)
        total = int((data or {}).get("info", {}).get("total", 0) or 0) if data else 0
        if total > 0:
            print(f"    ✓ (μητρώο) uid={uid}: {total} πράξεις -> ΕΠΙΛΕΧΘΗΚΕ")
            cur.execute(
                "INSERT OR REPLACE INTO org_cache "
                "(org_key, organization_uid, resolved_at, total_seen) "
                "VALUES (?, ?, ?, ?)",
                (org["key"], uid, datetime.now(timezone.utc).isoformat(), total)
            )
            conn.commit()
            return uid

    print(f"  ✗ ΑΠΟΤΥΧΙΑ: δεν βρέθηκε λειτουργικό uid για {org['label']}.")
    print(f"    Πρόσθεσε σωστό uid/latinName στη λίστα candidates.")
    return None


def resolve_uid_via_directory(label):
    """
    Ψάχνει το μητρώο φορέων της Διαύγειας με όρο το όνομα του φορέα και
    επιστρέφει το πρώτο πιθανό αριθμητικό uid. Δοκιμάζει διάφορα endpoints
    γιατί η δομή του API μπορεί να διαφέρει ανά έκδοση.
    """
    # καθάρισε το label από παρενθέσεις για καλύτερη αναζήτηση
    term = label.split("(")[0].strip()
    endpoints = [
        (ORG_URL, {"query": term, "wt": "json"}),
        (ORG_URL, {"q": term, "wt": "json"}),
        (ORG_URL + ".json", {"query": term}),
    ]
    for url, params in endpoints:
        time.sleep(SLEEP_BETWEEN)
        data = http_get_json(url, params)
        if not data:
            continue
        # δοκίμασε διάφορα πιθανά σχήματα απόκρισης
        items = (data.get("organizations") or data.get("items")
                 or data.get("results") or [])
        if isinstance(items, dict):
            items = items.get("organization") or []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            uid = it.get("uid") or it.get("organizationUid") or it.get("id")
            name = (it.get("label") or it.get("title") or it.get("name") or "")
            if uid and term[:8].lower() in name.lower():
                return str(uid)
    return None


# --------------------------------------------------------------------------
# PARSING ΗΜΕΡΟΜΗΝΙΑΣ
# --------------------------------------------------------------------------

# Πιθανές string μορφές που μπορεί να επιστρέψει το API/exports της Διαύγειας.
_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",   # ISO με ms + zone
    "%Y-%m-%dT%H:%M:%S%z",      # ISO + zone
    "%Y-%m-%dT%H:%M:%S.%f",     # ISO με ms
    "%Y-%m-%dT%H:%M:%S",        # ISO
    "%Y-%m-%d %H:%M:%S",        # ISO με κενό
    "%Y-%m-%d",                 # μόνο ημερομηνία
    "%d/%m/%Y %H:%M:%S",        # ελληνική με ώρα
    "%d/%m/%Y",                 # ελληνική ΗΗ/ΜΜ/ΕΕΕΕ
    "%d-%m-%Y",                 # ελληνική με παύλες
)


def parse_date(value):
    """
    Δέχεται ημερομηνία της Διαύγειας σε ΟΠΟΙΑΔΗΠΟΤΕ από τις μορφές:
      * epoch σε milliseconds  (π.χ. 1696291200000  ή "1696291200000")
      * epoch σε δευτερόλεπτα  (π.χ. 1696291200)
      * ISO 8601 με/χωρίς ms & timezone
      * ελληνική ΗΗ/ΜΜ/ΕΕΕΕ (με ή χωρίς ώρα)

    Επιστρέφει tuple (iso_date, year):
      iso_date -> "YYYY-MM-DD" (κανονικοποιημένο, ή "" αν απέτυχε)
      year     -> int (ή None)
    Όλα κανονικοποιούνται σε ευρωπαϊκή/ελληνική ζώνη ώρας (Europe/Athens),
    ώστε το φίλτρο έτους να μη "γλιστράει" λόγω UTC σε πράξεις κοντά στα
    μεσάνυχτα της Πρωτοχρονιάς.
    """
    if value is None or value == "":
        return "", None

    dt = None

    # 1) Αριθμητικό epoch (int/float ή καθαρά ψηφία σε string)
    s = str(value).strip()
    is_numeric = isinstance(value, (int, float)) or s.lstrip("-").isdigit()
    if is_numeric:
        try:
            num = float(value)
        except (TypeError, ValueError):
            num = None
        if num is not None:
            # Διάκριση ms vs sec: τιμές >~ 10^11 είναι σχεδόν σίγουρα ms.
            # (10^11 sec = έτος 5138· 10^11 ms = έτος 1973 -> ασφαλές κατώφλι)
            if abs(num) >= 1e11:
                num = num / 1000.0
            try:
                dt = datetime.fromtimestamp(num, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                dt = None

    # 2) String μορφές
    if dt is None and not is_numeric:
        # κανονικοποίηση 'Z' -> +00:00 ώστε να το πιάσει το %z
        s_norm = s.replace("Z", "+0000").replace("z", "+0000")
        # αφαίρεση ':' μέσα στο timezone offset (+03:00 -> +0300) για το %z
        if len(s_norm) >= 6 and (s_norm[-3] == ":") and (s_norm[-6] in "+-"):
            s_norm = s_norm[:-3] + s_norm[-2:]
        for fmt in _DATE_FORMATS:
            try:
                dt = datetime.strptime(s_norm, fmt)
                break
            except ValueError:
                continue
        # τελευταία προσπάθεια: fromisoformat (Python 3.11+ πιάνει πολλά)
        if dt is None:
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError:
                dt = None

    if dt is None:
        # Αν τίποτα δεν δούλεψε αλλά μοιάζει με ISO, κράτα τα 4 πρώτα ψηφία ως έτος.
        if len(s) >= 4 and s[:4].isdigit():
            return s[:10], int(s[:4])
        return "", None

    # Κανονικοποίηση σε Europe/Athens (UTC+2/+3) για σωστό έτος.
    try:
        from zoneinfo import ZoneInfo
        tz_athens = ZoneInfo("Europe/Athens")
        if dt.tzinfo is None:
            # naive ημερομηνίες θεωρούνται ήδη τοπική ώρα Ελλάδας
            dt = dt.replace(tzinfo=tz_athens)
        dt = dt.astimezone(tz_athens)
    except Exception:
        # fallback χωρίς zoneinfo: σταθερό +02:00
        if dt.tzinfo is None:
            from datetime import timedelta
            dt = dt.replace(tzinfo=timezone(timedelta(hours=2)))

    return dt.strftime("%Y-%m-%d"), dt.year


# --------------------------------------------------------------------------
# PARSING ΠΡΑΞΗΣ
# --------------------------------------------------------------------------

def classify_doc_type(decision_type_label, decision_type, subject):
    """
    Ταξινομεί την πράξη σε κατηγορία ροής δαπάνης (ίδια λογική με τη Γαύδο):
      'xep'      -> Χρηματικό Ένταλμα Πληρωμής / οριστικοποίηση πληρωμής
                    (η ΠΡΑΓΜΑΤΙΚΗ πληρωμή -> μετράει στο σύνολο)
      'analipsi' -> Ανάληψη υποχρέωσης / δέσμευση (ΔΕΝ μετράει)
      'symvasi'  -> Σύμβαση / απόφαση ανάθεσης (ΔΕΝ μετράει)
      'other'    -> οτιδήποτε άλλο (ΔΕΝ μετράει)
    Επιστρέφει (category, count_in_total) όπου count_in_total=1 μόνο για 'xep'.
    """
    s = " ".join(filter(None, [decision_type_label, decision_type, subject])).lower()

    # ΧΕΠ / οριστικοποίηση πληρωμής
    if ("ένταλμα" in s or "ενταλμα" in s or "οριστικοποίηση πληρωμ" in s
            or "οριστικοποιηση πληρωμ" in s or "χεπ" in s):
        return "xep", 1
    # Ανάληψη / δέσμευση
    if ("ανάληψη" in s or "αναληψη" in s or "δέσμευσ" in s or "δεσμευσ" in s):
        return "analipsi", 0
    # Σύμβαση / ανάθεση
    if ("σύμβαση" in s or "συμβαση" in s or "ανάθεσ" in s or "αναθεσ" in s
            or "κατακύρωσ" in s or "κατακυρωσ" in s):
        return "symvasi", 0
    return "other", 0


def extract_amount(decision):
    """
    Επιστρέφει (amount, currency) ΜΟΝΟ αν το API δίνει δομημένο ποσό.
    Αλλιώς (None, None). Δεν μαντεύουμε ποτέ ποσά από το θέμα/PDF.
    """
    ed = decision.get("extraFieldValues") or {}
    # Συνηθισμένα δομημένα πεδία ποσού στη Διαύγεια
    for key in ("amountWithVAT", "amountWithoutVAT", "totalAmount", "amount"):
        val = ed.get(key)
        if isinstance(val, dict):
            amt = val.get("amount", val.get("value"))
            cur = val.get("currency", "EUR")
            if amt not in (None, "", 0):
                try:
                    return float(amt), cur
                except (TypeError, ValueError):
                    pass
        elif isinstance(val, (int, float)) and val:
            return float(val), "EUR"
    # Sponsor/award amounts (π.χ. αναθέσεις)
    sponsor = ed.get("sponsor") or ed.get("contract")
    if isinstance(sponsor, dict):
        amt = sponsor.get("awardAmount") or sponsor.get("amount")
        if isinstance(amt, dict):
            try:
                return float(amt.get("amount")), amt.get("currency", "EUR")
            except (TypeError, ValueError):
                pass
    return None, None


def parse_decision(decision, org):
    ada = decision.get("ada")
    if not ada:
        return None

    # Η Διαύγεια δίνει το issueDate συχνά ως epoch ms· κάποια endpoints ως ISO ή
    # ελληνική μορφή. Χρησιμοποιούμε στιβαρό parser για σωστό έτος/φίλτρα.
    raw_issue = decision.get("issueDate")
    if raw_issue in (None, ""):
        raw_issue = decision.get("publishTimestamp")
    issue_iso, year = parse_date(raw_issue)

    raw_submission = (decision.get("submissionTimestamp")
                      or decision.get("publishTimestamp"))
    submission_iso, _ = parse_date(raw_submission)

    dtype = decision.get("decisionTypeUid") or decision.get("decisionType")
    dtype_label = decision.get("decisionTypeLabel") or ""

    # υπογράφων / μονάδα
    ed = decision.get("extraFieldValues") or {}
    signers = decision.get("signers") or ed.get("signerUid") or []
    if isinstance(signers, list):
        signer = ", ".join(
            (s.get("label") if isinstance(s, dict) else str(s)) for s in signers
        )
    else:
        signer = str(signers)

    units = decision.get("units") or ed.get("orgUnits") or []
    if isinstance(units, list):
        unit = ", ".join(
            (u.get("label") if isinstance(u, dict) else str(u)) for u in units
        )
    else:
        unit = str(units)

    amount, currency = extract_amount(decision)

    # Κατηγοριοποίηση ροής δαπάνης -> count_in_total μόνο για ΧΕΠ.
    doc_category, count_in_total = classify_doc_type(dtype_label, dtype, decision.get("subject"))

    doc_url = decision.get("documentUrl") or f"https://diavgeia.gov.gr/doc/{ada}"

    return {
        "ada": ada,
        "org_key": org["key"],
        "org_label": org["label"],
        "organization_uid": decision.get("organizationId")
                            or decision.get("organizationUid"),
        "subject": decision.get("subject") or "",
        "decision_type": str(dtype) if dtype else "",
        "decision_type_label": dtype_label,
        "issue_date": issue_iso or "",
        "submission_ts": submission_iso or "",
        "protocol": decision.get("protocolNumber") or "",
        "signer": signer,
        "unit": unit,
        "amount": amount,
        "amount_currency": currency,
        "year": year,
        "doc_url": doc_url,
        "doc_category": doc_category,
        "count_in_total": count_in_total,
        "raw_json": json.dumps(decision, ensure_ascii=False),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def upsert_decisions(conn, rows):
    if not rows:
        return 0
    cur = conn.cursor()
    cols = ["ada", "org_key", "org_label", "organization_uid", "subject",
            "decision_type", "decision_type_label", "issue_date",
            "submission_ts", "protocol", "signer", "unit", "amount",
            "amount_currency", "year", "doc_url", "doc_category",
            "count_in_total", "raw_json", "fetched_at"]
    placeholders = ",".join("?" * len(cols))
    sql = (f"INSERT OR IGNORE INTO decisions ({','.join(cols)}) "
           f"VALUES ({placeholders})")
    before = conn.total_changes
    cur.executemany(sql, [[r[c] for c in cols] for r in rows])
    conn.commit()
    return conn.total_changes - before


# --------------------------------------------------------------------------
# ΣΥΓΧΡΟΝΙΣΜΟΣ ΕΝΟΣ ΦΟΡΕΑ
# --------------------------------------------------------------------------

def sync_org(conn, org, full=False, since=None):
    print(f"\n=== {org['label']} ===")
    uid = find_working_query(org, conn)
    if not uid:
        return {"org": org["label"], "new": 0, "ok": False}

    cur = conn.cursor()
    from_date = None
    if not full:
        if since:
            from_date = since
        else:
            row = cur.execute(
                "SELECT last_issue FROM sync_meta WHERE org_key = ?",
                (org["key"],)
            ).fetchone()
            if row and row[0]:
                from_date = row[0][:10]   # YYYY-MM-DD
                print(f"  Incremental: από {from_date}")
    if full:
        print("  ΠΛΗΡΕΣ κατέβασμα (όλο το ιστορικό)...")

    page = 0
    total_new = 0
    total_seen = 0
    max_issue = None
    api_total = None

    while True:
        time.sleep(SLEEP_BETWEEN)
        data = search_page(uid, page=page, size=PAGE_SIZE, from_date=from_date)
        if data is None:
            print(f"  Σφάλμα στη σελίδα {page}, διακοπή φορέα.")
            break

        if api_total is None:
            api_total = int(data.get("info", {}).get("total", 0) or 0)
            print(f"  Σύνολο διαθέσιμων (με φίλτρο): {api_total}")

        decisions = data.get("decisions") or data.get("items") or []
        if not decisions:
            break

        rows = []
        for d in decisions:
            parsed = parse_decision(d, org)
            if not parsed:
                continue
            # Προαιρετικό φίλτρο: αν ο φορέας ορίζει unit_filter (λίστα λέξεων),
            # κράτα μόνο πράξεις που αναφέρουν Ρέθυμνο στη μονάδα/θέμα. Χρήσιμο
            # αν ο κωδικός Περιφέρειας επιστρέφει ΟΛΗ την Κρήτη αντί μόνο Π.Ε. Ρεθύμνου.
            uf = org.get("unit_filter")
            if uf:
                hay = ((parsed.get("unit") or "") + " " +
                       (parsed.get("subject") or "")).lower()
                if not any(k.lower() in hay for k in uf):
                    continue
            rows.append(parsed)
            if parsed["issue_date"]:
                if max_issue is None or parsed["issue_date"] > max_issue:
                    max_issue = parsed["issue_date"]

        added = upsert_decisions(conn, rows)
        total_new += added
        total_seen += len(rows)
        print(f"  σελίδα {page}: {len(rows)} πράξεις, +{added} νέες "
              f"(σύνολο νέων: {total_new})")

        # τέλος σελιδοποίησης
        if len(decisions) < PAGE_SIZE:
            break
        if api_total and total_seen >= api_total:
            break
        page += 1

    # ενημέρωση μεταδεδομένων συγχρονισμού
    if max_issue:
        cur.execute(
            "INSERT OR REPLACE INTO sync_meta (org_key, last_issue, last_sync_at) "
            "VALUES (?, ?, ?)",
            (org["key"], max_issue, datetime.now(timezone.utc).isoformat())
        )
    else:
        cur.execute(
            "INSERT OR IGNORE INTO sync_meta (org_key, last_issue, last_sync_at) "
            "VALUES (?, ?, ?)",
            (org["key"], None, datetime.now(timezone.utc).isoformat())
        )
    conn.commit()

    print(f"  ΟΛΟΚΛΗΡΩΘΗΚΕ: {total_new} νέες πράξεις για {org['label']}")
    return {"org": org["label"], "new": total_new, "ok": True}


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Συγχρονισμός Διαύγειας Π.Ε. Ρεθύμνης")
    ap.add_argument("--full", action="store_true",
                    help="Πλήρες κατέβασμα όλου του ιστορικού")
    ap.add_argument("--org", help="Συγχρονισμός μόνο ενός φορέα (key)")
    ap.add_argument("--since", help="Incremental από ημερομηνία YYYY-MM-DD")
    ap.add_argument("--db", default=DB_PATH, help="Διαδρομή βάσης SQLite")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    init_db(conn)

    orgs = ORGANIZATIONS
    if args.org:
        orgs = [o for o in ORGANIZATIONS if o["key"] == args.org]
        if not orgs:
            print(f"Άγνωστος φορέας: {args.org}")
            print("Διαθέσιμοι:", ", ".join(o["key"] for o in ORGANIZATIONS))
            sys.exit(1)

    print("=" * 60)
    print("ΣΥΓΧΡΟΝΙΣΜΟΣ ΔΙΑΥΓΕΙΑΣ — Π.Ε. ΡΕΘΥΜΝΗΣ")
    print(f"Λειτουργία: {'ΠΛΗΡΗΣ' if args.full else 'INCREMENTAL'}")
    print(f"Φορείς: {len(orgs)}")
    print("=" * 60)

    results = []
    for org in orgs:
        try:
            results.append(sync_org(conn, org, full=args.full, since=args.since))
        except Exception as e:
            print(f"  ΣΦΑΛΜΑ στον φορέα {org['label']}: {e}")
            results.append({"org": org["label"], "new": 0, "ok": False})

    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    conn.close()

    print("\n" + "=" * 60)
    print("ΣΥΝΟΨΗ")
    print("=" * 60)
    for r in results:
        status = "OK" if r["ok"] else "ΑΠΟΤΥΧΙΑ"
        print(f"  [{status}] {r['org']}: +{r['new']} νέες")
    print(f"\nΣΥΝΟΛΟ πράξεων στη βάση: {total}")


if __name__ == "__main__":
    main()
