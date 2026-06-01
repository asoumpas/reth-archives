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
from datetime import datetime, timezone, timedelta

import requests

# --------------------------------------------------------------------------
# ΡΥΘΜΙΣΕΙΣ
# --------------------------------------------------------------------------

# ΣΩΣΤΟ endpoint παραγωγής (επιβεβαιωμένο από την επίσημη βιβλιοθήκη diavgeia-api):
#   https://diavgeia.gov.gr/opendata/search?org=<κωδικός>&...
# Το φίλτρο φορέα είναι ΑΠΛΗ παράμετρος 'org' (δέχεται αριθμητικό uid Ή latin name),
# ΟΧΙ q=organizationUid:... (το οποίο αγνοείται σιωπηλά -> επιστρέφει όλη τη χώρα).
OPENDATA_ROOT = "https://diavgeia.gov.gr/opendata"
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
    # ======================================================================
    # ΔΗΜΟΣ ΡΕΘΥΜΝΗΣ — κύριος φορέας + υπο-φορείς (ΝΠΔΔ/επιχειρήσεις)
    # ======================================================================
    {"key": "dimos_rethymnis", "label": "Δήμος Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["6263", "dhmos_rethymnou"]},
    {"key": "reth_deya", "label": "ΔΕΥΑ Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["50453"]},
    {"key": "reth_koinofelis", "label": "Κοινωφελής Επιχείρηση Δήμου Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["53290"]},
    {"key": "reth_koinofelis_palaia", "label": "Κοινωφελής Επιχείρηση Δήμου Ρεθύμνου (παλαιά)",
     "group": "Δήμος Ρεθύμνης", "candidates": ["52401"]},
    {"key": "reth_kapi", "label": "ΚΑΠΗ Δήμου Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["50263"]},
    {"key": "reth_paidikoi", "label": "Παιδικοί & Βρεφονηπιακοί Σταθμοί Δήμου Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["50264"]},
    {"key": "reth_athlitikos", "label": "Δημοτικός Αθλητικός Οργανισμός Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["50265"]},
    {"key": "reth_filarmoniki", "label": "Δημοτική Φιλαρμονική - Ωδείο Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["50266"]},
    {"key": "reth_limeniko", "label": "Δημοτικό Λιμενικό Ταμείο Ρεθύμνου",
     "group": "Δήμος Ρεθύμνης", "candidates": ["50273"]},
    {"key": "reth_koin_mousiki", "label": "Κοινωνική Πολιτική & Μουσική Παιδεία Δήμου Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["52941"]},
    {"key": "reth_sxolikes", "label": "Σχολικές Επιτροπές Δήμου Ρεθύμνης",
     "group": "Δήμος Ρεθύμνης", "candidates": ["53543"]},
    {"key": "reth_koinofelis_arkadiou", "label": "Δημοτική Κοινωφελής Επιχείρηση Αρκαδίου",
     "group": "Δήμος Ρεθύμνης", "candidates": ["52550"]},
    {"key": "reth_nosokomeio", "label": "Δημοτικό Νοσοκομείο Ρεθύμνου",
     "group": "Δήμος Ρεθύμνης", "candidates": ["100068875"]},

    # ======================================================================
    # ΔΗΜΟΣ ΑΓΙΟΥ ΒΑΣΙΛΕΙΟΥ
    # ======================================================================
    {"key": "dimos_agiou_vasileiou", "label": "Δήμος Αγίου Βασιλείου",
     "group": "Δήμος Αγίου Βασιλείου", "candidates": ["6006", "agiosbasileios"]},
    {"key": "av_koinofelis", "label": "Δημοτική Κοινωφελής Επιχείρηση Αγίου Βασιλείου (ΔΗ.Κ.Ε.Α.Β.)",
     "group": "Δήμος Αγίου Βασιλείου", "candidates": ["54055"]},
    {"key": "av_paidikos_spiliou", "label": "Δημοτικός Παιδικός Σταθμός Σπηλίου",
     "group": "Δήμος Αγίου Βασιλείου", "candidates": ["52716"]},
    {"key": "av_sxoliki_a", "label": "Σχολική Επιτροπή Α/θμιας Εκπ/σης Δήμου Αγίου Βασιλείου",
     "group": "Δήμος Αγίου Βασιλείου", "candidates": ["100022798"]},
    {"key": "av_sxoliki_b", "label": "Σχολική Επιτροπή Β/θμιας Εκπ/σης Δήμου Αγίου Βασιλείου",
     "group": "Δήμος Αγίου Βασιλείου", "candidates": ["100022811"]},

    # ======================================================================
    # ΔΗΜΟΣ ΑΜΑΡΙΟΥ
    # ======================================================================
    {"key": "dimos_amariou", "label": "Δήμος Αμαρίου",
     "group": "Δήμος Αμαρίου", "candidates": ["6025", "dimos_amariou"]},
    {"key": "amari_koinofelis", "label": "Δημοτική Κοινωφελής Επιχείρηση Δήμου Αμαρίου",
     "group": "Δήμος Αμαρίου", "candidates": ["53499"]},

    # ======================================================================
    # ΔΗΜΟΣ ΑΝΩΓΕΙΩΝ
    # ======================================================================
    {"key": "dimos_anogeion", "label": "Δήμος Ανωγείων",
     "group": "Δήμος Ανωγείων", "candidates": ["6039", "dimos_anogeia"]},
    {"key": "anog_koinofelis", "label": "Δημοτική Κοινωφελής Επιχείρηση Δήμου Ανωγείων",
     "group": "Δήμος Ανωγείων", "candidates": ["50279"]},
    {"key": "anog_paidikos", "label": "Δημοτικός Παιδικός Σταθμός Ανωγείων",
     "group": "Δήμος Ανωγείων", "candidates": ["50294"]},
    {"key": "anog_stadio", "label": "Δημοτικό Στάδιο Ανωγείων",
     "group": "Δήμος Ανωγείων", "candidates": ["50297"]},
    {"key": "anog_sxolikes", "label": "Σχολικές Επιτροπές Δήμου Ανωγείων",
     "group": "Δήμος Ανωγείων", "candidates": ["50925"]},
    {"key": "anog_kapi", "label": "ΚΑΠΗ Δήμου Ανωγείων",
     "group": "Δήμος Ανωγείων", "candidates": ["99220311"]},

    # ======================================================================
    # ΔΗΜΟΣ ΜΥΛΟΠΟΤΑΜΟΥ
    # ======================================================================
    {"key": "dimos_mylopotamou", "label": "Δήμος Μυλοποτάμου",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["6201", "dimosmylopotamou"]},
    {"key": "myl_deyam", "label": "ΔΕΥΑ Μυλοποτάμου (Δ.Ε.Υ.Α.Μ.)",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51050"]},
    {"key": "myl_deyag", "label": "ΔΕΥΑ Δήμου Γεροποτάμου (Δ.Ε.Υ.Α.Γ.)",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["52004"]},
    {"key": "myl_avlopotamos", "label": "Οργανισμός Πολιτισμού-Τουρισμού Δήμου Μυλοποτάμου «Ο Αυλοπόταμος»",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51048"]},
    {"key": "myl_dhkemy", "label": "Δημοτική Κοινωφελής Επιχείρηση Μυλοποτάμου (ΔΗ.Κ.Ε.ΜΥ.)",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["52632"]},
    {"key": "myl_koinofelis_kouloukona", "label": "Δημοτική Κοινωφελής Επιχείρηση Δήμου Κουλούκωνα",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51224"]},
    {"key": "myl_koinofelis_geropotamou", "label": "Κοινωφελής Επιχείρηση Δήμου Γεροποτάμου",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51226"]},
    {"key": "myl_kapi_peramatos", "label": "ΚΑΠΗ Περάματος",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51220"]},
    {"key": "myl_vrefon_geropotamou", "label": "Βρεφονηπιακός Σταθμός Δήμου Γεροποτάμου",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51221"]},
    {"key": "myl_paidikos_zonianon", "label": "Κοινοτικός Παιδικός Σταθμός Ζωνιανών",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51222"]},
    {"key": "myl_paidikos_livadion", "label": "Δημοτικός Παιδικός Σταθμός Λιβαδίων",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["51223"]},
    {"key": "myl_sxolikes", "label": "Σχολικές Επιτροπές Δήμου Μυλοποτάμου",
     "group": "Δήμος Μυλοποτάμου", "candidates": ["53481"]},

    # ======================================================================
    # ΠΕΡΙΦΕΡΕΙΑ & ΑΠΟΚΕΝΤΡΩΜΕΝΗ (αναρτούν για ΟΛΗ την Κρήτη -> φίλτρο Ρεθύμνου)
    # ======================================================================
    {
        "key": "pe_rethymnis",
        "label": "Π.Ε. Ρεθύμνης (Περιφέρεια Κρήτης)",
        "group": "Περιφέρεια / Αποκεντρωμένη",
        "candidates": ["5010", "periferia_kritis"],
        "unit_ids": [
            "81119", "81120", "81121", "81122", "81123", "81124",
            "81819", "84062", "85314", "85315", "85660", "85877",
            "100033665", "100053649", "100068383",
        ],
        "signer_ids": ["111549"],   # Μαρία Λιονή, Αντιπεριφερειάρχης Ρεθύμνου
        "unit_filter": ["ρεθύμν", "ρεθυμν", "rethymn"],
    },
    {
        "key": "apok_dioikisi_kritis",
        "label": "Αποκεντρωμένη Διοίκηση Κρήτης (Ρεθύμνου)",
        "group": "Περιφέρεια / Αποκεντρωμένη",
        "candidates": ["50204", "apdik_krhths"],
        "unit_filter": ["ρεθύμν", "ρεθυμν", "rethymn"],
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
            org_group        TEXT,
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
        ("org_group",           "ALTER TABLE decisions ADD COLUMN org_group TEXT"),
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
    """Μία σελίδα αποτελεσμάτων search για συγκεκριμένο φορέα.

    Χρησιμοποιεί το σωστό endpoint /opendata/search με την παράμετρο 'org'
    (επιβεβαιωμένο από την επίσημη βιβλιοθήκη diavgeia-api). Το 'org' δέχεται
    είτε αριθμητικό κωδικό είτε latin name.
    """
    params = {
        "org": str(org_uid),
        "page": page,
        "size": size,
        "sort": sort,
        "status": "PUBLISHED",
    }
    # ΚΡΙΣΙΜΟ: αν ΔΕΝ δώσουμε ρητό εύρος ημερομηνιών, το API βάζει αυτόματα
    # φίλτρο "τελευταίο εξάμηνο" και χάνουμε όλο το ιστορικό! Δίνουμε λοιπόν
    # ρητό from_issue_date από το 2010 (έναρξη Διαύγειας) έως αύριο.
    if from_date:
        params["from_issue_date"] = from_date
    else:
        params["from_issue_date"] = "2010-10-01"
    # to_issue_date = αύριο (για να πιάνει και τις σημερινές)
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    params["to_issue_date"] = tomorrow
    data = http_get_json(SEARCH_URL, params)
    if data is not None:
        data["_requested_uid"] = str(org_uid)
    return data


def filter_was_applied(data, org_uid):
    """
    Ελεγχος ασφαλείας: επιβεβαιώνει ότι το φιλτράρισμα έγινε στον σωστό φορέα
    και δεν γύρισε όλη η Διαύγεια. Κοιτάζει αν οι πράξεις ανήκουν στον φορέα.
    Το org μπορεί να είναι latin name, οπότε ελέγχουμε και το πλήθος:
    ένα ρεαλιστικό σύνολο (όχι εκατομμύρια) θεωρείται έγκυρο φίλτρο.
    """
    if not data:
        return False
    total = int(data.get("info", {}).get("total", 0) or 0)
    # Δικλείδα: κανένας μεμονωμένος φορέας στη Ρεθύμνη δεν έχει > 1.000.000 πράξεις.
    # Αν δούμε τέτοιο νούμερο, το φίλτρο σχεδόν σίγουρα αγνοήθηκε.
    if total > 1_000_000:
        return False
    return True


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
        # ΕΛΕΓΧΟΣ ΑΣΦΑΛΕΙΑΣ: αν το API αγνόησε το φίλτρο (επιστρέφει όλη τη
        # Διαύγεια), ΑΠΟΡΡΙΨΕ τον κωδικό — αλλιώς θα κατεβάζαμε εκατομμύρια.
        if not filter_was_applied(data, cand):
            print(f"    ✗ '{cand}': το φίλτρο ΔΕΝ εφαρμόστηκε (αγνοήθηκε) — απόρριψη")
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

    # Raw IDs μονάδων/υπογραφόντων (για ακριβές φιλτράρισμα Π.Ε. Ρεθύμνου).
    raw_unit_ids = [str(x) for x in (decision.get("unitIds") or [])]
    raw_signer_ids = [str(x) for x in (decision.get("signerIds") or [])]

    amount, currency = extract_amount(decision)

    # Κατηγοριοποίηση ροής δαπάνης -> count_in_total μόνο για ΧΕΠ.
    doc_category, count_in_total = classify_doc_type(dtype_label, dtype, decision.get("subject"))

    doc_url = decision.get("documentUrl") or f"https://diavgeia.gov.gr/doc/{ada}"

    return {
        "ada": ada,
        "org_key": org["key"],
        "org_label": org["label"],
        "org_group": org.get("group", org["label"]),
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
        # Βοηθητικά (δεν αποθηκεύονται στη βάση· μόνο για το φίλτρο Ρεθύμνου)
        "_unit_ids": raw_unit_ids,
        "_signer_ids": raw_signer_ids,
    }


def upsert_decisions(conn, rows):
    if not rows:
        return 0
    cur = conn.cursor()
    cols = ["ada", "org_key", "org_label", "org_group", "organization_uid", "subject",
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
            # ΑΚΡΙΒΕΣ φίλτρο Π.Ε. Ρεθύμνου (για Περιφέρεια/Αποκεντρωμένη που
            # αναρτούν για όλη την Κρήτη). Κρατάμε την πράξη αν ισχύει ΕΝΑ από:
            #   (α) ανήκει σε μονάδα (unitId) της Π.Ε. Ρεθύμνης
            #   (β) υπογράφεται από signerId Ρεθύμνου (π.χ. Αντιπεριφερειάρχης)
            #   (γ) δίχτυ ασφαλείας: αναφέρει "Ρεθύμν" σε μονάδα/θέμα
            want_units = set(org.get("unit_ids") or [])
            want_signers = set(org.get("signer_ids") or [])
            uf = org.get("unit_filter")
            if want_units or want_signers or uf:
                keep = False
                if want_units and (want_units & set(parsed.get("_unit_ids", []))):
                    keep = True
                if want_signers and (want_signers & set(parsed.get("_signer_ids", []))):
                    keep = True
                if not keep and uf:
                    hay = ((parsed.get("unit") or "") + " " +
                           (parsed.get("subject") or "")).lower()
                    if any(k.lower() in hay for k in uf):
                        keep = True
                if not keep:
                    continue
            # καθάρισε τα βοηθητικά πεδία πριν την αποθήκευση
            parsed.pop("_unit_ids", None)
            parsed.pop("_signer_ids", None)
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
