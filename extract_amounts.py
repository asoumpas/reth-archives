#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_amounts.py
==================
Εξαγωγή χρηματικών ποσών από τα PDF των πράξεων (Διαύγεια), για τη βάση
diavgeia.db. ΙΔΙΑ προσέγγιση με το σύστημα Γαύδου:

  * Διαβάζει το PDF κάθε πράξης (μέσω doc_url ή του ΑΔΑ).
  * Βγάζει ποσό με κανόνες για ΧΕΠ / Αναλήψεις.
  * Καταγράφει δείκτη βεβαιότητας: high / medium / low.
  * Fallback "με όριο": μαντεύει αλλά αγνοεί μη ρεαλιστικά νούμερα/κωδικούς.
  * Resumable & σε παρτίδες (δεν κολλάει το GitHub Actions / διακόπτεται άφοβα).
  * ΔΕΝ πειράζει την κατηγορία/count_in_total — αυτά τα έχει βάλει ήδη ο
    diavgeia_sync.py, ώστε το "σύνολο" να μετράει ΜΟΝΟ ΧΕΠ.

ΣΗΜΑΝΤΙΚΟ — ΜΗ ΔΙΠΛΟΜΕΤΡΗΣΗ:
  Η ίδια δαπάνη εμφανίζεται ως Ανάληψη → Σύμβαση → Χρηματικό Ένταλμα Πληρωμής.
  Το σύνολο ευρώ στη σελίδα βασίζεται ΜΟΝΟ στα ΧΕΠ (count_in_total=1).
  Εδώ απλώς εξάγουμε ποσό για ΟΛΕΣ τις πράξεις (για εμφάνιση ανά γραμμή),
  αλλά το άθροισμα γίνεται μόνο στα ΧΕΠ από το build_json.py.

ΕΞΑΡΤΗΣΕΙΣ (τοπικά):  pip install pdfplumber requests
ΓΙΑ ΣΚΑΝΑΡΙΣΜΕΝΑ PDF (προαιρετικό OCR):  tesseract-ocr + tesseract-ocr-ell,
  poppler-utils και: pip install pdf2image pytesseract  (αφήνεται εκτός by default)

Χρήση:
  python extract_amounts.py --batch-size 150      # μία παρτίδα
  python extract_amounts.py --all                 # όλες (τοπικά, με την ησυχία σου)
  python extract_amounts.py --org dimos_rethymnis # μόνο έναν φορέα
  python extract_amounts.py --reset               # ξέχνα ό,τι έγινε, ξεκίνα ξανά
"""

import argparse
import re
import sqlite3
import time
from io import BytesIO

import requests

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

DB_PATH = "diavgeia.db"
MAX_REASONABLE = 5_000_000.0   # "φρένο": αγνόησε ποσά πάνω από αυτό (κωδικοί/ΑΦΜ/πρωτόκολλα)

# ----------------------------------------------------------------------
# Parsing ελληνικών ποσών
# ----------------------------------------------------------------------

AMOUNT = r"\d{1,3}(?:\.\d{3})*,\d{2}"   # 1.234,56 ή 500,00

# Λέξεις-παγίδες: αριθμοί κοντά τους ΔΕΝ είναι ποσά δαπάνης.
TRAP = re.compile(r"(ΑΦΜ|Α\.Φ\.Μ|αριθμ|πρωτ|ΚΑΕ|IBAN|Τ\.?Κ|κωδικ|τηλ)", re.IGNORECASE)


def gr_to_float(s):
    """'1.234,56' -> 1234.56"""
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_amount(text, doc_category):
    """
    Επιστρέφει (amount, confidence, method).
    doc_category: 'xep' | 'analipsi' | 'symvasi' | 'other' (από τη βάση).
    """
    t = re.sub(r"[ \t]+", " ", text or "")

    # ---- Χρηματικό Ένταλμα Πληρωμής (ΧΕΠ) ----
    if doc_category == "xep":
        m = re.search(
            r"Συνολικ[όο][ν]?\s+Ποσ[όο].{0,40}?\((" + AMOUNT + r")\)\s*ευρ",
            t, re.IGNORECASE | re.DOTALL)
        if m:
            v = gr_to_float(m.group(1))
            m2 = re.search(r"ΣΥΝΟΛΟ\s*:?\s*(" + AMOUNT + r")", t)
            if m2 and gr_to_float(m2.group(1)) == v:
                return v, "high", "xep_synolo+synoliko"
            return v, "medium", "xep_synoliko"
        m = re.search(r"ΣΥΝΟΛΟ\s*:?\s*(" + AMOUNT + r")", t)
        if m:
            return gr_to_float(m.group(1)), "medium", "xep_synolo"

    # ---- Ανάληψη Υποχρέωσης ----
    if doc_category == "analipsi":
        m = re.search(
            r"[ύυ]ψους.{0,40}?\((" + AMOUNT + r")\s*€\)",
            t, re.IGNORECASE | re.DOTALL)
        if m:
            return gr_to_float(m.group(1)), "high", "analipsi_ypsous"
        m = re.search(r"[ύυ]ψους\s+(" + AMOUNT + r")\s*€", t, re.IGNORECASE)
        if m:
            return gr_to_float(m.group(1)), "medium", "analipsi_noparen"

    # ---- Σύμβαση / Ανάθεση ----
    if doc_category == "symvasi":
        m = re.search(
            r"(?:συμβατικ[όο]|συνολικ[όο]).{0,30}?(" + AMOUNT + r")\s*(?:€|ευρ)",
            t, re.IGNORECASE | re.DOTALL)
        if m:
            return gr_to_float(m.group(1)), "medium", "symvasi_value"

    # ---- Fallback ΜΕ ΟΡΙΟ (μαντεύει αλλά με φρένα) ----
    # Προτίμησε ποσά κοντά σε σύμβολο/λέξη νομίσματος· αγνόησε παγίδες & τέρατα.
    currency_near = []
    plain = []
    for m in re.finditer(AMOUNT, t):
        ctx_before = t[max(0, m.start() - 45):m.start()]
        ctx_after = t[m.end():m.end() + 6]
        if TRAP.search(ctx_before):
            continue
        if re.search(r"Υπ[όο]λοιπο\s+προς\s+αν[άα]ληψη", ctx_before, re.IGNORECASE):
            continue
        v = gr_to_float(m.group(0))
        if v is None or v < 1 or v > MAX_REASONABLE:
            continue
        if "€" in ctx_after or re.search(r"ευρ", ctx_after, re.IGNORECASE):
            currency_near.append(v)
        else:
            plain.append(v)
    if currency_near:
        return max(currency_near), "low", "fallback_currency"
    if plain:
        return max(plain), "low", "fallback_max"

    return None, None, "none"


# ----------------------------------------------------------------------
# Εξαγωγή κειμένου από PDF
# ----------------------------------------------------------------------

def text_from_pdf(pdf_bytes):
    if pdfplumber is None:
        raise RuntimeError("Λείπει το pdfplumber. Τρέξε: pip install pdfplumber")
    text = ""
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text += (page.extract_text() or "") + "\n"
    return text


def pdf_url_for(row):
    url = row["doc_url"]
    if url:
        return url
    if row["ada"]:
        return f"https://diavgeia.gov.gr/doc/{row['ada']}?inline=true"
    return None


# ----------------------------------------------------------------------
# Επεξεργασία σε παρτίδες
# ----------------------------------------------------------------------

def process(db_path, batch_size, sleep, org=None, do_all=False):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    where = "amount_processed_at IS NULL"
    params = []
    if org:
        where += " AND org_key = ?"
        params.append(org)

    if do_all:
        rows = conn.execute(
            f"SELECT ada, doc_url, doc_category FROM decisions WHERE {where} "
            f"ORDER BY issue_date DESC", params).fetchall()
    else:
        rows = conn.execute(
            f"SELECT ada, doc_url, doc_category FROM decisions WHERE {where} "
            f"ORDER BY issue_date DESC LIMIT ?", params + [batch_size]).fetchall()

    if not rows:
        print("Δεν υπάρχουν άλλες πράξεις προς επεξεργασία. Τελειώσαμε!")
        conn.close()
        return 0

    print(f"Επεξεργασία {len(rows)} πράξεων...")
    headers = {"User-Agent": "diavgeia-rethymno-amounts/1.0 (open data archive)"}

    for i, r in enumerate(rows, 1):
        amount = conf = method = None
        try:
            url = pdf_url_for(r)
            resp = requests.get(url, headers=headers, timeout=45)
            resp.raise_for_status()
            text = text_from_pdf(resp.content)
            amount, conf, method = extract_amount(text, r["doc_category"])
        except Exception as e:
            method = f"error:{type(e).__name__}"

        conn.execute("""
            UPDATE decisions
            SET amount = COALESCE(?, amount),
                amount_confidence = ?,
                amount_method = ?,
                amount_processed_at = datetime('now')
            WHERE ada = ?
        """, (amount, conf, method, r["ada"]))
        conn.commit()
        print(f"  [{i}/{len(rows)}] {r['ada']}: {amount} ({conf}, {method})")
        time.sleep(sleep)

    remaining = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE amount_processed_at IS NULL"
    ).fetchone()[0]
    conn.close()
    if remaining:
        print(f"Έμειναν ακόμη {remaining} πράξεις. Ξανατρέξε για την επόμενη παρτίδα.")
    else:
        print("Όλες οι πράξεις επεξεργάστηκαν!")
    return remaining


def reset(db_path):
    conn = sqlite3.connect(db_path)
    n = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    conn.execute("UPDATE decisions SET amount_processed_at = NULL, "
                 "amount_confidence = NULL, amount_method = NULL")
    conn.commit()
    conn.close()
    print(f"Έγινε reset. Και οι {n} πράξεις θα επεξεργαστούν ξανά από την αρχή.")


def main():
    ap = argparse.ArgumentParser(description="Εξαγωγή ποσών από PDF Διαύγειας")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--batch-size", type=int, default=150)
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--org", help="Μόνο ένας φορέας (key)")
    ap.add_argument("--all", action="store_true", help="Όλες οι πράξεις (τοπικά)")
    ap.add_argument("--reset", action="store_true", help="Μηδενισμός & επανεκκίνηση")
    args = ap.parse_args()

    if args.reset:
        reset(args.db)
        return
    process(args.db, args.batch_size, args.sleep, org=args.org, do_all=args.all)


if __name__ == "__main__":
    main()
