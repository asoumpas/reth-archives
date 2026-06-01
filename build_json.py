#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_json.py
=============
Διαβάζει τη βάση diavgeia.db και παράγει το docs/data.json που τροφοδοτεί
τη στατική σελίδα αναζήτησης (docs/index.html).

Παράγει:
  docs/data.json  -> { meta, organizations, decision_types, years, decisions[] }

Σημείωση για ποσά: το πεδίο amount περιλαμβάνεται ΜΟΝΟ όπου το API το έδωσε
δομημένα. Δεν υπολογίζονται/εμφανίζονται αυτόματα σύνολα ευρώ.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

DB_PATH = "diavgeia.db"
OUT_PATH = os.path.join("docs", "data.json")


def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"Δεν βρέθηκε η βάση {DB_PATH}. Τρέξε πρώτα diavgeia_sync.py")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT ada, org_key, org_label, org_group, subject, decision_type,
               decision_type_label, issue_date, protocol, signer, unit,
               amount, amount_currency, year, doc_url,
               doc_category, count_in_total, amount_confidence
        FROM decisions
        ORDER BY issue_date DESC
    """).fetchall()

    decisions = []
    org_counts = {}
    group_counts = {}
    type_set = {}
    year_set = set()

    # Σύνολο ΧΕΠ ανά έτος & συνολικά (ΜΟΝΟ count_in_total=1 με ποσό).
    # Έτσι το "σύνολο ευρώ" αποφεύγει τη διπλομέτρηση ανάληψη/σύμβαση/ΧΕΠ.
    xep_total = 0.0
    xep_counted = 0
    xep_low = 0
    xep_total_by_year = {}

    for r in rows:
        d = dict(r)
        # συμπαγές record για μικρότερο data.json
        rec = {
            "ada": d["ada"],
            "org": d["org_key"],
            "orgLabel": d["org_label"],
            "group": d["org_group"] or d["org_label"],
            "subject": d["subject"],
            "type": d["decision_type"],
            "typeLabel": d["decision_type_label"],
            "date": (d["issue_date"] or "")[:10],
            "protocol": d["protocol"],
            "signer": d["signer"],
            "unit": d["unit"],
            "year": d["year"],
            "url": d["doc_url"],
            "cat": d["doc_category"] or "other",
            "cnt": d["count_in_total"] or 0,
        }
        if d["amount"] is not None:
            rec["amount"] = d["amount"]
            rec["currency"] = d["amount_currency"] or "EUR"
            if d["amount_confidence"]:
                rec["conf"] = d["amount_confidence"]
            # άθροισε στο σύνολο ΧΕΠ μόνο όσα μετρώνται
            if (d["count_in_total"] or 0) == 1:
                xep_total += d["amount"]
                xep_counted += 1
                if d["amount_confidence"] == "low":
                    xep_low += 1
                if d["year"]:
                    xep_total_by_year[d["year"]] = \
                        xep_total_by_year.get(d["year"], 0.0) + d["amount"]
        decisions.append(rec)

        org_counts[d["org_key"]] = org_counts.get(d["org_key"], 0)
        org_counts[d["org_key"]] += 1
        grp = d["org_group"] or d["org_label"]
        if grp:
            group_counts[grp] = group_counts.get(grp, 0) + 1
        if d["org_label"]:
            type_label = d["decision_type_label"] or d["decision_type"] or ""
            if type_label:
                type_set[type_label] = type_set.get(type_label, 0) + 1
        if d["year"]:
            year_set.add(d["year"])

    # λίστα φορέων με ετικέτες & πλήθη
    org_labels = {}
    org_groups = {}
    for r in cur.execute("SELECT DISTINCT org_key, org_label, org_group FROM decisions"):
        org_labels[r["org_key"]] = r["org_label"]
        org_groups[r["org_key"]] = r["org_group"] or r["org_label"]

    organizations = [
        {"key": k, "label": org_labels.get(k, k),
         "group": org_groups.get(k, ""), "count": org_counts.get(k, 0)}
        for k in sorted(org_counts, key=lambda x: org_labels.get(x, x))
    ]

    # ομάδες (δήμοι + Περιφέρεια) με συνολικά πλήθη
    groups = [
        {"label": g, "count": c}
        for g, c in sorted(group_counts.items(), key=lambda kv: -kv[1])
    ]

    decision_types = [
        {"label": t, "count": c}
        for t, c in sorted(type_set.items(), key=lambda kv: -kv[1])
    ]

    years = sorted(year_set, reverse=True)

    # Σύνοψη ποσών: σύνολο ΜΟΝΟ από ΧΕΠ (αποφυγή διπλομέτρησης).
    # has_amounts = False όσο δεν έχει τρέξει ακόμη η εξαγωγή ποσών από PDF.
    amount_summary = {
        "has_amounts": xep_counted > 0,
        "basis": "xep_only",   # το σύνολο βασίζεται μόνο σε Χρηματικά Εντάλματα Πληρωμής
        "xep_total": round(xep_total, 2),
        "xep_counted": xep_counted,
        "xep_low_confidence": xep_low,
        "currency": "EUR",
        "by_year": {str(y): round(v, 2) for y, v in sorted(xep_total_by_year.items())},
        "note": ("Εκτίμηση από αυτόματη εξαγωγή ποσών από PDF (κανόνες/OCR), "
                 "ΟΧΙ λογιστικά ακριβές στοιχείο. Περιλαμβάνει μόνο Χρηματικά "
                 "Εντάλματα Πληρωμής (όχι αναλήψεις/συμβάσεις) για αποφυγή "
                 "διπλομέτρησης."),
    }

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(decisions),
        "organizations": len(organizations),
    }

    payload = {
        "meta": meta,
        "organizations": organizations,
        "groups": groups,
        "decision_types": decision_types,
        "years": years,
        "amount_summary": amount_summary,
        "decisions": decisions,
    }

    os.makedirs("docs", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(OUT_PATH) / (1024 * 1024)
    print(f"Γράφτηκε {OUT_PATH}")
    print(f"  Πράξεις: {len(decisions)}")
    print(f"  Φορείς:  {len(organizations)}")
    print(f"  Τύποι:   {len(decision_types)}")
    print(f"  Έτη:     {len(years)}")
    print(f"  Μέγεθος: {size_mb:.2f} MB")
    if size_mb > 90:
        print("  ⚠ Προσοχή: το data.json πλησιάζει το όριο 100MB του GitHub.")
        print("    Σκέψου να χωρίσεις σε ανά-έτος αρχεία.")

    conn.close()


if __name__ == "__main__":
    main()
