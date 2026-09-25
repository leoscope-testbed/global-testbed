#!/usr/bin/env python3
"""
Parse a saved dump of `db.users.find(...).forEach(u => printjson(u))` output
(mongosh pretty-printed JS objects) and extract, per user:
  - institution / university (the part of `team` before the first " - ")
  - country (guessed from the `team` location text, falling back to the
    email domain's TLD when the team text doesn't name a country)

Usage:
    python3 scripts/extract_university_country.py scripts/users_list.txt

Country detection is best-effort, not authoritative - see MANUAL_OVERRIDES
and TLD_COUNTRY below to fix any misclassification for your dataset.
"""

import re
import sys
from collections import defaultdict

RECORD_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)
FIELD_RE = re.compile(r"""(\w+)\s*:\s*(?:'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)")""")

# Checked in order, first match wins. Keys are lowercase substrings to look
# for in the free-text `team`/location field.
COUNTRY_KEYWORDS = [
    ("china", "China"),
    ("germany", "Germany"),
    ("deutschland", "Germany"),
    ("norway", "Norway"),
    ("brazil", "Brazil"),
    ("netherlands", "Netherlands"),
    ("canada", "Canada"),
    ("british columbia", "Canada"),
    ("korea", "South Korea"),
    ("singapore", "Singapore"),
    ("italy", "Italy"),
    ("spain", "Spain"),
    ("denmark", "Denmark"),
    ("mauritius", "Mauritius"),
    ("united kingdom", "United Kingdom"),
    ("england", "United Kingdom"),
    ("scotland", "United Kingdom"),
    ("wales", "United Kingdom"),
    ("gbr", "United Kingdom"),
    ("guildford", "United Kingdom"),
    ("surrey", "United Kingdom"),
    ("belfast", "United Kingdom"),
    ("dorset", "United Kingdom"),
    ("uk", "United Kingdom"),
    ("usa", "United States"),
    ("united states", "United States"),
]

# Fallback by email domain suffix when the team text gives no country.
# Longest / most specific suffixes should be listed before shorter ones.
TLD_COUNTRY = [
    (".nus.edu", "Singapore"),
    (".ac.uk", "United Kingdom"),
    (".surrey.ac.uk", "United Kingdom"),
    (".co.uk", "United Kingdom"),
    (".uk", "United Kingdom"),
    (".edu.cn", "China"),
    (".edu.br", "Brazil"),
    (".mails.tsinghua.edu.cn", "China"),
    (".cn", "China"),
    (".de", "Germany"),
    (".nl", "Netherlands"),
    (".br", "Brazil"),
    (".ca", "Canada"),
    (".kr", "South Korea"),
    (".no", "Norway"),
    (".it", "Italy"),
    (".163.com", "China"),
    (".edu", "United States"),  # last resort generic fallback
]

# Explicit fixes for accounts where neither the team text nor the TLD gives
# a reliable answer (internal/test/admin accounts, .com/.org/.net domains).
MANUAL_OVERRIDES = {
    "admin@leoscope.surrey.ac.uk": ("LEOScope Admin (internal)", "United Kingdom"),
    "leoscope_test@gmail.com": ("Test account (internal)", "N/A - test account"),
    "james.s.earth@gmail.com": ("LEOScope Web Designer (internal)", "United Kingdom"),
    "admin4@leoscope.com": ("Microsoft Research / IIIT Delhi", "India"),
    "tedx.iiitdelhi@gmail.com": ("Microsoft Research / IIIT Delhi", "India"),
    "tedx.iiitdelhi3@gmail.com": ("Microsoft Research / IIIT Delhi", "India"),
    "tedx.iiitdelhi2@gmail.com": ("Microsoft Research / IIIT Delhi", "India"),
    "bos@nordu.net": ("NORDUnet", "Denmark"),
    "phokeer@isoc.org": ("Internet Society", "Mauritius"),
    "sean.hodgson@vodafone.com": ("Vodafone Group", "United Kingdom"),
    "andra.lutu@telefonica.com": ("Telefonica", "Spain"),
    "v.khandkar@surrey.ac.uk": ("University of Surrey", "United Kingdom"),
    # resolved from "NEEDS MANUAL REVIEW" on the first pass over users_list.txt
    "dravyajain@gmail.com": ("UCLA Scan Lab", "United States"),
    "leoscope.cm.tum@gmail.com": ("Technical University of Munich", "Germany"),
    "meetonk@gmail.com": ("Georgia Institute of Technology", "United States"),
    "nitindermohan@gmail.com": ("TU Delft", "Netherlands"),
}

# Canonicalize known spelling/casing variants of the same institution so
# counts aren't split across near-duplicates.
UNIVERSITY_CANONICAL = {
    "virginiatech": "Virginia Tech",
    "virginia tech aoe department": "Virginia Tech",
    "queens university": "Queen's University Belfast",
    "university of surrey": "University of Surrey",
    "tu delft": "TU Delft",
    "delft university of technology": "TU Delft",
    "university of osnabrueck": "University of Osnabrück",
    "osnabrueck university": "University of Osnabrück",
    "ucla": "UCLA",
    "university of california, los angeles": "UCLA",
    "universtiy of california, los angeles": "UCLA",
    "university of california": "UCLA",
    "technical university munich": "Technical University of Munich",
    "tum": "Technical University of Munich",
    "chair of connected mobility, technical university of munich": "Technical University of Munich",
    "microsoft research india": "Microsoft Research / IIIT Delhi",
}


def parse_records(text):
    records = []
    for block in RECORD_RE.findall(text):
        fields = {}
        for m in FIELD_RE.finditer(block):
            key = m.group(1)
            value = m.group(2) if m.group(2) is not None else m.group(3)
            fields[key] = value
        if "id" in fields:
            records.append(fields)
    return records


def guess_university(team):
    if not team:
        return None
    name = team.split(" - ", 1)[0].strip()
    if not name:
        return None
    return UNIVERSITY_CANONICAL.get(name.lower(), name)


def guess_country_from_team(team):
    if not team:
        return None
    lowered = team.lower()
    for needle, country in COUNTRY_KEYWORDS:
        if needle in lowered:
            return country
    return None


def guess_country_from_email(email):
    lowered = email.lower()
    for suffix, country in TLD_COUNTRY:
        if lowered.endswith(suffix):
            return country
    return None


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "scripts/users_list.txt"
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    records = parse_records(text)

    rows = []
    for r in records:
        user_id = r.get("id", "")
        team = r.get("team", "")
        university = guess_university(team)
        country = guess_country_from_team(team)
        source = "team-text"

        override = MANUAL_OVERRIDES.get(user_id.lower())
        if override:
            university = override[0] or university
            if override[1]:
                country = override[1]
                source = "manual-override"

        if not country:
            country = guess_country_from_email(user_id)
            source = "email-tld" if country else "unknown"

        rows.append({
            "id": user_id,
            "name": r.get("name", ""),
            "team": team,
            "university": university or "(unspecified)",
            "country": country or "Unknown",
            "source": source,
        })

    print(f"Parsed {len(rows)} user records from {path}\n")

    print("=" * 100)
    print("Per-user table")
    print("=" * 100)
    for row in sorted(rows, key=lambda x: (x["country"], x["university"])):
        print(f"{row['country']:<20} {row['university']:<55} {row['name']:<28} {row['id']}")

    country_counts = defaultdict(int)
    university_counts = defaultdict(int)
    for row in rows:
        country_counts[row["country"]] += 1
        university_counts[row["university"]] += 1

    print("\n" + "=" * 100)
    print("Users by country")
    print("=" * 100)
    for country, count in sorted(country_counts.items(), key=lambda x: -x[1]):
        print(f"{country:<25} {count}")

    print("\n" + "=" * 100)
    print("Users by university / institution")
    print("=" * 100)
    for uni, count in sorted(university_counts.items(), key=lambda x: -x[1]):
        print(f"{uni:<60} {count}")

    unknowns = [r for r in rows if r["country"] == "Unknown" or r["source"] == "unknown"]
    if unknowns:
        print("\n" + "=" * 100)
        print("NEEDS MANUAL REVIEW (country could not be determined) - add to MANUAL_OVERRIDES")
        print("=" * 100)
        for row in unknowns:
            print(f"{row['id']:<40} team={row['team']!r}")


if __name__ == "__main__":
    main()
