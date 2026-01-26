## This script is intended for ripping the database of water mills in Czech republic into tabular form (Excel).
## Based on fantastic public database - https://www.vodnimlyny.cz/
## Give them some support, they deserve it, pls.
## © dobo@dobo.sk, 2025
##
##
## Usage:
## Its vital to have these packages:
## python -m pip install requests beautifulsoup4 pandas openpyxl
## Debugging and testing:
## Prior real use, do the test/debug, this will rip only 60 records and write debug file
## python ./export_water_mills_Bohemia.py --debug --max-pages 2
## In real scenario, use some kind of throttling, You do not want to bring that webpage down!


import re
import time
import argparse
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://www.vodnimlyny.cz/mlyny/objekty/"  # original web database formatted as table
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; FreeBSD amd64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.7",
}

DMS_RE = re.compile(r"(?P<deg>\d+)\s*°\s*(?P<min>\d+)\s*'\s*(?P<sec>[\d.,]+)\s*''")

def dms_to_dd(dms: str) -> float:
    m = DMS_RE.search(dms)
    if not m:
        raise ValueError(f"Cannot parse DMS: {dms!r}")
    deg = int(m.group("deg"))
    minutes = int(m.group("min"))
    sec = float(m.group("sec").replace(",", "."))
    return deg + minutes / 60.0 + sec / 3600.0

def parse_page(html: str, page_url: str, debug: bool = False):
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")
    if debug:
        title = soup.title.get_text(" ", strip=True) if soup.title else "(no title)"
        print(f"  HTML title: {title}")
        print(f"  Found tables: {len(tables)}")

    if not tables:
        return [], "No <table> elements found"

    # There is one table on site - this takes it
    t = tables[0]
    trs = t.find_all("tr")
    if not trs:
        return [], "No <tr> rows in table"

    header_idx = None
    header_cells_norm = None

    # Find the record seems to be a header (has "gps" + "vodní tok"/"obec")
    for i, tr in enumerate(trs):
        cells = tr.find_all(["th", "td"])
        texts = [" ".join(c.get_text(" ", strip=True).split()).strip() for c in cells]
        norm = [x.lower() for x in texts]

        has_gps = any("gps" in x for x in norm)
        has_tok = any("vodní tok" in x or "watercourse" in x for x in norm)
        has_obec = any("obec" in x or "municipality" in x for x in norm)

        if has_gps and has_tok and has_obec:
            header_idx = i
            header_cells_norm = norm
            if debug:
                print("  Detected header row:", texts)
            break

    if header_idx is None:
        if debug:
            for k in range(min(3, len(trs))):
                cells = trs[k].find_all(["th", "td"])
                texts = [" ".join(c.get_text(" ", strip=True).split()).strip() for c in cells]
                print(f"  Row {k} sample cells:", texts)
        return [], "No matching header row detected (GPS/Vodní tok/Obec)"

    def pick(*cands):
        for cand in cands:
            cand = cand.lower()
            for j, h in enumerate(header_cells_norm):
                if cand in h:
                    return j
        return None

    i_name = pick("název", "name")
    i_gps  = pick("gps")
    i_tok  = pick("vodní tok", "watercourse")
    i_obec = pick("obec", "municipality")

    if any(x is None for x in (i_name, i_gps, i_tok, i_obec)):
        return [], f"Header indices missing: name={i_name}, gps={i_gps}, tok={i_tok}, obec={i_obec}"

    rows_out = []
    for tr in trs[header_idx + 1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        if len(tds) <= max(i_name, i_gps, i_tok, i_obec):
            continue

        # name + URL (link in <a>)
        name_link = tds[i_name].find("a")
        if name_link:
            name = name_link.get_text(" ", strip=True).replace("»", "").strip()
            href = name_link.get("href", "").strip()
            url = urljoin(page_url, href) if href else ""
        else:
            name = tds[i_name].get_text(" ", strip=True).replace("»", "").strip()
            url = ""

        # GPS: as two rows on the page (lat \n lon)
        gps_text = tds[i_gps].get_text("\n", strip=True)
        parts = [p.strip() for p in gps_text.splitlines() if p.strip()]
        if len(parts) < 2:
            continue
        lat_dms, lon_dms = parts[0], parts[1]

        try:
            lat = dms_to_dd(lat_dms)
            lon = dms_to_dd(lon_dms)
        except ValueError:
            continue

        vodni_tok = tds[i_tok].get_text(" ", strip=True)

        # "Obec / Katastrální území / Okres" is in one cell but on diferent rows
        loc_text = tds[i_obec].get_text("\n", strip=True)
        loc_parts = [p.strip() for p in loc_text.splitlines() if p.strip()]

        obec = loc_parts[0] if len(loc_parts) >= 1 else ""
        katastralni_uzemi = loc_parts[1] if len(loc_parts) >= 2 else ""
        okres = loc_parts[2] if len(loc_parts) >= 3 else ""

        rows_out.append({
            "jmeno": name,
            "url": url,
            "GPS_Lat": lat,
            "GPS_Lon": lon,
            "vodni_tok": vodni_tok,
            "obec": obec,
            "katastralni_uzemi": katastralni_uzemi,
            "okres": okres,
        })

    return rows_out, None

def export_all(max_pages=450, sleep_s=0.5, debug=False):
    out = []
    s = requests.Session()
    s.headers.update(HEADERS)

    for page in range(1, max_pages + 1):
        page_url = BASE if page == 1 else f"{BASE}?paginator-page={page}"
        r = s.get(page_url, timeout=30)

        if debug:
            print(f"\nFetching page {page}: {page_url}")
            print(f"  Status: {r.status_code}, bytes: {len(r.content)}")

        r.raise_for_status()

        rows, err = parse_page(r.text, page_url=page_url, debug=debug)

        if not rows:
            fname = f"debug_page{page}.html"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(r.text)
            print(f"Stopping at page {page} (no rows parsed). Reason: {err}")
            print(f"Saved HTML to: {fname}")
            break

        out.extend(rows)
        print(f"Page {page}: +{len(rows)} rows (total {len(out)})")

        time.sleep(sleep_s)

    df = pd.DataFrame(out).drop_duplicates(subset=["jmeno", "GPS_Lat", "GPS_Lon"])
    df.to_excel("vodni_mlyny_export.xlsx", index=False)
    print(f"Done: {len(df)} rows -> vodni_mlyny_export.xlsx")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=500)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    export_all(max_pages=args.max_pages, sleep_s=args.sleep, debug=args.debug)

if __name__ == "__main__":
    main()
