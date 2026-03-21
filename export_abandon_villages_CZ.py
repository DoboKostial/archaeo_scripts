#!/usr/bin/python
# This script rips https://zanikleobce.cz and parse the content to excel sheet
### USAGE:
# required toggles: --out (name of file where to export)
# optional toggles: --min-id , --max-id , --debug , --append , --sleep
# 'python export_abandon_villages_CZ.py --min-id 1 --max-id 50 --debug --out test.xlsx' ---> exports first 50 records in debug mode and stores in test.xlsx
# 'python export_abandon_villages_CZ.py -min-id 501 --max-id 1000 --sleep 1.0 --out zanikle_obce.xlsx --append' ---> exports 501-1000 record and appends it to file zanikle_obce. Waits 1 sec for every record to parse
#
# author: dobo@dobo.sk
# 

import re
import time
import argparse
import os
from urllib.parse import urljoin, urlparse, parse_qs

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_SITE = "https://www.zanikleobce.cz/"
DETAIL_TMPL = "https://www.zanikleobce.cz/index.php?obec={id}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) zanikleobce-export/3.3",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.7",
}

def clean_text(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.replace("\xa0", " ").split()).strip()

def session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def fetch_html(sess: requests.Session, url: str, timeout=30, retries=5, backoff=1.7, debug=False):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = sess.get(url, timeout=timeout)
            if r.status_code == 404:
                return None, 404
            r.raise_for_status()
            return r.text, r.status_code
        except Exception as e:
            last_err = e
            if debug:
                print(f"[WARN] fetch failed ({attempt}/{retries}) {url} -> {e}")
            time.sleep((backoff ** attempt) / 4.0)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")

def find_main_content_node(soup: BeautifulSoup):
    candidates = []
    for tag in soup.find_all(["div", "main", "section", "article", "td"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if len(txt) >= 250:
            candidates.append((len(txt), tag))
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
    return soup.body or soup

def extract_coords_wgs84(full_text: str):
    """
    Vrací (a,b) tak jak je to v textu nejčastěji: (lat, lon).
    Neřešíme tady přeznačení X/Y – to uděláme až při ukládání do sloupců.
    """
    t = full_text.replace(",", ".")
    m = re.search(
        r"\bX\b\s*[:=]?\s*([+-]?\d+(?:\.\d+)?)\s*.*?\bY\b\s*[:=]?\s*([+-]?\d+(?:\.\d+)?)",
        t,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        return m.group(1), m.group(2)

    if re.search(r"(WGS|GPS)", t, flags=re.IGNORECASE):
        nums = re.findall(r"([+-]?\d+(?:\.\d+)?)", t)
        for i in range(len(nums) - 1):
            a = float(nums[i]); b = float(nums[i + 1])
            # typicky lat ~ 47-51.5, lon ~ 11-19.5
            if 47.0 <= a <= 51.5 and 11.0 <= b <= 19.5:
                return str(a), str(b)  # lat, lon
            if 47.0 <= b <= 51.5 and 11.0 <= a <= 19.5:
                return str(b), str(a)  # lat, lon
    return "", ""

def extract_alt_names(node: BeautifulSoup) -> str:
    txt = node.get_text("\n", strip=True).replace("\xa0", " ")
    m = re.search(r"Tak[eé]\s*:\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    return clean_text(m.group(1)) if m else ""

def extract_facet_text(soup: BeautifulSoup, facet_key: str) -> str:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "menu=11" not in href:
            continue
        if f"{facet_key}=" not in href:
            continue
        txt = clean_text(a.get_text(" ", strip=True))
        if txt and "přidat" not in txt.lower() and txt.lower() != "více":
            if txt.lower() in {"deutsch", "english", "česky", "cesky"}:
                continue
            return txt
    return ""

def extract_name_from_self_link(soup: BeautifulSoup, obec_id: int) -> str:
    cands = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(BASE_SITE, href)
        qs = parse_qs(urlparse(abs_url).query)

        ob = (qs.get("obec") or [None])[0]
        if ob != str(obec_id):
            continue
        if "lang" in qs:
            continue

        txt = clean_text(a.get_text(" ", strip=True))
        if not txt:
            continue
        low = txt.lower()
        if low in {"více", "home", "úvod", "databáze", "deutsch", "english", "česky", "cesky"}:
            continue
        if "přidat" in low:
            continue
        if len(txt) > 80:
            continue

        cands.append(txt)

    if not cands:
        return ""
    cands.sort(key=len)
    return cands[0]

def is_probably_valid_obec_page(soup: BeautifulSoup, obec_id: int) -> bool:
    if extract_name_from_self_link(soup, obec_id):
        return True
    html = str(soup)
    return bool(re.search(r"menu=11&(?:typ|okr|duv|obd|stv)=", html, flags=re.IGNORECASE))

def parse_detail(html: str, url: str, obec_id: int, debug=False):
    soup = BeautifulSoup(html, "html.parser")
    if not is_probably_valid_obec_page(soup, obec_id):
        if debug:
            print(f"[SKIP] obec={obec_id} (not a valid obec page)")
        return None

    node = find_main_content_node(soup)

    name = extract_name_from_self_link(soup, obec_id)
    if not name:
        h1 = node.find("h1")
        if h1:
            name = clean_text(h1.get_text(" ", strip=True))

    alt = extract_alt_names(node)

    # coords from text (typically returns lat, lon)
    lat, lon = extract_coords_wgs84(clean_text(node.get_text(" ", strip=True)))

    rec = {
        "obec_id": obec_id,
        "nazev": name,
        "alternativni_nazvy": alt,
        "kategorie": extract_facet_text(soup, "typ"),
        "okres": extract_facet_text(soup, "okr"),
        "duvod_zaniku": extract_facet_text(soup, "duv"),
        "obdobi_zaniku": extract_facet_text(soup, "obd"),
        "soucasny_stav": extract_facet_text(soup, "stv"),
        "wgs84_x": lon,  # switched name
        "wgs84_y": lat,  # switched names
        "pocet_obrazku": len(node.find_all("img")),
        "url": url,
    }

    if debug:
        print("[DEBUG]", {
            "id": rec["obec_id"],
            "nazev": rec["nazev"],
            "kategorie": rec["kategorie"],
            "okres": rec["okres"],
            "duvod": rec["duvod_zaniku"],
            "obdobi": rec["obdobi_zaniku"],
            "stav": rec["soucasny_stav"],
            "wgs84_x(lon)": rec["wgs84_x"],
            "wgs84_y(lat)": rec["wgs84_y"],
            "img": rec["pocet_obrazku"],
        })

    return rec

def load_existing_if_append(path: str, append: bool):
    if not append or not os.path.exists(path):
        return None
    try:
        df = pd.read_excel(path)
        return df if "obec_id" in df.columns else None
    except Exception:
        return None

def export_range(min_id: int, max_id: int, out_xlsx: str, sleep_s=0.8, debug=False, append=False):
    sess = session()
    records = []
    existing_df = load_existing_if_append(out_xlsx, append=append)

    for ob_id in range(min_id, max_id + 1):
        url = DETAIL_TMPL.format(id=ob_id)
        html, status = fetch_html(sess, url, debug=debug)

        if html is None:
            if debug:
                print(f"[SKIP] obec={ob_id} (HTTP {status})")
            time.sleep(sleep_s)
            continue

        rec = parse_detail(html, url=url, obec_id=ob_id, debug=debug)
        if rec is not None:
            records.append(rec)

        time.sleep(sleep_s)

    batch_df = pd.DataFrame(records)

    if existing_df is not None and not existing_df.empty:
        df = pd.concat([existing_df, batch_df], ignore_index=True)
        df = df.drop_duplicates(subset=["obec_id"], keep="last")
    else:
        df = batch_df

    preferred = [
        "obec_id", "nazev", "alternativni_nazvy",
        "kategorie", "okres", "duvod_zaniku", "obdobi_zaniku", "soucasny_stav",
        "wgs84_x", "wgs84_y", "pocet_obrazku", "url",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    df.to_excel(out_xlsx, index=False)
    print(f"[DONE] {len(df)} rows -> {out_xlsx}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-id", type=int, default=1, help="Start obec id (inclusive)")
    ap.add_argument("--max-id", type=int, required=True, help="End obec id (inclusive)")
    ap.add_argument("--out", default="zanikle_obce_export.xlsx")
    ap.add_argument("--sleep", type=float, default=0.8)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--append", action="store_true", help="Append to existing XLSX and dedupe by obec_id")
    args = ap.parse_args()

    if args.min_id < 1 or args.max_id < args.min_id:
        raise SystemExit("Invalid range: require 1 <= min-id <= max-id")

    export_range(
        min_id=args.min_id,
        max_id=args.max_id,
        out_xlsx=args.out,
        sleep_s=args.sleep,
        debug=args.debug,
        append=args.append,
    )

if __name__ == "__main__":
    main()
