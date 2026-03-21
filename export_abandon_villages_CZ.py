import re
import time
import argparse
from urllib.parse import urljoin, urlparse, parse_qs

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_SITE = "https://www.zanikleobce.cz/"
DETAIL_TMPL = "https://www.zanikleobce.cz/index.php?obec={id}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) zanikleobce-export/3.0",
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
            if 47.0 <= a <= 51.5 and 11.0 <= b <= 19.5:
                return str(a), str(b)
            if 47.0 <= b <= 51.5 and 11.0 <= a <= 19.5:
                return str(b), str(a)
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
    if re.search(r"menu=11&(?:typ|okr|duv|obd|stv)=", html, flags=re.IGNORECASE):
        return True
    return False

def parse_detail(html: str, url: str, obec_id: int, debug=False, save_debug_html=False):
    soup = BeautifulSoup(html, "html.parser")

    if not is_probably_valid_obec_page(soup, obec_id):
        if debug and save_debug_html:
            with open(f"debug_obec_{obec_id}.html", "w", encoding="utf-8") as f:
                f.write(html)
        return None

    node = find_main_content_node(soup)

    name = extract_name_from_self_link(soup, obec_id)
    if not name:
        h1 = node.find("h1")
        if h1:
            name = clean_text(h1.get_text(" ", strip=True))

    alt = extract_alt_names(node)

    kategorie = extract_facet_text(soup, "typ")
    okres = extract_facet_text(soup, "okr")
    duvod = extract_facet_text(soup, "duv")
    obdobi = extract_facet_text(soup, "obd")
    stav = extract_facet_text(soup, "stv")

    full_text = clean_text(node.get_text(" ", strip=True))
    x, y = extract_coords_wgs84(full_text)

    img_count = len(node.find_all("img"))

    rec = {
        "obec_id": obec_id,
        "nazev": name,
        "alternativni_nazvy": alt,
        "kategorie": kategorie,
        "okres": okres,
        "duvod_zaniku": duvod,
        "obdobi_zaniku": obdobi,
        "soucasny_stav": stav,
        "wgs84_x": x,
        "wgs84_y": y,
        "pocet_obrazku": img_count,
        "url": url,
    }

    if debug:
        print("[DEBUG]", {
            "id": obec_id,
            "nazev": rec["nazev"],
            "okres": rec["okres"],
            "kategorie": rec["kategorie"],
            "duvod": rec["duvod_zaniku"],
            "obdobi": rec["obdobi_zaniku"],
            "stav": rec["soucasny_stav"],
            "x": rec["wgs84_x"],
            "y": rec["wgs84_y"],
            "img": rec["pocet_obrazku"],
        })

    return rec

def export_range(max_id: int, out_xlsx: str, sleep_s=0.8, debug=False, save_debug_html=False):
    sess = session()
    records = []

    for ob_id in range(1, max_id + 1):
        url = DETAIL_TMPL.format(id=ob_id)
        html, status = fetch_html(sess, url, debug=debug)

        if html is None:
            if debug:
                print(f"[SKIP] obec={ob_id} (HTTP {status})")
            continue

        rec = parse_detail(html, url=url, obec_id=ob_id, debug=debug, save_debug_html=save_debug_html)
        if rec is None:
            if debug:
                print(f"[SKIP] obec={ob_id} (not a valid obec page)")
            time.sleep(sleep_s)
            continue

        records.append(rec)

        if ob_id % 50 == 0:
            print(f"[INFO] processed {ob_id}/{max_id}, extracted {len(records)}")

        time.sleep(sleep_s)

    df = pd.DataFrame(records)
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
    ap.add_argument("--max-id", type=int, required=True)
    ap.add_argument("--out", default="zanikle_obce_export.xlsx")
    ap.add_argument("--sleep", type=float, default=0.8)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--save-debug-html", action="store_true")
    args = ap.parse_args()

    export_range(
        max_id=args.max_id,
        out_xlsx=args.out,
        sleep_s=args.sleep,
        debug=args.debug,
        save_debug_html=args.save_debug_html,
    )

if __name__ == "__main__":
    main()
