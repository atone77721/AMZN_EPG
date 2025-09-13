#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, re, sys, time, gzip, io
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape as html_unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
import requests
from xml.etree.ElementTree import Element, SubElement, ElementTree
import xml.etree.ElementTree as ET

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ----------------------- CLI -----------------------
@dataclass
class Options:
    urls: List[str]
    output: str
    cookie: Optional[str]
    user_agent: Optional[str]
    referer: Optional[str]
    channel_id_mode: str
    slugify_names: bool
    timezone_name: str
    timeout: int
    delay: float
    extra_headers: List[str]
    debug: bool
    post_url: str
    payload_1: Optional[str]
    payload_2: Optional[str]

def parse_args(argv: List[str]) -> Options:
    ap = argparse.ArgumentParser(description="Scrape Amazon Live TV pages; fallback to getLandingPage POST; emit XMLTV")
    ap.add_argument("--url", action="append", default=[], help="Live TV HTML URL (repeatable)")
    ap.add_argument("-o", "--output", default="data/amazon.xml", help="Output XMLTV (default: data/amazon.xml)")
    ap.add_argument("--cookie", help="Cookie header; if omitted uses AMAZON_COOKIE env")
    ap.add_argument("--user-agent", help="User-Agent override")
    ap.add_argument("--referer", help="Referer override (default: https://www.amazon.com/gp/video/livetv/)")
    ap.add_argument("--channel-id", choices=["name", "id"], default="name", help="Use channel 'name' or 'id'")
    ap.add_argument("--slugify-names", action="store_true", help="Slugify channel IDs")
    ap.add_argument("--timezone", default="America/Los_Angeles", help="Timezone (e.g., America/Los_Angeles)")
    ap.add_argument("--timeout", type=int, default=25, help="HTTP timeout (s)")
    ap.add_argument("--delay", type=float, default=0.4, help="Delay between requests")
    ap.add_argument("--header", action="append", default=[], help="Extra header 'Key: Value' (repeatable)")
    ap.add_argument("--debug", action="store_true", help="Save HTML/XHR bodies to responses/")
    ap.add_argument("--post-url", default="https://www.amazon.com/gp/video/api/getLandingPage?",
                    help="Amazon XHR endpoint (default: /gp/video/api/getLandingPage?)")
    ap.add_argument("--payload-1", help="POST form data for getLandingPage (payload 1)")
    ap.add_argument("--payload-2", help="POST form data for getLandingPage (payload 2)")
    args = ap.parse_args(argv)
    return Options(
        urls=list(args.url or []),
        output=args.output,
        cookie=args.cookie or os.environ.get("AMAZON_COOKIE"),
        user_agent=args.user_agent,
        referer=args.referer,
        channel_id_mode=args.channel_id,
        slugify_names=args.slugify_names,
        timezone_name=args.timezone,
        timeout=args.timeout,
        delay=args.delay,
        extra_headers=list(args.header or []),
        debug=args.debug,
        post_url=args.post_url,
        payload_1=args.payload_1 or os.environ.get("PV_PAYLOAD_1"),
        payload_2=args.payload_2 or os.environ.get("PV_PAYLOAD_2"),
    )

# ----------------------- helpers -----------------------
def coerce_timezone(name: str):
    if ZoneInfo:
        try: return ZoneInfo(name)
        except Exception: return timezone.utc
    return timezone.utc

def slugify(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]+", "", s)
    return s or "unknown"

def xmltv_time(ms: int, tzinfo) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tzinfo)
    off = dt.utcoffset() or datetime.now(timezone.utc).utcoffset()
    total = int((off.total_seconds() // 60) if off else 0)
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hh, mm = divmod(total, 60)
    return dt.strftime("%Y%m%d%H%M%S") + f" {sign}{hh:02d}{mm:02d}"

def apply_headers(cookie: Optional[str], ua: Optional[str], ref: Optional[str], extra: List[str]) -> Dict[str, str]:
    h: Dict[str, str] = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": ua or "Mozilla/5.0",
        "Referer": ref or "https://www.amazon.com/gp/video/livetv/",
        "Origin": "https://www.amazon.com",
    }
    if cookie: h["Cookie"] = cookie
    for line in extra:
        if ":" in line:
            k, v = line.split(":", 1)
            h[k.strip()] = v.strip()
    return h

def apply_json_headers(cookie: Optional[str], ua: Optional[str], ref: Optional[str], extra: List[str]) -> Dict[str, str]:
    h = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": ua or "Mozilla/5.0",
        "Referer": ref or "https://www.amazon.com/gp/video/livetv/",
        "Origin": "https://www.amazon.com",
        "X-Requested-With": "XMLHttpRequest",
    }
    if cookie: h["Cookie"] = cookie
    for line in extra:
        if ":" in line:
            k, v = line.split(":", 1)
            h[k.strip()] = v.strip()
    return h

# ----------------------- extraction -----------------------
SCRIPT_ID_RE = re.compile(r'<script[^>]+id=["\']dv-web-store-template["\'][^>]*>(.*?)</script>', re.I | re.S)

def _sanitize_json_escapes(s: str) -> str:
    return re.sub(r'\\([^"\\/bfnrtu])', lambda m: m.group(1), s)

def _try_json(s: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(s)
    except Exception:
        return None

def extract_store_json_from_html(html_text: str) -> Optional[Dict[str, Any]]:
    m = SCRIPT_ID_RE.search(html_text)
    if m:
        content = html_unescape(m.group(1)).strip()
        candidates = [content]
        if content[:1] in "\"'" and content[-1:] == content[:1]:
            candidates.append(content[1:-1].replace('\\"','"').replace("\\/","/"))
        for candidate in candidates:
            js = _try_json(candidate) or _try_json(_sanitize_json_escapes(candidate))
            if js: return js

    epg_idx = html_text.find('"containerType":"EpgGroup"')
    if epg_idx != -1:
        start = html_text.rfind("{", 0, epg_idx)
        if start != -1:
            i = start; depth = 0
            while i < len(html_text):
                if html_text[i] == "{": depth += 1
                elif html_text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        chunk = html_text[start:i+1]
                        js = _try_json(chunk) or _try_json(_sanitize_json_escapes(chunk))
                        if js: return js
                        break
                i += 1

    apollo = re.search(r'window\.__APOLLO_STATE__\s*=\s*({.*?});\s*</script>', html_text, re.S)
    if apollo:
        js = _try_json(apollo.group(1))
        if js: return js
    return None

def extract_epg(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    stations: List[Dict[str, Any]] = []
    def walk(o: Any):
        if isinstance(o, dict):
            if o.get("containerType") == "EpgGroup" and isinstance(o.get("entities"), list):
                for ent in o["entities"]:
                    st = ent.get("station") if isinstance(ent, dict) else None
                    if isinstance(st, dict):
                        stations.append(st)
            if "station" in o and isinstance(o["station"], dict):
                stations.append(o["station"])
            if "stations" in o and isinstance(o["stations"], list):
                for st in o["stations"]:
                    if isinstance(st, dict) and ("schedule" in st or "name" in st):
                        stations.append(st)
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for v in o: walk(v)
    walk(payload)
    seen = set(); uniq = []
    for st in stations:
        key = (st.get("id"), st.get("name"))
        if key not in seen:
            seen.add(key); uniq.append(st)
    return uniq

def merge(master: Dict[Tuple[str, str], Dict[str, Any]], stations: List[Dict[str, Any]]):
    for st in stations:
        key = (st.get("id"), st.get("name"))
        cur = master.get(key)
        if not cur:
            copy = dict(st); copy["schedule"] = list(st.get("schedule") or [])
            master[key] = copy
        else:
            sched = cur.setdefault("schedule", [])
            seen = {(i.get("start"), i.get("end"), (i.get("metadata") or {}).get("title"))
                    for i in sched if isinstance(i, dict)}
            for it in st.get("schedule") or []:
                if not isinstance(it, dict): continue
                sig = (it.get("start"), it.get("end"), (it.get("metadata") or {}).get("title"))
                if sig not in seen:
                    sched.append(it); seen.add(sig)

def build_xmltv(stations: Iterable[Dict[str, Any]], tzinfo, use_names=True, slug_ids=False):
    tv = Element("tv", attrib={"source-info-name": "PrimeVideo", "generator-info-name": "prime_livetv.py"})
    added = set()
    for st in stations:
        sid = st.get("id")
        sname = st.get("name") or sid or "unknown"
        ch_id = sname if use_names else (sid or sname)
        if slug_ids: ch_id = slugify(ch_id)
        logo = st.get("logo")
        if ch_id not in added:
            ch = SubElement(tv, "channel", attrib={"id": ch_id})
            SubElement(ch, "display-name", attrib={"lang": "en"}).text = sname
            if isinstance(logo, str) and logo:
                SubElement(ch, "icon", attrib={"src": logo})
            added.add(ch_id)
        sched = st.get("schedule") or []
        if not isinstance(sched, list): continue
        for it in sched:
            if not isinstance(it, dict): continue
            start = it.get("start"); end = it.get("end")
            meta = it.get("metadata", {}) if isinstance(it.get("metadata", {}), dict) else {}
            if start is None or end is None: continue
            try: start = int(start); end = int(end)
            except Exception: continue
            pr = SubElement(tv, "programme", attrib={
                "start": xmltv_time(start, tzinfo),
                "stop": xmltv_time(end, tzinfo),
                "channel": ch_id
            })
            SubElement(pr, "title", attrib={"lang": "en"}).text = str(meta.get("title") or meta.get("seriesTitle") or "Untitled")
            if meta.get("seriesTitle"):
                SubElement(pr, "sub-title", attrib={"lang": "en"}).text = str(meta["seriesTitle"])
            if meta.get("synopsis"):
                SubElement(pr, "desc", attrib={"lang": "en"}).text = str(meta["synopsis"])
            img = (meta.get("image") or {}).get("url") if isinstance(meta.get("image"), dict) else None
            if img:
                SubElement(pr, "icon", attrib={"src": str(img)})
            if meta.get("releaseYear"):
                SubElement(pr, "date").text = str(meta["releaseYear"])
            rating = meta.get("contentMaturityRating") or {}
            if isinstance(rating, dict) and rating.get("rating"):
                r = SubElement(pr, "rating", attrib={"system": rating.get("locale", "us")})
                SubElement(r, "value").text = str(rating["rating"])
    return tv

def write_xml_pretty(tv_elem: Element, out_path: str):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    try:
        ET.indent(tv_elem, space="  ")
    except Exception:
        pass
    ElementTree(tv_elem).write(out_path, encoding="utf-8", xml_declaration=True)

# ----------------------- network -----------------------
def fetch_html(url: str, headers: Dict[str, str], timeout: int) -> Tuple[int, str, Dict[str,str]]:
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        return resp.status_code, resp.text, dict(resp.headers)
    except requests.RequestException as e:
        print(f"Request error for {url}: {e}", file=sys.stderr)
        return 0, "", {}

def post_json(url: str, headers: Dict[str, str], payload: str, timeout: int) -> Tuple[int, Optional[Dict[str, Any]], str]:
    try:
        resp = requests.post(url, headers=headers, data=payload.encode("utf-8"), timeout=timeout, allow_redirects=True)
        body = resp.content or b""
        if len(body) >= 2 and body[0] == 0x1F and body[1] == 0x8B:
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                text = gz.read().decode("utf-8", errors="strict")
        else:
            text = body.decode("utf-8", errors="replace")
        if "application/json" in (resp.headers.get("content-type") or "").lower():
            try:
                return resp.status_code, json.loads(text), text
            except Exception:
                return resp.status_code, None, text
        try:
            return resp.status_code, json.loads(text), text
        except Exception:
            return resp.status_code, None, text
    except requests.RequestException as e:
        return 0, None, f"Request error: {e}"

def apply_live_urls() -> List[str]:
    return [
        "https://www.amazon.com/gp/video/livetv/ref=atv_hm_liv_LR375370_slct?serviceToken=v0_Cl0KJGYyNjIwZWVmLWU4YjMtNDNiYS1hOGYwLTM5OTU5NTVkN2Q0ORDYua6HlDMaLExpNitvL2dzaDBoR0NjVGdhVGdLTHptYkF6dHpuZ29zb2VJMDZ6YWhmZEk9IAESBmZpbHRlchgBIgRob21lKgRsaXZlWj4KDGxpbmVhckZpbHRlchIuCixhbXpuMS1wdi1saW5lYXItbGl2ZV90YWItZmlsdGVyLWFuaW1lX2dhbWluZ3oAggEGMABQAHAA&dvWebSPAClientVersion=1.0.111305.0",
        "https://www.amazon.com/gp/video/livetv/ref=atv_hm_liv_LRd39d40_slct?serviceToken=v0_Cl0KJDYwMTRmN2Q2LTY2ODEtNDQ1NC05NWY2LTA4NWE5ZDc1Y2ZkYhCYxb-HlDMaLExpNitvL2dzaDBoR0NjVGdhVGdLTHptYkF6dHpuZ29zb2VJMDZ6YWhmZEk9IAESBmZpbHRlchgBIgRob21lKgRsaXZlWjgKDGxpbmVhckZpbHRlchIoCiZhbXpuMS1wdi1saW5lYXItbGl2ZV90YWItZmlsdGVyLWZhbWlseXoAggEGMABQAHAA&dvWebSPAClientVersion=1.0.111305.0",
        "https://www.amazon.com/gp/video/livetv/ref=atv_hm_liv_LR9d1a7d_slct?serviceToken=v0_Cl0KJGY0YWIwYjY0LTBjMjctNDkxZS04ODZiLTc0MWJkYzA1M2FkYhDQvsmHlDMaLExpNitvL2dzaDBoR0NjVGdhVGdLTHptYkF6dHpuZ29zb2VJMDZ6YWhmZEk9IAESBmZpbHRlchgBIgRob21lKgRsaXZlWjgKDGxpbmVhckZpbHRlchIoCiZhbXpuMS1wdi1saW5lYXItbGl2ZV90YWItZmlsdGVyLW1vdmllc3oAggEGMABQAHAA&dvWebSPAClientVersion=1.0.111305.0",
        "https://www.amazon.com/gp/video/livetv/ref=atv_hm_liv_LRd40aee_slct?serviceToken=v0_Cl0KJGMxNzg5ZmQzLTY4ZGItNGU4ZC1hNzMzLTE2ZDkyMzAxYzc1MhCQ9MyHlDMaLExpNitvL2dzaDBoR0NjVGdhVGdLTHptYkF6dHpuZ29zb2VJMDZ6YWhmZEk9IAESBmZpbHRlchgBIgRob21lKgRsaXZlWjsKDGxpbmVhckZpbHRlchIrCilhbXpuMS1wdi1saW5lYXItbGl2ZV90YWItZmlsdGVyLXN1YnNjcmliZXoAggEGMABQAHAA&dvWebSPAClientVersion=1.0.111305.0",
        "https://www.amazon.com/gp/video/livetv/ref=atv_hm_liv_LR755354_slct?serviceToken=v0_Cl0KJDRjNGMwZmI0LTdiZmUtNDExZi1iMTY1LWEyMjNmNDhhY2E1ZhDQrM-HlDMaLExpNitvL2dzaDBoR0NjVGdhVGdLTHptYkF6dHpuZ29zb2VJMDZ6YWhmZEk9IAESBmZpbHRlchgBIgRob21lKgRsaXZlWjYKDGxpbmVhckZpbHRlchImCiRhbXpuMS1wdi1saW5lYXItbGl2ZV90YWItZmlsdGVyLW5ld3N6AIIBBjAAUABwAA%3D%3D&dvWebSPAClientVersion=1.0.111305.0",
    ]

# ----------------------- main -----------------------
def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    if not opts.urls:
        opts.urls = apply_live_urls()

    headers_html = apply_headers(opts.cookie, opts.user_agent, opts.referer, opts.extra_headers)
    headers_xhr  = apply_json_headers(opts.cookie, opts.user_agent, opts.referer, opts.extra_headers)
    if opts.debug:
        os.makedirs("responses", exist_ok=True)

    master: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # 1) Try HTML pages first
    for i, url in enumerate(opts.urls, 1):
        print(f"[{i}/{len(opts.urls)}] Fetching {urlparse(url).path}…")
        status, html, resp_hdrs = fetch_html(url, headers_html, opts.timeout)
        if opts.debug:
            with open(f"responses/page_{i}.html", "w", encoding="utf-8") as f:
                f.write(html or "")
            with open(f"responses/page_{i}_headers.txt", "w", encoding="utf-8") as f:
                for k,v in (resp_hdrs or {}).items():
                    f.write(f"{k}: {v}\n")
        if status != 200 or not html:
            print(f"  -> HTTP {status}; empty or failed body, skipping.", file=sys.stderr)
            time.sleep(opts.delay); continue
        store = extract_store_json_from_html(html)
        if not store:
            print("  -> No store JSON found.", file=sys.stderr)
            time.sleep(opts.delay); continue
        stations = extract_epg(store)
        if not stations:
            print("  -> No stations found in store JSON, skipping.", file=sys.stderr)
            time.sleep(opts.delay); continue
        merge(master, stations)
        print(f"  -> Aggregated stations: {len(master)}")
        time.sleep(opts.delay)

    # 2) Fallback to getLandingPage POST payloads
    if not master and (opts.payload_1 or opts.payload_2):
        print("No stations from HTML. Falling back to getLandingPage POST payloads…")
        for idx, payload in enumerate([opts.payload_1, opts.payload_2], 1):
            if not payload:
                continue
            st_code, j, raw = post_json(opts.post_url, headers_xhr, payload, opts.timeout)
            if opts.debug:
                with open(f"responses/landing_page_p{idx}.txt", "w", encoding="utf-8") as f:
                    f.write(raw or "")
            if st_code != 200 or j is None:
                print(f"  -> POST {idx} HTTP {st_code}; invalid/empty JSON.", file=sys.stderr)
                continue
            stations = extract_epg(j)
            if not stations:
                print(f"  -> POST {idx} yielded no stations.", file=sys.stderr)
                continue
            merge(master, stations)
            print(f"  -> POST {idx} aggregated stations: {len(master)}")

    if not master:
        print("No stations aggregated from HTML or XHR payloads.", file=sys.stderr)
        return 3

    tzinfo = coerce_timezone(opts.timezone_name)
    tv = build_xmltv(
        stations=list(master.values()),
        tzinfo=tzinfo,
        use_names=(opts.channel_id_mode == "name"),
        slug_ids=opts.slugify_names,
    )
    write_xml_pretty(tv, opts.output)
    print(f"Wrote XMLTV: {opts.output}")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
