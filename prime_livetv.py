#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, re, sys, time, gzip, io
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

def parse_args(argv: List[str]) -> Options:
    ap = argparse.ArgumentParser(description="Prime LiveTV → XMLTV (handles JSON or HTML Live pages)")
    ap.add_argument("--url", action="append", default=[], help="Live TV URL(s) (repeatable)")
    ap.add_argument("-o", "--output", default="data/amazon.xml", help="Output XMLTV (default: data/amazon.xml)")
    ap.add_argument("--cookie", help="Cookie header; if omitted, uses AMAZON_COOKIE env")
    ap.add_argument("--user-agent", help="User-Agent override")
    ap.add_argument("--referer", help="Referer override (default: https://www.amazon.com/gp/video/livetv/)")
    ap.add_argument("--channel-id", choices=["name", "id"], default="name", help="Use channel 'name' or 'id'")
    ap.add_argument("--slugify-names", action="store_true", help="Slugify channel IDs")
    ap.add_argument("--timezone", default="America/Los_Angeles", help="Timezone (e.g., America/Los_Angeles)")
    ap.add_argument("--timeout", type=int, default=25, help="HTTP timeout (s)")
    ap.add_argument("--delay", type=float, default=0.3, help="Delay between requests")
    ap.add_argument("--header", action="append", default=[], help="Extra header 'Key: Value' (repeatable)")
    ap.add_argument("--debug", action="store_true", help="Save responses/ for inspection")
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

def xmltv_time(dt: datetime) -> str:
    off = dt.utcoffset() or datetime.now(timezone.utc).utcoffset()
    total = int((off.total_seconds() // 60) if off else 0)
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hh, mm = divmod(total, 60)
    return dt.strftime("%Y%m%d%H%M%S") + f" {sign}{hh:02d}{mm:02d}"

# ----------------------- headers -----------------------
def apply_headers(cookie: Optional[str], ua: Optional[str], ref: Optional[str], extra: List[str]) -> Dict[str, str]:
    h: Dict[str, str] = {
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": ua or "Mozilla/5.0",
        "Referer": ref or "https://www.amazon.com/gp/video/livetv/",
        "Origin": "https://www.amazon.com",
        "X-Requested-With": "WebSPA",
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

def extract_store_from_html_or_text(text: str) -> Optional[Dict[str, Any]]:
    # If it’s already JSON (as in your header sample), parse directly
    t = text.lstrip()
    if t.startswith("{") or t.startswith("["):
        return _try_json(t)

    # Otherwise try to locate the store script
    m = SCRIPT_ID_RE.search(text)
    if m:
        content = html_unescape(m.group(1)).strip()
        for cand in (content,
                     (content[1:-1].replace('\\"','"').replace("\\/","/") if (content[:1] in "\"'" and content[-1:]==content[:1]) else content),
                     _sanitize_json_escapes(content)):
            js = _try_json(cand)
            if js: return js

    # Fallback: look for an object containing "EpgGroup"
    epg_idx = text.find('"containerType":"EpgGroup"')
    if epg_idx != -1:
        start = text.rfind("{", 0, epg_idx)
        if start != -1:
            i = start; depth = 0
            while i < len(text):
                if text[i] == "{": depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        chunk = text[start:i+1]
                        js = _try_json(chunk) or _try_json(_sanitize_json_escapes(chunk))
                        if js: return js
                        break
                i += 1
    return None

def extract_epg_stations(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract true EPG stations (EpgGroup → entities[].station, or fallbacks 'station'/'stations')."""
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

def synthesize_live_events(payload: Dict[str, Any], tzinfo) -> List[Dict[str, Any]]:
    """
    Some LiveTV pages return JSON with carousels of EVENT items but no EPG.
    We convert visible 'LIVE' events into pseudo-stations with a single 'now' programme.
    """
    stations: Dict[str, Dict[str, Any]] = {}

    def norm_provider(ent: Dict[str, Any]) -> str:
        # Try to infer provider from entitlementCues.providerLogo.imageUrl
        logo = (((ent.get("entitlementCues") or {}).get("providerLogo") or {}).get("imageUrl")) or ""
        m = re.search(r"/logos/([^/._]+)", logo) or re.search(r"/([a-z0-9]+)logo", logo, re.I)
        if m: return m.group(1).replace("-", " ").title()
        return "Prime Live"

    def first_image(ent: Dict[str, Any]) -> Optional[str]:
        imgs = ent.get("images") or {}
        for key in ("hero", "cover", "boxart"):
            u = (imgs.get(key) or {}).get("url")
            if u: return u
        return None

    now = datetime.now(tzinfo)
    default_dur = timedelta(hours=2)

    # Walk containers → entities
    def walk(o: Any):
        if isinstance(o, dict):
            if "entities" in o and isinstance(o["entities"], list):
                for ent in o["entities"]:
                    if not isinstance(ent, dict): continue
                    et = ent.get("entityType") or ""
                    live = ((ent.get("liveInfo") or {}).get("status") == "LIVE")
                    title = ent.get("title") or ent.get("displayTitle")
                    synopsis = ent.get("synopsis") or ""
                    if et in ("EVENT", "TITLE") and (live or title):
                        provider = norm_provider(ent)
                        logo = (((ent.get("entitlementCues") or {}).get("providerLogo") or {}).get("imageUrl")) or ""
                        img = first_image(ent)
                        ch_key = provider
                        st = stations.setdefault(ch_key, {"id": ch_key, "name": provider, "logo": logo, "schedule": []})
                        # If we can’t parse a real start, synthesize now→now+2h
                        start_dt = now
                        stop_dt = now + default_dur
                        meta = {
                            "title": title or "Live",
                            "synopsis": synopsis,
                            "image": {"url": img} if img else None,
                            "releaseYear": None,
                            "contentMaturityRating": {"rating": (ent.get("maturityRatingBadge") or {}).get("displayText", ""), "locale": "us"},
                        }
                        st["schedule"].append({
                            "start": int(start_dt.timestamp() * 1000),
                            "end": int(stop_dt.timestamp() * 1000),
                            "metadata": meta,
                        })
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for v in o: walk(v)

    walk(payload)
    return list(stations.values())

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
            try:
                start = int(start); end = int(end)
            except Exception:
                continue
            pr = SubElement(tv, "programme", attrib={
                "start": xmltv_time(datetime.fromtimestamp(start/1000, tzinfo)),
                "stop": xmltv_time(datetime.fromtimestamp(end/1000, tzinfo)),
                "channel": ch_id
            })
            SubElement(pr, "title", attrib={"lang": "en"}).text = str(meta.get("title") or "Live")
            if meta.get("synopsis"):
                SubElement(pr, "desc", attrib={"lang": "en"}).text = str(meta["synopsis"])
            img = (meta.get("image") or {}).get("url") if isinstance(meta.get("image"), dict) else None
            if img:
                SubElement(pr, "icon", attrib={"src": str(img)})
            rating = meta.get("contentMaturityRating") or {}
            if isinstance(rating, dict) and (rating.get("rating") or rating.get("displayText")):
                r = SubElement(pr, "rating", attrib={"system": rating.get("locale", "us")})
                SubElement(r, "value").text = str(rating.get("rating") or rating.get("displayText"))
    return tv

def write_xml_pretty(tv_elem: Element, out_path: str):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    try:
        ET.indent(tv_elem, space="  ")
    except Exception:
        pass
    ElementTree(tv_elem).write(out_path, encoding="utf-8", xml_declaration=True)

# ----------------------- network -----------------------
def fetch_text(url: str, headers: Dict[str, str], timeout: int) -> Tuple[int, str, Dict[str, str]]:
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        # requests auto-decompresses; ensure text
        text = resp.text
        return resp.status_code, text, dict(resp.headers)
    except requests.RequestException as e:
        print(f"Request error for {url}: {e}", file=sys.stderr)
        return 0, "", {}

# ----------------------- main -----------------------
def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    if not opts.urls:
        print("Provide at least one --url (Live TV page).", file=sys.stderr)
        return 2

    headers = apply_headers(opts.cookie, opts.user_agent, opts.referer, opts.extra_headers)
    if opts.debug:
        os.makedirs("responses", exist_ok=True)

    tzinfo = coerce_timezone(opts.timezone_name)
    master: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for i, url in enumerate(opts.urls, 1):
        path = urlparse(url).path
        print(f"[{i}/{len(opts.urls)}] GET {path} …")
        status, body, resp_hdrs = fetch_text(url, headers, opts.timeout)
        if opts.debug:
            with open(f"responses/live_{i}.txt", "w", encoding="utf-8") as f:
                f.write(body or "")
            with open(f"responses/live_{i}_headers.txt", "w", encoding="utf-8") as f:
                for k,v in (resp_hdrs or {}).items():
                    f.write(f"{k}: {v}\n")

        if status != 200 or not body:
            print(f"  -> HTTP {status}; empty/failed body. Skipping.", file=sys.stderr)
            time.sleep(opts.delay); continue

        store = extract_store_from_html_or_text(body)
        if not store:
            print("  -> Couldn’t parse JSON/store. Skipping.", file=sys.stderr)
            time.sleep(opts.delay); continue

        stations = extract_epg_stations(store)
        if stations:
            merge(master, stations)
            print(f"  -> EPG stations found: {len(stations)} (total {len(master)})")
        else:
            synth = synthesize_live_events(store, tzinfo)
            if synth:
                merge(master, synth)
                print(f"  -> No EPG group; synthesized {sum(len(s.get('schedule',[])) for s in synth)} live entries across {len(synth)} channels (total {len(master)})")
            else:
                print("  -> No stations or live events found.", file=sys.stderr)

        time.sleep(opts.delay)

    if not master:
        print("No data extracted from provided URLs.", file=sys.stderr)
        return 3

    tv = build_xmltv(list(master.values()), tzinfo, use_names=(opts.channel_id_mode=="name"), slug_ids=opts.slugify_names)
    write_xml_pretty(tv, opts.output)
    print(f"Wrote XMLTV: {opts.output}")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
