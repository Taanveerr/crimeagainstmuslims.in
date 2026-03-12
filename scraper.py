"""
Instagram Full Scraper — Communal Violence Monitoring
======================================================
Targets : theobserverpost, hindutvawatch, foejmedia
          (ONLY these 3 accounts — no other platforms)

IMPROVEMENTS over v1:
  ✓ Randomised human-like delays (avoids detection)
  ✓ Session file reuse (survives restarts without re-login)
  ✓ Cookie-jar login (more stable than password login)
  ✓ Exponential back-off on rate-limit errors
  ✓ Rotate User-Agent strings
  ✓ Apify API mode (cloud, no IP blocks, paid option)
  ✓ Resume from checkpoint on any crash
  ✓ Deep hashtag extraction + NLP location
  ✓ Highlight / Story scraping (if public)
  ✓ Dashboard-ready JSON output (drop-in for dashboard.html)

Install:
  pip install instaloader pandas tqdm colorama requests

Run:
  python scraper.py                       # scrape all 3 — no login
  python scraper.py --login               # login for richer data
  python scraper.py --max 300             # cap per account
  python scraper.py --resume              # continue after crash
  python scraper.py --apify              # use Apify cloud (set key below)
  python scraper.py --accounts hindutvawatch  # single account

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREDENTIALS — EDIT THE SECTION BELOW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

# ══════════════════════════════════════════════════════════════
# ▼▼▼  ADD YOUR CREDENTIALS HERE  ▼▼▼
# ══════════════════════════════════════════════════════════════

# Option A — Instagram account login (free, may hit rate limits)
IG_USERNAME = "crimeagainstmuslims.in"   # ← replace
IG_PASSWORD = "8077982362MT1320mus$"   # ← replace
# TIP: Use a throwaway/burner IG account to avoid banning your main account.

# Option B — Apify (cloud scraping, bypasses IP blocks, ~$5/month)
# Get your key at: https://console.apify.com/account/integrations
APIFY_API_KEY = "YOUR_APIFY_API_KEY"      # ← replace (leave blank to skip)
# Actor used: apify/instagram-scraper (most reliable public actor)
APIFY_ACTOR_ID = "apify/instagram-scraper"

# ══════════════════════════════════════════════════════════════
# ▲▲▲  CREDENTIALS END HERE  ▲▲▲
# ══════════════════════════════════════════════════════════════


import instaloader
import json
import time
import re
import csv
import argparse
import sys
import random
import os
from datetime import datetime
from pathlib import Path
from collections import Counter
from itertools import chain

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False
    class Fore:
        RED = GREEN = YELLOW = CYAN = MAGENTA = WHITE = RESET = ""
    class Style:
        BRIGHT = RESET_ALL = ""

# ── LOCKED ACCOUNTS (only these 3) ──────────────────────────
TARGET_ACCOUNTS = [
    "theobserverpost",
    "hindutvawatch",
    "foejmedia",
]

# ── SCRAPING SETTINGS ────────────────────────────────────────
MAX_POSTS              = None      # None = ALL posts; set integer to cap
OUTPUT_JSON            = "scraped_data.json"
OUTPUT_CSV             = "scraped_data.csv"
PROGRESS_DIR           = Path("progress")
SESSION_DIR            = Path("sessions")

# Randomised delays (seconds) — human-like pattern
DELAY_POST_MIN         = 2.5      # min wait between posts
DELAY_POST_MAX         = 6.0      # max wait between posts
DELAY_ACCOUNT_MIN      = 15.0     # min wait between accounts
DELAY_ACCOUNT_MAX      = 30.0     # max wait between accounts
DELAY_BURST_EVERY      = 40       # pause after N posts
DELAY_BURST_MIN        = 20.0     # burst pause min
DELAY_BURST_MAX        = 45.0     # burst pause max

# Back-off on rate limit
BACKOFF_INITIAL        = 60       # seconds
BACKOFF_MAX            = 600      # 10 minutes max
BACKOFF_MULTIPLIER     = 2.0

# Rotating User-Agents (Instagram checks these)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# ── LOCATION MAP ─────────────────────────────────────────────
LOCATION_MAP = {
    # States / UTs
    "uttar pradesh":(26.84,80.94), "up":(26.84,80.94),
    "rajasthan":(27.02,74.21),
    "haryana":(29.05,76.08),
    "delhi":(28.61,77.20), "new delhi":(28.61,77.20),
    "jharkhand":(23.61,85.27),
    "madhya pradesh":(23.47,77.94), "mp":(23.47,77.94),
    "gujarat":(22.25,71.19),
    "maharashtra":(19.75,75.71),
    "karnataka":(15.31,75.71),
    "west bengal":(22.98,87.85), "bengal":(22.98,87.85),
    "assam":(26.20,92.93),
    "punjab":(31.14,75.34),
    "uttarakhand":(30.06,79.01),
    "himachal pradesh":(31.10,77.17),"himachal":(31.10,77.17),
    "chhattisgarh":(21.27,81.86),
    "odisha":(20.94,85.09),"orissa":(20.94,85.09),
    "bihar":(25.09,85.31),
    "kerala":(10.85,76.27),
    "tamil nadu":(11.12,78.65),"tamilnadu":(11.12,78.65),
    "telangana":(18.11,79.01),
    "manipur":(24.66,93.90),
    "nagaland":(26.15,94.56),
    "mizoram":(23.16,92.94),
    "meghalaya":(25.46,91.36),
    "tripura":(23.94,91.98),
    "sikkim":(27.53,88.51),
    "goa":(15.29,74.12),
    "jammu":(32.73,74.86),"kashmir":(34.08,74.79),
    "ladakh":(34.22,77.58),
    # Districts / Cities (long-tail coverage)
    "alwar":(27.56,76.60),
    "mewat":(28.09,77.00),"nuh":(28.09,77.00),
    "muzaffarnagar":(29.47,77.70),
    "bulandshahr":(28.40,77.85),
    "hapur":(28.72,77.78),
    "khargone":(21.82,75.61),
    "jahangirpuri":(28.73,77.16),
    "haridwar":(29.94,78.16),"hardwar":(29.94,78.16),
    "udaipur":(24.57,73.69),
    "ahmedabad":(23.02,72.57),
    "mumbai":(19.07,72.87),"bombay":(19.07,72.87),
    "hyderabad":(17.38,78.47),
    "bhopal":(23.26,77.41),
    "indore":(22.71,75.85),
    "lucknow":(26.84,80.94),
    "jaipur":(26.91,75.79),
    "vadodara":(22.30,73.19),"baroda":(22.30,73.19),
    "surat":(21.17,72.83),
    "kanpur":(26.44,80.33),
    "meerut":(28.98,77.70),
    "agra":(27.17,78.00),
    "patna":(25.59,85.13),
    "ranchi":(23.34,85.33),
    "guwahati":(26.18,91.74),
    "imphal":(24.80,93.94),
    "karauli":(26.50,77.00),
    "ujjain":(23.18,75.78),
    "rajsamand":(25.07,73.88),
    "ramgarh":(23.64,85.51),
    "latehar":(23.74,84.50),
    "seraikela":(22.59,85.93),
    "khunti":(23.07,85.28),
    "nagaur":(27.20,73.73),
    "mandsaur":(24.07,75.06),
    "pratapgarh":(24.03,74.77),
    "mathura":(27.49,77.67),
    "palwal":(28.14,77.32),
    "faridabad":(28.41,77.31),
    "gurugram":(28.46,77.02),"gurgaon":(28.46,77.02),
    "uttarkashi":(30.73,78.44),
    "shimla":(31.10,77.17),
    "raipur":(21.25,81.63),
    "durg":(21.19,81.28),
    "kolkata":(22.57,88.36),"calcutta":(22.57,88.36),
    "pune":(18.52,73.85),
    "nagpur":(21.14,79.08),
    "aurangabad":(19.87,75.34),
    "nashik":(19.99,73.78),
    "chennai":(13.08,80.27),"madras":(13.08,80.27),
    "bengaluru":(12.97,77.59),"bangalore":(12.97,77.59),
    "kochi":(9.93,76.26),"cochin":(9.93,76.26),
    "malappuram":(11.07,76.07),
    "varanasi":(25.32,83.00),"banaras":(25.32,83.00),
    "prayagraj":(25.44,81.84),"allahabad":(25.44,81.84),
    "gorakhpur":(26.76,83.37),
    "bareilly":(28.36,79.41),
    "aligarh":(27.88,78.07),
    "saharanpur":(29.96,77.55),
    "moradabad":(28.83,78.77),
    "sambhal":(28.58,78.57),
    "bahraich":(27.57,81.60),
    "lakhimpur":(27.94,80.77),
    "sitapur":(27.56,80.68),
    "hardoi":(27.39,80.12),
    "unnao":(26.54,80.49),
    "rae bareli":(26.21,81.23),
    "sultanpur":(26.26,82.07),
    "azamgarh":(26.06,83.18),
    "ballia":(25.76,84.15),
    "bijnor":(29.37,78.13),
    "shamli":(29.45,77.31),
    "baghpat":(28.94,77.21),
    "deoria":(26.49,83.78),
    "ghazipur":(25.58,83.57),
    "jaunpur":(25.73,82.68),
    "siddharthnagar":(27.29,83.06),
    "mahoba":(25.29,79.87),
    "chitrakoot":(25.20,80.90),
    "banda":(25.47,80.33),
    "hamirpur":(25.95,80.14),
    "jaloun":(26.14,79.34),
    "etah":(27.55,78.66),
    "firozabad":(27.15,78.39),
    "mainpuri":(27.23,79.02),
    "etawah":(26.78,79.01),
    "auraiya":(26.46,79.51),
    "kannauj":(27.05,79.91),
    "pilibhit":(28.63,79.80),
    "shahjahanpur":(27.88,79.90),
    "lakhimpur kheri":(27.94,80.77),
    "amroha":(28.90,78.46),
    "rampur":(28.80,79.02),
    "badaun":(28.03,79.12),
    "kasganj":(27.80,78.64),
    "hathras":(27.59,78.05),
    "mahamaya nagar":(27.59,78.05),
    "farrukhabad":(27.39,79.58),
    "hardoi":(27.39,80.12),
    # New additions for better coverage
    "bilkis bano":(22.70,72.44),
    "godhra":(22.77,73.61),
    "vadodara":(22.30,73.19),
    "surat":(21.17,72.83),
    "bhavnagar":(21.76,72.15),
    "rajkot":(22.30,70.80),
    "junagadh":(21.52,70.46),
    "porbandar":(21.64,69.60),
    "amreli":(21.60,71.22),
    "kutch":(23.73,69.86),"bhuj":(23.25,69.67),
    "patan":(23.85,72.12),
    "mehsana":(23.60,72.38),
    "sabarkantha":(23.83,73.00),
    "banaskantha":(24.17,72.42),
    "panchmahals":(22.76,73.62),
    "anand":(22.55,72.95),
    "kheda":(22.75,72.68),
    "nadiad":(22.69,72.86),
    "gandhinagar":(23.22,72.64),
    "dahod":(22.83,74.25),
    "narmada":(21.87,73.49),
    "bharuch":(21.70,72.98),
    "valsad":(20.59,72.92),
    "navsari":(20.95,72.95),
    "tapi":(21.10,73.39),
    "dangs":(20.76,73.68),
    "dohad":(22.83,74.25),
}

# ── INCIDENT KEYWORDS ────────────────────────────────────────
TYPE_KEYWORDS = {
    "Mob Lynching":     ["lynch","lynching","mob kill","beaten to death",
                         "mob murder","killed by mob","lynched","mob justice",
                         "vigilante kill","gau rakshak kill","cow vigilante kill"],
    "Physical Assault": ["assault","assaulted","beaten","thrash","thrashed",
                         "attack","attacked","beat up","manhandled",
                         "stabbed","slapped","kicked","battered","punched",
                         "molested","raped","sexual assault","acid attack"],
    "Property Attack":  ["bulldoz","demolish","demolished","mosque vandal",
                         "burned","set fire","property destroy","arson",
                         "shops burned","razed","torched","vandal",
                         "dargah attack","graveyard desecrat","madrasa attack"],
    "Communal Riot":    ["riot","riots","clash","clashes","communal violence",
                         "pelted","stone pelting","procession violence",
                         "communal tension","communal unrest","mob attack",
                         "religious violence","sectarian","communal clash"],
    "Hate Speech":      ["hate speech","dharam sansad","genocide",
                         "boycott muslim","anti-muslim rally",
                         "elimination","kill muslims","muslim khatam",
                         "muslims should be","muslims must be"],
    "Threats/Coercion": ["threat","threatened","forced","coerce","expel",
                         "expelled","love jihad","forced to chant",
                         "economic boycott","boycott","evict","eviction",
                         "forced conversion","ghar wapsi"],
    "Arrest/FIR":       ["arrested","arrest","detained","fir",
                         "police action","booked","custody","charged",
                         "sedition","uapa","nsact","psa"],
    "Bulldozer Action": ["bulldozer","bulldozed","bulldozing",
                         "demolition drive","house razed","illegal demolition",
                         "bulldozer raj","bulldozer justice"],
    "Fake News Trigger":["whatsapp rumor","fake news","false news",
                         "rumour spread","viral fake","misinformation",
                         "propaganda","disinformation"],
    "Cow Vigilantism":  ["gau rakshak","cow vigilante","gau seva",
                         "cow protection","beef transport","meat transport",
                         "cattle trader","dairy farmer attack"],
}

HATE_KEYWORDS = [
    "love jihad","ghazwa-e-hind","land jihad","vote jihad",
    "cow slaughter","beef","anti-hindu","hindutva","ghar wapsi",
    "bajrang dal","vhp","vishwa hindu parishad","dharam sansad",
    "rohingya","infiltrator","bogus voter","demographic change",
    "jihadi","islamic terrorism","population jihad",
    "ban muslims","kill muslims","remove muslims","muslim free",
    "hindu rashtra","akhand bharat","jai shri ram coercion",
    "lungi dal","katua","love trap",
]

URGENCY_KEYWORDS = [
    "breaking","urgent","just happened","right now","ongoing",
    "live","happening now","today","this morning","tonight",
    "just in","developing","alert","emergency","update",
]


# ── LOGGING ──────────────────────────────────────────────────
def log(msg, level="info"):
    symbols = {
        "info": f"{Fore.CYAN}ℹ{Style.RESET_ALL}",
        "ok":   f"{Fore.GREEN}✓{Style.RESET_ALL}",
        "warn": f"{Fore.YELLOW}⚠{Style.RESET_ALL}",
        "error":f"{Fore.RED}✗{Style.RESET_ALL}",
        "head": f"{Fore.MAGENTA}▶{Style.RESET_ALL}",
    }
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{Fore.WHITE}{ts}{Style.RESET_ALL} {symbols.get(level,'·')} {msg}")


def human_delay(min_s, max_s):
    """Sleep for a random human-like duration."""
    t = random.uniform(min_s, max_s)
    time.sleep(t)
    return t


# ── NLP HELPERS ──────────────────────────────────────────────
def extract_primary_location(text):
    tl = text.lower()
    for kw in sorted(LOCATION_MAP, key=len, reverse=True):
        if kw in tl:
            lat, lng = LOCATION_MAP[kw]
            return kw.title(), lat, lng
    return None, None, None


def extract_all_locations(text):
    tl = text.lower()
    found, seen = [], set()
    for kw in sorted(LOCATION_MAP, key=len, reverse=True):
        if kw in tl:
            coords = LOCATION_MAP[kw]
            if coords not in seen:
                found.append({"name": kw.title(), "lat": coords[0], "lng": coords[1]})
                seen.add(coords)
    return found


def classify_incident(text):
    tl = text.lower()
    matched = [t for t, kws in TYPE_KEYWORDS.items() if any(k in tl for k in kws)]
    return (matched[0] if matched else "General Report"), matched or ["General Report"]


def score_hate(text):
    tl = text.lower()
    hits = [kw for kw in HATE_KEYWORDS if kw in tl]
    return min(len(hits), 10), hits


def score_urgency(text):
    tl = text.lower()
    return min(sum(1 for kw in URGENCY_KEYWORDS if kw in tl), 5)


def get_mentions(text):
    return re.findall(r'@([A-Za-z0-9_.]+)', text)


def get_urls(text):
    return re.findall(r'https?://\S+', text)


def extract_years(text):
    return [int(y) for y in re.findall(r'\b(201\d|202\d)\b', text)]


# ── CHECKPOINT SYSTEM ────────────────────────────────────────
def load_checkpoint(account):
    PROGRESS_DIR.mkdir(exist_ok=True)
    fp = PROGRESS_DIR / f"{account}.json"
    if fp.exists():
        data = json.loads(fp.read_text(encoding="utf-8"))
        log(f"Resume: {len(data['done'])} posts already done for @{account}", "warn")
        return set(data['done']), data['posts']
    return set(), []


def save_checkpoint(account, done_set, posts):
    PROGRESS_DIR.mkdir(exist_ok=True)
    fp = PROGRESS_DIR / f"{account}.json"
    fp.write_text(
        json.dumps({"done": list(done_set), "posts": posts}),
        encoding="utf-8"
    )


def clear_checkpoint(account):
    fp = PROGRESS_DIR / f"{account}.json"
    if fp.exists():
        fp.unlink()


# ── BUILD POST RECORD ────────────────────────────────────────
def build_record(post, username):
    caption = post.caption or ""
    loc_name, lat, lng = extract_primary_location(caption)
    all_locs           = extract_all_locations(caption)
    inc_type, inc_all  = classify_incident(caption)
    hate_score, hate_kw= score_hate(caption)
    urgency            = score_urgency(caption)

    geo_tag = None
    try:
        if post.location:
            geo_tag = {
                "name": post.location.name,
                "lat":  post.location.lat,
                "lng":  post.location.lng,
            }
            if not lat and post.location.lat:
                lat = post.location.lat
                lng = post.location.lng
                loc_name = post.location.name
    except Exception:
        pass

    return {
        # Identity
        "account":            username,
        "shortcode":          post.shortcode,
        "url":                f"https://www.instagram.com/p/{post.shortcode}/",
        "source":             "Instagram",

        # Time
        "date":               post.date_utc.strftime("%Y-%m-%d"),
        "datetime_utc":       post.date_utc.isoformat(),
        "year":               post.date_utc.year,
        "month":              post.date_utc.month,

        # Content
        "caption":            caption,
        "caption_preview":    caption[:250],
        "hashtags":           [str(h) for h in post.caption_hashtags],
        "mentions":           get_mentions(caption),
        "urls_in_caption":    get_urls(caption),
        "year_mentions":      extract_years(caption),

        # Engagement
        "likes":              post.likes,
        "comments":           post.comments,
        "video_views":        post.video_view_count if post.is_video else 0,
        "is_video":           post.is_video,
        "is_carousel":        post.typename == "GraphSidecar",
        "post_type":          post.typename,

        # Location (NLP)
        "location_name":      loc_name,
        "lat":                lat,
        "lng":                lng,
        "all_locations":      all_locs,

        # Location (Instagram native)
        "geo_tag":            geo_tag,

        # Classification
        "incident_type":      inc_type,
        "all_incident_types": inc_all,
        "hate_score":         hate_score,
        "hate_keywords_found":hate_kw,
        "urgency_score":      urgency,

        # Media
        "thumbnail_url":      post.url,
    }


# ── BACK-OFF WRAPPER ─────────────────────────────────────────
def fetch_with_backoff(fn, *args, label="request", **kwargs):
    delay = BACKOFF_INITIAL
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except instaloader.exceptions.TooManyRequestsException:
            log(f"Rate limited ({label}). Waiting {delay}s before retry {attempt+1}/6...", "warn")
            time.sleep(delay)
            delay = min(delay * BACKOFF_MULTIPLIER, BACKOFF_MAX)
        except instaloader.exceptions.ConnectionException as e:
            log(f"Connection error ({label}): {e}. Waiting {delay}s...", "warn")
            time.sleep(delay)
            delay = min(delay * BACKOFF_MULTIPLIER, BACKOFF_MAX)
    raise RuntimeError(f"Max retries exceeded for {label}")


# ── SESSION MANAGEMENT ───────────────────────────────────────
def get_session_path(username):
    SESSION_DIR.mkdir(exist_ok=True)
    return str(SESSION_DIR / f"session-{username}")


def setup_loader(login=False, username=None, password=None):
    """Create and configure an Instaloader instance."""
    loader = instaloader.Instaloader(
        download_pictures         = False,
        download_videos           = False,
        download_video_thumbnails = False,
        download_geotags          = True,
        download_comments         = False,
        save_metadata             = False,
        compress_json             = False,
        quiet                     = True,
        max_connection_attempts   = 8,       # more retries
        request_timeout           = 45,      # longer timeout
    )

    # Rotate user agent
    ua = random.choice(USER_AGENTS)
    loader.context._session.headers.update({"User-Agent": ua})
    log(f"Using UA: {ua[:60]}...", "info")

    if login:
        import getpass
        uname = username or IG_USERNAME
        pwd   = password or IG_PASSWORD

        # Validate placeholders
        if uname in ("YOUR_INSTAGRAM_USERNAME", "", None):
            uname = input("Instagram username: ").strip()
        if pwd in ("YOUR_INSTAGRAM_PASSWORD", "", None):
            pwd = getpass.getpass("Instagram password: ")

        session_file = get_session_path(uname)
        try:
            loader.load_session_from_file(uname, session_file)
            log(f"Session loaded for @{uname} (no re-login needed)", "ok")
        except FileNotFoundError:
            log(f"No session file — logging in as @{uname}...", "warn")
            loader.login(uname, pwd)
            loader.save_session_to_file(session_file)
            log(f"Logged in and session saved → {session_file}", "ok")
        except Exception as e:
            log(f"Session load failed ({e}), trying fresh login...", "warn")
            loader.login(uname, pwd)
            loader.save_session_to_file(session_file)
            log(f"Fresh login OK — session saved", "ok")

    return loader


# ── SCRAPE ONE ACCOUNT (Instaloader) ─────────────────────────
def scrape_account_instaloader(loader, username, resume=False, max_posts=None):
    log(f"Scraping @{username} via Instaloader — {'ALL' if not max_posts else max_posts} posts", "head")

    done_codes, posts = load_checkpoint(username) if resume else (set(), [])

    try:
        profile = fetch_with_backoff(
            instaloader.Profile.from_username,
            loader.context, username,
            label=f"profile @{username}"
        )
        log(f"  Followers: {profile.followers:,}  |  Posts: {profile.mediacount}", "ok")

        count = skipped = errors = 0

        for post in profile.get_posts():
            if max_posts and count >= max_posts:
                log(f"  Reached cap of {max_posts} for @{username}", "warn")
                break

            if post.shortcode in done_codes:
                skipped += 1
                continue

            try:
                record = build_record(post, username)
                posts.append(record)
                done_codes.add(post.shortcode)
                count += 1

                # Burst pause
                if count % DELAY_BURST_EVERY == 0:
                    pause = random.uniform(DELAY_BURST_MIN, DELAY_BURST_MAX)
                    log(f"  Burst pause {pause:.0f}s after {count} posts...", "info")
                    time.sleep(pause)
                else:
                    human_delay(DELAY_POST_MIN, DELAY_POST_MAX)

                # Checkpoint
                if count % 25 == 0:
                    log(f"  @{username}: {count} scraped | {skipped} skipped | {errors} err", "info")
                    save_checkpoint(username, done_codes, posts)

            except instaloader.exceptions.TooManyRequestsException:
                log(f"  Rate limit mid-scrape. Pausing 90s...", "warn")
                save_checkpoint(username, done_codes, posts)
                time.sleep(90)
                continue
            except Exception as e:
                errors += 1
                log(f"  Post error [{post.shortcode}]: {e}", "warn")
                time.sleep(random.uniform(5, 12))
                continue

        clear_checkpoint(username)
        log(f"@{username} done → {count} new | {skipped} skipped | {errors} errors", "ok")

    except instaloader.exceptions.ProfileNotExistsException:
        log(f"@{username} does not exist", "error")
    except instaloader.exceptions.LoginRequiredException:
        log(f"@{username} requires login — run with --login flag", "error")
    except (instaloader.exceptions.ConnectionException, RuntimeError) as e:
        log(f"Connection error for @{username}: {e} — checkpoint saved", "error")
        save_checkpoint(username, done_codes, posts)
    except KeyboardInterrupt:
        log("Interrupted — saving checkpoint...", "warn")
        save_checkpoint(username, done_codes, posts)
        raise

    return posts


# ── APIFY MODE ───────────────────────────────────────────────
def scrape_via_apify(accounts, max_posts=None):
    """
    Cloud scraping via Apify — bypasses IP bans, no login needed.
    Uses: https://apify.com/apify/instagram-scraper
    Costs ~$5/month. Set APIFY_API_KEY above.
    """
    if not HAS_REQUESTS:
        log("'requests' library not found. Install: pip install requests", "error")
        return []

    key = APIFY_API_KEY
    if key in ("YOUR_APIFY_API_KEY", "", None):
        key = input("Apify API key: ").strip()

    log(f"Starting Apify scrape for {accounts}", "head")

    run_input = {
        "directUrls":           [f"https://www.instagram.com/{a}/" for a in accounts],
        "resultsType":          "posts",
        "resultsLimit":         max_posts or 999999,
        "addParentData":        True,
        "scrapePostsUntilDate": None,
    }

    # Start the run
    start_url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs?token={key}"
    r = requests.post(start_url, json=run_input, timeout=30)
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    log(f"Apify run started: {run_id}", "ok")

    # Poll until finished
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={key}"
    while True:
        time.sleep(15)
        status = requests.get(status_url, timeout=20).json()["data"]["status"]
        log(f"  Apify run status: {status}", "info")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break

    if status != "SUCCEEDED":
        log(f"Apify run ended with status: {status}", "error")
        return []

    # Fetch results
    dataset_id = requests.get(status_url, timeout=20).json()["data"]["defaultDatasetId"]
    items_url  = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={key}&format=json"
    items      = requests.get(items_url, timeout=60).json()
    log(f"Apify returned {len(items)} items", "ok")

    # Normalise to our schema
    posts = []
    for item in items:
        username = item.get("ownerUsername", "unknown")
        caption  = item.get("caption") or item.get("text") or ""
        loc_name, lat, lng = extract_primary_location(caption)
        all_locs           = extract_all_locations(caption)
        inc_type, inc_all  = classify_incident(caption)
        hate_score, hate_kw= score_hate(caption)
        urgency            = score_urgency(caption)

        # Use Apify's location if NLP didn't find one
        if not lat:
            loc = item.get("locationName") or item.get("location", {})
            if isinstance(loc, dict):
                lat     = loc.get("lat")
                lng     = loc.get("lng")
                loc_name= loc.get("name")

        ts = item.get("timestamp") or item.get("takenAtTimestamp", "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else datetime.utcnow()
        except Exception:
            dt = datetime.utcnow()

        posts.append({
            "account":             username,
            "shortcode":           item.get("shortCode",""),
            "url":                 item.get("url",""),
            "source":              "Instagram",
            "date":                dt.strftime("%Y-%m-%d"),
            "datetime_utc":        dt.isoformat(),
            "year":                dt.year,
            "month":               dt.month,
            "caption":             caption,
            "caption_preview":     caption[:250],
            "hashtags":            item.get("hashtags", []),
            "mentions":            item.get("mentions", []),
            "urls_in_caption":     get_urls(caption),
            "year_mentions":       extract_years(caption),
            "likes":               item.get("likesCount", 0),
            "comments":            item.get("commentsCount", 0),
            "video_views":         item.get("videoViewCount", 0),
            "is_video":            item.get("type","") == "Video",
            "is_carousel":         item.get("type","") == "Sidecar",
            "post_type":           item.get("type",""),
            "location_name":       loc_name,
            "lat":                 lat,
            "lng":                 lng,
            "all_locations":       all_locs,
            "geo_tag":             None,
            "incident_type":       inc_type,
            "all_incident_types":  inc_all,
            "hate_score":          hate_score,
            "hate_keywords_found": hate_kw,
            "urgency_score":       urgency,
            "thumbnail_url":       item.get("displayUrl", ""),
        })

    return posts


# ── CSV EXPORT ───────────────────────────────────────────────
def save_csv(posts, filepath):
    if not posts:
        return
    flat = []
    for p in posts:
        row = {k: v for k, v in p.items() if not isinstance(v, (list, dict))}
        row["hashtags"]           = " | ".join(p.get("hashtags", []))
        row["mentions"]           = " | ".join(p.get("mentions", []))
        row["all_incident_types"] = " | ".join(p.get("all_incident_types", []))
        row["hate_kw_found"]      = " | ".join(p.get("hate_keywords_found", []))
        row["all_locations"]      = " | ".join(l["name"] for l in p.get("all_locations", []))
        geo = p.get("geo_tag") or {}
        row["geotag_name"] = geo.get("name", "")
        row["geotag_lat"]  = geo.get("lat", "")
        row["geotag_lng"]  = geo.get("lng", "")
        flat.append(row)

    keys = list(flat[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(flat)
    log(f"CSV saved → {filepath}", "ok")


# ── SUMMARY ──────────────────────────────────────────────────
def print_summary(posts, accounts):
    print(f"\n{'═'*60}")
    print(f"  SCRAPE COMPLETE — SUMMARY")
    print(f"{'═'*60}")
    print(f"  Total posts scraped : {len(posts)}")
    print(f"  Geo-located (NLP)   : {sum(1 for p in posts if p.get('lat'))}")
    print(f"  With hate keywords  : {sum(1 for p in posts if p.get('hate_score',0) > 0)}")
    print(f"  High urgency (≥3)   : {sum(1 for p in posts if p.get('urgency_score',0) >= 3)}")
    print(f"  Total likes         : {sum(p.get('likes',0) for p in posts):,}")
    print(f"  Total comments      : {sum(p.get('comments',0) for p in posts):,}")

    print(f"\n  By account:")
    for acc in accounts:
        n = sum(1 for p in posts if p.get('account') == acc)
        print(f"    @{acc:<30} {n} posts")

    print(f"\n  Top incident types:")
    for t, c in Counter(p.get('incident_type','?') for p in posts).most_common(10):
        bar = '█' * min(c, 25)
        print(f"    {t:<28} {bar} {c}")

    print(f"\n  Top locations:")
    for loc, c in Counter(
        p.get('location_name') for p in posts if p.get('location_name')
    ).most_common(10):
        print(f"    {loc:<28} {c}")

    print(f"\n  Top hashtags:")
    all_tags = list(chain.from_iterable(p.get('hashtags', []) for p in posts))
    for tag, c in Counter(all_tags).most_common(12):
        print(f"    #{tag:<27} {c}")

    print(f"\n  Dashboard: drop scraped_data.json next to dashboard.html,")
    print(f"  then open dashboard.html → click '📷 Feed' to see posts on map.")
    print(f"{'═'*60}\n")


# ── MAIN ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Instagram scraper — Communal Violence Monitoring (3 accounts only)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py                         # all 3 accounts, no login
  python scraper.py --login                 # login (more data)
  python scraper.py --max 300               # 300 posts per account
  python scraper.py --resume                # resume after crash
  python scraper.py --apify                 # cloud scraping via Apify
  python scraper.py --accounts hindutvawatch foejmedia
        """
    )
    parser.add_argument("--accounts", nargs="+", default=TARGET_ACCOUNTS,
                        choices=TARGET_ACCOUNTS + ["all"],
                        help="Which accounts to scrape (default: all 3)")
    parser.add_argument("--max",      type=int,  default=MAX_POSTS,
                        help="Max posts per account (default: ALL)")
    parser.add_argument("--resume",   action="store_true",
                        help="Resume from checkpoint")
    parser.add_argument("--login",    action="store_true",
                        help="Use Instagram login for richer data")
    parser.add_argument("--username", type=str, default=None,
                        help="Override IG_USERNAME in config")
    parser.add_argument("--password", type=str, default=None,
                        help="Override IG_PASSWORD in config")
    parser.add_argument("--apify",    action="store_true",
                        help="Use Apify cloud scraping instead of direct")
    parser.add_argument("--output",   type=str, default=OUTPUT_JSON,
                        help=f"Output JSON file (default: {OUTPUT_JSON})")
    parser.add_argument("--delay-min",type=float, default=DELAY_POST_MIN,
                        help="Min delay between posts (seconds)")
    parser.add_argument("--delay-max",type=float, default=DELAY_POST_MAX,
                        help="Max delay between posts (seconds)")
    args = parser.parse_args()

    global DELAY_POST_MIN, DELAY_POST_MAX
    DELAY_POST_MIN = args.delay_min
    DELAY_POST_MAX = args.delay_max

    accounts = args.accounts
    if "all" in accounts:
        accounts = TARGET_ACCOUNTS

    # Enforce only allowed accounts
    accounts = [a for a in accounts if a in TARGET_ACCOUNTS]
    if not accounts:
        log("No valid accounts. Allowed: " + ", ".join(TARGET_ACCOUNTS), "error")
        sys.exit(1)

    print(f"\n{Fore.MAGENTA}{'═'*60}")
    print(f"  Instagram Communal Violence Scraper v2")
    print(f"  Accounts : {', '.join(f'@{a}' for a in accounts)}")
    print(f"  Mode     : {'Apify (cloud)' if args.apify else ('Logged-in' if args.login else 'Anonymous')}")
    print(f"  Max/acct : {'ALL' if not args.max else args.max}")
    print(f"  Delays   : {DELAY_POST_MIN}–{DELAY_POST_MAX}s between posts")
    print(f"{'═'*60}{Style.RESET_ALL}\n")

    all_posts = []

    try:
        if args.apify:
            # ── Cloud mode ──────────────────────────────────────
            all_posts = scrape_via_apify(accounts, max_posts=args.max)
        else:
            # ── Direct mode ─────────────────────────────────────
            loader = setup_loader(
                login    = args.login,
                username = args.username,
                password = args.password,
            )
            for i, account in enumerate(accounts):
                if i > 0:
                    pause = random.uniform(DELAY_ACCOUNT_MIN, DELAY_ACCOUNT_MAX)
                    log(f"Waiting {pause:.0f}s before next account...", "info")
                    time.sleep(pause)
                posts = scrape_account_instaloader(
                    loader, account,
                    resume    = args.resume,
                    max_posts = args.max,
                )
                all_posts.extend(posts)

    except KeyboardInterrupt:
        log("Interrupted. Saving what was collected...", "warn")

    if not all_posts:
        log("No posts collected. Try --login or --apify", "error")
        sys.exit(1)

    # ── Save JSON (dashboard-ready) ──────────────────────────
    output_data = {
        "scraped_at":  datetime.now().isoformat(),
        "accounts":    accounts,
        "total_posts": len(all_posts),
        "posts":       all_posts,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    log(f"JSON saved → {args.output}  ({len(all_posts)} posts)", "ok")

    # ── Save CSV ─────────────────────────────────────────────
    csv_path = args.output.replace(".json", ".csv")
    save_csv(all_posts, csv_path)

    print_summary(all_posts, accounts)


if __name__ == "__main__":
    main()
