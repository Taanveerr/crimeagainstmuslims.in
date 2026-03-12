"""
Instagram Full Scraper — Communal Violence Monitoring
======================================================
Targets : theobserverpost, hindutvawatch, foejmedia
Scrapes : ALL posts (no limit), captions, likes, comments,
          hashtags, mentions, location tags, media type,
          timestamps, story highlights (if public)

Requires: pip install instaloader pandas tqdm colorama

Run     : python scraper.py
          python scraper.py --accounts hindutvawatch foejmedia
          python scraper.py --resume          (continue interrupted scrape)
          python scraper.py --login           (use login for more data)
          python scraper.py --max 500         (cap at 500 posts per account)

Output  : scraped_data.json  → load into dashboard
          scraped_data.csv   → open in Excel
          progress/          → auto-resume checkpoint files
"""

import instaloader
import json
import time
import re
import csv
import argparse
import sys
from datetime import datetime
from pathlib import Path
from collections import Counter
from itertools import chain

# ── Optional libs ────────────────────────────────────────────
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

# ── CONFIG ───────────────────────────────────────────────────
DEFAULT_ACCOUNTS = [
    "theobserverpost",
    "hindutvawatch",
    "foejmedia",
]

MAX_POSTS              = None   # None = ALL posts; set e.g. 500 to cap
OUTPUT_JSON            = "scraped_data.json"
OUTPUT_CSV             = "scraped_data.csv"
PROGRESS_DIR           = Path("progress")
DELAY_BETWEEN_POSTS    = 2.0    # seconds (avoid Instagram rate limit)
DELAY_BETWEEN_ACCOUNTS = 12.0   # seconds between accounts


# ── LOCATION MAP ─────────────────────────────────────────────
LOCATION_MAP = {
    # States / UTs
    "uttar pradesh": (26.84, 80.94), "up": (26.84, 80.94),
    "rajasthan": (27.02, 74.21),
    "haryana": (29.05, 76.08),
    "delhi": (28.61, 77.20), "new delhi": (28.61, 77.20),
    "jharkhand": (23.61, 85.27),
    "madhya pradesh": (23.47, 77.94), "mp": (23.47, 77.94),
    "gujarat": (22.25, 71.19),
    "maharashtra": (19.75, 75.71),
    "karnataka": (15.31, 75.71),
    "west bengal": (22.98, 87.85), "bengal": (22.98, 87.85),
    "assam": (26.20, 92.93),
    "punjab": (31.14, 75.34),
    "uttarakhand": (30.06, 79.01),
    "himachal pradesh": (31.10, 77.17), "himachal": (31.10, 77.17),
    "chhattisgarh": (21.27, 81.86),
    "odisha": (20.94, 85.09), "orissa": (20.94, 85.09),
    "bihar": (25.09, 85.31),
    "kerala": (10.85, 76.27),
    "tamil nadu": (11.12, 78.65), "tamilnadu": (11.12, 78.65),
    "telangana": (18.11, 79.01),
    "manipur": (24.66, 93.90),
    "nagaland": (26.15, 94.56),
    "mizoram": (23.16, 92.94),
    "meghalaya": (25.46, 91.36),
    "tripura": (23.94, 91.98),
    "sikkim": (27.53, 88.51),
    "goa": (15.29, 74.12),
    "jammu": (32.73, 74.86), "kashmir": (34.08, 74.79),
    "ladakh": (34.22, 77.58),
    # Cities
    "alwar": (27.56, 76.60),
    "mewat": (28.09, 77.00), "nuh": (28.09, 77.00),
    "muzaffarnagar": (29.47, 77.70),
    "bulandshahr": (28.40, 77.85),
    "hapur": (28.72, 77.78),
    "khargone": (21.82, 75.61),
    "jahangirpuri": (28.73, 77.16),
    "haridwar": (29.94, 78.16), "hardwar": (29.94, 78.16),
    "udaipur": (24.57, 73.69),
    "ahmedabad": (23.02, 72.57),
    "mumbai": (19.07, 72.87), "bombay": (19.07, 72.87),
    "hyderabad": (17.38, 78.47),
    "bhopal": (23.26, 77.41),
    "indore": (22.71, 75.85),
    "lucknow": (26.84, 80.94),
    "jaipur": (26.91, 75.79),
    "vadodara": (22.30, 73.19), "baroda": (22.30, 73.19),
    "surat": (21.17, 72.83),
    "kanpur": (26.44, 80.33),
    "meerut": (28.98, 77.70),
    "agra": (27.17, 78.00),
    "patna": (25.59, 85.13),
    "ranchi": (23.34, 85.33),
    "guwahati": (26.18, 91.74),
    "imphal": (24.80, 93.94),
    "karauli": (26.50, 77.00),
    "ujjain": (23.18, 75.78),
    "rajsamand": (25.07, 73.88),
    "ramgarh": (23.64, 85.51),
    "latehar": (23.74, 84.50),
    "seraikela": (22.59, 85.93),
    "khunti": (23.07, 85.28),
    "nagaur": (27.20, 73.73),
    "mandsaur": (24.07, 75.06),
    "pratapgarh": (24.03, 74.77),
    "mathura": (27.49, 77.67),
    "palwal": (28.14, 77.32),
    "faridabad": (28.41, 77.31),
    "gurugram": (28.46, 77.02), "gurgaon": (28.46, 77.02),
    "uttarkashi": (30.73, 78.44),
    "shimla": (31.10, 77.17),
    "raipur": (21.25, 81.63),
    "durg": (21.19, 81.28),
    "kolkata": (22.57, 88.36), "calcutta": (22.57, 88.36),
    "pune": (18.52, 73.85),
    "nagpur": (21.14, 79.08),
    "aurangabad": (19.87, 75.34),
    "nashik": (19.99, 73.78),
    "chennai": (13.08, 80.27), "madras": (13.08, 80.27),
    "bengaluru": (12.97, 77.59), "bangalore": (12.97, 77.59),
    "kochi": (9.93, 76.26), "cochin": (9.93, 76.26),
    "malappuram": (11.07, 76.07),
    "varanasi": (25.32, 83.00), "banaras": (25.32, 83.00),
    "prayagraj": (25.44, 81.84), "allahabad": (25.44, 81.84),
    "gorakhpur": (26.76, 83.37),
    "bareilly": (28.36, 79.41),
    "aligarh": (27.88, 78.07),
    "saharanpur": (29.96, 77.55),
    "moradabad": (28.83, 78.77),
    "sambhal": (28.58, 78.57),
    "bahraich": (27.57, 81.60),
    "sambhal": (28.58, 78.57),
}

# ── INCIDENT TYPES ───────────────────────────────────────────
TYPE_KEYWORDS = {
    "Mob Lynching":     ["lynch", "lynching", "mob kill", "beaten to death",
                         "mob murder", "killed by mob", "lynched"],
    "Physical Assault": ["assault", "assaulted", "beaten", "thrash", "thrashed",
                         "attack", "attacked", "beat up", "manhandled",
                         "stabbed", "slapped", "kicked", "battered"],
    "Property Attack":  ["bulldoz", "demolish", "demolished", "mosque vandal",
                         "burned", "set fire", "property destroy", "arson",
                         "shops burned", "razed", "torched", "vandal"],
    "Communal Riot":    ["riot", "riots", "clash", "clashes", "communal violence",
                         "pelted", "stone pelting", "procession violence",
                         "communal tension", "communal unrest"],
    "Hate Speech":      ["hate speech", "dharam sansad", "genocide",
                         "boycott muslim", "anti-muslim rally",
                         "elimination", "kill muslims"],
    "Threats/Coercion": ["threat", "threatened", "forced", "coerce", "expel",
                         "expelled", "love jihad", "forced to chant",
                         "economic boycott"],
    "Arrest/FIR":       ["arrested", "arrest", "detained", "fir",
                         "police action", "booked", "custody", "charged"],
    "Bulldozer Action": ["bulldozer", "bulldozed", "bulldozing",
                         "demolition drive", "house razed", "illegal demolition"],
    "Fake News Trigger":["whatsapp rumor", "fake news", "false news",
                         "rumour spread", "viral fake", "misinformation"],
}

# ── HATE KEYWORDS ────────────────────────────────────────────
HATE_KEYWORDS = [
    "love jihad", "ghazwa-e-hind", "land jihad", "vote jihad",
    "cow slaughter", "beef", "anti-hindu", "hindutva", "ghar wapsi",
    "bajrang dal", "vhp", "vishwa hindu parishad", "dharam sansad",
    "rohingya", "infiltrator", "bogus voter", "demographic change",
    "jihadi", "islamic terrorism", "population jihad",
    "ban muslims", "kill muslims", "remove muslims",
]

URGENCY_KEYWORDS = [
    "breaking", "urgent", "just happened", "right now", "ongoing",
    "live", "happening now", "today", "this morning", "tonight",
]


# ── HELPER FUNCTIONS ─────────────────────────────────────────
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


def extract_primary_location(text):
    text_lower = text.lower()
    for kw in sorted(LOCATION_MAP, key=len, reverse=True):
        if kw in text_lower:
            lat, lng = LOCATION_MAP[kw]
            return kw.title(), lat, lng
    return None, None, None


def extract_all_locations(text):
    text_lower = text.lower()
    found, seen = [], set()
    for kw in sorted(LOCATION_MAP, key=len, reverse=True):
        if kw in text_lower:
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


# ── CHECKPOINT (RESUME) ──────────────────────────────────────
def load_checkpoint(account):
    PROGRESS_DIR.mkdir(exist_ok=True)
    fp = PROGRESS_DIR / f"{account}.json"
    if fp.exists():
        data = json.loads(fp.read_text())
        log(f"Resume: {len(data['done'])} posts already done for @{account}", "warn")
        return set(data['done']), data['posts']
    return set(), []


def save_checkpoint(account, done_set, posts):
    PROGRESS_DIR.mkdir(exist_ok=True)
    fp = PROGRESS_DIR / f"{account}.json"
    fp.write_text(json.dumps({"done": list(done_set), "posts": posts}))


def clear_checkpoint(account):
    fp = PROGRESS_DIR / f"{account}.json"
    if fp.exists():
        fp.unlink()


# ── SCRAPE ONE ACCOUNT ───────────────────────────────────────
def scrape_account(loader, username, resume=False, max_posts=None):
    log(f"Scraping @{username} — {'ALL' if not max_posts else max_posts} posts", "head")

    done_codes, posts = load_checkpoint(username) if resume else (set(), [])

    try:
        profile = instaloader.Profile.from_username(loader.context, username)
        log(f"  Followers: {profile.followers:,}  |  Posts available: {profile.mediacount}", "ok")

        count = skipped = errors = 0

        for post in profile.get_posts():

            # Stop at max
            if max_posts and count >= max_posts:
                log(f"  Reached cap of {max_posts} posts for @{username}", "warn")
                break

            # Skip already scraped in resume mode
            if post.shortcode in done_codes:
                skipped += 1
                continue

            try:
                caption = post.caption or ""
                loc_name, lat, lng = extract_primary_location(caption)
                all_locs           = extract_all_locations(caption)
                inc_type, inc_all  = classify_incident(caption)
                hate_score, hate_kw= score_hate(caption)
                urgency            = score_urgency(caption)

                # Instagram native geotag
                geo_tag = None
                try:
                    if post.location:
                        geo_tag = {
                            "name": post.location.name,
                            "lat":  post.location.lat,
                            "lng":  post.location.lng,
                        }
                        # Use native geotag coordinates if NLP found nothing
                        if not lat and post.location.lat:
                            lat = post.location.lat
                            lng = post.location.lng
                            loc_name = post.location.name
                except Exception:
                    pass

                record = {
                    # Identity
                    "account":          username,
                    "shortcode":        post.shortcode,
                    "url":              f"https://www.instagram.com/p/{post.shortcode}/",

                    # Time
                    "date":             post.date_utc.strftime("%Y-%m-%d"),
                    "datetime_utc":     post.date_utc.isoformat(),
                    "year":             post.date_utc.year,
                    "month":            post.date_utc.month,

                    # Content
                    "caption":          caption,          # FULL caption, no truncation
                    "caption_preview":  caption[:200],
                    "hashtags":         [str(h) for h in post.caption_hashtags],
                    "mentions":         get_mentions(caption),
                    "urls_in_caption":  get_urls(caption),
                    "year_mentions":    [int(y) for y in re.findall(r'\b(201\d|202\d)\b', caption)],

                    # Engagement
                    "likes":            post.likes,
                    "comments":         post.comments,
                    "video_views":      post.video_view_count if post.is_video else 0,
                    "is_video":         post.is_video,
                    "is_carousel":      post.typename == "GraphSidecar",

                    # Location (NLP extracted from caption)
                    "location_name":    loc_name,
                    "lat":              lat,
                    "lng":              lng,
                    "all_locations":    all_locs,

                    # Location (Instagram geotag)
                    "geo_tag":          geo_tag,

                    # Classification
                    "incident_type":        inc_type,
                    "all_incident_types":   inc_all,
                    "hate_score":           hate_score,
                    "hate_keywords_found":  hate_kw,
                    "urgency_score":        urgency,

                    # Media
                    "thumbnail_url":    post.url,
                    "post_type":        post.typename,
                }

                posts.append(record)
                done_codes.add(post.shortcode)
                count += 1

                # Checkpoint every 25 posts
                if count % 25 == 0:
                    log(f"  @{username}: {count} scraped | {skipped} skipped | {errors} errors", "info")
                    save_checkpoint(username, done_codes, posts)

                time.sleep(DELAY_BETWEEN_POSTS)

            except Exception as e:
                errors += 1
                log(f"  Post error [{post.shortcode}]: {e}", "warn")
                time.sleep(4)
                continue

        clear_checkpoint(username)
        log(f"@{username} done → {count} new | {skipped} skipped | {errors} errors", "ok")

    except instaloader.exceptions.ProfileNotExistsException:
        log(f"@{username} does not exist", "error")
    except instaloader.exceptions.LoginRequiredException:
        log(f"@{username} requires login — run with --login flag", "error")
    except instaloader.exceptions.ConnectionException as e:
        log(f"Connection error for @{username}: {e} — progress saved", "error")
        save_checkpoint(username, done_codes, posts)
    except KeyboardInterrupt:
        log("Interrupted! Saving checkpoint...", "warn")
        save_checkpoint(username, done_codes, posts)
        raise
    except Exception as e:
        log(f"Unexpected error for @{username}: {e}", "error")
        save_checkpoint(username, done_codes, posts)

    return posts


# ── SAVE CSV ─────────────────────────────────────────────────
def save_csv(posts, filepath):
    if not posts:
        return
    flat = []
    for p in posts:
        row = {k: v for k, v in p.items() if not isinstance(v, (list, dict))}
        row["hashtags"]          = " | ".join(p.get("hashtags", []))
        row["mentions"]          = " | ".join(p.get("mentions", []))
        row["all_incident_types"]= " | ".join(p.get("all_incident_types", []))
        row["hate_kw_found"]     = " | ".join(p.get("hate_keywords_found", []))
        row["all_locations"]     = " | ".join(l["name"] for l in p.get("all_locations", []))
        geo = p.get("geo_tag") or {}
        row["geotag_name"]       = geo.get("name", "")
        row["geotag_lat"]        = geo.get("lat", "")
        row["geotag_lng"]        = geo.get("lng", "")
        flat.append(row)

    keys = list(flat[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(flat)
    log(f"CSV saved → {filepath}", "ok")


# ── SUMMARY ──────────────────────────────────────────────────
def print_summary(posts):
    print(f"\n{'═'*55}")
    print(f"  SCRAPE COMPLETE — SUMMARY")
    print(f"{'═'*55}")
    print(f"  Total posts scraped : {len(posts)}")
    print(f"  Geo-located (NLP)   : {sum(1 for p in posts if p['lat'])}")
    print(f"  With hate keywords  : {sum(1 for p in posts if p['hate_score'] > 0)}")
    print(f"  High urgency (≥3)   : {sum(1 for p in posts if p['urgency_score'] >= 3)}")
    print(f"  Total likes         : {sum(p.get('likes',0) for p in posts):,}")
    print(f"  Total comments      : {sum(p.get('comments',0) for p in posts):,}")

    print(f"\n  By account:")
    for acc in DEFAULT_ACCOUNTS:
        n = sum(1 for p in posts if p['account'] == acc)
        print(f"    @{acc:<28} {n} posts")

    print(f"\n  Top incident types:")
    for t, c in Counter(p['incident_type'] for p in posts).most_common(8):
        bar = '█' * min(c, 30)
        print(f"    {t:<28} {bar} {c}")

    print(f"\n  Top locations:")
    for loc, c in Counter(p['location_name'] for p in posts if p['location_name']).most_common(8):
        print(f"    {loc:<28} {c}")

    print(f"\n  Top hashtags:")
    all_tags = list(chain.from_iterable(p.get('hashtags', []) for p in posts))
    for tag, c in Counter(all_tags).most_common(10):
        print(f"    #{tag:<27} {c}")

    print(f"{'═'*55}\n")


# ── ENTRY POINT ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Full Instagram scraper for communal violence monitoring",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py                          # scrape all 3 accounts, ALL posts
  python scraper.py --max 200                # cap at 200 posts per account
  python scraper.py --resume                 # continue interrupted scrape
  python scraper.py --login                  # login for more data access
  python scraper.py --accounts hindutvawatch # only one account
        """
    )
    parser.add_argument("--accounts", nargs="+", default=DEFAULT_ACCOUNTS)
    parser.add_argument("--max", type=int, default=None,
                        help="Max posts per account (default: ALL)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint files")
    parser.add_argument("--login", action="store_true",
                        help="Login to Instagram (required for some accounts)")
    parser.add_argument("--username", type=str, default=None,
                        help="Instagram username for login")
    parser.add_argument("--password", type=str, default=None,
                        help="Instagram password for login")
    parser.add_argument("--output", type=str, default=OUTPUT_JSON,
                        help=f"Output JSON filename (default: {OUTPUT_JSON})")
    parser.add_argument("--delay", type=float, default=DELAY_BETWEEN_POSTS,
                        help=f"Delay between posts in seconds (default: {DELAY_BETWEEN_POSTS})")
    args = parser.parse_args()

    global DELAY_BETWEEN_POSTS
    DELAY_BETWEEN_POSTS = args.delay

    print(f"\n{Fore.MAGENTA}{'═'*55}")
    print(f"  Instagram Communal Violence Scraper")
    print(f"  Accounts : {', '.join(args.accounts)}")
    print(f"  Max posts: {'ALL' if not args.max else args.max} per account")
    print(f"  Resume   : {args.resume}")
    print(f"  Login    : {args.login}")
    print(f"{'═'*55}{Style.RESET_ALL}\n")

    # Setup loader
    loader = instaloader.Instaloader(
        download_pictures         = False,
        download_videos           = False,
        download_video_thumbnails = False,
        download_geotags          = True,   # fetch Instagram native geotag
        download_comments         = False,
        save_metadata             = False,
        compress_json             = False,
        quiet                     = True,
        max_connection_attempts   = 5,
        request_timeout           = 30,
    )

    # Optional login
    if args.login:
        import getpass
        uname = args.username or input("Instagram username: ").strip()
        pwd   = args.password or getpass.getpass("Password: ")
        session_file = f"session-{uname}"
        try:
            loader.load_session_from_file(uname, session_file)
            log(f"Session loaded for @{uname}", "ok")
        except Exception:
            log("Logging in fresh...", "warn")
            loader.login(uname, pwd)
            loader.save_session_to_file(session_file)
            log(f"Logged in as @{uname} — session saved for next time", "ok")

    # Scrape all accounts
    all_posts = []
    try:
        for i, account in enumerate(args.accounts):
            if i > 0:
                log(f"Waiting {DELAY_BETWEEN_ACCOUNTS}s before next account...", "info")
                time.sleep(DELAY_BETWEEN_ACCOUNTS)
            posts = scrape_account(
                loader, account,
                resume=args.resume,
                max_posts=args.max
            )
            all_posts.extend(posts)
    except KeyboardInterrupt:
        log("Interrupted. Saving collected data...", "warn")

    if not all_posts:
        log("No posts collected. Check account names or use --login", "error")
        sys.exit(1)

    # Save JSON
    output = {
        "scraped_at":  datetime.now().isoformat(),
        "accounts":    args.accounts,
        "total_posts": len(all_posts),
        "posts":       all_posts,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log(f"JSON saved → {args.output}  ({len(all_posts)} posts)", "ok")

    # Save CSV
    csv_path = args.output.replace(".json", ".csv")
    save_csv(all_posts, csv_path)

    print_summary(all_posts)
    log("Load scraped_data.json into the dashboard HTML to visualize.", "ok")


if __name__ == "__main__":
    main()
