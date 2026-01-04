# بوت الفحص المطور @vsjj
import re
import time
import json
import threading
import os
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urlparse
import requests
import discord
from discord import app_commands
from discord.ext import commands

CONFIG = {}
PROXIES_CACHE = []
CHECK_EXECUTOR = ThreadPoolExecutor(max_workers=80)  # will be re-created after config load
CHECK_SEM = None
MAX_INFLIGHT_CHECKS = 60
CHECK_TIMEOUT = 15

# -------- HTTP session (fast + stable under high concurrency) --------
HTTP = requests.Session()
try:
    _adapter = requests.adapters.HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=0)
    HTTP.mount("http://", _adapter)
    HTTP.mount("https://", _adapter)
except Exception:
    pass

def _http_get(url, **kwargs):
    """Wrapper to reuse a pooled requests.Session (prevents socket exhaustion)."""
    return HTTP.get(url, **kwargs)

# -------- Discord send helper (reliable + non-crashy) --------
async def get_channel_safe(bot: discord.Client, channel_id: int):
    ch = bot.get_channel(channel_id)
    if ch:
        return ch
    try:
        return await bot.fetch_channel(channel_id)
    except Exception:
        return None

async def send_with_retry(
    bot: discord.Client,
    channel_id: int,
    *,
    content: str | None = None,
    embed: discord.Embed | None = None,
    tries: int = 3,
) -> bool:
    """Send a message with small retries to survive transient Discord HTTP errors."""
    ch = await get_channel_safe(bot, channel_id)
    if not ch:
        return False
    for i in range(max(1, tries)):
        try:
            await ch.send(content=content, embed=embed)
            return True
        except discord.Forbidden:
            return False
        except discord.HTTPException:
            await asyncio.sleep(1.5 * (i + 1))
        except Exception:
            await asyncio.sleep(1.0 * (i + 1))
    return False

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Instagram 219.0.0.12.117 Android (31/12; 480dpi; 1080x2260; samsung; SM-G998B; p3s; exynos2100; en_US; 337326283)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 303.0.0.11.109",
    "Mozilla/5.0 (Linux; Android 10; SM-A205U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0"
]

# -------- Media resolver (page URL -> direct image/gif URL) --------
MEDIA_CACHE = {}
MEDIA_EXTS = (".png", ".jpg", ".jpeg", ".gif", ".webp")

def canonicalize_media_url(u: str) -> str:
    """Normalize media URLs for Discord embeds (strip expiring Discord CDN query params)."""
    u = _strip_url(u)
    if not u:
        return u
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if host.endswith("cdn.discordapp.com") or host.endswith("media.discordapp.net"):
            return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        pass
    return u

def is_embed_image_url(u: str) -> bool:
    """True if URL is suitable for embed images (png/jpg/gif/webp)."""
    u = canonicalize_media_url(u)
    if not u or not u.startswith("http"):
        return False
    low = u.lower()
    if low.endswith((".mp4", ".mov", ".webm")):
        return False
    ext = _path_ext(u)
    if ext:
        return True
    try:
        host = (urlparse(u).netloc or "").lower()
        if any(h in host for h in ("cdn.discordapp.com", "media.discordapp.net", "media.tenor.com", "i.giphy.com", "media.giphy.com")):
            return True
    except Exception:
        pass
    return False


def _strip_url(u: str) -> str:
    u = (u or "").strip()
    return u.strip("<>").strip()


def _path_ext(u: str) -> str:
    """Return media file extension from URL path if present; else empty string."""
    try:
        p = urlparse(u)
        path = (p.path or "").lower()
        for ext in MEDIA_EXTS:
            if path.endswith(ext):
                return ext
    except Exception:
        pass
    return ""


def _path_has_media_ext(u: str) -> bool:
    try:
        p = urlparse(u)
        path = (p.path or "").lower()
        return any(path.endswith(ext) for ext in MEDIA_EXTS)
    except Exception:
        return False


def _is_direct_media(u: str) -> bool:
    return is_embed_image_url(u)


def resolve_media_url(url: str) -> str:
    """Best-effort: convert Tenor/Giphy/page URL to a direct image/gif URL."""
    url = _strip_url(url)
    if not url:
        return url
    if url in MEDIA_CACHE:
        return MEDIA_CACHE[url]
    if _is_direct_media(url):
        url2 = canonicalize_media_url(url)
        MEDIA_CACHE[url] = url2
        return url2

    headers = get_headers()
    headers["Accept"] = "text/html,application/xhtml+xml"

    try:
        r = _http_get(url, timeout=(5, 10), headers=headers, allow_redirects=True)
        html = r.text or ""

        def _meta(prop_or_name: str):
            m = re.search(
                rf'<meta[^>]+(?:property|name)=["\']{re.escape(prop_or_name)}["\'][^>]+content=["\']([^"\']+)["\']',
                html, re.I
            )
            return m.group(1).strip() if m else None

        candidates = []
        for key in ("og:image:secure_url", "og:image", "twitter:image", "og:video:secure_url", "og:video"):
            v = _meta(key)
            if v:
                candidates.append(_strip_url(v))

        for c in candidates:
            if _is_direct_media(c):
                MEDIA_CACHE[url] = c
                return c

        if candidates:
            c0 = canonicalize_media_url(_strip_url(candidates[0]))
            MEDIA_CACHE[url] = c0
            return c0

    except Exception:
        pass

    MEDIA_CACHE[url] = url
    return url

async def resolve_media_url_async(url: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(CHECK_EXECUTOR, resolve_media_url, url)


def get_headers():
    ua = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    if "Mobile" in ua or "Android" in ua or "iPhone" in ua:
        headers["Sec-Ch-Ua-Mobile"] = "?1"
        headers["Sec-Ch-Ua-Platform"] = '"Android"' if "Android" in ua else '"iOS"'
    else:
        headers["Sec-Ch-Ua-Mobile"] = "?0"
        headers["Sec-Ch-Ua-Platform"] = '"Windows"' if "Windows" in ua else '"macOS"'
    return headers


def load_proxies():
    global PROXIES_CACHE
    try:
        if os.path.exists("proxies.txt"):
            with open("proxies.txt", "r") as f:
                PROXIES_CACHE = [line.strip() for line in f if line.strip()]
            print(f"Loaded {len(PROXIES_CACHE)} proxies.")
        else:
            PROXIES_CACHE = []
            print("No proxies.txt found.")
    except Exception as e:
        print(f"Error loading proxies: {e}")
        PROXIES_CACHE = []


def load_config():
    global CONFIG
    try:
        p = os.path.join(os.getcwd(), "config.json")
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                CONFIG = json.load(f)
            if "admin_id" in CONFIG and CONFIG["admin_id"]:
                try:
                    CONFIG["admin_id"] = int(CONFIG["admin_id"])
                except ValueError:
                    pass
        else:
            CONFIG = {}
    except Exception:
        CONFIG = {}


def normalize_interval(iv: int | None) -> int:
    cfg = (CONFIG.get("monitor") or {})
    default_iv = cfg.get("default_interval") or 60
    min_iv = cfg.get("min_interval") or 60
    try:
        val = int(iv) if iv is not None else int(default_iv)
    except Exception:
        val = int(default_iv)
    if val < int(min_iv):
        val = int(min_iv)
    return val


def profile_unavailable(text: str) -> bool:
    t = (text or "").lower()
    patterns = [
        "profile isn't available",
        "the link may be broken, or the profile may have been removed",
        "sorry, this page isn't available",
        "عذرًا، هذه الصفحة غير متاحة",
        "content unavailable",
        "resource was not cached",
        "page not found",
        "user not found",
    ]
    return any(p in t for p in patterns)

PROXY_LOCK = threading.Lock()


# ---- Proxy health & selection (fixes: deleting good proxies on IG 429) ----
# PROXY_STATE keeps lightweight per-proxy scoring + cooldowns.
# We ONLY delete a proxy when it is truly broken (timeout / connect / auth / 5xx from proxy),
# but we put it on cooldown when Instagram rate-limits / blocks it (401/403/429).
PROXY_STATE = {}  # raw_proxy -> {"fails": int, "blocked_hits": int, "cooldown_until": float}

def _proxy_fmt(raw: str) -> str | None:
    if not raw:
        return None
    p = raw.strip()
    if not p:
        return None
    # Accept formats:
    #   ip:port
    #   ip:port:user:pass
    #   user:pass@ip:port
    if "@" not in p and p.count(":") == 3:
        parts = p.split(":")
        p = f"{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    if not p.startswith("http"):
        p = "http://" + p
    return p


def _rewrite_proxies_file():
    """Persist current proxy list back to proxies.txt (atomic)."""
    try:
        tmp = "proxies.txt.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for p in PROXIES_CACHE:
                if p:
                    f.write(str(p).strip() + "\n")
        os.replace(tmp, "proxies.txt")
    except Exception:
        # best-effort: ignore
        pass


def mark_proxy_bad(raw_proxy: str, reason: str = ""):
    """Mark proxy as broken; delete after a small number of hard failures."""
    if not raw_proxy:
        return
    with PROXY_LOCK:
        st = PROXY_STATE.get(raw_proxy) or {"fails": 0, "blocked_hits": 0, "cooldown_until": 0.0}
        st["fails"] = int(st.get("fails") or 0) + 1
        PROXY_STATE[raw_proxy] = st

        # Hard delete threshold
        hard_del = 2  # keep low: broken proxies slow everything down
        if st["fails"] >= hard_del and raw_proxy in PROXIES_CACHE:
            try:
                PROXIES_CACHE.remove(raw_proxy)
                PROXY_STATE.pop(raw_proxy, None)
                _rewrite_proxies_file()
                print(f"⚠️ Removed bad proxy. Remaining: {len(PROXIES_CACHE)} | reason={reason or 'fail'}")
            except Exception:
                pass


def mark_proxy_blocked(raw_proxy: str, block_code: int = 429, http_code: int | None = None):
    """Instagram blocked/rate-limited this proxy. Don't delete it; just cool it down."""
    if not raw_proxy:
        return
    cfgm = (CONFIG.get("monitor") or {})
    try:
        cooldown = int(cfgm.get("proxy_block_cooldown", 900))  # 15 min default
    except Exception:
        cooldown = 900
    now = time.time()
    code = http_code if http_code is not None else block_code
    with PROXY_LOCK:
        st = PROXY_STATE.get(raw_proxy) or {"fails": 0, "blocked_hits": 0, "cooldown_until": 0.0}
        st["blocked_hits"] = int(st.get("blocked_hits") or 0) + 1
        st["cooldown_until"] = max(float(st.get("cooldown_until") or 0.0), now + cooldown)
        st["last_block_code"] = code
        PROXY_STATE[raw_proxy] = st


def remove_bad_proxy(raw_proxy):
    """Backward-compatible name: remove ONLY truly bad proxies (not IG 429)."""
    mark_proxy_bad(raw_proxy, reason="remove_bad_proxy()")


def get_proxy_tuple():
    """Return (proxies_dict, raw_proxy_line) or (None, None).

    Important:
    - Do NOT delete proxies on IG 429/403/401. Those are usually *Instagram rate limits*,
      not a dead proxy.
    - We keep a cooldown per proxy. If ALL proxies are cooling down, we return None so
      the caller can backoff instead of hammering IG.
    """
    global PROXIES_CACHE

    if not PROXIES_CACHE:
        load_proxies()

    now = time.time()
    p = None
    with PROXY_LOCK:
        if PROXIES_CACHE:
            # pick only proxies that are not in cooldown
            candidates = []
            for raw in PROXIES_CACHE:
                st = PROXY_STATE.get(raw) or {}
                if now >= float(st.get("cooldown_until") or 0):
                    candidates.append(raw)
            if candidates:
                p = random.choice(candidates)
            else:
                # all proxies cooling down (likely IG rate-limit) -> force backoff
                return None, None

    if not p:
        return None, None

    try:
        raw = p
        # Support "host:port:user:pass" -> "user:pass@host:port"
        if "@" not in p and p.count(":") == 3:
            host, port, user, pw = p.split(":")
            p = f"{user}:{pw}@{host}:{port}"
        formatted = f"http://{p}" if not p.startswith("http") else p
        return {"http": formatted, "https": formatted}, raw
    except Exception:
        return None, None


def get_random_proxy():
    d, _ = get_proxy_tuple()
    return d


def fetch_profile(u: str):
    """Fetch public IG HTML with proxies + safe backoff.

    Key change: **do not loop 10 times** per check. When IG rate-limits,
    repeating immediately just makes it worse.
    """
    headers = get_headers()
    max_tries = int(((CONFIG.get("proxy") or {}).get("max_tries")) or 3)
    max_tries = max(1, min(max_tries, 5))

    # 1) Try via proxy (few attempts)
    for attempt in range(max_tries):
        proxies, raw_p = get_proxy_tuple()
        if not proxies:
            break
        try:
            r = _http_get(u, timeout=(5, 10), headers=headers, proxies=proxies, allow_redirects=True)

            # IG rate-limit / blocks: mark cooldown and return immediately (no hammering)
            if r.status_code in (401, 403, 429):
                mark_proxy_blocked(raw_p, block_code=r.status_code)
                return r

            return r

        except requests.exceptions.RequestException as e:
            # network/proxy errors -> score it as bad (cooldown)
            mark_proxy_bad(raw_p, reason=str(e))
            if attempt == max_tries - 1:
                print(f"Request Error (Final Attempt): {e}")

    # 2) Fallback: direct (no proxy)
    try:
        return _http_get(u, timeout=(5, 10), headers=headers, allow_redirects=True)
    except Exception:
        pass

    class DummyResponse:
        status_code = 0
        text = ""
    return DummyResponse()


def extract_username(s: str) -> str:
    s = (s or "").strip().lstrip("@")
    if s.startswith("http"):
        p = urlparse(s)
        parts = p.path.strip("/").split("/")
        return (parts[0] if parts else "").lower()
    return s.strip("/").lower()


def exists_via_search(username: str):
    # Returns:
    #   True  -> found
    #   False -> not found
    #   "blocked" -> rate-limited / blocked
    #   None  -> unknown (network/proxy/etc)
    for attempt in range(10):
        try:
            proxies, raw_p = get_proxy_tuple()
            headers = get_headers()
            headers["Accept"] = "application/json"
            headers["X-Requested-With"] = "XMLHttpRequest"
            headers["Referer"] = "https://www.instagram.com/"
            resp = _http_get(
                f"https://www.instagram.com/web/search/topsearch/?context=blended&query={username}",
                timeout=15,
                headers=headers,
                proxies=proxies,
            )

            if resp.status_code in (401, 403, 429):
                mark_proxy_blocked(raw_p, block_code=resp.status_code)
                continue

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    return None

                users = data.get("users") or []
                for item in users:
                    u2 = (item.get("user") or {}).get("username", "").lower()
                    if u2 == username.lower():
                        return True
                return False
        except Exception:
            pass
        time.sleep(1)
    return None


def exists_via_reset(username: str):
    url = f"https://www.instagram.com/accounts/password/reset/?username={username}"
    for attempt in range(10):
        try:
            proxies, raw_p = get_proxy_tuple()
            headers = get_headers()
            r = _http_get(url, timeout=12, headers=headers, proxies=proxies, allow_redirects=True)
            if r.status_code in (401, 403, 429):
                mark_proxy_blocked(raw_p, block_code=r.status_code)
                continue
            t = (r.text or "").lower()
            if "no users found" in t or "user not found" in t:
                return False
            if r.status_code == 404:
                return False
            # 200/302 is not a reliable positive signal for existence; treat as unknown
            return None
        except Exception:
            pass
    return None


def appears_available(html: str, username: str | None = None) -> bool:
    try:
        if not html:
            return False
        if profile_unavailable(html):
            return False
        m_type = re.search(r'<meta[^>]+property=["\']og:type["\'][^>]+content=["\']profile["\']', html, re.I)
        if username:
            u_clean = username.lower().strip()
            deep_android = re.search(rf'property=["\']al:android:url["\'][^>]+content=["\']instagram://user\?username={re.escape(u_clean)}["\']', html, re.I)
            deep_ios = re.search(rf'property=["\']al:ios:url["\'][^>]+content=["\']instagram://user\?username={re.escape(u_clean)}["\']', html, re.I)
            if deep_android or deep_ios:
                return True
        if m_type:
            return True
        return False
    except Exception:
        return False


def get_profile_meta(username: str):
    pic, followers, following, posts = None, None, None, None
    name = username
    try:
        proxies, raw_p = get_proxy_tuple()
        headers = get_headers()
        headers["Accept"] = "application/json"
        resp = _http_get(f"https://www.instagram.com/web/search/topsearch/?query={username}", timeout=15, headers=headers, proxies=proxies)
        if resp.status_code in (401, 403, 429):
            mark_proxy_blocked(raw_p, block_code=resp.status_code)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("users") or []:
                u = item.get("user") or {}
                if (u.get("username", "").lower()) == username.lower():
                    pic = u.get("profile_pic_url_hd") or u.get("profile_pic_url")
                    followers = u.get("follower_count")
                    name = u.get("username", name)
                    break
    except Exception:
        pass
    try:
        url = f"https://www.instagram.com/{username}/"
        r = fetch_profile(url)
        text = r.text
        if not pic:
            m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', text)
            if m:
                pic = m.group(1)
        m_desc = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']', text)
        if m_desc:
            desc = m_desc.group(1)
            if not followers:
                f_match = re.search(r'([\d,KM\.]+) Followers', desc)
                if f_match:
                    followers = f_match.group(1)
            fol_match = re.search(r'([\d,KM\.]+) Following', desc)
            if fol_match:
                following = fol_match.group(1)
            p_match = re.search(r'([\d,KM\.]+) Posts', desc)
            if p_match:
                posts = p_match.group(1)
    except Exception:
        pass
    return {"username": name, "profile_pic": pic, "followers": followers, "following": following, "posts": posts}

IG_WEB_PROFILE_INFO = "https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"

def fetch_web_profile_info(username: str):
    """Call IG web_profile_info endpoint.

    IMPORTANT:
    - When we get blocked (401/403/429 or 'Please wait'), we STOP quickly and let
      the monitor loop backoff. Retrying 10 times per check causes permanent 429s.
    """
    url = IG_WEB_PROFILE_INFO.format(username=username)
    headers = get_headers()
    headers.update({
        "Accept": "application/json, text/plain, */*",
        "X-IG-App-ID": "936619743392459",
        "X-Requested-With": "XMLHttpRequest",
    })

    max_tries = int(((CONFIG.get("proxy") or {}).get("max_tries_api")) or 2)
    max_tries = max(1, min(max_tries, 4))
    last_exc = None

    for attempt in range(max_tries):
        proxies, raw_p = get_proxy_tuple()
        if not proxies:
            break
        try:
            r = _http_get(url, timeout=(5, 10), headers=headers, proxies=proxies)
            http_code = r.status_code

            if http_code in (401, 403, 429):
                mark_proxy_blocked(raw_p, block_code=http_code)
                return "blocked", {"http_code": http_code, "block_code": http_code, "raw": (r.text or "")[:500], "method": "web_profile_info"}

            if http_code == 404:
                return "unavailable", {"http_code": http_code, "raw": (r.text or "")[:500], "method": "web_profile_info"}

            if http_code != 200:
                return "unknown", {"http_code": http_code, "raw": (r.text or "")[:500], "method": "web_profile_info"}

            try:
                j = r.json()
            except Exception:
                return "unknown", {"http_code": http_code, "raw": (r.text or "")[:500], "method": "web_profile_info"}

            # Soft rate limit sometimes returns 200 with {"status":"fail","message":"Please wait..."}
            if isinstance(j, dict) and j.get("status") == "fail":
                msg = (j.get("message") or "").lower()
                if any(k in msg for k in ("wait", "rate", "limit", "please", "temporarily", "too many")):
                    mark_proxy_blocked(raw_p, block_code=429)
                    return "blocked", {"http_code": http_code, "block_code": 429, "json": j, "method": "web_profile_info"}
                return "unknown", {"http_code": http_code, "json": j, "method": "web_profile_info"}

            user = None
            try:
                user = (((j or {}).get("data") or {}).get("user")) if isinstance(j, dict) else None
            except Exception:
                user = None

            if user and isinstance(user, dict) and user.get("username"):
                return "available", {
                    "http_code": http_code,
                    "json": j,
                    "method": "web_profile_info",
                    "profile_pic": user.get("profile_pic_url") or user.get("profile_pic_url_hd"),
                    "full_name": user.get("full_name"),
                    "followers": user.get("follower_count"),
                    "following": user.get("following_count"),
                    "posts": user.get("media_count"),
                }

            return "unavailable", {"http_code": http_code, "json": j, "method": "web_profile_info"}

        except Exception as e:
            last_exc = e
            mark_proxy_bad(raw_p, reason=str(e))

    return "unknown", {"http_code": None, "error": str(last_exc), "method": "web_profile_info"}


def check(target: str):
    name = extract_username(target)

    # 1) Try the web_profile_info endpoint first
    s1, info1 = fetch_web_profile_info(name)

    # Suspicious "unavailable" with HTTP 200 (might be a soft block / login redirect)
    # If API says unavailable but 200, we should double check via HTML fallback to be safe.
    if s1 == "unavailable" and info1.get("http_code") == 200:
        s1 = "blocked" # Force fallback logic below

    if s1 in ("available", "unavailable"):
        u = f"https://www.instagram.com/{name}/"
        return {
            "state": s1,
            "status_code": info1.get("http_code") or 0,
            "block_code": info1.get("block_code"),
            "method": info1.get("method"),
            "url": u,
            "profile_pic": info1.get("profile_pic"),
            "full_name": info1.get("full_name"),
            "followers": info1.get("followers"),
            "following": info1.get("following"),
            "posts": info1.get("posts"),
            "profile_unavailable": (s1 == "unavailable"),
            "is_profile": (s1 == "available"),
            "html": "",
        }
    if s1 == "blocked":
        # The API may be rate-limited/blocked but the public profile page can still
        # tell us if the user truly exists (matches what you see on mobile).
        u = f"https://www.instagram.com/{name}/"
        try:
            r_html = fetch_profile(u)
            html = (r_html.text or "")
            if r_html.status_code == 404 or profile_unavailable(html):
                return {
                    "state": "unavailable",
                    "status_code": r_html.status_code,
                    "block_code": None,
                    "method": "profile_html",
                    "url": u,
                    "profile_pic": None,
                    "full_name": None,
                    "profile_unavailable": True,
                    "is_profile": False,
                    "html": html,
                }
            if appears_available(html, name):
                meta = get_profile_meta(name) or {}
                return {
                    "state": "available",
                    "status_code": r_html.status_code,
                    "block_code": None,
                    "method": "profile_html",
                    "url": u,
                    "profile_pic": meta.get("profile_pic"),
                    "full_name": meta.get("full_name"),
                    "profile_unavailable": False,
                    "is_profile": True,
                    "html": html,
                }
        except Exception:
            pass

        # If HTML fallback couldn't confirm, report as blocked (keep the original API signal).
        return {
            "state": "blocked",
            "status_code": info1.get("http_code") or 0,
            "block_code": info1.get("block_code") or 429,
            "method": info1.get("method"),
            "url": u,
            "profile_pic": None,
            "full_name": None,
            "profile_unavailable": False,
            "is_profile": False,
            "html": "",
        }
    # 2) Fallback to the old methods
    u = f"https://www.instagram.com/{name}/"
    reset_result = exists_via_reset(name)
    search_result = exists_via_search(name)
    r = fetch_profile(u)
    unavailable = profile_unavailable(r.text)
    signal = appears_available(r.text, name)

    state = "unavailable"
    block_code = None
    if r.status_code == 429:
        state = "blocked"
        block_code = 429
    elif search_result is True or signal:
        state = "available"
    elif unavailable or r.status_code == 404:
        state = "unavailable"
    else:
        if reset_result is False or search_result is False:
            state = "unavailable"
        else:
            state = "unknown"

    pic = None
    try:
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', r.text)
        if m:
            pic = m.group(1)
    except Exception:
        pass

    return {
        "state": state,
        "status_code": getattr(r, "status_code", 0),
        "block_code": block_code,
        "profile_unavailable": unavailable,
        "url": u,
        "profile_pic": pic,
        "html": r.text,
        "is_profile": signal,
        "method": "profile_html",
    }


monitors = {}
monitors_lock = threading.Lock()

pending_adds = {}
# key -> unix timestamp when /add was issued, waiting for reason selection
pending_adds_lock = threading.Lock()


def start_monitor(bot, username: str, interval: int, channel_id: int, trader_name: str,
                  owner_id: int, reason: str, label: str, start_timestamp: float = None):
    monitor_key = f"{owner_id}:{username}"
    stop = asyncio.Event()
    started_at = datetime.fromtimestamp(start_timestamp) if start_timestamp else datetime.now()
    final_start_time = start_timestamp if start_timestamp else time.time()

    async def loop():
        start_time = final_start_time
        last_state = None

        # Spread checks after restart so 500+ monitors don't spike at the same second
        base_interval0 = normalize_interval(interval)
        next_run = time.time()
        if start_timestamp is not None:
            try:
                _base = int(base_interval0 or 60)
                stagger = abs(hash(monitor_key)) % _base
            except Exception:
                stagger = random.randint(0, int(base_interval0 or 60))
            if stagger:
                await asyncio.sleep(float(stagger))
            next_run = time.time()


        # Initial jitter so new monitors don't all hit Instagram in the same second
        try:
            initial_jitter = abs(hash(monitor_key)) % max(1, int(base_interval0 or 60))
            await asyncio.sleep(float(initial_jitter))
        except Exception:
            pass

        # High-confidence confirmation
        banned_streak = 0
        active_streak = 0
        confirmed_banned = False
        blocked_streak = 0
        CONFIRM_BANNED = int(((CONFIG.get("monitor") or {}).get("confirm_banned", 2)) or 2)
        CONFIRM_UNBANNED = int(((CONFIG.get("monitor") or {}).get("confirm_unbanned", 2)) or 2)

        while not stop.is_set():
            cur = last_state
            override_delay = None
            try:
                with monitors_lock:
                    if monitor_key not in monitors:
                        break

                loop_obj = asyncio.get_running_loop()
                # Concurrency gate + hard timeout (so monitors never hang)
                try:
                    if CHECK_SEM is not None:
                        await CHECK_SEM.acquire()
                    fut = loop_obj.run_in_executor(CHECK_EXECUTOR, check, username)
                    info = await asyncio.wait_for(fut, timeout=CHECK_TIMEOUT)
                except asyncio.TimeoutError:
                    info = {"state": "unknown", "status_code": 0, "method": "timeout", "error": "timeout"}
                finally:
                    try:
                        if CHECK_SEM is not None:
                            CHECK_SEM.release()
                    except Exception:
                        pass

                cur = info.get("state")
                http_code = info.get("status_code") or 0
                block_code = info.get("block_code")

                status_icon = "✅ ACTIVE" if cur == "available" else "❌ BANNED"
                if cur == "blocked":
                    status_icon = f"⚠️ BLOCKED ({block_code or 429})"
                if cur == "unknown":
                    status_icon = "❓ UNKNOWN"

                code_str = f"HTTP {http_code}"
                if cur == "blocked" and block_code and block_code != http_code:
                    code_str = f"HTTP {http_code} / IG {block_code}"

                print(f"{datetime.now().strftime('%H:%M:%S')} [Monitor @{username}] {status_icon} | {code_str}")

                # State machine
                if cur == "unavailable":
                    banned_streak += 1
                    active_streak = 0
                    blocked_streak = 0
                    if banned_streak >= CONFIRM_BANNED:
                        confirmed_banned = True

                elif cur == "available":
                    active_streak += 1
                    banned_streak = 0
                    blocked_streak = 0

                    # If it starts ACTIVE before we've confirmed banned, treat it as UNBANNED (User request)
                    if not confirmed_banned:
                        confirmed_banned = True
                        active_streak = CONFIRM_UNBANNED

                    # Fire unban only after confirmed banned + consecutive ACTIVE
                    # Fast confirm: don't wait a full interval to confirm unban
                    if confirmed_banned and CONFIRM_UNBANNED > 1 and active_streak == 1:
                        fast_delay = int(((CONFIG.get("monitor") or {}).get("fast_confirm_delay", 8)) or 8)
                        await asyncio.sleep(max(1, fast_delay))
                        try:
                            if CHECK_SEM is not None:
                                await CHECK_SEM.acquire()
                            fut2 = loop_obj.run_in_executor(CHECK_EXECUTOR, check, username)
                            info2 = await asyncio.wait_for(fut2, timeout=CHECK_TIMEOUT)
                        except asyncio.TimeoutError:
                            info2 = {"state": "unknown", "status_code": 0, "method": "timeout"}
                        finally:
                            try:
                                if CHECK_SEM is not None:
                                    CHECK_SEM.release()
                            except Exception:
                                pass

                        if (info2 or {}).get("state") == "available":
                            info = info2
                            cur = "available"
                            active_streak += 1
                        else:
                            active_streak = 0

                    if confirmed_banned and active_streak >= CONFIRM_UNBANNED:
                        elapsed_seconds = int(time.time() - start_time)

                        # Pull latest stored reason (so /reason edits are reflected)
                        reason_latest = reason
                        with monitors_lock:
                            md = monitors.get(monitor_key) or {}
                            reason_latest = md.get("reason") or reason_latest

                        if not info.get("profile_pic"):
                            try:
                                meta_latest = get_profile_meta(username)
                                if meta_latest.get("profile_pic"):
                                    info["profile_pic"] = meta_latest["profile_pic"]
                                    info["followers"] = meta_latest.get("followers") or info.get("followers")
                                    info["following"] = meta_latest.get("following") or info.get("following")
                                    info["posts"] = meta_latest.get("posts") or info.get("posts")
                                    info["full_name"] = meta_latest.get("full_name") or info.get("full_name")
                            except Exception:
                                pass

                        info_for_embed = {
                            "username": username,
                            "url": info.get("url") or f"https://www.instagram.com/{username}/",
                            "profile_pic": info.get("profile_pic"),
                            "full_name": info.get("full_name"),
                            "followers": info.get("followers"),
                            "following": info.get("following"),
                            "posts": info.get("posts"),
                        }
                        brand = trader_brands.get(str(owner_id)) or {}
                        owner_name = brand.get("name") or trader_name

                        embed_obj = build_unban_embed_discord(
                            info_for_embed,
                            elapsed_seconds,
                            owner_name=owner_name,
                            reason=reason_latest,
                            brand=brand
                        )
                        await send_with_retry(bot, channel_id, content=f"<@{owner_id}>", embed=embed_obj)

                        # Stop and remove the monitor
                        with monitors_lock:
                            if monitor_key in monitors:
                                del monitors[monitor_key]
                        with store_lock:
                            cid = str(channel_id)
                            if cid in trader_monitors:
                                recs = trader_monitors[cid]
                                label_to_remove = next((k for k, v in recs.items() if v.get("username") == username), None)
                                if label_to_remove:
                                    del recs[label_to_remove]
                                    save_store()
                        stop.set()
                        break

                else:
                    # blocked/unknown -> backoff to reduce rate limits (BLOCKED كثير)
                    active_streak = 0
                    banned_streak = 0

                    cfgm = (CONFIG.get("monitor") or {})
                    if cur == "blocked":
                        blocked_streak += 1
                        try:
                            bmin = int(cfgm.get("blocked_backoff_min", 90))
                            bmax = int(cfgm.get("blocked_backoff_max", 900))
                            bjit = int(cfgm.get("blocked_backoff_jitter", 25))
                        except Exception:
                            bmin, bmax, bjit = 90, 900, 25

                        delay = min(bmax, bmin * (2 ** max(0, blocked_streak - 1)))
                        delay = float(delay + random.randint(0, max(0, bjit)))
                        override_delay = delay
                    else:
                        blocked_streak = 0
                        try:
                            udelay = int(cfgm.get("unknown_backoff", 30))
                        except Exception:
                            udelay = 30
                        override_delay = float(max(5, udelay))

            except Exception as e:
                print(f"Error in monitor loop for {username}: {e}")

            last_state = cur
            base_interval = normalize_interval(interval)
            if override_delay is not None:
                next_run = time.time() + float(override_delay)
            else:
                next_run = next_run + float(base_interval)
            sleep_for = max(0.0, next_run - time.time())
            await asyncio.sleep(sleep_for)

    task = asyncio.create_task(loop())
    with monitors_lock:
        if monitor_key in monitors:
            monitors[monitor_key]["task"].cancel()
        monitors[monitor_key] = {
            "task": task,
            "stop_event": stop,
            "started_at": started_at.isoformat(),
            "start_time": final_start_time,
            "channel_id": channel_id,
            "owner_id": owner_id,
            "reason": reason,
            "label": label,
            "trader_name": trader_name,
        }
    return task




def stop_monitor(username: str, owner_id: int):
    monitor_key = f"{owner_id}:{username}"
    with monitors_lock:
        monitor_data = monitors.pop(monitor_key, None)
        if monitor_data:
            monitor_data["stop_event"].set()
            if "task" in monitor_data: monitor_data["task"].cancel()
            return True
        return False


# -------- Monitor watchdog (24/7 stability) --------
async def monitor_watchdog():
    # Restarts any monitor task that died unexpectedly
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await asyncio.sleep(30)
            to_restart = []
            with monitors_lock:
                for key, rec in list(monitors.items()):
                    t = rec.get("task")
                    stop_ev = rec.get("stop_event")
                    if not t:
                        continue
                    if t.done() and (stop_ev and not stop_ev.is_set()):
                        to_restart.append(rec.copy())
            # Restart outside the lock
            for rec in to_restart:
                try:
                    start_monitor(
                        bot,
                        rec.get("username"),
                        rec.get("interval", 60),
                        rec.get("channel_id"),
                        rec.get("trader_name", ""),
                        int(rec.get("owner_id") or 0),
                        rec.get("reason", "ID"),
                        rec.get("label", "ID"),
                        rec.get("start_time"),
                    )
                except Exception:
                    pass
        except Exception:
            # never crash watchdog
            pass

trader_monitors, trader_channels, trader_brands, authorized_users, authorized_admins = {}, {}, {}, {}, []
store_lock = threading.Lock()

def load_store():
    global trader_monitors, trader_channels, trader_brands, authorized_users, authorized_admins
    try:
        if os.path.exists("bot_store.json"):
            with open("bot_store.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                trader_monitors = data.get("trader_monitors") or {}
                trader_channels = data.get("trader_channels", {})
                trader_brands = data.get("trader_brands", {})
                authorized_admins = data.get("authorized_admins") or []
                authorized_users = data.get("authorized_users") or {}
    except Exception: pass


def save_store():
    # Atomic write to avoid corrupt bot_store.json on crashes / power loss
    payload = {
        "trader_monitors": trader_monitors,
        "trader_channels": trader_channels,
        "trader_brands": trader_brands,
        "authorized_users": authorized_users,
        "authorized_admins": authorized_admins,
    }
    tmp = "bot_store.json.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
        os.replace(tmp, "bot_store.json")
    except Exception:
        # fallback (best-effort)
        try:
            with open("bot_store.json", "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=4, ensure_ascii=False)
        except Exception:
            pass


load_config()
# ---- Concurrency tuning (24/7 + up to 500 monitors) ----
_mon = (CONFIG.get("monitor") or {})
try:
    _max_workers = int(_mon.get("max_workers") or _mon.get("max_concurrency") or 80)
except Exception:
    _max_workers = 80
_max_workers = max(10, min(_max_workers, 200))

# Re-create executor now that config is loaded (so config changes apply)
try:
    CHECK_EXECUTOR.shutdown(wait=False, cancel_futures=True)
except Exception:
    pass
CHECK_EXECUTOR = ThreadPoolExecutor(max_workers=_max_workers)

try:
    _inflight = int(_mon.get("max_inflight") or _mon.get("max_concurrency") or _max_workers)
except Exception:
    _inflight = _max_workers
MAX_INFLIGHT_CHECKS = max(5, min(_inflight, _max_workers))

try:
    CHECK_TIMEOUT = int(_mon.get("check_timeout") or 15)
except Exception:
    CHECK_TIMEOUT = 15

TOKEN = CONFIG.get("bot_token")
admin_id = CONFIG.get("admin_id")
intents = discord.Intents.default()
intents.message_content, intents.members = True, True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

@bot.event
async def on_ready():
    RED = "\033[1;31m"
    RESET = "\033[0m"
    banner = rf"""{RED}
    
    _    _       _       _      _       _     
   / \  | |__ __| |_   _| | ___| | __ _| |__  
  / _ \ | '_ \ / _` | | | |/ _ \ |/ _` | '_ \ 
 / ___ \| |_) | (_| | |_| |  __/ | (_| | | | |
/_/   \_\_.__/ \__,_|\__,_|\___|_|\__,_|_| |_|
                                              
    [+] Owner    : Abdulelah
    [+] Instagram: @vsjj
    [+] Status   : Online & Ready 🚀
    {RESET}"""
    print(banner)
    print(f"Logged in as {bot.user}")
    load_store()
    load_proxies()
    # Init concurrency semaphore for monitor checks
    global CHECK_SEM
    if CHECK_SEM is None:
        CHECK_SEM = asyncio.Semaphore(MAX_INFLIGHT_CHECKS)

    # Command sync (avoid duplicates: default = guild only)
    sync_mode = (((CONFIG.get("discord") or {}).get("sync_mode")) or "guild").lower()
    try:
        if sync_mode == "global":
            await bot.tree.sync()
            print("Commands synced (global).")
        else:
            for g in getattr(bot, "guilds", []):
                try:
                    try:
                        bot.tree.copy_global_to(guild=g)
                    except Exception:
                        pass
                    await bot.tree.sync(guild=g)
                except Exception:
                    pass
            print("Commands synced (guild).")
    except Exception as e:
        print(f"Command sync failed: {e}")
    for channel_id_str, recs in list(trader_monitors.items()):
        for label, rec in list(recs.items()):
            username = rec.get("username")
            if not username:
                continue
            start_monitor(bot, username, rec.get("interval"), int(channel_id_str), rec.get("trader"), rec.get("owner_id"), rec.get("reason"), label, rec.get("start_time"))
    total_monitors = sum(len(v) for v in trader_monitors.values())
    print(f"Started {total_monitors} monitors.")
    # 24/7: watchdog to auto-restart monitors if any task dies
    try:
        if not getattr(bot, "_watchdog_started", False):
            bot._watchdog_started = True
            asyncio.create_task(monitor_watchdog())
    except Exception:
        pass
    if total_monitors == 0:
        print("No monitors restored from storage. Use /add to start monitoring.")
        try:
            auto_list = (CONFIG.get("autostart") or {}).get("monitors") or []
            default_iv = (CONFIG.get("monitor") or {}).get("default_interval") or 60
            default_channel_id = None
            if str(admin_id) in trader_channels:
                default_channel_id = trader_channels[str(admin_id)]
            elif trader_channels:
                default_channel_id = next(iter(trader_channels.values()))
            if auto_list and default_channel_id:
                for uname in auto_list:
                    start_monitor(bot, uname, default_iv, int(default_channel_id), (trader_brands.get(str(admin_id)) or {}).get("name"), int(admin_id) if admin_id else 0, "ID", uname, start_timestamp=time.time())
                print(f"Autostarted {len(auto_list)} monitors from config.")
        except Exception as e:
            print(f"Autostart failed: {e}")


def is_admin(user_id: int) -> bool:
    return str(user_id) == str(admin_id) or str(user_id) in authorized_admins

async def check_auth(interaction: discord.Interaction) -> bool:
    if is_admin(interaction.user.id): return True
    uid_str = str(interaction.user.id)
    if uid_str not in authorized_users or time.time() >= authorized_users[uid_str]:
        await interaction.response.send_message("❌ You are not authorized.", ephemeral=True)
        return False
    return True

@bot.tree.command(name="reload_proxies", description="Reload proxies list from file (Admin only)")
async def slash_reload_proxies(interaction: discord.Interaction):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    load_proxies()
    await interaction.response.send_message(f"✅ Reloaded proxies. Count: {len(PROXIES_CACHE)}", ephemeral=True)


@bot.tree.command(name="test_proxies", description="Test proxies and remove broken ones (Admin only)")
@app_commands.describe(sample="How many proxies to test (0 = all)")
async def slash_test_proxies(interaction: discord.Interaction, sample: int = 0):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    load_proxies()
    proxies_list = list(PROXIES_CACHE)
    if not proxies_list:
        return await interaction.followup.send("⚠️ No proxies loaded (proxies.txt empty).", ephemeral=True)

    if sample and sample > 0:
        proxies_list = random.sample(proxies_list, min(int(sample), len(proxies_list)))

    test_url = "https://www.instagram.com/robots.txt"
    timeout = (5, 8)

    def _test_one(raw: str):
        px = _proxy_fmt(raw)
        if not px:
            return raw, False, "bad_format"
        try:
            r = _http_get(test_url, timeout=timeout, headers=get_headers(), proxies={"http": px, "https": px}, allow_redirects=True)
            # If we got ANY HTTP response, the proxy is alive.
            # Only delete on proxy-auth required / proxy gateway errors.
            if r.status_code in (407, 502, 503, 504):
                return raw, False, f"http_{r.status_code}"
            return raw, True, f"http_{r.status_code}"
        except Exception as e:
            return raw, False, str(e)[:60]

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(CHECK_EXECUTOR, _test_one, p) for p in proxies_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    ok, bad = 0, []
    for r in results:
        if isinstance(r, Exception):
            continue
        raw, is_ok, note = r
        if is_ok:
            ok += 1
        else:
            bad.append((raw, note))

    removed = 0
    for raw, note in bad:
        # Mark as bad (will delete after threshold=2); force delete immediately for tester
        with PROXY_LOCK:
            if raw in PROXIES_CACHE:
                try:
                    PROXIES_CACHE.remove(raw)
                    PROXY_STATE.pop(raw, None)
                    removed += 1
                except Exception:
                    pass
    if removed:
        _rewrite_proxies_file()

    msg = f"✅ Proxy test done. Tested: {len(proxies_list)} | OK: {ok} | Removed: {removed} | Remaining: {len(PROXIES_CACHE)}"
    if bad:
        preview = "\n".join([f"- {raw} ({note})" for raw, note in bad[:12]])
        msg += "\n\n⚠️ Removed (preview):\n" + preview
        if len(bad) > 12:
            msg += f"\n... +{len(bad)-12} more"
    await interaction.followup.send(msg, ephemeral=True)



@bot.tree.command(name="add_session", description="Add a new Instagram session ID (Admin only)")
@app_commands.describe(session_id="The Session ID string")
async def slash_add_session(interaction: discord.Interaction, session_id: str):
    await interaction.response.send_message("❌ This command has been disabled.", ephemeral=True)

@bot.tree.command(name="sync", description="Sync slash commands (safe, no duplicates)")
async def slash_sync(interaction: discord.Interaction):
    # Fast response
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    # Default: use config "discord.sync_mode" ("guild" or "global")
    sync_mode = (((CONFIG.get("discord") or {}).get("sync_mode")) or "guild").lower()

    try:
        if sync_mode == "global":
            await bot.tree.sync()
            await interaction.followup.send("✅ Synced global commands.", ephemeral=True)
            return

        # Guild mode (recommended for instant availability + avoids duplicates)
        if not interaction.guild:
            await interaction.followup.send("❌ This command must be used inside a server (guild).", ephemeral=True)
            return

        try:
            bot.tree.copy_global_to(guild=interaction.guild)
        except Exception:
            pass
        await bot.tree.sync(guild=interaction.guild)
        await interaction.followup.send("✅ Synced guild commands.", ephemeral=True)

    except Exception as e:
        try:
            await interaction.followup.send(f"❌ Sync failed: {e}", ephemeral=True)
        except Exception:
            pass

@bot.tree.command(name="add_admin", description="Add a new admin (Admin only)")
@app_commands.describe(user_id="The User ID to add as admin")
async def slash_add_admin(interaction: discord.Interaction, user_id: str):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    with store_lock:
        if user_id not in authorized_admins:
            authorized_admins.append(user_id)
            save_store()
    await interaction.response.send_message(f"✅ Added admin {user_id}", ephemeral=True)

@bot.tree.command(name="del_admin", description="Remove an admin (Admin only)")
@app_commands.describe(user_id="The User ID to remove from admins")
async def slash_del_admin(interaction: discord.Interaction, user_id: str):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    with store_lock:
        if user_id in authorized_admins:
            authorized_admins.remove(user_id)
            save_store()
    await interaction.response.send_message(f"✅ Removed admin {user_id}", ephemeral=True)

@bot.tree.command(name="activate", description="Activate a user subscription (Admin only)")
@app_commands.describe(user_id="User ID", days="Duration in days (default 30)")
async def slash_activate(interaction: discord.Interaction, user_id: str, days: int = 30):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    await interaction.response.defer()
    try:
        uid = int(user_id)
        authorized_users[str(uid)] = time.time() + (days * 86400)
        save_store()
        guild = interaction.guild
        try:
            member = await guild.fetch_member(uid)
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True)
            }
            channel = await guild.create_text_channel(f"trader-{member.display_name.lower()}", overwrites=overwrites)
            with store_lock:
                trader_channels[str(uid)] = channel.id
                save_store()
            embed = discord.Embed(title="**Welcome Trader!**", description=(f"**Hello {member.mention}, your private channel is ready**\n\n" "**Please set up your branding using the slash command :**\n" "**/setup** (interactive) أو **/setup_quick** (روابط)\n\n" "**• Name: Will appear at the top (Author) and footer**\n" "**• Avatar: Small icon next to your name**\n" "**• GIF: Large animation at the bottom**"), color=0x0099FF)
            await send_with_retry(bot, interaction.channel_id, content=member.mention, embed=embed)
            await interaction.followup.send(f"✅ Activated <@{uid}> and created {channel.mention}")
        except Exception as e: await interaction.followup.send(f"✅ Activated <@{uid}> (Channel error: {e})")
    except ValueError: await interaction.followup.send("❌ Invalid ID")

# -------- Interactive Branding Setup (Upload Icon + Optional GIF) --------
async def wait_for_image(interaction: discord.Interaction, prompt_text: str):
    """Wait for the user to upload an image or paste a URL. Returns a DIRECT media URL or None."""
    try:
        if prompt_text:
            await interaction.followup.send(prompt_text, ephemeral=True)

        def check_msg(m: discord.Message):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        msg = await bot.wait_for("message", check=check_msg, timeout=90.0)

        url = None
        if msg.attachments:
            url = msg.attachments[0].url
        else:
            url = (msg.content or "").strip()

        if url:
            url = str(url).strip()
            # Convert page link -> direct media link (stable embeds)
            try:
                url = await resolve_media_url_async(url)
            except Exception:
                pass
            url = canonicalize_media_url(url)
            if url and not is_embed_image_url(url):
                await interaction.followup.send("❌ الرابط ليس صورة/GIF مباشر. ارسل رابط مباشر ينتهي بـ .png/.jpg/.gif أو ارفع الملف هنا.", ephemeral=True)
                return None
        return url
    except asyncio.TimeoutError:
        try:
            await interaction.followup.send("⏳ Timeout. Setup cancelled.", ephemeral=True)
        except Exception:
            pass
        return None


class GifView(discord.ui.View):
    def __init__(self, brand_data: dict):
        super().__init__(timeout=90)
        self.brand_data = brand_data

    @discord.ui.button(label="Yes, Add GIF", style=discord.ButtonStyle.success)
    async def yes_gif(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True)
        url = await wait_for_image(interaction, "🎬 **ارسل الـ GIF/الصورة الآن** (ارفع ملف هنا أو الصق رابط)")
        if not url:
            return
        self.brand_data["image"] = url
        with store_lock:
            trader_brands[str(interaction.user.id)] = self.brand_data
            save_store()
        await interaction.followup.send("✅ تم حفظ الـ GIF/الصورة. جرّب `/test_embed`.", ephemeral=True)
        self.stop()

    @discord.ui.button(label="No, Skip", style=discord.ButtonStyle.secondary)
    async def no_gif(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True)
        self.brand_data["image"] = None
        with store_lock:
            trader_brands[str(interaction.user.id)] = self.brand_data
            save_store()
        await interaction.followup.send("✅ تم حفظ الإعدادات بدون GIF. جرّب `/test_embed`.", ephemeral=True)
        self.stop()


class SetupModal(discord.ui.Modal, title="Branding Setup"):
    name = discord.ui.TextInput(label="Brand Name", placeholder="My Store", required=True, max_length=32)

    async def on_submit(self, interaction: discord.Interaction):
        brand_data = {"name": str(self.name.value).strip(), "icon": None, "image": None}

        # Respond immediately (avoid Discord 3s timeout)
        await interaction.response.send_message(
            f"✅ تم حفظ الاسم: **{brand_data['name']}**\n\n"
            "📸 **الخطوة 2: ارفع الأيقونة/الصورة الآن** (ارفع ملف هنا أو الصق رابط)",
            ephemeral=True
        )

        icon_url = await wait_for_image(interaction, "")
        if not icon_url:
            return

        brand_data["icon"] = icon_url
        with store_lock:
            trader_brands[str(interaction.user.id)] = brand_data
            save_store()

        await interaction.followup.send(
            "🎬 **هل تريد إضافة GIF/صورة أسفل الإمبد؟**",
            view=GifView(brand_data),
            ephemeral=True
        )


@bot.tree.command(name="setup", description="Interactive branding setup (upload icon + optional GIF)")
async def slash_setup(interaction: discord.Interaction):
    if not await check_auth(interaction):
        return
    await interaction.response.send_modal(SetupModal())


@bot.tree.command(name="setup_quick", description="Quick branding setup (paste URLs)")
@app_commands.describe(name="Brand Name", avatar_url="Icon URL (or page link)", gif_url="GIF/Image URL (or page link, optional)")
async def slash_setup_quick(interaction: discord.Interaction, name: str, avatar_url: str, gif_url: str = ""):
    if not await check_auth(interaction):
        return
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=True)
    except Exception:
        pass

    avatar_url2 = await resolve_media_url_async(avatar_url) if avatar_url else avatar_url
    gif_url2 = await resolve_media_url_async(gif_url) if gif_url else gif_url

    with store_lock:
        trader_brands[str(interaction.user.id)] = {"name": name, "icon": avatar_url2, "image": gif_url2 or None}
        save_store()

    await interaction.followup.send(f"✅ Branding setup complete for **{name}**! Try `/test_embed`.", ephemeral=True)

@bot.tree.command(name="renew", description="Renew a user subscription (Admin only)")
@app_commands.describe(user_id="User ID", days="Duration in days (default 30)")
async def slash_renew(interaction: discord.Interaction, user_id: str, days: int = 30):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    try:
        uid_str = str(int(user_id))
        if uid_str in authorized_users:
            authorized_users[uid_str] = max(time.time(), authorized_users[uid_str]) + (days * 86400)
            save_store()
            await interaction.response.send_message(f"✅ Renewed <@{user_id}>")
        else: await interaction.response.send_message("⚠️ Not active", ephemeral=True)
    except ValueError: await interaction.response.send_message("❌ Invalid ID", ephemeral=True)

@bot.tree.command(name="deactivate", description="Deactivate a user (Admin only)")
@app_commands.describe(user_id="User ID")
async def slash_deactivate(interaction: discord.Interaction, user_id: str):
    if not is_admin(interaction.user.id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    try:
        uid_str = str(int(user_id))
        if uid_str in authorized_users:
            del authorized_users[uid_str]
            save_store()
            await interaction.response.send_message(f"🚫 Deactivated <@{user_id}>")
        else: await interaction.response.send_message("⚠️ User not found in active list", ephemeral=True)
    except ValueError: await interaction.response.send_message("❌ Invalid ID", ephemeral=True)

@bot.tree.command(name="set_brand", description="Update brand name and icon")
@app_commands.describe(name="New Brand Name", icon_url="New Icon URL")
async def slash_set_brand(interaction: discord.Interaction, name: str, icon_url: str):
    if not await check_auth(interaction): return
    with store_lock:
        trader_brands[str(interaction.user.id)] = {"name": name, "icon": icon_url}
        save_store()
    await interaction.response.send_message("✅ Brand updated")

@bot.tree.command(name="set_name", description="Update brand name")
@app_commands.describe(name="New Brand Name")
async def slash_set_name(interaction: discord.Interaction, name: str):
    if not await check_auth(interaction): return
    with store_lock:
        brand = trader_brands.get(str(interaction.user.id)) or {}
        brand["name"] = name
        trader_brands[str(interaction.user.id)] = brand
        save_store()
    await interaction.response.send_message(f"✅ Name updated to **{name}**")

@bot.tree.command(name="set_avatar", description="Update brand avatar")
@app_commands.describe(icon_url="New Icon URL (or page link)")
async def slash_set_avatar(interaction: discord.Interaction, icon_url: str):
    if not await check_auth(interaction):
        return
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=True)
    except Exception:
        pass

    icon_url2 = await resolve_media_url_async(icon_url) if icon_url else icon_url

    with store_lock:
        brand = trader_brands.get(str(interaction.user.id)) or {}
        brand["icon"] = icon_url2
        trader_brands[str(interaction.user.id)] = brand
        save_store()

    await interaction.followup.send("✅ Avatar updated")

@bot.tree.command(name="set_image", description="Update brand GIF/Image")
@app_commands.describe(image_url="New Image/GIF URL (or page link)")
async def slash_set_image(interaction: discord.Interaction, image_url: str):
    if not await check_auth(interaction):
        return
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=True)
    except Exception:
        pass

    image_url2 = await resolve_media_url_async(image_url) if image_url else image_url

    with store_lock:
        brand = trader_brands.get(str(interaction.user.id)) or {}
        brand["image"] = image_url2
        trader_brands[str(interaction.user.id)] = brand
        save_store()

    await interaction.followup.send("✅ Image updated")

@bot.tree.command(name="fix_media", description="Fix saved icon/gif links (convert page links to direct URLs)")
async def slash_fix_media(interaction: discord.Interaction):
    if not await check_auth(interaction):
        return
    await interaction.response.defer(thinking=True)
    uid = str(interaction.user.id)
    brand = trader_brands.get(uid) or {}
    changed = False

    if brand.get("icon"):
        new_icon = await resolve_media_url_async(str(brand["icon"]))
        if new_icon != brand["icon"]:
            brand["icon"] = new_icon
            changed = True

    if brand.get("image"):
        new_img = await resolve_media_url_async(str(brand["image"]))
        if new_img != brand["image"]:
            brand["image"] = new_img
            changed = True

    if changed:
        with store_lock:
            trader_brands[uid] = brand
            save_store()
        await interaction.followup.send("✅ Fixed. Try `/test_embed` now.")
    else:
        await interaction.followup.send("✅ Nothing to fix (links already look direct).")


@bot.tree.command(name="set", description="Bind your channel")
async def slash_set(interaction: discord.Interaction):
    if not await check_auth(interaction): return
    with store_lock:
        trader_channels[str(interaction.user.id)] = interaction.channel_id
        save_store()
    await interaction.response.send_message(f"Channel set to <#{interaction.channel_id}>")

@bot.tree.command(name="help", description="Show help menu")
async def slash_help(interaction: discord.Interaction):
    brand = trader_brands.get(str(interaction.user.id)) or {}
    embed = discord.Embed(title="**Help Menu**", description="**Bot Commands & Usage**", color=0x8B0000)
    embed.add_field(name="**Setup**", value="`/setup [Name] [Icon] [Gif]`\n`/set` (Bind Channel)", inline=False)
    embed.add_field(name="**Monitoring**", value="`/add [User]`\n`/list`\n`/remove [User]`\n`/clearchat`", inline=False)
    embed.add_field(name="**Branding**", value="`/set_name [Name]`\n`/set_avatar [URL]`\n`/set_image [URL]`", inline=False)
    embed.add_field(name="**Status**", value="**System Online**", inline=False)
    if admin_id and interaction.user.id == int(admin_id):
        embed.add_field(name="**Admin**", value="`/activate` `/renew` `/deactivate`\n`/sync` `/health`", inline=False)
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.set_footer(text=f"Requested by @{interaction.user.display_name}", icon_url=interaction.user.display_avatar.url)
    embed.timestamp = datetime.now()
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="admin", description="Show admin dashboard (Admin only)")
async def slash_admin(interaction: discord.Interaction):
    if admin_id and interaction.user.id != admin_id:
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    embed = discord.Embed(title="**Admin Dashboard**", description="**User Management**\n`/activate [ID] [Days]`\n`/renew [ID] [Days]`\n`/deactivate [ID]`\n\n**System**\n`/sync` - Sync Commands\n`/health` - System Status", color=0x8B0000)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="health", description="Show system health (Admin only)")
async def slash_health(interaction: discord.Interaction):
    if admin_id and interaction.user.id != int(admin_id):
        await interaction.response.send_message("❌ Not authorized", ephemeral=True)
        return
    with monitors_lock: monitor_count = len(monitors)
    worker_count = getattr(CHECK_EXECUTOR, "_max_workers", 0)
    proxy_count = len(PROXIES_CACHE)
    brand = trader_brands.get(str(interaction.user.id)) or {}
    brand_name = brand.get("name") or interaction.user.display_name
    brand_icon = canonicalize_media_url(brand.get("icon") or "") or interaction.user.display_avatar.url
    embed = discord.Embed(title="**System Health**", description="**Runtime Performance**", color=0x8B0000)
    embed.set_author(name=brand_name, icon_url=brand_icon)
    embed.add_field(name="**Monitors**", value=f"`{monitor_count}` active", inline=True)
    embed.add_field(name="**Workers**", value=f"`{worker_count}` threads", inline=True)
    embed.add_field(name="**Proxies**", value=f"`{proxy_count}` loaded", inline=True)
    embed.add_field(name="**Status**", value="**Online & Stable**", inline=False)
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.set_footer(text=f"Requested by @{interaction.user.display_name}", icon_url=interaction.user.display_avatar.url)
    embed.timestamp = datetime.now()
    await interaction.response.send_message(embed=embed)

class ReasonView(discord.ui.View):
    def __init__(self, label: str, username: str, interval: int, trader: str | None):
        super().__init__(timeout=60)
        self.label, self.username, self.interval, self.trader = label, username, interval, trader
    @discord.ui.button(label="ID", style=discord.ButtonStyle.primary)
    async def button_id(self, interaction: discord.Interaction, button: discord.ui.Button): await self.button_callback(interaction, "ID")
    @discord.ui.button(label="Logout", style=discord.ButtonStyle.secondary)
    async def button_logout(self, interaction: discord.Interaction, button: discord.ui.Button): await self.button_callback(interaction, "Logout")
    async def button_callback(self, interaction: discord.Interaction, reason: str):
        await interaction.response.defer()
        cid = str(interaction.channel_id)
        current_ts = time.time()
        with store_lock:
            recs = trader_monitors.get(cid) or {}
            recs[self.label] = {"username": self.username, "interval": self.interval, "trader": self.trader, "reason": reason, "start_time": current_ts, "channel_id": interaction.channel_id, "owner_id": interaction.user.id}
            trader_monitors[cid] = recs
            save_store()
        start_monitor(bot, self.username, self.interval, interaction.channel_id, self.trader or interaction.user.display_name, interaction.user.id, reason, self.label, start_timestamp=current_ts)
        msg = await interaction.followup.send(f"Monitor started for **@{self.username}**.\nReason: **{reason}**")
        try: await msg.delete(delay=60)
        except: pass
        self.stop()


class AddReasonView(discord.ui.View):
    """Reason selector shown after /add. Monitoring starts only after the user chooses a reason."""
    def __init__(self, bot: discord.Client, username: str, interval: int, channel_id: int, channel_name: str,
                 owner_id: int, trader_name: str, label: str):
        super().__init__(timeout=60)
        self.bot = bot
        self.username = username
        self.interval = interval
        self.channel_id = int(channel_id)
        self.channel_name = channel_name
        self.owner_id = int(owner_id)
        self.trader_name = trader_name
        self.label = label
        self._key = f"{self.owner_id}:{self.username}"

    async def on_timeout(self):
        # Clean pending state if user didn't pick a reason in time
        try:
            with pending_adds_lock:
                pending_adds.pop(self._key, None)
        except Exception:
            pass
        try:
            for item in self.children:
                item.disabled = True
        except Exception:
            pass

    async def _start(self, interaction: discord.Interaction, reason_label: str):
        # Permission: only requester (or admin) can pick
        if interaction.user.id != self.owner_id and not is_admin(interaction.user.id):
            return await interaction.response.send_message("❌ Not authorized.", ephemeral=True)

        current_ts = time.time()
        cid = str(self.channel_id)

        with store_lock:
            recs = trader_monitors.get(cid) or {}
            recs[self.username] = {
                "username": self.username,
                "interval": int(self.interval),
                "trader": self.trader_name,
                "reason": reason_label,
                "start_time": current_ts,
                "channel_id": self.channel_id,
                "owner_id": self.owner_id,
            }
            trader_monitors[cid] = recs
            save_store()

        start_monitor(
            self.bot,
            self.username,
            int(self.interval),
            self.channel_id,
            self.channel_name,
            self.owner_id,
            reason_label,
            self.label,
            start_timestamp=current_ts
        )

        with pending_adds_lock:
            pending_adds.pop(self._key, None)

        msg = f"✅ Added @{self.username}. Monitoring started.\nReason: **{reason_label}**"
        try:
            await interaction.response.edit_message(content=msg, view=None)
        except Exception:
            try:
                await interaction.followup.send(msg)
            except Exception:
                pass
        self.stop()

    @discord.ui.button(label="ID", style=discord.ButtonStyle.primary)
    async def pick_id(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._start(interaction, "ID")

    @discord.ui.button(label="Logout", style=discord.ButtonStyle.danger)
    async def pick_logout(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._start(interaction, "Logout")

@bot.tree.command(name="add", description="Start monitoring a user")
@app_commands.describe(username="Instagram Username or URL")
async def slash_add(interaction: discord.Interaction, username: str):
    if not await check_auth(interaction): return
    try:
        if not interaction.response.is_done():
            await interaction.response.defer()
    except Exception:
        pass
    u = extract_username(username)
    if f"{interaction.user.id}:{u}" in monitors:
        await interaction.followup.send(f"✅ @{u} is already added and being monitored")
        return
    
    loop = asyncio.get_running_loop()
    pre = await loop.run_in_executor(CHECK_EXECUTOR, check, u)
    state = pre.get("state")
    if state == "available":
        await interaction.followup.send(f"❌ Already active: @{u}")
        return
    # Treat unknown as possibly banned due to network/proxy issues – proceed to add
    
    brand = trader_brands.get(str(interaction.user.id)) or {}
    customer_name = brand.get("name") or interaction.user.display_name
    default_interval = 60
    
    cid = str(interaction.channel_id)

    monitor_key = f"{interaction.user.id}:{u}"
    with pending_adds_lock:
        if monitor_key in pending_adds:
            await interaction.followup.send(f"⏳ Pending reason selection for @{u}. Please choose **ID** or **Logout** from the previous message.")
            return
        pending_adds[monitor_key] = time.time()

    # Do NOT start monitoring until user picks a reason (ID / Logout)
    view = AddReasonView(
        bot=bot,
        username=u,
        interval=default_interval,
        channel_id=interaction.channel_id,
        channel_name=getattr(interaction.channel, "name", str(interaction.channel_id)),
        owner_id=interaction.user.id,
        trader_name=customer_name,
        label=u,
    )
    await interaction.followup.send(f"اختر الريزون لـ @{u}:", view=view)


@bot.tree.command(name="reason", description="Change reason for a monitored username")
@app_commands.describe(username="Instagram username", reason="Reason label", label="Optional label (if you have multiple)")
async def slash_reason(interaction: discord.Interaction, username: str, reason: str, label: str = None):
    await interaction.response.defer(ephemeral=True)

    cid = str(interaction.channel_id)
    if cid not in trader_monitors or not trader_monitors[cid]:
        return await interaction.followup.send("❌ No monitors found in this channel.", ephemeral=True)

    uname = extract_username(username).lower()
    updated = 0

    # Update stored monitors in this channel
    with store_lock:
        recs = trader_monitors.get(cid, {})
        for lbl, rec in list(recs.items()):
            if not rec or (rec.get("username") or "").lower() != uname:
                continue
            if label and lbl != label:
                continue

            # Permission: owner or admin
            if interaction.user.id != int(rec.get("owner_id") or 0) and not is_admin(interaction.user.id):
                continue

            rec["reason"] = reason
            trader_monitors[cid][lbl] = rec
            updated += 1

    # Update running monitor metadata
    with monitors_lock:
        for key, mrec in list(monitors.items()):
            if (mrec.get("username") or "").lower() != uname:
                continue
            if label and mrec.get("label") != label:
                continue
            if interaction.user.id != int(mrec.get("owner_id") or 0) and not is_admin(interaction.user.id):
                continue
            mrec["reason"] = reason
            monitors[key] = mrec

    if updated:
        save_store()
        await interaction.followup.send(f"✅ Updated reason for **@{uname}** to **{reason}**" + (f" (label: {label})" if label else ""), ephemeral=True)
    else:
        await interaction.followup.send("❌ Couldn't update. Either the monitor wasn't found, label mismatch, or you don't own it.", ephemeral=True)

@bot.tree.command(name="remove", description="Stop monitoring a user")
@app_commands.describe(target="Username to remove")
async def slash_remove(interaction: discord.Interaction, target: str):
    if not await check_auth(interaction): return
    u = extract_username(target)
    if stop_monitor(u, interaction.user.id):
        with store_lock:
            cid = str(interaction.channel_id)
            if cid in trader_monitors:
                recs = trader_monitors[cid]
                label_to_remove = next((k for k, v in recs.items() if v.get("username") == u), None)
                if label_to_remove:
                    del recs[label_to_remove]
                    save_store()
        await interaction.response.send_message(f"✅ Removed @{u}")
    else: await interaction.response.send_message(f"❌ @{u} not found in your monitors.")

@bot.tree.command(name="list", description="List your active monitors")
async def slash_list(interaction: discord.Interaction):
    if not await check_auth(interaction): return
    cid = str(interaction.channel_id)
    recs = trader_monitors.get(cid) or {}
    if not recs:
        await interaction.response.send_message("No active monitors in this channel.")
        return
    lines = []
    for label, rec in recs.items():
        u = rec.get("username")
        start_ts = rec.get("start_time")
        elapsed = seconds_to_hm(int(time.time() - start_ts)) if start_ts else "N/A"
        lines.append(f"• **@{u}** (Started {elapsed} ago)")
    embed = discord.Embed(title="**Active Monitors**", description="\n".join(lines), color=0x8B0000)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="clearchat", description="Clear chat messages (except alerts)")
async def slash_clearchat(interaction: discord.Interaction, amount: int = 100):
    if not await check_auth(interaction): return
    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=amount, check=lambda m: len(m.embeds) == 0)
    await interaction.followup.send(f"🧹 Cleared {len(deleted)} messages.", ephemeral=True)


def seconds_to_hm(s: int) -> str:
    m = s // 60
    h = m // 60
    m = m % 60
    return f"{h}h {m}m" if h > 0 else f"{m}m"


def build_unban_embed_discord(info: dict, elapsed_seconds: int, owner_name: str = None, reason: str = None, brand: dict = None):
    username = info.get("username", "Unknown")

    # Dark red theme
    e = discord.Embed(title="**Account Unbanned!**", color=0x8B0000)

    # Use data we already have (no extra IG calls here -> instant sending)
    pic = info.get("profile_pic") or info.get("profile_pic_url")
    if pic and str(pic).startswith("http"):
        e.set_thumbnail(url=pic)

    # Branding (only set URLs if they look usable)
    if brand and brand.get("name"):
        icon = canonicalize_media_url(brand.get("icon") or "")
        if icon and str(icon).startswith("http") and _is_direct_media(str(icon)):
            e.set_author(name=brand["name"], icon_url=str(icon))
        else:
            e.set_author(name=brand["name"])

    followers = info.get("followers")
    following = info.get("following")
    posts = info.get("posts")

    # Keep fields stable even if counts are missing
    if followers is None:
        followers = "N/A"
    if following is None:
        following = "N/A"
    if posts is None:
        posts = "N/A"

    e.description = f"**@{username}** has been successfully restored."
    e.add_field(name="**Followers**", value=f"**{followers}**", inline=True)
    e.add_field(name="**Reason**", value=f"**{reason or 'ID'}**", inline=True)
    e.add_field(name="**Time Taken**", value=f"**{seconds_to_hm(elapsed_seconds)}**", inline=True)
    e.add_field(name="**Status**", value="**ACTIVE**", inline=True)
    e.add_field(name="**Profile Link**", value=f"**https://www.instagram.com/{username}/**", inline=False)

    img = (brand or {}).get("image")
    if img and str(img).startswith("http") and _is_direct_media(str(img)):
        e.set_image(url=str(img))

    e.set_footer(text=f"@{owner_name}" if owner_name else "Instagram Unban")
    e.timestamp = datetime.now()
    return e


@bot.tree.command(name="test_embed", description="Preview the unban alert")
async def slash_test_embed(interaction: discord.Interaction):
    if not await check_auth(interaction):
        return
    await interaction.response.defer(thinking=True)

    brand = trader_brands.get(str(interaction.user.id)) or {}
    changed = False
    if brand.get("icon"):
        icon2 = await resolve_media_url_async(str(brand["icon"]))
        if icon2 != brand["icon"]:
            brand["icon"] = icon2
            changed = True
    if brand.get("image"):
        img2 = await resolve_media_url_async(str(brand["image"]))
        if img2 != brand["image"]:
            brand["image"] = img2
            changed = True
    if changed:
        with store_lock:
            trader_brands[str(interaction.user.id)] = brand
            save_store()

    dummy_info = {
        "username": "instagram",
        "url": "https://www.instagram.com/instagram",
        "profile_pic": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a5/Instagram_icon.png/2048px-Instagram_icon.png",
        "followers": "N/A",
        "following": "N/A",
        "posts": "N/A",
    }
    elapsed = 3600 + 150
    owner_name = brand.get("name") or interaction.user.display_name
    embed = build_unban_embed_discord(dummy_info, elapsed, owner_name=owner_name, reason="ID", brand=brand)
    await interaction.followup.send("This is a preview of the unban alert:", embed=embed)

if __name__ == "__main__":
    if TOKEN: bot.run(TOKEN)
    else: print("Error: bot_token not found.")
