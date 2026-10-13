"""Small HTTP helpers on the standard library, with retries for network errors only."""

from __future__ import annotations

import datetime as dt
import email.utils
import shutil
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from pipeline.schema import SourceError

USER_AGENT = "days-out-early-vote-tracker/1.0 (+https://github.com/PleaseRuiz/early-vote-2026)"
TIMEOUT = 120
RETRIES = 4


@dataclass
class Meta:
    url: str
    last_modified: dt.datetime
    etag: str
    size: int | None


def _meta(url: str, headers) -> Meta:
    lm = headers.get("Last-Modified")
    if not lm:
        raise SourceError(f"{url} returned no Last-Modified header")
    parsed = email.utils.parsedate_to_datetime(lm)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    size = headers.get("Content-Length")
    return Meta(url=url, last_modified=parsed, etag=(headers.get("ETag") or "").strip('"'),
                size=int(size) if size and size.isdigit() else None)


def _open(url: str, method: str = "GET"):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})
    last_exc: Exception | None = None
    for attempt in range(RETRIES):
        try:
            return urllib.request.urlopen(req, timeout=TIMEOUT)
        except urllib.error.HTTPError as exc:
            if exc.code in (500, 502, 503, 504) and attempt < RETRIES - 1:
                last_exc = exc
            else:
                raise SourceError(f"{url} returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_exc = exc
        time.sleep(2 ** (attempt + 1))
    raise SourceError(f"could not reach {url}: {last_exc}")


def head(url: str) -> Meta:
    with _open(url, "HEAD") as resp:
        return _meta(url, resp.headers)


def download(url: str, dest: Path) -> Meta:
    with _open(url) as resp:
        meta = _meta(url, resp.headers)
        with dest.open("wb") as out:
            shutil.copyfileobj(resp, out, length=1 << 20)
    size = dest.stat().st_size
    if meta.size is not None and size != meta.size:
        raise SourceError(f"{url}: downloaded {size:,} bytes, expected {meta.size:,}")
    if size == 0:
        raise SourceError(f"{url}: empty response")
    return meta


def get_text(url: str, encoding: str = "utf-8") -> tuple[str, Meta]:
    with _open(url) as resp:
        meta = _meta(url, resp.headers)
        body = resp.read()
    if meta.size is not None and len(body) != meta.size:
        raise SourceError(f"{url}: got {len(body):,} bytes, expected {meta.size:,}")
    return body.decode(encoding), meta
