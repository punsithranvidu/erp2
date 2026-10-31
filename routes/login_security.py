"""Login-only, shared-device-independent throttling backed by PostgreSQL.

A signed, random browser cookie survives IP changes. It is an identifier, not
proof of physical hardware: clients that discard it can obtain a new identity.
IP and header hashes are audit signals only, never office-wide lockout keys.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import math
import re
import secrets
import time

from flask import current_app, g, request
from itsdangerous import BadSignature, URLSafeTimedSerializer

COOKIE_NAME = "erp_login_device"
COOKIE_AGE = 180 * 24 * 60 * 60
ATTEMPT_LIMIT = 4
LOCK_SECONDS = (15 * 60, 30 * 60, 60 * 60)
HISTORY_SECONDS = 24 * 60 * 60


def init_login_security(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS login_device_limits (
            device_key TEXT PRIMARY KEY,
            failures INTEGER NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 0,
            blocked_until DOUBLE PRECISION NOT NULL DEFAULT 0,
            updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS login_device_limits_updated_idx
        ON login_device_limits (updated_at)
    """)


class PostgresLoginStore:
    def __init__(self, connect):
        self.connect = connect

    @contextmanager
    def locked(self, key):
        # Hold the row lock through credential verification so simultaneous
        # requests cannot all pass the last available attempt across workers.
        with self.connect() as conn:
            conn.execute("""
                INSERT INTO login_device_limits (device_key, updated_at)
                VALUES (%s, %s) ON CONFLICT (device_key) DO NOTHING
            """, (key, time.time()))
            state = dict(conn.execute("""
                SELECT failures, level, blocked_until, updated_at
                FROM login_device_limits WHERE device_key=%s FOR UPDATE
            """, (key,)).fetchone())
            yield state
            conn.execute("""
                UPDATE login_device_limits
                SET failures=%s, level=%s, blocked_until=%s, updated_at=%s
                WHERE device_key=%s
            """, (state["failures"], state["level"], state["blocked_until"],
                  state["updated_at"], key))
            # Bounded retention: remove obsolete records occasionally, using
            # the timestamp index. Never retain passwords or raw fingerprints.
            if secrets.randbelow(100) == 0:
                conn.execute("DELETE FROM login_device_limits WHERE updated_at < %s",
                             (time.time() - 7 * HISTORY_SECONDS,))


@dataclass
class LoginResult:
    user: object = None
    error: str = None
    retry_after: int = 0
    attempts_left: int = ATTEMPT_LIMIT
    verified_attempt: bool = False


class LoginProtection:
    def __init__(self, store, audit, clock=time.time):
        self.store = store
        self.audit = audit
        self.clock = clock

    def check(self, key, authenticate=None, username=None, password=None):
        with self.store.locked(key) as state:
            now = self.clock()
            if now - state["updated_at"] >= HISTORY_SECONDS and state["blocked_until"] <= now:
                state.update(failures=0, level=0, blocked_until=0)
            remaining = math.ceil(state["blocked_until"] - now)
            if remaining > 0:
                # Blocked requests do not extend the punishment indefinitely.
                return LoginResult(error="Too many failed attempts on this browser.",
                                   retry_after=remaining, attempts_left=0)
            if authenticate is None:
                return LoginResult(attempts_left=ATTEMPT_LIMIT - state["failures"])
            user, error = authenticate(username, password)
            state["updated_at"] = now
            if not error:
                state.update(failures=0, level=0, blocked_until=0)
                return LoginResult(user=user, verified_attempt=True)
            state["failures"] += 1
            left = ATTEMPT_LIMIT - state["failures"]
            self.audit("failed_login", device=key[:16], attempts_left=left)
            if left <= 0:
                duration = LOCK_SECONDS[min(state["level"], len(LOCK_SECONDS) - 1)]
                state.update(failures=0, level=state["level"] + 1,
                             blocked_until=now + duration)
                self.audit("login_browser_blocked", device=key[:16], seconds=duration)
                return LoginResult(error="Too many failed attempts on this browser.",
                                   retry_after=duration, attempts_left=0, verified_attempt=True)
            suffix = "attempt" if left == 1 else "attempts"
            return LoginResult(error=f"Invalid username or password. {left} {suffix} left.",
                               attempts_left=left, verified_attempt=True)


def serializer():
    return URLSafeTimedSerializer(current_app.secret_key, salt="erp-login-device-v1")


def browser_key():
    token = request.cookies.get(COOKIE_NAME, "")
    try:
        identity = serializer().loads(token, max_age=COOKIE_AGE)
        if not isinstance(identity, str) or not re.fullmatch(r"[a-f0-9]{64}", identity):
            raise BadSignature("Invalid device identifier")
    except BadSignature:
        identity = secrets.token_hex(32)
        g.new_login_device = serializer().dumps(identity)
        g.login_device_missing = True
    return hashlib.sha256(identity.encode()).hexdigest()


def set_device_cookie(response):
    if getattr(g, "new_login_device", None):
        response.set_cookie(COOKIE_NAME, g.new_login_device, max_age=COOKIE_AGE,
                            secure=current_app.config.get("SESSION_COOKIE_SECURE", True),
                            httponly=True, samesite="Lax", path="/login")
    return response


def audit_signals():
    # Browser headers are spoofable: retain only a short hash for log correlation.
    value = "|".join(request.headers.get(name, "")[:512] for name in
                     ("User-Agent", "Accept-Language", "Sec-CH-UA", "Sec-CH-UA-Platform"))
    return hashlib.sha256(value.encode()).hexdigest()[:16]
