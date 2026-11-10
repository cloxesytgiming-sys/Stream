#!/usr/bin/env python3
"""Discord Stream Time Engine — core library (imported by bot.py)."""
from __future__ import annotations

import base64
import json
import os
import random
import select
import socket
import ssl
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "data"

ME_URL = "https://discord.com/api/v9/users/@me?with_analytics_token=true"
USERS_URL = "https://discord.com/api/v9/users"
PROPERTIES_URL = "https://cordapi.dolfi.es/api/v2/properties/windows"
GATEWAY_URL = "gateway.discord.gg"

CLIENT_VERSION = "1.0.9253"
CLIENT_BUILD_NUMBER = 594031
NATIVE_BUILD_NUMBER = 88414
OS_VERSION = "10.0.26200"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) discord/1.0.9253 Chrome/148.0.7778.280 "
    "Electron/42.7.1 Safari/537.36"
)

AUTH_MAX_AGE = 12 * 3600

STREAM_TIERS = [
    (1, "Newcomer", 1), (2, "Fledgling", 5), (3, "Breakout", 20),
    (4, "Standout", 75), (5, "Trendsetter", 150), (6, "Headliner", 300),
    (7, "Star", 500), (8, "Sensation", 1000), (9, "Visionary", 2000),
    (10, "Phenomenon", 5000),
]

OPUS_SILENCE = b"\xf8\xff\xfe"

PERMISSION_ADMINISTRATOR = 1 << 3
PERMISSION_STREAM        = 1 << 9
PERMISSION_VIEW_CHANNEL  = 1 << 10
PERMISSION_CONNECT       = 1 << 20


def _state_path(user_id: int) -> Path:
    STATE_DIR.mkdir(exist_ok=True)
    return STATE_DIR / f"{user_id}.json"


def _user_id_from_token(token: str) -> str:
    try:
        first = token.split(".", 1)[0]
        pad = "=" * (-len(first) % 4)
        return base64.b64decode(first + pad).decode("utf-8")
    except Exception:
        return ""


def _get_random_game():
    try:
        path = ROOT / "games.txt"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if ":" in line]
                if lines:
                    choice = random.choice(lines)
                    appid, name = choice.split(":", 1)
                    return appid.strip(), name.strip()
    except Exception:
        pass
    return None, None


def enable_personalization(token: str, super_props: str) -> bool:
    url = "https://discord.com/api/v9/users/@me/settings"
    payload = json.dumps({
        "consent": {"personalization": True, "usage_statistics": True}
    }).encode("utf-8")
    headers = {
        "authorization": token, "user-agent": USER_AGENT,
        "x-super-properties": super_props, "content-type": "application/json"
    }
    req = urllib.request.Request(url, method="PATCH", data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        return False


class Console:
    RED = "\x1b[91m"
    ORANGE = "\x1b[38;5;208m"
    GREEN = "\x1b[92m"
    YELLOW = "\x1b[93m"
    CYAN = "\x1b[96m"
    WHITE = "\x1b[97m"
    GREY = "\x1b[90m"
    MAGENTA = "\x1b[95m"
    RESET = "\x1b[0m"
    RULE = "─" * 48

    def __init__(self) -> None:
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        if sys.platform == "win32":
            self._enable_ansi()

    @staticmethod
    def _enable_ansi() -> None:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)

    def _stamp(self) -> str:
        return f"{self.GREY}{time.strftime('%H:%M:%S')}{self.RESET}"

    def _line(self, glyph, target, detail, color):
        print(f"  {self._stamp()} {glyph} {self.WHITE}{target}{self.RESET} {self.GREY}·{self.RESET} {color}{detail}{self.RESET}")

    def ok(self, t, d):    self._line(f"{self.GREEN}✓{self.RESET}", t, d, self.GREEN)
    def sent(self, t, d):  self._line(f"{self.CYAN}↑{self.RESET}", t, d, self.CYAN)
    def live(self, t, d):  self._line(f"{self.MAGENTA}●{self.RESET}", t, d, self.MAGENTA)
    def warn(self, t, d):  self._line(f"{self.YELLOW}!{self.RESET}", t, d, self.YELLOW)
    def err(self, t, d):   self._line(f"{self.RED}✗{self.RESET}", t, d, self.RED)
    def info(self, d):     print(f"  {self._stamp()} {self.GREY}·{self.RESET} {self.WHITE}{d}{self.RESET}")
    def rule(self):        print(f"  {self.GREY}{self.RULE}{self.RESET}")
    def clear(self):       print("\x1b[2J\x1b[3J\x1b[H", end="")


@dataclass
class State:
    token: str = ""
    cookie: str = ""
    fingerprint: str = ""
    analytics_token: str = ""
    fetched_at: int = 0
    total_stream_hours_claimed: float = 0.0
    total_stream_sessions: int = 0
    last_guild_id: str = ""
    last_channel_id: str = ""
    viewer_tokens: list = field(default_factory=list)

    @classmethod
    def load(cls, user_id: int) -> "State":
        path = _state_path(user_id)
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                known = set(cls().__dict__.keys())
                return cls(**{k: v for k, v in data.items() if k in known})
            except Exception:
                pass
        return cls()

    def save(self, user_id: int) -> None:
        path = _state_path(user_id)
        existing = {}
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
        existing.update(asdict(self))
        path.write_text(json.dumps(existing, indent=2), encoding="utf-8")

    @property
    def is_stale(self):   return not self.analytics_token or (time.time() - self.fetched_at) > AUTH_MAX_AGE
    @property
    def has_cookie(self): return bool(self.cookie.strip())
    @property
    def has_token(self):  return bool(self.token.strip())


def _local_props():
    return {
        "os": "Windows", "browser": "Discord Client", "release_channel": "stable",
        "client_version": CLIENT_VERSION, "os_version": OS_VERSION,
        "os_arch": "x64", "app_arch": "x64", "system_locale": "en-US",
        "has_client_mods": False, "browser_user_agent": USER_AGENT,
        "browser_version": "42.7.1", "os_sdk_version": "26200",
        "client_build_number": CLIENT_BUILD_NUMBER,
        "native_build_number": NATIVE_BUILD_NUMBER,
        "client_event_source": None, "client_app_state": "focused",
    }


def build_super_props(launch_signature, heartbeat_session):
    try:
        req = urllib.request.Request(
            PROPERTIES_URL, method="POST", data=b"{}",
            headers={"content-type": "application/json", "user-agent": USER_AGENT},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            props = json.loads(resp.read().decode("utf-8"))["properties"]
    except Exception:
        props = _local_props()
    props["client_launch_id"] = str(uuid.uuid4())
    props["launch_signature"] = launch_signature
    props["client_heartbeat_session_id"] = heartbeat_session
    raw = json.dumps(props, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii"), props


def _read_analytics_token(auth_token, super_props):
    req = urllib.request.Request(
        ME_URL,
        headers={"authorization": auth_token, "user-agent": USER_AGENT,
                 "x-super-properties": super_props},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"token rejected (HTTP {exc.code})") from exc
    token = data.get("analytics_token")
    if not token:
        raise RuntimeError("no analytics_token returned by discord")
    return token


def ensure_token(state: State, user_id: int, super_props: str) -> None:
    if not state.is_stale:
        return
    state.analytics_token = _read_analytics_token(state.token, super_props)
    state.fetched_at = int(time.time())
    state.save(user_id)


@dataclass(frozen=True)
class Session:
    heartbeat_session: str
    launch_signature: str
    super_props: str
    props_dict: dict

    @classmethod
    def new(cls):
        hb = str(uuid.uuid4())
        sig = str(uuid.uuid4())
        b64, raw = build_super_props(sig, hb)
        return cls(hb, sig, b64, raw)


def _make_unique_session(base: Session) -> Session:
    hb = str(uuid.uuid4())
    sig = str(uuid.uuid4())
    props = dict(base.props_dict)
    props["client_launch_id"] = str(uuid.uuid4())
    props["launch_signature"] = sig
    props["client_heartbeat_session_id"] = hb
    raw = json.dumps(props, separators=(",", ":")).encode("utf-8")
    return Session(hb, sig, base64.b64encode(raw).decode("ascii"), props)


class MiniWebSocket:
    def __init__(self, host, port=443, path="/"):
        host_clean = host.replace("wss://", "").replace("https://", "").rstrip("/")
        if ":" in host_clean:
            h, p = host_clean.split(":", 1)
            self.host = h
            try: self.port = int(p)
            except ValueError: self.port = port
        else:
            self.host, self.port = host_clean, port
        self.path = path
        self.sock = None
        self.closed = True
        self._lock = threading.RLock()

    def connect(self):
        raw = socket.create_connection((self.host, self.port), timeout=15)
        ctx = ssl.create_default_context()
        self.sock = ctx.wrap_socket(raw, server_hostname=self.host)
        key = base64.b64encode(os.urandom(16)).decode()
        host_hdr = self.host if self.port == 443 else f"{self.host}:{self.port}"
        req = (f"GET {self.path} HTTP/1.1\r\nHost: {host_hdr}\r\n"
               f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
               f"User-Agent: {USER_AGENT}\r\n\r\n")
        self.sock.sendall(req.encode())
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.sock.recv(4096)
            if not chunk: raise ConnectionError("failed handshake")
            data += chunk
        if "101" not in data.split(b"\r\n")[0].decode("utf-8", "ignore"):
            raise ConnectionError("handshake rejected")
        self.closed = False

    def send(self, data: str):
        with self._lock:
            if self.closed or not self.sock:
                raise ConnectionError("socket closed")
            try:
                payload = data.encode()
                ln = len(payload)
                mask = os.urandom(4)
                hdr = bytearray([0x81])
                if ln <= 125: hdr.append(0x80 | ln)
                elif ln <= 65535:
                    hdr.append(0x80 | 126); hdr.extend(ln.to_bytes(2, "big"))
                else:
                    hdr.append(0x80 | 127); hdr.extend(ln.to_bytes(8, "big"))
                hdr.extend(mask)
                masked = bytearray(payload[i] ^ mask[i % 4] for i in range(ln))
                self.sock.sendall(hdr + masked)
            except Exception as e:
                self.close()
                raise ConnectionError(f"write error: {e}") from e

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk: raise ConnectionError("socket closed")
            buf.extend(chunk)
        return bytes(buf)

    def recv(self, timeout=5.0):
        if self.closed or not self.sock: return None
        try:
            r, _, _ = select.select([self.sock], [], [], timeout)
            if not r: return None
            head = self._recv_exact(2)
        except Exception:
            self.close(); return None
        opcode = head[0] & 0x0F
        if opcode == 0x8:
            code = reason = 0
            try:
                c_len = head[1] & 0x7F
                if c_len >= 2:
                    p = self._recv_exact(c_len)
                    code = struct.unpack(">H", p[:2])[0]
                    reason = p[2:].decode("utf-8", "ignore")
            except Exception:
                pass
            self.close()
            return {"op": "close", "code": code, "reason": reason}
        if opcode == 0x9:
            try:
                with self._lock:
                    if self.sock and not self.closed:
                        self.sock.sendall(bytearray([0x8A, 0x80]) + os.urandom(4))
            except Exception:
                self.close()
            return {"op": "ping"}
        length = head[1] & 0x7F
        try:
            if length == 126: length = int.from_bytes(self._recv_exact(2), "big")
            elif length == 127: length = int.from_bytes(self._recv_exact(8), "big")
            payload = self._recv_exact(length)
            return json.loads(payload.decode())
        except Exception:
            self.close(); return None

    def close(self):
        self.closed = True
        with self._lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
                self.sock = None


class VoiceConnection:
    def __init__(self, endpoint, server_id, user_id, session_id, token,
                 console, label="voice", is_stream=False, stream_key=""):
        cleaned = endpoint.replace("wss://", "").replace("https://", "").rstrip("/")
        if ":" in cleaned:
            self.host, ps = cleaned.split(":", 1)
            try: self.port = int(ps)
            except ValueError: self.port = 443
        else:
            self.host, self.port = cleaned, 443
        self.endpoint = f"{self.host}:{self.port}"
        self.server_id = server_id
        self.user_id = user_id
        self.session_id = session_id
        self.token = token
        self.console = console
        self.label = label
        self.is_stream = is_stream
        self.stream_key = stream_key
        self.ws = None
        self.udp_sock = None
        self.ssrc = 0
        self.voice_ip = ""
        self.voice_port = 0
        self.external_ip = ""
        self.external_port = 0
        self.secret_key = b""
        self.encryption_mode = ""
        self.heartbeat_interval = 13.75
        self.running = False
        self.nonce_counter = 0
        self._hb_thread = None
        self._audio_thread = None

    def connect(self) -> bool:
        C = self.console
        try:
            self.ws = MiniWebSocket(self.host, port=self.port, path="/?v=8")
            self.ws.connect()
        except Exception as exc:
            C.err(self.label, f"ws connect failed: {exc}"); return False
        msg = self._recv_op(8, timeout=10)
        if not msg:
            C.err(self.label, "no Hello"); return False
        self.heartbeat_interval = msg.get("d", {}).get("heartbeat_interval", 13750) / 1000.0
        identify_d = {
            "server_id": self.server_id, "user_id": self.user_id,
            "session_id": self.session_id, "token": self.token,
            "max_dave_protocol_version": 1,
        }
        if self.is_stream:
            if self.stream_key: identify_d["stream_key"] = self.stream_key
            identify_d["video"] = True
            identify_d["streams"] = [{"type": "screen", "rid": "100", "quality": 100}]
        self.ws.send(json.dumps({"op": 0, "d": identify_d}))
        msg = self._recv_op(2, timeout=10)
        if not msg:
            C.err(self.label, "no Ready"); return False
        d = msg.get("d", {})
        self.ssrc = d.get("ssrc", 0)
        self.voice_ip = d.get("ip", "")
        self.voice_port = d.get("port", 0)
        modes = d.get("modes", [])
        for m in ["aead_aes256_gcm_rtpsize", "aead_xchacha20_poly1305_rtpsize",
                  "xsalsa20_poly1305_lite", "xsalsa20_poly1305"]:
            if m in modes: self.encryption_mode = m; break
        if not self.encryption_mode and modes: self.encryption_mode = modes[0]
        if not self._udp_discovery():
            C.warn(self.label, "UDP discovery failed, using fallback")
            self.external_ip = "127.0.0.1"
            self.external_port = random.randint(10000, 60000)
        self.ws.send(json.dumps({"op": 1, "d": {
            "protocol": "udp", "data": {
                "address": self.external_ip, "port": self.external_port,
                "mode": self.encryption_mode,
            },
        }}))
        msg = self._recv_op(4, timeout=10)
        if msg:
            d = msg.get("d", {})
            self.secret_key = bytes(d.get("secret_key", []))
            self.encryption_mode = d.get("mode", self.encryption_mode)
        self.running = True
        return True

    def _recv_op(self, target, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = self.ws.recv(timeout=min(max(0.1, deadline - time.time()), 2.0))
            if msg is None:
                if self.ws.closed: return None
                continue
            if msg.get("op") == "close":
                self.console.err(self.label, f"closed: {msg.get('code')} ({msg.get('reason')})")
                return None
            if msg.get("op") == target: return msg
            if msg.get("op") == 3: self._send_heartbeat()
        return None

    def _send_heartbeat(self):
        if self.ws and not self.ws.closed:
            try: self.ws.send(json.dumps({"op": 3, "d": int(time.time() * 1000)}))
            except Exception: pass

    def _udp_discovery(self):
        try:
            self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_sock.settimeout(5)
            pkt = bytearray(74)
            struct.pack_into(">HHI", pkt, 0, 0x0001, 70, self.ssrc)
            self.udp_sock.sendto(bytes(pkt), (self.voice_ip, self.voice_port))
            data, _ = self.udp_sock.recvfrom(256)
            if len(data) < 74: return False
            end = data.index(0, 8)
            self.external_ip = data[8:end].decode("utf-8")
            self.external_port = struct.unpack_from(">H", data, 72)[0]
            return bool(self.external_ip)
        except Exception:
            if self.udp_sock:
                try: self.udp_sock.close()
                except Exception: pass
                self.udp_sock = None
            return False

    def start_heartbeat(self):
        def _loop():
            while self.running and self.ws and not self.ws.closed:
                self._send_heartbeat()
                try:
                    m = self.ws.recv(timeout=0.5)
                    if m and m.get("op") == 3: self._send_heartbeat()
                except Exception: pass
                time.sleep(max(1.0, self.heartbeat_interval - 0.5))
        self._hb_thread = threading.Thread(target=_loop, daemon=True)
        self._hb_thread.start()

    def send_speaking(self, speaking=True):
        if self.ws and not self.ws.closed:
            try:
                self.ws.send(json.dumps({"op": 5, "d": {
                    "speaking": 1 if speaking else 0, "delay": 0, "ssrc": self.ssrc,
                }}))
            except Exception: pass

    def start_silence_pump(self):
        if not self.udp_sock or not self.secret_key: return
        def _pump():
            seq = ts = 0
            while self.running:
                try:
                    if HAS_CRYPTO and "aes256_gcm" in self.encryption_mode:
                        self._send_encrypted_silence_gcm(seq, ts)
                    else:
                        try: self.udp_sock.sendto(struct.pack(">I", seq) + b"\xc9",
                                                    (self.voice_ip, self.voice_port))
                        except Exception: pass
                    seq = (seq + 1) & 0xFFFF
                    ts = (ts + 960) & 0xFFFFFFFF
                    time.sleep(1.0)
                except Exception: break
        self._audio_thread = threading.Thread(target=_pump, daemon=True)
        self._audio_thread.start()

    def _send_encrypted_silence_gcm(self, seq, ts):
        if not self.udp_sock or not self.secret_key: return
        try:
            hdr = struct.pack(">BBHII", 0x80, 0x78, seq & 0xFFFF, ts & 0xFFFFFFFF, self.ssrc)
            self.nonce_counter += 1
            nonce = struct.pack(">I", self.nonce_counter) + b"\x00" * 8
            enc = AESGCM(self.secret_key[:32]).encrypt(nonce, OPUS_SILENCE, hdr)
            pkt = hdr + enc + struct.pack(">I", self.nonce_counter)
            self.udp_sock.sendto(pkt, (self.voice_ip, self.voice_port))
        except Exception: pass

    def close(self):
        self.running = False
        if self.udp_sock:
            try: self.udp_sock.close()
            except Exception: pass
        if self.ws: self.ws.close()


def _discord_api_get(url, token, super_props=""):
    headers = {"authorization": token, "user-agent": USER_AGENT}
    if super_props: headers["x-super-properties"] = super_props
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try: err = json.loads(exc.read().decode())
        except Exception: err = {"message": str(exc.reason)}
        return exc.code, err
    except Exception as exc:
        return 0, {"message": str(exc)}


def validate_stream_target(token, super_props, guild_id, channel_id, console):
    C = console
    if not guild_id.isdigit():
        C.err("server", f"invalid server ID: {guild_id}"); return False, "bad id"
    if not channel_id.isdigit():
        C.err("channel", f"invalid channel ID: {channel_id}"); return False, "bad id"

    status, user_data = _discord_api_get("https://discord.com/api/v9/users/@me", token, super_props)
    if status == 401:
        C.err("auth", "token invalid"); return False, "unauthorized"
    if status != 200:
        C.err("auth", f"api error {status}"); return False, "api error"

    g_status, g_data = _discord_api_get(f"https://discord.com/api/v9/guilds/{guild_id}", token, super_props)
    if g_status in (403, 404):
        C.err("server", "server not found / not a member"); return False, "not found"
    elif g_status != 200:
        C.err("server", f"lookup failed {g_status}"); return False, "lookup error"
    C.ok("server", f"verified: {g_data.get('name')} ({guild_id})")

    ch_status, ch_data = _discord_api_get(f"https://discord.com/api/v9/channels/{channel_id}", token, super_props)
    if ch_status in (403, 404):
        C.err("channel", "channel not found"); return False, "not found"
    elif ch_status != 200:
        C.err("channel", f"lookup failed {ch_status}"); return False, "lookup error"
    ch_name = ch_data.get("name", "channel")
    ch_type = ch_data.get("type")
    if ch_type not in (2, 13):
        C.err("channel", f"not a voice channel (type {ch_type})"); return False, "not voice"
    C.ok("channel", f"verified: #{ch_name}")

    raw = ch_data.get("permissions")
    if raw is not None:
        try:
            p = int(raw)
            admin = bool(p & PERMISSION_ADMINISTRATOR)
            if not (admin or p & PERMISSION_VIEW_CHANNEL):
                C.err("perms", "no VIEW_CHANNEL"); return False, "no view"
            if not (admin or p & PERMISSION_CONNECT):
                C.err("perms", "no CONNECT"); return False, "no connect"
            if not (admin or p & PERMISSION_STREAM):
                C.err("perms", "no STREAM"); return False, "no stream"
            C.ok("perms", "all checks passed")
        except Exception:
            pass
    return True, f"#{ch_name}"


def validate_viewer_token(token, super_props):
    status, data = _discord_api_get("https://discord.com/api/v9/users/@me", token, super_props)
    if status == 200:
        return True, f"@{data.get('username', '?')} ({data.get('id', '?')})"
    if status == 401: return False, "unauthorized"
    if status == 403: return False, "forbidden"
    return False, f"HTTP {status}"


class ViewerAccount:
    def __init__(self, token, session, console, guild_id, channel_id):
        self.token = token.strip()
        self.session = _make_unique_session(session)
        self.console = console
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.user_id = _user_id_from_token(self.token)
        self.username = ""
        self.label = f"viewer:{(self.user_id or 'unknown')[:8]}"
        self.ws = None
        self.running = False
        self.heartbeat_interval = 41.25
        self.sequence = None
        self.session_id = ""
        self.voice_session_id = "" 
        self.voice_token = ""       
        self.voice_endpoint = ""    
        self.voice_conn = None
        self.voice_connected = False
        self.stream_key = ""
        self.watching = False
        self.media_conn = None
        self._event_thread = None

    def start(self):
        C = self.console
        try:
            self.ws = MiniWebSocket(GATEWAY_URL, path="/?v=9&encoding=json")
            self.ws.connect()
        except Exception as exc:
            C.err(self.label, f"gateway connect failed: {exc}"); return False
        self.running = True
        hello = None
        deadline = time.time() + 10
        while time.time() < deadline:
            m = self.ws.recv(timeout=1.0)
            if m and m.get("op") == 10:
                hello = m; break
            if isinstance(m, dict) and m.get("op") == "close":
                self._teardown(); return False
        if not hello:
            self._teardown(); return False
        self.heartbeat_interval = hello.get("d", {}).get("heartbeat_interval", 41250) / 1000.0
        self.ws.send(json.dumps({"op": 2, "d": {
            "token": self.token, "capabilities": 16381,
            "properties": self.session.props_dict,
            "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
        }}))
        ready = False
        deadline = time.time() + 15
        while time.time() < deadline:
            m = self.ws.recv(timeout=1.0)
            if not m: continue
            if isinstance(m, dict) and m.get("op") == "close":
                C.err(self.label, f"closed ({m.get('code')})"); self._teardown(); return False
            if m.get("op") == 0 and m.get("t") == "READY":
                d = m.get("d", {})
                self.session_id = d.get("session_id", "")
                user = d.get("user", {})
                self.user_id = str(user.get("id", self.user_id))
                self.username = user.get("username", "")
                self.label = f"viewer:{self.username or self.user_id[:8]}"
                ready = True; break
        if not ready:
            self._teardown(); return False
        self.ws.send(json.dumps({"op": 4, "d": {
            "guild_id": self.guild_id, "channel_id": self.channel_id,
            "self_mute": True, "self_deaf": False, "self_video": False,
        }}))
        C.ok(self.label, "joined vc as viewer")
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()
        return True

    def stop(self):
        self.running = False
        for c in (self.media_conn, self.voice_conn):
            try:
                if c: c.close()
            except Exception: pass
        self.media_conn = self.voice_conn = None
        if self.ws and not self.ws.closed:
            try:
                self.ws.send(json.dumps({"op": 4, "d": {
                    "guild_id": self.guild_id, "channel_id": None,
                    "self_mute": False, "self_deaf": False, "self_video": False,
                }}))
                time.sleep(0.2)
            except Exception: pass
        if self.ws: self.ws.close()

    def _teardown(self):
        self.running = False
        if self.ws: self.ws.close()

    def _event_loop(self):
        C = self.console
        last_hb = time.time()
        while self.running and self.ws and not self.ws.closed:
            try:
                now = time.time()
                if now - last_hb > self.heartbeat_interval:
                    self.ws.send(json.dumps({"op": 1, "d": self.sequence}))
                    last_hb = now
                m = self.ws.recv(timeout=1.0)
                if not m: continue
                if m.get("op") == "close":
                    C.warn(self.label, f"closed ({m.get('code')})"); break
                op = m.get("op"); t = m.get("t"); d = m.get("d") or {}
                if m.get("s") is not None: self.sequence = m.get("s")
                if op == 11: pass
                elif op == 1:
                    self.ws.send(json.dumps({"op": 1, "d": self.sequence}))
                    last_hb = now
                elif op == 0:
                    if t == "VOICE_STATE_UPDATE":
                        if str(d.get("user_id")) == str(self.user_id):
                            sid = d.get("session_id", "")
                            if sid: self.voice_session_id = sid
                            if not d.get("channel_id") and self.voice_connected:
                                C.warn(self.label, "removed from vc"); self.running = False; break
                            self._try_voice_connect()
                    elif t == "VOICE_SERVER_UPDATE":
                        self.voice_token = d.get("token", "")
                        self.voice_endpoint = d.get("endpoint", "")
                        self._try_voice_connect()
                    elif t == "STREAM_CREATE": self._on_stream_create(d)
                    elif t == "STREAM_SERVER_UPDATE": self._on_stream_server_update(d)
                    elif t == "STREAM_DELETE":
                        if d.get("stream_key") == self.stream_key:
                            self.watching = False
                            if self.media_conn:
                                try: self.media_conn.close()
                                except Exception: pass
                                self.media_conn = None
            except Exception as exc:
                C.warn(self.label, f"event loop err: {exc}"); break

    def _try_voice_connect(self):
        if self.voice_conn or not self.voice_token or not self.voice_endpoint or not self.voice_session_id:
            return
        
        C = self.console
        if self.voice_conn:
            try: self.voice_conn.close()
            except Exception: pass
            self.voice_conn = None
            self.voice_connected = False
        
        self.voice_conn = VoiceConnection(
            endpoint=self.voice_endpoint, server_id=self.guild_id, user_id=self.user_id,
            session_id=self.voice_session_id, token=self.voice_token, console=C, label=self.label,
        )
        if self.voice_conn.connect():
            self.voice_conn.start_heartbeat()
            self.voice_conn.send_speaking(False)
            self.voice_conn.start_silence_pump() 
            self.voice_connected = True
            C.ok(self.label, f"voice ready (ssrc={self.voice_conn.ssrc})")
        else:
            C.err(self.label, "voice handshake failed")

    def _on_stream_create(self, d):
        sk = d.get("stream_key", "")
        streamer = str(d.get("user_id", ""))
        ch = str(d.get("channel_id", ""))
        g = str(d.get("guild_id", ""))
        if not sk or streamer == str(self.user_id): return
        if g and g != str(self.guild_id): return
        if ch and ch != str(self.channel_id): return
        self.watch_stream(sk)

    def watch_stream(self, stream_key):
        if not stream_key or not self.ws or self.ws.closed: return
        if self.stream_key == stream_key and self.watching: return
        self.stream_key = stream_key
        try:
            self.ws.send(json.dumps({"op": 20, "d": {
                "stream_key": stream_key, "allow_multiple": True,
            }}))
            self.watching = True
            self.console.ok(self.label, "now watching stream")
        except Exception as exc:
            self.console.warn(self.label, f"watch failed: {exc}")

    def _on_stream_server_update(self, d):
        C = self.console
        token = d.get("token", ""); endpoint = d.get("endpoint", "")
        media_sid = d.get("media_session_id") or d.get("session_id") or self.session_id
        if not token or not endpoint or not self.stream_key or self.media_conn: return
        C.info(f"{self.label} connecting media route {endpoint}")
        try:
            self.media_conn = VoiceConnection(
                endpoint=endpoint, server_id=self.guild_id, user_id=self.user_id,
                session_id=media_sid, token=token, console=C,
                label=f"{self.label}:media", is_stream=True, stream_key=self.stream_key,
            )
            if self.media_conn.connect():
                self.media_conn.start_heartbeat()
                C.ok(self.label, "media connected")
            else:
                C.warn(self.label, "media connect failed (viewer still counted)")
                self.media_conn = None
        except Exception as exc:
            C.warn(self.label, f"media error: {exc}")
            self.media_conn = None


class ViewerManager:
    def __init__(self, tokens, session, console):
        self.tokens = [t.strip() for t in tokens if t and t.strip()]
        self.session = session
        self.console = console
        self.viewers = []
        self._lock = threading.Lock()
        self.stream_key = ""  

    def start_all(self, guild_id, channel_id):
        C = self.console
        if not self.tokens: return
        C.info(f"spawning {len(self.tokens)} viewer(s)...")
        def _spawn(token):
            try:
                v = ViewerAccount(token, self.session, C, guild_id, channel_id)
                if v.start():
                    with self._lock: 
                        self.viewers.append(v)
                        if self.stream_key:
                            v.watch_stream(self.stream_key)
            except Exception as exc:
                C.err("viewer", f"failed to start: {exc}")
        threads = [threading.Thread(target=_spawn, args=(t,), daemon=True) for t in self.tokens]
        for th in threads: th.start()

    def set_stream_key(self, stream_key):
        self.stream_key = stream_key
        with self._lock: viewers = list(self.viewers)
        for v in viewers:
            try: v.watch_stream(stream_key)
            except Exception: pass

    def stop_all(self):
        with self._lock:
            viewers = list(self.viewers); self.viewers = []
        for v in viewers:
            try: v.stop()
            except Exception: pass

    def status_summary(self):
        with self._lock: viewers = list(self.viewers)
        if not viewers: return "none"
        watching = sum(1 for v in viewers if v.watching)
        in_vc = sum(1 for v in viewers if v.voice_connected)
        return f"{watching} watching / {in_vc} in-vc / {len(viewers)} total"


class GatewayStreamEngine:
    def __init__(self, state, owner_id, session, console,
                 on_stop=None, on_stream_create=None):
        self.state = state
        self.owner_id = owner_id
        self.session = session
        self.console = console
        self.on_stop = on_stop
        self.on_stream_create = on_stream_create
        self.stream_created = threading.Event()
        self._stop_requested = threading.Event()
        self.ws = None; self.voice_conn = None; self.stream_conn = None
        self.viewer_manager = None
        self.running = False
        self.heartbeat_interval = 41.25
        self.sequence = None
        self.start_time = 0.0
        self.gateway_session_id = ""
        self.user_id = ""
        self.stream_key = ""
        self.voice_session_id = ""
        self.voice_token = ""
        self.voice_endpoint = ""
        self.voice_connected = False
        self.stream_token = ""
        self.stream_endpoint = ""
        self.stream_connected = False
        self.stream_ever_active = False
        self._stream_recreate_attempts = 0
        self._user_left_vc = False
        self._status_thread = None
        self._restart_thread = None

    def stop_stream(self):
        self._stop_requested.set()
        self.running = False

    def start_stream(self, guild_id, channel_id, wait_for_input=True):
        C = self.console
        if self._stop_requested.is_set(): return
        C.rule()
        C.info(f"connecting to discord gateway (owner {self.owner_id})...")
        self.ws = MiniWebSocket(GATEWAY_URL, path="/?v=9&encoding=json")
        try:
            self.ws.connect()
        except Exception as exc:
            C.err("gateway", f"connect failed: {exc}")
            if wait_for_input:
                C.rule(); input("press enter...")
            return
        if self._stop_requested.is_set(): return
        C.ok("gateway", "connected")
        self.running = True
        self.start_time = time.time()
        self.user_id = _user_id_from_token(self.state.token) or ""
        self.stream_ever_active = False
        self._stream_recreate_attempts = 0
        self._user_left_vc = False
        if self.state.viewer_tokens:
            self.viewer_manager = ViewerManager(self.state.viewer_tokens, self.session, C)
            threading.Thread(target=self.viewer_manager.start_all,
                             args=(guild_id, channel_id), daemon=True).start()
        else:
            C.warn("viewers", "no viewer tokens — badge will NOT count!")
        threading.Thread(target=self._event_loop, args=(guild_id, channel_id), daemon=True).start()
        
        self._status_thread = threading.Thread(target=self._status_display, daemon=True)
        self._status_thread.start()

        self._restart_thread = threading.Thread(
            target=self._auto_restart_loop, args=(guild_id, channel_id), daemon=True
        )
        self._restart_thread.start()

        if wait_for_input:
            try:
                import msvcrt
                while self.running and not self._stop_requested.is_set():
                    if msvcrt.kbhit():
                        ch = msvcrt.getch()
                        if ch in (b'\r', b'\n', b'\x03'): break
                    time.sleep(0.1)
            except Exception:
                while self.running and not self._stop_requested.is_set(): time.sleep(0.5)
        else:
            while self.running and not self._stop_requested.is_set(): time.sleep(0.5)
        self._cleanup(guild_id, wait_for_input=wait_for_input)

    def _status_display(self):
        while self.running:
            time.sleep(60)
            if not self.running: break
            el = time.time() - self.start_time
            if self.stream_connected: s = "LIVE"
            elif self.voice_connected: s = "starting"
            else: s = "reconnecting"
            vi = f" · viewers: {self.viewer_manager.status_summary()}" if self.viewer_manager else ""
            self.console.live(f"stream[{self.owner_id}]", f"{s} · {int(el/60)}m ({el/3600:.2f}h){vi}")

    def _auto_restart_loop(self, guild_id, channel_id):
        interval = 1800  # 30 minutes
        while self.running and not self._stop_requested.is_set():
            for _ in range(interval):
                if not self.running or self._stop_requested.is_set():
                    return
                time.sleep(1)
            if not self.running or self._stop_requested.is_set():
                return
            self.console.warn(f"stream[{self.owner_id}]", "30m reached — auto-restarting voice session...")
            self._recreate_voice_session(guild_id, channel_id)

    def _recreate_voice_session(self, guild_id, channel_id):
        C = self.console
        tag = f"[{self.owner_id}]"

        if self.ws and not self.ws.closed:
            try:
                self.ws.send(json.dumps({"op": 4, "d": {
                    "guild_id": guild_id, "channel_id": None,
                    "self_mute": False, "self_deaf": False, "self_video": False,
                }}))
            except Exception: pass

        if self.voice_conn:
            try: self.voice_conn.close()
            except Exception: pass
            self.voice_conn = None
        self.voice_connected = False
        self.stream_connected = False

        for _ in range(3):
            if not self.running: return
            time.sleep(1)

        if self.ws and not self.ws.closed:
            try:
                self.ws.send(json.dumps({"op": 4, "d": {
                    "guild_id": guild_id, "channel_id": channel_id,
                    "self_mute": False, "self_deaf": False, "self_video": False,
                }}))
                C.ok("voice" + tag, f"rejoined {channel_id} for auto-restart")
            except Exception: pass

    def _reconnect(self, guild_id, channel_id, can_resume=True):
        C = self.console
        if not self.running: return False
        self.stream_connected = False
        self.voice_connected = False
        for c in (self.stream_conn, self.voice_conn):
            try:
                if c: c.close()
            except Exception: pass
        self.stream_conn = self.voice_conn = None
        if self.ws:
            try: self.ws.close()
            except Exception: pass
            self.ws = None
        attempts = 0; backoff = 3
        while self.running:
            attempts += 1
            C.warn(f"gateway[{self.owner_id}]", f"reconnecting in {backoff}s (try {attempts})...")
            for _ in range(backoff * 10):
                if not self.running: return False
                time.sleep(0.1)
            try:
                self.ws = MiniWebSocket(GATEWAY_URL, path="/?v=9&encoding=json")
                self.ws.connect()
                hello = None
                for _ in range(15):
                    m = self.ws.recv(timeout=1.0)
                    if isinstance(m, dict) and m.get("op") == "close":
                        if m.get("code") == 4004:
                            C.err("auth", "invalid token"); self.running = False; return False
                    if m and m.get("op") == 10: hello = m; break
                    time.sleep(0.1)
                if not hello:
                    self.ws.close(); self.ws = None
                    backoff = min(15, backoff + 2); continue
                self.heartbeat_interval = hello.get("d", {}).get("heartbeat_interval", 41250) / 1000.0
                self.ws.send(json.dumps({"op": 1, "d": self.sequence}))
                resumed = False
                if can_resume and self.gateway_session_id and self.sequence is not None:
                    try:
                        self.ws.send(json.dumps({"op": 6, "d": {
                            "token": self.state.token, "session_id": self.gateway_session_id,
                            "seq": self.sequence,
                        }}))
                        r = self.ws.recv(timeout=4.0)
                        if r and r.get("op") == 0 and r.get("t") == "RESUMED":
                            resumed = True
                        elif r and r.get("op") == 0 and r.get("t") == "READY":
                            self._handle_dispatch("READY", r.get("d", {}), guild_id, channel_id)
                            resumed = True
                    except Exception:
                        pass
                if not resumed:
                    self.ws.send(json.dumps({"op": 2, "d": {
                        "token": self.state.token, "capabilities": 16381,
                        "properties": self.session.props_dict,
                        "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
                    }}))
                    t = self.ws.recv(timeout=2.0)
                    if isinstance(t, dict) and t.get("op") == "close" and t.get("code") == 4004:
                        self.running = False; return False
                    elif isinstance(t, dict) and t.get("op") == 0 and t.get("t") == "READY":
                        self._handle_dispatch("READY", t.get("d", {}), guild_id, channel_id)
                        resumed = True
                if resumed:
                    self.ws.send(json.dumps({"op": 4, "d": {
                        "guild_id": guild_id, "channel_id": channel_id,
                        "self_mute": False, "self_deaf": False, "self_video": False,
                    }}))
                C.ok(f"gateway[{self.owner_id}]", "reconnected")
                return True
            except Exception as exc:
                C.warn(f"gateway[{self.owner_id}]", f"retry failed: {exc}")
                if self.ws:
                    try: self.ws.close()
                    except Exception: pass
                    self.ws = None
                backoff = min(15, backoff + 2)
        return False

    def _event_loop(self, guild_id, channel_id):
        C = self.console
        last_hb = time.time()
        while self.running:
            try:
                if not self.ws or self.ws.closed:
                    if not self._reconnect(guild_id, channel_id): break
                    last_hb = time.time(); continue
                now = time.time()
                if now - last_hb > self.heartbeat_interval:
                    try:
                        self.ws.send(json.dumps({"op": 1, "d": self.sequence}))
                        last_hb = now
                    except Exception:
                        if not self._reconnect(guild_id, channel_id): break
                        last_hb = time.time(); continue
                msg = self.ws.recv(timeout=1.0)
                if not msg: continue
                if isinstance(msg, dict) and msg.get("op") == "close":
                    code = msg.get("code")
                    C.warn(f"gateway[{self.owner_id}]", f"closed ({code})")
                    if code == 4004: self.running = False; break
                    if code in (4010, 4011, 4012, 4013, 4014): self.running = False; break
                    if not self._reconnect(guild_id, channel_id): break
                    last_hb = time.time(); continue
                op = msg.get("op"); t = msg.get("t"); d = msg.get("d") or {}
                if msg.get("s") is not None: self.sequence = msg.get("s")
                if op == 10:
                    self.heartbeat_interval = d.get("heartbeat_interval", 41250) / 1000.0
                    last_hb = time.time()
                    self.ws.send(json.dumps({"op": 1, "d": self.sequence}))
                    self.ws.send(json.dumps({"op": 2, "d": {
                        "token": self.state.token, "capabilities": 16381,
                        "properties": self.session.props_dict,
                        "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
                    }}))
                elif op == 1:
                    self.ws.send(json.dumps({"op": 1, "d": self.sequence})); last_hb = now
                elif op == 11: pass
                elif op == 0:
                    if t == "RESUMED":
                        C.ok(f"gateway[{self.owner_id}]", "resumed")
                        self.ws.send(json.dumps({"op": 4, "d": {
                            "guild_id": guild_id, "channel_id": channel_id,
                            "self_mute": False, "self_deaf": False, "self_video": False,
                        }}))
                    else:
                        self._handle_dispatch(t, d, guild_id, channel_id)
                elif op == 7:
                    if not self._reconnect(guild_id, channel_id): break
                    last_hb = time.time()
                elif op == 9:
                    if not self._reconnect(guild_id, channel_id, can_resume=bool(d)): break
                    last_hb = time.time()
            except Exception as exc:
                if not self.running: break
                C.warn(f"gateway[{self.owner_id}]", f"dropped: {exc}")
                if not self._reconnect(guild_id, channel_id): break
                last_hb = time.time()

    def _handle_dispatch(self, event_type, d, guild_id, channel_id):
        C = self.console
        tag = f"[{self.owner_id}]"
        if event_type == "READY":
            self.gateway_session_id = d.get("session_id", "")
            u = d.get("user", {})
            self.user_id = str(u.get("id", self.user_id))
            C.ok("auth" + tag, f"authenticated ({u.get('username', '?')})")

            appid, game_name = _get_random_game()
            if appid and game_name:
                try:
                    self.ws.send(json.dumps({
                        "op": 3,
                        "d": {
                            "since": 0,
                            "activities": [{
                                "name": game_name,
                                "type": 0,
                                "application_id": appid
                            }],
                            "status": "online",
                            "afk": False
                        }
                    }))
                    C.ok("status" + tag, f"playing {game_name}")
                except Exception:
                    pass

            self.ws.send(json.dumps({"op": 4, "d": {
                "guild_id": guild_id, "channel_id": channel_id,
                "self_mute": False, "self_deaf": False, "self_video": False,
            }}))
            C.ok("voice" + tag, f"joining {channel_id}")
        elif event_type == "VOICE_STATE_UPDATE":
            if str(d.get("user_id")) == str(self.user_id):
                ac = d.get("channel_id")
                if d.get("session_id"): self.voice_session_id = d.get("session_id")
                if (self.voice_connected or self.stream_connected or self.stream_ever_active):
                    if not ac or str(ac) != str(channel_id):
                        self._user_left_vc = True
                        self.voice_connected = self.stream_connected = False
                        self.running = False
                        return
                self._try_voice_connect(guild_id, channel_id)
        elif event_type == "VOICE_SERVER_UPDATE":
            self.voice_token = d.get("token", "")
            self.voice_endpoint = d.get("endpoint") or ""
            if not self.voice_endpoint or not self.voice_token:
                C.err("voice" + tag, "empty update"); return
            
            C.ok("voice" + tag, f"server: {self.voice_endpoint}")
            self._try_voice_connect(guild_id, channel_id)
            
        elif event_type == "STREAM_CREATE":
            self.stream_key = d.get("stream_key", "")
            C.ok("stream" + tag, f"Go Live! key: {self.stream_key}")
            self.stream_connected = True
            self.stream_ever_active = True
            self._stream_recreate_attempts = 0
            self.stream_created.set()
            if self.on_stream_create:
                try: self.on_stream_create(self)
                except Exception: pass
            if self.viewer_manager and self.stream_key:
                self.viewer_manager.set_stream_key(self.stream_key)
            if self.voice_conn and self.voice_conn.ws and not self.voice_conn.ws.closed:
                try:
                    v = self.voice_conn.ssrc + 1
                    r = self.voice_conn.ssrc + 2
                    self.voice_conn.ws.send(json.dumps({"op": 12, "d": {
                        "audio_ssrc": self.voice_conn.ssrc,
                        "video_ssrc": v, "rtx_ssrc": r,
                        "streams": [{"type": "screen", "rid": "100", "ssrc": v,
                                     "rtx_ssrc": r, "active": True, "quality": 100,
                                     "max_bitrate": 8000000, "max_framerate": 30,
                                     "max_resolution": {"type": "fixed", "width": 1920, "height": 1080}}],
                    }}))
                    self.voice_conn.send_speaking(True)
                except Exception: pass
            C.live("stream" + tag, "BROADCASTING LIVE")
        elif event_type == "STREAM_SERVER_UPDATE":
            if d.get("endpoint") and d.get("token"):
                self.stream_token = d.get("token")
                self.stream_endpoint = d.get("endpoint")
                C.ok("stream" + tag, f"media route: {self.stream_endpoint}")
        elif event_type == "STREAM_DELETE":
            self.stream_connected = False
            if self.stream_conn:
                try: self.stream_conn.close()
                except Exception: pass
                self.stream_conn = None
            if not self.running or not self.voice_connected or self._user_left_vc: return
            if self._stream_recreate_attempts < 3:
                self._stream_recreate_attempts += 1
                def _re():
                    time.sleep(2.0)
                    if self.running and self.ws and not self.ws.closed:
                        try:
                            self.ws.send(json.dumps({"op": 18, "d": {
                                "type": "guild", "guild_id": guild_id,
                                "channel_id": channel_id, "preferred_region": None,
                            }}))
                        except Exception: pass
                threading.Thread(target=_re, daemon=True).start()
            else:
                C.err("stream" + tag, "stream stopped repeatedly — stopping"); self.running = False

    def _try_voice_connect(self, guild_id, channel_id):
        if self.voice_conn or not self.voice_token or not self.voice_endpoint or not self.voice_session_id:
            return

        C = self.console
        tag = f"[{self.owner_id}]"
        if self.voice_connected:
            if self.voice_conn:
                try: self.voice_conn.close()
                except Exception: pass
            self.voice_connected = False
        
        self.voice_conn = VoiceConnection(
            endpoint=self.voice_endpoint, server_id=guild_id, user_id=self.user_id,
            session_id=self.voice_session_id, token=self.voice_token, console=C, label="voice" + tag,
        )
        if self.voice_conn.connect():
            self.voice_conn.start_heartbeat()
            self.voice_conn.send_speaking(False)
            self.voice_conn.start_silence_pump()
            self.voice_connected = True
            C.ok("voice" + tag, f"connected (ssrc={self.voice_conn.ssrc})")
            time.sleep(0.5)
            try:
                self.ws.send(json.dumps({"op": 18, "d": {
                    "type": "guild", "guild_id": guild_id,
                    "channel_id": channel_id, "preferred_region": None,
                }}))
            except Exception: pass
        else:
            C.err("voice" + tag, "handshake failed")

    def _cleanup(self, guild_id, wait_for_input=True):
        C = self.console
        tag = f"[{self.owner_id}]"
        self.running = False
        if self.viewer_manager:
            C.info(f"stopping viewers{tag}...")
            self.viewer_manager.stop_all()
        if self.ws and not self.ws.closed and self.stream_key:
            try:
                self.ws.send(json.dumps({"op": 19, "d": {"stream_key": self.stream_key}}))
            except Exception: pass
        if self.ws and not self.ws.closed and not self._user_left_vc:
            try:
                self.ws.send(json.dumps({"op": 4, "d": {
                    "guild_id": guild_id, "channel_id": None,
                    "self_mute": False, "self_deaf": False, "self_video": False,
                }}))
            except Exception: pass
        for c in (self.stream_conn, self.voice_conn):
            try:
                if c: c.close()
            except Exception: pass
        if self.ws:
            time.sleep(0.3); self.ws.close()
        if (self.stream_connected or self.stream_ever_active) and self.start_time > 0:
            el = time.time() - self.start_time
            h = el / 3600.0
            if el >= 10:
                self.state.total_stream_hours_claimed += h
                self.state.total_stream_sessions += 1
                self.state.save(self.owner_id)
                C.ok("stream" + tag, f"ended · {el:.0f}s ({h:.2f}h)")
        if self.on_stop and self.stream_created.is_set():
            try: self.on_stop(self)
            except Exception: pass
        if wait_for_input:
            C.rule(); input("press enter...")


def inspect_profile(state, session, console):
    C = console
    if not state.has_token:
        C.err("auth", "no token"); return
    C.info("fetching profile...")
    uid = _user_id_from_token(state.token)
    if not uid: return
    url = f"{USERS_URL}/{uid}/profile?with_mutual_guilds=false&with_mutual_friends_count=false"
    req = urllib.request.Request(url, headers={
        "authorization": state.token, "user-agent": USER_AGENT,
        "x-super-properties": session.super_props,
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        user = data.get("user", {})
        badges = data.get("badges", [])
        C.ok("account", f"@{user.get('username')} · ID: {uid}")
        for b in badges:
            C.info(f"  • {b.get('id')}: {b.get('description', '')}")
    except Exception as exc:
        C.err("profile", f"failed: {exc}")