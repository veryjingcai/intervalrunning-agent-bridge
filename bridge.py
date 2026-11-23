#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
intervalrunning 智能体桥接服务 v1

一副桥，两边对接：
  对下（手机 App）：UDP 发现广播响应 + HTTP 服务，监听 0.0.0.0:47823
  对上（智能体）：  MCP stdio server（JSON-RPC 2.0，逐行 JSON），读写同一份数据

用法：
  python bridge.py mcp       # 默认。启动网络后台线程 + MCP stdio（给 WorkBuddy 用）
  python bridge.py serve     # 只跑网络服务，前台常驻（手机随时可连）
  python bridge.py status    # 查看当前状态
  python bridge.py selftest  # 自检，不改真实数据

数据：与本文件同目录的 data/store.json
依赖：仅 Python 3.7+ 标准库，无需 pip install
"""

import argparse
import contextlib
import copy
import datetime
import hashlib
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_STORE = os.path.join(HERE, "data", "store.json")
PORT = 47823
DEVICE_NAME = "智能体"
DEVICE_ID = "intervalrunning-bridge-001"
CLIENT_NAME = "intervalrunning"
PERMISSIONS = ("READ_ONLY", "READ_WRITE")
STEP_TYPES = ("WARMUP", "WORK", "RECOVERY", "COOLDOWN")
TARGET_TYPES = ("OPEN", "PACE_RANGE", "HR_RANGE", "DISTANCE")
INTENSITY_LEVELS = ("BEGINNER", "EASY", "MODERATE", "HARD", "EXTREME")
DEFAULT_CLIENT = "default"
MAX_PAIRS = 32
MAX_BODY = 20 * 1024 * 1024


def now_ms():
    return int(time.time() * 1000)


def log(msg):
    # MCP 模式 stdout 只能走 JSON-RPC，日志一律 stderr
    print("[bridge] " + str(msg), file=sys.stderr, flush=True)


def local_tz():
    """桥端机器的本地时区（手机没上报时兜底用）。"""
    return datetime.datetime.now().astimezone().tzinfo


def resolve_tz(tz_id=None, offset_minutes=None):
    """
    解析时区，优先用手机上报的 IANA 时区名，其次用固定偏移，最后兜底本机时区。

    为什么不能只用本机时区：手机与电脑不同区时，同一个日历日在两台机器上
    会差出一整天（差 8 小时就跨过零点），计划条目就会被排到错误的日期上。
    """
    if tz_id:
        try:
            from zoneinfo import ZoneInfo  # Python 3.9+
            return ZoneInfo(tz_id)
        except Exception:
            pass
    if offset_minutes is not None:
        try:
            return datetime.timezone(datetime.timedelta(minutes=int(offset_minutes)))
        except Exception:
            pass
    return local_tz()


def date_ms(s, tz=None):
    """yyyy-MM-dd -> 当天 00:00 的 epoch 毫秒，与 App 的 dateToMillis 同一口径。"""
    if not isinstance(s, str):
        raise ValueError("日期必须是 yyyy-MM-dd 字符串")
    day = datetime.datetime.strptime(s, "%Y-%m-%d").date()
    tz = tz or local_tz()
    return int(datetime.datetime(day.year, day.month, day.day, tzinfo=tz).timestamp() * 1000)


def ms_to_date(ms, tz=None):
    """epoch 毫秒 -> 本地日历日。跨时区比较日期时必须用同一个 tz。"""
    tz = tz or local_tz()
    return datetime.datetime.fromtimestamp(ms / 1000.0, tz).date()


def monday_of(day):
    """该日期所在周的周一（与 App 的 previousOrSame(MONDAY) 一致）。"""
    return day - datetime.timedelta(days=day.weekday())


def num(value, default=0.0):
    """把任意值安全地转成数字：脏数据不该让整个接口崩掉。"""
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


class PairRejected(Exception):
    """配对被拒：带业务错误码，交给 HTTP 层原样回给手机。"""

    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code
        self.msg = msg


class WorkoutsConflict(Exception):
    """课表版本/数量前置条件不满足：拒绝整批写入，不做部分生效。"""

    def __init__(self, msg, detail=None):
        super().__init__(msg)
        self.msg = msg
        self.detail = detail or {}


class PlanConflict(Exception):
    """手机上传计划时基于的版本已过期：说明期间智能体写过新计划，必须先拉取。"""

    def __init__(self, msg, detail=None):
        super().__init__(msg)
        self.msg = msg
        self.detail = detail or {}


class ToolError(Exception):
    """MCP 工具参数/前置条件错误：回给调用方一句可读说明。"""


class FileLock:
    """
    跨进程文件锁。

    线程锁只管得住一个进程；桥接服务可能同时跑着 `mcp`（stdin/stdout 给智能体）
    和 `serve`（前台常驻）两个进程，两边都在改同一份 store.json。
    没有跨进程锁时，「读-改-写」会互相覆盖，实测 75 条只剩 6 条。
    """

    def __init__(self, path):
        self.path = path + ".lock"
        self._fh = None
        self._locked = False

    def __enter__(self):
        # 打开锁文件本身也可能被系统瞬时拒绝（Windows 上多进程同时创建/打开同一路径，
        # 实测 400 次并发里有 1 次 Permission denied）。这里必须重试：
        # 拿不到锁就退化成「无锁写」，等于把丢数据的问题又请回来。
        deadline = time.time() + 30
        last = None
        while True:
            try:
                self._fh = open(self.path, "a+b")
                break
            except OSError as exc:
                last = exc
                if time.time() > deadline:
                    raise
                time.sleep(0.02)
        try:
            if os.name == "nt":
                import msvcrt
                self._fh.seek(0, os.SEEK_END)
                if self._fh.tell() == 0:
                    self._fh.write(b"\0")
                    self._fh.flush()
                self._fh.seek(0)
                # 非阻塞抢锁 + 轮询等待：阻塞版 msvcrt.locking 只等 10 秒就放弃，
                # 放弃后写操作就会退化成「无锁写」，正好是丢数据的场景。
                lock_deadline = time.time() + 30
                while True:
                    try:
                        msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                        self._locked = True
                        break
                    except OSError:
                        if time.time() > lock_deadline:
                            raise
                        time.sleep(0.02)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
                self._locked = True
        except OSError as exc:
            log("取文件锁失败（退化为单进程模式）：%s" % exc)
        return self

    def __exit__(self, *exc):
        if self._fh:
            try:
                if self._locked:
                    if os.name == "nt":
                        import msvcrt
                        self._fh.seek(0)
                        msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None
            self._locked = False
        return False


# ---------------------------------------------------------------- 存储

class Store:
    """线程安全 + 跨进程安全的 JSON 存储；原子写，避免 MCP 与 HTTP 并发时写坏文件。"""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = self._blank()
        self._load()

    @staticmethod
    def _blank():
        return {
            "device": {"id": DEVICE_ID, "name": DEVICE_NAME},
            "pairs": {},          # token -> {client, clientId, permission, pairedAt, lastSeen}
            "sessions": {},       # "clientId::id" -> session
            # 旧版（无设备标识）上传的记录，迁移后冷存在这里：
            # 它们是孤儿数据，留在 sessions 里会让设备统计与总条数永远多出一截。
            "legacySessions": {},
            "plans": {},          # clientId -> {plan, revision, archives}
            "workouts": {},       # clientId -> {workouts: {id -> snapshot}, revision}
            "lastActiveClientId": "",
            "clientTz": {"tzId": "", "tzOffsetMinutes": 0},
            "lastSyncAt": 0,
            "lastSyncCount": 0,
        }

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                base = self._blank()
                base.update(loaded)
                for key in ("pairs", "sessions", "plans", "workouts"):
                    if not isinstance(base.get(key), dict):
                        base[key] = {}
                if not isinstance(base.get("lastActiveClientId"), str):
                    base["lastActiveClientId"] = ""
                if not isinstance(base.get("clientTz"), dict):
                    base["clientTz"] = {"tzId": "", "tzOffsetMinutes": 0}
                if not isinstance(base.get("planArchives"), list):
                    base["planArchives"] = []
                self.data = base
                self._migrate()
        except Exception as exc:
            log("读取存储失败，按空数据启动：%s" % exc)

    def _migrate(self):
        """旧版本的数据结构 -> 当前结构。旧版本只有一份全局计划与全局会话。"""
        data = self.data
        # 旧结构：全局 plan / planRevision / planArchives
        if data.get("plan") and DEFAULT_CLIENT not in data["plans"]:
            data["plans"][DEFAULT_CLIENT] = {
                "plan": data["plan"],
                "revision": int(data.get("planRevision", 0) or 0),
                "archives": list(data.get("planArchives") or []),
            }
        data.pop("plan", None)
        data.pop("planRevision", None)
        data.pop("planArchives", None)
        # 旧结构：会话按裸 id 存，没有设备维度
        sessions = data["sessions"]
        for key in list(sessions.keys()):
            if "::" in key:
                continue
            item = sessions.pop(key)
            if isinstance(item, dict):
                item.setdefault("clientId", DEFAULT_CLIENT)
            sessions["%s::%s" % (DEFAULT_CLIENT, key)] = item

        # default 槽是老版本的混装槽：谁先连上就把记录写进去，没有设备标识。
        # 它们不属于任何一台现役手机，留在 sessions 里只会让「设备列表 / 总条数」
        # 永远比手机本机多出一截（智能体拿它比对怎么都对不上）。
        # 这里整体挪到 legacySessions 冷存：不删数据、可追溯，但不再参与统计与查询。
        legacy_bucket = data.get("legacySessions")
        if not isinstance(legacy_bucket, dict):
            legacy_bucket = {}
            data["legacySessions"] = legacy_bucket
        for key in list(sessions.keys()):
            if not key.startswith(DEFAULT_CLIENT + "::"):
                continue
            legacy_bucket[key] = sessions.pop(key)

        # 计划同理：default 槽里可能还躺着旧版（无设备标识）上传的计划。
        # 智能体不指定设备时只认「当前活跃设备」，这个槽永远读不到，
        # 但会出现在状态里让人以为「计划也是乱的」。一并冷存。
        legacy_plans = data.get("legacyPlans")
        if not isinstance(legacy_plans, dict):
            legacy_plans = {}
            data["legacyPlans"] = legacy_plans
        default_plan_slot = data.get("plans", {}).get(DEFAULT_CLIENT)
        if isinstance(default_plan_slot, dict) and default_plan_slot.get("plan"):
            legacy_plans[DEFAULT_CLIENT] = data["plans"].pop(DEFAULT_CLIENT)

    def save(self):
        with self.lock:
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".store-", suffix=".json", dir=parent or ".")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                self._replace(tmp)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise

    def _replace(self, tmp):
        """原子替换。Windows 上目标文件被别的进程短暂占用时会报 WinError 5，重试即可。"""
        last = None
        for attempt in range(12):
            try:
                os.replace(tmp, self.path)
                return
            except PermissionError as exc:  # WinError 5 / 32：文件正被占用
                last = exc
                time.sleep(0.05 * (attempt + 1))
            except OSError as exc:
                last = exc
                break
        raise last

    @contextlib.contextmanager
    def transaction(self):
        """读-改-写全程持有跨进程锁，中途抛异常则不落盘。"""
        with self.lock:
            with FileLock(self.path):
                self._load()
                yield self.data
                self.save()

    def snapshot(self):
        with self.lock:
            self._load()
            return copy.deepcopy(self.data)

    # ---- 时区与活跃设备 ----

    def active_tz(self):
        """当前生效的时区：手机上报的优先，否则本机时区。"""
        with self.lock:
            self._load()
            info = self.data.get("clientTz") or {}
            return resolve_tz(info.get("tzId"), info.get("tzOffsetMinutes"))

    def note_tz(self, tz_id=None, offset_minutes=None):
        """记住手机上报的时区，供计划编解码使用。"""
        if not tz_id and offset_minutes is None:
            return
        with self.transaction() as data:
            info = data.setdefault("clientTz", {"tzId": "", "tzOffsetMinutes": 0})
            if tz_id:
                info["tzId"] = str(tz_id)
            if offset_minutes is not None:
                try:
                    info["tzOffsetMinutes"] = int(offset_minutes)
                except (TypeError, ValueError):
                    pass

    def active_client_id(self):
        with self.lock:
            self._load()
            return self.data.get("lastActiveClientId") or ""

    def touch_active(self, client_id):
        if not client_id:
            return
        with self.transaction() as data:
            data["lastActiveClientId"] = client_id

    # ---- 协议操作 ----

    def find_pair(self, token):
        if not token:
            return None
        with self.lock:
            self._load()
            pair = self.data["pairs"].get(token)
            return copy.deepcopy(pair) if pair else None

    def pair(self, client, token, permission, client_id=None, tz_id=None, offset_minutes=None):
        """
        登记或刷新一个配对。

        两条硬规则（防越权）：
        1. 同一个令牌只能绑定一台设备：令牌已绑过别的 clientId 就拒绝，
           否则拿到令牌的人可以冒充另一台手机读写它的数据。
        2. 同一个令牌不能提权：已登记只读的令牌再来登记读写，直接拒绝。
           想要更高权限只能断开后重新连接（手机会生成新令牌）。
        """
        with self.transaction() as data:
            old = data["pairs"].get(token)
            if old:
                old_client = old.get("clientId") or ""
                if client_id and old_client and old_client != client_id:
                    raise PairRejected(
                        "TOKEN_CONFLICT",
                        "该令牌已绑定另一台设备（%s），请在手机端断开后重新连接" % old_client)
                if old.get("permission") == "READ_ONLY" and permission == "READ_WRITE":
                    raise PairRejected(
                        "PERMISSION_ESCALATION",
                        "该令牌登记的是只读权限，不能直接提权；请在手机端断开后重新连接")
            if not client_id:
                client_id = "tok-" + hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]
            data["pairs"][token] = {
                "client": client,
                "clientId": client_id,
                "permission": permission,
                "pairedAt": old.get("pairedAt", now_ms()) if old else now_ms(),
                "lastSeen": now_ms(),
            }
            # 令牌表不该无限增长：只保留最近连接的 MAX_PAIRS 个
            if len(data["pairs"]) > MAX_PAIRS:
                ordered = sorted(data["pairs"].items(),
                                 key=lambda kv: kv[1].get("lastSeen", 0))
                for old_token, _ in ordered[:len(data["pairs"]) - MAX_PAIRS]:
                    data["pairs"].pop(old_token, None)
            info = data.setdefault("clientTz", {"tzId": "", "tzOffsetMinutes": 0})
            if tz_id:
                info["tzId"] = str(tz_id)
            if offset_minutes is not None:
                try:
                    info["tzOffsetMinutes"] = int(offset_minutes)
                except (TypeError, ValueError):
                    pass
            data["lastActiveClientId"] = client_id
            return copy.deepcopy(data["pairs"][token])

    def touch(self, token):
        with self.transaction() as data:
            pair = data["pairs"].get(token)
            if pair:
                pair["lastSeen"] = now_ms()

    def upsert_sessions(self, sessions, client_id, full_snapshot=True):
        """
        写入训练记录，按设备隔离。

        键是「clientId::会话id」：两台手机的会话 id 都从 1 开始，
        不带设备维度就会互相覆盖。

        **协议约定手机每次上传的都是本机全部记录**，所以默认按「全量快照」处理：
        本次没带到的同设备记录，说明手机上已经删了，桥端也要跟着删。
        少了这一步，槽位只增不减——手机上删掉的训练永远留在桥里，
        智能体读到的条数永远比手机本机多，拿它和任何一台手机都比对不上。

        [full_snapshot] 只在「绕过 HTTP 直接写存储」的调试/自检场景才传 False：
        这时不做裁剪，避免一次单条写入把同设备的历史全清掉。
        """
        client_id = client_id or DEFAULT_CLIENT
        saved = 0
        removed = 0
        with self.transaction() as data:
            bucket = data["sessions"]
            uploaded_ids = set()
            for s in sessions:
                sid = s.get("id")
                if sid is None or str(sid).strip() == "" or str(sid) == "None":
                    continue
                key = "%s::%s" % (client_id, sid)
                uploaded_ids.add(str(sid))
                # 旧版本按裸 id 存的记录归到 default 设备下：
                # 真实手机来报同一条时，把这条旧记录认领过去，不产生重复。
                legacy_key = "%s::%s" % (DEFAULT_CLIENT, sid)
                if key not in bucket and legacy_key in bucket:
                    legacy = bucket.pop(legacy_key)
                    if isinstance(legacy, dict):
                        legacy["clientId"] = client_id
                    bucket[key] = legacy
                item = dict(s)
                item["id"] = sid
                item["clientId"] = client_id
                bucket[key] = item
                saved += 1

            # 全量快照：删掉本设备本次没再上报的记录。只动自己的槽位，
            # default 槽与其它设备的数据一律不碰。
            if full_snapshot and client_id != DEFAULT_CLIENT:
                prefix = client_id + "::"
                for key in list(bucket.keys()):
                    if not key.startswith(prefix):
                        continue
                    if key[len(prefix):] not in uploaded_ids:
                        bucket.pop(key, None)
                        removed += 1

            data["lastSyncAt"] = now_ms()
            data["lastSyncCount"] = len(sessions)
            data["lastActiveClientId"] = client_id
            device_total = sum(1 for k in bucket.keys() if k.startswith(client_id + "::"))
            return {
                "saved": saved,
                "removed": removed,
                "deviceTotal": device_total,
                "total": len(bucket),
            }

    def _plan_slot(self, data, client_id):
        slot = data["plans"].setdefault(client_id or DEFAULT_CLIENT, {
            "plan": None, "revision": 0, "archives": [],
            # 谁最后写的、什么时候写的；手机上传不再能无声盖掉智能体计划
            "source": None, "writtenAt": 0, "phoneRevision": 0,
            # 手机对当前版本的验收结果：智能体只有看到 APPLIED 才能判定任务完成
            "acceptance": None,
        })
        if not isinstance(slot.get("archives"), list):
            slot["archives"] = []
        try:
            slot["revision"] = int(slot.get("revision", 0))
        except (TypeError, ValueError):
            slot["revision"] = 0
        try:
            slot["phoneRevision"] = int(slot.get("phoneRevision", 0))
        except (TypeError, ValueError):
            slot["phoneRevision"] = 0
        return slot

    def get_plan(self, client_id):
        with self.lock:
            self._load()
            slot = self.data["plans"].get(client_id or DEFAULT_CLIENT) or {}
            return copy.deepcopy(slot.get("plan"))

    def get_plan_revision(self, client_id):
        with self.lock:
            self._load()
            slot = self.data["plans"].get(client_id or DEFAULT_CLIENT) or {}
            return int(slot.get("revision", 0) or 0)

    def get_plan_acceptance(self, client_id):
        """当前版本的验收结果；没有记录时返回 PENDING。"""
        with self.lock:
            self._load()
            slot = self.data["plans"].get(client_id or DEFAULT_CLIENT) or {}
            revision = int(slot.get("revision", 0) or 0)
            acceptance = slot.get("acceptance")
            if not isinstance(acceptance, dict):
                return {"revision": revision, "status": "PENDING",
                        "problems": [], "message": "等待手机验收", "reportedAt": 0}
            return copy.deepcopy(acceptance)

    def record_plan_acceptance(self, client_id, revision, status, problems=None,
                               message=""):
        """
        记录手机对某个版本的验收结果。

        版本必须与当前版本一致才更新「当前验收状态」；比当前版本旧的迟到回执只进历史，
        不能把智能体刚写的新计划标成上一版的通过结果。
        """
        client_id = client_id or DEFAULT_CLIENT
        status = str(status or "").upper()
        if status not in ("APPLIED", "REJECTED"):
            raise ValueError("status 必须是 APPLIED 或 REJECTED")
        try:
            revision = int(revision)
        except (TypeError, ValueError):
            raise ValueError("revision 必须是整数")
        problems = [str(item) for item in (problems or [])]
        with self.transaction() as data:
            slot = self._plan_slot(data, client_id)
            current = int(slot.get("revision", 0))
            record = {
                "revision": revision,
                "status": status,
                "problems": problems,
                "message": str(message or ""),
                "reportedAt": now_ms(),
            }
            stale = revision != current
            history = slot.setdefault("acceptanceHistory", [])
            if not isinstance(history, list):
                history = []
                slot["acceptanceHistory"] = history
            history.append(copy.deepcopy(record))
            # 留最近 20 条足够追溯：手机每轮同步都报一次的成功记录不该无限膨胀
            if len(history) > 20:
                del history[:-20]
            if not stale:
                slot["acceptance"] = record
            return {"recorded": True, "stale": stale, "current": current,
                    "status": slot.get("acceptance", {}).get("status", "PENDING")}

    def device_last_seen(self, client_id):
        """该设备最后一次与桥端通信的时间（毫秒）；没有记录返回 0。"""
        client_id = client_id or DEFAULT_CLIENT
        with self.lock:
            self._load()
            latest = 0
            for pair in self.data.get("pairs", {}).values():
                if client_id_of(pair) != client_id:
                    continue
                try:
                    latest = max(latest, int(pair.get("lastSeen", 0) or 0))
                except (TypeError, ValueError):
                    continue
            return latest

    def set_plan(self, client_id, plan, bump_revision=True):
        """
        写入计划。智能体主动写（bump_revision=True）时，若已有计划先留档再写，
        绝不直接删除/覆盖；手机上传自己的计划（bump_revision=False）不归档。
        返回被归档的旧计划名（无则 None），供 MCP 回执展示。
        """
        with self.transaction() as data:
            slot = self._plan_slot(data, client_id)
            archived = None
            old = slot.get("plan")
            if bump_revision and old:
                archived = old.get("name") or "未命名计划"
                slot["archives"].append({
                    "plan": copy.deepcopy(old),
                    "archivedAt": now_ms(),
                    "revision": slot["revision"],
                })
                # 桥只是镜像，留最近 20 份足够追溯；App 侧归档是永久的
                if len(slot["archives"]) > 20:
                    del slot["archives"][:-20]
                log("智能体写入新计划：旧计划「%s」已留档" % archived)
            slot["plan"] = copy.deepcopy(plan)
            slot["writtenAt"] = now_ms()
            slot["source"] = "agent" if bump_revision else "phone"
            # 只给智能体写入的计划升级版本号；手机上传走 phone_upload_plan，
            # 不再直接调用这里，避免不升版本、无来源的静默覆盖。
            if bump_revision:
                slot["revision"] = int(slot.get("revision", 0)) + 1
            # 新版本必须重新等手机验收：上一版的 APPLIED 绝不能继承给新计划，
            # 否则智能体会把「上一版已落盘」误判成「这一版已完成」。
            slot["acceptance"] = {
                "revision": int(slot.get("revision", 0)),
                "status": "PENDING",
                "problems": [],
                "message": "已提交，等待手机验收",
                "reportedAt": 0,
            }
            # 智能体显式写某台设备时不能改「当前活跃设备」，否则一次临时写入
            # 会把后续省略 clientId 的调用带到错误的槽位。
            return archived

    def phone_upload_plan(self, client_id, plan, base_revision=None):
        """
        手机上传自己的计划。

        - baseRevision 必须等于当前 revision：期间智能体写过新计划就拒绝，
          手机先拉取，避免用旧计划无声覆盖智能体写入；
        - 接受后只推进 phoneRevision 并记录 source/writtenAt，不推进智能体版本，
          否则手机每轮同步都会让自己重新拉取同一份计划；
        - 智能体计划不会被直接丢弃：手机上传只在「已拉取过该版本」时才会通过。
        """
        with self.transaction() as data:
            slot = self._plan_slot(data, client_id)
            revision = int(slot.get("revision", 0))
            if base_revision is not None:
                try:
                    base_revision = int(base_revision)
                except (TypeError, ValueError):
                    raise PlanConflict("baseRevision 必须是整数")
                if base_revision != revision:
                    raise PlanConflict(
                        "计划已被智能体更新，请先拉取最新计划",
                        {"revision": revision, "baseRevision": base_revision})
            slot["plan"] = copy.deepcopy(plan)
            slot["source"] = "phone"
            slot["writtenAt"] = now_ms()
            slot["phoneRevision"] = int(slot.get("phoneRevision", 0)) + 1
            # 手机用自己的计划覆盖了智能体刚提交的那一版：只有「还没验收」才改成
            # PHONE_UPLOADED。已经 APPLIED/REJECTED 的最终结论必须保留——
            # 手机落库后会把同一份计划回传上来，不能把刚拿到的验收结果又抹掉。
            current_acceptance = slot.get("acceptance") or {}
            if not isinstance(current_acceptance, dict) or \
                    current_acceptance.get("status") in (None, "", "PENDING"):
                slot["acceptance"] = {
                    "revision": revision,
                    "status": "PHONE_UPLOADED",
                    "problems": [],
                    "message": "手机已上传自己的计划，智能体提交的版本已被覆盖",
                    "reportedAt": now_ms(),
                }
            data["lastActiveClientId"] = client_id or data.get("lastActiveClientId", "")
            return {
                "revision": revision,
                "phoneRevision": slot["phoneRevision"],
                "clientId": client_id or DEFAULT_CLIENT,
            }

    def _workout_slot(self, data, client_id):
        slot = data["workouts"].setdefault(client_id or DEFAULT_CLIENT,
                                           {"workouts": {}, "revision": 0})
        if not isinstance(slot.get("workouts"), dict):
            slot["workouts"] = {}
        if not isinstance(slot.get("deleted"), dict):
            slot["deleted"] = {}
        try:
            slot["revision"] = int(slot.get("revision", 0))
        except (TypeError, ValueError):
            slot["revision"] = 0
        return slot

    def get_workouts(self, client_id):
        with self.lock:
            self._load()
            slot = self.data["workouts"].get(client_id or DEFAULT_CLIENT) or {}
            items = slot.get("workouts") or {}
            return int(slot.get("revision", 0) or 0), [
                copy.deepcopy(item) for _, item in sorted(
                    items.items(), key=lambda kv: num(kv[1].get("id"), 0))
            ]

    def get_workout_deletions(self, client_id):
        """已被智能体删除的课表 id：手机据此删本机对应课表。"""
        with self.lock:
            self._load()
            slot = self.data["workouts"].get(client_id or DEFAULT_CLIENT) or {}
            deleted = slot.get("deleted") or {}
            return [int(wid) for wid in deleted.keys() if str(wid).isdigit()]

    @staticmethod
    def _custom_count(slot):
        """自定义课表数量：预设单独计数，绝不算进清理目标。"""
        return sum(1 for item in (slot.get("workouts") or {}).values()
                   if not item.get("isPreset"))

    def set_workouts(self, client_id, workouts, bump_revision=False, replace=False,
                     base_revision=None, source="phone"):
        """
        写入课表数据（按设备隔离）。

        - 手机上传（replace=True）：整份镜像，但必须带 base_revision；
          版本已变化就整批拒绝，避免用旧快照覆盖智能体刚做的删除；
        - 智能体 MCP 写入：按 id 增量 upsert 并升版本；
        - 已删除（墓碑）的 id 永不再接受写入，杜绝复活。
        """
        with self.transaction() as data:
            slot = self._workout_slot(data, client_id)
            revision = int(slot.get("revision", 0))
            if base_revision is not None:
                try:
                    base_revision = int(base_revision)
                except (TypeError, ValueError):
                    raise WorkoutsConflict("baseRevision 必须是整数")
                if base_revision != revision:
                    raise WorkoutsConflict(
                        "课表版本已变化，请先拉取最新状态",
                        {"revision": revision, "baseRevision": base_revision})

            old = slot.get("workouts") or {}
            rejected = [item.get("id") for item in workouts
                        if item.get("id") is not None
                        and str(item.get("id")) in (slot.get("deleted") or {})]
            if rejected:
                # 任一 id 已被删除：整批拒绝，不能因为一条墓碑就把其余课表也清掉
                raise WorkoutsConflict("包含已删除的课表，不能复活", {
                    "rejectedIds": rejected, "revision": revision})

            target = {} if replace else dict(old)
            for item in workouts:
                wid = item.get("id")
                if wid is None:
                    continue
                key = str(wid)
                item = copy.deepcopy(item)
                if item.get("isPreset"):
                    item["source"] = "phone"
                else:
                    previous = old.get(key) or {}
                    item["source"] = previous.get("source") or source
                target[key] = item
            slot["workouts"] = target
            if bump_revision:
                slot["revision"] = revision + 1
            # 只有手机上传才更新「当前活跃设备」；智能体写别的设备时不动这里
            if source == "phone":
                data["lastActiveClientId"] = client_id or data.get("lastActiveClientId", "")
            return {
                "revision": int(slot.get("revision", 0)),
                "customCount": self._custom_count(slot),
                "rejectedIds": rejected,
            }

    def delete_workouts(self, client_id, ids, expected_revision=None,
                        expected_custom_count=None):
        """
        批量、原子、幂等删除自定义课表。

        先校验版本与自定义数量，任一不符整批拒绝；删除后写墓碑且不可恢复。
        已删除的 id 再次提交视为成功（幂等），不重复升版本。
        """
        with self.transaction() as data:
            slot = self._workout_slot(data, client_id)
            revision = int(slot.get("revision", 0))
            custom_before = self._custom_count(slot)
            if expected_revision is not None and int(expected_revision) != revision:
                raise WorkoutsConflict("课表版本已变化", {
                    "revision": revision, "expectedRevision": expected_revision,
                    "customCount": custom_before})
            if expected_custom_count is not None and int(expected_custom_count) != custom_before:
                raise WorkoutsConflict("自定义课表数量与预期不一致", {
                    "customCount": custom_before,
                    "expectedCustomCount": expected_custom_count,
                    "revision": revision})

            deleted, already, missing, presets = [], [], [], []
            tombstones = slot.setdefault("deleted", {})
            for wid in ids:
                key = str(wid)
                if key in tombstones:
                    already.append(wid)
                    continue
                item = slot["workouts"].get(key)
                if not item:
                    missing.append(wid)
                    continue
                if item.get("isPreset"):
                    presets.append(wid)
                    continue
                slot["workouts"].pop(key, None)
                tombstones[key] = now_ms()
                deleted.append(wid)
            if deleted:
                slot["revision"] = revision + 1
            return {
                "revision": int(slot.get("revision", 0)),
                "beforeCustomCount": custom_before,
                "afterCustomCount": self._custom_count(slot),
                "deletedIds": deleted,
                "alreadyDeletedIds": already,
                "notFoundIds": missing,
                "presetIds": presets,
            }

    def keep_only_custom_workouts(self, client_id, keep_ids, expected_revision=None,
                                  expected_custom_count=None):
        """
        原子清理：只保留指定自定义课表，其余全部标记删除。

        把「删 11 条」变成一个全有或全无的集合操作，避免循环删除造成部分完成。
        """
        keep = {str(wid) for wid in keep_ids}
        with self.transaction() as data:
            slot = self._workout_slot(data, client_id)
            revision = int(slot.get("revision", 0))
            custom_before = self._custom_count(slot)
            if expected_revision is not None and int(expected_revision) != revision:
                raise WorkoutsConflict("课表版本已变化", {
                    "revision": revision, "expectedRevision": expected_revision,
                    "customCount": custom_before})
            if expected_custom_count is not None and int(expected_custom_count) != custom_before:
                raise WorkoutsConflict("自定义课表数量与预期不一致", {
                    "customCount": custom_before,
                    "expectedCustomCount": expected_custom_count,
                    "revision": revision})

            active_custom = {key for key, item in slot["workouts"].items()
                             if not item.get("isPreset")}
            invalid = keep - active_custom
            if invalid:
                raise WorkoutsConflict("保留列表包含不存在或不可删除的课表", {
                    "invalidIds": sorted(invalid, key=lambda v: num(v, 0))})

            tombstones = slot.setdefault("deleted", {})
            deleted = []
            for key in sorted(active_custom - keep, key=lambda v: num(v, 0)):
                slot["workouts"].pop(key, None)
                tombstones[key] = now_ms()
                deleted.append(int(key))
            if deleted:
                slot["revision"] = revision + 1
            return {
                "revision": int(slot.get("revision", 0)),
                "beforeCustomCount": custom_before,
                "afterCustomCount": self._custom_count(slot),
                "deletedIds": deleted,
            }

    def device_summary(self):
        """
        当前有哪些设备、各有多少条训练记录、最后一次出现是什么时候。

        智能体不看这个就只能靠「当前活跃设备」猜，一旦两台手机都连着，
        很容易把 A 的条数拿去和 B 的本机数据比，得出「对不上」的结论。
        """
        with self.lock:
            self._load()
            sessions = self.data["sessions"]
            counts = {}
            for item in sessions.values():
                cid = item.get("clientId") or DEFAULT_CLIENT
                counts[cid] = counts.get(cid, 0) + 1
            last_seen = {}
            for pair in self.data["pairs"].values():
                cid = pair.get("clientId")
                if not cid:
                    # 旧版客户端没上报设备标识，这些令牌不做设备维度统计
                    continue
                last_seen[cid] = max(last_seen.get(cid, 0), int(pair.get("lastSeen") or 0))
            active = self.data.get("lastActiveClientId") or ""
            devices = [
                {
                    "clientId": cid,
                    "sessions": count,
                    "lastSeen": last_seen.get(cid, 0),
                    "active": cid == active,
                }
                for cid, count in sorted(counts.items(), key=lambda kv: -kv[1])
            ]
            return devices

    def sessions_sorted(self, limit, since_ms=None, order="desc", client_id=None):
        with self.lock:
            self._load()
            items = list(self.data["sessions"].values())
        if client_id:
            items = [s for s in items if (s.get("clientId") or DEFAULT_CLIENT) == client_id]
        if since_ms is not None:
            items = [s for s in items if num(s.get("startedAt"), -1) >= since_ms]
        # 脏数据（startedAt 是字符串或缺失）不能把整个排序搞崩
        items.sort(key=lambda s: num(s.get("startedAt"), 0.0), reverse=(order != "asc"))
        return items[:limit]


# ---------------------------------------------------------------- 协议工具

def parse_discover(raw):
    """手机广播包 -> 是否本应用的发现请求。"""
    try:
        obj = json.loads(raw)
    except Exception:
        return False
    return (isinstance(obj, dict) and obj.get("v") == 1
            and obj.get("type") == "discover" and obj.get("client") == CLIENT_NAME)


def build_agent_reply():
    return json.dumps({
        "v": 1, "type": "agent", "id": DEVICE_ID, "name": DEVICE_NAME, "port": PORT,
    }, ensure_ascii=False, separators=(",", ":"))


def error_body(code, msg, detail=None):
    body = {"code": code, "msg": msg}
    if detail:
        body["detail"] = detail
    return json.dumps(body, ensure_ascii=False)


def token_from_headers(headers):
    raw = headers.get("X-Agent-Token")
    if raw and raw.strip():
        return raw.strip()
    auth = headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def client_id_of(pair):
    """配对记录里的设备标识；旧记录没有就退回默认设备。"""
    if not isinstance(pair, dict):
        return DEFAULT_CLIENT
    return pair.get("clientId") or DEFAULT_CLIENT


# ---------------------------------------------------------------- 网络服务

class NetServer:
    def __init__(self, store, port=PORT, host="0.0.0.0"):
        self.store = store
        self.port = port
        self.host = host
        self.udp_sock = None
        self.httpd = None
        self.threads = []
        self.running = False

    def start(self):
        self.running = True
        store = self.store
        port = self.port
        host = self.host

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):
                log("HTTP %s - %s" % (self.address_string(), fmt % args))

            def _send(self, code, body, ctype="application/json; charset=utf-8"):
                data = body.encode("utf-8") if isinstance(body, str) else json.dumps(
                    body, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                if self.close_connection:
                    self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data)

            def _read_body(self):
                """
                把请求体读完。

                必须读：**鉴权失败也要读**。HTTP/1.1 默认长连接，
                请求体没读完就回响应的话，残留的字节会被当成下一个请求的开头，
                解析出「Bad request version」之类的垃圾错误 —— 这会让手机在
                一次 401/403 之后，后续同连接上的请求全部失败。
                返回 (原始字节, 错误说明)。
                """
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    self.close_connection = True
                    return None, "Content-Length 不是合法数字"
                if length <= 0:
                    return None, None
                if length > MAX_BODY:
                    # 读不完也不能留着不管：这条连接只能断开
                    self.close_connection = True
                    return None, "请求体过大"
                try:
                    return self.rfile.read(length), None
                except OSError:
                    self.close_connection = True
                    return None, "请求体读取中断"

            def _parse_body(self, raw):
                if not raw:
                    return None
                try:
                    return json.loads(raw.decode("utf-8"))
                except Exception:
                    return None

            def _authorized(self, require_write=False):
                token = token_from_headers(self.headers)
                pair = store.find_pair(token)
                if not pair:
                    self._send(401, error_body("UNAUTHORIZED", "未配对或令牌失效，请重新配对"))
                    return None, None
                if require_write and pair.get("permission") != "READ_WRITE":
                    self._send(403, error_body("PERMISSION_DENIED", "当前是只读权限，不能下发训练计划或课表数据"))
                    return None, None
                store.touch(token)
                return token, pair

            def do_POST(self):
                path = self.path.split("?")[0]
                # 请求体先读完：鉴权失败也要读，否则长连接上的下一个请求会读到残缺 body
                raw, problem = self._read_body()
                if problem:
                    return self._send(400, error_body("BAD_REQUEST", problem))
                if path == "/v1/pair":
                    return self._pair(self._parse_body(raw))
                if path == "/v1/sessions":
                    return self._sessions(self._parse_body(raw))
                if path == "/v1/plan":
                    return self._push_plan(self._parse_body(raw))
                if path == "/v1/plan/result":
                    return self._plan_result(self._parse_body(raw))
                if path == "/v1/workouts":
                    return self._push_workouts(self._parse_body(raw))
                return self._send(404, error_body("NOT_FOUND", "未知端点"))

            def _pair(self, body):
                if not isinstance(body, dict):
                    return self._send(400, error_body("BAD_REQUEST", "请求体不是合法 JSON"))
                if body.get("v") != 1 or body.get("client") != CLIENT_NAME:
                    return self._send(400, error_body("BAD_VERSION", "协议版本或客户端不匹配"))
                token = body.get("token")
                perm = body.get("permission")
                if not isinstance(token, str) or not token.strip():
                    return self._send(400, error_body("BAD_TOKEN", "缺少连接令牌"))
                if perm not in PERMISSIONS:
                    return self._send(400, error_body("BAD_PERMISSION", "权限必须是 READ_ONLY 或 READ_WRITE"))
                client_id = body.get("clientId")
                if client_id is not None and not isinstance(client_id, str):
                    return self._send(400, error_body("BAD_REQUEST", "clientId 必须是字符串"))
                try:
                    record = store.pair(
                        body.get("client", CLIENT_NAME), token.strip(), perm,
                        client_id=(client_id or "").strip() or None,
                        tz_id=body.get("tzId"),
                        offset_minutes=body.get("tzOffsetMinutes"),
                    )
                except PairRejected as rejected:
                    return self._send(403, error_body(rejected.code, rejected.msg))
                log("连接成功 clientId=%s permission=%s" % (record.get("clientId"), perm))
                return self._send(200, {"code": "OK", "device": store.snapshot()["device"],
                                        "permission": record.get("permission"),
                                        "clientId": record.get("clientId")})

            def _sessions(self, body):
                token, pair = self._authorized(require_write=False)
                if not token:
                    return
                if not isinstance(body, dict):
                    return self._send(400, error_body("BAD_REQUEST", "请求体不是合法 JSON"))
                if body.get("v") != 1:
                    return self._send(400, error_body("BAD_VERSION", "协议版本不匹配"))
                # 二次校验：请求体里的 permission 必须与配对时一致
                if body.get("permission") != pair.get("permission"):
                    return self._send(403, error_body("PERMISSION_MISMATCH",
                                                      "上传包权限与配对记录不一致"))
                owner = client_id_of(pair)
                body_client = body.get("clientId")
                if body_client and body_client != owner:
                    return self._send(403, error_body("TOKEN_CONFLICT",
                                                      "上传包的设备标识与令牌绑定的设备不一致"))
                sessions = body.get("sessions")
                if not isinstance(sessions, list):
                    return self._send(400, error_body("BAD_SESSIONS", "sessions 必须是数组"))
                count = body.get("count")
                if isinstance(count, int) and count != len(sessions):
                    return self._send(400, error_body("BAD_SESSIONS",
                                                      "count(%d) 与 sessions 条数(%d) 不一致"
                                                      % (count, len(sessions))))
                for index, item in enumerate(sessions):
                    if not isinstance(item, dict):
                        return self._send(400, error_body("BAD_SESSIONS",
                                                          "sessions[%d] 不是对象" % index))
                    if item.get("id") is None:
                        return self._send(400, error_body("BAD_SESSIONS",
                                                          "sessions[%d] 缺少 id" % index))
                    if not isinstance(item.get("startedAt"), (int, float)) or isinstance(
                            item.get("startedAt"), bool):
                        return self._send(400, error_body("BAD_SESSIONS",
                                                          "sessions[%d].startedAt 必须是数字" % index))
                store.note_tz(body.get("tzId"), body.get("tzOffsetMinutes"))
                result = store.upsert_sessions(sessions, owner)
                log("收到训练记录 %d 条，入库 %d 条、按全量剔除 %d 条（设备 %s 共 %d 条）"
                    % (len(sessions), result["saved"], result["removed"], owner,
                       result["deviceTotal"]))
                return self._send(200, {"code": "OK", "received": len(sessions),
                                        "saved": result["saved"],
                                        # 手机上已删除、本次快照里不再出现而被清掉的条数：
                                        # 手机端与智能体都靠它确认「桥里就是手机的当前全量」
                                        "removed": result["removed"],
                                        "total": result["total"],
                                        "deviceTotal": result["deviceTotal"],
                                        "clientId": owner})

            def _push_plan(self, body):
                # 手机把当前计划推给桥接服务：MCP 的 get_plan 从这里读到内容。
                # 只有读写档允许——只读档的授权范围是训练历史，不含计划。
                token, pair = self._authorized(require_write=True)
                if not token:
                    return
                if not isinstance(body, dict) or body.get("v") != 1:
                    return self._send(400, error_body("BAD_VERSION", "协议版本不匹配"))
                if body.get("permission") != pair.get("permission"):
                    return self._send(403, error_body("PERMISSION_MISMATCH",
                                                      "上传包权限与配对记录不一致"))
                owner = client_id_of(pair)
                body_client = body.get("clientId")
                if body_client and body_client != owner:
                    return self._send(403, error_body("TOKEN_CONFLICT",
                                                      "上传包的设备标识与令牌绑定的设备不一致"))
                store.note_tz(body.get("tzId"), body.get("tzOffsetMinutes"))
                plan = body.get("plan")
                # 用**这台上报的手机**的时区校验：同一份计划在不同时区的手机上是不同的日期。
                # 本次请求没带时区时才退回桥端记录的时区。
                tz = None
                if body.get("tzId") or body.get("tzOffsetMinutes") is not None:
                    tz = resolve_tz(body.get("tzId"), body.get("tzOffsetMinutes"))
                problems = verify_plan_shape(plan, tz=tz or store.active_tz())
                if problems:
                    return self._send(400, error_body(
                        "BAD_PLAN", "计划格式不合法：" + "；".join(problems)))
                try:
                    uploaded = store.phone_upload_plan(
                        owner, plan, base_revision=body.get("baseRevision"))
                except PlanConflict as conflict:
                    return self._send(409, error_body("CONFLICT", conflict.msg,
                                                      conflict.detail))
                log("收到手机上传的计划「%s」，%d 次训练（版本 %d）"
                    % (plan.get("name", ""), len(plan.get("entries", [])),
                       uploaded["revision"]))
                return self._send(200, {
                    "code": "OK",
                    "name": plan.get("name", ""),
                    "entries": len(plan.get("entries", [])),
                    "clientId": owner,
                    "revision": uploaded["revision"],
                    "phoneRevision": uploaded["phoneRevision"],
                })

            def _plan_result(self, body):
                # 手机上报验收结果：智能体只有读到 APPLIED 才能判定计划真正落盘
                token, pair = self._authorized(require_write=True)
                if not token:
                    return
                if not isinstance(body, dict) or body.get("v") != 1:
                    return self._send(400, error_body("BAD_VERSION", "协议版本不匹配"))
                if body.get("permission") != pair.get("permission"):
                    return self._send(403, error_body("PERMISSION_MISMATCH",
                                                      "上报包权限与配对记录不一致"))
                owner = client_id_of(pair)
                body_client = body.get("clientId")
                if body_client and body_client != owner:
                    return self._send(403, error_body("TOKEN_CONFLICT",
                                                      "上报包的设备标识与令牌绑定的设备不一致"))
                revision = body.get("revision")
                if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
                    return self._send(400, error_body("BAD_REQUEST", "revision 必须是正整数"))
                status = str(body.get("status") or "").upper()
                if status not in ("APPLIED", "REJECTED"):
                    return self._send(400, error_body(
                        "BAD_REQUEST", "status 必须是 APPLIED 或 REJECTED"))
                problems = body.get("problems") or []
                if not isinstance(problems, list):
                    return self._send(400, error_body("BAD_REQUEST", "problems 必须是数组"))
                if status == "REJECTED" and not problems:
                    problems = ["手机已拒绝该计划，但未提供明细"]
                try:
                    result = store.record_plan_acceptance(
                        owner, revision, status, problems, body.get("message", ""))
                except ValueError as exc:
                    return self._send(400, error_body("BAD_REQUEST", str(exc)))
                log("手机验收结果：设备 %s 版本 %d %s（当前版本 %d%s）"
                    % (owner, revision, status, result["current"],
                       "，迟到回执" if result["stale"] else ""))
                return self._send(200, {
                    "code": "OK",
                    "clientId": owner,
                    "revision": revision,
                    "status": status,
                    "stale": result["stale"],
                    "currentRevision": result["current"],
                })

            def _push_workouts(self, body):
                # 课表数据读取权限：手机把课表库推给智能体（镜像，不升版本）。
                token, pair = self._authorized()
                if not token:
                    return
                if not isinstance(body, dict) or body.get("v") != 1:
                    return self._send(400, error_body("BAD_VERSION", "协议版本不匹配"))
                if body.get("permission") != pair.get("permission"):
                    return self._send(403, error_body("PERMISSION_MISMATCH",
                                                      "上传包权限与配对记录不一致"))
                owner = client_id_of(pair)
                body_client = body.get("clientId")
                if body_client and body_client != owner:
                    return self._send(403, error_body("TOKEN_CONFLICT",
                                                      "上传包的设备标识与令牌绑定的设备不一致"))
                workouts = body.get("workouts")
                if not isinstance(workouts, list):
                    return self._send(400, error_body("BAD_REQUEST", "workouts 必须是数组"))
                for index, item in enumerate(workouts):
                    if not isinstance(item, dict) or item.get("id") is None:
                        return self._send(400, error_body("BAD_REQUEST",
                                                          "workouts[%d] 缺少 id" % index))
                    if not workout_snapshot_ok(item.get("snapshot")):
                        return self._send(400, error_body(
                            "BAD_REQUEST",
                            "workouts[%d].snapshot 不是可解析的课表快照：%s"
                            % (index, str(item.get("snapshot"))[:120])))
                try:
                    # 手机上传是镜像，但必须基于当前版本：旧快照不能覆盖智能体的删除
                    result = store.set_workouts(
                        owner, workouts, bump_revision=False, replace=True,
                        base_revision=body.get("baseRevision"),
                        source="phone",
                    )
                except WorkoutsConflict as conflict:
                    return self._send(409, error_body("CONFLICT", conflict.msg,
                                                      conflict.detail))
                log("收到手机上传的课表 %d 套（设备 %s，拒绝复活 %d 条）"
                    % (len(workouts), owner, len(result["rejectedIds"])))
                return self._send(200, {
                    "code": "OK",
                    "received": len(workouts),
                    "rejectedIds": result["rejectedIds"],
                    "customCount": result["customCount"],
                    "revision": result["revision"],
                    "clientId": owner,
                })

            def do_GET(self):
                path = self.path.split("?")[0]
                if path == "/v1/ping":
                    # 探活：只验「桥端活着且认识这个令牌」，不要求写权限、不读计划体。
                    # 手机设置页的状态灯靠它区分「已配对」与「真的连得上」。
                    token, pair = self._authorized()
                    if not token:
                        return
                    return self._send(200, {
                        "code": "OK",
                        "clientId": client_id_of(pair),
                        "permission": pair.get("permission"),
                    })
                if path == "/v1/plan":
                    # 服务端反查权限，不信请求体；只读档直接拒绝
                    token, pair = self._authorized(require_write=True)
                    if not token:
                        return
                    owner = client_id_of(pair)
                    plan = store.get_plan(owner)
                    if not plan:
                        return self._send(404, error_body("NO_PLAN", "智能体侧还没有训练计划"))
                    return self._send(200, {
                        "code": "OK",
                        "revision": store.get_plan_revision(owner),
                        "plan": plan,
                        "clientId": owner,
                    })
                if path == "/v1/workouts":
                    # 课表数据写入权限：智能体写入的课表才允许本机拉取落库
                    token, pair = self._authorized(require_write=True)
                    if not token:
                        return
                    owner = client_id_of(pair)
                    revision, workouts = store.get_workouts(owner)
                    custom = [w for w in workouts if not w.get("isPreset")]
                    presets = [w for w in workouts if w.get("isPreset")]
                    return self._send(200, {
                        "code": "OK",
                        "workoutRevision": revision,
                        "customCount": len(custom),
                        "presetCount": len(presets),
                        "workouts": workouts,
                        "deletedIds": store.get_workout_deletions(owner),
                        "clientId": owner,
                    })
                return self._send(404, error_body("NOT_FOUND", "未知端点"))

        class ExclusiveHTTPServer(ThreadingHTTPServer):
            """
            端口独占：一台机器只允许一个 bridge 进程接管 HTTP 端口。

            Windows 下 HTTPServer 默认打开 SO_REUSEADDR，两个进程可以同时 bind 同一个端口，
            结果「谁先启动谁一直服务、新进程也在监听」，连接随机落到两个进程上——
            旧进程还在跑旧代码时，手机上看到的就是修了也不生效、数据一会儿一个样。
            关掉它：第二个进程 bind 直接失败，日志里能立刻看出来。
            """

            allow_reuse_address = (os.name != "nt")
            daemon_threads = True

        self.httpd = ExclusiveHTTPServer((host, port), Handler)
        self.threads.append(threading.Thread(target=self.httpd.serve_forever,
                                             name="http", daemon=True))

        def udp_loop():
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Windows 上不要 SO_REUSEADDR：那会让两个进程都收到发现广播、
            # 各自回一个包，手机在两条连接之间来回跳。独占绑定才是「一台机器一个桥」。
            if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind((host, port))
            self.udp_sock = sock
            reply = build_agent_reply().encode("utf-8")
            sock.settimeout(0.5)
            log("UDP 发现监听 %s:%d" % (host, port))
            while self.running:
                try:
                    data, addr = sock.recvfrom(4096)
                    if parse_discover(data.decode("utf-8", "replace")):
                        sock.sendto(reply, addr)
                        log("响应发现请求 from %s:%d" % (addr[0], addr[1]))
                except socket.timeout:
                    continue
                except OSError:
                    if self.running:
                        log("UDP 异常：" + traceback.format_exc(limit=1))
                    break

        self.threads.append(threading.Thread(target=udp_loop, name="udp", daemon=True))
        for t in self.threads:
            t.start()
        log("网络服务已启动（HTTP + UDP :%d）" % port)

    def stop(self):
        self.running = False
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self.udp_sock:
            try:
                self.udp_sock.close()
            except OSError:
                pass
        log("网络服务已停止")


# ---------------------------------------------------------------- 计划编码

def build_plan(name, start_date, weeks, note, entries, now=None, tz=None, notes_out=None):
    """把友好参数编成 App 的 decodePlan 能直接解析的计划 JSON。"""
    now = now or now_ms()
    if not isinstance(weeks, int) or isinstance(weeks, bool) or not (2 <= weeks <= 16):
        raise ValueError("weeks 必须是 2..16 的整数")
    if not isinstance(entries, list):
        raise ValueError("entries 必须是数组")
    start_ms = date_ms(start_date, tz)
    out_entries = []
    for idx, e in enumerate(entries):
        if not isinstance(e, dict):
            raise ValueError("第 %d 条 entry 不是对象" % (idx + 1))
        date = e.get("date")
        if not isinstance(date, str):
            raise ValueError("第 %d 条 entry 缺少 date(yyyy-MM-dd)" % (idx + 1))
        item = {
            "id": int(e.get("id", idx + 1)),
            "date": date_ms(date, tz),
            "hour": int(e.get("hour", 19)),
            "minute": int(e.get("minute", 0)),
            "workoutId": int(e.get("workoutId", 0)),
            "workoutName": str(e.get("workoutName", "")),
        }
        if not (0 <= item["hour"] <= 23):
            raise ValueError("第 %d 条 hour 越界" % (idx + 1))
        if not (0 <= item["minute"] <= 59):
            raise ValueError("第 %d 条 minute 越界" % (idx + 1))
        steps = e.get("steps")
        if steps:
            item["customWorkout"] = encode_workout_snapshot(
                item["workoutName"] or "计划内训练", steps, now, notes_out=notes_out)
        out_entries.append(item)
    return {
        "name": str(name or "智能体训练计划"),
        "startDate": start_ms,
        "weeks": weeks,
        "note": str(note or ""),
        "entries": out_entries,
    }


STEP_NAME_MAX_CHARS = 7


def truncate_step_label(raw):
    """步骤名统一 7 个字符，与 App 编辑器的输入上限一致。"""
    text = str(raw or "")
    return text[:STEP_NAME_MAX_CHARS]


def normalize_snapshot_labels(raw):
    """把已有 customWorkout 快照里的超长步骤名截短，保留其余字段原样。"""
    if not isinstance(raw, str) or not raw.strip():
        return raw
    try:
        obj = json.loads(raw)
        steps = obj.get("workout", {}).get("steps") or []
        changed = False
        for step in steps:
            if isinstance(step, dict) and step.get("label"):
                short = truncate_step_label(step["label"])
                if short != step["label"]:
                    step["label"] = short
                    changed = True
        if not changed:
            return raw
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return raw


# ---------------------------------------------------------------- App 训练段规则
#
# 这里是智能体与 App 之间的规则闸门。桥端不信任模型输出的任何字段：
# 先按 App 的字段语义做参数映射，再逐条校验；不合规一律拒绝写入并给出修正清单，
# 让智能体重新生成，而不是等落库后到手机上才暴露（手机侧只能整份拒收）。
#
# 规则来源（改这里必须同步改 App 的 AgentPlanRules.kt）：
# - Workout.kt：durationSec / distanceM 二选一；热身只有按时间且不带目标；
#   快跑与慢跑在同一课表内共用一种计量方式。
# - StepEditSheet：时长 1..10859 秒、距离 100..21900 米、心率 40..220 bpm、
#   配速 150..600 秒/公里、步频 120..220、步骤名 7 个字符。
# - WorkoutExpander：循环组＝相邻且 repeatGroupId 相同，重复次数取组内首段，1..99。

DURATION_SEC_RANGE = (1, 10859)        # 编辑上限 180'59"
DISTANCE_M_RANGE = (100, 21900)        # 编辑上限 21.9 km
HR_BPM_RANGE = (40, 220)
HR_PERCENT_RANGE = (1.0, 150.0)
PACE_SEC_RANGE = (150.0, 600.0)        # 2'30" ~ 10'00" /km
CADENCE_RANGE = (120, 220)
HR_GATE_BPM_RANGE = (40, 220)
HR_GATE_PERCENT_RANGE = (1, 100)
REPEAT_RANGE = (1, 99)

# 手机在 90 秒内与桥端通信过就算在线：自动同步周期是 15 秒，留足网络抖动余量
PHONE_ONLINE_WINDOW_MS = 90_000

# 智能体可用的目标类型：DISTANCE 是历史遗留，App 编辑器不产生、引擎也不消费。
AGENT_TARGET_TYPES = ("OPEN", "HR_RANGE", "PACE_RANGE")

STEP_NAMES = {"WARMUP": "热身", "WORK": "快跑", "RECOVERY": "慢跑", "COOLDOWN": "缓和"}

STEP_TYPE_ALIASES = {
    "WARMUP": "WARMUP", "WARM_UP": "WARMUP", "WARM-UP": "WARMUP", "热身": "WARMUP",
    "WORK": "WORK", "FAST": "WORK", "INTERVAL": "WORK", "快跑": "WORK",
    "RECOVERY": "RECOVERY", "JOG": "RECOVERY", "SLOW": "RECOVERY",
    "慢跑": "RECOVERY", "恢复": "RECOVERY",
    "COOLDOWN": "COOLDOWN", "COOL_DOWN": "COOLDOWN", "COOL-DOWN": "COOLDOWN",
    "缓和": "COOLDOWN", "冷身": "COOLDOWN",
}

TARGET_TYPE_ALIASES = {
    "OPEN": "OPEN", "NONE": "OPEN", "NO_TARGET": "OPEN", "无目标": "OPEN",
    "HR_RANGE": "HR_RANGE", "HEART_RATE": "HR_RANGE", "HR": "HR_RANGE", "心率": "HR_RANGE",
    "PACE_RANGE": "PACE_RANGE", "PACE": "PACE_RANGE", "配速": "PACE_RANGE",
    "DISTANCE": "DISTANCE", "距离": "DISTANCE",
}

STEP_KEY_ALIASES = {
    "type": "type", "kind": "type", "stepType": "type",
    # App 原生快照会带 order：段序以数组下标为准，这里接受但不再覆盖
    "order": "order",
    "durationSec": "durationSec", "duration": "durationSec",
    "durationSeconds": "durationSec", "timeSec": "durationSec", "seconds": "durationSec",
    "distanceM": "distanceM", "distance": "distanceM",
    "distanceMeters": "distanceM", "meters": "distanceM",
    "label": "label", "name": "label",
    "repeatCount": "repeatCount", "repeat": "repeatCount", "times": "repeatCount",
    "repeatGroupId": "repeatGroupId", "groupId": "repeatGroupId",
    "loopGroupId": "repeatGroupId",
    "hrGateMaxBpm": "hrGateMaxBpm", "hrGatePercent": "hrGatePercent",
    "cadenceSpm": "cadenceSpm", "cadence": "cadenceSpm",
    "target": "target",
}

TARGET_KEY_ALIASES = {
    "type": "type", "kind": "type", "targetType": "type",
    "minValue": "minValue", "min": "minValue",
    "maxValue": "maxValue", "max": "maxValue",
    "percent": "percent", "isPercent": "percent",
}

APP_STEP_RULES_TEXT = (
    "App 课表硬规则（违反任意一条都会被拒绝写入并需要重新生成）：\n"
    "1. 每段 durationSec 与 distanceM 必须二选一，禁止同时出现，也禁止都不给；\n"
    "2. 快跑(WORK)与慢跑(RECOVERY)在同一套训练里必须统一计量方式："
    "要么全部按时间，要么全部按距离，不允许混用；\n"
    "3. 热身(WARMUP)只能按时间（只填 durationSec），且不能设置 target；\n"
    "4. target.type 只能是 OPEN / HR_RANGE / PACE_RANGE。"
    "HR_RANGE 必须显式给 percent：true=强度百分比(1..150)，false=绝对心率(40..220 bpm)；"
    "PACE_RANGE 单位是秒/公里(150..600)，也接受 \"4:30\"；OPEN 不能带 minValue/maxValue；\n"
    "5. 循环：同一 repeatGroupId 的步骤必须相邻且 repeatCount 相同（1..99）；"
    "单个步骤要重复时同时给 repeatCount 与 repeatGroupId；\n"
    "6. cadenceSpm 只用于快跑/慢跑(120..220)；hrGatePercent / hrGateMaxBpm 只用于慢跑，二选一；\n"
    "7. 每条 entry 必须给可执行的 steps（推荐），或 list_workouts 返回的真实 workoutId；"
    "只写 workoutName 不算训练内容；\n"
    "8. 目标类型为 OPEN（无目标）时，workoutName/label/note/description 里禁止出现心率、配速、"
    "步频的数值标准；\n"
    "9. 心率、配速、步频、距离、时长必须写进对应字段（target/cadenceSpm/distanceM/durationSec），"
    "workoutName/label/note 只写名称；文本里出现的数值必须与结构化字段一致，"
    "否则 App 读不到、实时提醒不会触发。"
)


APP_PLAN_WORKFLOW_TEXT = (
    "计划任务必须按五步闭环执行，任何一步都不能省略：\n"
    "1) 生成：按下面列出的 App 课表硬规则生成；\n"
    "2) 自检：调用 validate_plan。problems 非空时先修正并重新自检，"
    "自检不通过禁止提交；\n"
    "3) 提交：调用 set_plan。返回 status=PENDING_APP_ACCEPTANCE 只代表「已提交到桥端」，"
    "不代表完成，也不代表手机已落盘；\n"
    "4) 验收：调用 wait_for_plan_result（timeoutSec 默认 45）等待手机结果；"
    "超时用 get_plan_status 继续查询；\n"
    "5) 回传：只有 status=APPLIED 才能向用户宣布完成；status=REJECTED 时按 problems "
    "修正后从第 1 步重走；PENDING/TIMEOUT/NO_DEVICE/PHONE_UPLOADED 一律不得宣布完成，"
    "必须明确告诉用户「计划尚未落盘」。"
)


class RuleViolation(ValueError):
    """模型输出违反 App 课表规则：携带逐条问题，调用方据此拦截并要求重新生成。"""

    def __init__(self, problems):
        self.problems = list(problems)
        ValueError.__init__(self, "；".join(self.problems))


def _range_text(rng):
    return "%s..%s" % (rng[0], rng[1])


def _rule_number(value):
    """数值解析：接受 92 / 92.0 / "92"；布尔与空值不算数字。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("，", ",")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _clock_seconds(value):
    """把 "4:30" / "4'30\\"" / "4分30秒" 读成秒数；读不出返回 None。"""
    text = str(value).strip()
    for mark in ("′", "’", "'", "分"):
        text = text.replace(mark, ":")
    for mark in ("″", "”", '"', "秒"):
        text = text.replace(mark, "")
    text = text.replace("：", ":").strip()
    for suffix in ("/km", "/公里", "每公里"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    if ":" not in text:
        return None
    parts = [part.strip() for part in text.split(":")]
    if len(parts) != 2:
        return None
    minutes = _rule_number(parts[0])
    seconds = _rule_number(parts[1])
    if minutes is None or seconds is None:
        return None
    return minutes * 60 + seconds


def _duration_seconds(value):
    number = _rule_number(value)
    return number if number is not None else _clock_seconds(value)


def _distance_meters(value):
    number = _rule_number(value)
    if number is not None:
        return number
    text = str(value).strip().lower().replace(" ", "")
    if text.endswith("km"):
        number = _rule_number(text[:-2])
        return None if number is None else number * 1000
    if text.endswith("m"):
        return _rule_number(text[:-1])
    return None


def _pace_seconds(value):
    number = _rule_number(value)
    return number if number is not None else _clock_seconds(value)


def _int_value(value):
    number = _rule_number(value)
    if number is None:
        return None
    rounded = int(number + 0.5) if number >= 0 else int(number - 0.5)
    if abs(number - rounded) > 1e-6:
        return None
    return rounded


def _bool_value(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value) if value in (0, 1) else None
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "y", "是"):
        return True
    if text in ("false", "0", "no", "n", "否"):
        return False
    return None


def _canonical_keys(raw, aliases, prefix, problems):
    """把模型常用写法映射成 App 字段名；认不出的字段直接报错，绝不静默丢弃。"""
    lowered = {str(key).lower(): canon for key, canon in aliases.items()}
    out = {}
    for key, value in raw.items():
        canon = aliases.get(key) if key in aliases else lowered.get(str(key).strip().lower())
        if canon is None:
            problems.append("%s 有无法识别的字段 \"%s\"；可用字段：%s"
                            % (prefix, key,
                               "、".join(sorted(set(aliases.values()) - {"order"}))))
            continue
        if canon in out and out[canon] != value:
            problems.append("%s 同时给了 \"%s\" 的两种写法，取值不一致" % (prefix, canon))
            continue
        out[canon] = value
    return out


def _normalize_target(raw, prefix, problems):
    """把 target 映射成 App 的 TargetSpec，并校验类型与区间。"""
    target = {"type": "OPEN", "percent": False}
    if raw is None:
        return target
    if not isinstance(raw, dict):
        problems.append(prefix + "的 target 必须是对象")
        return target
    fields = _canonical_keys(raw, TARGET_KEY_ALIASES, prefix + "的 target", problems)
    raw_type = fields.get("type")
    if raw_type is None:
        ttype = "OPEN"
        if "minValue" in fields or "maxValue" in fields or "percent" in fields:
            problems.append(prefix + "的 target 缺少 type；写了 minValue/maxValue 时必须显式写 "
                            "HR_RANGE 或 PACE_RANGE")
    else:
        type_key = str(raw_type).strip().upper().replace(" ", "_").replace("-", "_")
        ttype = TARGET_TYPE_ALIASES.get(type_key) or TARGET_TYPE_ALIASES.get(str(raw_type).strip())
        if ttype is None:
            problems.append("%s 的 target.type=\"%s\" 无法识别；只能是 %s"
                            % (prefix, raw_type, "/".join(AGENT_TARGET_TYPES)))
            ttype = "OPEN"
    if ttype == "DISTANCE":
        problems.append(prefix + "的 target.type=DISTANCE 不受 App 支持："
                        "距离请写在步骤的 distanceM 上，目标用 OPEN/HR_RANGE/PACE_RANGE")
        ttype = "OPEN"
    target["type"] = ttype

    percent_raw = fields.get("percent")
    percent = _bool_value(percent_raw) if percent_raw is not None else None
    if percent_raw is not None and percent is None:
        problems.append(prefix + "的 target.percent 不是布尔值")
        percent = False
    if ttype == "HR_RANGE" and percent is None:
        problems.append(prefix + "的心率目标必须显式给 target.percent："
                        "true=强度百分比(1..150)，false=绝对心率(40..220 bpm)")
        percent = False
    target["percent"] = bool(percent)

    min_raw = fields.get("minValue")
    max_raw = fields.get("maxValue")
    if ttype == "OPEN":
        if target["percent"]:
            problems.append(prefix + "的 target.type=OPEN 不需要 percent（只有 HR_RANGE 用它）")
        if min_raw is not None or max_raw is not None:
            problems.append(prefix + "的 target.type=OPEN 不能带 minValue/maxValue")
        return target

    if ttype == "HR_RANGE":
        if min_raw is None or max_raw is None:
            problems.append(prefix + "的 HR_RANGE 必须同时给 minValue 与 maxValue")
            return target
        lo = _rule_number(min_raw)
        hi = _rule_number(max_raw)
        if lo is None or hi is None:
            problems.append(prefix + "的心率目标不是有效数字")
            return target
        rng = HR_PERCENT_RANGE if target["percent"] else HR_BPM_RANGE
        if not (rng[0] <= lo <= rng[1]) or not (rng[0] <= hi <= rng[1]):
            unit = ("百分比 %s" % _range_text(HR_PERCENT_RANGE) if target["percent"]
                    else "绝对心率 %s bpm" % _range_text(HR_BPM_RANGE))
            problems.append("%s 的心率目标越界（%s）" % (prefix, unit))
        elif lo > hi:
            problems.append(prefix + "的心率目标 minValue 不能大于 maxValue")
        else:
            target["minValue"] = float(lo)
            target["maxValue"] = float(hi)
        return target

    # PACE_RANGE
    if target["percent"]:
        problems.append(prefix + "的配速目标不能用 percent（percent 只用于心率）")
    if min_raw is None and max_raw is None:
        # 允许空配速：距离段由马拉松成绩换算（亚索 800 就是这种写法）
        return target
    if min_raw is None or max_raw is None:
        problems.append(prefix + "的配速目标要么两个值都给，要么都不给")
        return target
    lo = _pace_seconds(min_raw)
    hi = _pace_seconds(max_raw)
    if lo is None or hi is None:
        problems.append(prefix + "的配速目标不是有效秒数（也接受 \"4:30\" 写法）")
        return target
    if not (PACE_SEC_RANGE[0] <= lo <= PACE_SEC_RANGE[1]) or \
            not (PACE_SEC_RANGE[0] <= hi <= PACE_SEC_RANGE[1]):
        problems.append("%s 的配速目标越界（%s 秒/公里）" % (prefix, _range_text(PACE_SEC_RANGE)))
    elif lo > hi:
        problems.append(prefix + "的配速目标 minValue 不能大于 maxValue")
    else:
        target["minValue"] = float(lo)
        target["maxValue"] = float(hi)
    return target


def normalize_step(raw, index, group_ids):
    """把一个模型步骤映射成 App 的 Step 字段；返回 (step, notes, problems)。"""
    prefix = "第 %d 段" % (index + 1)
    problems = []
    notes = []
    if not isinstance(raw, dict):
        return None, notes, [prefix + "不是对象"]
    fields = _canonical_keys(raw, STEP_KEY_ALIASES, prefix, problems)

    raw_type = fields.get("type", "WORK")
    type_key = str(raw_type).strip().upper().replace(" ", "_").replace("-", "_")
    stype = STEP_TYPE_ALIASES.get(type_key) or STEP_TYPE_ALIASES.get(str(raw_type).strip())
    if stype is None:
        problems.append("%s 的 type=\"%s\" 无法识别；只能是 WARMUP/WORK/RECOVERY/COOLDOWN"
                        % (prefix, raw_type))
        stype = "WORK"
    step = {"order": index, "type": stype}

    has_duration = "durationSec" in fields
    has_distance = "distanceM" in fields
    duration = None
    distance = None
    if has_duration:
        duration = _duration_seconds(fields["durationSec"])
        if duration is None:
            problems.append(prefix + "的 durationSec 不是有效秒数（也接受 \"10:00\"）")
    if has_distance:
        distance = _distance_meters(fields["distanceM"])
        if distance is None:
            problems.append(prefix + "的 distanceM 不是有效米数（也接受 \"1km\"）")
    if has_duration and has_distance:
        problems.append(prefix + "同时设置了 durationSec 与 distanceM：同一段只能二选一")
    elif not has_duration and not has_distance:
        problems.append(prefix + "既没有 durationSec 也没有 distanceM：必须二选一")
    if duration is not None:
        duration_int = _int_value(duration)
        if duration_int is None:
            problems.append(prefix + "的 durationSec 必须是整数秒")
        elif not (DURATION_SEC_RANGE[0] <= duration_int <= DURATION_SEC_RANGE[1]):
            problems.append("%s 的 durationSec=%s 越界（%s 秒）"
                            % (prefix, duration_int, _range_text(DURATION_SEC_RANGE)))
        else:
            step["durationSec"] = duration_int
    if distance is not None:
        distance_int = _int_value(distance)
        if distance_int is None:
            problems.append(prefix + "的 distanceM 必须是整数米")
        elif not (DISTANCE_M_RANGE[0] <= distance_int <= DISTANCE_M_RANGE[1]):
            problems.append("%s 的 distanceM=%s 越界（%s 米）"
                            % (prefix, distance_int, _range_text(DISTANCE_M_RANGE)))
        else:
            step["distanceM"] = distance_int

    if stype == "WARMUP":
        if has_distance:
            problems.append(prefix + "是热身：只能按时间（durationSec），不能填 distanceM")
            step.pop("distanceM", None)
        if duration is None:
            problems.append(prefix + "是热身：必须给 durationSec")

    step["target"] = _normalize_target(fields.get("target"), prefix, problems)
    if stype == "WARMUP" and step["target"]["type"] != "OPEN":
        problems.append(prefix + "是热身：不能设置目标区间")

    repeat = 1
    if "repeatCount" in fields:
        repeat = _int_value(fields["repeatCount"])
        if repeat is None:
            problems.append(prefix + "的 repeatCount 不是整数")
            repeat = 1
        elif not (REPEAT_RANGE[0] <= repeat <= REPEAT_RANGE[1]):
            problems.append("%s 的 repeatCount=%s 越界（%s）"
                            % (prefix, repeat, _range_text(REPEAT_RANGE)))
            repeat = 1
    group = None
    if "repeatGroupId" in fields:
        group = _int_value(fields["repeatGroupId"])
        if group is None or group <= 0:
            problems.append(prefix + "的 repeatGroupId 必须是正整数")
            group = None
    if group is None and repeat > 1:
        # 单段重复：App 只在循环组里读取 repeatCount，这里补成组语义，避免参数被静默忽略
        group = (max(group_ids) + 1) if group_ids else 1
        group_ids.add(group)
        notes.append("%s 只写了 repeatCount=%d 没写 repeatGroupId：已补齐为循环组 %d"
                     % (prefix, repeat, group))
    step["repeatGroupId"] = group
    step["repeatCount"] = repeat

    if "cadenceSpm" in fields:
        cadence = _int_value(fields["cadenceSpm"])
        if cadence is None:
            problems.append(prefix + "的 cadenceSpm 不是整数")
        elif stype not in ("WORK", "RECOVERY"):
            problems.append("%s 是%s：cadenceSpm 只用于快跑/慢跑"
                            % (prefix, STEP_NAMES.get(stype, stype)))
        elif not (CADENCE_RANGE[0] <= cadence <= CADENCE_RANGE[1]):
            problems.append("%s 的 cadenceSpm=%s 越界（%s）"
                            % (prefix, cadence, _range_text(CADENCE_RANGE)))
        else:
            step["cadenceSpm"] = cadence

    gate_bpm = None
    gate_percent = None
    if "hrGateMaxBpm" in fields:
        gate_bpm = _int_value(fields["hrGateMaxBpm"])
        if gate_bpm is None or not (HR_GATE_BPM_RANGE[0] <= gate_bpm <= HR_GATE_BPM_RANGE[1]):
            problems.append("%s 的 hrGateMaxBpm 越界或不是整数（%s bpm）"
                            % (prefix, _range_text(HR_GATE_BPM_RANGE)))
            gate_bpm = None
    if "hrGatePercent" in fields:
        gate_percent = _int_value(fields["hrGatePercent"])
        if gate_percent is None or \
                not (HR_GATE_PERCENT_RANGE[0] <= gate_percent <= HR_GATE_PERCENT_RANGE[1]):
            problems.append("%s 的 hrGatePercent 越界或不是整数（%s%%）"
                            % (prefix, _range_text(HR_GATE_PERCENT_RANGE)))
            gate_percent = None
    if gate_bpm is not None or gate_percent is not None:
        if stype != "RECOVERY":
            problems.append("%s 是%s：心率门槛只用于慢跑"
                            % (prefix, STEP_NAMES.get(stype, stype)))
        elif gate_bpm is not None and gate_percent is not None:
            problems.append(prefix + "的 hrGateMaxBpm 与 hrGatePercent 只能二选一")
        else:
            if gate_bpm is not None:
                step["hrGateMaxBpm"] = gate_bpm
            if gate_percent is not None:
                step["hrGatePercent"] = gate_percent

    label = fields.get("label")
    if label is not None:
        raw_label = str(label)
        short = truncate_step_label(raw_label)
        if short.strip():
            step["label"] = short
            # 原始文本留给参数一致性校验：7 字截断可能正好把配速/心率数字截掉，
            # 不能因为截断就漏掉「参数只写在文本里」的问题
            if short != raw_label:
                step["_raw_label"] = raw_label

    return step, notes, problems


def normalize_workout_steps(steps, context="训练"):
    """把模型给的 steps 映射成 App 字段并逐条校验；返回 (normalized, problems, notes)。"""
    problems = []
    notes = []
    if not isinstance(steps, list) or not steps:
        return [], [context + " 没有任何步骤：每条 entry 必须给出可执行的 steps"], notes

    # 先收集显式循环组 id，单段 repeatCount>1 补组时不会撞号
    group_ids = set()
    for raw in steps:
        if not isinstance(raw, dict):
            continue
        value = raw.get("repeatGroupId", raw.get("groupId", raw.get("loopGroupId")))
        parsed = _int_value(value) if value is not None else None
        if parsed is not None and parsed > 0:
            group_ids.add(parsed)

    normalized = []
    for index, raw in enumerate(steps):
        step, step_notes, step_problems = normalize_step(raw, index, group_ids)
        notes.extend(step_notes)
        problems.extend(step_problems)
        if step is not None:
            normalized.append(step)

    # 快跑与慢跑必须统一计量方式（App 的 About/编辑器同一条规则）
    modes = []
    for step in normalized:
        if step["type"] not in ("WORK", "RECOVERY"):
            continue
        if step.get("distanceM") is not None and step.get("durationSec") is None:
            modes.append((step["order"], step["type"], "距离"))
        elif step.get("durationSec") is not None and step.get("distanceM") is None:
            modes.append((step["order"], step["type"], "时间"))
    if len({mode for _, _, mode in modes}) > 1:
        detail = "、".join("第 %d 段%s按%s" % (order + 1, STEP_NAMES.get(stype, stype), mode)
                          for order, stype, mode in modes)
        problems.append(context + " 里快跑与慢跑混用了两种计量方式（" + detail + "）："
                        "同一套训练必须统一为全部按时间（只填 durationSec）"
                        "或全部按距离（只填 distanceM）")

    # 循环组：相邻 + 次数一致，与 WorkoutExpander 的展开语义一致
    groups = {}
    for step in normalized:
        gid = step.get("repeatGroupId")
        if gid is None:
            continue
        groups.setdefault(gid, []).append((step["order"], step.get("repeatCount", 1)))
    for gid, items in groups.items():
        indexes = [order for order, _ in items]
        if indexes != list(range(indexes[0], indexes[-1] + 1)):
            problems.append("%s 的循环组 %s 跨了不连续的段（第 %s 段）：同组步骤必须相邻"
                            % (context, gid, "、".join(str(i + 1) for i in indexes)))
        counts = sorted({count for _, count in items})
        if len(counts) > 1:
            problems.append("%s 的循环组 %s 内 repeatCount 不一致（%s）：同组每一段都要写同一个次数"
                            % (context, gid, "、".join(str(c) for c in counts)))
    return normalized, problems, notes


def encode_workout_snapshot(name, steps, now=None, notes_out=None):
    """构造 BackupCodec.encodeWorkoutSnapshot 的字符串：{"workout": {...}}。

    写入前先过规则闸门：字段映射 + 逐条校验。任何一条不合规都抛 RuleViolation，
    绝不把「能编码但跑不了」的课表写进桥端存储。
    """
    normalized, problems, notes = normalize_workout_steps(steps, "训练「%s」" % name)
    if notes_out is not None:
        notes_out.extend(notes)
    if problems:
        raise RuleViolation(problems)
    now = now or now_ms()
    encoded = []
    for step in normalized:
        item = {
            "order": step["order"],
            "type": step["type"],
            "repeatCount": step.get("repeatCount", 1),
        }
        if step.get("durationSec") is not None:
            item["durationSec"] = step["durationSec"]
        if step.get("distanceM") is not None:
            item["distanceM"] = step["distanceM"]
        target = step.get("target") or {"type": "OPEN", "percent": False}
        item["target"] = {
            "type": target.get("type", "OPEN"),
            "percent": bool(target.get("percent", False)),
        }
        if target.get("minValue") is not None:
            item["target"]["minValue"] = float(target["minValue"])
        if target.get("maxValue") is not None:
            item["target"]["maxValue"] = float(target["maxValue"])
        for key in ("repeatGroupId", "hrGateMaxBpm", "hrGatePercent", "cadenceSpm"):
            if step.get(key) is not None:
                item[key] = step[key]
        if step.get("label"):
            item["label"] = step["label"]
        encoded.append(item)
    workout = {
        "name": str(name), "description": "", "intensity": "", "applicableTo": "",
        "intensityLevel": "MODERATE", "createdAt": now, "updatedAt": now,
        "steps": encoded,
    }
    return json.dumps({"workout": workout}, ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------- 文本参数一致性
#
# 目标区间是 App 实时提醒的唯一数据源：目标类型为 OPEN 时，标题/描述/步骤名里
# 不能再出现心率、配速、步频标准；任何量化参数只写在文本里，App 都读不到。

HR_TEXT_KEYS = ("心率", "bpm", "hrmax", "hrr", "最大心率", "储备心率")
PACE_TEXT_KEYS = ("配速", "pace", "/km", "/公里", "每公里")
CADENCE_TEXT_KEYS = ("步频", "步/分", "步每分钟", "spm")

HR_TEXT_RANGE_RE = re.compile(
    r"(\d{2,3})\s*(?:-|~|—|–|至|到)\s*(\d{2,3})\s*(?:bpm|次/分|次每分钟)?")
HR_TEXT_PERCENT_RE = re.compile(
    r"(\d{1,3}(?:\.\d+)?)\s*(?:-|~|—|–|至|到)\s*(\d{1,3}(?:\.\d+)?)\s*%")
# 单值阈值：心率<150、心率 150、150bpm 都要能识别，不能只认区间
HR_TEXT_SINGLE_RE = re.compile(r"(?:心率|bpm|最大心率|储备心率)\D{0,6}(\d{2,3})")
HR_TEXT_BPM_RE = re.compile(r"(\d{2,3})\s*bpm", re.IGNORECASE)
PACE_TEXT_RE = re.compile(r"(\d{1,2})\s*['′:：]\s*(\d{2})\s*[\"″]?")
CADENCE_TEXT_RE = re.compile(r"(?:步频\D{0,4}(\d{3})|(\d{3})\s*(?:步/分|步每分钟|spm))")
DISTANCE_TEXT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(km|公里|m|米)")
DURATION_TEXT_RE = re.compile(r"(\d+)\s*(?:分钟|分)\s*(?:(\d+)\s*秒)?")


def scan_text_standards(text):
    """从标题/描述/步骤名里识别量化标准；只认带上下文的写法，避免把 4×4 当参数。"""
    raw = str(text or "")
    found = {"hr": [], "hr_percent": [], "pace": [], "cadence": [],
             "distance": [], "duration": []}
    if not raw.strip():
        return found
    lower = raw.lower()
    if any(key in lower for key in HR_TEXT_KEYS):
        for match in HR_TEXT_PERCENT_RE.finditer(raw):
            found["hr_percent"].append(
                (float(match.group(1)), float(match.group(2)), match.group(0)))
        for match in HR_TEXT_RANGE_RE.finditer(raw):
            found["hr"].append(
                (int(match.group(1)), int(match.group(2)), match.group(0)))
        for match in HR_TEXT_SINGLE_RE.finditer(raw):
            value = int(match.group(1))
            if not any(value in (lo, hi) for lo, hi, _ in found["hr"]):
                found["hr"].append((value, value, match.group(0)))
        for match in HR_TEXT_BPM_RE.finditer(raw):
            value = int(match.group(1))
            if not any(value in (lo, hi) for lo, hi, _ in found["hr"]):
                found["hr"].append((value, value, match.group(0)))
    if any(key in lower for key in PACE_TEXT_KEYS):
        for match in PACE_TEXT_RE.finditer(raw):
            seconds = int(match.group(1)) * 60 + int(match.group(2))
            # 配速合理区间 2'00" ~ 15'00"；避免把 4:4 之类的时间碎片当成配速
            if 120 <= seconds <= 900:
                found["pace"].append((seconds, seconds, match.group(0)))
    if any(key in lower for key in CADENCE_TEXT_KEYS):
        for match in CADENCE_TEXT_RE.finditer(raw):
            value = match.group(1) or match.group(2)
            if value:
                found["cadence"].append((int(value), int(value), match.group(0)))
    for match in DISTANCE_TEXT_RE.finditer(raw):
        value = float(match.group(1))
        if match.group(2) in ("km", "公里"):
            value *= 1000
        found["distance"].append((int(round(value)), match.group(0)))
    for match in DURATION_TEXT_RE.finditer(raw):
        seconds = int(match.group(1)) * 60 + int(match.group(2) or 0)
        found["duration"].append((seconds, match.group(0)))
    return found


def _text_value_matches(value, lo, hi):
    return min(lo, hi) - 1 <= value <= max(lo, hi) + 1


def _hr_text_matches(target, found):
    """文本心率与结构化目标是否一致；跨单位（bpm vs 百分比）无法换算，不强行比较。"""
    if target.get("type") != "HR_RANGE":
        return True
    lo = target.get("minValue")
    hi = target.get("maxValue")
    if lo is None or hi is None:
        return True
    if target.get("percent"):
        if not found["hr_percent"]:
            return True
        for text_lo, text_hi, _ in found["hr_percent"]:
            if _text_value_matches(text_lo, lo, hi) and _text_value_matches(text_hi, lo, hi):
                return True
        return False
    if not found["hr"]:
        return True
    for text_lo, text_hi, _ in found["hr"]:
        if _text_value_matches(text_lo, lo, hi) and _text_value_matches(text_hi, lo, hi):
            return True
    return False


def _pace_text_matches(target, found):
    if target.get("type") != "PACE_RANGE":
        return True
    lo = target.get("minValue")
    hi = target.get("maxValue")
    if lo is None or hi is None:
        # 亚索式空配速由马拉松成绩换算，文本里的配速就是换算结果，允许
        return True
    if not found["pace"]:
        return True
    for text_lo, text_hi, _ in found["pace"]:
        if _text_value_matches(text_lo, lo, hi) and _text_value_matches(text_hi, lo, hi):
            return True
    return False


def _repeat_times(step):
    if step.get("repeatGroupId") is None:
        return 1
    return max(1, int(step.get("repeatCount", 1) or 1))


def _total_distance(steps):
    return sum(int(step.get("distanceM") or 0) * _repeat_times(step) for step in steps)


def _total_duration(steps):
    return sum(int(step.get("durationSec") or 0) * _repeat_times(step) for step in steps)


def validate_workout_text(workout_name, workout_description, steps, context):
    """标题/描述/步骤名里的量化标准必须落到结构化字段，且不能与目标类型冲突。"""
    problems = []
    steps = steps or []
    for index, step in enumerate(steps):
        at = "%s 第 %d 段" % (context, index + 1)
        label = step.get("_raw_label") or step.get("label") or ""
        found = scan_text_standards(label)
        target = step.get("target") or {}
        ttype = target.get("type", "OPEN")
        has_hr_text = bool(found["hr"] or found["hr_percent"])

        if ttype == "OPEN" and (has_hr_text or found["pace"] or found["cadence"]):
            problems.append(
                "%s 目标类型是 OPEN（无目标），但步骤名含量化标准「%s」："
                "请删除文本里的量化描述，或改成 HR_RANGE/PACE_RANGE/cadenceSpm" % (at, label))
        if ttype == "HR_RANGE":
            if found["pace"]:
                problems.append("%s 目标是心率，但步骤名含配速标准「%s」："
                                "配速必须写入 target.type=PACE_RANGE" % (at, label))
            if not _hr_text_matches(target, found):
                problems.append("%s 步骤名的心率标准与 target 不一致：「%s」" % (at, label))
        if ttype == "PACE_RANGE":
            if has_hr_text:
                problems.append("%s 目标是配速，但步骤名含心率标准「%s」："
                                "心率必须写入 target.type=HR_RANGE" % (at, label))
            if not _pace_text_matches(target, found):
                problems.append("%s 步骤名的配速标准与 target 不一致：「%s」" % (at, label))

        if found["cadence"]:
            if step.get("cadenceSpm") is None:
                problems.append("%s 步骤名写了步频「%s」，但没有配置 cadenceSpm" % (at, label))
            elif any(value != step["cadenceSpm"] for value, _, _ in found["cadence"]):
                problems.append("%s 步骤名的步频与 cadenceSpm=%s 不一致"
                                % (at, step["cadenceSpm"]))

        if found["distance"]:
            distance = step.get("distanceM")
            text_m = found["distance"][0][0]
            if distance is None:
                problems.append("%s 步骤名写了距离「%s」，但本段按时间计量（没有 distanceM）"
                                % (at, label))
            elif abs(int(distance) - text_m) > 50:
                problems.append("%s 步骤名的距离与 distanceM=%s 不一致" % (at, distance))
        if found["duration"]:
            duration = step.get("durationSec")
            text_sec = found["duration"][0][0]
            if duration is None:
                problems.append("%s 步骤名写了时长「%s」，但本段按距离计量（没有 durationSec）"
                                % (at, label))
            elif abs(int(duration) - text_sec) > 5:
                problems.append("%s 步骤名的时长与 durationSec=%s 不一致" % (at, duration))

    title = "%s %s" % (workout_name or "", workout_description or "")
    title_found = scan_text_standards(title)
    hr_types = [step.get("target", {}) for step in steps
                if step.get("target", {}).get("type") == "HR_RANGE"]
    pace_types = [step.get("target", {}) for step in steps
                  if step.get("target", {}).get("type") == "PACE_RANGE"]
    cadences = [step.get("cadenceSpm") for step in steps
                if step.get("cadenceSpm") is not None]

    has_title_hr_bpm = bool(title_found["hr"])
    has_title_hr_pct = bool(title_found["hr_percent"])
    has_step_hr_bpm = any(not item.get("percent") for item in hr_types)
    has_step_hr_pct = any(item.get("percent") for item in hr_types)
    if has_title_hr_pct and not has_step_hr_pct:
        problems.append("%s 的标题/描述写了心率百分比，但没有任何段落配置 HR_RANGE(percent=true)"
                        % context)
    if has_title_hr_bpm and not has_step_hr_bpm:
        problems.append("%s 的标题/描述写了心率 bpm，但没有任何段落配置绝对心率 HR_RANGE"
                        % context)
    if title_found["pace"] and not pace_types:
        problems.append("%s 的标题/描述写了配速标准，但没有任何段落配置 PACE_RANGE" % context)
    if title_found["cadence"] and not cadences:
        problems.append("%s 的标题/描述写了步频标准，但没有任何段落配置 cadenceSpm" % context)
    if hr_types and has_title_hr_pct:
        if not any(_hr_text_matches(item, title_found) for item in hr_types):
            problems.append("%s 的标题/描述心率标准与所有段落的心率目标都不一致" % context)
    if pace_types and title_found["pace"]:
        if not any(_pace_text_matches(item, title_found) for item in pace_types):
            problems.append("%s 的标题/描述配速标准与所有段落的配速目标都不一致" % context)

    # 标题里的距离/时长也必须在步骤里有对应配置（或与总距离/总时长一致）
    title_distances = [value for value, _ in title_found["distance"]]
    if title_distances:
        singles = [int(step["distanceM"]) for step in steps if step.get("distanceM")]
        total = _total_distance(steps)
        for value in title_distances:
            if not any(abs(item - value) <= 50 for item in singles) and abs(total - value) > 50:
                problems.append("%s 的标题/描述写了距离 %s 米，但步骤里没有对应距离"
                                % (context, value))
    title_durations = [value for value, _ in title_found["duration"]]
    if title_durations:
        singles = [int(step["durationSec"]) for step in steps if step.get("durationSec")]
        total = _total_duration(steps)
        for value in title_durations:
            if not any(abs(item - value) <= 10 for item in singles) and abs(total - value) > 10:
                problems.append("%s 的标题/描述写了时长 %s 分钟，但步骤里没有对应时长"
                                % (context, max(1, value // 60)))
    return problems


def validate_plan_text(plan):
    """计划名称/备注里的量化标准必须在计划内找到对应的结构化目标。"""
    if not isinstance(plan, dict):
        return []
    entries = [entry for entry in (plan.get("entries") or []) if isinstance(entry, dict)]
    # 引用课表库的条目无法在这里展开步骤，跳过标题级校验，交给 App 侧课表库
    if any(not entry.get("customWorkout") and int(entry.get("workoutId") or 0) > 0
           for entry in entries):
        return []
    steps = []
    for entry in entries:
        raw = entry.get("customWorkout")
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            workout = json.loads(raw).get("workout") or {}
        except Exception:
            continue
        steps.extend(workout.get("steps") or [])
    if not steps:
        return []
    title = "%s %s" % (plan.get("name", ""), plan.get("note", ""))
    found = scan_text_standards(title)
    problems = []
    has_hr_bpm = any(step.get("target", {}).get("type") == "HR_RANGE"
                     and not step.get("target", {}).get("percent") for step in steps)
    has_hr_pct = any(step.get("target", {}).get("type") == "HR_RANGE"
                     and step.get("target", {}).get("percent") for step in steps)
    has_pace = any(step.get("target", {}).get("type") == "PACE_RANGE" for step in steps)
    has_cadence = any(step.get("cadenceSpm") is not None for step in steps)
    if found["hr"] and not has_hr_bpm:
        problems.append("计划名称/备注写了心率 bpm，但计划里没有任何段落配置绝对心率目标")
    if found["hr_percent"] and not has_hr_pct:
        problems.append("计划名称/备注写了心率百分比，但计划里没有任何段落配置百分比心率目标")
    if found["pace"] and not has_pace:
        problems.append("计划名称/备注写了配速标准，但计划里没有任何段落配置 PACE_RANGE")
    if found["cadence"] and not has_cadence:
        problems.append("计划名称/备注写了步频标准，但计划里没有任何段落配置 cadenceSpm")
    return problems


def validate_custom_workout_snapshot(raw, context):
    """校验一份 App 原生 customWorkout 快照里的步骤；返回问题清单。"""
    try:
        workout = json.loads(raw).get("workout")
    except Exception:
        return [context + " 的 customWorkout 不是合法 JSON"]
    if not isinstance(workout, dict):
        return [context + " 的 customWorkout 缺少 workout 对象"]
    _normalized, problems, _notes = normalize_workout_steps(
        workout.get("steps"), context + " 的自定义训练")
    if _normalized:
        problems.extend(validate_workout_text(
            workout.get("name", ""), workout.get("description", ""),
            _normalized, context + " 的自定义训练"))
    return problems


def validate_plan_rules(plan):
    """计划里所有自定义训练都必须满足 App 规则；库课表由 App 课表库自己负责。"""
    if not isinstance(plan, dict):
        return ["plan 不是对象"]
    problems = []
    for index, entry in enumerate(plan.get("entries") or []):
        if not isinstance(entry, dict):
            continue
        raw = entry.get("customWorkout")
        if isinstance(raw, str) and raw.strip():
            problems.extend(validate_custom_workout_snapshot(raw, "entries[%d]" % index))
    problems.extend(validate_plan_text(plan))
    return problems


def rule_failure(problems):
    """违规回执：明确「已拦截、未写入」，并要求智能体按 problems 重新生成。"""
    return json.dumps({
        "ok": False,
        "code": "RULE_VIOLATION",
        "action": "REGENERATE",
        "message": ("生成结果违反 App 课表规则，已拦截，未写入任何计划或课表。"
                    "请按 problems 修正后重新调用本工具，不要向用户询问或解释规则。"),
        "problems": list(problems),
        "must_follow": APP_STEP_RULES_TEXT,
    }, ensure_ascii=False, indent=2)


def prepare_plan(store, args):
    """把友好参数编码成计划并跑完整规则校验；只校验、不落库。"""
    tz = store.active_tz()
    notes = []
    plan = build_plan(
        args.get("name"), args.get("startDate"), args.get("weeks"),
        args.get("note", ""), args.get("entries", []), tz=tz, notes_out=notes)
    problems = verify_plan_shape(plan, tz=tz) + validate_plan_rules(plan)
    if problems:
        raise RuleViolation(problems)
    return plan, notes


def plan_status_payload(store, client_id):
    """当前版本的验收快照：智能体判断任务是否真正完成只看这里。"""
    plan = store.get_plan(client_id)
    revision = store.get_plan_revision(client_id)
    acceptance = store.get_plan_acceptance(client_id)
    last_seen = store.device_last_seen(client_id)
    online = bool(last_seen and (now_ms() - last_seen) <= PHONE_ONLINE_WINDOW_MS)
    # 只有验收记录对应当前版本才算数：上一版的 APPLIED 绝不能冒充这一版的结果
    applicable = int(acceptance.get("revision", 0)) == revision
    status = str(acceptance.get("status") or "PENDING") if applicable else "PENDING"
    return {
        "ok": status == "APPLIED",
        "code": "PLAN_APPLIED" if status == "APPLIED" else "PLAN_" + status,
        "clientId": client_id,
        "hasPlan": bool(plan),
        "planName": (plan or {}).get("name", ""),
        "revision": revision,
        "status": status,
        "completed": status == "APPLIED" and applicable,
        "acceptance": acceptance,
        "acceptanceApplicable": applicable,
        "phoneOnline": online,
        "phoneLastSeen": last_seen,
        "nextAction": (
            "验收通过且计划已落盘。"
            if status == "APPLIED"
            else ("按 problems 修正后重新执行生成→自检→提交。" if status == "REJECTED"
                  else "尚未落盘：继续调用 wait_for_plan_result 或 get_plan_status；"
                       "若手机长时间离线，明确告诉用户计划未同步。")
        ),
    }



def workout_snapshot_ok(raw):
    """
    校验 App 原生课表快照：{"workout": {"steps": [...]}}。

    空步骤课表在 App 里也是合法数据（可继续编辑），这里不能因为 steps 为空就拒收，
    否则手机上传整个课表库时会因为个别空课表导致全部课表数据同步失败。
    """
    if not isinstance(raw, str) or not raw.strip():
        return False
    try:
        workout = json.loads(raw).get("workout")
        if not isinstance(workout, dict):
            return False
        steps = workout.get("steps", [])
        return isinstance(steps, list)
    except Exception:
        return False


def entry_trainable(entry):
    """一条 entry 在 App 里能否解析出训练内容：库课表 id，或可解析的自定义步骤。"""
    if not isinstance(entry, dict):
        return False
    try:
        workout_id = int(entry.get("workoutId", 0))
    except (TypeError, ValueError):
        return False
    if workout_id > 0:
        return True
    raw = entry.get("customWorkout")
    if not isinstance(raw, str) or not raw.strip():
        return False
    try:
        workout = json.loads(raw).get("workout")
        return bool(workout and workout.get("steps"))
    except Exception:
        return False


def app_parseable_count(plan):
    """当前计划里 App 真正能训练/显示的条目数，用于回执与状态自检。"""
    if not isinstance(plan, dict):
        return 0
    return sum(1 for entry in plan.get("entries", []) if entry_trainable(entry))


def normalize_plan_for_app(plan):
    """
    把 set_plan 风格的 steps 补成 App 原生 customWorkout。

    set_plan_json 的调用方容易传入「workoutId=0 + steps」这种 set_plan 写法，
    原样落库会让 App 的 decodePlan 找不到步骤，条目显示「已删除」。
    这里在落库前补齐编码（复用 build_plan 的同一函数），返回补齐条数。
    """
    fixed = 0
    for index, entry in enumerate(plan.get("entries") or []):
        if not isinstance(entry, dict):
            continue
        where = "entries[%d]" % index
        # 有的写入方把 customWorkout 直接给成对象：先落成 App 能读的字符串，不能静默丢掉
        raw_custom = entry.get("customWorkout")
        if isinstance(raw_custom, dict):
            nested = raw_custom.get("workout")
            if isinstance(nested, dict):
                entry["customWorkout"] = json.dumps(
                    raw_custom, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(raw_custom.get("steps"), list):
                name = str(entry.get("workoutName") or plan.get("name") or "计划内训练")
                entry["customWorkout"] = encode_workout_snapshot(name, raw_custom["steps"])
                fixed += 1
        # 已有的 customWorkout 也要按 7 字规则整理，避免「接口写长名、打开编辑才截短」
        if isinstance(entry.get("customWorkout"), str):
            entry["customWorkout"] = normalize_snapshot_labels(entry["customWorkout"])
            problems = validate_custom_workout_snapshot(entry["customWorkout"], where)
            if problems:
                raise RuleViolation(problems)
        if entry_trainable(entry):
            continue
        steps = entry.get("steps")
        if not isinstance(steps, list) or not steps:
            continue
        name = str(entry.get("workoutName") or plan.get("name") or "计划内训练")
        entry["customWorkout"] = encode_workout_snapshot(name, steps)
        fixed += 1
    return fixed


def verify_plan_shape(plan, tz=None):
    """
    镜像 App 的 decodePlan 关键规则，确保生成的计划不会被引擎丢弃条目。

    App 侧对越界值是「夹紧」而不是「丢弃」（hour 被夹到 23，minute 被夹到 59），
    但夹紧后的值已经不是调用方写的那个值了 —— 与其静默改写，不如在桥端就拒掉。
    """
    problems = []
    if not isinstance(plan, dict):
        return ["plan 不是对象"]
    start_date = plan.get("startDate")
    if not isinstance(start_date, int) or isinstance(start_date, bool) or start_date <= 0:
        problems.append("startDate 非法")
    weeks = plan.get("weeks")
    if not isinstance(weeks, int) or isinstance(weeks, bool) or not (2 <= weeks <= 16):
        problems.append("weeks 越界")
    entries = plan.get("entries")
    if not isinstance(entries, list):
        return problems + ["entries 不是数组"]
    if not entries:
        problems.append("entries 为空（空计划没有可执行的训练）")
    tz = tz or local_tz()
    # 计划周期：起始周的周一 ~ 起始周 + weeks 周 - 1 天（与 App 的 startWeek/endDate 同口径）
    plan_start = plan_end = None
    if isinstance(start_date, int) and not isinstance(start_date, bool) and start_date > 0:
        plan_start = monday_of(ms_to_date(start_date, tz))
        if isinstance(weeks, int) and not isinstance(weeks, bool) and weeks > 0:
            plan_end = plan_start + datetime.timedelta(weeks=weeks) - datetime.timedelta(days=1)
    ids = set()
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            problems.append("entries[%d] 不是对象" % i)
            continue
        date = e.get("date")
        if not isinstance(date, int) or isinstance(date, bool) or date <= 0:
            problems.append("entries[%d].date 非法（会被 App 丢弃）" % i)
        else:
            hour = e.get("hour", 19)
            minute = e.get("minute", 0)
            if not isinstance(hour, int) or isinstance(hour, bool) or not (0 <= hour <= 23):
                problems.append("entries[%d].hour 越界（0..23）" % i)
            if not isinstance(minute, int) or isinstance(minute, bool) or not (0 <= minute <= 59):
                problems.append("entries[%d].minute 越界（0..59）" % i)
            if plan_start and plan_end:
                day = ms_to_date(date, tz)
                if day < plan_start or day > plan_end:
                    problems.append("entries[%d].date(%s) 不在计划周期(%s ~ %s)内"
                                    % (i, day, plan_start, plan_end))
        eid = e.get("id")
        if not isinstance(eid, int) or isinstance(eid, bool) or eid <= 0 or eid in ids:
            problems.append("entries[%d].id 非法或重复（会被 App 改写）" % i)
        ids.add(eid)
        # 关键必填校验：workoutId<=0 又没有可解析的 customWorkout 时，
        # App 的 decodePlan 解析不到任何训练内容，条目会显示「已删除」。
        if not entry_trainable(e):
            cw = e.get("customWorkout")
            if isinstance(cw, str) and cw.strip():
                problems.append("entries[%d].customWorkout 无法解析出步骤（App 会判为已删除）" % i)
            else:
                problems.append(
                    "entries[%d].workoutId=0 且无有效 customWorkout（App 会判为已删除）" % i)
    return problems


# ---------------------------------------------------------------- MCP 工具

def plan_args_schema():
    """validate_plan 的友好参数 schema；字段与 set_plan 完全一致，避免自检与提交两套口径。"""
    return {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "计划名"},
            "startDate": {"type": "string", "description": "起始周的周一，yyyy-MM-dd"},
            "weeks": {"type": "integer", "description": "周数 2..16"},
            "note": {"type": "string", "description": "备注"},
            "clientId": {"type": "string", "description": "写入哪台设备的计划；省略则写当前活跃设备"},
            "entries": {
                "type": "array",
                "description": (
                    "逐条训练。每条必须二选一：给 steps（推荐，计划内自定义训练）"
                    "或给 list_workouts 返回的真实 workoutId；只写 workoutName 不算训练内容。"
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "date": {"type": "string", "description": "yyyy-MM-dd"},
                        "hour": {"type": "integer", "default": 19},
                        "minute": {"type": "integer", "default": 0},
                        "workoutName": {
                            "type": "string",
                            "description": "训练显示名；只写名字不会生成训练内容，也不要在这里写配速/心率/步频数值",
                        },
                        "workoutId": {
                            "type": "integer",
                            "default": 0,
                            "description": "课表库里已存在的课表 id（先用 list_workouts 查）；0 表示使用 steps",
                        },
                        "steps": {
                            "type": "array",
                            "description": (
                                "计划内自定义训练的步骤。每段 durationSec 与 distanceM 二选一；"
                                "快跑与慢跑必须统一计量方式；热身只能按时间且不带目标。"
                            ),
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": list(STEP_TYPES),
                                        "description": (
                                            "WARMUP=热身（只能按时间、不能带 target）；"
                                            "WORK=快跑；RECOVERY=慢跑；COOLDOWN=缓和"
                                        ),
                                    },
                                    "durationSec": {
                                        "type": "integer",
                                        "description": "按时间计量：秒，1..10859；与 distanceM 二选一",
                                    },
                                    "distanceM": {
                                        "type": "integer",
                                        "description": "按距离计量：米，100..21900；与 durationSec 二选一",
                                    },
                                    "label": {
                                        "type": "string",
                                        "description": "步骤显示名，最多 7 个字符；不要在这里写配速/心率/步频数值",
                                    },
                                    "repeatCount": {
                                        "type": "integer",
                                        "default": 1,
                                        "description": (
                                            "循环次数 1..99。放在循环组里时，同组每一段都写同一个值"
                                        ),
                                    },
                                    "repeatGroupId": {
                                        "type": "integer",
                                        "description": (
                                            "循环组 id：同组步骤必须相邻；单个步骤重复也要建组"
                                        ),
                                    },
                                    "hrGateMaxBpm": {
                                        "type": "integer",
                                        "description": "仅慢跑：恢复段心率门槛 40..220 bpm，与 hrGatePercent 二选一",
                                    },
                                    "hrGatePercent": {
                                        "type": "integer",
                                        "description": "仅慢跑：恢复段心率门槛 1..100 %，与 hrGateMaxBpm 二选一",
                                    },
                                    "cadenceSpm": {
                                        "type": "integer",
                                        "description": "仅快跑/慢跑：本段节拍器步频 120..220",
                                    },
                                    "target": {
                                        "type": "object",
                                        "description": (
                                            "目标区间。HR_RANGE 必须同时给 minValue/maxValue "
                                            "并显式写 percent；PACE_RANGE 单位秒/公里"
                                            "（也接受 \"4:30\"），两个值要么都给要么都不给；"
                                            "OPEN 不能带 minValue/maxValue。"
                                        ),
                                        "properties": {
                                            "type": {
                                                "type": "string",
                                                "enum": list(AGENT_TARGET_TYPES),
                                            },
                                            "minValue": {"type": "number"},
                                            "maxValue": {"type": "number"},
                                            "percent": {
                                                "type": "boolean",
                                                "description": (
                                                    "仅 HR_RANGE 必填：true=强度百分比(1..150)，"
                                                    "false=绝对心率(40..220 bpm)"
                                                ),
                                            },
                                        },
                                        "required": ["type"],
                                    },
                                },
                                "required": ["type"],
                            },
                        },
                    },
                    "required": ["date"],
                },
            },
        },
        "required": ["name", "startDate", "weeks", "entries"],
    }


def tool_defs():
    return [
        {
            "name": "list_sessions",
            "description": (
                "读取手机上传的训练历史。返回条数统计与记录数组"
                "（id/训练名/开始时间/时长/距离/配速/心率/卡路里/设备标识）。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "最多返回多少条，默认 50，上限 500", "default": 50},
                    "since": {"type": "string", "description": "只要这个日期（含）之后的记录，格式 yyyy-MM-dd"},
                    "order": {"type": "string", "enum": ["desc", "asc"], "description": "按开始时间排序，默认 desc"},
                    "clientId": {"type": "string", "description": "只看某台设备；省略则看当前活跃设备"},
                },
            },
        },
        {
            "name": "get_status",
            "description": (
                "查看桥接状态：设备名、已连接设备数、权限、训练记录数、上次同步时间、"
                "网络服务是否在运行、当前活跃设备与各设备的计划版本。"
            ),
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "validate_plan",
            "description": (
                "计划自检：按 App 课表规则对提交前的计划做全字段校验，不写入任何数据。"
                "problems 非空时必须修正并重新自检，禁止进入提交流程。\n"
                + APP_STEP_RULES_TEXT
            ),
            "inputSchema": plan_args_schema(),
        },
        {
            "name": "set_plan",
            "description": (
                "提交训练计划（写入前按 App 课表规则强校验，违规一律拒绝并需要重新生成）。\n"
                "提交成功只代表桥端已接收，**任务尚未完成**：必须调用 wait_for_plan_result "
                "等待手机验收，只有 APPLIED 才能判定完成。\n"
                + APP_STEP_RULES_TEXT +
                "\n示例（4×4 间歇，全部按时间计量）：\n"
                "entries: [{\"date\":\"2026-11-24\",\"workoutName\":\"挪威 4×4\",\"steps\":["
                "{\"type\":\"WARMUP\",\"durationSec\":600},"
                "{\"type\":\"WORK\",\"durationSec\":240,\"repeatGroupId\":1,\"repeatCount\":4,"
                "\"target\":{\"type\":\"HR_RANGE\",\"percent\":true,\"minValue\":78,\"maxValue\":93}},"
                "{\"type\":\"RECOVERY\",\"durationSec\":180,\"repeatGroupId\":1,\"repeatCount\":4,"
                "\"hrGatePercent\":70},"
                "{\"type\":\"COOLDOWN\",\"durationSec\":300}]}]\n"
                "每条 entry 用 date(yyyy-MM-dd) 指定训练日；weeks 必须是 2..16。"
                "若已有计划，会先留档旧计划再写入新计划，绝不直接覆盖；"
                "手机读写档打开时会自动同步并同样先归档旧计划。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "计划名"},
                    "startDate": {"type": "string", "description": "起始周的周一，yyyy-MM-dd"},
                    "weeks": {"type": "integer", "description": "周数 2..16"},
                    "note": {"type": "string", "description": "备注"},
                    "clientId": {"type": "string", "description": "写入哪台设备的计划；省略则写当前活跃设备"},
                    "entries": {
                        "type": "array",
                        "description": (
                            "逐条训练。每条必须二选一：给 steps（推荐，计划内自定义训练）"
                            "或给 list_workouts 返回的真实 workoutId；只写 workoutName 不算训练内容。"
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "date": {"type": "string", "description": "yyyy-MM-dd"},
                                "hour": {"type": "integer", "default": 19},
                                "minute": {"type": "integer", "default": 0},
                                "workoutName": {
                                    "type": "string",
                                    "description": "训练显示名；只写名字不会生成训练内容，也不要在这里写配速/心率/步频数值",
                                },
                                "workoutId": {
                                    "type": "integer",
                                    "default": 0,
                                    "description": "课表库里已存在的课表 id（先用 list_workouts 查）；0 表示使用 steps",
                                },
                                "steps": {
                                    "type": "array",
                                    "description": (
                                        "计划内自定义训练的步骤。每段 durationSec 与 distanceM 二选一；"
                                        "快跑与慢跑必须统一计量方式；热身只能按时间且不带目标。"
                                    ),
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "type": {
                                                "type": "string",
                                                "enum": list(STEP_TYPES),
                                                "description": (
                                                    "WARMUP=热身（只能按时间、不能带 target）；"
                                                    "WORK=快跑；RECOVERY=慢跑；COOLDOWN=缓和"
                                                ),
                                            },
                                            "durationSec": {
                                                "type": "integer",
                                                "description": "按时间计量：秒，1..10859；与 distanceM 二选一",
                                            },
                                            "distanceM": {
                                                "type": "integer",
                                                "description": "按距离计量：米，100..21900；与 durationSec 二选一",
                                            },
                                            "label": {
                                                "type": "string",
                                                "description": "步骤显示名，最多 7 个字符；不要在这里写配速/心率/步频数值",
                                            },
                                            "repeatCount": {
                                                "type": "integer",
                                                "default": 1,
                                                "description": (
                                                    "循环次数 1..99。放在循环组里时，同组每一段都写同一个值"
                                                ),
                                            },
                                            "repeatGroupId": {
                                                "type": "integer",
                                                "description": (
                                                    "循环组 id：同组步骤必须相邻；单个步骤重复也要建组"
                                                ),
                                            },
                                            "hrGateMaxBpm": {
                                                "type": "integer",
                                                "description": "仅慢跑：恢复段心率门槛 40..220 bpm，与 hrGatePercent 二选一",
                                            },
                                            "hrGatePercent": {
                                                "type": "integer",
                                                "description": "仅慢跑：恢复段心率门槛 1..100 %，与 hrGateMaxBpm 二选一",
                                            },
                                            "cadenceSpm": {
                                                "type": "integer",
                                                "description": "仅快跑/慢跑：本段节拍器步频 120..220",
                                            },
                                            "target": {
                                                "type": "object",
                                                "description": (
                                                    "目标区间。HR_RANGE 必须同时给 minValue/maxValue "
                                                    "并显式写 percent；PACE_RANGE 单位秒/公里"
                                                    "（也接受 \"4:30\"），两个值要么都给要么都不给；"
                                                    "OPEN 不能带 minValue/maxValue。"
                                                ),
                                                "properties": {
                                                    "type": {
                                                        "type": "string",
                                                        "enum": list(AGENT_TARGET_TYPES),
                                                    },
                                                    "minValue": {"type": "number"},
                                                    "maxValue": {"type": "number"},
                                                    "percent": {
                                                        "type": "boolean",
                                                        "description": (
                                                            "仅 HR_RANGE 必填：true=强度百分比(1..150)，"
                                                            "false=绝对心率(40..220 bpm)"
                                                        ),
                                                    },
                                                },
                                                "required": ["type"],
                                            },
                                        },
                                        "required": ["type"],
                                    },
                                },
                            },
                            "required": ["date"],
                        },
                    },
                },
                "required": ["name", "startDate", "weeks", "entries"],
            },
        },
        {
            "name": "get_plan",
            "description": "读取当前已写入的计划（App 原生 JSON）。用于核对或修改前查看。注意：读到计划不代表手机已验收，必须用 get_plan_status 看验收状态。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "clientId": {"type": "string", "description": "读哪台设备的计划；省略则读当前活跃设备"},
                },
            },
        },
        {
            "name": "get_plan_status",
            "description": (
                "查询当前版本的手机验收状态（不阻塞）。返回 status：PENDING/APPLIED/REJECTED/"
                "PHONE_UPLOADED；REJECTED 会带手机给出的 problems。"
                "任何 PENDING 以外的结论都只能由这里或 wait_for_plan_result 得出。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "clientId": {"type": "string", "description": "查哪台设备；省略则用当前活跃设备"},
                },
            },
        },
        {
            "name": "wait_for_plan_result",
            "description": (
                "等待手机对当前提交版本的验收结果（长轮询，默认最多 45 秒）。"
                "只有返回 status=APPLIED 才能向用户宣布任务完成；"
                "REJECTED 按 problems 修正后重新执行生成→自检→提交；"
                "TIMEOUT/PENDING/NO_DEVICE/PHONE_UPLOADED 都表示计划尚未落盘，"
                "不得默认成功，应继续用 get_plan_status 查询或提示用户检查手机。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "clientId": {"type": "string", "description": "等哪台设备；省略则用当前活跃设备"},
                    "timeoutSec": {
                        "type": "integer",
                        "default": 45,
                        "description": "最长等待秒数，1..120；超时不代表成功",
                    },
                },
            },
        },
        {
            "name": "set_plan_json",
            "description": (
                "直接写入 App 原生计划 JSON（当 set_plan 的友好参数不够用时使用）。"
                "字段：name/startDate(epoch毫秒)/weeks/note/entries；entry 的 customWorkout "
                "必须是 {\"workout\":{\"steps\":[...]}} 字符串或同结构对象。"
                "写入前与 set_plan 走同一套 App 课表规则校验（含 steps 与 customWorkout 里的每一段），"
                "违规一律拒绝并需要重新生成。已有计划会先留档再写入。\n"
                + APP_STEP_RULES_TEXT
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "plan": {"type": "object", "description": "完整的计划对象"},
                    "clientId": {"type": "string", "description": "写入哪台设备；省略则写当前活跃设备"},
                },
                "required": ["plan"],
            },
        },
        {
            "name": "list_workouts",
            "description": "读取手机上传的课表数据（课表数据读取权限；名称与完整步骤快照）。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "clientId": {"type": "string", "description": "读哪台设备；省略则读当前活跃设备"},
                },
            },
        },
        {
            "name": "set_workouts",
            "description": (
                "写入课表数据（课表数据写入权限，需手机授权读写档）。每条给 id 与 "
                "snapshot（App 原生 {\"workout\":{\"steps\":[...]}} JSON 字符串）；"
                "也可用 name + steps 由工具编码。写入范围仅限「我的课表」："
                "带 isPreset/presetKey 或命中本机内置预设的条目只会另存副本，绝不覆盖预设。"
                "写入前按 App 课表规则强校验，违规一律拒绝并需要重新生成。"
                "手机联网且权限为读写时会自动同步落库。\n"
                + APP_STEP_RULES_TEXT
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "clientId": {"type": "string", "description": "写入哪台设备；省略则写当前活跃设备"},
                    "workouts": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "name": {"type": "string"},
                                "snapshot": {"type": "string"},
                                "steps": {
                                    "type": "array",
                                    "description": "与 set_plan 的 steps 同一套字段与规则",
                                    "items": {"type": "object"},
                                },
                                "isPreset": {"type": "boolean", "description": "来源标记：true 表示内置预设，手机只会另存副本，不会覆盖原课表"},
                                "presetKey": {"type": "string"},
                            },
                        },
                    },
                },
                "required": ["workouts"],
            },
        },
        {
            "name": "delete_workout",
            "description": (
                "删除手机课表库里的一套自定义课表（内置预设不可删除）。"
                "手机联网且权限为读写时会自动同步，本机对应课表也会被删掉；训练历史不受影响。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "description": "要删除的课表 id（先 list_workouts 查）"},
                    "clientId": {"type": "string", "description": "操作哪台设备；省略则用当前活跃设备"},
                },
                "required": ["id"],
            },
        },
        {
            "name": "delete_workouts",
            "description": (
                "批量、原子删除自定义课表（内置预设不可删除）。"
                "建议带上 expectedRevision 与 expectedCustomCount：版本或数量不符时整批拒绝，"
                "不会出现删一半。重复提交同 id 幂等。手机联网时自动同步删除。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "ids": {"type": "array", "items": {"type": "integer"},
                            "description": "要删除的课表 id 列表"},
                    "expectedRevision": {"type": "integer",
                                         "description": "可选：期望的 revision，防止期间被其他写入改动"},
                    "expectedCustomCount": {"type": "integer",
                                            "description": "可选：期望的当前自定义课表数量"},
                    "clientId": {"type": "string", "description": "操作哪台设备；省略则用当前活跃设备"},
                },
                "required": ["ids"],
            },
        },
        {
            "name": "keep_only_custom_workouts",
            "description": (
                "把自定义课表原子清理到指定保留列表：只保留 keepIds，其余全部删除并写墓碑。"
                "带 expectedRevision / expectedCustomCount 做前置校验；任一不符整批拒绝。"
                "这是「21 条保留 10 条」的推荐做法，一次事务完成。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keepIds": {"type": "array", "items": {"type": "integer"},
                                "description": "要保留的自定义课表 id 列表"},
                    "expectedRevision": {"type": "integer", "description": "期望的当前 revision"},
                    "expectedCustomCount": {"type": "integer", "description": "期望的当前自定义课表数量"},
                    "clientId": {"type": "string", "description": "操作哪台设备；省略则用当前活跃设备"},
                },
                "required": ["keepIds"],
            },
        },
    ]


def _target_client(store, args):
    """
    智能体侧的目标设备：显式指定优先，否则用当前活跃设备。

    没有任何设备连接时必须报错，不能退回 `default` 空槽——
    否则智能体会把计划写到一个手机永远不读的地方，却仍然回执「成功」。
    """
    wanted = (args.get("clientId") or "").strip()
    if wanted:
        return wanted
    active = store.active_client_id()
    if not active:
        raise ToolError("当前没有设备连接：请先打开手机的数据共享接口，"
                        "或显式传 clientId")
    return active


def call_tool(store, name, args, net=None):
    args = args or {}
    try:
        if name == "list_sessions":
            limit = max(1, min(500, int(args.get("limit", 50))))
            tz = store.active_tz()
            since_ms = date_ms(args["since"], tz) if args.get("since") else None
            explicit = (args.get("clientId") or "").strip()
            wanted = explicit or store.active_client_id() or None
            items = store.sessions_sorted(limit, since_ms, args.get("order", "desc"), wanted)
            # 回执里写清「这次读的是哪台设备」并附上设备清单：
            # 省略 clientId 时桥端只会给当前活跃设备，不写出来就容易被当成「所有手机合并后的数据」
            payload = {
                "count": len(items),
                "clientId": wanted or "",
                "clientIdExplicit": bool(explicit),
                "devices": store.device_summary(),
                "sessions": items,
            }
            text = json.dumps(payload, ensure_ascii=False, indent=2)
            return text, False

        if name == "get_status":
            snap = store.snapshot()
            pairs = snap["pairs"]
            perms = {}
            for p in pairs.values():
                perm = p.get("permission", "?")
                perms[perm] = perms.get(perm, 0) + 1
            last = snap.get("lastSyncAt") or 0
            last_text = (datetime.datetime.fromtimestamp(last / 1000).strftime("%Y-%m-%d %H:%M:%S")
                         if last else "尚无同步")
            active = snap.get("lastActiveClientId") or ""
            clients = {}
            for p in pairs.values():
                # 旧版客户端不上报设备标识，这些令牌只用于鉴权；
                # 把它们算成一台叫 default 的设备，只会让「到底几台手机」更难看懂。
                cid = p.get("clientId")
                if not cid:
                    continue
                clients[cid] = {
                    "permission": p.get("permission"),
                    "lastSeen": p.get("lastSeen"),
                    "sessions": sum(1 for s in snap["sessions"].values()
                                    if (s.get("clientId") or DEFAULT_CLIENT) == cid),
                }
            plans = {}
            for cid, slot in snap.get("plans", {}).items():
                plans[cid] = {
                    "name": (slot.get("plan") or {}).get("name", ""),
                    "revision": int(slot.get("revision", 0) or 0),
                    "phoneRevision": int(slot.get("phoneRevision", 0) or 0),
                    "source": slot.get("source"),
                    "writtenAt": slot.get("writtenAt", 0),
                    "entries": len((slot.get("plan") or {}).get("entries", []) or []),
                    "parseableEntries": app_parseable_count(slot.get("plan")),
                    "archives": len(slot.get("archives", []) or []),
                    # 智能体判断任务是否完成只看这一项：PENDING/REJECTED 都不算落盘
                    "acceptance": (slot.get("acceptance") or {}).get("status", "PENDING"),
                    "acceptanceRevision": int(
                        (slot.get("acceptance") or {}).get("revision", 0) or 0),
                }
            status = {
                "device": snap["device"],
                "networkRunning": bool(net and net.running),
                "pairedClients": len(pairs),
                "permissions": perms,
                "sessionCount": len(snap["sessions"]),
                "lastSync": last_text,
                "lastSyncCount": snap.get("lastSyncCount", 0),
                "activeClientId": active,
                "clients": clients,
                "plans": plans,
                "activePlanName": (snap.get("plans", {}).get(active) or {}).get("plan", {}).get("name", "")
                if (snap.get("plans", {}).get(active) or {}).get("plan") else "",
                "activeAcceptance": (snap.get("plans", {}).get(active) or {}).get("acceptance")
                or {"status": "PENDING"},
                "hasPlan": bool((snap.get("plans", {}).get(active) or {}).get("plan")),
                "parseableEntries": app_parseable_count(
                    (snap.get("plans", {}).get(active) or {}).get("plan")),
                "planArchives": len((snap.get("plans", {}).get(active) or {}).get("archives", []) or []),
                "clientTz": snap.get("clientTz"),
                "storePath": store.path,
            }
            return json.dumps(status, ensure_ascii=False, indent=2), False

        if name == "validate_plan":
            # 自检只校验，不写库；失败是「检查结果」，不是工具故障，交给智能体修正
            try:
                plan, notes = prepare_plan(store, args)
            except RuleViolation as exc:
                return json.dumps({
                    "ok": False,
                    "code": "SELF_CHECK_FAILED",
                    "action": "FIX_AND_RECHECK",
                    "message": "自检未通过，禁止提交。请按 problems 修正后重新调用 validate_plan。",
                    "problems": exc.problems,
                    "must_follow": APP_STEP_RULES_TEXT,
                }, ensure_ascii=False, indent=2), False
            except (TypeError, ValueError) as exc:
                return json.dumps({
                    "ok": False,
                    "code": "SELF_CHECK_FAILED",
                    "action": "FIX_AND_RECHECK",
                    "message": "自检未通过，禁止提交。",
                    "problems": [str(exc)],
                }, ensure_ascii=False, indent=2), False
            return json.dumps({
                "ok": True,
                "code": "SELF_CHECK_PASSED",
                "action": "SUBMIT",
                "message": "自检通过，可以调用 set_plan 提交。提交后仍需等待手机验收。",
                "name": plan["name"],
                "weeks": plan["weeks"],
                "entries": len(plan["entries"]),
                "parseableEntries": app_parseable_count(plan),
                "mapping": notes,
            }, ensure_ascii=False, indent=2), False

        if name == "set_plan":
            try:
                plan, notes = prepare_plan(store, args)
            except RuleViolation as exc:
                return rule_failure(exc.problems), True
            except (TypeError, ValueError) as exc:
                return rule_failure([str(exc)]), True
            target = _target_client(store, args)
            archived = store.set_plan(target, plan)
            revision = store.get_plan_revision(target)
            last_seen = store.device_last_seen(target)
            phone_online = bool(
                last_seen and (now_ms() - last_seen) <= PHONE_ONLINE_WINDOW_MS)
            return json.dumps({
                "ok": True,
                "code": "PLAN_SUBMITTED",
                "action": "WAIT_FOR_APP_ACCEPTANCE",
                "status": "PENDING_APP_ACCEPTANCE",
                "completed": False,
                "clientId": target,
                "revision": revision,
                "name": plan["name"],
                "weeks": plan["weeks"],
                "entries": len(plan["entries"]),
                "parseableEntries": app_parseable_count(plan),
                "archivedPlan": archived,
                "mapping": notes,
                "phoneSeen": last_seen > 0,
                "phoneOnline": phone_online,
                "message": ("计划已提交到桥端，但任务尚未完成：手机还没有验收。"
                            "不要向用户宣布成功。"),
                "nextAction": (
                    "调用 wait_for_plan_result 等待手机返回 APPLIED/REJECTED；"
                    "超时用 get_plan_status 继续查，未拿到 APPLIED 前不得判定完成。"
                    if phone_online else
                    "手机当前不在线：先让用户打开 App 的数据共享接口，"
                    "再调用 wait_for_plan_result；不要判定完成。"
                ),
            }, ensure_ascii=False, indent=2), False

        if name == "get_plan":
            target = _target_client(store, args)
            plan = store.get_plan(target)
            if not plan:
                return ("当前设备（%s）没有计划。用 set_plan 或 set_plan_json 写入。"
                        % target), False
            return json.dumps(plan, ensure_ascii=False, indent=2), False

        if name == "get_plan_status":
            target = _target_client(store, args)
            payload = plan_status_payload(store, target)
            return json.dumps(payload, ensure_ascii=False, indent=2), False

        if name == "wait_for_plan_result":
            target = _target_client(store, args)
            try:
                timeout = int(args.get("timeoutSec", 45))
            except (TypeError, ValueError):
                timeout = 45
            timeout = max(1, min(120, timeout))
            if not store.get_plan(target):
                payload = plan_status_payload(store, target)
                payload.update({
                    "ok": False,
                    "code": "NO_PLAN",
                    "status": "NO_PLAN",
                    "completed": False,
                    "message": "还没有提交计划，无法等待验收。",
                    "nextAction": "先调用 validate_plan 自检，再用 set_plan 提交。",
                })
                return json.dumps(payload, ensure_ascii=False, indent=2), False
            if store.device_last_seen(target) == 0:
                payload = plan_status_payload(store, target)
                payload.update({
                    "ok": False,
                    "code": "NO_DEVICE",
                    "status": "NO_DEVICE",
                    "completed": False,
                    "message": "这台设备从未连接过桥端，计划没有落盘。",
                    "nextAction": "让用户先在 App 里打开数据共享接口并连接本机，再重新查询。",
                })
                return json.dumps(payload, ensure_ascii=False, indent=2), False
            deadline = time.time() + timeout
            while True:
                payload = plan_status_payload(store, target)
                status = payload.get("status")
                # APPLIED / REJECTED / PHONE_UPLOADED 都是手机的最终表态，立即回传
                if status in ("APPLIED", "REJECTED", "PHONE_UPLOADED"):
                    payload["waitedSec"] = max(0, timeout - int(max(0, deadline - time.time())))
                    return json.dumps(payload, ensure_ascii=False, indent=2), False
                if time.time() >= deadline:
                    payload.update({
                        "ok": False,
                        "code": "PLAN_PENDING",
                        "completed": False,
                        "waitedSec": timeout,
                        "message": "等待超时，手机还没有回传验收结果，计划未确认落盘。",
                        "nextAction": ("不要判定完成。调用 get_plan_status 继续查询；"
                                       "若 phoneOnline=false，提示用户检查手机与本机连接。"),
                    })
                    return json.dumps(payload, ensure_ascii=False, indent=2), False
                time.sleep(1.0)

        if name == "set_plan_json":
            plan = args.get("plan")
            fixed = 0
            if isinstance(plan, dict):
                try:
                    # 兼容 set_plan 风格：有 steps 但没有 customWorkout 的 entry 自动补编码
                    fixed = normalize_plan_for_app(plan)
                except RuleViolation as exc:
                    return rule_failure(exc.problems), True
                except (TypeError, ValueError) as exc:
                    return rule_failure([str(exc)]), True
            problems = verify_plan_shape(plan, tz=store.active_tz()) + validate_plan_rules(plan)
            if problems:
                return rule_failure(problems), True
            target = _target_client(store, args)
            archived = store.set_plan(target, plan)
            revision = store.get_plan_revision(target)
            last_seen = store.device_last_seen(target)
            phone_online = bool(
                last_seen and (now_ms() - last_seen) <= PHONE_ONLINE_WINDOW_MS)
            return json.dumps({
                "ok": True,
                "code": "PLAN_SUBMITTED",
                "action": "WAIT_FOR_APP_ACCEPTANCE",
                "status": "PENDING_APP_ACCEPTANCE",
                "completed": False,
                "clientId": target,
                "revision": revision,
                "name": plan.get("name", ""),
                "entries": len(plan.get("entries", [])),
                "parseableEntries": app_parseable_count(plan),
                "archivedPlan": archived,
                "autoEncodedEntries": fixed,
                "phoneSeen": last_seen > 0,
                "phoneOnline": phone_online,
                "message": ("原生计划已提交到桥端，但任务尚未完成：手机还没有验收。"
                            "不要向用户宣布成功。"),
                "nextAction": (
                    "调用 wait_for_plan_result 等待手机返回 APPLIED/REJECTED；"
                    "超时用 get_plan_status 继续查。"
                    if phone_online else
                    "手机当前不在线：先让用户打开 App 的数据共享接口，再等待验收。"
                ),
            }, ensure_ascii=False, indent=2), False

        if name == "list_workouts":
            target = _target_client(store, args)
            revision, workouts = store.get_workouts(target)
            custom = [w for w in workouts if not w.get("isPreset")]
            presets = [w for w in workouts if w.get("isPreset")]
            text = json.dumps({
                "clientId": target,
                "revision": revision,
                "customCount": len(custom),
                "presetCount": len(presets),
                "custom": custom,
                "presets": presets,
                "deletedIds": store.get_workout_deletions(target),
            }, ensure_ascii=False, indent=2)
            return text, False

        if name == "set_workouts":
            target = _target_client(store, args)
            raw = args.get("workouts")
            if not isinstance(raw, list) or not raw:
                return "workouts 必须是非空数组", True
            _, existing_workouts = store.get_workouts(target)
            next_id = max([1_000_000] + [
                int(w.get("id")) for w in existing_workouts
                if isinstance(w.get("id"), int)
            ]) + 1
            prepared = []
            for index, item in enumerate(raw):
                if not isinstance(item, dict):
                    return "workouts[%d] 不是对象" % index, True
                if item.get("isPreset") or item.get("presetKey"):
                    return "workouts[%d] 来自内置预设，预设只读、不可写入" % index, True
                snapshot = item.get("snapshot")
                if not snapshot and item.get("steps"):
                    try:
                        snapshot = encode_workout_snapshot(
                            item.get("name") or "智能体课表", item["steps"])
                    except RuleViolation as exc:
                        return rule_failure(exc.problems), True
                    except (TypeError, ValueError) as exc:
                        return rule_failure(
                            ["workouts[%d] 步骤编码失败：%s" % (index, exc)]), True
                if not workout_snapshot_ok(snapshot):
                    return ("workouts[%d] 缺少可解析的 snapshot（需 "
                            "{\"workout\":{\"steps\":[...]}}）" % index), True
                # 直接给 snapshot 也不能绕过规则：解码后按同一套 App 规则逐段校验
                snapshot_problems = validate_custom_workout_snapshot(
                    snapshot, "workouts[%d]" % index)
                if snapshot_problems:
                    return rule_failure(snapshot_problems), True
                wid = item.get("id")
                if wid in (None, 0):
                    # 新建时允许不传 id：桥端分配一个不会与手机本地 id 冲突的正整数
                    wid = next_id
                    next_id += 1
                if isinstance(wid, bool) or not isinstance(wid, int) or wid <= 0:
                    return "workouts[%d].id 必须是正整数" % index, True
                prepared.append({
                    "id": wid,
                    "name": str(item.get("name") or ""),
                    "isPreset": bool(item.get("isPreset", False)),
                    "presetKey": item.get("presetKey"),
                    "snapshot": snapshot,
                })
            result = store.set_workouts(target, prepared, bump_revision=True, source="agent")
            return ("已写入 %d 套课表数据到设备 %s，版本 %d，当前自定义课表 %d 套；"
                    "手机连接且权限为读写时会自动同步。"
                    % (len(prepared), target, result["revision"], result["customCount"])), False

        if name in ("delete_workout", "delete_workouts"):
            target = _target_client(store, args)
            if name == "delete_workout":
                ids = [args.get("id")]
            else:
                ids = args.get("ids")
            if not isinstance(ids, list) or not ids:
                return "ids 必须是非空数组（先用 list_workouts 查）", True
            for index, wid in enumerate(ids):
                if isinstance(wid, bool) or not isinstance(wid, int) or wid <= 0:
                    return "ids[%d] 必须是正整数" % index, True
            try:
                result = store.delete_workouts(
                    target, ids,
                    expected_revision=args.get("expectedRevision"),
                    expected_custom_count=args.get("expectedCustomCount"),
                )
            except WorkoutsConflict as conflict:
                return "删除被拒绝（未做任何修改）：%s；当前状态 %s" % (
                    conflict.msg, json.dumps(conflict.detail, ensure_ascii=False)), True
            return json.dumps({
                "ok": True,
                "clientId": target,
                "beforeCustomCount": result["beforeCustomCount"],
                "afterCustomCount": result["afterCustomCount"],
                "deletedIds": result["deletedIds"],
                "alreadyDeletedIds": result["alreadyDeletedIds"],
                "notFoundIds": result["notFoundIds"],
                "presetIds": result["presetIds"],
                "revision": result["revision"],
                "message": "删除完成；手机联网时会自动同步，训练历史不受影响。",
            }, ensure_ascii=False, indent=2), False

        if name == "keep_only_custom_workouts":
            target = _target_client(store, args)
            keep_ids = args.get("keepIds")
            if not isinstance(keep_ids, list):
                return "keepIds 必须是数组（先用 list_workouts 查）", True
            for index, wid in enumerate(keep_ids):
                if isinstance(wid, bool) or not isinstance(wid, int) or wid <= 0:
                    return "keepIds[%d] 必须是正整数" % index, True
            try:
                result = store.keep_only_custom_workouts(
                    target, keep_ids,
                    expected_revision=args.get("expectedRevision"),
                    expected_custom_count=args.get("expectedCustomCount"),
                )
            except WorkoutsConflict as conflict:
                return "清理被拒绝（未做任何修改）：%s；当前状态 %s" % (
                    conflict.msg, json.dumps(conflict.detail, ensure_ascii=False)), True
            return json.dumps({
                "ok": True,
                "clientId": target,
                "beforeCustomCount": result["beforeCustomCount"],
                "afterCustomCount": result["afterCustomCount"],
                "deletedIds": result["deletedIds"],
                "revision": result["revision"],
                "message": "已按保留列表清理，手机联网时自动同步。",
            }, ensure_ascii=False, indent=2), False

        return "未知工具：" + name, True
    except ToolError as exc:
        return str(exc), True
    except Exception as exc:
        return "工具执行出错：%s" % exc, True


# ---------------------------------------------------------------- MCP stdio

def mcp_main(store):
    net = NetServer(store)
    try:
        net.start()
    except Exception as exc:
        log("网络服务启动失败（MCP 仍可用）：%s" % exc)

    def ok(msg_id, result):
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def err(msg_id, code, message):
        return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}

    try:
        sys.stdin.reconfigure(encoding="utf-8")
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    log("MCP stdio 已就绪")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        method = msg.get("method")
        msg_id = msg.get("id")

        if method == "initialize":
            resp = ok(msg_id, {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "intervalrunning-bridge", "version": "1.0.0"},
                # 规则必须随握手一次性给到模型：只写在工具描述里时，模型经常只挑
                # 「写一个四周计划」这类通用做法，把训练细节塞进 workoutName，
                # 到写入时才被拒绝。这里前置约束，把「先看规则再生成」变成默认行为。
                "instructions": (
                    "本服务把手机上的跑步课表与训练计划暴露给智能体，写入会用 App 的课表规则强校验。\n"
                    + APP_PLAN_WORKFLOW_TEXT + "\n"
                    + APP_STEP_RULES_TEXT +
                    "\n推荐调用顺序：list_sessions（看历史）→ list_workouts（看可用课表）→ "
                    "validate_plan（自检）→ set_plan（提交）→ wait_for_plan_result（等待手机验收）。"
                    "工具返回 RULE_VIOLATION 或 REJECTED 时不要向用户解释规则，"
                    "直接按 problems 修正并重新走完整链路；返回 PENDING/TIMEOUT 时不得判定完成，"
                    "用 get_plan_status 继续查询。"
                ),
            })
        elif method in ("notifications/initialized", "initialized"):
            continue
        elif method == "ping":
            resp = ok(msg_id, {})
        elif method == "tools/list":
            resp = ok(msg_id, {"tools": tool_defs()})
        elif method == "tools/call":
            params = msg.get("params") or {}
            name = params.get("name")
            text, is_error = call_tool(store, name, params.get("arguments") or {}, net)
            resp = ok(msg_id, {
                "content": [{"type": "text", "text": text}],
                "isError": is_error,
            })
        elif msg_id is not None:
            resp = err(msg_id, -32601, "Method not found: %s" % method)
        else:
            continue
        sys.stdout.write(json.dumps(resp, ensure_ascii=False, separators=(",", ":")) + "\n")
        sys.stdout.flush()
    net.stop()


# ---------------------------------------------------------------- 自检

def selftest(store_path):
    """真实起一个临时端口的 NetServer，走完整协议验证，不改真实数据。"""
    import http.client as _http_client
    results = []

    def check(name, cond, detail=""):
        results.append((name, bool(cond), detail))

    tmpdir = tempfile.mkdtemp(prefix="ir-bridge-test-")
    store = Store(os.path.join(tmpdir, "store.json"))

    # 协议工具
    check("发现包解析-合法", parse_discover('{"v":1,"type":"discover","client":"intervalrunning"}'))
    check("发现包解析-坏JSON", not parse_discover("{oops"))
    check("发现包解析-错客户端", not parse_discover('{"v":1,"type":"discover","client":"other"}'))
    reply = json.loads(build_agent_reply())
    check("回包字段", reply["v"] == 1 and reply["type"] == "agent" and reply["port"] == PORT
          and reply["id"] and reply["name"])

    # 起临时端口
    port = PORT + 1
    net = NetServer(store, port=port, host="127.0.0.1")
    net.start()
    time.sleep(0.3)

    def http(method, path, body=None, token=None, permission=None):
        conn = _http_client.HTTPConnection("127.0.0.1", port, timeout=5)
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token
        payload = None
        if body is not None:
            payload = json.dumps(body, ensure_ascii=False)
            if permission:
                body["permission"] = permission
                payload = json.dumps(body, ensure_ascii=False)
        conn.request(method, path, payload.encode("utf-8") if isinstance(payload, str) else payload, headers)
        r = conn.getresponse()
        raw = r.read().decode("utf-8")
        conn.close()
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return r.status, parsed

    # UDP 发现
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3)
    sock.sendto(json.dumps({"v": 1, "type": "discover", "client": "intervalrunning"}).encode(),
                ("127.0.0.1", port))
    try:
        data, _ = sock.recvfrom(4096)
        udp_ok = json.loads(data.decode()).get("type") == "agent"
    except Exception:
        udp_ok = False
    sock.close()
    check("UDP 发现单播回包", udp_ok)

    # 连接（手机侧会带 clientId 与时区）
    code, body = http("POST", "/v1/pair", {"v": 1, "client": "intervalrunning",
                                            "token": "tok-ro", "permission": "READ_ONLY",
                                            "clientId": "phone-a", "tzOffsetMinutes": 480,
                                            "tzId": "Asia/Shanghai"})
    check("连接-只读", code == 200 and body.get("code") == "OK", str(body))
    check("连接回执带设备标识", body.get("clientId") == "phone-a", str(body))

    code, body = http("POST", "/v1/pair", {"v": 1, "client": "intervalrunning",
                                            "token": "tok-rw", "permission": "READ_WRITE",
                                            "clientId": "phone-a"})
    check("连接-读写", code == 200, str(body))

    code, body = http("POST", "/v1/pair", {"v": 1, "client": "intervalrunning",
                                            "token": "x", "permission": "ADMIN"})
    check("连接-拒绝非法权限", code == 400, str(body))

    # 探活端点：只验活与令牌，不要求写权限（手机设置页状态灯用）
    code, body = http("GET", "/v1/ping", token="tok-ro")
    check("探活-只读档也放行", code == 200 and body.get("code") == "OK"
          and body.get("permission") == "READ_ONLY", str(body))
    check("探活回执带设备标识", body.get("clientId") == "phone-a", str(body))
    code, body = http("GET", "/v1/ping")
    check("探活-无令牌被拒", code == 401 and body.get("code") == "UNAUTHORIZED", str(body))
    code, body = http("GET", "/v1/ping", token="no-such-token")
    check("探活-未知令牌被拒", code == 401, str(body))

    # 越权防护：同一令牌不能改绑设备、不能提权
    code, body = http("POST", "/v1/pair", {"v": 1, "client": "intervalrunning",
                                            "token": "tok-ro", "permission": "READ_ONLY",
                                            "clientId": "phone-b"})
    check("令牌不能改绑到别的设备", code == 403 and body.get("code") == "TOKEN_CONFLICT", str(body))
    code, body = http("POST", "/v1/pair", {"v": 1, "client": "intervalrunning",
                                            "token": "tok-ro", "permission": "READ_WRITE",
                                            "clientId": "phone-a"})
    check("令牌不能提权", code == 403 and body.get("code") == "PERMISSION_ESCALATION", str(body))

    # 上传（只读档允许）
    sessions = [{"id": 1, "workoutName": "轻松跑", "startedAt": 1763400000000,
                 "totalDurationMs": 1800000, "distanceM": 5000.0,
                 "avgPaceSecPerKm": 360.0, "avgHr": 150, "maxHr": 170, "calories": 300.0}]
    code, body = http("POST", "/v1/sessions", {"v": 1, "client": "intervalrunning",
                                                "count": 1, "sessions": sessions,
                                                "clientId": "phone-a"},
                      token="tok-ro", permission="READ_ONLY")
    check("只读档可上传", code == 200 and body.get("saved") == 1, str(body))

    # 权限不一致
    code, body = http("POST", "/v1/sessions", {"v": 1, "count": 1, "sessions": sessions},
                      token="tok-ro", permission="READ_WRITE")
    check("权限不一致被拒", code == 403 and body.get("code") == "PERMISSION_MISMATCH", str(body))

    # count 与实际条数不符
    code, body = http("POST", "/v1/sessions", {"v": 1, "count": 5, "sessions": sessions,
                                                "clientId": "phone-a"},
                      token="tok-ro", permission="READ_ONLY")
    check("count 与条数不符被拒", code == 400 and body.get("code") == "BAD_SESSIONS", str(body))

    # startedAt 不是数字
    code, body = http("POST", "/v1/sessions",
                      {"v": 1, "count": 1, "clientId": "phone-a",
                       "sessions": [dict(sessions[0], startedAt="昨天")]},
                      token="tok-ro", permission="READ_ONLY")
    check("startedAt 非数字被拒", code == 400 and body.get("code") == "BAD_SESSIONS", str(body))

    # 去重 upsert（同一设备同一条 id 覆盖，不产生第二条）
    sessions2 = [dict(sessions[0], workoutName="改过的名字")]
    http("POST", "/v1/sessions", {"v": 1, "count": 1, "sessions": sessions2,
                                  "clientId": "phone-a"},
         token="tok-ro", permission="READ_ONLY")
    check("同 id 去重", len(store.snapshot()["sessions"]) == 1
          and list(store.snapshot()["sessions"].values())[0]["workoutName"] == "改过的名字")

    # 另一台设备同 id 不覆盖
    http("POST", "/v1/pair", {"v": 1, "client": "intervalrunning", "token": "tok-b",
                               "permission": "READ_ONLY", "clientId": "phone-b"})
    http("POST", "/v1/sessions", {"v": 1, "count": 1, "clientId": "phone-b",
                                  "sessions": [dict(sessions[0], workoutName="另一台手机")]},
         token="tok-b", permission="READ_ONLY")
    bucket = store.snapshot()["sessions"]
    check("不同设备同 id 不互相覆盖", len(bucket) == 2, str(list(bucket.keys())))

    # 全量快照语义：手机每次上传的是本机全部记录，本次没带到的同设备记录必须删掉。
    # 少了这一步，槽位只增不减，智能体读到的条数永远比手机本机多，怎么比对都对不上。
    code, body = http("POST", "/v1/sessions",
                      {"v": 1, "count": 0, "sessions": [], "clientId": "phone-b"},
                      token="tok-b", permission="READ_ONLY")
    bucket = store.snapshot()["sessions"]
    check("空快照清空本设备记录",
          code == 200 and bucket.get("phone-a::1") is not None
          and not any(k.startswith("phone-b::") for k in bucket),
          str(body)[:120])
    check("回执带剔除条数与本设备总数",
          body.get("removed") == 1 and body.get("deviceTotal") == 0, str(body)[:160])
    # 别的设备不受影响
    check("裁剪只动本设备槽位", "phone-a::1" in bucket, str(list(bucket.keys())))

    # 脏数据不该让排序接口崩。
    # HTTP 入口已经挡住非数字 startedAt，所以这里**绕过入口直接写存储**，
    # 模拟「历史遗留脏数据 / 手工改过 store.json」的情况，验证读取层自己也能扛住。
    # 注意 full_snapshot=False：这是绕过 HTTP 直接写存储，不是手机的全量上传，
    # 按全量处理会把 phone-a 已有的记录一并裁掉。
    store.upsert_sessions([{"id": 9, "workoutName": "脏数据", "startedAt": "not-a-number"}],
                          "phone-a", full_snapshot=False)
    text, is_err = call_tool(store, "list_sessions", {"clientId": "phone-a"})
    check("脏 startedAt 不让 list_sessions 崩溃", not is_err, text[:120])

    # 未配对
    code, body = http("GET", "/v1/plan", token="bad-token")
    check("未配对拉计划被拒", code == 401, str(body))

    # 只读拉计划
    code, body = http("GET", "/v1/plan", token="tok-ro")
    check("只读档拉计划被拒", code == 403 and body.get("code") == "PERMISSION_DENIED", str(body))

    # 写计划 + 读写拉计划
    plan = build_plan("测试计划", "2026-11-23", 4, "自检", [
        {"id": 1, "date": "2026-11-23", "hour": 7, "workoutName": "轻松跑",
         "steps": [{"type": "WARMUP", "durationSec": 300},
                   {"type": "WORK", "durationSec": 120,
                    "target": {"type": "HR_RANGE", "percent": False,
                               "minValue": 150, "maxValue": 170}}]},
        {"id": 2, "date": "2026-11-25", "workoutId": 5, "workoutName": "间歇"},
    ], tz=resolve_tz("Asia/Shanghai", 480))
    check("计划格式校验", not verify_plan_shape(plan, tz=resolve_tz("Asia/Shanghai", 480)),
          str(verify_plan_shape(plan, tz=resolve_tz("Asia/Shanghai", 480))))

    # 手机上传计划（新链路）：只读档拒绝、读写档成功，随后 MCP get_plan 应能读到
    code, body = http("POST", "/v1/plan", {"v": 1, "plan": plan, "clientId": "phone-a"},
                      token="tok-ro", permission="READ_ONLY")
    check("只读档上传计划被拒", code == 403 and body.get("code") == "PERMISSION_DENIED", str(body))
    rev0 = store.get_plan_revision("phone-a")
    code, body = http("POST", "/v1/plan", {"v": 1, "plan": plan, "clientId": "phone-a"},
                      token="tok-rw", permission="READ_WRITE")
    check("读写档上传计划成功", code == 200 and body.get("code") == "OK", str(body))
    check("上传后桥端有计划", store.get_plan("phone-a") is not None)
    check("手机上传不产生归档", len(store.snapshot()["plans"]["phone-a"]["archives"]) == 0)
    check("手机上传不升级版本", store.get_plan_revision("phone-a") == rev0)
    phone_rev0 = store.snapshot()["plans"]["phone-a"].get("phoneRevision", 0)
    code, body = http("POST", "/v1/plan",
                      {"v": 1, "plan": plan, "clientId": "phone-a", "baseRevision": rev0},
                      token="tok-rw", permission="READ_WRITE")
    check("手机带正确版本上传成功", code == 200 and body.get("phoneRevision") == phone_rev0 + 1,
          str(body)[:160])
    check("手机上传只增 phoneRevision",
          store.get_plan_revision("phone-a") == rev0
          and store.snapshot()["plans"]["phone-a"].get("source") == "phone")
    code, body = http("POST", "/v1/plan",
                      {"v": 1, "plan": plan, "clientId": "phone-a",
                       "baseRevision": rev0 - 1},
                      token="tok-rw", permission="READ_WRITE")
    check("手机用旧版本上传计划被拒", code == 409 and body.get("code") == "CONFLICT",
          str(body)[:160])

    code, body = http("GET", "/v1/plan", token="tok-rw")
    check("拉计划带版本号", body.get("revision") == rev0, str(body)[:120])
    check("拉计划响应含统一 code 字段", body.get("code") == "OK", str(body)[:120])
    text, is_err = call_tool(store, "get_plan", {"clientId": "phone-a"})
    check("MCP get_plan 读到上传的计划", not is_err and "测试计划" in text, text[:120])

    code, body = http("GET", "/v1/plan", token="tok-rw")
    check("读写档拉计划成功", code == 200 and body["plan"]["name"] == "测试计划", str(body)[:200])
    check("自定义步骤进计划", bool(body["plan"]["entries"][0].get("customWorkout")))

    # MCP 工具层
    text, is_err = call_tool(store, "list_sessions", {"clientId": "phone-a"})
    check("MCP list_sessions", not is_err and '"count": 2' in text, text[:160])
    text, is_err = call_tool(store, "get_status", {})
    check("MCP get_status", not is_err and "pairedClients" in text)
    rev_before_mcp = store.get_plan_revision("phone-a")
    archives_before_mcp = len(store.snapshot()["plans"]["phone-a"]["archives"])
    text, is_err = call_tool(store, "set_plan", {
        "name": "MCP 计划", "startDate": "2026-11-23", "weeks": 3,
        "clientId": "phone-a",
        "entries": [{
            "date": "2026-11-24", "workoutName": "节奏跑",
            "steps": [{"type": "WORK", "durationSec": 600}],
        }],
    })
    check("MCP set_plan 返回待验收",
          not is_err and "PLAN_SUBMITTED" in text
          and "PENDING_APP_ACCEPTANCE" in text
          and '"completed": false' in text, text)
    check("智能体写新计划先归档旧计划",
          '"archivedPlan": "' in text
          and len(store.snapshot()["plans"]["phone-a"]["archives"]) == archives_before_mcp + 1,
          text[:200])
    check("智能体写计划升级版本",
          store.get_plan_revision("phone-a") == rev_before_mcp + 1)

    # ---- 验收闭环：提交 → PENDING → 手机回执 → APPLIED/REJECTED ----
    text, is_err = call_tool(store, "get_plan_status", {"clientId": "phone-a"})
    check("提交后状态为 PENDING",
          not is_err and '"status": "PENDING"' in text and '"completed": false' in text,
          text[:200])
    accepted_rev = store.get_plan_revision("phone-a")
    store.record_plan_acceptance("phone-a", accepted_rev, "REJECTED", ["第 1 段快慢跑混用"])
    text, is_err = call_tool(store, "get_plan_status", {"clientId": "phone-a"})
    check("手机拒绝后状态为 REJECTED 且带明细",
          not is_err and '"status": "REJECTED"' in text and "快慢跑混用" in text,
          text[:220])
    text, is_err = call_tool(store, "wait_for_plan_result",
                             {"clientId": "phone-a", "timeoutSec": 1})
    check("wait 立即返回已拒绝结果",
          not is_err and '"status": "REJECTED"' in text and '"completed": false' in text,
          text[:220])
    store.record_plan_acceptance("phone-a", accepted_rev, "APPLIED", [], "已落库")
    text, is_err = call_tool(store, "wait_for_plan_result",
                             {"clientId": "phone-a", "timeoutSec": 2})
    check("wait 收到 APPLIED 才判定完成",
          not is_err and '"status": "APPLIED"' in text and '"completed": true' in text,
          text[:220])

    # 新提交必须把上一版的 APPLIED 重置为 PENDING，不能继承完成态
    rev_before_resubmit = store.get_plan_revision("phone-a")
    call_tool(store, "set_plan", {
        "clientId": "phone-a", "name": "再次提交", "startDate": "2026-11-23", "weeks": 2,
        "entries": [{"date": "2026-11-23", "workoutName": "Y",
                     "steps": [{"type": "WORK", "durationSec": 60}]}],
    })
    text, is_err = call_tool(store, "get_plan_status", {"clientId": "phone-a"})
    check("新提交重置为 PENDING，不继承上一版 APPLIED",
          not is_err and '"status": "PENDING"' in text
          and store.get_plan_revision("phone-a") == rev_before_resubmit + 1, text[:220])

    # 等待超时：手机没回执时必须返回 PENDING，绝不能默认成功
    text, is_err = call_tool(store, "wait_for_plan_result",
                             {"clientId": "phone-a", "timeoutSec": 1})
    check("等待超时返回 PENDING 且未完成",
          not is_err and '"code": "PLAN_PENDING"' in text
          and '"completed": false' in text and '"waitedSec": 1' in text, text[:240])

    # 迟到回执不能把当前版本标成上一版的结果
    current_rev = store.get_plan_revision("phone-a")
    store.record_plan_acceptance("phone-a", current_rev - 1, "APPLIED", [], "迟到的通过")
    check("迟到回执不覆盖当前版本",
          store.get_plan_acceptance("phone-a").get("status") == "PENDING",
          str(store.get_plan_acceptance("phone-a")))

    # HTTP 验收回执：手机把 APP 端结果推回桥端
    code, body = http("POST", "/v1/plan/result", {
        "v": 1, "clientId": "phone-a", "revision": current_rev,
        "status": "REJECTED", "problems": ["HTTP 回执测试"], "message": "手机拒绝",
    }, token="tok-rw", permission="READ_WRITE")
    acceptance = store.get_plan_acceptance("phone-a")
    check("HTTP 验收回执被接收",
          code == 200 and body.get("code") == "OK", str(body))
    check("HTTP 回执写入当前版本",
          acceptance.get("status") == "REJECTED"
          and acceptance.get("problems") == ["HTTP 回执测试"], str(acceptance))
    code, body = http("POST", "/v1/plan/result", {
        "v": 1, "clientId": "phone-a", "revision": current_rev,
        "status": "APPLIED",
    }, token="tok-ro", permission="READ_ONLY")
    check("只读档不能上报验收",
          code == 403 and body.get("code") == "PERMISSION_DENIED", str(body))

    # 手机回传自己计划时：待验收 -> PHONE_UPLOADED；已通过 -> 保留 APPLIED
    upload_plan = build_plan("覆盖测试", "2026-11-23", 2, "", [
        {"date": "2026-11-23", "workoutId": 5, "workoutName": "轻松跑"},
    ], tz=resolve_tz("Asia/Shanghai", 480))
    store.set_plan("phone-u", upload_plan)
    store.phone_upload_plan("phone-u", upload_plan)
    check("待验收时手机覆盖计划 -> PHONE_UPLOADED",
          store.get_plan_acceptance("phone-u").get("status") == "PHONE_UPLOADED",
          str(store.get_plan_acceptance("phone-u")))
    store.set_plan("phone-u", upload_plan)
    uid = store.get_plan_revision("phone-u")
    store.record_plan_acceptance("phone-u", uid, "APPLIED", [], "已落库")
    store.phone_upload_plan("phone-u", upload_plan)
    check("已通过的验收不会被手机回传抹掉",
          store.get_plan_acceptance("phone-u").get("status") == "APPLIED",
          str(store.get_plan_acceptance("phone-u")))

    # 自检：只校验，不写库
    rev_before_check = store.get_plan_revision("phone-a")
    text, is_err = call_tool(store, "validate_plan", {
        "clientId": "phone-a", "name": "自检通过", "startDate": "2026-11-23", "weeks": 2,
        "entries": [{"date": "2026-11-23", "workoutName": "轻松跑",
                     "steps": [{"type": "WARMUP", "durationSec": 300},
                               {"type": "WORK", "durationSec": 600}]}],
    })
    check("自检通过且不写库",
          not is_err and "SELF_CHECK_PASSED" in text
          and store.get_plan_revision("phone-a") == rev_before_check, text[:200])
    text, is_err = call_tool(store, "validate_plan", {
        "clientId": "phone-a", "name": "自检失败", "startDate": "2026-11-23", "weeks": 2,
        "entries": [{"date": "2026-11-23", "workoutName": "混搭",
                     "steps": [{"type": "WORK", "distanceM": 1000},
                               {"type": "RECOVERY", "durationSec": 60}]}],
    })
    check("自检失败返回明细且不写库",
          not is_err and "SELF_CHECK_FAILED" in text and "混用了两种计量方式" in text
          and store.get_plan_revision("phone-a") == rev_before_check, text[:240])

    # 智能体写「另一台设备」不能改变当前活跃设备，否则后续省略 clientId 会写错槽位
    active_before = store.active_client_id()
    call_tool(store, "set_plan", {
        "clientId": "other-slot", "name": "别的设备", "startDate": "2026-11-23",
        "weeks": 2, "entries": [{"date": "2026-11-23", "workoutName": "X",
                                 "steps": [{"type": "WORK", "durationSec": 60}]}],
    })
    check("智能体写别的设备不改活跃设备", store.active_client_id() == active_before,
          store.active_client_id())

    text, is_err = call_tool(store, "get_plan", {"clientId": "phone-a"})
    check("MCP get_plan", not is_err and '"entries"' in text, text[:120])
    text, is_err = call_tool(store, "set_plan", {"name": "坏", "startDate": "2026-11-23",
                                                  "weeks": 99, "entries": []})
    check("MCP set_plan 校验", is_err)

    # 反馈里的静默失败：workoutId=0 + 原始 steps，没有 customWorkout
    bad_entry = {
        "id": 1, "date": date_ms("2026-11-23", resolve_tz("Asia/Shanghai", 480)), "workoutId": 0,
        "workoutName": "无内容",
    }
    bad_plan = {"name": "坏", "startDate": date_ms("2026-11-23", resolve_tz("Asia/Shanghai", 480)),
                "weeks": 2, "entries": [bad_entry]}
    check("无训练内容 entry 被校验拒绝",
          any("无有效 customWorkout" in p for p in verify_plan_shape(bad_plan)))

    raw_steps_plan = {
        "name": "原生修复验证", "startDate": date_ms("2026-11-23", resolve_tz("Asia/Shanghai", 480)),
        "weeks": 2,
        "entries": [{
            "id": 1, "date": date_ms("2026-11-23", resolve_tz("Asia/Shanghai", 480)), "hour": 7,
            "workoutId": 0, "workoutName": "测试",
            "steps": [{"type": "WORK", "distanceM": 5000,
                       "target": {"type": "OPEN", "percent": False}}],
        }],
    }
    text, is_err = call_tool(store, "set_plan_json", {"plan": raw_steps_plan, "clientId": "phone-a"})
    written = store.get_plan("phone-a")
    check("set_plan_json 自动补 customWorkout",
          not is_err and '"autoEncodedEntries": 1' in text
          and "PENDING_APP_ACCEPTANCE" in text
          and bool(written["entries"][0].get("customWorkout")), text[:220])
    check("补齐后 App 可解析", app_parseable_count(written) == 1)
    check("回执包含可解析条数", '"parseableEntries": 1' in text, text[:220])

    # 越界值与超周期条目必须被拒（App 侧只会夹紧，静默改写不是我们要的）
    tz_sh = resolve_tz("Asia/Shanghai", 480)
    out_of_range = {
        "name": "越界", "startDate": date_ms("2026-11-23", tz_sh), "weeks": 2,
        "entries": [{"id": 1, "date": date_ms("2026-11-23", tz_sh), "hour": 99, "minute": 88,
                     "workoutId": 5}],
    }
    check("hour/minute 越界被拒",
          any("hour 越界" in p for p in verify_plan_shape(out_of_range, tz=tz_sh)),
          str(verify_plan_shape(out_of_range, tz=tz_sh)))
    beyond = {
        "name": "超周期", "startDate": date_ms("2026-11-23", tz_sh), "weeks": 2,
        "entries": [{"id": 1, "date": date_ms("2026-12-30", tz_sh), "workoutId": 5}],
    }
    check("条目日期超出计划周期被拒",
          any("不在计划周期" in p for p in verify_plan_shape(beyond, tz=tz_sh)),
          str(verify_plan_shape(beyond, tz=tz_sh)))
    empty_plan = {"name": "空", "startDate": date_ms("2026-11-23", tz_sh), "weeks": 2, "entries": []}
    check("空计划被拒", any("entries 为空" in p for p in verify_plan_shape(empty_plan, tz=tz_sh)))

    # ---- App 训练段规则闸门（智能体输出必须 100% 合规） ----
    _norm, rule_problems, _notes = normalize_workout_steps([
        {"type": "WORK", "distanceM": 1000},
        {"type": "RECOVERY", "durationSec": 60},
    ], "混合计量")
    check("规则-快慢跑混用计量被拦",
          any("混用了两种计量方式" in p for p in rule_problems), str(rule_problems))

    _norm, rule_problems, _notes = normalize_workout_steps([
        {"type": "WORK", "durationSec": 240, "distanceM": 800},
    ], "两种量")
    check("规则-同时给时长与距离被拦",
          any("只能二选一" in p for p in rule_problems), str(rule_problems))

    _norm, rule_problems, _notes = normalize_workout_steps([
        {"type": "WARMUP", "distanceM": 1000,
         "target": {"type": "HR_RANGE", "percent": True, "minValue": 50, "maxValue": 60}},
    ], "热身")
    check("规则-热身距离与目标被拦",
          any("热身：只能按时间" in p for p in rule_problems)
          and any("热身：不能设置目标区间" in p for p in rule_problems), str(rule_problems))

    _norm, rule_problems, _notes = normalize_workout_steps([
        {"type": "WORK", "durationSec": 240,
         "target": {"type": "HR_RANGE", "minValue": 150, "maxValue": 170}},
    ], "心率")
    check("规则-心率目标缺 percent 被拦",
          any("必须显式给 target.percent" in p for p in rule_problems), str(rule_problems))

    _norm, rule_problems, rule_notes = normalize_workout_steps([
        {"type": "WORK", "durationSec": 60, "repeatCount": 5},
    ], "单段重复")
    check("映射-单段 repeatCount 自动补循环组",
          not rule_problems and _norm[0].get("repeatGroupId") == 1 and bool(rule_notes),
          str(rule_notes))

    _norm, rule_problems, _notes = normalize_workout_steps([
        {"type": "WORK", "distanceM": 1000,
         "target": {"type": "PACE_RANGE", "minValue": "4:30", "maxValue": "4:40"}},
    ], "配速")
    check("映射-配速 4:30 读成秒/公里",
          not rule_problems and _norm[0]["target"]["minValue"] == 270.0, str(rule_problems))

    set_plan_desc = next(t["description"] for t in tool_defs() if t["name"] == "set_plan")
    check("prompt-工具描述前置 App 规则",
          "App 课表硬规则" in set_plan_desc and "不允许混用" in set_plan_desc)

    # ---- 文本参数一致性：量化标准必须落到结构化字段 ----
    def _text_check(title, steps, name="训练", desc=""):
        normalized, probs, _ = normalize_workout_steps(steps, title)
        return probs + validate_workout_text(name, desc, normalized, title)

    _p = _text_check("文本-OPEN配速", [
        {"type": "WORK", "durationSec": 240, "label": "配速4'30\""}])
    check("规则-OPEN 目标禁止文本配速",
          any("目标类型是 OPEN" in item for item in _p), str(_p))

    _p = _text_check("文本-心率配速混写", [
        {"type": "WORK", "durationSec": 240, "label": "心率160，配速4'30\"",
         "target": {"type": "HR_RANGE", "percent": False,
                    "minValue": 160, "maxValue": 170}}])
    check("规则-心率目标禁止文本配速",
          any("目标是心率" in item for item in _p), str(_p))

    _p = _text_check("文本-距离落到字段", [
        {"type": "WORK", "distanceM": 1000, "label": "阈值1000m"}])
    check("规则-距离与 distanceM 一致", not _p, str(_p))

    _p = _text_check("文本-距离只在名称", [
        {"type": "WORK", "durationSec": 240, "label": "阈值1000m"}])
    check("规则-时间步禁止文本距离",
          any("按时间计量" in item for item in _p), str(_p))

    _p = _text_check("文本-步频只在名称", [
        {"type": "WORK", "durationSec": 240, "label": "步频180",
         "target": {"type": "HR_RANGE", "percent": False,
                    "minValue": 150, "maxValue": 160}}])
    check("规则-步频必须配置 cadenceSpm",
          any("没有配置 cadenceSpm" in item for item in _p), str(_p))

    _p = _text_check("文本-标题心率一致", [
        {"type": "WORK", "durationSec": 240,
         "target": {"type": "HR_RANGE", "percent": False,
                    "minValue": 160, "maxValue": 170}}],
        name="心率160-170 阈值跑")
    check("规则-标题心率与目标一致", not _p, str(_p))

    _p = _text_check("文本-标题心率无目标", [
        {"type": "WORK", "durationSec": 240}], name="心率160-170 阈值跑")
    check("规则-标题心率必须有对应目标",
          any("标题/描述写了心率" in item for item in _p), str(_p))

    _p = _text_check("文本-结构名不误判", [
        {"type": "WARMUP", "durationSec": 600},
        {"type": "WORK", "durationSec": 240, "repeatGroupId": 1, "repeatCount": 4},
        {"type": "RECOVERY", "durationSec": 180, "repeatGroupId": 1, "repeatCount": 4}],
        name="挪威 4×4")
    check("规则-结构名不误判为标准", not _p, str(_p))

    plan_text = {"name": "配速4'30\" 阈值计划", "startDate": 1, "weeks": 2,
                 "entries": [{"id": 1, "date": 1,
                              "customWorkout": encode_workout_snapshot("阈值跑", [
                                  {"type": "WORK", "durationSec": 240,
                                   "target": {"type": "HR_RANGE", "percent": False,
                                              "minValue": 160, "maxValue": 170}}])}]}
    plan_text_problems = validate_plan_rules(plan_text)
    check("规则-计划标题配速必须有结构化目标",
          any("计划名称/备注写了配速" in item for item in plan_text_problems),
          str(plan_text_problems))

    # 全参数结构化样例：配速、心率、步频、恢复门槛都必须落到字段并可通过自检
    structured_args = {
        "clientId": "phone-a", "name": "结构化参数验证",
        "startDate": "2026-11-23", "weeks": 2,
        "entries": [{
            "date": "2026-11-24", "workoutName": "阈值 1000m",
            "steps": [
                {"type": "WARMUP", "durationSec": 600},
                {"type": "WORK", "distanceM": 1000, "repeatGroupId": 1, "repeatCount": 4,
                 "cadenceSpm": 180,
                 "target": {"type": "PACE_RANGE", "minValue": "4:30", "maxValue": "4:40"}},
                {"type": "RECOVERY", "distanceM": 200, "repeatGroupId": 1, "repeatCount": 4,
                 "hrGatePercent": 70,
                 "target": {"type": "HR_RANGE", "percent": True,
                            "minValue": 60, "maxValue": 70}},
                {"type": "COOLDOWN", "durationSec": 300},
            ],
        }],
    }
    text, is_err = call_tool(store, "validate_plan", structured_args)
    check("自检-全参数结构化计划通过",
          not is_err and "SELF_CHECK_PASSED" in text, text[:240])
    encoded = encode_workout_snapshot(
        "阈值 1000m", structured_args["entries"][0]["steps"])
    encoded_steps = json.loads(encoded)["workout"]["steps"]
    check("编码-配速/心率/步频/恢复门槛全部落到字段",
          encoded_steps[1]["target"]["type"] == "PACE_RANGE"
          and encoded_steps[1]["cadenceSpm"] == 180
          and encoded_steps[2]["target"]["type"] == "HR_RANGE"
          and encoded_steps[2]["target"]["percent"] is True
          and encoded_steps[2]["hrGatePercent"] == 70,
          encoded[:400])

    # 违规写入必须整份拦截，且桥端版本不变（客户端不会拉到坏计划）
    rev_before_rule = store.get_plan_revision("phone-a")
    text, is_err = call_tool(store, "set_plan", {
        "clientId": "phone-a", "name": "违规计划", "startDate": "2026-11-23", "weeks": 2,
        "entries": [{"date": "2026-11-23", "workoutName": "混搭",
                     "steps": [{"type": "WORK", "distanceM": 1000},
                               {"type": "RECOVERY", "durationSec": 60}]}],
    })
    check("set_plan 违规被拦截且未写入",
          is_err and "RULE_VIOLATION" in text and "REGENERATE" in text
          and store.get_plan_revision("phone-a") == rev_before_rule, text[:200])

    # 直接塞 customWorkout 快照也不能绕过规则
    bad_snapshot = json.loads(
        encode_workout_snapshot("合法占位", [{"type": "WORK", "durationSec": 60}]))
    bad_snapshot["workout"]["steps"].append({"type": "RECOVERY", "distanceM": 200})
    bad_snapshot_text = json.dumps(bad_snapshot, ensure_ascii=False)
    text, is_err = call_tool(store, "set_plan_json", {
        "clientId": "phone-a",
        "plan": {"name": "快照绕过", "startDate": date_ms("2026-11-23", tz_sh), "weeks": 2,
                 "entries": [{"id": 1, "date": date_ms("2026-11-23", tz_sh), "workoutId": 0,
                              "customWorkout": bad_snapshot_text}]},
    })
    check("set_plan_json 违规快照被拦截",
          is_err and "RULE_VIOLATION" in text, text[:200])

    text, is_err = call_tool(store, "set_workouts", {
        "clientId": "phone-a",
        "workouts": [{"name": "混搭课表", "snapshot": bad_snapshot_text}],
    })
    check("set_workouts 违规快照被拦截",
          is_err and "RULE_VIOLATION" in text, text[:200])

    # 时区口径：同一个日历日在手机时区与桥端时区必须落在同一时刻
    tz_sh = resolve_tz("Asia/Shanghai", 480)
    ms_sh = date_ms("2026-11-23", tz_sh)
    check("计划日期按手机时区换算",
          datetime.datetime.fromtimestamp(ms_sh / 1000.0, tz_sh).strftime("%Y-%m-%d %H:%M")
          == "2026-11-23 00:00",
          str(ms_sh))
    check("桥端时区与手机时区相差整日（回归基线）",
          ms_sh != date_ms("2026-11-23", resolve_tz("America/New_York", -300)))

    # 并发写：多线程同时写，不该丢数据（读-改-写必须串行）
    store2 = Store(os.path.join(tmpdir, "store.json"))
    errors = []
    # 基线按当时的实际条数取：前面几个用例会增删记录（例如全量快照会清掉已删的），
    # 写死一个数字迟早会因为别的用例调整而假红。
    baseline_sessions = len(store2.snapshot()["sessions"])

    def writer(tag):
        try:
            for i in range(25):
                store2.upsert_sessions(
                    [{"id": "%s-%d" % (tag, i), "workoutName": tag, "startedAt": 1763400000000 + i}],
                    "phone-%s" % tag,
                    # 这一项只验存储层的读-改-写是否串行；每条都是「单条增量」而不是
                    # 手机的全量快照，所以关掉裁剪，否则同一设备的 25 次写会互相删除。
                    full_snapshot=False)
        except Exception as exc:  # 并发下写失败也算问题
            errors.append(repr(exc))

    threads = [threading.Thread(target=writer, args=("t%d" % i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("并发写不报错", not errors, "; ".join(errors[:3]))
    check("并发写不丢数据（75 条全在）",
          len(store.snapshot()["sessions"]) == baseline_sessions + 75,
          str(len(store.snapshot()["sessions"])))

    # 课表数据读取/写入权限
    ro_client = (store.find_pair("tok-ro") or {}).get("clientId") or DEFAULT_CLIENT
    snapshot = encode_workout_snapshot("自检课表", [{"type": "WORK", "durationSec": 300}])
    code, body = http("POST", "/v1/workouts",
                      {"v": 1, "clientId": ro_client,
                       "workouts": [{"id": 11, "snapshot": snapshot}]},
                      token="tok-ro", permission="READ_ONLY")
    check("只读档可上传课表数据", code == 200 and body.get("received") == 1, str(body)[:160])
    check("手机上传不升课表版本", store.get_workouts(ro_client)[0] == 0)
    code, body = http("GET", "/v1/workouts", token="tok-ro")
    check("只读档不能拉课表数据", code == 403 and body.get("code") == "PERMISSION_DENIED",
          str(body)[:160])
    text, is_err = call_tool(store, "set_workouts", {
        "clientId": ro_client,
        "workouts": [{"id": 12, "name": "智能体课表",
                      "steps": [{"type": "WORK", "durationSec": 600}]}],
    })
    check("智能体写课表数据", not is_err and "已写入 1 套课表数据" in text, text[:160])
    rev, items = store.get_workouts(ro_client)
    check("课表版本升级且内容完整", rev == 1 and len(items) == 2,
          "rev=%d count=%d" % (rev, len(items)))
    text, is_err = call_tool(store, "list_workouts", {"clientId": ro_client})
    check("MCP list_workouts 可读", not is_err and "智能体课表" in text, text[:160])

    # 预设只读：智能体不能把预设写入课表库
    text, is_err = call_tool(store, "set_workouts", {
        "clientId": ro_client,
        "workouts": [{"id": 13, "name": "内置课表改写版", "isPreset": True,
                      "presetKey": "preset-x", "steps": [{"type": "WORK", "durationSec": 120}]}],
    })
    check("智能体写预设被拒绝", is_err and "预设只读" in text, text[:160])

    # 空步骤课表会把手机用户课表清成 0 段，必须在桥端就拒绝
    empty_snapshot = json.dumps({"workout": {"name": "空课表", "steps": []}}, ensure_ascii=False)
    text, is_err = call_tool(store, "set_workouts", {
        "clientId": ro_client,
        "workouts": [{"id": 14, "name": "空课表", "snapshot": empty_snapshot}],
    })
    check("智能体写空课表被拒绝", is_err and "没有任何步骤" in text, text[:160])

    # 步骤名 7 字上限 + 步频透传：接口写入时就要按 App 规则收口，不能等打开编辑器才截
    text, is_err = call_tool(store, "set_plan", {
        "clientId": "phone-a", "name": "步频验证计划", "startDate": "2026-11-23", "weeks": 2,
        "entries": [{
            "date": "2026-11-23", "workoutName": "步频课",
            "steps": [{
                "type": "WORK", "durationSec": 300, "cadenceSpm": 182,
                "label": "这是一个超过七个字的步骤名称",
            }],
        }],
    })
    written = store.get_plan("phone-a") or {}
    step = json.loads(written["entries"][0]["customWorkout"])["workout"]["steps"][0]
    check("接口写入步骤名截到 7 字", not is_err and len(step.get("label", "")) <= 7,
          str(step)[:160])
    check("接口写入步频保留", step.get("cadenceSpm") == 182, str(step)[:160])

    before_delete = len(store.get_workouts(ro_client)[1])
    rev_before_delete = store.get_workouts(ro_client)[0]
    text, is_err = call_tool(store, "delete_workout", {"clientId": ro_client, "id": 12})
    after_items = store.get_workouts(ro_client)[1]
    check("MCP 删除课表成功",
          not is_err and '"ok": true' in text and len(after_items) == before_delete - 1,
          text[:160])
    check("删除课表升版本并移除条目",
          store.get_workouts(ro_client)[0] == rev_before_delete + 1
          and all(item.get("id") != 12 for item in after_items))
    check("删除墓碑可供手机拉取", 12 in store.get_workout_deletions(ro_client))

    # 原子清理：21 条保留 10 条的同类操作，最终数量必须精确
    for wid in (20, 21, 22):
        call_tool(store, "set_workouts", {
            "clientId": ro_client,
            "workouts": [{"id": wid, "name": "清理测试%d" % wid,
                          "steps": [{"type": "WORK", "durationSec": 60}]}],
        })
    rev, items = store.get_workouts(ro_client)
    custom_before = sum(1 for item in items if not item.get("isPreset"))
    text, is_err = call_tool(store, "keep_only_custom_workouts", {
        "clientId": ro_client, "keepIds": [20],
        "expectedRevision": rev, "expectedCustomCount": custom_before,
    })
    _, after = store.get_workouts(ro_client)
    check("原子清理精确保留指定课表",
          not is_err and '"afterCustomCount": 1' in text
          and [item.get("id") for item in after] == [20], text[:200])

    # 带旧 revision 的清理必须整批拒绝，不能部分生效
    text, is_err = call_tool(store, "keep_only_custom_workouts", {
        "clientId": ro_client, "keepIds": [20], "expectedRevision": 0,
    })
    check("旧版本清理被拒绝", is_err and "课表版本已变化" in text, text[:160])

    # 已删除 id 的复活尝试：整批拒绝，且不影响其余课表
    new_rev = store.get_workouts(ro_client)[0]
    snap = encode_workout_snapshot("复活尝试", [{"type": "WORK", "durationSec": 60}])
    code, body = http("POST", "/v1/workouts", {
        "v": 1, "clientId": ro_client, "baseRevision": new_rev,
        "workouts": [{"id": 21, "snapshot": snap}],
    }, token="tok-ro", permission="READ_ONLY")
    check("已删除课表不能复活", code == 409, str(body)[:160])
    check("复活失败不影响现有课表",
          [item.get("id") for item in store.get_workouts(ro_client)[1]] == [20])

    # 手机用旧版本上传：整批拒绝，避免覆盖删除结果
    code, body = http("POST", "/v1/workouts", {
        "v": 1, "clientId": ro_client, "baseRevision": 0,
        "workouts": [{"id": 30, "snapshot": snap}],
    }, token="tok-ro", permission="READ_ONLY")
    check("旧版本上传被拒绝", code == 409, str(body)[:160])

    # 手机删掉课表后再上传：桥端应整份替换，而不是留着旧记录
    code, body = http("POST", "/v1/workouts",
                      {"v": 1, "clientId": ro_client, "workouts": []},
                      token="tok-ro", permission="READ_ONLY")
    check("手机删除课表后桥端同步消失",
          code == 200 and len(store.get_workouts(ro_client)[1]) == 0, str(body)[:160])

    net.stop()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n自检结果：%d/%d 通过\n" % (passed, len(results)))
    for name, ok, detail in results:
        print("  [%s] %s%s" % ("PASS" if ok else "FAIL", name,
                               ("  -> " + detail) if not ok and detail else ""))
    print("\n临时数据目录：%s" % tmpdir)
    return 0 if passed == len(results) else 1


def status_cmd(store):
    text, _ = call_tool(store, "get_status", {})
    print(text)


def main():
    parser = argparse.ArgumentParser(description="intervalrunning 智能体桥接服务 v1")
    parser.add_argument("mode", nargs="?", default="mcp",
                        choices=["mcp", "serve", "status", "selftest"],
                        help="mcp=默认；serve=只跑网络；status=看状态；selftest=自检")
    parser.add_argument("--store", default=DEFAULT_STORE, help="store.json 路径")
    args = parser.parse_args()

    if args.mode == "selftest":
        return selftest(args.store)

    store = Store(args.store)
    if args.mode == "status":
        status_cmd(store)
        return 0
    if args.mode == "serve":
        net = NetServer(store)
        net.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            net.stop()
        return 0
    return mcp_main(store)


if __name__ == "__main__":
    sys.exit(main())
