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

def build_plan(name, start_date, weeks, note, entries, now=None, tz=None):
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
                item["workoutName"] or "计划内训练", steps, now)
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


def encode_workout_snapshot(name, steps, now=None):
    """构造 BackupCodec.encodeWorkoutSnapshot 的字符串：{"workout": {...}}。"""
    now = now or now_ms()
    encoded = []
    for i, s in enumerate(steps):
        if not isinstance(s, dict):
            raise ValueError("第 %d 个 step 不是对象" % (i + 1))
        stype = s.get("type", "WORK")
        if stype not in STEP_TYPES:
            raise ValueError("step.type 必须是 %s 之一" % "/".join(STEP_TYPES))
        step = {"order": i, "type": stype, "repeatCount": int(s.get("repeatCount", 1))}
        if s.get("durationSec") is not None:
            step["durationSec"] = int(s["durationSec"])
        if s.get("distanceM") is not None:
            step["distanceM"] = int(s["distanceM"])
        if "durationSec" not in step and "distanceM" not in step:
            raise ValueError("step 至少要给 durationSec 或 distanceM")
        target = s.get("target") or {}
        if not isinstance(target, dict):
            raise ValueError("step.target 必须是对象")
        ttype = target.get("type", "OPEN")
        if ttype not in TARGET_TYPES:
            raise ValueError("target.type 必须是 %s 之一" % "/".join(TARGET_TYPES))
        t = {"type": ttype, "percent": bool(target.get("percent", False))}
        if target.get("minValue") is not None:
            t["minValue"] = float(target["minValue"])
        if target.get("maxValue") is not None:
            t["maxValue"] = float(target["maxValue"])
        step["target"] = t
        for k in ("repeatGroupId", "hrGateMaxBpm", "hrGatePercent", "cadenceSpm"):
            if s.get(k) is not None:
                step[k] = int(s[k])
        if s.get("label"):
            step["label"] = truncate_step_label(s["label"])
        encoded.append(step)
    workout = {
        "name": str(name), "description": "", "intensity": "", "applicableTo": "",
        "intensityLevel": "MODERATE", "createdAt": now, "updatedAt": now,
        "steps": encoded,
    }
    return json.dumps({"workout": workout}, ensure_ascii=False, separators=(",", ":"))


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
    for entry in plan.get("entries", []):
        if not isinstance(entry, dict):
            continue
        # 已有的 customWorkout 也要按 7 字规则整理，避免「接口写长名、打开编辑才截短」
        if isinstance(entry.get("customWorkout"), str):
            entry["customWorkout"] = normalize_snapshot_labels(entry["customWorkout"])
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
            "name": "set_plan",
            "description": (
                "创建训练计划。每条 entry 用 date(yyyy-MM-dd) 指定训练日；"
                "给 steps 则生成计划内自定义训练（不依赖课表库），不给则用 workoutId 引用课表库。"
                "weeks 必须是 2..16。若已有计划，会先留档旧计划再写入新计划，绝不直接覆盖；"
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
                        "description": "逐条训练",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "date": {"type": "string", "description": "yyyy-MM-dd"},
                                "hour": {"type": "integer", "default": 19},
                                "minute": {"type": "integer", "default": 0},
                                "workoutName": {"type": "string"},
                                "workoutId": {"type": "integer", "default": 0},
                                "steps": {
                                    "type": "array",
                                    "description": "自定义训练步骤；type=WARMUP/WORK/RECOVERY/COOLDOWN",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "type": {"type": "string", "enum": list(STEP_TYPES)},
                                            "durationSec": {"type": "integer"},
                                            "distanceM": {"type": "integer"},
                                            "label": {"type": "string"},
                                            "repeatCount": {"type": "integer", "default": 1},
                                            "repeatGroupId": {"type": "integer"},
                                            "hrGateMaxBpm": {"type": "integer"},
                                            "hrGatePercent": {"type": "integer"},
                                            "cadenceSpm": {"type": "integer"},
                                            "target": {
                                                "type": "object",
                                                "properties": {
                                                    "type": {"type": "string", "enum": list(TARGET_TYPES)},
                                                    "minValue": {"type": "number"},
                                                    "maxValue": {"type": "number"},
                                                    "percent": {"type": "boolean"},
                                                },
                                            },
                                        },
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
            "description": "读取当前已写入的计划（App 原生 JSON）。用于核对或修改前查看。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "clientId": {"type": "string", "description": "读哪台设备的计划；省略则读当前活跃设备"},
                },
            },
        },
        {
            "name": "set_plan_json",
            "description": (
                "直接写入 App 原生计划 JSON（当 set_plan 的友好参数不够用时使用）。"
                "字段：name/startDate(epoch毫秒)/weeks/note/entries。"
                "已有计划会先留档再写入。"
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
                "手机联网且权限为读写时会自动同步落库。"
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
                                "steps": {"type": "array", "items": {"type": "object"}},
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
                "hasPlan": bool((snap.get("plans", {}).get(active) or {}).get("plan")),
                "parseableEntries": app_parseable_count(
                    (snap.get("plans", {}).get(active) or {}).get("plan")),
                "planArchives": len((snap.get("plans", {}).get(active) or {}).get("archives", []) or []),
                "clientTz": snap.get("clientTz"),
                "storePath": store.path,
            }
            return json.dumps(status, ensure_ascii=False, indent=2), False

        if name == "set_plan":
            tz = store.active_tz()
            plan = build_plan(
                args.get("name"), args.get("startDate"), args.get("weeks"),
                args.get("note", ""), args.get("entries", []), tz=tz)
            problems = verify_plan_shape(plan, tz=tz)
            if problems:
                return "计划校验失败：" + "；".join(problems), True
            target = _target_client(store, args)
            archived = store.set_plan(target, plan)
            entries = plan["entries"]
            prefix = ("已归档旧计划「%s」；" % archived) if archived else ""
            return (prefix + "已写入计划「%s」（设备 %s）：%d 周，%d 次训练（%s 起），"
                    "App 可解析 %d 条。手机打开数据共享接口后会自动同步。"
                    % (plan["name"], target, plan["weeks"], len(entries),
                       args.get("startDate"), app_parseable_count(plan))), False

        if name == "get_plan":
            target = _target_client(store, args)
            plan = store.get_plan(target)
            if not plan:
                return ("当前设备（%s）没有计划。用 set_plan 或 set_plan_json 写入。"
                        % target), False
            return json.dumps(plan, ensure_ascii=False, indent=2), False

        if name == "set_plan_json":
            plan = args.get("plan")
            fixed = 0
            if isinstance(plan, dict):
                try:
                    # 兼容 set_plan 风格：有 steps 但没有 customWorkout 的 entry 自动补编码
                    fixed = normalize_plan_for_app(plan)
                except (TypeError, ValueError) as exc:
                    return ("步骤补齐失败：%s。请改用 set_plan，或提供合法的 customWorkout 字符串。"
                            % exc), True
            problems = verify_plan_shape(plan, tz=store.active_tz())
            if problems:
                return "计划校验失败：" + "；".join(problems), True
            target = _target_client(store, args)
            archived = store.set_plan(target, plan)
            prefix = ("已归档旧计划「%s」；" % archived) if archived else ""
            suffix = ("其中 %d 条 steps 已自动编码成 App 原生 customWorkout。" % fixed) if fixed else ""
            return (prefix + "已写入原生计划「%s」（设备 %s），%d 次训练，App 可解析 %d 条。%s"
                    % (plan.get("name", ""), target, len(plan.get("entries", [])),
                       app_parseable_count(plan), suffix)), False

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
                    except (TypeError, ValueError) as exc:
                        return "workouts[%d] 步骤编码失败：%s" % (index, exc), True
                if not workout_snapshot_ok(snapshot):
                    return ("workouts[%d] 缺少可解析的 snapshot（需 "
                            "{\"workout\":{\"steps\":[...]}}）" % index), True
                # 空步骤课表写回手机只会把用户课表清成 0 段，智能体侧直接拒绝
                try:
                    steps_count = len(json.loads(snapshot).get("workout", {}).get("steps") or [])
                except Exception:
                    steps_count = 0
                if steps_count <= 0:
                    return "workouts[%d] 的课表没有步骤，拒绝写入" % index, True
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
                    "target": {"type": "HR_RANGE", "minValue": 150, "maxValue": 170}}]},
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
    check("MCP set_plan", not is_err and "已写入计划" in text, text)
    check("智能体写新计划先归档旧计划",
          "已归档旧计划" in text
          and len(store.snapshot()["plans"]["phone-a"]["archives"]) == archives_before_mcp + 1,
          text[:160])
    check("智能体写计划升级版本",
          store.get_plan_revision("phone-a") == rev_before_mcp + 1)

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
    check("MCP get_plan", not is_err and "MCP 计划" in text)
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
          not is_err and "已自动编码" in text
          and bool(written["entries"][0].get("customWorkout")), text[:180])
    check("补齐后 App 可解析", app_parseable_count(written) == 1)
    check("回执包含可解析条数", "App 可解析 1 条" in text, text[:180])

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
    text, is_err = call_tool(store, "set_workouts", {
        "clientId": ro_client,
        "workouts": [{"id": 14, "name": "空课表",
                      "snapshot": encode_workout_snapshot("空课表", [])}],
    })
    check("智能体写空课表被拒绝", is_err and "没有步骤" in text, text[:160])

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
