"""通知与集成：消息模板、静默时段、合并补发与失败重试。

支持 webhook / slack / email / dingtalk 四类集成。平台离线运行，投递为
**模拟投递**：不发起真实网络请求，而是按集成类型 + 事件确定性地给出
「已送达 / 失败」结果与延迟，并把每次投递记入事件日志。

核心能力
--------
- **消息模板**：每个集成可按事件（``build.finished`` / ``build.passed`` /
  ``build.failed`` / ``quiet.digest``）配置各自的标题与正文模板，支持
  ``${project_name}`` / ``${pass_rate}`` / ``${failed_cases}`` 等占位符；
  同一事件发往多个渠道（集成）时，各渠道用各自的模板。模板可预览。
- **静默时段**：按项目配置每日静默起止时间（支持跨午夜，如 22:00–08:00），
  可选周末全天静默。静默期内的通知不立即投递，而是进入队列，到下一个
  非静默时刻把同一集成积压的多条消息**合并成一条汇总消息**一次性补发。
- **失败重试**：投递失败自动按退避计划重试（默认 10s / 60s，最多 3 次），
  超过次数标记为 ``dead`` 并保留错误信息，仍可在页面上手动重试。
- **留痕**：入队（``deferred``）、每次尝试（``delivered`` / ``failed``）、
  合并补发（``quiet.digest``）都写入 ``notify_events``，共享 ``delivery_key``。

集成只投递它订阅的事件（``events`` 字段）。
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import re
import time
from collections import defaultdict
from typing import Optional

from .models import INTEGRATION_TYPES, new_id

# 各类型的展示名
_TYPE_LABEL = {
    "webhook": "Webhook",
    "slack": "Slack",
    "email": "Email",
    "dingtalk": "钉钉",
}

# 构建事件（集成可订阅）
NOTIFY_EVENTS = ["build.finished", "build.passed", "build.failed"]
# 可配置模板的事件（多出静默补发汇总模板）
TEMPLATE_EVENTS = NOTIFY_EVENTS + ["quiet.digest"]

EVENT_LABELS = {
    "build.finished": "构建结束",
    "build.passed": "构建成功",
    "build.failed": "构建失败",
    "quiet.digest": "静默补发汇总",
    "test": "测试投递",
}

# 模板占位符说明（前端编辑模板时展示）
PLACEHOLDERS = [
    ("${project_name}", "项目名称"),
    ("${build_id}", "构建编号"),
    ("${status}", "构建状态（passed / failed / cancelled / error）"),
    ("${passed}", "通过用例数"),
    ("${total}", "用例总数"),
    ("${pass_rate}", "通过率（百分数，如 87.5）"),
    ("${duration}", "构建耗时（秒）"),
    ("${failed_cases}", "失败用例名（多个换行，无失败时为「无」）"),
    ("${event}", "事件类型"),
]

# 预览用的样例数据（保证所有占位符都有非空、直观的取值）
SAMPLE_CONTEXT = {
    "event": "build.failed",
    "project_name": "演示项目 · 测试与CI",
    "build_id": "build_20261007_ab12cd",
    "status": "failed",
    "passed": 7,
    "total": 8,
    "pass_rate": 87.5,
    "duration": 42.5,
    "failed_cases": "登录接口\n用户列表查询",
}

# 各渠道默认模板。同一事件在不同渠道下措辞 / 格式刻意做出差异，
# 体现「同一事件，每个渠道用各自的模板」。
DEFAULT_TEMPLATES: dict[str, dict[str, dict[str, str]]] = {
    "webhook": {
        "build.finished": {
            "title": "构建结束 · ${project_name}",
            "body": (
                "构建 ${build_id} 已结束\n"
                "状态：${status}\n"
                "通过率：${pass_rate}%（${passed}/${total}）\n"
                "耗时：${duration}s\n"
                "失败用例：\n${failed_cases}"
            ),
        },
        "build.passed": {
            "title": "构建成功 · ${project_name}",
            "body": (
                "构建 ${build_id} 全部通过 ✅\n"
                "通过率：${pass_rate}%（${passed}/${total}）\n"
                "耗时：${duration}s"
            ),
        },
        "build.failed": {
            "title": "构建失败 · ${project_name}",
            "body": (
                "构建 ${build_id} 失败 ❌\n"
                "通过率：${pass_rate}%（${passed}/${total}）\n"
                "耗时：${duration}s\n"
                "失败用例：\n${failed_cases}"
            ),
        },
        "quiet.digest": {
            "title": "静默期补发 · ${project_name}（${count} 条）",
            "body": "静默时段内共有 ${count} 条通知，现合并补发：\n\n${messages}",
        },
        "test": {
            "title": "测试通知 · ${project_name}",
            "body": "这是一条来自测试与CI平台的测试通知。",
        },
    },
    "slack": {
        "build.finished": {
            "title": "🔔 *${project_name}* 构建结束（${status}）",
            "body": (
                "• 构建：`${build_id}`\n"
                "• 通过率：${pass_rate}%（${passed}/${total}）\n"
                "• 耗时：${duration}s\n"
                "• 失败用例：\n```\n${failed_cases}\n```"
            ),
        },
        "build.passed": {
            "title": "✅ *${project_name}* 构建通过",
            "body": (
                "构建 `${build_id}` 全部通过 🎉\n"
                "通过率：${pass_rate}%（${passed}/${total}），耗时 ${duration}s"
            ),
        },
        "build.failed": {
            "title": "❌ *${project_name}* 构建失败",
            "body": (
                "构建 `${build_id}` 失败，请相关同学关注：\n"
                "通过率：${pass_rate}%（${passed}/${total}），耗时 ${duration}s\n"
                "失败用例：\n```\n${failed_cases}\n```"
            ),
        },
        "quiet.digest": {
            "title": "📥 静默期补发 · *${project_name}*（${count} 条）",
            "body": "夜间静默时段积压了 ${count} 条通知，汇总如下：\n\n${messages}",
        },
        "test": {
            "title": "🔔 *${project_name}* 测试通知",
            "body": "Slack 集成连通性测试：如果你看到这条消息，说明配置正常。",
        },
    },
    "email": {
        "build.finished": {
            "title": "[CI] ${project_name} 构建结束（${status}，通过率 ${pass_rate}%）",
            "body": (
                "您好，\n\n"
                "构建 ${build_id} 已结束。\n\n"
                "当前状态：${status}\n"
                "通过情况：${passed}/${total}（${pass_rate}%）\n"
                "执行耗时：${duration}s\n"
                "失败用例：\n${failed_cases}\n\n"
                "—— 自动化测试与持续集成平台"
            ),
        },
        "build.passed": {
            "title": "[CI] ${project_name} 构建成功（通过率 ${pass_rate}%）",
            "body": (
                "您好，\n\n构建 ${build_id} 全部通过。\n\n"
                "通过情况：${passed}/${total}（${pass_rate}%）\n"
                "执行耗时：${duration}s\n\n"
                "—— 自动化测试与持续集成平台"
            ),
        },
        "build.failed": {
            "title": "[CI][告警] ${project_name} 构建失败（通过率 ${pass_rate}%）",
            "body": (
                "您好，\n\n构建 ${build_id} 失败，请及时处理。\n\n"
                "通过情况：${passed}/${total}（${pass_rate}%）\n"
                "执行耗时：${duration}s\n"
                "失败用例：\n${failed_cases}\n\n"
                "—— 自动化测试与持续集成平台"
            ),
        },
        "quiet.digest": {
            "title": "[CI] ${project_name} 静默期通知汇总（${count} 条）",
            "body": (
                "您好，\n\n以下消息在静默时段被暂时保留，现统一补发：\n\n"
                "${messages}\n\n—— 自动化测试与持续集成平台"
            ),
        },
        "test": {
            "title": "[CI] ${project_name} 通知测试",
            "body": "这是一封测试邮件，用于验证 Email 集成配置。",
        },
    },
    "dingtalk": {
        "build.finished": {
            "title": "【构建通知】${project_name} 构建${status}",
            "body": (
                "### 构建${status}：${project_name}\n"
                "- 构建编号：${build_id}\n"
                "- 通过率：${pass_rate}%（${passed}/${total}）\n"
                "- 耗时：${duration}s\n"
                "- 失败用例：\n\n${failed_cases}"
            ),
        },
        "build.passed": {
            "title": "【构建成功】${project_name} 全部通过",
            "body": (
                "### ✅ 构建成功：${project_name}\n"
                "- 构建编号：${build_id}\n"
                "- 通过率：${pass_rate}%（${passed}/${total}）\n"
                "- 耗时：${duration}s"
            ),
        },
        "build.failed": {
            "title": "【构建失败】${project_name} 请及时处理",
            "body": (
                "### ❌ 构建失败：${project_name}\n"
                "- 构建编号：${build_id}\n"
                "- 通过率：${pass_rate}%（${passed}/${total}）\n"
                "- 耗时：${duration}s\n"
                "- 失败用例：\n\n${failed_cases}"
            ),
        },
        "quiet.digest": {
            "title": "【静默补发】${project_name}（${count} 条）",
            "body": "### 📥 静默时段通知汇总（${count} 条）\n\n${messages}",
        },
        "test": {
            "title": "【测试】${project_name} 钉钉通知",
            "body": "### 钉钉机器人测试\n若收到此消息，说明集成配置正常。",
        },
    },
}

# 自动重试：首次尝试 + 2 次重试；间隔与次数可在构造时覆盖
MAX_ATTEMPTS = 3
RETRY_BACKOFF = (10.0, 60.0)  # 第 2、3 次尝试距上次的秒数

_PLACEHOLDER_RE = re.compile(r"\$\{([^}]+)\}")
_MISSING = object()


def _seeded_int(*parts) -> int:
    h = hashlib.md5("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def _lookup(context: dict, path: str):
    """按 ``a.b.0`` 路径在嵌套 dict / list 中取值，缺失返回哨兵。"""
    cur: object = context
    for part in path.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return _MISSING
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return _MISSING
    return cur


def render_template_text(text: str, context: dict) -> str:
    """渲染模板字符串；未提供值的占位符保持原样，便于发现笔误。"""
    if not text:
        return ""

    def _sub(match: re.Match) -> str:
        value = _lookup(context, match.group(1).strip())
        return match.group(0) if value is _MISSING else str(value)

    return _PLACEHOLDER_RE.sub(_sub, str(text))


def _parse_hhmm(value: str) -> Optional[tuple[int, int]]:
    try:
        hour_str, minute_str = str(value).split(":", 1)
        hour, minute = int(hour_str), int(minute_str)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except (ValueError, AttributeError):
        pass
    return None


def quiet_state(settings: Optional[dict], at: _dt.datetime) -> tuple[bool, Optional[float]]:
    """计算给定时刻是否处于静默时段。

    返回 ``(是否静默, 补发时刻时间戳)``。``start == end`` 视为全天静默；
    ``start > end``（如 22:00–08:00）视为跨午夜窗口。
    """
    if not settings or not settings.get("enabled"):
        return False, None

    start = _parse_hhmm(settings.get("start", "22:00"))
    end = _parse_hhmm(settings.get("end", "08:00"))
    if start is None or end is None:
        return False, None
    start_time = _dt.time(*start)
    end_time = _dt.time(*end)

    # 周末全天静默：补发时刻为周一的窗口起点（无窗口则 00:00）
    if settings.get("weekend") and at.weekday() >= 5:
        monday = at.date() + _dt.timedelta(days=7 - at.weekday())
        release = _dt.datetime.combine(monday, start_time)
        return True, release.timestamp()

    now_time = at.time()
    if start_time == end_time:
        # 起止相同 → 全天静默，次日同一时刻解除
        release = _dt.datetime.combine(at.date() + _dt.timedelta(days=1), end_time)
        return True, release.timestamp()
    if start_time < end_time:
        if start_time <= now_time < end_time:
            release = _dt.datetime.combine(at.date(), end_time)
            return True, release.timestamp()
    else:
        # 跨午夜
        if now_time >= start_time:
            release = _dt.datetime.combine(at.date() + _dt.timedelta(days=1), end_time)
            return True, release.timestamp()
        if now_time < end_time:
            release = _dt.datetime.combine(at.date(), end_time)
            return True, release.timestamp()
    return False, None


class NotificationManager:
    """通知与集成管理。"""

    def __init__(self, registry, max_attempts: int = MAX_ATTEMPTS,
                 retry_backoff: tuple[float, ...] = RETRY_BACKOFF):
        self.registry = registry
        self._store = registry.store("integrations")
        self._events = registry.store("notify_events")
        self._queue = registry.store("notify_queue")
        self._settings = registry.store("notify_settings")
        self.max_attempts = max(1, int(max_attempts))
        self.retry_backoff = tuple(retry_backoff)

    # -- 集成 CRUD --------------------------------------------------------
    def create(self, project_id: str, payload: dict) -> dict:
        itype = payload.get("type", "webhook")
        if itype not in INTEGRATION_TYPES:
            itype = "webhook"
        integration = {
            "id": new_id("int"),
            "project_id": project_id,
            "type": itype,
            "name": payload.get("name", _TYPE_LABEL[itype]),
            "enabled": bool(payload.get("enabled", True)),
            "config": payload.get("config") or {},
            "events": payload.get("events") or ["build.finished"],
            "templates": self._normalize_templates(itype, payload.get("templates")),
        }
        self._store.insert(integration)
        return integration

    def list(self, project_id: str) -> list[dict]:
        return self._store.query(where=[("project_id", "eq", project_id)],
                                 order_by="created_at", order="asc")

    def get(self, integration_id: str) -> Optional[dict]:
        return self._store.get(integration_id)

    def update(self, integration_id: str, patch: dict) -> Optional[dict]:
        integration = self._store.get(integration_id)
        if integration is None:
            return None
        allowed = {k: patch[k] for k in
                   ("name", "type", "enabled", "config", "events") if k in patch}
        if "templates" in patch:
            itype = patch.get("type") or integration.get("type") or "webhook"
            allowed["templates"] = self._normalize_templates(
                itype, patch["templates"])
        elif "type" in patch and patch["type"] in INTEGRATION_TYPES:
            # 渠道变了：旧渠道的自定义模板不再适用，回退到新渠道默认模板
            allowed["templates"] = {}
        return self._store.update(integration_id, allowed)

    def delete(self, integration_id: str) -> bool:
        return self._store.delete(integration_id)

    @staticmethod
    def _normalize_templates(itype: str, templates) -> dict:
        """只保留已知事件且含非空 title / body 的覆盖项。"""
        if not isinstance(templates, dict):
            return {}
        out: dict[str, dict[str, str]] = {}
        for event, tpl in templates.items():
            if event not in TEMPLATE_EVENTS or not isinstance(tpl, dict):
                continue
            title = str(tpl.get("title") or "").strip()
            body = str(tpl.get("body") or "").strip()
            if title or body:
                out[event] = {"title": title, "body": body}
        return out

    # -- 模板 --------------------------------------------------------------
    def get_template(self, integration: dict, event: str) -> dict[str, str]:
        """集成自定义模板优先，否则取该渠道的默认模板。"""
        custom = (integration.get("templates") or {}).get(event)
        default = DEFAULT_TEMPLATES.get(integration.get("type", "webhook"),
                                        DEFAULT_TEMPLATES["webhook"])
        base = default.get(event) or default["build.finished"]
        if not custom:
            return {"title": base["title"], "body": base["body"], "custom": False}
        return {
            "title": custom.get("title") or base["title"],
            "body": custom.get("body") or base["body"],
            "custom": True,
        }

    def _build_context(self, project_id: str, event: str, payload: dict) -> dict:
        context = {
            "event": event,
            "project_name": project_id,
            "build_id": "—",
            "status": "—",
            "passed": 0,
            "total": 0,
            "pass_rate": 0,
            "duration": 0,
            "failed_cases": "无",
        }
        context.update(payload or {})
        project = self.registry.store("projects").get(project_id)
        if project and not (payload or {}).get("project_name"):
            context["project_name"] = project.get("name", project_id)
        return context

    def render_integration(self, integration: dict, event: str,
                           context: dict) -> dict[str, str]:
        tpl = self.get_template(integration, event)
        return {
            "title": render_template_text(tpl["title"], context),
            "body": render_template_text(tpl["body"], context),
        }

    def render_preview(self, itype: str, event: str,
                       title: str = "", body: str = "",
                       context: Optional[dict] = None) -> dict[str, str]:
        """模板预览：未填的字段回退到该渠道默认模板，用样例数据渲染。"""
        if itype not in INTEGRATION_TYPES:
            itype = "webhook"
        default = DEFAULT_TEMPLATES[itype].get(
            event, DEFAULT_TEMPLATES[itype]["build.finished"])
        ctx = dict(SAMPLE_CONTEXT)
        ctx.update(context or {})
        return {
            "title": render_template_text(title or default["title"], ctx),
            "body": render_template_text(body or default["body"], ctx),
        }

    # -- 静默时段设置 ------------------------------------------------------
    def default_settings(self) -> dict:
        return {"enabled": False, "start": "22:00", "end": "08:00",
                "weekend": False}

    def get_settings(self, project_id: str) -> dict:
        doc = self._settings.get(f"quiet_{project_id}")
        settings = self.default_settings()
        if doc:
            settings.update({k: doc[k] for k in
                             ("enabled", "start", "end", "weekend") if k in doc})
        settings["project_id"] = project_id
        return settings

    def save_settings(self, project_id: str, patch: dict) -> dict:
        current = self.get_settings(project_id)
        enabled = bool(patch.get("enabled", current["enabled"]))
        weekend = bool(patch.get("weekend", current["weekend"]))
        start = patch.get("start", current["start"])
        end = patch.get("end", current["end"])
        if _parse_hhmm(start) is None or _parse_hhmm(end) is None:
            raise ValueError("时间格式应为 HH:MM，如 22:00")
        doc = {"id": f"quiet_{project_id}", "project_id": project_id,
               "enabled": enabled, "start": start, "end": end,
               "weekend": weekend, "updated_at": time.time()}
        existing = self._settings.get(doc["id"])
        if existing:
            self._settings.update(doc["id"], doc)
        else:
            doc["created_at"] = time.time()
            self._settings.insert(doc)
        return self.get_settings(project_id)

    def settings_state(self, project_id: str, at: Optional[float] = None) -> dict:
        """设置 + 当前是否静默 / 何时补发（供前端展示）。"""
        settings = self.get_settings(project_id)
        moment = _dt.datetime.fromtimestamp(at if at is not None else time.time())
        in_quiet, release_at = quiet_state(settings, moment)
        settings["in_quiet"] = in_quiet
        settings["release_at"] = release_at
        return settings

    # -- 投递（模拟） ------------------------------------------------------
    def _target_of(self, integration: dict) -> str:
        config = integration.get("config") or {}
        return config.get("url") or config.get("address") or \
            config.get("channel") or "未配置目标"

    def _deliver(self, integration: dict, event: str, payload: dict) -> dict:
        target = self._target_of(integration)
        latency = 8 + _seeded_int(integration["id"], event,
                                  payload.get("build_id"),
                                  payload.get("digest", False)) % 120
        failed = not target or target == "未配置目标" or \
            (integration.get("config") or {}).get("fail", False)
        return {
            "status": "failed" if failed else "delivered",
            "recipient": target,
            "latency_ms": latency,
            "error": "目标地址未配置" if target == "未配置目标"
            else ("模拟投递失败（fail 标志）" if failed else None),
        }

    def _record(self, integration: dict, event: str, payload: dict,
                outcome: dict, *, title: str = "", body: str = "",
                status: Optional[str] = None, delivery_key: Optional[str] = None,
                attempt: int = 1, deferred: bool = False, digest: bool = False,
                queued_count: Optional[int] = None,
                queue_id: Optional[str] = None,
                release_at: Optional[float] = None) -> dict:
        record = {
            "id": new_id("evt"),
            "project_id": integration.get("project_id"),
            "integration_id": integration["id"],
            "integration_name": integration.get("name"),
            "type": integration.get("type"),
            "event": event,
            "status": status or outcome["status"],
            "recipient": outcome.get("recipient", "—"),
            "latency_ms": outcome.get("latency_ms", 0),
            "error": outcome.get("error"),
            "payload": payload,
            "title": title,
            "body": body,
            "delivery_key": delivery_key,
            "attempt": attempt,
            "deferred": deferred,
            "digest": digest,
            "queued_count": queued_count,
            "queue_id": queue_id,
            "release_at": release_at,
            "created_at": time.time(),
        }
        self._events.insert(record)
        return record

    def _enqueue(self, integration: dict, event: str, payload: dict,
                 title: str, body: str, *, release_at: float,
                 deferred: bool, digest: bool, attempts: int,
                 delivery_key: str, error: Optional[str] = None) -> dict:
        item = {
            "id": new_id("nq"),
            "project_id": integration.get("project_id"),
            "integration_id": integration["id"],
            "integration_name": integration.get("name"),
            "type": integration.get("type"),
            "event": event,
            "payload": payload,
            "title": title,
            "body": body,
            "deferred": deferred,
            "digest": digest,
            "release_at": release_at,
            "attempts": attempts,
            "max_attempts": self.max_attempts,
            "last_error": error,
            "last_attempt_at": None,
            "delivery_key": delivery_key,
            "status": "pending",
        }
        self._queue.insert(item)
        return item

    def fire(self, project_id: str, event: str, payload: dict,
             at: Optional[float] = None) -> list[dict]:
        """向订阅了该事件的所有启用集成投递通知，返回事件记录列表。

        静默时段内只入队并记一条 ``deferred`` 日志；投递失败则入重试队列。
        """
        at = time.time() if at is None else at
        moment = _dt.datetime.fromtimestamp(at)
        settings = self.get_settings(project_id)
        in_quiet, release_at = quiet_state(settings, moment)
        context = self._build_context(project_id, event, payload)

        records: list[dict] = []
        for integration in self.list(project_id):
            if not integration.get("enabled", True):
                continue
            if event not in (integration.get("events") or ["build.finished"]):
                continue
            rendered = self.render_integration(integration, event, context)
            delivery_key = new_id("dlv")

            if in_quiet:
                item = self._enqueue(
                    integration, event, payload, rendered["title"],
                    rendered["body"], release_at=release_at or at,
                    deferred=True, digest=False, attempts=0,
                    delivery_key=delivery_key)
                records.append(self._record(
                    integration, event, payload,
                    {"status": "deferred", "recipient": "静默期保留",
                     "latency_ms": 0},
                    title=rendered["title"], body=rendered["body"],
                    status="deferred", delivery_key=delivery_key,
                    deferred=True, queue_id=item["id"],
                    release_at=release_at))
                continue

            records.append(self._attempt_delivery(
                integration, event, payload, rendered["title"],
                rendered["body"], at=at, delivery_key=delivery_key,
                attempt=1, digest=False))
        return records

    def _schedule_next_attempt(self, item_id: str, attempt: int, at: float,
                               error: Optional[str], final: bool) -> float | None:
        """失败后更新现有队列项：还有次数则排定下一次，否则标记 dead。"""
        if final:
            self._queue.update(item_id, {"status": "dead", "attempts": attempt,
                                         "last_attempt_at": at,
                                         "last_error": error})
            return None
        wait = self.retry_backoff[min(attempt - 1, len(self.retry_backoff) - 1)]
        release_at = at + wait
        self._queue.update(item_id, {"attempts": attempt, "last_attempt_at": at,
                                     "last_error": error, "release_at": release_at})
        return release_at

    def _attempt_delivery(self, integration: dict, event: str, payload: dict,
                          title: str, body: str, *, at: float,
                          delivery_key: str, attempt: int,
                          digest: bool, queued_count: Optional[int] = None,
                          queue_item: Optional[dict] = None,
                          max_attempts: Optional[int] = None) -> dict:
        """执行一次投递并写日志。

        失败时：若带 ``queue_item``（补发 / 重试路径）就原地更新该队列项，
        否则（``fire`` 首次投递路径）新建一条重试队列项。
        """
        max_attempts = self.max_attempts if max_attempts is None else max_attempts
        outcome = self._deliver(integration, event, payload)
        queue_id: Optional[str] = None
        release_at: Optional[float] = None
        if outcome["status"] == "failed":
            final = attempt >= max_attempts
            error = outcome.get("error")
            if queue_item is not None:
                queue_id = queue_item["id"]
                release_at = self._schedule_next_attempt(
                    queue_item["id"], attempt, at, error, final)
            elif not final:
                # fire 首次失败：新建待重试队列项
                wait = self.retry_backoff[min(attempt - 1,
                                              len(self.retry_backoff) - 1)]
                release_at = at + wait
                item = self._enqueue(
                    integration, event, payload, title, body,
                    release_at=release_at, deferred=False, digest=digest,
                    attempts=attempt, delivery_key=delivery_key, error=error)
                queue_id = item["id"]
            else:
                # max_attempts == 1：直接留一条 dead 记录
                item = self._enqueue(
                    integration, event, payload, title, body,
                    release_at=None, deferred=False, digest=digest,
                    attempts=attempt, delivery_key=delivery_key, error=error)
                self._queue.update(item["id"], {"status": "dead"})
                queue_id = item["id"]

        return self._record(
            integration, event, payload, outcome,
            title=title, body=body, delivery_key=delivery_key,
            attempt=attempt, deferred=False, digest=digest,
            queued_count=queued_count, queue_id=queue_id,
            release_at=release_at)

    # -- 补发与重试 --------------------------------------------------------
    def _digest_messages(self, items: list[dict]) -> str:
        lines = []
        for i, item in enumerate(items, 1):
            stamp = _dt.datetime.fromtimestamp(
                item.get("created_at", time.time())).strftime("%m-%d %H:%M")
            label = EVENT_LABELS.get(item.get("event"), item.get("event", ""))
            body = str(item.get("body") or "").replace("\n", "\n   ")
            lines.append(f"{i}. [{stamp}] {label}｜{item.get('title', '')}\n"
                         f"   {body}")
        return "\n".join(lines)

    def _flush_quiet_group(self, integration_id: str, items: list[dict],
                           at: float) -> Optional[dict]:
        """把同一集成静默期积压的消息合并成一条汇总消息投递。"""
        items = sorted(items, key=lambda q: q.get("created_at", 0))
        integration = self.get(integration_id)
        project_id = items[0].get("project_id")
        payload = {"digest": True, "count": len(items),
                   "project_id": project_id, "build_id": None}

        if integration is None or not integration.get("enabled", True):
            reason = "集成已删除" if integration is None else "集成已停用"
            for item in items:
                self._queue.update(item["id"],
                                   {"status": "dead", "last_error": reason})
            ghost = integration or {"id": integration_id,
                                    "project_id": project_id,
                                    "name": items[0].get("integration_name"),
                                    "type": items[0].get("type", "webhook")}
            return self._record(
                ghost, "quiet.digest", payload,
                {"status": "failed", "recipient": "—", "latency_ms": 0,
                 "error": reason},
                title="静默补发失败", status="failed",
                queued_count=len(items), attempt=1, digest=True)

        project = self.registry.store("projects").get(project_id)
        context = {
            "event": "quiet.digest",
            "project_name": (project or {}).get("name", project_id),
            "count": len(items),
            "messages": self._digest_messages(items),
        }
        rendered = self.render_integration(integration, "quiet.digest", context)
        record = self._attempt_delivery(
            integration, "quiet.digest", payload,
            rendered["title"], rendered["body"], at=at,
            delivery_key=new_id("dlv"), attempt=1, digest=True,
            queued_count=len(items))

        # 原始消息无论汇总投递成败都已被消费（成功=已补发；失败=转为汇总重试项）
        for item in items:
            self._queue.update(item["id"], {"status": "merged"})
        return record

    def _flush_retry(self, item: dict, at: float) -> Optional[dict]:
        integration = self.get(item["integration_id"])
        if integration is None or not integration.get("enabled", True):
            reason = "集成已删除" if integration is None else "集成已停用"
            self._queue.update(item["id"],
                               {"status": "dead", "last_error": reason})
            ghost = integration or {
                "id": item["integration_id"],
                "project_id": item.get("project_id"),
                "name": item.get("integration_name"),
                "type": item.get("type", "webhook")}
            return self._record(
                ghost, item.get("event", "build.finished"),
                item.get("payload") or {},
                {"status": "failed", "recipient": "—", "latency_ms": 0,
                 "error": reason},
                title=item.get("title", ""), body=item.get("body", ""),
                status="failed", delivery_key=item.get("delivery_key"),
                attempt=item.get("attempts", 0) + 1,
                digest=bool(item.get("digest")),
                queued_count=(item.get("payload") or {}).get("count"))

        attempt = int(item.get("attempts", 0)) + 1
        max_attempts = int(item.get("max_attempts") or self.max_attempts)
        record = self._attempt_delivery(
            integration, item.get("event", "build.finished"),
            item.get("payload") or {}, item.get("title", ""),
            item.get("body", ""), at=at,
            delivery_key=item.get("delivery_key") or new_id("dlv"),
            attempt=attempt, digest=bool(item.get("digest")),
            queued_count=(item.get("payload") or {}).get("count"),
            queue_item=item, max_attempts=max_attempts)

        fresh = self._queue.get(item["id"])
        if record["status"] == "delivered":
            self._queue.update(item["id"], {"status": "sent",
                                            "attempts": attempt,
                                            "last_attempt_at": at})
        elif fresh and fresh.get("status") == "dead":
            pass  # _attempt_delivery 已标记 dead
        return record

    def flush_due(self, at: Optional[float] = None) -> dict:
        """补发 / 重试所有到期队列项，由调度器定时循环与手动接口调用。"""
        at = time.time() if at is None else at
        due = [q for q in self._queue.all()
               if q.get("status") == "pending"
               and q.get("release_at") is not None
               and q["release_at"] <= at]
        fresh = [q for q in due if q.get("deferred")]
        retries = [q for q in due if not q.get("deferred")]

        groups: dict[str, list[dict]] = defaultdict(list)
        for item in fresh:
            groups[item["integration_id"]].append(item)

        records: list[dict] = []
        for integration_id, items in groups.items():
            record = self._flush_quiet_group(integration_id, items, at)
            if record:
                records.append(record)
        for item in sorted(retries, key=lambda q: q.get("release_at", 0)):
            records.append(self._flush_retry(item, at))

        return {"due": len(due), "digests": len(groups),
                "retries": len(retries), "records": records}

    def retry_item(self, queue_id: str, at: Optional[float] = None) -> dict:
        """手动立即重试一条队列消息（dead 也可，额外给予重试机会）。"""
        at = time.time() if at is None else at
        item = self._queue.get(queue_id)
        if item is None:
            return {"error": "队列消息不存在"}
        if item.get("status") == "sent":
            return {"error": "该消息已送达，无需重试"}

        integration = self.get(item["integration_id"])
        if integration is None:
            return {"error": "集成已删除，无法重试"}
        if not integration.get("enabled", True):
            return {"error": "集成已停用，请先启用"}

        # 手动重试：静默积压项按单条立即发送；dead 项放宽次数上限
        if item.get("deferred"):
            self._queue.update(item["id"], {"deferred": False})
            item["deferred"] = False
        if item.get("status") == "dead":
            item["max_attempts"] = int(item.get("attempts", 0)) + 2
            self._queue.update(item["id"],
                               {"max_attempts": item["max_attempts"],
                                "status": "pending"})
        record = self._flush_retry(item, at)
        return record

    def queue(self, project_id: str, limit: int = 100) -> list[dict]:
        items = self._queue.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="desc",
                                  limit=limit)
        return [q for q in items if q.get("status") in ("pending", "dead")]

    def send_test(self, integration_id: str) -> dict:
        """显式测试投递：绕过静默时段、不进重试队列。"""
        integration = self.get(integration_id)
        if integration is None:
            return {"error": "集成不存在"}
        payload = {"test": True, "project_id": integration.get("project_id")}
        context = self._build_context(integration["project_id"], "test", payload)
        rendered = self.render_integration(integration, "test", context)
        outcome = self._deliver(integration, "test", payload)
        record = self._record(integration, "test", payload, outcome,
                              title=rendered["title"], body=rendered["body"],
                              delivery_key=new_id("dlv"), attempt=1)
        record["integration"] = integration
        return record

    def events(self, project_id: str, limit: int = 100) -> list[dict]:
        return self._events.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="desc",
                                  limit=limit)
