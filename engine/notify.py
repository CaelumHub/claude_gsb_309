"""通知与集成：可定制模板、静默时段、合并补发、失败重试与留痕。

支持 webhook / slack / email / dingtalk 四类集成。平台离线运行，投递为
**模拟投递**：不发起真实网络请求，而是按集成类型 + 事件确定性地给出
「已送达 / 失败」结果与延迟。

能力一览
--------
1. **消息模板**：每个集成对每类事件（``build.finished`` /
   ``build.passed`` / ``build.failed``）各持有一份 ``title`` + ``body``
   模板，支持 ``${project_name}``、``${pass_rate}``、``${failed_cases}``
   等占位符；未自定义时按渠道使用内置默认模板——同一事件发往多个渠道，
   各渠道渲染各自的模板。

2. **静默时段**：每个集成可配置 ``quiet_hours``（如 22:00-08:00，支持
   跨午夜）。静默期内命中的通知不立即投递，而是入队（``notify_queue``），
   事件日志留一条 ``queued`` 记录；静默结束后的下一次调度把同一集成的
   积压消息**合并成一条 digest 补发**。

3. **失败重试**：投递失败按退避计划自动重试（默认最多 3 次），每次尝试
   （成功与否、延迟、错误原因）都追加到事件记录的 ``attempts`` 中全程
   留痕；日志页可对最终失败 / 重试中的消息手动再试，或对静默队列立即
   强制补发。

事件类型：
- ``build.finished``  构建结束（成功或失败都发，若订阅）
- ``build.passed``    构建成功
- ``build.failed``    构建失败（含 error / cancelled）
- ``test``            测试投递（用示例数据渲染模板，且**绕过静默**）
- ``digest``          静默结束后的合并补发（系统内部事件）

集成只投递它订阅的事件（``events`` 字段）。
"""

from __future__ import annotations

import datetime
import hashlib
import json
import re
import threading
import time
from typing import Optional

from .models import INTEGRATION_TYPES, new_id

# 各类型的默认目标，仅用于展示（不真实发送）
_TYPE_LABEL = {
    "webhook": "Webhook",
    "slack": "Slack",
    "email": "Email",
    "dingtalk": "钉钉",
}

# 可订阅的构建事件
EVENTS = ["build.finished", "build.passed", "build.failed"]

EVENT_LABELS = {
    "build.finished": "构建结束",
    "build.passed": "构建成功",
    "build.failed": "构建失败",
    "test": "测试投递",
    "digest": "静默补发",
}

_STATUS_LABELS = {
    "passed": "通过",
    "failed": "失败",
    "error": "异常",
    "cancelled": "已取消",
    "running": "运行中",
}

_TRIGGER_LABELS = {
    "manual": "手动触发",
    "schedule": "定时触发",
    "webhook": "Webhook 触发",
    "ci": "CI 触发",
    "auto_seed": "初始化触发",
}

# 模板可用占位符目录（name 供渲染，example 供「预览」示例数据）
PLACEHOLDERS = [
    {"name": "project_name", "label": "项目名", "example": "演示项目 · 测试与CI"},
    {"name": "suite_name", "label": "套件名", "example": "冒烟测试套件"},
    {"name": "env_name", "label": "环境名", "example": "staging 预发环境"},
    {"name": "build_id", "label": "构建 ID", "example": "build_1791298319336_efd70a"},
    {"name": "status", "label": "构建状态（英文）", "example": "failed"},
    {"name": "status_label", "label": "构建状态（中文）", "example": "失败"},
    {"name": "pass_rate", "label": "通过率（%）", "example": 91.7},
    {"name": "passed", "label": "通过用例数", "example": 11},
    {"name": "total", "label": "用例总数", "example": 12},
    {"name": "failed", "label": "失败用例数", "example": 1},
    {"name": "failed_cases", "label": "失败用例名列表", "example": "登录接口、下单接口"},
    {"name": "duration", "label": "构建耗时（秒）", "example": 42.5},
    {"name": "trigger_label", "label": "触发方式", "example": "定时触发"},
    {"name": "count", "label": "合并补发条数（仅 digest 模板）", "example": 3},
    {"name": "items", "label": "合并消息明细（仅 digest 模板）", "example": "1. [03:07] 构建失败 …"},
    {"name": "start_time", "label": "静默窗口最早消息时间", "example": "07 03:07"},
    {"name": "end_time", "label": "静默窗口最晚消息时间", "example": "07 06:40"},
]

# 预览接口使用的示例构建数据
SAMPLE_PAYLOAD = {
    "project_id": "proj_demo",
    "project_name": "演示项目 · 测试与CI",
    "suite_name": "冒烟测试套件",
    "env_name": "staging 预发环境",
    "build_id": "build_20261007_abcdef",
    "status": "failed",
    "passed": 11,
    "total": 12,
    "failed": 1,
    "pass_rate": 91.7,
    "duration": 42.5,
    "failed_cases": ["登录接口", "下单接口"],
    "trigger": "schedule",
}

# 投递失败后的重试退避（秒）：第 1 次失败后 30s 再试，第 2 次后 120s
RETRY_DELAYS = [30, 120, 300]
DEFAULT_MAX_ATTEMPTS = 3

# 各渠道的内置默认模板（title / body），每类事件一份
_BODY_COMMON = (
    "项目：${project_name}\n"
    "套件/环境：${suite_name} / ${env_name}\n"
    "构建：${build_id}\n"
    "状态：${status_label}\n"
    "通过率：${pass_rate}%（${passed}/${total}，失败 ${failed}）\n"
    "失败用例：${failed_cases}\n"
    "耗时：${duration}s\n"
    "触发方式：${trigger_label}"
)

DEFAULT_TEMPLATES: dict[str, dict[str, dict[str, str]]] = {
    "webhook": {
        "build.finished": {
            "title": "【CI】${project_name} 构建${status_label}",
            "body": _BODY_COMMON,
        },
        "build.passed": {
            "title": "【CI】${project_name} 构建通过（${pass_rate}%）",
            "body": _BODY_COMMON,
        },
        "build.failed": {
            "title": "【CI】${project_name} 构建失败：${failed_cases}",
            "body": _BODY_COMMON,
        },
    },
    "slack": {
        "build.finished": {
            "title": ":hammer_and_wrench: *${project_name}* 构建${status_label}（${pass_rate}%）",
            "body": "*${project_name}* 构建结果\n"
                    "• 状态：${status_label}\n"
                    "• 通过率：${pass_rate}%（${passed}/${total}）\n"
                    "• 失败用例：${failed_cases}\n"
                    "• 耗时：${duration}s（${trigger_label}）\n"
                    "• 构建：`${build_id}`",
        },
        "build.passed": {
            "title": ":white_check_mark: *${project_name}* 构建通过（${pass_rate}%）",
            "body": "*${project_name}* 全部用例通过 :tada:\n"
                    "• 通过率：${pass_rate}%（${passed}/${total}）\n"
                    "• 耗时：${duration}s\n"
                    "• 构建：`${build_id}`",
        },
        "build.failed": {
            "title": ":x: *${project_name}* 构建失败（${pass_rate}%）",
            "body": "*${project_name}* 有 ${failed} 条用例失败\n"
                    "• 失败用例：${failed_cases}\n"
                    "• 通过率：${pass_rate}%（${passed}/${total}）\n"
                    "• 环境：${env_name}\n"
                    "• 构建：`${build_id}`",
        },
    },
    "email": {
        "build.finished": {
            "title": "【构建通知】${project_name} 构建${status_label} · ${pass_rate}%",
            "body": _BODY_COMMON,
        },
        "build.passed": {
            "title": "【构建通过】${project_name} 全部用例通过",
            "body": _BODY_COMMON,
        },
        "build.failed": {
            "title": "【构建失败】${project_name} 有 ${failed} 条用例失败",
            "body": _BODY_COMMON,
        },
    },
    "dingtalk": {
        "build.finished": {
            "title": "🔔 ${project_name} 构建${status_label}（${passed}/${total}）",
            "body": "### 构建${status_label}通知\n\n"
                    "- **项目**：${project_name}\n"
                    "- **状态**：${status_label}\n"
                    "- **通过率**：${pass_rate}%（${passed}/${total}）\n"
                    "- **失败用例**：${failed_cases}\n"
                    "- **耗时**：${duration}s（${trigger_label}）\n"
                    "- **构建**：${build_id}",
        },
        "build.passed": {
            "title": "✅ ${project_name} 构建通过，通过率 ${pass_rate}%",
            "body": "### 构建通过通知\n\n"
                    "- **项目**：${project_name}\n"
                    "- **通过率**：${pass_rate}%（${passed}/${total}）\n"
                    "- **耗时**：${duration}s\n"
                    "- **构建**：${build_id}",
        },
        "build.failed": {
            "title": "❌ ${project_name} 构建失败：${failed_cases}",
            "body": "### 构建失败通知\n\n"
                    "- **项目**：${project_name}\n"
                    "- **失败用例**：${failed_cases}\n"
                    "- **通过率**：${pass_rate}%（${passed}/${total}）\n"
                    "- **环境**：${env_name}\n"
                    "- **构建**：${build_id}",
        },
    },
}

# 各渠道的合并补发（digest）模板
DIGEST_TEMPLATES = {
    "webhook": {
        "title": "【静默补发】${project_name} · ${count} 条通知",
        "body": "静默时段积压 ${count} 条构建通知（${start_time} ~ ${end_time}）：\n${items}",
    },
    "slack": {
        "title": ":sleeping: *${project_name}* 静默补发 · ${count} 条通知",
        "body": "*静默时段积压 ${count} 条通知*（${start_time} ~ ${end_time}）\n${items}",
    },
    "email": {
        "title": "【静默补发】${project_name} 静默时段 ${count} 条通知汇总",
        "body": "以下为静默时段（${start_time} ~ ${end_time}）积压的 ${count} 条通知：\n\n${items}",
    },
    "dingtalk": {
        "title": "🔕 ${project_name} 静默补发：${count} 条通知",
        "body": "### 静默补发（${count} 条）\n\n> 时段：${start_time} ~ ${end_time}\n\n${items}",
    },
}

_PLACEHOLDER_RE = re.compile(r"\$\{([a-zA-Z_][\w.]*)\}")
_HHMM_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]?\d)$")


# ---------------------------------------------------------------------------
# 模板渲染与静默时段计算（无副作用的纯函数，便于单测）
# ---------------------------------------------------------------------------

def render_template(template: str, context: dict) -> tuple[str, list[str]]:
    """渲染模板。

    返回 ``(渲染后文本, 缺失占位符名列表)``；上下文里没有的占位符保持
    原样 ``${name}`` 保留，方便使用者在预览里发现拼写错误。
    """
    missing: list[str] = []

    def _sub(m: re.Match) -> str:
        key = m.group(1)
        if key not in context or context[key] is None or context[key] == "":
            missing.append(key)
            return m.group(0)
        value = context[key]
        if isinstance(value, (list, dict)):
            value = json.dumps(value, ensure_ascii=False)
        return str(value)

    return _PLACEHOLDER_RE.sub(_sub, template or ""), missing


def build_context(event: str, payload: dict) -> dict:
    """从构建事件 payload 组装模板上下文（缺字段给空串，不报错）。"""
    p = payload or {}
    failed_cases = p.get("failed_cases") or []
    if isinstance(failed_cases, list):
        failed_cases_text = "、".join(str(c) for c in failed_cases) or "无"
    else:
        failed_cases_text = str(failed_cases)
    status = p.get("status") or ""
    return {
        "project_name": p.get("project_name") or p.get("project_id") or "",
        "suite_name": p.get("suite_name") or "",
        "env_name": p.get("env_name") or p.get("env_id") or "",
        "build_id": p.get("build_id") or "",
        "status": status,
        "status_label": _STATUS_LABELS.get(status, status),
        "pass_rate": p.get("pass_rate", ""),
        "passed": p.get("passed", ""),
        "total": p.get("total", ""),
        "failed": p.get("failed", ""),
        "failed_cases": failed_cases_text,
        "duration": p.get("duration", ""),
        "trigger": p.get("trigger") or "",
        "trigger_label": _TRIGGER_LABELS.get(p.get("trigger"), p.get("trigger") or ""),
    }


def parse_hhmm(value: str) -> int:
    """``HH:MM`` → 从零点起的分钟数；非法格式抛 :class:`ValueError`。"""
    m = _HHMM_RE.match((value or "").strip())
    if not m:
        raise ValueError(f"时间格式应为 HH:MM：{value!r}")
    return int(m.group(1)) * 60 + int(m.group(2))


def is_quiet(quiet_hours: Optional[dict], at: Optional[datetime.datetime] = None) -> bool:
    """判断 ``at``（默认现在）是否落在静默时段内。

    支持跨午夜（如 22:00-08:00）：start > end 时表示「从 start 到次日 end」。
    """
    if not quiet_hours or not quiet_hours.get("enabled"):
        return False
    try:
        start = parse_hhmm(quiet_hours.get("start", ""))
        end = parse_hhmm(quiet_hours.get("end", ""))
    except ValueError:
        return False
    if start == end:
        return False
    at = at or datetime.datetime.now()
    cur = at.hour * 60 + at.minute
    if start < end:
        return start <= cur < end
    return cur >= start or cur < end


def next_quiet_end(quiet_hours: Optional[dict],
                   at: Optional[datetime.datetime] = None) -> Optional[float]:
    """静默时段结束时刻（epoch 秒）；未启用 / 配置非法返回 None。"""
    if not is_quiet(quiet_hours, at):
        return None
    at = at or datetime.datetime.now()
    end_min = parse_hhmm(quiet_hours["end"])
    candidate = at.replace(hour=end_min // 60, minute=end_min % 60,
                           second=0, microsecond=0)
    if end_min <= at.hour * 60 + at.minute:
        candidate += datetime.timedelta(days=1)  # 跨午夜，结束点在次日
    return candidate.timestamp()


def _seeded_int(*parts) -> int:
    h = hashlib.md5("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return int(h[:8], 16)


class NotificationManager:
    """通知与集成管理（模板 / 静默 / 合并补发 / 重试）。"""

    def __init__(self, registry):
        self._store = registry.store("integrations")
        self._events = registry.store("notify_events")
        self._queue = registry.store("notify_queue")
        # flush 可能被调度线程与「手动补发」请求并发触发，进程内串行化
        self._flush_lock = threading.RLock()

    # -- 集成 CRUD --------------------------------------------------------
    def create(self, project_id: str, payload: dict) -> dict:
        itype = payload.get("type", "webhook")
        if itype not in INTEGRATION_TYPES:
            itype = "webhook"
        templates = self._validate_templates(payload.get("templates"))
        quiet_hours = self._validate_quiet_hours(payload.get("quiet_hours"))
        max_attempts = self._validate_max_attempts(payload.get("max_attempts"))
        integration = {
            "id": new_id("int"),
            "project_id": project_id,
            "type": itype,
            "name": payload.get("name", _TYPE_LABEL[itype]),
            "enabled": bool(payload.get("enabled", True)),
            "config": payload.get("config") or {},
            "events": payload.get("events") or ["build.finished"],
            "templates": templates,
            "quiet_hours": quiet_hours,
            "max_attempts": max_attempts,
        }
        self._store.insert(integration)
        return integration

    def list(self, project_id: str) -> list[dict]:
        return self._store.query(where=[("project_id", "eq", project_id)],
                                 order_by="created_at", order="asc")

    def get(self, integration_id: str) -> Optional[dict]:
        return self._store.get(integration_id)

    def update(self, integration_id: str, patch: dict) -> Optional[dict]:
        clean = {k: patch[k] for k in
                 ("name", "type", "enabled", "config", "events",
                  "templates", "quiet_hours", "max_attempts") if k in patch}
        if "templates" in clean:
            clean["templates"] = self._validate_templates(clean["templates"])
        if "quiet_hours" in clean:
            clean["quiet_hours"] = self._validate_quiet_hours(clean["quiet_hours"])
        if "max_attempts" in clean:
            clean["max_attempts"] = self._validate_max_attempts(clean["max_attempts"])
        if "type" in clean and clean["type"] not in INTEGRATION_TYPES:
            raise ValueError("不支持的集成类型")
        return self._store.update(integration_id, clean)

    def delete(self, integration_id: str) -> bool:
        # 队列里尚未发出的静默消息不再可能补发，标记过期留痕
        for item in self._queue.query(where=[("integration_id", "eq", integration_id)]):
            if item.get("status") in ("queued", "sending"):
                self._set_queue(item["id"], "expired")
                if item.get("event_id"):
                    self._events.update(item["event_id"],
                                        {"status": "expired", "note": "集成已删除"})
        return self._store.delete(integration_id)

    # -- 校验 -------------------------------------------------------------
    @staticmethod
    def _validate_templates(raw) -> dict:
        if not raw:
            return {}
        if not isinstance(raw, dict):
            raise ValueError("templates 必须是对象")
        out: dict[str, dict] = {}
        for event, tpl in raw.items():
            if event not in EVENTS:
                raise ValueError(f"未知事件类型：{event}")
            if not isinstance(tpl, dict):
                raise ValueError("模板必须包含 title / body")
            title = tpl.get("title")
            body = tpl.get("body")
            if title is not None and not isinstance(title, str):
                raise ValueError("模板标题必须是字符串")
            if body is not None and not isinstance(body, str):
                raise ValueError("模板正文必须是字符串")
            if title is None and body is None:
                continue
            out[event] = {"title": title or "", "body": body or ""}
        return out

    @staticmethod
    def _validate_quiet_hours(raw) -> dict:
        if raw is None:
            return {"enabled": False, "start": "22:00", "end": "08:00"}
        if not isinstance(raw, dict):
            raise ValueError("quiet_hours 必须是对象")
        enabled = bool(raw.get("enabled", False))
        start = (raw.get("start") or "22:00").strip()
        end = (raw.get("end") or "08:00").strip()
        s = parse_hhmm(start)
        e = parse_hhmm(end)
        if s == e:
            raise ValueError("静默开始与结束时间不能相同")
        return {"enabled": enabled, "start": start, "end": end}

    @staticmethod
    def _validate_max_attempts(raw) -> int:
        if raw is None:
            return DEFAULT_MAX_ATTEMPTS
        try:
            n = int(raw)
        except (TypeError, ValueError):
            raise ValueError("最大重试次数必须是 1~5 的整数")
        if not 1 <= n <= 5:
            raise ValueError("最大重试次数必须在 1~5 之间")
        return n

    # -- 模板解析 / 预览 --------------------------------------------------
    def get_template(self, integration: dict, event: str) -> dict:
        """取集成对某事件的有效模板：自定义优先，否则用渠道默认。"""
        custom = (integration.get("templates") or {}).get(event)
        if custom:
            tpl = dict(DEFAULT_TEMPLATES.get(integration.get("type"),
                                             DEFAULT_TEMPLATES["webhook"])[event])
            if custom.get("title"):
                tpl["title"] = custom["title"]
            if custom.get("body"):
                tpl["body"] = custom["body"]
            return tpl
        return dict(DEFAULT_TEMPLATES.get(integration.get("type"),
                                          DEFAULT_TEMPLATES["webhook"])[event])

    def preview(self, itype: str, event: str, title: Optional[str],
                body: Optional[str], payload: Optional[dict] = None) -> dict:
        """用示例数据（可被 ``payload`` 覆盖）渲染模板，供配置页预览。"""
        if itype not in INTEGRATION_TYPES:
            raise ValueError("不支持的集成类型")
        if event not in EVENTS:
            raise ValueError("未知事件类型")
        default = DEFAULT_TEMPLATES[itype][event]
        merged = dict(SAMPLE_PAYLOAD)
        if payload:
            merged.update(payload)
        context = build_context(event, merged)
        rendered_title, miss_t = render_template(title or default["title"], context)
        rendered_body, miss_b = render_template(body or default["body"], context)
        return {
            "type": itype,
            "event": event,
            "title": rendered_title,
            "body": rendered_body,
            "missing": sorted(set(miss_t + miss_b)),
            "context": context,
        }

    # -- 投递（模拟） -----------------------------------------------------
    def _target_of(self, integration: dict) -> str:
        cfg = integration.get("config") or {}
        return cfg.get("url") or cfg.get("address") or cfg.get("channel") or "未配置目标"

    def _deliver(self, integration: dict, event: str, payload: dict) -> dict:
        target = self._target_of(integration)
        # 确定性投递结果：未配置目标 / 显式 fail 标志 → 失败
        latency = 8 + _seeded_int(integration["id"], event,
                                  (payload or {}).get("build_id")) % 120
        failed = not target or target == "未配置目标" or \
            (integration.get("config") or {}).get("fail", False)
        return {
            "status": "failed" if failed else "delivered",
            "recipient": target,
            "latency_ms": latency,
            "error": "目标地址未配置" if target == "未配置目标"
                     else ("模拟投递失败（config.fail=true）" if failed else None),
        }

    def _new_event_record(self, integration: dict, event: str,
                          payload: dict, title: str, body: str) -> dict:
        return {
            "id": new_id("evt"),
            "project_id": integration.get("project_id"),
            "integration_id": integration["id"],
            "integration_name": integration.get("name"),
            "type": integration.get("type"),
            "event": event,
            "status": "pending",
            "recipient": self._target_of(integration),
            "latency_ms": None,
            "title": title,
            "body": body,
            "payload": payload,
            "attempt": 0,
            "max_attempts": integration.get("max_attempts", DEFAULT_MAX_ATTEMPTS),
            "attempts": [],
            "next_retry_at": None,
            "queued_item_ids": [],
            "created_at": time.time(),
        }

    def _save_record(self, record: dict):
        if self._events.get(record["id"]) is None:
            self._events.insert(record)
        else:
            self._events.update(record["id"], record)

    def _enqueue_retry(self, record: dict, queued_ids: list[str],
                       at: Optional[float] = None) -> dict:
        attempt = record["attempt"] + 1
        item = {
            "id": new_id("nq"),
            "project_id": record["project_id"],
            "integration_id": record["integration_id"],
            "kind": "retry",
            "event": record["event"],
            "status": "queued",
            "release_at": record["next_retry_at"],
            "event_id": record["id"],
            "attempt": attempt,
            "queued_ids": queued_ids,
            "created_at": at if at is not None else time.time(),
        }
        self._queue.insert(item)
        return item

    def _apply_attempt(self, integration: dict, record: dict, attempt: int,
                       reason: str, queued_ids: Optional[list[str]] = None,
                       at: Optional[float] = None) -> dict:
        """对一条事件记录执行第 ``attempt`` 次投递，并落库全部痕迹。

        - 成功：记录置 delivered，静默队列项（若有）置 sent / 其原事件置 digested；
        - 失败且未达上限：记录置 retrying，写一条带退避时间的 retry 队列项；
        - 失败且达上限：记录置 failed，静默队列项置 failed。
        """
        queued_ids = queued_ids or record.get("queued_item_ids") or []
        outcome = self._deliver(integration, record["event"], record.get("payload") or {})
        now = at if at is not None else time.time()
        record["attempts"] = list(record.get("attempts") or [])
        record["attempts"].append({
            "attempt": attempt,
            "status": outcome["status"],
            "recipient": outcome["recipient"],
            "latency_ms": outcome["latency_ms"],
            "error": outcome["error"],
            "reason": reason,
            "at": now,
        })
        record["attempt"] = attempt
        record["recipient"] = outcome["recipient"]
        record["latency_ms"] = outcome["latency_ms"]
        record["max_attempts"] = integration.get("max_attempts",
                                                 record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
        record["next_retry_at"] = None

        if outcome["status"] == "delivered":
            record["status"] = "delivered"
            if queued_ids:
                self._finalize_queued(queued_ids, "sent", record["id"], "digested")
        elif attempt < record["max_attempts"]:
            delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
            record["status"] = "retrying"
            record["next_retry_at"] = now + delay
            record["retry_delay_s"] = delay
            self._enqueue_retry(record, queued_ids, at=now)
        else:
            record["status"] = "failed"
            if queued_ids:
                self._finalize_queued(queued_ids, "failed", record["id"], "failed")

        self._save_record(record)
        return record

    def _finalize_queued(self, queued_ids: list[str], queue_status: str,
                         digest_event_id: str, origin_status: str) -> None:
        for qid in queued_ids:
            item = self._queue.get(qid)
            if item and item.get("status") in ("queued", "sending"):
                self._queue.update(qid, {
                    "status": queue_status,
                    "digest_event_id": digest_event_id,
                    "sent_at": time.time(),
                })
            if item and item.get("event_id"):
                self._events.update(item["event_id"], {
                    "status": origin_status,
                    "digest_event_id": digest_event_id,
                })

    def _set_queue(self, qid: str, status: str, **extra) -> None:
        patch = {"status": status}
        patch.update(extra)
        self._queue.update(qid, patch)

    # -- 事件触发 ---------------------------------------------------------
    def fire(self, project_id: str, event: str, payload: dict,
             *, at: Optional[float] = None) -> list[dict]:
        """向订阅了该事件的所有启用集成投递通知，返回事件记录列表。

        静默期内不直接投递：渲染好的消息入队并产生一条 ``queued`` 记录，
        等静默结束由 :meth:`flush_due` 合并补发。
        """
        at_ts = at if at is not None else time.time()
        at_dt = datetime.datetime.fromtimestamp(at_ts)
        context = build_context(event, payload)
        records: list[dict] = []

        for integration in self.list(project_id):
            if not integration.get("enabled", True):
                continue
            if event not in (integration.get("events") or ["build.finished"]):
                continue

            tpl = self.get_template(integration, event)
            title, _ = render_template(tpl["title"], context)
            body, _ = render_template(tpl["body"], context)

            quiet = integration.get("quiet_hours")
            release_at = next_quiet_end(quiet, at_dt)
            if release_at is not None:
                record = self._new_event_record(integration, event, payload, title, body)
                record["status"] = "queued"
                record["release_at"] = release_at
                item = {
                    "id": new_id("nq"),
                    "project_id": project_id,
                    "integration_id": integration["id"],
                    "event_id": record["id"],
                    "kind": "quiet",
                    "event": event,
                    "payload": payload,
                    "title": title,
                    "body": body,
                    "status": "queued",
                    "queued_at": at_ts,
                    "release_at": release_at,
                    "created_at": at_ts,
                }
                self._queue.insert(item)
                record["queue_item_id"] = item["id"]
                self._events.insert(record)
                records.append(record)
            else:
                record = self._new_event_record(integration, event, payload, title, body)
                records.append(self._apply_attempt(integration, record, 1,
                                                   reason="dispatch", at=at_ts))
        return records

    def send_test(self, integration_id: str) -> dict:
        """测试投递：示例数据渲染模板、绕过静默立即发送。"""
        integration = self.get(integration_id)
        if integration is None:
            return {"error": "集成不存在"}
        payload = dict(SAMPLE_PAYLOAD)
        payload["project_id"] = integration.get("project_id")
        tpl = self.get_template(integration, "build.finished")
        context = build_context("build.finished", payload)
        title, _ = render_template(tpl["title"], context)
        body, _ = render_template(tpl["body"], context)
        record = self._new_event_record(integration, "test", payload, title, body)
        record = self._apply_attempt(integration, record, 1, reason="manual_test")
        record["integration"] = integration
        return record

    # -- 静默队列：合并补发 -----------------------------------------------
    def pending_summary(self, project_id: Optional[str] = None,
                        at: Optional[float] = None) -> dict:
        """静默队列概览（供页面显示「N 条待补发」）。"""
        at = at if at is not None else time.time()
        where = [("status", "eq", "queued")]
        if project_id:
            where.append(("project_id", "eq", project_id))
        items = self._queue.query(where=where)
        quiet = [q for q in items if q.get("kind") == "quiet"]
        retries = [q for q in items if q.get("kind") == "retry"]
        due = [q for q in items if (q.get("release_at") or 0) <= at]
        return {
            "queued": len(quiet),
            "retrying": len(retries),
            "due": len(due),
            "next_release_at": min((q["release_at"] for q in items
                                    if q.get("release_at")), default=None),
        }

    def flush_due(self, at: Optional[float] = None) -> dict:
        """补发所有到期消息：失败重试 + 静默积压合并 digest。

        由调度器周期性调用；``at`` 可注入时间便于测试。
        """
        at = at if at is not None else time.time()
        with self._flush_lock:
            self._recover_stale(at)
            items = self._queue.query(where=[("status", "eq", "queued")],
                                      order_by="release_at", order="asc")
            due = [q for q in items if (q.get("release_at") or 0) <= at]
            retries = [q for q in due if q.get("kind") == "retry"]
            quiet = [q for q in due if q.get("kind") == "quiet"]

            retry_records = [self._process_retry(q, at) for q in retries]

            grouped: dict[str, list[dict]] = {}
            for q in quiet:
                grouped.setdefault(q["integration_id"], []).append(q)
            digest_records = [self._send_digest(iid, qs, at)
                              for iid, qs in grouped.items()]

            return {
                "flushed_at": at,
                "retries": [r["id"] for r in retry_records if r],
                "digests": [r["id"] for r in digest_records if r],
            }

    def flush_project(self, project_id: str, force: bool = False,
                      at: Optional[float] = None) -> dict:
        """补发某项目的积压；``force=True`` 时忽略静默结束时间立即补发。"""
        at = at if at is not None else time.time()
        if force:
            for q in self._queue.query(where=[("project_id", "eq", project_id)]):
                if q.get("status") == "queued" and q.get("kind") == "quiet" \
                        and (q.get("release_at") or 0) > at:
                    self._queue.update(q["id"], {"release_at": at})
        return self.flush_due(at)

    def flush_integration(self, integration_id: str, force: bool = True,
                          at: Optional[float] = None) -> dict:
        """对单个集成立即强制补发静默积压（页面「立即补发」按钮）。"""
        at = at if at is not None else time.time()
        with self._flush_lock:
            items = self._queue.query(where=[("integration_id", "eq", integration_id)])
            quiet = [q for q in items if q.get("kind") == "quiet"
                     and q.get("status") in ("queued", "sending")]
            if force:
                for q in quiet:
                    if q.get("status") == "queued":
                        self._queue.update(q["id"], {"release_at": at})
            return self.flush_due(at)

    def _recover_stale(self, at: float) -> None:
        """进程重启等情况下可能残留在 sending 的项，超时后重新入队。

        - 重试队列项：超过 10 分钟仍 sending，重新排队；
        - 静默项：只要还有一条「重试中」的 digest 事件引用它（digest 可能
          正在自动重试），就保持 sending，避免同一条消息被二次合并。
        """
        live_digests = {}
        for rec in self._events.all():
            if rec.get("event") == "digest" and rec.get("status") == "retrying":
                for qid in rec.get("queued_item_ids") or []:
                    live_digests[qid] = rec["id"]
        for q in self._queue.query(where=[("status", "eq", "sending")]):
            age = at - (q.get("updated_at") or q.get("created_at") or at)
            if q.get("kind") == "quiet" and q["id"] in live_digests:
                continue
            if age > 600:
                self._queue.update(q["id"], {"status": "queued"})

    def _process_retry(self, item: dict, at: Optional[float] = None) -> Optional[dict]:
        record = self._events.get(item.get("event_id")) if item.get("event_id") else None
        integration = self.get(item["integration_id"])
        self._queue.update(item["id"], {"status": "sending"})
        if record is None or integration is None:
            self._queue.update(item["id"], {"status": "expired",
                                           "note": "记录或集成已不存在"})
            return None
        if record.get("status") not in ("retrying", "failed"):
            self._queue.update(item["id"], {"status": "done",
                                           "note": "记录已处于终态"})
            return None
        try:
            result = self._apply_attempt(integration, record, item["attempt"],
                                         reason="auto_retry",
                                         queued_ids=item.get("queued_ids"), at=at)
            # 本次重试已被消费；若仍失败，_apply_attempt 已按退避排好下一条重试
            self._queue.update(item["id"], {"status": "done",
                                           "result": result.get("status")})
            return result
        except Exception as exc:  # noqa: BLE001
            self._queue.update(item["id"], {"status": "queued",
                                           "note": f"处理异常，下轮重试: {exc}"})
            return None

    def _send_digest(self, integration_id: str, items: list[dict],
                     at: Optional[float] = None) -> Optional[dict]:
        integration = self.get(integration_id)
        if integration is None:
            for q in items:
                self._set_queue(q["id"], "expired", note="集成已删除")
                if q.get("event_id"):
                    self._events.update(q["event_id"],
                                        {"status": "expired", "note": "集成已删除"})
            return None

        items.sort(key=lambda q: q.get("queued_at") or q.get("created_at") or 0)
        for q in items:
            self._queue.update(q["id"], {"status": "sending"})

        times = [q.get("queued_at") or q.get("created_at") for q in items]
        lines = [f"{idx}. [{datetime.datetime.fromtimestamp(t).strftime('%m-%d %H:%M')}] {q.get('title', '')}"
                 for idx, (q, t) in enumerate(zip(items, times), start=1)]
        context = {
            "project_name": items[0].get("payload", {}).get("project_name")
                            or integration.get("project_id"),
            "count": len(items),
            "start_time": datetime.datetime.fromtimestamp(min(times)).strftime("%m-%d %H:%M"),
            "end_time": datetime.datetime.fromtimestamp(max(times)).strftime("%m-%d %H:%M"),
            "items": "\n".join(lines),
        }
        tpl = DIGEST_TEMPLATES.get(integration.get("type"), DIGEST_TEMPLATES["webhook"])
        title, _ = render_template(tpl["title"], context)
        body, _ = render_template(tpl["body"], context)

        payload = {
            "project_id": integration.get("project_id"),
            "project_name": context["project_name"],
            "digest": True,
            "count": len(items),
            "events": [q.get("event") for q in items],
            "build_ids": [q.get("payload", {}).get("build_id") for q in items],
            "window": [min(times), max(times)],
        }
        record = self._new_event_record(integration, "digest", payload, title, body)
        record["queued_item_ids"] = [q["id"] for q in items]
        return self._apply_attempt(integration, record, 1, reason="quiet_digest",
                                   queued_ids=[q["id"] for q in items], at=at)

    # -- 手动重试 ---------------------------------------------------------
    def retry_event(self, event_id: str) -> dict:
        """对投递失败 / 重试中的事件立即再试一次（手动）。"""
        record = self._events.get(event_id)
        if record is None:
            return {"error": "事件不存在"}
        if record.get("status") in ("delivered", "queued", "digested", "expired"):
            return {"error": f"当前状态 {record.get('status')} 无需重试"}
        integration = self.get(record["integration_id"])
        if integration is None:
            return {"error": "集成已删除，无法重试"}

        # 作废尚未触发的自动重试队列项，避免与本次手动重试重复
        for q in self._queue.query(where=[("event_id", "eq", event_id)]):
            if q.get("kind") == "retry" and q.get("status") == "queued":
                self._queue.update(q["id"], {"status": "cancelled",
                                             "note": "手动重试取代"})
        attempt = len(record.get("attempts") or []) + 1
        return self._apply_attempt(integration, record, attempt, reason="manual_retry",
                                   queued_ids=record.get("queued_item_ids"))

    # -- 查询 -------------------------------------------------------------
    def events(self, project_id: str, limit: int = 100) -> list[dict]:
        return self._events.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="desc",
                                  limit=limit)

    def queue(self, project_id: str) -> list[dict]:
        return self._queue.query(where=[("project_id", "eq", project_id)],
                                 order_by="created_at", order="desc")
