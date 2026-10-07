"""通知模块测试：模板、静默时段、合并补发、失败重试与留痕。"""

from __future__ import annotations

import datetime as dt
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import NotificationManager
from engine.notify import quiet_state, render_template_text, SAMPLE_CONTEXT
from storage import StoreRegistry


def _ts(y, mo, d, h, mi=0):
    return dt.datetime(y, mo, d, h, mi).timestamp()


class TestRender(unittest.TestCase):
    def test_basic_placeholders(self):
        text = "${project_name} ${passed}/${total} ${pass_rate}%"
        out = render_template_text(text, {"project_name": "P", "passed": 3,
                                          "total": 4, "pass_rate": 75.0})
        self.assertEqual(out, "P 3/4 75.0%")

    def test_missing_placeholder_kept(self):
        self.assertEqual(render_template_text("${unknown}", {}), "${unknown}")

    def test_nested_path(self):
        out = render_template_text("${a.b}", {"a": {"b": "ok"}})
        self.assertEqual(out, "ok")

    def test_preview_sample_has_failed_cases(self):
        self.assertNotIn("${failed_cases}",
                         render_template_text("${failed_cases}", SAMPLE_CONTEXT))


class TestQuietState(unittest.TestCase):
    def test_inside_window(self):
        s = {"enabled": True, "start": "22:00", "end": "08:00"}
        quiet, release = quiet_state(s, dt.datetime(2026, 10, 7, 3, 0))
        self.assertTrue(quiet)
        self.assertEqual(dt.datetime.fromtimestamp(release),
                         dt.datetime(2026, 10, 7, 8, 0))

    def test_evening_cross_midnight(self):
        s = {"enabled": True, "start": "22:00", "end": "08:00"}
        quiet, release = quiet_state(s, dt.datetime(2026, 10, 6, 23, 30))
        self.assertTrue(quiet)
        self.assertEqual(dt.datetime.fromtimestamp(release),
                         dt.datetime(2026, 10, 7, 8, 0))

    def test_outside_window(self):
        s = {"enabled": True, "start": "22:00", "end": "08:00"}
        quiet, release = quiet_state(s, dt.datetime(2026, 10, 7, 12, 0))
        self.assertFalse(quiet)
        self.assertIsNone(release)

    def test_boundary(self):
        s = {"enabled": True, "start": "09:00", "end": "18:00"}
        self.assertTrue(quiet_state(s, dt.datetime(2026, 10, 7, 9, 0))[0])
        self.assertFalse(quiet_state(s, dt.datetime(2026, 10, 7, 18, 0))[0])

    def test_weekend_all_day(self):
        s = {"enabled": True, "start": "22:00", "end": "08:00", "weekend": True}
        # 2026-10-03 是周六
        quiet, release = quiet_state(s, dt.datetime(2026, 10, 3, 12, 0))
        self.assertTrue(quiet)
        self.assertEqual(dt.datetime.fromtimestamp(release),
                         dt.datetime(2026, 10, 5, 22, 0))
        # 工作日中午不静默
        self.assertFalse(quiet_state(s, dt.datetime(2026, 10, 7, 12, 0))[0])

    def test_disabled(self):
        s = {"enabled": False, "start": "22:00", "end": "08:00"}
        self.assertFalse(quiet_state(s, dt.datetime(2026, 10, 7, 3, 0))[0])


class TestNotificationBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry = StoreRegistry(os.path.join(self.tmp.name, "store"))
        self.registry.store("projects").insert(
            {"id": "p1", "name": "演示项目"})
        self.mgr = NotificationManager(self.registry,
                                       retry_backoff=(100.0, 300.0))

    def tearDown(self):
        self.tmp.cleanup()

    def _int(self, itype="webhook", config=None, events=None, templates=None):
        return self.mgr.create("p1", {
            "type": itype, "name": f"int-{itype}",
            "config": config if config is not None else {"url": "http://hook"},
            "events": events or ["build.failed"],
            "templates": templates,
        })

    def _payload(self, **kw):
        base = {"build_id": "b1", "project_id": "p1", "project_name": "演示项目",
                "status": "failed", "passed": 7, "total": 8,
                "pass_rate": 87.5, "duration": 12.3,
                "failed_cases": "登录接口\n慢接口"}
        base.update(kw)
        return base


class TestTemplatePerChannel(TestNotificationBase):
    def test_each_channel_uses_own_template(self):
        slack = self._int("slack", config={"url": "http://s"},
                          events=["build.failed"])
        email = self._int("email", config={"address": "a@b.com"},
                          events=["build.failed"])
        records = self.mgr.fire("p1", "build.failed", self._payload())
        by_type = {r["type"]: r for r in records}
        self.assertIn("*演示项目*", by_type["slack"]["title"])  # Slack 加粗
        self.assertTrue(by_type["email"]["title"].startswith("[CI][告警]"))
        # 占位符已被替换
        self.assertIn("87.5", by_type["slack"]["body"])
        self.assertIn("登录接口", by_type["email"]["body"])

    def test_custom_template_overrides_default(self):
        self._int(templates={"build.failed": {
            "title": "自定义标题 ${project_name}",
            "body": "自定义正文 ${pass_rate}%"}})
        records = self.mgr.fire("p1", "build.failed", self._payload())
        self.assertEqual(records[0]["title"], "自定义标题 演示项目")
        self.assertEqual(records[0]["body"], "自定义正文 87.5%")

    def test_partial_custom_falls_back_to_default(self):
        self._int(templates={"build.failed": {"title": "仅标题", "body": ""}})
        records = self.mgr.fire("p1", "build.failed", self._payload())
        self.assertEqual(records[0]["title"], "仅标题")
        self.assertIn("b1", records[0]["body"])  # body 回退默认模板

    def test_preview(self):
        out = self.mgr.render_preview("slack", "build.failed",
                                      title="预览 ${project_name}", body="")
        self.assertEqual(out["title"], "预览 演示项目 · 测试与CI")
        self.assertIn("登录接口", out["body"])

    def test_preview_default_body_fully_rendered(self):
        out = self.mgr.render_preview("email", "build.passed")
        self.assertNotIn("${", out["title"])
        self.assertNotIn("${", out["body"])


class TestQuietQueue(TestNotificationBase):
    def test_deferred_then_merged_digest(self):
        self.mgr.save_settings("p1", {"enabled": True,
                                      "start": "22:00", "end": "08:00"})
        integration = self._int(events=["build.finished"])
        at_night = _ts(2026, 10, 7, 3, 0)

        # 夜间两条事件：都不投递，只入队留痕
        r1 = self.mgr.fire("p1", "build.finished",
                           self._payload(build_id="b1"), at=at_night)
        r2 = self.mgr.fire("p1", "build.finished",
                           self._payload(build_id="b2"), at=at_night + 60)
        self.assertEqual(len(r1) + len(r2), 2)
        self.assertTrue(all(r["status"] == "deferred" for r in r1 + r2))
        self.assertEqual(len(self.mgr.queue("p1")), 2)

        # 窗口未结束前 flush：什么都不补发
        nothing = self.mgr.flush_due(at=at_night + 120)
        self.assertEqual(nothing["digests"], 0)

        # 8 点后 flush：同一集成的两条合并成一条 quiet.digest
        result = self.mgr.flush_due(at=_ts(2026, 10, 7, 8, 0))
        self.assertEqual(result["digests"], 1)
        digest = result["records"][0]
        self.assertEqual(digest["event"], "quiet.digest")
        self.assertEqual(digest["status"], "delivered")
        self.assertEqual(digest["queued_count"], 2)
        self.assertIn("b1", digest["body"])
        self.assertIn("b2", digest["body"])
        self.assertEqual(digest["integration_id"], integration["id"])
        # 原队列项已合并消费，不再挂起
        self.assertEqual(self.mgr.queue("p1"), [])

    def test_separate_integrations_get_separate_digests(self):
        self.mgr.save_settings("p1", {"enabled": True,
                                      "start": "22:00", "end": "08:00"})
        self._int("slack", config={"url": "http://s"},
                  events=["build.finished"])
        self._int("email", config={"address": "a@b.com"},
                  events=["build.finished"])
        at_night = _ts(2026, 10, 7, 3, 0)
        self.mgr.fire("p1", "build.finished", self._payload(), at=at_night)
        result = self.mgr.flush_due(at=_ts(2026, 10, 7, 8, 0))
        self.assertEqual(result["digests"], 2)
        types = sorted(r["type"] for r in result["records"])
        self.assertEqual(types, ["email", "slack"])

    def test_digest_uses_channel_digest_template(self):
        self.mgr.save_settings("p1", {"enabled": True,
                                      "start": "22:00", "end": "08:00"})
        self._int("email", config={"address": "a@b.com"},
                  events=["build.finished"])
        at_night = _ts(2026, 10, 7, 3, 0)
        self.mgr.fire("p1", "build.finished", self._payload(), at=at_night)
        digest = self.mgr.flush_due(at=_ts(2026, 10, 7, 8, 0))["records"][0]
        self.assertIn("静默期通知汇总", digest["title"])
        self.assertIn("1 条", digest["title"])

    def test_failed_digest_retries_then_manual_retry_succeeds(self):
        self.mgr.save_settings("p1", {"enabled": True,
                                      "start": "22:00", "end": "08:00"})
        # 无目标地址 → 投递必然失败
        self._int(config={}, events=["build.finished"])
        at_night = _ts(2026, 10, 7, 3, 0)
        self.mgr.fire("p1", "build.finished", self._payload(), at=at_night)
        first = self.mgr.flush_due(at=_ts(2026, 10, 7, 8, 0))
        self.assertEqual(first["records"][0]["status"], "failed")
        # 失败的汇总进入重试队列（只有一条 pending）
        pending = self.mgr.queue("p1")
        self.assertEqual(len(pending), 1)
        self.assertTrue(pending[0]["digest"])
        self.assertGreater(pending[0]["release_at"], _ts(2026, 10, 7, 8, 0))

    def test_weekend_quiet_defers_until_monday(self):
        self.mgr.save_settings("p1", {"enabled": True,
                                      "start": "22:00", "end": "08:00",
                                      "weekend": True})
        self._int(events=["build.finished"])
        # 周六中午
        records = self.mgr.fire("p1", "build.finished", self._payload(),
                                at=_ts(2026, 10, 3, 12, 0))
        self.assertEqual(records[0]["status"], "deferred")
        # 周日 flush 仍不补发
        self.assertEqual(
            self.mgr.flush_due(at=_ts(2026, 10, 4, 12, 0))["digests"], 0)
        # 周一 22 点才是 release（周末结束接夜间窗口起点）
        result = self.mgr.flush_due(at=_ts(2026, 10, 5, 22, 0))
        self.assertEqual(result["digests"], 1)


class TestRetry(TestNotificationBase):
    def _failing(self):
        return self._int(config={}, events=["build.failed"])

    def test_automatic_retry_with_backoff(self):
        mgr = NotificationManager(self.registry, max_attempts=3,
                                  retry_backoff=(100.0, 300.0))
        integration = mgr.create("p1", {"type": "webhook", "name": "坏地址",
                                        "config": {}, "events": ["build.failed"]})
        t0 = _ts(2026, 10, 7, 10, 0)
        mgr.fire("p1", "build.failed", self._payload(), at=t0)
        pending = mgr.queue("p1")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["attempts"], 1)
        self.assertAlmostEqual(pending[0]["release_at"], t0 + 100)

        # 未到时间不重试
        self.assertEqual(mgr.flush_due(at=t0 + 50)["retries"], 0)
        # 第二次尝试仍失败，下一次间隔 300
        mgr.flush_due(at=t0 + 100)
        pending = mgr.queue("p1")
        self.assertEqual(pending[0]["attempts"], 2)
        self.assertAlmostEqual(pending[0]["release_at"], t0 + 400)
        # 第三次（最后一次）失败 → dead
        mgr.flush_due(at=t0 + 400)
        dead = mgr.queue("p1")
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]["status"], "dead")
        self.assertEqual(dead[0]["attempts"], 3)

        # 事件日志：1 首次 + 2 次重试，共享 delivery_key
        events = [e for e in mgr.events("p1") if e["event"] == "build.failed"]
        self.assertEqual(len(events), 3)
        keys = {e["delivery_key"] for e in events}
        self.assertEqual(len(keys), 1)
        self.assertEqual([e["attempt"] for e in events], [3, 2, 1])
        self.assertTrue(all(e["error"] for e in events))

    def test_manual_retry_dead_then_succeed(self):
        mgr = NotificationManager(self.registry, max_attempts=1,
                                  retry_backoff=(10.0,))
        integration = mgr.create("p1", {"type": "webhook", "name": "坏地址",
                                        "config": {}, "events": ["build.failed"]})
        t0 = _ts(2026, 10, 7, 10, 0)
        mgr.fire("p1", "build.failed", self._payload(), at=t0)
        dead = mgr.queue("p1")
        self.assertEqual(dead[0]["status"], "dead")

        # 修好目标地址后手动重试 → 成功
        mgr.update(integration["id"], {"config": {"url": "http://fixed"}})
        record = mgr.retry_item(dead[0]["id"], at=t0 + 5)
        self.assertEqual(record["status"], "delivered")
        self.assertEqual(mgr.queue("p1"), [])

    def test_delivered_not_queued(self):
        self._int()
        self.mgr.fire("p1", "build.failed", self._payload(),
                      at=_ts(2026, 10, 7, 10, 0))
        self.assertEqual(self.mgr.queue("p1"), [])

    def test_test_delivery_bypasses_quiet(self):
        self.mgr.save_settings("p1", {"enabled": True,
                                      "start": "00:00", "end": "23:59"})
        integration = self._int()
        result = self.mgr.send_test(integration["id"])
        self.assertEqual(result["status"], "delivered")
        self.assertEqual(self.mgr.queue("p1"), [])


class TestAuditTrail(TestNotificationBase):
    def test_event_carries_rendered_content(self):
        self._int(templates={"build.failed": {"title": "T ${project_name}",
                                              "body": "B ${pass_rate}%"}})
        records = self.mgr.fire("p1", "build.failed", self._payload())
        rec = records[0]
        self.assertEqual(rec["title"], "T 演示项目")
        self.assertEqual(rec["body"], "B 87.5%")
        self.assertEqual(rec["recipient"], "http://hook")
        self.assertGreater(rec["latency_ms"], 0)
        self.assertEqual(rec["deferred"], False)

    def test_disabled_integration_skipped(self):
        integration = self._int()
        self.mgr.update(integration["id"], {"enabled": False})
        self.assertEqual(
            self.mgr.fire("p1", "build.failed", self._payload()), [])


if __name__ == "__main__":
    unittest.main()
