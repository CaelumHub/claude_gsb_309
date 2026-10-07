"""引擎层单元测试。

覆盖：测试执行器（步骤/断言/超时/取消）、cron、环境依赖解析、
覆盖率、报告生成、缺陷、通知。
"""

from __future__ import annotations

import datetime
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, CronSchedule, DefectManager,
                    EnvironmentManager, NotificationManager, ReportGenerator,
                    TestExecutor, cron_matches, parse_cron)
from engine.executor import evaluate_assertion, resolve_expr, safe_eval
from storage import BuildStoreRegistry, StoreRegistry


class TestResolveExpr(unittest.TestCase):
    def test_pure_reference_returns_value(self):
        self.assertEqual(resolve_expr("${a.b}", {"a": {"b": 42}}), 42)

    def test_partial_substitution(self):
        self.assertEqual(resolve_expr("x=${a}", {"a": "hi"}), "x=hi")

    def test_missing_path_returns_none(self):
        self.assertIsNone(resolve_expr("${a.b.c}", {"a": {}}))

    def test_list_index(self):
        self.assertEqual(resolve_expr("${items.0}", {"items": ["x", "y"]}), "x")


class TestSafeEval(unittest.TestCase):
    def test_arithmetic(self):
        self.assertEqual(safe_eval("2 + 3 * 4", {}), 14)

    def test_forbidden_import(self):
        with self.assertRaises(Exception):
            safe_eval("__import__('os')", {})

    def test_forbidden_attribute(self):
        with self.assertRaises(Exception):
            safe_eval("().__class__", {})


class TestEvaluateAssertion(unittest.TestCase):
    def test_equals_with_string_number(self):
        ok, _ = evaluate_assertion("equals", 14, "14")
        self.assertTrue(ok)

    def test_between(self):
        ok, _ = evaluate_assertion("between", 14, [10, 20])
        self.assertTrue(ok)
        ok, _ = evaluate_assertion("between", 5, [10, 20])
        self.assertFalse(ok)

    def test_regex(self):
        ok, _ = evaluate_assertion("regex", "release-2.31.0", r"^\d+\.\d+")
        # 注意：regex 比较的是 str(actual)
        ok, _ = evaluate_assertion("regex", "2.31.0-x", r"^\d+\.\d+")
        self.assertTrue(ok)

    def test_contains(self):
        ok, _ = evaluate_assertion("contains", {"ok": True}, "ok")
        self.assertTrue(ok)


class TestExecutorRun(unittest.TestCase):
    def test_passing_case(self):
        case = {
            "id": "c1", "name": "健康检查",
            "steps": [
                {"action": "request", "method": "GET", "url": "/api/health"},
                {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200},
                {"action": "assert", "type": "truthy", "actual": "${resp.body.ok}", "expected": True},
            ],
        }
        result = TestExecutor().execute_case(case, {"latency_ms": 0})
        self.assertEqual(result["status"], "passed")

    def test_failing_case(self):
        case = {
            "id": "c2", "name": "失败",
            "steps": [
                {"action": "request", "method": "GET", "url": "/api/error"},
                {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200},
            ],
        }
        result = TestExecutor().execute_case(case, {"latency_ms": 0})
        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(result["assertions"]), 1)
        self.assertFalse(result["assertions"][0]["ok"])

    def test_script_and_between(self):
        case = {
            "id": "c3", "name": "脚本",
            "steps": [
                {"action": "script", "expr": "2 + 3 * 4", "save_as": "r"},
                {"action": "assert", "type": "equals", "actual": "${r}", "expected": 14},
                {"action": "assert", "type": "between", "actual": "${r}", "expected": [10, 20]},
            ],
        }
        result = TestExecutor().execute_case(case, {})
        self.assertEqual(result["status"], "passed")

    def test_timeout(self):
        case = {
            "id": "c4", "name": "超时", "timeout": 0.1,
            "steps": [
                {"action": "sleep", "seconds": 0.05},
                {"action": "sleep", "seconds": 0.05},
                {"action": "sleep", "seconds": 0.05},
            ],
        }
        result = TestExecutor().execute_case(case, {})
        self.assertEqual(result["status"], "timeout")

    def test_disabled_skipped(self):
        case = {"id": "c5", "name": "禁用", "enabled": False, "steps": []}
        result = TestExecutor().execute_case(case, {})
        self.assertEqual(result["status"], "skipped")

    def test_env_isolation_changes_result(self):
        """同一用例，高失败率环境与零失败率环境结果不同（环境隔离）。"""
        case = {
            "id": "c6", "name": "接口",
            "steps": [
                {"action": "request", "method": "GET", "url": "/api/health"},
                {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200},
            ],
        }
        # 稳定环境：一定通过
        r1 = TestExecutor().execute_case(case, {"latency_ms": 0, "fail_rate": 0.0})
        self.assertEqual(r1["status"], "passed")
        # 失败率 1.0 的环境：一定失败
        r2 = TestExecutor().execute_case(case, {"latency_ms": 0, "fail_rate": 1.0})
        self.assertEqual(r2["status"], "failed")


class TestCron(unittest.TestCase):
    def test_parse_and_match(self):
        sched = parse_cron("*/10 * * * *")
        self.assertEqual(sched.minute, [0, 10, 20, 30, 40, 50])
        self.assertTrue(cron_matches("0 9 * * 1", datetime.datetime(2026, 10, 5, 9, 0)))
        self.assertFalse(cron_matches("0 9 * * 1", datetime.datetime(2026, 10, 6, 9, 0)))

    def test_invalid(self):
        with self.assertRaises(ValueError):
            parse_cron("* * *")


class TestEnvironments(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry = StoreRegistry(os.path.join(self.tmp.name, "store"))
        self.mgr = EnvironmentManager(self.registry, self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_resolve_dependencies(self):
        env = self.mgr.create("p1", {
            "name": "dev",
            "dependencies": [
                {"name": "requests", "constraint": ">=2.28"},
                {"name": "flask", "constraint": ">=3.0"},
                {"name": "numpy", "constraint": ">=99.0"},
            ],
        })
        resolved = self.mgr.resolve(env["id"])
        self.assertEqual(resolved["resolved_count"], 2)
        self.assertEqual(resolved["conflict_count"], 1)
        statuses = {d["name"]: d["status"] for d in resolved["dependencies"]}
        self.assertEqual(statuses["numpy"], "conflict")

    def test_workspace_isolation(self):
        e1 = self.mgr.create("p1", {"name": "a"})
        e2 = self.mgr.create("p1", {"name": "b"})
        self.assertNotEqual(self.mgr.workspace_dir(e1["id"]), self.mgr.workspace_dir(e2["id"]))
        self.assertTrue(os.path.isdir(self.mgr.workspace_dir(e1["id"])))

    def test_snapshot(self):
        env = self.mgr.create("p1", {"name": "dev", "variables": {"X": "1"}})
        snap = self.mgr.snapshot(env["id"])
        self.assertEqual(snap["variables"]["X"], "1")


class TestCoverage(unittest.TestCase):
    def test_stable_per_build(self):
        with tempfile.TemporaryDirectory() as d:
            reg = BuildStoreRegistry(os.path.join(d, "builds"))
            reg.for_project("p1").create("b1")
            cov = CoverageAnalyzer(reg)
            c1 = cov.generate("p1", "b1", 0.8)
            c2 = cov.generate("p1", "b1", 0.8)
            self.assertEqual(c1["percent"], c2["percent"])  # 确定性
            self.assertGreaterEqual(c1["percent"], 0)
            self.assertLessEqual(c1["percent"], 100)

    def test_trend(self):
        with tempfile.TemporaryDirectory() as d:
            reg = BuildStoreRegistry(os.path.join(d, "builds"))
            cov = CoverageAnalyzer(reg)
            for bid in ("b1", "b2"):
                reg.for_project("p1").create(bid)
                cov.generate("p1", bid, 0.5)
            self.assertEqual(len(cov.trend("p1")["points"]), 2)


class TestReport(unittest.TestCase):
    def test_report_metrics(self):
        with tempfile.TemporaryDirectory() as d:
            reg = BuildStoreRegistry(os.path.join(d, "builds"))
            store = reg.for_project("p1")
            store.create("b1")
            store.set_total("b1", 4)
            for i in range(4):
                store.record_result("b1", {"case_id": f"c{i}", "case_name": f"c{i}",
                                           "group": "g", "priority": "P1",
                                           "status": "passed" if i < 3 else "failed",
                                           "duration": 0.1 + i * 0.1, "logs": []})
            store.finish("b1", "failed")
            rep = ReportGenerator(reg).build_report("p1", "b1")
            self.assertEqual(rep["summary"]["passed"], 3)
            self.assertEqual(rep["summary"]["pass_rate"], 75.0)
            self.assertEqual(len(rep["failures"]), 1)
            self.assertAlmostEqual(rep["durations"]["max"], 0.4, places=3)


class TestDefects(unittest.TestCase):
    def test_create_from_case(self):
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = DefectManager(reg)
            defect = mgr.create_from_case("p1", {
                "case_id": "c1", "case_name": "登录", "priority": "P0", "status": "failed",
                "assertions": [{"ok": False, "message": "期望 == 200"}], "steps": [],
            }, "b1")
            self.assertIsNotNone(defect)
            self.assertEqual(defect["source_case_id"], "c1")
            self.assertEqual(mgr.stats("p1")["total"], 1)


class TestNotify(unittest.TestCase):
    def test_fire_and_events(self):
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            integration = mgr.create("p1", {"type": "webhook", "config": {"url": "http://x"},
                                            "events": ["build.failed"]})
            fired = mgr.fire("p1", "build.passed", {"build_id": "b1"})
            self.assertEqual(fired, [])  # 未订阅 build.passed
            fired = mgr.fire("p1", "build.failed", {"build_id": "b1"})
            self.assertEqual(len(fired), 1)
            self.assertEqual(fired[0]["status"], "delivered")
            self.assertEqual(len(mgr.events("p1")), 1)

    def test_test_delivery_without_target(self):
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            integration = mgr.create("p1", {"type": "webhook", "config": {},
                                            "max_attempts": 1})
            result = mgr.send_test(integration["id"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(len(result["attempts"]), 1)

    def test_render_template_placeholders(self):
        from engine.notify import render_template
        out, missing = render_template(
            "${project_name} ${pass_rate}%", {"project_name": "P", "pass_rate": 90})
        self.assertEqual(out, "P 90%")
        self.assertEqual(missing, [])
        out, missing = render_template("${project_name} ${unknown}", {"project_name": "P"})
        self.assertEqual(out, "P ${unknown}")
        self.assertEqual(missing, ["unknown"])

    def test_per_channel_templates(self):
        """同一事件发往多个渠道，每个渠道使用各自的默认模板。"""
        from engine.notify import build_context
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            wb = mgr.create("p1", {"type": "webhook", "config": {"url": "http://x"},
                                   "events": ["build.failed"]})
            sl = mgr.create("p1", {"type": "slack", "config": {"url": "http://s"},
                                   "events": ["build.failed"]})
            ctx = build_context("build.failed", {"project_name": "Proj", "build_id": "b1"})
            w_title = mgr.get_template(wb, "build.failed")["title"]
            s_title = mgr.get_template(sl, "build.failed")["title"]
            self.assertIn("CI", w_title)
            self.assertIn(":x:", s_title)
            self.assertNotEqual(w_title, s_title)
            # 自定义模板覆盖默认
            mgr.update(sl["id"], {"templates": {"build.failed": {
                "title": "自定义 ${project_name}", "body": "b"}}})
            rec = mgr.fire("p1", "build.failed", {"project_name": "Proj", "build_id": "b9"})
            sl_rec = [r for r in rec if r["integration_id"] == sl["id"]][0]
            self.assertEqual(sl_rec["title"], "自定义 Proj")

    def test_quiet_hours_enqueue_and_digest(self):
        from engine.notify import is_quiet, next_quiet_end
        qh = {"enabled": True, "start": "22:00", "end": "08:00"}
        self.assertTrue(is_quiet(qh, datetime.datetime(2026, 10, 7, 3, 0)))
        self.assertTrue(is_quiet(qh, datetime.datetime(2026, 10, 7, 23, 0)))
        self.assertFalse(is_quiet(qh, datetime.datetime(2026, 10, 7, 10, 0)))
        end = next_quiet_end(qh, datetime.datetime(2026, 10, 7, 3, 0))
        self.assertEqual(datetime.datetime.fromtimestamp(end).hour, 8)

        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            mgr.create("p1", {"type": "webhook", "config": {"url": "http://x"},
                              "events": ["build.finished"], "quiet_hours": qh})
            # 凌晨 3 点：入队，不投递
            t3 = datetime.datetime(2026, 10, 7, 3, 0).timestamp()
            recs = mgr.fire("p1", "build.finished",
                            {"build_id": "b1", "project_name": "P"}, at=t3)
            self.assertEqual(recs[0]["status"], "queued")
            self.assertEqual(mgr.pending_summary(at=t3)["queued"], 1)
            # 静默未结束：flush 不补发
            r = mgr.flush_due(at=t3 + 60)
            self.assertEqual(r["digests"], [])
            # 又来一条，静默期内合并
            t5 = datetime.datetime(2026, 10, 7, 5, 0).timestamp()
            mgr.fire("p1", "build.finished", {"build_id": "b2"}, at=t5)
            # 8 点后：合并成一条 digest
            t8 = datetime.datetime(2026, 10, 7, 8, 0).timestamp()
            r = mgr.flush_due(at=t8)
            self.assertEqual(len(r["digests"]), 1)
            digest = reg.store("notify_events").get(r["digests"][0])
            self.assertEqual(digest["event"], "digest")
            self.assertEqual(digest["status"], "delivered")
            self.assertEqual(digest["payload"]["count"], 2)
            self.assertIn("2 条通知", digest["title"])
            self.assertEqual(digest["payload"]["build_ids"], ["b1", "b2"])
            # 原始两条事件都标记为已合并补发
            statuses = sorted(e["status"] for e in mgr.events("p1") if e["event"] == "build.finished")
            self.assertEqual(statuses, ["digested", "digested"])

    def test_force_flush_during_quiet(self):
        qh = {"enabled": True, "start": "22:00", "end": "08:00"}
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            mgr.create("p1", {"type": "webhook", "config": {"url": "http://x"},
                              "events": ["build.finished"], "quiet_hours": qh})
            t3 = datetime.datetime(2026, 10, 7, 3, 0).timestamp()
            mgr.fire("p1", "build.finished", {"build_id": "b1"}, at=t3)
            r = mgr.flush_project("p1", force=True, at=t3 + 10)
            self.assertEqual(len(r["digests"]), 1)

    def test_retry_flow(self):
        """失败投递自动重试，每次尝试留痕；手动重试可再试。"""
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            # 未配置目标 → 确定性失败
            mgr.create("p1", {"type": "webhook", "config": {},
                              "events": ["build.finished"], "max_attempts": 3})
            base = datetime.datetime(2026, 10, 7, 10, 0).timestamp()
            recs = mgr.fire("p1", "build.finished", {"build_id": "b1"}, at=base)
            rec = recs[0]
            self.assertEqual(rec["status"], "retrying")
            self.assertEqual(len(rec["attempts"]), 1)
            self.assertIsNotNone(rec["next_retry_at"])
            # 退避时间未到：不重试
            self.assertEqual(mgr.flush_due(at=base + 10)["retries"], [])
            # 到期后第 2 次仍失败
            r2 = mgr.flush_due(at=base + 40)
            self.assertEqual(len(r2["retries"]), 1)
            rec = reg.store("notify_events").get(rec["id"])
            self.assertEqual(rec["status"], "retrying")
            self.assertEqual(len(rec["attempts"]), 2)
            # 第 3 次（上限）失败 → 最终失败
            r3 = mgr.flush_due(at=base + 400)
            self.assertEqual(len(r3["retries"]), 1)
            rec = reg.store("notify_events").get(rec["id"])
            self.assertEqual(rec["status"], "failed")
            self.assertEqual(len(rec["attempts"]), 3)
            self.assertTrue(all(a["status"] == "failed" for a in rec["attempts"]))
            self.assertTrue(all(a["error"] for a in rec["attempts"]))
            # 手动再试一次，仍然失败，但尝试次数 +1
            again = mgr.retry_event(rec["id"])
            self.assertEqual(again["status"], "failed")
            self.assertEqual(len(again["attempts"]), 4)

    def test_digest_retry_then_success(self):
        """合并补发本身失败时，digest 走重试，静默项在成功后才标记 sent。"""
        qh = {"enabled": True, "start": "22:00", "end": "08:00"}
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            mgr.create("p1", {"type": "webhook", "config": {},  # 目标缺失 → 失败
                              "events": ["build.finished"], "quiet_hours": qh})
            t3 = datetime.datetime(2026, 10, 7, 3, 0).timestamp()
            mgr.fire("p1", "build.finished", {"build_id": "b1"}, at=t3)
            r = mgr.flush_due(at=datetime.datetime(2026, 10, 7, 8, 0).timestamp())
            digest = reg.store("notify_events").get(r["digests"][0])
            self.assertEqual(digest["status"], "retrying")
            # 静默项保持 sending，不会被二次合并
            quiet = reg.store("notify_queue").all()
            self.assertTrue(all(q["status"] == "sending" for q in quiet if q["kind"] == "quiet"))
            # 配好目标后，重试成功
            integration = mgr.list("p1")[0]
            mgr.update(integration["id"], {"config": {"url": "http://now-ok"}})
            r2 = mgr.flush_due(at=datetime.datetime(2026, 10, 7, 8, 10).timestamp())
            self.assertEqual(len(r2["retries"]), 1)
            digest = reg.store("notify_events").get(digest["id"])
            self.assertEqual(digest["status"], "delivered")
            quiet = reg.store("notify_queue").all()
            self.assertTrue(all(q["status"] == "sent" for q in quiet if q["kind"] == "quiet"))

    def test_preview(self):
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            r = mgr.preview("slack", "build.failed",
                            "构建挂了 ${project_name}", "通过率 ${pass_rate}%")
            self.assertIn("演示项目", r["title"])
            self.assertEqual(r["missing"], [])
            r2 = mgr.preview("webhook", "build.finished", "${no_such_field}", "b")
            self.assertIn("no_such_field", r2["missing"])

    def test_quiet_hours_validation(self):
        with tempfile.TemporaryDirectory() as d:
            reg = StoreRegistry(os.path.join(d, "store"))
            mgr = NotificationManager(reg)
            with self.assertRaises(ValueError):
                mgr.create("p1", {"quiet_hours": {"enabled": True,
                                                  "start": "25:00", "end": "08:00"}})
            with self.assertRaises(ValueError):
                mgr.create("p1", {"quiet_hours": {"enabled": True,
                                                  "start": "08:00", "end": "08:00"}})
            with self.assertRaises(ValueError):
                mgr.create("p1", {"max_attempts": 9})


if __name__ == "__main__":
    unittest.main()
