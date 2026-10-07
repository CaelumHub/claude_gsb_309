"""通知相关 REST API 测试（需要 Flask；无 Flask 环境自动跳过）。"""

from __future__ import annotations

import datetime
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from flask import Flask
    from web.routes import api as api_bp
    from engine import NotificationManager
    from storage import StoreRegistry
except ImportError:  # 运行环境未装 Flask 时跳过（核心逻辑由 test_notify 覆盖）
    api_bp = None

if api_bp is not None:

    def _ts(y, mo, d, h, mi=0):
        return datetime.datetime(y, mo, d, h, mi).timestamp()

    class TestNotifyApi(unittest.TestCase):
        def setUp(self):
            self.tmp = tempfile.TemporaryDirectory()
            self.registry = StoreRegistry(os.path.join(self.tmp.name, "store"))
            self.registry.store("projects").insert({"id": "p1", "name": "P"})
            self.notify = NotificationManager(self.registry)
            self.app = Flask(__name__)
            self.app.register_blueprint(api_bp)
            self.app.config["NOTIFY"] = self.notify
            self.app.config["STORE_REGISTRY"] = self.registry

        def tearDown(self):
            self.tmp.cleanup()

        @property
        def client(self):
            return self.app.test_client()

        def _payload(self):
            return {"build_id": "b1", "project_id": "p1", "project_name": "P",
                    "status": "failed", "passed": 1, "total": 2,
                    "pass_rate": 50.0, "duration": 3.2,
                    "failed_cases": "登录接口"}

        def test_defaults_and_preview(self):
            c = self.client
            r = c.get("/api/notify/templates/defaults")
            self.assertEqual(r.status_code, 200)
            self.assertIn("slack", r.get_json()["templates"])
            r = c.post("/api/notify/templates/preview",
                       json={"type": "email", "event": "build.passed"})
            body = r.get_json()
            self.assertNotIn("${", body["title"])
            self.assertIn("87.5", body["body"])  # 样例通过率已渲染

        def test_create_with_templates_quiet_and_queue(self):
            c = self.client
            r = c.post("/api/projects/p1/integrations", json={
                "type": "slack", "name": "S", "config": {"url": "http://s"},
                "events": ["build.finished"],
                "templates": {"build.finished": {"title": "T ${project_name}",
                                                 "body": "B"}}})
            self.assertEqual(r.status_code, 200)
            int_id = r.get_json()["id"]

            # 开启静默
            r = c.put("/api/projects/p1/quiet",
                      json={"enabled": True, "start": "22:00", "end": "08:00"})
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.get_json()["in_quiet"] is False)

            # 夜间事件入队
            self.notify.fire("p1", "build.finished", self._payload(),
                             at=_ts(2026, 10, 7, 3, 0))
            r = c.get("/api/projects/p1/queue")
            self.assertEqual(len(r.get_json()["queue"]), 1)

            # 8 点合并补发
            r = c.post("/api/projects/p1/quiet/flush")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.get_json()["digests"], 1)

            # 事件日志含 deferred 与 digest
            events = c.get("/api/projects/p1/events").get_json()["events"]
            statuses = {e["status"] for e in events}
            self.assertIn("deferred", statuses)
            self.assertTrue(any(e["event"] == "quiet.digest" for e in events))

        def test_invalid_quiet_returns_400(self):
            r = self.client.put("/api/projects/p1/quiet",
                                json={"enabled": True, "start": "xx",
                                      "end": "08:00"})
            self.assertEqual(r.status_code, 400)

        def test_retry_dead_item(self):
            self.notify.max_attempts = 1
            c = self.client
            r = c.post("/api/projects/p1/integrations", json={
                "type": "webhook", "name": "坏", "config": {},
                "events": ["build.failed"]})
            int_id = r.get_json()["id"]
            self.notify.fire("p1", "build.failed", self._payload(),
                             at=_ts(2026, 10, 7, 10, 0))
            dead = self.notify.queue("p1")
            self.assertEqual(dead[0]["status"], "dead")

            # 修复目标后手动重试
            c.put(f"/api/integrations/{int_id}",
                  json={"config": {"url": "http://ok"}})
            r = c.post(f"/api/queue/{dead[0]['id']}/retry")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.get_json()["status"], "delivered")

        def test_retry_missing_item_400(self):
            r = self.client.post("/api/queue/nope/retry")
            self.assertEqual(r.status_code, 400)

        def test_type_change_clears_templates(self):
            c = self.client
            r = c.post("/api/projects/p1/integrations", json={
                "type": "slack", "config": {"url": "http://s"},
                "templates": {"build.failed": {"title": "X", "body": "Y"}}})
            int_id = r.get_json()["id"]
            r = c.put(f"/api/integrations/{int_id}", json={"type": "email"})
            self.assertEqual(r.get_json()["templates"], {})


if __name__ == "__main__":
    unittest.main()
