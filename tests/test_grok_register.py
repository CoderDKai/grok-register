import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from threading import Thread
from unittest.mock import patch

import grok_register as register


class ConfigTests(unittest.TestCase):
    def test_load_run_config_reads_count_and_workers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "run": {
                            "count": 23,
                            "max_workers": 7,
                            "remote_push_batch_size": 6,
                            "output_mode": "full",
                        }
                    }
                ),
                encoding="utf-8",
            )

            run_conf = register.load_run_config(config_path)

        self.assertEqual(run_conf["count"], 23)
        self.assertEqual(run_conf["max_workers"], 7)
        self.assertEqual(run_conf["remote_push_batch_size"], 6)
        self.assertEqual(run_conf["output_mode"], "full")


class OutputModeTests(unittest.TestCase):
    def test_write_success_output_writes_full_account_when_output_mode_full(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sso_file = Path(tmpdir) / "sso.txt"
            grok_file = Path(tmpdir) / "grok.txt"

            with patch.object(register, "SSO_FILE", str(sso_file)), patch.object(register, "GROK_FILE", str(grok_file)), patch.object(
                register, "OUTPUT_MODE", "full"
            ), patch.object(register, "queue_remote_token", return_value=True) as queue_mock:
                register.write_success_output(
                    email_address="boss@example.com",
                    grok_password="Secret!123",
                    sso_val="token-123",
                    sso_rw_val="rw-456",
                )

            self.assertEqual(sso_file.read_text(encoding="utf-8"), "token-123\n")
            grok_text = grok_file.read_text(encoding="utf-8")
            self.assertIn("Email: boss@example.com", grok_text)
            self.assertIn("Password: Secret!123", grok_text)
            self.assertIn("SSO: token-123", grok_text)
            self.assertIn("SSO-RW: rw-456", grok_text)
            queue_mock.assert_called_once_with("token-123")


class RunJobTimeoutTests(unittest.TestCase):
    def test_run_job_returns_false_when_async_job_ignores_timeout(self):
        async def hanging_job(thread_id, task_id, timeout_sec=180):
            await asyncio.sleep(1)
            return True

        start = time.monotonic()
        with patch.object(register, "async_run_job", new=hanging_job), patch.object(
            register, "JOB_TIMEOUT_GRACE_SEC", 0.05
        ):
            result = register.run_job(1, 1, timeout_sec=0.05)
        elapsed = time.monotonic() - start

        self.assertFalse(result)
        self.assertLess(elapsed, 0.5)


class CleanupTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_safe_await_during_cleanup_times_out(self):
        logs = []

        async def hanging_cleanup():
            await asyncio.sleep(1)

        await register.safe_await_during_cleanup(
            hanging_cleanup(),
            timeout_sec=0.05,
            label="page.close",
            log=logs.append,
        )

        self.assertTrue(any("page.close" in msg and "超时" in msg for msg in logs))


class WorkerTaskStatsTests(unittest.TestCase):
    def test_worker_task_does_not_deadlock_when_printing_stats(self):
        original_stats = register.stats.copy()
        register.stats.update({"total": 9, "success": 0, "failed": 0, "start_time": time.time()})

        worker = Thread(target=register.worker_task, args=(((0, 10),)), daemon=True)

        with patch.object(register, "run_job", return_value=True), patch.object(register.time, "sleep", return_value=None):
            worker.start()
            worker.join(timeout=0.2)

        try:
            self.assertFalse(worker.is_alive(), "worker_task 在第10个任务完成后发生死锁")
        finally:
            register.stats.clear()
            register.stats.update(original_stats)


class RemotePushBatchingTests(unittest.TestCase):
    def setUp(self):
        self.original_tokens = list(register.pending_remote_tokens)

    def tearDown(self):
        register.pending_remote_tokens[:] = self.original_tokens

    def test_queue_remote_token_flushes_when_reaching_batch_size(self):
        register.pending_remote_tokens[:] = [f"token-{i}" for i in range(9)]

        with patch.object(register, "push_tokens_to_remote", return_value=True) as push_mock:
            register.queue_remote_token("token-9")

        push_mock.assert_called_once_with([f"token-{i}" for i in range(10)])
        self.assertEqual(register.pending_remote_tokens, [])

    def test_flush_remote_tokens_pushes_remaining_tokens_on_finish(self):
        register.pending_remote_tokens[:] = ["a", "b", "c"]

        with patch.object(register, "push_tokens_to_remote", return_value=True) as push_mock:
            register.flush_remote_tokens(force=True)

        push_mock.assert_called_once_with(["a", "b", "c"])
        self.assertEqual(register.pending_remote_tokens, [])

    def test_push_tokens_to_remote_success_log_has_checkmark_prefix(self):
        class Response:
            status_code = 200
            text = "ok"

        with patch.object(register, "load_api_config", return_value={"endpoint": "https://example.com", "token": "abc"}), patch.object(
            register.requests, "post", return_value=Response()
        ), patch("builtins.print") as print_mock:
            result = register.push_tokens_to_remote(["token-1"])

        self.assertTrue(result)
        print_mock.assert_any_call("✅ [远程推送] 已追加 1 个 token")


class RunMultiThreadTests(unittest.TestCase):
    def test_run_multi_thread_processes_all_tasks(self):
        seen = []

        def fake_worker(task):
            seen.append(task)
            return True

        with patch.object(register, "worker_task", side_effect=fake_worker):
            success_count = register.run_multi_thread(12, max_workers=3)

        self.assertEqual(success_count, 12)
        self.assertEqual(len(seen), 12)


if __name__ == "__main__":
    unittest.main()
