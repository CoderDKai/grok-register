import asyncio
import json
import re
import time
import random
import gc
import os
import string
import threading
import tempfile
import traceback
from concurrent.futures import FIRST_COMPLETED, CancelledError, ThreadPoolExecutor, wait
from datetime import datetime as dt
from html import unescape
from typing import Dict, List, Optional, Tuple

import requests
from camoufox.async_api import AsyncCamoufox

# 导入项目的邮件系统
from email_register import create_temp_email, wait_for_verification_code

os.environ["NO_PROXY"] = "localhost,127.0.0.1"
os.environ["no_proxy"] = "localhost,127.0.0.1"

SIGNUP_URL = "https://accounts.x.ai/sign-up?redirect=grok-com"

file_lock = threading.Lock()
stats_lock = threading.Lock()
remote_push_lock = threading.Lock()
timestamp = dt.now().strftime("%m%d%H%M")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
GROK_DIR = os.path.join(SCRIPT_DIR, "result_grok")
SSO_DIR = os.path.join(SCRIPT_DIR, "sso")
DEBUG_DIR = os.path.join(tempfile.gettempdir(), "grok_debug_headless")
os.makedirs(GROK_DIR, exist_ok=True)
os.makedirs(SSO_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

GROK_FILE = os.path.join(GROK_DIR, f"grok_hl_{timestamp}.txt")
SSO_FILE = os.path.join(SSO_DIR, f"sso_{timestamp}.txt")

first_names = ["James", "John", "Robert", "Michael", "William",
               "David", "Richard", "Joseph", "Thomas", "Charles"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones",
              "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

# 邮箱缓存: email -> mail_token
_temp_email_cache: Dict[str, str] = {}

# 全局统计
stats = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "start_time": time.time(),
}

RUN_COUNT = 10
RUN_MAX_WORKERS = 1
RUN_TIMEOUT_SEC = 180
RUN_MAX_RETRIES = 3
RUN_STARTUP_DELAY_RANGE = (2, 8)
RUN_RETRY_WAIT_RANGE = (10, 30)
RUN_STATS_EVERY = 10
RUN_PROGRESS_EVERY = 50
JOB_TIMEOUT_GRACE_SEC = 20
CLEANUP_TIMEOUT_SEC = 5
REMOTE_PUSH_BATCH_SIZE = 10
OUTPUT_MODE = "sso"
pending_remote_tokens: List[str] = []


def generate_password(length=14):
    upper = random.choice(string.ascii_uppercase)
    lower = random.choice(string.ascii_lowercase)
    digit = random.choice(string.digits)
    special = random.choice("!@#$%&*")
    rest = random.choices(
        string.ascii_letters + string.digits + "!@#$%&*",
        k=length - 4,
    )
    chars = list(upper + lower + digit + special + "".join(rest))
    random.shuffle(chars)
    return "".join(chars)


def load_json_config(config_path=CONFIG_PATH) -> Dict:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_run_config(config_path=CONFIG_PATH) -> Dict[str, object]:
    conf = load_json_config(config_path)
    run_conf = conf.get("run", {}) if isinstance(conf, dict) else {}
    return {
        "count": int(run_conf.get("count", RUN_COUNT)),
        "max_workers": int(run_conf.get("max_workers", RUN_MAX_WORKERS)),
        "timeout_sec": int(run_conf.get("timeout_sec", RUN_TIMEOUT_SEC)),
        "max_retries": int(run_conf.get("max_retries", RUN_MAX_RETRIES)),
        "startup_delay_min_sec": float(run_conf.get("startup_delay_min_sec", RUN_STARTUP_DELAY_RANGE[0])),
        "startup_delay_max_sec": float(run_conf.get("startup_delay_max_sec", RUN_STARTUP_DELAY_RANGE[1])),
        "retry_wait_min_sec": float(run_conf.get("retry_wait_min_sec", RUN_RETRY_WAIT_RANGE[0])),
        "retry_wait_max_sec": float(run_conf.get("retry_wait_max_sec", RUN_RETRY_WAIT_RANGE[1])),
        "stats_every": int(run_conf.get("stats_every", RUN_STATS_EVERY)),
        "progress_every": int(run_conf.get("progress_every", RUN_PROGRESS_EVERY)),
        "job_timeout_grace_sec": float(run_conf.get("job_timeout_grace_sec", JOB_TIMEOUT_GRACE_SEC)),
        "cleanup_timeout_sec": float(run_conf.get("cleanup_timeout_sec", CLEANUP_TIMEOUT_SEC)),
        "remote_push_batch_size": int(run_conf.get("remote_push_batch_size", REMOTE_PUSH_BATCH_SIZE)),
        "output_mode": str(run_conf.get("output_mode", OUTPUT_MODE)).strip().lower() or OUTPUT_MODE,
        "signup_url": str(run_conf.get("signup_url", SIGNUP_URL)).strip() or SIGNUP_URL,
    }


def load_api_config(config_path=CONFIG_PATH) -> Dict[str, str]:
    conf = load_json_config(config_path)
    api_conf = conf.get("api", {}) if isinstance(conf, dict) else {}
    return {
        "endpoint": str(api_conf.get("endpoint", "")).strip(),
        "token": str(api_conf.get("token", "")).strip(),
    }


def configure_runtime(config_path=CONFIG_PATH) -> Dict[str, object]:
    global SIGNUP_URL
    global RUN_COUNT, RUN_MAX_WORKERS, RUN_TIMEOUT_SEC, RUN_MAX_RETRIES
    global RUN_STARTUP_DELAY_RANGE, RUN_RETRY_WAIT_RANGE, RUN_STATS_EVERY, RUN_PROGRESS_EVERY
    global JOB_TIMEOUT_GRACE_SEC, CLEANUP_TIMEOUT_SEC, REMOTE_PUSH_BATCH_SIZE, OUTPUT_MODE

    run_conf = load_run_config(config_path)
    RUN_COUNT = max(1, int(run_conf["count"]))
    RUN_MAX_WORKERS = max(1, int(run_conf["max_workers"]))
    RUN_TIMEOUT_SEC = max(1, int(run_conf["timeout_sec"]))
    RUN_MAX_RETRIES = max(1, int(run_conf["max_retries"]))
    RUN_STARTUP_DELAY_RANGE = (
        float(run_conf["startup_delay_min_sec"]),
        float(run_conf["startup_delay_max_sec"]),
    )
    RUN_RETRY_WAIT_RANGE = (
        float(run_conf["retry_wait_min_sec"]),
        float(run_conf["retry_wait_max_sec"]),
    )
    RUN_STATS_EVERY = max(1, int(run_conf["stats_every"]))
    RUN_PROGRESS_EVERY = max(1, int(run_conf["progress_every"]))
    JOB_TIMEOUT_GRACE_SEC = max(0, float(run_conf["job_timeout_grace_sec"]))
    CLEANUP_TIMEOUT_SEC = max(0.1, float(run_conf["cleanup_timeout_sec"]))
    REMOTE_PUSH_BATCH_SIZE = max(1, int(run_conf["remote_push_batch_size"]))
    OUTPUT_MODE = "full" if str(run_conf["output_mode"]) == "full" else "sso"
    SIGNUP_URL = str(run_conf["signup_url"])
    return run_conf


def print_stats():
    with stats_lock:
        elapsed = time.time() - stats["start_time"]
        success_rate = (stats["success"] / stats["total"] * 100) if stats["total"] > 0 else 0
        print(
            f"\n[统计] 进度: {stats['success']}/{stats['total']} ({success_rate:.1f}%) | "
            f"成功: {stats['success']} | 失败: {stats['failed']} | 耗时: {elapsed/60:.1f}分钟\n"
        )


def push_tokens_to_remote(tokens: List[str]) -> bool:
    normalized_tokens = [str(token).strip() for token in tokens if str(token).strip()]
    if not normalized_tokens:
        return True

    api_conf = load_api_config()
    endpoint = api_conf["endpoint"]
    api_token = api_conf["token"]
    if not endpoint or not api_token:
        return True

    try:
        resp = requests.post(
            endpoint,
            json={"default": normalized_tokens},
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        if resp.status_code == 200:
            print(f"✅ [远程推送] 已追加 {len(normalized_tokens)} 个 token")
            return True
        print(f"[远程推送] 失败: HTTP {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"[远程推送] 异常: {type(e).__name__}: {e}")
    return False


def flush_remote_tokens(force: bool = False) -> bool:
    batches: List[List[str]] = []
    with remote_push_lock:
        while len(pending_remote_tokens) >= REMOTE_PUSH_BATCH_SIZE:
            batches.append(pending_remote_tokens[:REMOTE_PUSH_BATCH_SIZE])
            del pending_remote_tokens[:REMOTE_PUSH_BATCH_SIZE]
        if force and pending_remote_tokens:
            batches.append(pending_remote_tokens[:])
            pending_remote_tokens.clear()

    for batch_tokens in batches:
        if not push_tokens_to_remote(batch_tokens):
            with remote_push_lock:
                pending_remote_tokens[:0] = batch_tokens
            return False

    return True


def queue_remote_token(token: str) -> bool:
    normalized = str(token or "").strip()
    if not normalized:
        return False
    with remote_push_lock:
        pending_remote_tokens.append(normalized)
    return flush_remote_tokens(force=False)


def write_success_output(email_address: str, grok_password: str, sso_val: str, sso_rw_val: str = "") -> None:
    with file_lock:
        if OUTPUT_MODE == "full":
            with open(GROK_FILE, "a", encoding="utf-8") as f:
                f.write(f"Email: {email_address}\n")
                f.write(f"Password: {grok_password}\n")
                f.write(f"SSO: {sso_val}\n")
                if sso_rw_val:
                    f.write(f"SSO-RW: {sso_rw_val}\n")
                f.write("-" * 40 + "\n")
                f.flush()
                os.fsync(f.fileno())

        with open(SSO_FILE, "a", encoding="utf-8") as f:
            f.write(f"{sso_val}\n")
            f.flush()
            os.fsync(f.fileno())

    queue_remote_token(sso_val)


async def safe_await_during_cleanup(awaitable, timeout_sec, label, log):
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_sec)
    except asyncio.TimeoutError:
        log(f"[清理] {label} 超时({timeout_sec}s)，跳过")
    except Exception as e:
        log(f"[清理] {label} 异常: {type(e).__name__}: {e}")
    return None


async def async_run_job(thread_id, task_id, timeout_sec=180):
    prefix = f"[T{thread_id}-#{task_id}]"
    job_start = time.time()
    email_address = None
    mail_token = None

    def log(msg):
        elapsed = time.time() - job_start
        print(f"{prefix} [{elapsed:5.1f}s] {msg}")

    def check_timeout():
        if time.time() - job_start > timeout_sec:
            raise TimeoutError("Timeout")

    camoufox_obj = None
    browser = None
    context = None
    page = None

    async def acquire_email(step_label: str, max_attempts: int = 5) -> bool:
        nonlocal email_address, mail_token

        for attempt in range(max_attempts):
            try:
                # 使用项目邮件系统创建临时邮箱
                email, password, token = await asyncio.to_thread(create_temp_email)
                if not email or not token:
                    log(f"{step_label} 创建邮箱失败，重试...")
                    await asyncio.sleep(2)
                    continue

                email_address = email
                mail_token = token
                _temp_email_cache[email] = token
                log(f"{step_label} ✓ 邮箱: {email_address}")
                return True
            except Exception as e:
                log(f"{step_label} 创建邮箱异常: {e}")

            if attempt < max_attempts - 1:
                await asyncio.sleep(2)

        return False

    async def submit_email_address(step_label: str, current_email: str) -> None:
        email_input = page.locator('input[name="email"]')
        await email_input.wait_for(timeout=10000)
        await email_input.click()
        try:
            await email_input.fill("")
        except Exception:
            pass
        await email_input.type(current_email, delay=random.randint(30, 70))
        await asyncio.sleep(0.5)
        await email_input.press("Enter")
        log(f"{step_label} 邮箱已提交")
        await asyncio.sleep(2)

    async def read_verify_page_email() -> str:
        try:
            result = await page.evaluate('''() => {
                const text = document.body ? document.body.innerText : '';
                const match = text.match(/We(?:'|')ve emailed .*? to\\s+([^\\s]+@[^\\s]+)\\.?/i);
                return match ? match[1] : '';
            }''')
            return (result or "").strip().lower().rstrip('.')
        except Exception:
            return ""

    async def wait_for_verify_email_match(expected_email: str, timeout_ms: int = 8000) -> bool:
        expected = (expected_email or "").strip().lower()
        deadline = time.time() + timeout_ms / 1000

        while time.time() < deadline:
            shown_email = await read_verify_page_email()
            if shown_email:
                log(f"[步骤4] 页面显示待验证邮箱: {shown_email}")
                if shown_email == expected:
                    return True
            await asyncio.sleep(0.5)

        return False

    async def dump_signup_debug(label: str) -> None:
        try:
            title = await page.title()
            current_url = page.url
            body = await page.evaluate(
                'document.body ? document.body.innerText.substring(0, 800) : "NO_BODY"'
            )
            visible_input_names = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll("input"))
                    .filter((el) => {
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
                    })
                    .map((el) => ({
                        name: el.name || "",
                        id: el.id || "",
                        type: el.type || "",
                        valueLength: (el.value || "").length,
                        autocomplete: el.autocomplete || "",
                        inputmode: el.inputMode || "",
                        maxlength: el.maxLength || 0,
                    }));
            }''')
            log(f"[诊断:{label}] Title={title}")
            log(f"[诊断:{label}] URL={current_url}")
            log(f"[诊断:{label}] 可见输入框={visible_input_names}")
            log(f"[诊断:{label}] 页面文本={body[:500]}")
        except Exception as e:
            log(f"[诊断:{label}] 获取页面状态失败: {type(e).__name__}: {e}")

    async def detect_code_error_message() -> Optional[str]:
        try:
            result = await page.evaluate('''() => {
                const text = document.body ? document.body.innerText : '';
                const lines = text.split(/\\n+/).map((line) => line.trim()).filter(Boolean);
                const hit = lines.find((line) => /((incorrect|invalid|wrong).{0,20}code)|(code.{0,20}(incorrect|invalid|wrong))/i.test(line));
                return hit || '';
            }''')
            return (result or "").strip() or None
        except Exception:
            return None

    async def clear_code_inputs() -> None:
        try:
            await page.evaluate('''() => {
                const inputs = Array.from(document.querySelectorAll("input"));
                for (const input of inputs) {
                    const name = input.name || "";
                    const id = input.id || "";
                    const isCodeInput =
                        input.autocomplete === "one-time-code" ||
                        input.inputMode === "numeric" ||
                        input.maxLength === 1 ||
                        /code/i.test(name) ||
                        /code/i.test(id);
                    if (!isCodeInput) continue;
                    input.focus();
                    input.value = "";
                    input.dispatchEvent(new Event("input", { bubbles: true }));
                    input.dispatchEvent(new Event("change", { bubbles: true }));
                }
            }''')
        except Exception:
            pass

    try:
        log("[步骤1] 创建临时邮箱...")
        if not await acquire_email("[步骤1]"):
            log("[步骤1] ❌ 邮箱创建失败(5次重试)")
            return False

        grok_password = generate_password()
        fname = random.choice(first_names)
        lname = random.choice(last_names)

        check_timeout()
        log("[步骤2] 启动 Camoufox (headless)...")
        selected_os = random.choice(["windows", "macos", "linux"])

        camoufox_obj = AsyncCamoufox(
            headless=True,
            os=selected_os,
            locale="en-US",
            humanize=False,
            geoip=False,
            i_know_what_im_doing=True,
            block_webrtc=True,
            disable_coop=True,
        )
        browser = await camoufox_obj.__aenter__()
        context = await browser.new_context()
        page = await context.new_page()

        ua = await page.evaluate("navigator.userAgent")
        log(f"[步骤2] ✓ Camoufox 就绪 (OS={selected_os}, UA={ua[:50]}...)")

        check_timeout()
        log("[步骤3] 打开注册页面...")
        max_redirect_retries = 3
        for redirect_attempt in range(max_redirect_retries):
            try:
                await page.goto(SIGNUP_URL, wait_until="domcontentloaded", timeout=30000)
                break
            except Exception as e:
                error_str = str(e)
                if "NS_ERROR_REDIRECT_LOOP" in error_str or "redirect" in error_str.lower():
                    log(f"[步骤3] ⚠ 重定向循环，第{redirect_attempt+1}次重试...")
                    await asyncio.sleep(2)
                    if redirect_attempt < max_redirect_retries - 1:
                        continue
                raise
        await asyncio.sleep(3)

        title = await page.title()
        url = page.url
        log(f"[步骤3] Title: {title}")
        log(f"[步骤3] URL: {url}")

        for cf_wait in range(30):
            has_signup = await page.locator(
                "button:has-text('Sign up with email'), "
                "button:has-text('Sign up with X'), "
                "input[name='email']"
            ).count()
            if has_signup > 0:
                log(f"[步骤3] ✓ 注册页面已加载 ({cf_wait}s)")
                break
            if cf_wait == 0:
                log("[步骤3] 等待页面内容加载...")
            if cf_wait % 5 == 4:
                title = await page.title()
                log(f"[步骤3] 等待中... Title={title}")
            await asyncio.sleep(1)
        else:
            log("[步骤3] ⚠ 30s内未检测到注册页面元素")
            await page.screenshot(path=os.path.join(DEBUG_DIR, f"cf_T{thread_id}.png"))
            title = await page.title()
            log(f"[诊断] Title={title}, URL={page.url}")

        await page.screenshot(path=os.path.join(DEBUG_DIR, f"step3_T{thread_id}.png"))

        try:
            btn = page.locator("button:has-text('Sign up with email')")
            await btn.click(timeout=5000)
            log("[步骤3] 点击 'Sign up with email'")
            await asyncio.sleep(1)
        except Exception:
            log("[步骤3] 'Sign up with email' 未找到，继续")

        check_timeout()
        log("[步骤3] 填入邮箱...")
        await submit_email_address("[步骤3]", email_address)

        check_timeout()
        log("[步骤4] 等待验证码...")

        code_filled = False
        code_fetch_started = False
        tried_codes = set()
        last_entered_code = None
        
        for attempt in range(40):
            check_timeout()

            if await page.locator('input[name="password"]').count() > 0:
                if code_fetch_started and not code_filled:
                    log("[步骤4] ✓ 验证码已通过，密码页已出现")
                log("[步骤4] ✓ 已到密码页面")
                code_filled = True
                break

            if not code_filled:
                if code_fetch_started:
                    code_error = await detect_code_error_message()
                    if code_error:
                        if last_entered_code:
                            tried_codes.add(last_entered_code)
                        log(f"[步骤4] ⚠ 页面提示验证码错误: {code_error}")
                        log(f"[步骤4] ⚠ 判错验证码={last_entered_code or '-'}，当前邮箱={email_address}")
                        await clear_code_inputs()
                        code_fetch_started = False
                        last_entered_code = None
                        await asyncio.sleep(1)
                        continue

                visible_inputs = page.locator("input:visible")
                input_count = await visible_inputs.count()
                email_input_visible = await page.locator('input[name="email"]:visible').count() > 0
                code_inputs = page.locator(
                    "input[autocomplete='one-time-code'], "
                    "input[inputmode='numeric'], "
                    "input[maxlength='1'], "
                    "input[name*='code'], "
                    "input[id*='code']"
                )
                code_input_count = await code_inputs.count()

                ready_for_code = code_input_count > 0 or (input_count > 0 and not email_input_visible)

                if not ready_for_code:
                    if email_input_visible and attempt in (2, 5, 9):
                        log("[步骤4] 仍停留在邮箱页，重试提交邮箱...")
                        await submit_email_address("[步骤4]", email_address)
                        await asyncio.sleep(2)

                    if attempt % 5 == 4:
                        log("[步骤4] 等待验证码输入框出现...")
                    await asyncio.sleep(1)
                    continue

                if ready_for_code and not code_fetch_started:
                    verify_email = await read_verify_page_email()
                    if verify_email and verify_email != email_address.lower():
                        log(f"[步骤4] ⚠ 页面待验证邮箱与当前邮箱不一致: 页面={verify_email}, 当前={email_address}")
                        await asyncio.sleep(1)
                        continue

                    if not verify_email:
                        matched = await wait_for_verify_email_match(email_address, timeout_ms=5000)
                        if not matched:
                            log(f"[步骤4] ⚠ 页面未确认切换到当前邮箱 {email_address}，继续等待")
                            await asyncio.sleep(1)
                            continue

                    remaining = int(timeout_sec - (time.time() - job_start) - 20)
                    if remaining <= 0:
                        log("[步骤4] ❌ 剩余时间不足，无法继续等待验证码")
                        return False

                    code_timeout = min(120, remaining)
                    log(f"[步骤4] 开始拉取验证码（超时 {code_timeout}s）...")
                    code_fetch_started = True
                    
                    # 使用项目邮件系统获取验证码
                    code = await asyncio.to_thread(
                        wait_for_verification_code,
                        mail_token=mail_token,
                        timeout=code_timeout,
                    )

                    if code:
                        log(f"[步骤4] ✓ 验证码: {code}")
                        log(f"[步骤4] 当前验证码对应邮箱: {email_address}")
                        target_input = code_inputs.first if code_input_count > 0 else visible_inputs.first
                        await target_input.click()
                        await clear_code_inputs()
                        await target_input.type(str(code), delay=80)
                        last_entered_code = code
                        await asyncio.sleep(2)

                        password_count = await page.locator('input[name="password"]').count()
                        if password_count > 0:
                            code_filled = True
                            log("[步骤4] ✓ 验证码已提交，密码页已出现")
                        else:
                            log("[步骤4] ⚠ 验证码已输入，但密码页未立即出现，继续观察")
                    else:
                        log("[步骤4] ❌ 未获取到验证码")
                        return False

            if code_filled:
                break
            await asyncio.sleep(1)

        check_timeout()
        log("[步骤5] 填写个人信息...")

        try:
            await page.locator('input[name="password"]').wait_for(timeout=15000)
        except Exception:
            log("[步骤5] ❌ 密码页面未出现")
            await page.screenshot(path=os.path.join(DEBUG_DIR, f"no_pw_T{thread_id}.png"))
            await dump_signup_debug("no-password")
            return False

        await page.locator('input[name="givenName"]').fill(fname)
        await asyncio.sleep(0.3)
        await page.locator('input[name="familyName"]').fill(lname)
        await asyncio.sleep(0.3)
        await page.locator('input[name="password"]').fill(grok_password)
        log(f"[步骤5] ✓ {fname} {lname}, 密码长度={len(grok_password)}")

        check_timeout()
        log("[步骤6] 等待 Turnstile 验证...")
        await asyncio.sleep(3)

        async def find_cf_frame():
            for frame in page.frames:
                if "challenges.cloudflare" in frame.url:
                    return frame
            return None

        async def get_turnstile_box():
            cf_frame = await find_cf_frame()
            if not cf_frame:
                log(f"[步骤6] 未找到 CF frame (共 {len(page.frames)} frames)")
                return None
            try:
                frame_el = await cf_frame.frame_element()
                box = await frame_el.bounding_box()
                if box and box["width"] > 0:
                    return box
                log("[步骤6] iframe bounding_box 为空或宽度0")
                return None
            except Exception as e:
                log(f"[步骤6] 获取 iframe box 异常: {e}")
                return None

        async def click_turnstile_mouse():
            try:
                box = await get_turnstile_box()
                if not box:
                    return False

                jitter_x = random.uniform(-2, 4)
                jitter_y = random.uniform(-3, 3)
                click_x = box["x"] + 28 + jitter_x
                click_y = box["y"] + box["height"] / 2 + jitter_y
                log(f"[步骤6] iframe box: ({box['x']:.0f},{box['y']:.0f}) "
                    f"{box['width']:.0f}x{box['height']:.0f}")
                log(f"[步骤6] mouse.click → ({click_x:.1f}, {click_y:.1f})")

                mid_x = click_x + random.uniform(40, 100)
                mid_y = click_y + random.uniform(-40, 40)
                await page.mouse.move(mid_x, mid_y)
                await asyncio.sleep(random.uniform(0.08, 0.2))
                await page.mouse.move(click_x, click_y)
                await asyncio.sleep(random.uniform(0.03, 0.1))
                await page.mouse.click(click_x, click_y)
                log("[步骤6] ✓ mouse.click 已执行")
                return True
            except Exception as e:
                log(f"[步骤6] mouse.click 异常: {e}")
                return False

        ts_final = "unknown"
        click_attempt = 0
        for wait_sec in range(90):
            ts = await page.evaluate('''() => {
                var inp = document.querySelector('input[name="cf-turnstile-response"]');
                if (!inp) return "no_input";
                if (!inp.value) return "empty";
                if (inp.value.length < 10) return "short";
                return "passed:" + inp.value.length;
            }''')
            ts_final = ts
            if ts.startswith("passed"):
                log(f"[步骤6] ✓ Turnstile 已通过! (等了 {wait_sec + 3}s)")
                break

            if ts == "no_input" and wait_sec >= 10:
                log("[步骤6] Turnstile input 不存在 (可能无需验证), 跳过等待")
                ts_final = "skipped"
                break

            if wait_sec in (2, 5, 9, 14, 20, 28, 38, 50, 65, 80):
                log(f"[步骤6] 第{click_attempt+1}次点击尝试 (Turnstile={ts})...")
                await click_turnstile_mouse()
                click_attempt += 1
                await asyncio.sleep(3)
                ts_after = await page.evaluate('''() => {
                    var inp = document.querySelector('input[name="cf-turnstile-response"]');
                    return inp ? (inp.value ? "passed:" + inp.value.length : "empty") : "no_input";
                }''')
                if ts_after.startswith("passed"):
                    log(f"[步骤6] ✓ 点击后 Turnstile 已通过!")
                    ts_final = ts_after
                    break
                continue

            if wait_sec % 10 == 9:
                log(f"[步骤6] {wait_sec+1}s Turnstile: {ts}")
            await asyncio.sleep(1)
        else:
            log(f"[步骤6] ⚠ Turnstile 最终: {ts_final}, 仍尝试提交")

        await page.screenshot(path=os.path.join(DEBUG_DIR, f"step6_T{thread_id}.png"))

        log("[步骤6] 点击 'Complete sign up'...")
        try:
            submit = page.locator("button:has-text('Complete sign up')")
            await submit.click(timeout=5000)
        except Exception:
            log("[步骤6] 按钮点击失败，JS 提交...")
            await page.evaluate('''() => {
                var btns = document.querySelectorAll("button");
                var btn = Array.from(btns).find(
                    b => b.textContent.includes("Complete") || b.type === "submit"
                );
                if (btn) btn.click();
            }''')
        log("[步骤6] ✓ 已提交")

        log("[步骤7] 等待注册结果...")
        await asyncio.sleep(3)

        sso_val = None
        sso_rw_val = ""
        for i in range(30):
            cookies = await context.cookies()
            cdict = {c["name"]: c["value"] for c in cookies}
            if "sso" in cdict:
                sso_val = cdict["sso"]
                sso_rw_val = cdict.get("sso-rw", "")
                break
            if i % 5 == 4:
                log(f"[步骤7] {i+1}s URL: {page.url}")
            await asyncio.sleep(1)

        if not sso_val:
            log("[步骤7] ❌ 未获取 SSO, 收集诊断...")
            await page.screenshot(path=os.path.join(DEBUG_DIR, f"fail_T{thread_id}.png"))
            title = await page.title()
            log(f"[诊断] Title={title}")
            log(f"[诊断] URL={page.url}")
            try:
                body = await page.evaluate(
                    'document.body ? document.body.innerText.substring(0, 500) : "NO_BODY"'
                )
                log(f"[诊断] 页面: {body[:300]}")
            except Exception:
                pass
            cookies = await context.cookies()
            log(f"[诊断] cookies: {[c['name'] for c in cookies]}")
            return False

        log(f"[步骤7] 🎉 注册成功! SSO={sso_val[:30]}...")

        write_success_output(email_address, grok_password, sso_val, sso_rw_val)

        print(f"\n✅ [{task_id}] SSO已保存: {sso_val[:40]}...\n")

        return True

    except TimeoutError:
        log(f"TIMEOUT ({time.time() - job_start:.1f}s)")
        if page:
            try:
                await page.screenshot(
                    path=os.path.join(DEBUG_DIR, f"timeout_T{thread_id}.png")
                )
            except Exception:
                pass
        return False
    except Exception as e:
        log(f"异常: {type(e).__name__}: {e}")
        traceback.print_exc()
        if page:
            try:
                await page.screenshot(
                    path=os.path.join(DEBUG_DIR, f"error_T{thread_id}.png")
                )
            except Exception:
                pass
        return False
    finally:
        if page:
            await safe_await_during_cleanup(page.close(), CLEANUP_TIMEOUT_SEC, "page.close", log)
        if context:
            await safe_await_during_cleanup(context.close(), CLEANUP_TIMEOUT_SEC, "context.close", log)
        if camoufox_obj:
            await safe_await_during_cleanup(
                camoufox_obj.__aexit__(None, None, None),
                CLEANUP_TIMEOUT_SEC,
                "camoufox.__aexit__",
                log,
            )
        gc.collect()


def run_job(thread_id, task_id, timeout_sec=180):
    hard_timeout_sec = timeout_sec + JOB_TIMEOUT_GRACE_SEC
    try:
        return asyncio.run(
            asyncio.wait_for(
                async_run_job(thread_id, task_id, timeout_sec),
                timeout=hard_timeout_sec,
            )
        )
    except asyncio.TimeoutError:
        print(f"[T{thread_id}-#{task_id}] HARD TIMEOUT ({hard_timeout_sec}s)")
        return False


def worker_task(args):
    thread_id, task_id = args

    with stats_lock:
        stats["total"] += 1
        current_total = stats["total"]

    startup_min, startup_max = RUN_STARTUP_DELAY_RANGE
    if startup_max > 0:
        time.sleep(random.uniform(startup_min, startup_max))

    success = False
    for retry in range(RUN_MAX_RETRIES):
        success = run_job(thread_id, task_id, timeout_sec=RUN_TIMEOUT_SEC)
        if success:
            break
        if retry < RUN_MAX_RETRIES - 1:
            wait_min, wait_max = RUN_RETRY_WAIT_RANGE
            wait_time = random.uniform(wait_min, wait_max)
            print(f"[T{thread_id}-#{task_id}] 第{retry+1}次失败，{wait_time:.0f}s后重试...")
            time.sleep(wait_time)

    should_print_stats = False
    with stats_lock:
        if success:
            stats["success"] += 1
        else:
            stats["failed"] += 1
        if current_total % RUN_STATS_EVERY == 0:
            should_print_stats = True

    if should_print_stats:
        print_stats()

    return success


def run_multi_thread(total_count, max_workers=1):
    print(f"[Main] 启动注册，目标: {total_count} 个账号，线程数: {max_workers}")
    print(f"[Main] SSO文件: {SSO_FILE}")
    print("=" * 60)

    with stats_lock:
        stats["total"] = 0
        stats["success"] = 0
        stats["failed"] = 0
        stats["start_time"] = time.time()

    with remote_push_lock:
        pending_remote_tokens.clear()

    tasks = [(i % max_workers, i + 1) for i in range(total_count)]
    task_iter = iter(tasks)
    futures = {}
    completed = 0
    success_count = 0
    interrupted = False
    executor = ThreadPoolExecutor(max_workers=max_workers)

    def submit_next_task():
        try:
            task = next(task_iter)
        except StopIteration:
            return False
        futures[executor.submit(worker_task, task)] = task
        return True

    try:
        for _ in range(min(max_workers, total_count)):
            submit_next_task()

        while futures:
            done, _ = wait(futures, timeout=1, return_when=FIRST_COMPLETED)
            if not done:
                continue

            for future in done:
                task = futures.pop(future)
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                except CancelledError:
                    print(f"[Main] 任务 {task} 已取消")
                except Exception as e:
                    print(f"[错误] 任务 {task} 执行异常: {e}")
                finally:
                    completed += 1
                    if completed % RUN_PROGRESS_EVERY == 0:
                        print(f"[进度] 已完成 {completed}/{total_count} ({completed/total_count*100:.1f}%)")
                    submit_next_task()
    except KeyboardInterrupt:
        interrupted = True
        print("\n[Main] 收到中断信号，停止提交新任务并取消未开始任务...")
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        if not interrupted:
            executor.shutdown(wait=True)
            flush_remote_tokens(force=True)

    return success_count


def main():
    run_conf = configure_runtime()

    print("=" * 60)
    print("     Grok 注册助手 [合并版 - 实时写入/批量推送]")
    print("=" * 60)
    print(f"  邮件系统: DomainMail/DuckMail (来自 config.json)")
    print(f"  SSO 目录: {SSO_DIR}")
    print(f"  输出模式: {OUTPUT_MODE}")
    print(f"  远程推送: 满 {REMOTE_PUSH_BATCH_SIZE} 个追加一次")
    print("=" * 60)

    count = RUN_COUNT
    max_workers = RUN_MAX_WORKERS
    print(f"\n目标: 注册 {count} 个账号")
    print(f"并发: {max_workers} 线程")
    print(f"SSO: {SSO_FILE}")
    print("=" * 60)

    start_time = time.time()
    success_count = run_multi_thread(count, max_workers=max_workers)
    elapsed = time.time() - start_time

    print("=" * 60)
    print("注册任务全部完成!")
    print(f"总计: {count} 个")
    print(f"成功: {success_count} 个")
    print(f"失败: {count - success_count} 个")
    print(f"成功率: {success_count/count*100:.1f}%")
    print(f"耗时: {elapsed/60:.1f} 分钟")
    print(f"SSO: {SSO_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
