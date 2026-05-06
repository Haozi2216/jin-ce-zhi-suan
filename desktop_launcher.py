#!/usr/bin/env python
"""桌面端启动器：启动 FastAPI 服务 + 系统托盘菜单。

打包后作为桌面程序的入口，双击 exe / app 即可运行。
"""
import os
import sys
import time
import socket
import webbrowser
import json
import threading
import asyncio
import signal
import traceback
import subprocess
import datetime
import atexit

# ---------------------------------------------------------------------------
# macOS 原生 AppKit 阻塞（无第三方依赖）
# ---------------------------------------------------------------------------
def _mac_app_run():
    """在 macOS 上启动 NSApplication 主循环，阻塞直到 quit。"""
    try:
        while True:
            time.sleep(1)
    except Exception:
        while True:
            time.sleep(1)

# ---------------------------------------------------------------------------
# 路径工具
# ---------------------------------------------------------------------------
def _bundle_path(relative):
    """返回打包资源目录下的文件路径（只读）。
    Windows: _MEIPASS/...
    macOS:   AppName.app/Contents/Resources/...
    """
    if getattr(sys, "_MEIPASS", None):
        return os.path.join(sys._MEIPASS, relative)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative)

def _app_bundle_root():
    """返回 .app 包的根目录（可写），仅 macOS 有意义。"""
    if sys.platform == "darwin":
        exe_dir = os.path.dirname(os.path.abspath(sys.executable))
        contents_dir = os.path.dirname(exe_dir)
        bundle_root = os.path.dirname(contents_dir)
        return bundle_root
    return os.path.dirname(os.path.abspath(sys.executable))

def _default_app_data_dir():
    """返回桌面端默认数据目录（可写）。"""
    if sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/jin-ce-zhi-suan")
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

def _ensure_desktop_env_defaults():
    """在 GUI 启动时补齐关键环境变量，保证后续路径与日志可用。"""
    if sys.platform == "darwin":
        os.environ.setdefault("PROJECT_ROOT", _app_bundle_root())
    os.environ.setdefault("DESKTOP_CONFIG_DIR", _default_app_data_dir())

class _TeeTextIO:
    """将写入同时转发到多个文本流，用于把 stdout/stderr 同时写到日志文件。"""
    def __init__(self, *streams):
        self._streams = [s for s in streams if s is not None]

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
            except Exception:
                pass
        for s in self._streams:
            try:
                if hasattr(s, "flush"):
                    s.flush()
            except Exception:
                pass

    def isatty(self):
        for s in self._streams:
            try:
                if hasattr(s, "isatty") and s.isatty():
                    return True
            except Exception:
                continue
        return False

    def fileno(self):
        for s in self._streams:
            try:
                if hasattr(s, "fileno"):
                    return s.fileno()
            except Exception:
                continue
        raise OSError("No fileno available")

    @property
    def encoding(self):
        for s in self._streams:
            try:
                enc = getattr(s, "encoding", None)
                if enc:
                    return enc
            except Exception:
                continue
        return "utf-8"

    @property
    def errors(self):
        for s in self._streams:
            try:
                err = getattr(s, "errors", None)
                if err:
                    return err
            except Exception:
                continue
        return "replace"

    def __getattr__(self, name):
        for s in self._streams:
            try:
                return getattr(s, name)
            except Exception:
                continue
        raise AttributeError(name)

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass

_desktop_log_file = [None]
_desktop_log_path = [None]
_desktop_lock_path = [None]
_desktop_last_url_path = [None]

def _init_desktop_logging():
    """初始化桌面端落盘日志，解决 Finder 双击看不到 stdout/stderr 的问题。"""
    try:
        log_root = os.path.join(os.environ.get("DESKTOP_CONFIG_DIR", _default_app_data_dir()), "logs")
        os.makedirs(log_root, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_path = os.path.join(log_root, f"desktop-{ts}.log")
        f = open(log_path, "a", encoding="utf-8", errors="replace", buffering=1)
        _desktop_log_file[0] = f
        _desktop_log_path[0] = log_path

        stdout = getattr(sys, "stdout", None)
        stderr = getattr(sys, "stderr", None)
        sys.stdout = _TeeTextIO(stdout, f)
        sys.stderr = _TeeTextIO(stderr, f)

        def _close():
            try:
                f.flush()
                f.close()
            except Exception:
                pass

        atexit.register(_close)
        print(f"[desktop] Log file: {log_path}")
    except Exception:
        pass

def _is_pid_alive(pid):
    """判断 PID 是否存活（跨平台尽量兼容）。"""
    try:
        pid = int(pid)
    except Exception:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    except Exception:
        return False

def _acquire_single_instance_lock():
    """保证桌面端单实例运行，避免重复双击导致多进程反复通知/抢端口。"""
    try:
        root = os.environ.get("DESKTOP_CONFIG_DIR", _default_app_data_dir())
        os.makedirs(root, exist_ok=True)
        lock_path = os.path.join(root, "desktop.lock")
        last_url_path = os.path.join(root, "desktop_last_url.txt")
        _desktop_lock_path[0] = lock_path
        _desktop_last_url_path[0] = last_url_path

        if os.path.exists(lock_path):
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    old_pid = (f.read() or "").strip()
            except Exception:
                old_pid = ""

            if old_pid and _is_pid_alive(old_pid):
                url = ""
                try:
                    if os.path.exists(last_url_path):
                        with open(last_url_path, "r", encoding="utf-8") as f:
                            url = (f.read() or "").strip()
                except Exception:
                    url = ""
                if not url:
                    url = "http://127.0.0.1:8000"
                print(f"[desktop] Another instance is running (pid={old_pid}). Opening: {url}")
                if sys.platform == "darwin" and getattr(sys, "frozen", False):
                    choice = _mac_control_dialog(url, _desktop_log_path[0])
                    if choice == "退出服务":
                        try:
                            os.kill(int(old_pid), signal.SIGTERM)
                        except Exception:
                            pass
                        raise SystemExit(0)
                    if choice == "打开看板":
                        _open_url(url)
                    raise SystemExit(0)
                else:
                    _mac_notify("金策智算", "程序已在运行，正在打开页面…")
                    _open_url(url)
                    raise SystemExit(0)

        with open(lock_path, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))

        def _cleanup_lock():
            try:
                if _desktop_lock_path[0] and os.path.exists(_desktop_lock_path[0]):
                    with open(_desktop_lock_path[0], "r", encoding="utf-8") as f:
                        pid_in_file = (f.read() or "").strip()
                    if pid_in_file == str(os.getpid()):
                        os.remove(_desktop_lock_path[0])
            except Exception:
                pass

        atexit.register(_cleanup_lock)
    except SystemExit:
        raise
    except Exception:
        pass

def _mac_notify(title, message):
    """在 macOS 上发送通知，用于 GUI 无窗口时给用户可见反馈。"""
    if sys.platform != "darwin":
        return
    try:
        safe_title = (title or "").replace('"', '\\"')
        safe_msg = (message or "").replace('"', '\\"')
        subprocess.run(
            [
                "osascript",
                "-e",
                f'display notification "{safe_msg}" with title "{safe_title}"',
            ],
            timeout=5,
        )
    except Exception:
        pass

def _open_url(url):
    """更可靠地打开 URL：macOS 优先用 open，其它平台 fallback 到 webbrowser。"""
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", url], timeout=5)
            return True
    except Exception:
        pass
    try:
        return bool(webbrowser.open(url))
    except Exception:
        return False

def _mac_control_dialog(url, log_path=None):
    # 控制对话框：在没有托盘/窗口的场景下提供“打开看板/退出服务”入口
    if sys.platform != "darwin":
        return ""
    try:
        safe_url = (url or "").replace('"', '\\"')
        safe_log = (log_path or "").replace('"', '\\"')
        msg = f"服务已启动：{safe_url}"
        if safe_log:
            msg += f"\\n\\n日志：{safe_log}"
        script = [
            f'set btn to button returned of (display dialog "{msg}" with title "金策智算" buttons {{"打开看板","退出服务","继续后台"}} default button "打开看板")',
            "return btn",
        ]
        proc = subprocess.run(["osascript", "-e", script[0], "-e", script[1]], capture_output=True, text=True, timeout=60)
        return (proc.stdout or "").strip()
    except Exception:
        return ""

# ---------------------------------------------------------------------------
# 端口工具
# ---------------------------------------------------------------------------
def read_config_port():
    cfg_path = _bundle_path("config.json")
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        p = cfg.get("system", {}).get("server_port", 8000)
        return int(p)
    except Exception:
        user_dir = os.environ.get("DESKTOP_CONFIG_DIR", "")
        if user_dir:
            user_cfg = os.path.join(user_dir, "config.json")
            try:
                with open(user_cfg, "r", encoding="utf-8") as f:
                    return json.load(f).get("system", {}).get("server_port", 8000)
            except Exception:
                pass
        return 8000

def _safe_int(value, default_value):
    """安全转换整数；非法值时返回默认值。"""
    try:
        return int(value)
    except Exception:
        return int(default_value)

def read_desktop_startup_timeout():
    """读取桌面端启动超时时间（秒）。

    优先级（高 -> 低）：
    1) 环境变量 JZ_DESKTOP_STARTUP_TIMEOUT
    2) 用户目录 config.json: desktop.startup_timeout_seconds
    3) 打包内置 config.json: desktop.startup_timeout_seconds
    4) 默认值 180
    """
    # 默认值适当放宽，避免弱机器/首启时误判为失败。
    default_timeout = 180

    # 环境变量可用于运维快速覆盖，不需要改配置文件。
    env_value = str(os.environ.get("JZ_DESKTOP_STARTUP_TIMEOUT", "") or "").strip()
    if env_value:
        return max(30, _safe_int(env_value, default_timeout))

    # 用户配置优先，符合桌面端部署可定制预期。
    user_dir = os.environ.get("DESKTOP_CONFIG_DIR", "")
    if user_dir:
        user_cfg = os.path.join(user_dir, "config.json")
        try:
            with open(user_cfg, "r", encoding="utf-8") as f:
                payload = json.load(f)
            v = payload.get("desktop", {}).get("startup_timeout_seconds", default_timeout)
            return max(30, _safe_int(v, default_timeout))
        except Exception:
            pass

    # 回退读取打包内置配置，保证无用户配置时也有可控默认行为。
    bundle_cfg = _bundle_path("config.json")
    try:
        with open(bundle_cfg, "r", encoding="utf-8") as f:
            payload = json.load(f)
        v = payload.get("desktop", {}).get("startup_timeout_seconds", default_timeout)
        return max(30, _safe_int(v, default_timeout))
    except Exception:
        return default_timeout

def find_free_port(start_port=8000):
    port = start_port
    while port < 65535:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.3)
                s.connect(("127.0.0.1", port))
                port += 1
        except Exception:
            return port
    return port

def wait_for_server(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect((host, port))
                return True
        except Exception:
            time.sleep(0.3)
    return False

# ---------------------------------------------------------------------------
# FastAPI 服务
# ---------------------------------------------------------------------------
def _ensure_deps_in_path():
    """将第三方包路径加入 sys.path（仅打包模式需要）。"""
    if not getattr(sys, "frozen", False):
        return
    meipass = getattr(sys, "_MEIPASS", "") or ""
    python_dir = sys.prefix
    candidates = []
    for base in [meipass, python_dir]:
        if not base:
            continue
        sp = os.path.join(base, "Lib", "site-packages")
        if os.path.isdir(sp):
            candidates.append(sp)
        ver = "python%d.%d" % (sys.version_info.major, sys.version_info.minor)
        for lib_name in ("lib", "lib64"):
            for py_path in (ver, "python" + ver.replace(".", "")):
                sp = os.path.join(base, lib_name, py_path, "site-packages")
                if os.path.isdir(sp):
                    candidates.append(sp)
    for sp in candidates:
        if sp not in sys.path:
            sys.path.insert(0, sp)
    if meipass and meipass not in sys.path:
        sys.path.insert(0, meipass)

def _install_windows_socketpair_patch():
    """在 Windows 冻结模式下完全绕过 socketpair 自唤醒机制。

    背景：
    - ProactorEventLoop 创建时调用 socket.socketpair() 构建 self-pipe。
    - 实测对方的 Win11 机器上，TCP socketpair（bind+listen+accept）超时，
      UDP socketpair（互相 connect+send/recv）也超时。
    - 说明该机器上进程内 socket 间通信被安全软件拦截。
    - 但简单的 bind/listen 单端口操作（如 server 端口）不受影响。

    方案：
    - 替换 socket.socketpair 为返回"假 socket"对象。
    - 假对象的 fileno() 返回 -1，使 signal.set_wakeup_fd(-1) 成功（移除唤醒 fd）。
    - send()/close()/setblocking() 均为空操作。
    - 自唤醒在 Windows ProactorEventLoop 中本质是非必需的，
      因为信号机制在 Windows 上基本无效。
    - 通过环境变量 JZ_PATCH_SOCKETPAIR 控制开关（默认开启）。
    """
    if sys.platform != "win32" or (not getattr(sys, "frozen", False)):
        return
    enabled = str(os.environ.get("JZ_PATCH_SOCKETPAIR", "1") or "").strip()
    if enabled != "1":
        print("[desktop] Skip socketpair patch (JZ_PATCH_SOCKETPAIR!=1)")
        return

    try:
        import socket as _socket
        import asyncio.proactor_events

        if getattr(_socket, "_jz_socketpair_patched", False):
            return

        class _JzFakeSocket:
            """假 socket 对象，替代 socketpair 产物。
            fileno() 返回 -1 使 signal.set_wakeup_fd 静默，
            send/close/setblocking 均为空操作。"""
            def __init__(self):
                self._closed = False

            def fileno(self):
                return -1

            def setblocking(self, flag):
                pass

            def send(self, data):
                return len(data) if data else 0

            def close(self):
                self._closed = True

            def __repr__(self):
                return f"<_JzFakeSocket closed={self._closed}>"

        def _jz_socketpair(family=_socket.AF_INET, type=_socket.SOCK_STREAM, proto=0):
            return _JzFakeSocket(), _JzFakeSocket()

        _socket.socketpair = _jz_socketpair
        _socket._jz_socketpair_patched = True
        print("[desktop] Patched socket.socketpair with dummy (no TCP/UDP needed)")

        # 额外补丁：直接替换 asyncio.proactor_events 模块里的 socket 引用
        try:
            _proactor = asyncio.proactor_events
            _proactor_socket = getattr(_proactor, "socket", None) or _socket
            if not getattr(_proactor_socket, "_jz_socketpair_patched", False):
                _proactor_socket._jz_socketpair_patched = True
                _proactor_socket.socketpair = _jz_socketpair
                if hasattr(_proactor_socket, "_fallback_socketpair"):
                    _proactor_socket._fallback_socketpair = _jz_socketpair
        except Exception as proactor_err:
            print(f"[desktop] Failed to patch asyncio.proactor_events socket: {proactor_err}")
    except Exception as patch_err:
        print(f"[desktop] Failed to install socketpair patch: {patch_err}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# 系统托盘图标
# ---------------------------------------------------------------------------
def _create_tray_icon(port):
    """创建系统托盘图标，返回 Icon 实例。"""
    try:
        from pystray import Icon, MenuItem, Menu
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        print("[desktop] pystray/PIL not available, falling back to native loop")
        traceback.print_exc()
        return None

    # 创建托盘图标图像（简单彩色方块）
    def _make_icon():
        # 优先使用项目的 logo.png 作为菜单栏图标（打包后也会随 datas 一起进入资源目录）
        logo_path = _bundle_path("logo.png")
        try:
            if os.path.exists(logo_path):
                src = Image.open(logo_path).convert("RGBA")
                canvas = Image.new("RGBA", (64, 64), (0, 0, 0, 0))

                max_size = 56
                scale = min(max_size / max(1, src.width), max_size / max(1, src.height))
                nw = max(1, int(src.width * scale))
                nh = max(1, int(src.height * scale))
                src = src.resize((nw, nh), Image.LANCZOS)
                x = (64 - nw) // 2
                y = (64 - nh) // 2
                canvas.paste(src, (x, y), src)
                return canvas
        except Exception:
            # 兜底：logo 读取失败时回退到生成图标，确保托盘可用
            traceback.print_exc()

        img = Image.new("RGB", (64, 64), "#2563EB")
        draw = ImageDraw.Draw(img)
        try:
            if sys.platform == "darwin":
                font = None
                for fp in [
                    "/System/Library/Fonts/Helvetica.ttc",
                    "/System/Library/Fonts/SFNSDisplay.ttf",
                ]:
                    if os.path.exists(fp):
                        font = ImageFont.truetype(fp, 36)
                        break
                if font is None:
                    font = ImageFont.load_default()
            else:
                font = ImageFont.truetype(
                    "/System/Library/Fonts/Helvetica.ttc", 36
                )
        except (OSError, IOError):
            font = ImageFont.load_default()
        draw.text((16, 10), "J", fill="white", font=font)
        return img

    icon_image = _make_icon()

    def _menu_title(item=None):
        status = "运行中" if server_running.is_set() else "已停止"
        return f"金策智算  [{status}]"

    def _open_dashboard(tray_icon, item):
        url = f"http://127.0.0.1:{port}"
        webbrowser.open(url)

    def _restart_server(tray_icon, item):
        _stop_server_thread()
        time.sleep(0.5)
        _start_server_thread(port)

    def _stop_server(tray_icon, item):
        _stop_server_thread()

    def _show_about(tray_icon, item):
        url = f"http://127.0.0.1:{port}"
        print(f"\n[desktop] 服务地址: {url}")

    def _quit(tray_icon, item):
        # 退出即停止服务（推荐）
        _stop_server_and_wait(timeout=3.0)
        tray_icon.stop()

    try:
        menu = Menu(
            MenuItem(_menu_title, lambda tray_icon, item: None, enabled=False),
            Menu.SEPARATOR,
            MenuItem("打开看板", _open_dashboard),
            MenuItem("重启服务", _restart_server),
            MenuItem("停止服务", _stop_server),
            Menu.SEPARATOR,
            MenuItem("关于", _show_about),
            MenuItem("退出", _quit),
        )
        return Icon("金策智算", icon_image, "金策智算", menu)
    except Exception:
        print("[desktop] Failed to create tray icon, falling back to native loop")
        traceback.print_exc()
        return None

# ---------------------------------------------------------------------------
# 服务生命周期
# ---------------------------------------------------------------------------
server_thread = [None]
server_running = threading.Event()
server_error = [None]
uvicorn_server = [None]  # 保存 uvicorn.Server 实例引用
server_module_ref = [None]  # 保存导入后的 server 模块引用，用于读取启动阶段快照

def _dump_all_threads_stack(reason):
    """将当前进程所有线程调用栈写入日志，用于定位启动卡死点。"""
    try:
        print(f"[desktop] ===== THREAD DUMP BEGIN: {reason} =====")
        frames = sys._current_frames()  # noqa: SLF001 - 诊断用途，读取所有线程栈
        thread_map = {t.ident: t for t in threading.enumerate()}
        for ident, frame in frames.items():
            t = thread_map.get(ident)
            t_name = t.name if t is not None else "unknown"
            t_alive = t.is_alive() if t is not None else False
            print(f"[desktop] --- thread ident={ident} name={t_name} alive={t_alive} ---")
            try:
                stack_lines = traceback.format_stack(frame)
                for ln in stack_lines:
                    line = str(ln or "").rstrip("\n")
                    if line:
                        print(f"[desktop] {line}")
            except Exception as stack_err:
                print(f"[desktop] format_stack failed: {stack_err}")
        print(f"[desktop] ===== THREAD DUMP END: {reason} =====")
    except Exception as dump_err:
        print(f"[desktop] Thread dump failed: {dump_err}")

def _stop_server_and_wait(timeout=5.0):
    # 停止 uvicorn 并等待线程退出；用于“退出应用即停止服务”
    _stop_server_thread()
    t = server_thread[0]
    if t is None:
        return
    try:
        t.join(timeout=timeout)
    except Exception:
        pass

def _server_thread_target(port):
    """启动服务器线程。通过捕获 uvicorn.Server 实例实现优雅停止。"""
    try:
        # 记录服务线程 ID，供 watchdog 定位当前执行栈。
        server_tid = threading.get_ident()
        _ensure_deps_in_path()
        _install_windows_socketpair_patch()

        server_dir = _bundle_path("")
        if server_dir not in sys.path:
            sys.path.insert(0, server_dir)

        os.environ["SERVER_PORT"] = str(port)
        os.environ.setdefault("SERVER_HOST", "127.0.0.1")
        # 桌面端默认避免在启动阶段进行外部网络拉取，防止无网络/被墙环境导致长时间卡住
        os.environ.setdefault("JZ_DISABLE_AKSHARE_STOCK_LIST", "1")
        # 桌面端启动时优先保证服务可启动，跳过 matplotlib 字体扫描（必要时可手动清除该环境变量恢复）
        os.environ.setdefault("JZ_SKIP_MPL_FONT_CONFIG", "1")
        # Finder 双击启动时，matplotlib 字体缓存可能卡住；将 MPLCONFIGDIR 指向可写目录，避免权限/锁文件问题
        mpl_dir = os.path.join(os.environ.get("DESKTOP_CONFIG_DIR", _default_app_data_dir()), "matplotlib")
        try:
            os.makedirs(mpl_dir, exist_ok=True)
            os.environ.setdefault("MPLCONFIGDIR", mpl_dir)
        except Exception:
            pass

        import importlib.util
        print("[desktop] Importing server module...")
        server_py = _bundle_path("server.py")
        spec = importlib.util.spec_from_file_location("server_module", server_py)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot load {server_py}")
        server_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(server_mod)
        # 保存模块引用，供主线程读取 get_startup_trace_snapshot() 诊断启动卡点。
        server_module_ref[0] = server_mod
        print("[desktop] Server module imported.")

        import uvicorn
        # Windows 冻结环境事件循环策略控制：
        # - 该客户机日志已定位到 SelectorEventLoop 在 socket._fallback_socketpair() 卡住；
        # - 因此默认采用 Proactor，避免 selector 自唤醒管道构建卡死。
        # 可通过环境变量 JZ_EVENT_LOOP_POLICY 覆盖：
        #   proactor / selector / auto
        if sys.platform == "win32" and getattr(sys, "frozen", False):
            loop_policy = str(os.environ.get("JZ_EVENT_LOOP_POLICY", "proactor") or "").strip().lower()
            if loop_policy == "selector":
                try:
                    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
                    print("[desktop] Applied WindowsSelectorEventLoopPolicy")
                except Exception as policy_err:
                    print(f"[desktop] Failed to apply selector event loop policy: {policy_err}")
            elif loop_policy == "proactor":
                try:
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                    print("[desktop] Applied WindowsProactorEventLoopPolicy")
                except Exception as policy_err:
                    print(f"[desktop] Failed to apply proactor event loop policy: {policy_err}")
            else:
                print(f"[desktop] Keep default event loop policy (JZ_EVENT_LOOP_POLICY={loop_policy})")
        print("[desktop] Starting uvicorn...")
        cfg = uvicorn.Config(
            server_mod.app,
            host="127.0.0.1",
            port=port,
            ws_ping_interval=20.0,
            ws_ping_timeout=180.0,
            ws_max_queue=1024,
            log_level="warning",
        )
        svr = uvicorn.Server(cfg)
        uvicorn_server[0] = svr
        # watchdog：周期打印 uvicorn 关键状态，判断是否卡在 run/serve 早期阶段。
        watchdog_stop = threading.Event()
        def _uvicorn_watchdog():
            while not watchdog_stop.wait(5.0):
                try:
                    # 周期采样服务线程栈顶，便于识别卡点（无需等待超时后的全量 thread dump）。
                    stack_tip = "unavailable"
                    try:
                        frames = sys._current_frames()  # noqa: SLF001 - 仅诊断使用
                        frame = frames.get(server_tid)
                        if frame is not None:
                            tips = []
                            cur = frame
                            depth = 0
                            while cur is not None and depth < 6:
                                code = cur.f_code
                                tips.append(f"{os.path.basename(code.co_filename)}:{cur.f_lineno}:{code.co_name}")
                                cur = cur.f_back
                                depth += 1
                            if tips:
                                stack_tip = " <- ".join(tips)
                    except Exception as stack_tip_err:
                        stack_tip = f"stack_sample_failed:{stack_tip_err}"
                    print(
                        "[desktop] uvicorn watchdog: started={} should_exit={} force_exit={} stack={}".format(
                            getattr(svr, "started", None),
                            getattr(svr, "should_exit", None),
                            getattr(svr, "force_exit", None),
                            stack_tip,
                        )
                    )
                except Exception as wd_err:
                    print(f"[desktop] uvicorn watchdog error: {wd_err}")
        wd_thread = threading.Thread(target=_uvicorn_watchdog, name="uvicorn-watchdog", daemon=True)
        wd_thread.start()
        print(f"[desktop] Entering uvicorn.run thread={threading.current_thread().name}")
        svr.run()
        watchdog_stop.set()
        print("[desktop] uvicorn.run returned")
    except SystemExit:
        pass
    except BaseException as e:
        server_error[0] = e
        import traceback
        traceback.print_exc()
        _dump_all_threads_stack(f"server_thread_exception:{type(e).__name__}")
    finally:
        server_running.clear()
        uvicorn_server[0] = None
        server_module_ref[0] = None

def _start_server_thread(port):
    if server_running.is_set():
        print("[desktop] Server already running")
        return
    server_error[0] = None
    t = threading.Thread(target=_server_thread_target, args=(port,), daemon=False)
    t.start()
    server_thread[0] = t

    _mac_notify("金策智算", "正在启动服务…")

    timeout = read_desktop_startup_timeout()
    start_ts = time.time()
    notified_slow = False
    last_progress_log_ts = 0.0

    def _read_server_startup_trace():
        """读取 server.py 启动阶段快照；失败时返回空字典。"""
        mod = server_module_ref[0]
        if mod is None:
            return {}
        getter = getattr(mod, "get_startup_trace_snapshot", None)
        if getter is None:
            return {}
        try:
            snap = getter()
            return snap if isinstance(snap, dict) else {}
        except Exception:
            return {}

    def _finalize_server_ready():
        """服务就绪后的收口动作：写回 URL、通知并尝试打开浏览器。"""
        server_running.set()
        url = f"http://127.0.0.1:{port}"
        try:
            if _desktop_last_url_path[0]:
                with open(_desktop_last_url_path[0], "w", encoding="utf-8") as f:
                    f.write(url)
        except Exception:
            pass
        print(f"[desktop] Server ready: {url}")
        _mac_notify("金策智算", "启动成功，正在打开浏览器…")
        if not _open_url(url):
            print("[desktop] Failed to open browser automatically.")
            if sys.platform == "darwin" and getattr(sys, "frozen", False):
                _show_crash_dialog(f"服务已启动，但无法自动打开浏览器。\n请手动访问：{url}\n日志：{_desktop_log_path[0] or '未知'}")

    def _wait_server_ready_in_background():
        """超时后继续后台等待，避免把“慢启动”误报成“启动失败”。

        这里不设置总超时：线程为 daemon，不阻塞退出；只要服务最终起来就自动拉起浏览器。
        """
        while server_error[0] is None:
            if wait_for_server("127.0.0.1", port, timeout=2):
                if not server_running.is_set():
                    print("[desktop] Server became ready after initial timeout.")
                    _finalize_server_ready()
                return
            time.sleep(1.0)

    while True:
        if wait_for_server("127.0.0.1", port, timeout=2):
            _finalize_server_ready()
            break

        if server_error[0] is not None:
            break

        if time.time() - start_ts > timeout:
            break

        # 每 5 秒打印一次启动进度（含 server.py 阶段快照），便于用户日志快速定位卡点。
        now_ts = time.time()
        if now_ts - last_progress_log_ts >= 5.0:
            elapsed = now_ts - start_ts
            stage_info = _read_server_startup_trace()
            if stage_info:
                stage = str(stage_info.get("stage", "") or "unknown")
                status = str(stage_info.get("status", "") or "unknown")
                detail = str(stage_info.get("detail", "") or "")
                print(f"[desktop] Waiting server... elapsed={elapsed:.1f}s stage={stage} status={status} detail={detail}")
            else:
                print(f"[desktop] Waiting server... elapsed={elapsed:.1f}s stage=unavailable")
            last_progress_log_ts = now_ts

        if not notified_slow and time.time() - start_ts > 15:
            _mac_notify("金策智算", "启动较慢，仍在初始化…")
            notified_slow = True

    if not server_running.is_set():
        print(f"[desktop] Server did not start within {timeout}s.")
        t_alive = bool(server_thread[0] and server_thread[0].is_alive())
        print(f"[desktop] Server thread alive at timeout: {t_alive}")
        # 超时时额外打印一次阶段快照，帮助快速判定卡在哪个阶段。
        timeout_stage_info = _read_server_startup_trace()
        if timeout_stage_info:
            print(f"[desktop] Startup trace at timeout: {timeout_stage_info}")
        _dump_all_threads_stack("startup_timeout")
        if server_error[0]:
            print(f"[desktop] Error: {server_error[0]}")
        else:
            # 非异常但超时，判定为“慢启动”并继续后台等待。
            print("[desktop] Startup is taking longer than expected; continue waiting in background.")
            _mac_notify("金策智算", "启动较慢，正在后台继续初始化…")
            threading.Thread(target=_wait_server_ready_in_background, daemon=True).start()
        if sys.platform == "darwin" and getattr(sys, "frozen", False):
            msg = "服务启动失败或超时。\n\n"
            if server_error[0]:
                msg += f"错误: {type(server_error[0]).__name__}: {server_error[0]}\n\n"
            if _desktop_log_path[0]:
                msg += f"日志: {_desktop_log_path[0]}\n"
            msg += "你也可以从终端运行 app 内可执行文件以查看输出。"
            _show_crash_dialog(msg[:900])

def _stop_server_thread():
    svr = uvicorn_server[0]
    if svr is not None:
        print("[desktop] Stopping server...")
        svr.should_exit = True

# ---------------------------------------------------------------------------
# 首次运行初始化（仅打包模式）
# ---------------------------------------------------------------------------
def _init_config_on_first_run():
    if not getattr(sys, "frozen", False):
        return

    import shutil
    exe_dir = os.path.dirname(os.path.abspath(sys.executable))

    if sys.platform == "win32":
        os.environ["DESKTOP_CONFIG_DIR"] = exe_dir
        user_cfg = os.path.join(exe_dir, "config.json")
        if not os.path.exists(user_cfg):
            bundle_cfg = _bundle_path("config.json")
            if os.path.exists(bundle_cfg):
                shutil.copy2(bundle_cfg, user_cfg)
                print(f"[desktop] Copied config.json to {user_cfg}")

    elif sys.platform == "darwin":
        os.environ["PROJECT_ROOT"] = _app_bundle_root()
        app_data = os.path.expanduser("~/Library/Application Support/jin-ce-zhi-suan")
        os.makedirs(app_data, exist_ok=True)
        os.environ["DESKTOP_CONFIG_DIR"] = app_data
        user_cfg = os.path.join(app_data, "config.json")
        if not os.path.exists(user_cfg):
            bundle_cfg = _bundle_path("config.json")
            if os.path.exists(bundle_cfg):
                shutil.copy2(bundle_cfg, user_cfg)
                print(f"[desktop] Copied config.json to {user_cfg}")
        user_data = os.path.join(app_data, "data")
        if not os.path.isdir(user_data):
            bundle_data = os.path.join(_bundle_path(""), "data")
            if os.path.isdir(bundle_data):
                shutil.copytree(bundle_data, user_data, dirs_exist_ok=True)

    else:
        os.environ["DESKTOP_CONFIG_DIR"] = exe_dir
        user_cfg = os.path.join(exe_dir, "config.json")
        if not os.path.exists(user_cfg):
            bundle_cfg = _bundle_path("config.json")
            if os.path.exists(bundle_cfg):
                shutil.copy2(bundle_cfg, user_cfg)
                print(f"[desktop] Copied config.json to {user_cfg}")

# ---------------------------------------------------------------------------
# 主进程
# ---------------------------------------------------------------------------
def _show_crash_dialog(message):
    """在 macOS GUI 模式下弹出错误对话框。"""
    if sys.platform != "darwin":
        return
    try:
        safe_msg = (message or "").replace('"', '\\"')
        cmd = [
            "osascript",
            "-e",
            f'display dialog "{safe_msg}" with title "金策智算 - 启动失败" buttons {{"确定"}} with icon stop',
        ]
        subprocess.run(cmd, timeout=8)
    except Exception:
        pass

def _blocking_main_loop():
    """阻塞主线程，保持进程存活。"""
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

def main():
    try:
        _run_main()
    except SystemExit:
        raise
    except BaseException as e:
        error_msg = "错误类型: {}\n错误信息: {}\n\nPython: {}\n平台: {}\n\n{}".format(
            type(e).__name__,
            str(e),
            sys.version.split()[0],
            sys.platform,
            traceback.format_exc(),
        )
        print("[desktop] CRASH: " + error_msg)
        if sys.platform == "darwin" and getattr(sys, "frozen", False):
            _show_crash_dialog(error_msg[:500])
        # 即使出错也保持进程不退出，让用户能看到错误
        time.sleep(5)
        sys.exit(1)

def _run_main():
    """主逻辑入口（被 main() 的 try/except 包裹）。"""
    _ensure_desktop_env_defaults()
    _init_desktop_logging()
    _acquire_single_instance_lock()
    # macOS Finder 双击启动时默认 CWD 可能是 "/"，会导致 server.py 内的相对路径落到 "/data" 并触发只读文件系统错误；
    # 统一将工作目录切换到用户可写目录，确保 data/ 等相对路径正常工作。
    if getattr(sys, "frozen", False):
        try:
            os.chdir(os.environ.get("DESKTOP_CONFIG_DIR", _default_app_data_dir()))
        except Exception:
            pass
    print(f"[desktop] JinCeZhiSuan Desktop")
    print(f"[desktop] CWD: {os.getcwd()}")
    print(f"[desktop] Mode: {'frozen' if getattr(sys, 'frozen', False) else 'dev'}")

    config_port = read_config_port()
    actual_port = find_free_port(config_port)
    if actual_port != config_port:
        print(f"[desktop] Port {config_port} in use, switching to {actual_port}")
    print(f"[desktop] Port: {actual_port}")
    os.environ["SERVER_PORT"] = str(actual_port)
    os.environ.setdefault("SERVER_HOST", "127.0.0.1")

    _init_config_on_first_run()

    # 启动服务（启动成功后会自动打开浏览器）
    _start_server_thread(actual_port)

    url = f"http://127.0.0.1:{actual_port}"
    tray = _create_tray_icon(actual_port)
    if tray is None:
        if sys.platform == "darwin" and getattr(sys, "frozen", False):
            _show_crash_dialog("无法创建菜单栏托盘图标。\n请确认打包环境已安装 pystray / Pillow / pyobjc，并重新打包。")
        _stop_server_and_wait(timeout=3.0)
        raise SystemExit(1)

    print("[desktop] System tray icon ready")
    tray.run()
    _stop_server_and_wait(timeout=3.0)
    raise SystemExit(0)

if __name__ == "__main__":
    main()
