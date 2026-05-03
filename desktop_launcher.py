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
        _ensure_deps_in_path()

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
        print("[desktop] Server module imported.")

        import uvicorn
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
        svr.run()
    except SystemExit:
        pass
    except BaseException as e:
        server_error[0] = e
        import traceback
        traceback.print_exc()
    finally:
        server_running.clear()
        uvicorn_server[0] = None

def _start_server_thread(port):
    if server_running.is_set():
        print("[desktop] Server already running")
        return
    server_error[0] = None
    t = threading.Thread(target=_server_thread_target, args=(port,), daemon=False)
    t.start()
    server_thread[0] = t

    _mac_notify("金策智算", "正在启动服务…")

    timeout = 90
    start_ts = time.time()
    notified_slow = False
    while True:
        if wait_for_server("127.0.0.1", port, timeout=2):
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
            break

        if server_error[0] is not None:
            break

        if time.time() - start_ts > timeout:
            break

        if not notified_slow and time.time() - start_ts > 15:
            _mac_notify("金策智算", "启动较慢，仍在初始化…")
            notified_slow = True

    if not server_running.is_set():
        print(f"[desktop] Server did not start within {timeout}s.")
        if server_error[0]:
            print(f"[desktop] Error: {server_error[0]}")
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
