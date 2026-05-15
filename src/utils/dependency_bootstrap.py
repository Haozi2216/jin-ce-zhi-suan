"""项目启动依赖自检与自动安装工具。"""

from __future__ import annotations

import importlib.metadata
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List

# 使用环境变量避免同一进程重复执行依赖检查。
_BOOTSTRAP_DONE_ENV = "JZ_DEPENDENCY_BOOTSTRAP_DONE"
# 提供手动关闭开关，便于离线排障或定制启动流程。
_BOOTSTRAP_ENABLED_ENV = "JZ_AUTO_INSTALL_DEPS"
# 简单提取 requirements 包名，兼容常见版本约束写法。
_REQUIREMENT_NAME_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.\-]+)")


def _normalize_distribution_name(name: str) -> str:
    """统一包名格式，减少大小写和分隔符差异带来的误判。"""
    return str(name or "").strip().lower().replace("_", "-")


def _project_root_from_current_file() -> Path:
    """基于当前文件位置推导项目根目录。"""
    return Path(__file__).resolve().parents[2]


def _read_requirement_names(requirements_path: Path) -> List[str]:
    """从 requirements.txt 中提取包名列表。"""
    requirement_names: List[str] = []
    seen_names = set()

    for raw_line in requirements_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        # 跳过空行、注释和 include/editable 等扩展语法。
        if not line or line.startswith("#") or line.startswith("-"):
            continue

        # 去掉环境标记，例如 "package; python_version >= '3.9'"。
        line = line.split(";", 1)[0].strip()
        if not line:
            continue

        match = _REQUIREMENT_NAME_PATTERN.match(line)
        if not match:
            continue

        normalized_name = _normalize_distribution_name(match.group(1))
        if normalized_name and normalized_name not in seen_names:
            seen_names.add(normalized_name)
            requirement_names.append(normalized_name)

    return requirement_names


def _find_missing_requirements(requirement_names: List[str]) -> List[str]:
    """找出当前解释器环境中缺失的依赖包。"""
    missing_names: List[str] = []

    for requirement_name in requirement_names:
        try:
            importlib.metadata.distribution(requirement_name)
        except importlib.metadata.PackageNotFoundError:
            missing_names.append(requirement_name)

    return missing_names


def _ensure_pip_available(python_executable: str, project_root: Path) -> None:
    """确保当前解释器具备 pip，缺失时尝试自动补齐。"""
    pip_check = subprocess.run(
        [python_executable, "-m", "pip", "--version"],
        cwd=str(project_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if pip_check.returncode == 0:
        return

    print("[bootstrap] pip 不可用，正在尝试通过 ensurepip 自动安装...")
    ensurepip_result = subprocess.run(
        [python_executable, "-m", "ensurepip", "--upgrade"],
        cwd=str(project_root),
        check=False,
    )
    if ensurepip_result.returncode != 0:
        raise RuntimeError("当前 Python 环境缺少 pip，且 ensurepip 自动修复失败。")


def ensure_project_dependencies(project_root: str | Path | None = None) -> None:
    """在项目启动前强制检查依赖，缺失时自动安装。"""
    if os.environ.get(_BOOTSTRAP_DONE_ENV) == "1":
        return

    # 冻结打包环境的依赖通常已随程序分发，不在运行时执行 pip。
    if getattr(sys, "frozen", False):
        os.environ[_BOOTSTRAP_DONE_ENV] = "1"
        return

    if str(os.environ.get(_BOOTSTRAP_ENABLED_ENV, "1")).strip().lower() in {"0", "false", "off"}:
        os.environ[_BOOTSTRAP_DONE_ENV] = "1"
        return

    resolved_project_root = Path(project_root) if project_root else _project_root_from_current_file()
    requirements_path = resolved_project_root / "requirements.txt"
    if not requirements_path.exists():
        os.environ[_BOOTSTRAP_DONE_ENV] = "1"
        return

    requirement_names = _read_requirement_names(requirements_path)
    if not requirement_names:
        os.environ[_BOOTSTRAP_DONE_ENV] = "1"
        return

    missing_names = _find_missing_requirements(requirement_names)
    if not missing_names:
        os.environ[_BOOTSTRAP_DONE_ENV] = "1"
        return

    python_executable = sys.executable or "python"
    print(f"[bootstrap] 检测到缺失依赖: {', '.join(missing_names)}")
    print(f"[bootstrap] 正在使用 {python_executable} 自动安装 requirements.txt ...")

    _ensure_pip_available(python_executable, resolved_project_root)

    install_result = subprocess.run(
        [python_executable, "-m", "pip", "install", "-r", str(requirements_path)],
        cwd=str(resolved_project_root),
        check=False,
    )
    if install_result.returncode != 0:
        raise RuntimeError(
            "依赖自动安装失败，请手动执行 `python -m pip install -r requirements.txt` 后重试。"
        )

    remaining_missing_names = _find_missing_requirements(requirement_names)
    if remaining_missing_names:
        raise RuntimeError(
            "依赖安装后仍存在缺失包: {}".format(", ".join(remaining_missing_names))
        )

    print("[bootstrap] 依赖检查完成，所有 requirements 依赖均已就绪。")
    os.environ[_BOOTSTRAP_DONE_ENV] = "1"
