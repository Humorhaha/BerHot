"""
utils/user_utils.py — Twitter 用户名解析工具

从 scheduled.py 迁入，与调度逻辑解耦。
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import yaml
from loguru import logger


def _extract_username(url_or_name: str) -> str | None:
    """
    从 Twitter/X URL 提取用户名，或直接返回用户名。

    Examples:
      https://x.com/karpathy         → "karpathy"
      https://x.com/user?lang=ar     → "user"
      @karpathy                      → "karpathy"
      karpathy                       → "karpathy"
    """
    url_or_name = url_or_name.strip()
    if not url_or_name:
        return None
    if url_or_name.startswith(("http://", "https://")):
        parsed = urlparse(url_or_name)
        path = parsed.path.strip("/")
        if "/" in path:
            path = path.split("/")[0]
        username = path.split("?")[0].strip()
        return username or None
    return url_or_name.lstrip("@").strip() or None


def load_usernames_from_yaml(yaml_path: Path) -> list[str]:
    """
    从 users.yaml 加载 Twitter 用户名列表，返回去重后的有序列表。

    支持格式：
      1. 纯 URL 列表（每行一个 https://x.com/username）
      2. YAML list（- user1）
      3. YAML dict（usernames: [user1, user2]）
    """
    if not yaml_path.exists():
        raise FileNotFoundError(f"用户配置文件不存在: {yaml_path}")

    usernames: set[str] = set()
    content = yaml_path.read_text(encoding="utf-8")

    # 尝试 YAML 解析
    try:
        data = yaml.safe_load(content)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    u = _extract_username(item)
                    if u:
                        usernames.add(u.lower())
        elif isinstance(data, dict):
            for key in ("usernames", "users", "sources", "accounts"):
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        if isinstance(item, str):
                            u = _extract_username(item)
                            if u:
                                usernames.add(u.lower())
    except yaml.YAMLError:
        pass

    # YAML 解析无结果时按行解析
    if not usernames:
        for line in content.strip().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                u = _extract_username(line)
                if u:
                    usernames.add(u.lower())

    result = sorted(usernames)
    logger.info(f"loaded {len(result)} usernames from {yaml_path.name}")
    return result
