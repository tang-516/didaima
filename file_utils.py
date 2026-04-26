import os
import re
import unicodedata
from pathlib import Path
from urllib.parse import unquote


def safe_dir_name(name: str) -> str:
    """保留中文，但去掉路径穿越/非法字符，适合作为目录名"""
    name = (name or "").strip()
    name = name.replace("..", "_")
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name).strip(". ")
    return name or "unnamed"


def safe_filename_keep_unicode(name: str, max_len: int = 180) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFKC", name)
    name = os.path.basename(name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.strip(" .")
    if not name:
        name = "file"
    root, ext = os.path.splitext(name)
    if len(name) > max_len:
        root = root[: max_len - len(ext)]
        name = root + ext
    return name
