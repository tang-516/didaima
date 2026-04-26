import datetime
from urllib.parse import unquote
import re
from pathlib import Path, PurePosixPath
import aiomysql
import unicodedata
import os
from quart import Blueprint, jsonify
from werkzeug.utils import secure_filename
from urllib.parse import quote
import traceback
from app.api.file_utils import safe_filename_keep_unicode, safe_dir_name
from app.schema import Fail, Success
from app.settings import get_pg_connection
import json
from quart import request, send_file

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# print(BASE_DIR)
UPLOAD_DIR = os.path.join(BASE_DIR, 'app', 'data', 'input')

file_router = Blueprint('file_router', __name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = (APP_DIR / "data" / "input").resolve()


def get_input_dir() -> Path:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    return INPUT_DIR


@file_router.route("/file/input/<path:filename>", methods=["GET"])
async def handle_file_input_get(filename: str):
    input_dir = get_input_dir()
    input_dir.mkdir(parents=True, exist_ok=True)
    rel = str(filename).strip().replace("\\", "/").lstrip("/")
    p = PurePosixPath(rel)
    # 禁止 .. / 空段
    if not rel or any(part in ("..", "") for part in p.parts):
        return Fail(code=403, message="非法文件路径"), 403
    file_path = (input_dir / Path(*p.parts)).resolve()
    print("GET /file/input hit:", filename)
    print("input_dir =", input_dir)
    print("file_path  =", file_path)
    # 防穿越：必须仍在 input_dir 下
    try:
        file_path.relative_to(input_dir)
    except Exception:
        return Fail(code=403, message="非法文件路径"), 403
    if not file_path.exists() or not file_path.is_file():
        return Fail(code=404, message=f"文件不存在：{file_path}"), 404
    return await send_file(str(file_path), as_attachment=False)


@file_router.route("/file/preview", methods=["POST"])
async def handle_file_preview():
    try:
        data = await request.json
    except Exception:
        return Fail(code=400, message="请求体格式错误，必须为JSON"), 400
    data = data or {}
    cn_job_name = (data.get("cn_job_name") or data.get("cnJobName") or "").strip()
    config_name = (data.get("config_name") or data.get("configName") or "").strip()
    node_id = (data.get("node_id") or data.get("nodeId") or "").strip()
    if not cn_job_name or not config_name or not node_id:
        return Fail(code=401, message="缺少参数：cn_job_name / config_name / node_id"), 401
    # 目录安全化 + node_id 归一化
    job_dir = safe_dir_name(cn_job_name)
    cfg_dir = safe_dir_name(config_name)
    node_dir = safe_dir_name(node_id.replace("node_", "").strip())
    input_dir = get_input_dir()
    input_dir.mkdir(parents=True, exist_ok=True)
    target_dir = (input_dir / job_dir / cfg_dir / node_dir).resolve()

    # 防穿越：必须仍在 input_dir 下
    try:
        target_dir.relative_to(input_dir)
    except Exception:
        return Fail(code=403, message="非法目录路径"), 403

    if not target_dir.exists() or not target_dir.is_dir():
        return Fail(code=404, message=f"节点目录不存在：{target_dir}"), 404
    # 列出目录下文件（排除子目录）
    files = [p for p in target_dir.iterdir() if p.is_file()]
    if not files:
        return Fail(code=404, message="该节点目录下没有可预览文件"), 404
    # 默认选择：最新修改的文件（更符合“刚上传就预览”）
    files_sorted = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    chosen = files_sorted[0]
    # 生成相对路径 URL
    rel = chosen.relative_to(input_dir).as_posix()
    rel_url = quote(rel, safe="/")
    file_url = request.host_url.rstrip("/") + "/file/input/" + rel_url
    # 同时返回文件列表（有多个时前端可展示选择）
    file_items = []
    for p in files_sorted:
        relp = p.relative_to(input_dir).as_posix()
        file_items.append({
            "name": p.name,
            "relative_path": relp,
            "url": request.host_url.rstrip("/") + "/file/input/" + quote(relp, safe="/")
        })
    return Success(
        data={
            "url": file_url, "name": chosen.name, "relative_path": rel, "files": file_items
        }
    ), 200


@file_router.route("/file/upload", methods=["POST"])
async def upload_file():
    try:
        files = await request.files
        form = await request.form
        file = files.get("file")
        if not file:
            return Fail(code=400, message="请求中没有名为 file 的上传字段"), 400
        file_data = file.read()
        if not file_data:
            return Fail(code=400, message="请求中没有文件内容"), 400

        # 必传参数：config_name + node_id（cn_job_name 建议也传，用于最外层目录）
        cn_job_name = (form.get("cn_job_name") or form.get("cnJobName") or
                       request.headers.get("X-CN-Job-Name") or "").strip()
        config_name = (form.get("config_name") or form.get("configName") or
                       request.headers.get("X-Config-Name") or "").strip()
        node_id = (form.get("node_id") or form.get("nodeId") or request.headers.get("X-Node-Id") or "").strip()
        if not cn_job_name:
            return Fail(code=400, message="缺少参数 cn_job_name"), 400
        if not config_name:
            return Fail(code=400, message="缺少参数 config_name"), 400
        if not node_id:
            return Fail(code=400, message="缺少参数 node_id"), 400

        # 文件名：优先 header X-Filename（兼容中文/编码），否则用上传对象自带 filename
        raw_filename = (request.headers.get("X-Filename", "") or "").strip()
        filename = unquote(raw_filename) if raw_filename else (getattr(file, "filename", "") or "")
        filename = (filename or "").strip()
        if not filename:
            return Fail(code=400, message="缺少文件名：请提供 Header X-Filename 或上传文件自带 filename"), 400
        content_type = request.headers.get("Content-Type", "") or ""
    except Exception as e:
        return Fail(code=400, message=f"请求格式错误，无法读取文件: {str(e)}"), 400

    # 清洗文件名（保留中文）
    filename = safe_filename_keep_unicode(filename)
    # 没扩展名则按 content-type 补
    if not os.path.splitext(filename)[1]:
        if "csv" in content_type.lower():
            filename += ".csv"
        else:
            filename += ".bin"

    # 目录安全化
    job_dir = safe_dir_name(cn_job_name)
    cfg_dir = safe_dir_name(config_name)
    node_dir = safe_dir_name(node_id.replace("node_", "").strip())
    input_dir: Path = get_input_dir()
    input_dir.mkdir(parents=True, exist_ok=True)

    # 最终路径：input/<cn_job_name>/<config_name>/<node_id>/<filename>
    save_dir = (input_dir / job_dir / cfg_dir / node_dir).resolve()
    # 防穿越：save_dir 必须在 input_dir 下
    try:
        save_dir.relative_to(input_dir)
    except Exception:
        return Fail(code=403, message="非法保存路径"), 403
    os.makedirs(str(save_dir), exist_ok=True)
    save_path = (save_dir / filename).resolve()
    try:
        save_path.relative_to(input_dir)
    except Exception:
        return Fail(code=403, message="非法保存路径"), 403
    try:
        with open(str(save_path), "wb") as f:
            f.write(file_data)
    except Exception as e:
        return Fail(code=500, message=f"保存文件失败：{str(e)}"), 500
    rel_path = save_path.relative_to(input_dir).as_posix()
    return Success(
        message="上传成功",
        data={
            "filename": filename,
            "cn_job_name": cn_job_name,
            "config_name": config_name,
            "node_id": node_id,
            "relative_path": rel_path,
        }
    ), 200


@file_router.route("/file/download", methods=["POST"])
async def handle_file_return():
    """根据传递的 configName 查询 PostgreSQL，读取 config_schema 中 data_save 的路径并返回文件"""
    conn = None
    try:
        # 1) 参数
        data = await request.get_json() or {}
        config_name = (data.get("configName") or data.get("config_name") or "").strip()
        if not config_name:
            return Fail(code=400, message="缺少 configName 参数"), 400
        # 2) DB
        conn = await get_pg_connection()
        if conn is None:
            return Fail(code=500, message="数据库连接失败"), 500
        row = await conn.fetchrow(
            """
            SELECT config_schema
            FROM lowcode.config_for_job
            WHERE config_name = $1
            LIMIT 1
            """,
            config_name,
        )
        if not row or row["config_schema"] is None:
            return Fail(code=404, message="未找到对应的配置"), 404
        # 3) 解析 config_schema
        try:
            raw_config_schema = row["config_schema"]
            if isinstance(raw_config_schema, dict):
                run_config_data = raw_config_schema
            elif isinstance(raw_config_schema, str):
                run_config_data = json.loads(raw_config_schema)
            else:
                return Fail(code=500, message="config_schema 字段类型不支持"), 500
        except Exception as e:
            return Fail(code=500, message=f"config_schema 不是合法 JSON：{str(e)}"), 500

        ops = run_config_data.get("ops", {}) or {}
        checked_paths = []

        # 4) 遍历 ops，找包含 data_save 的 op
        for op_name, op_body in ops.items():
            if "data_save" not in str(op_name or ""):
                continue

            cfg = (op_body or {}).get("config", {}) or {}
            saving_path = str(cfg.get("saving_path") or "").strip()
            saving_file_name = str(cfg.get("saving_file_name") or "").strip()

            if not (saving_path and saving_file_name):
                continue

            # 安全化文件名，防止路径穿越
            saving_file_name = os.path.basename(saving_file_name)

            # 防止响应头注入
            safe_disp_name = (
                saving_file_name
                .replace("\r", "")
                .replace("\n", "")
                .replace('"', "_")
            )

            # 拼接并规范化路径
            full_file_path = os.path.normpath(
                os.path.join(saving_path, saving_file_name)
            )
            checked_paths.append(full_file_path)

            if os.path.exists(full_file_path) and os.path.isfile(full_file_path):
                resp = await send_file(full_file_path, as_attachment=True)

                # Content-Disposition 兼容中文文件名
                ext = os.path.splitext(safe_disp_name)[1]
                ascii_fallback = "download" + ext
                utf8_name = quote(safe_disp_name, safe="")
                resp.headers["Content-Disposition"] = (
                    f'attachment; filename="{ascii_fallback}"; '
                    f"filename*=UTF-8''{utf8_name}"
                )
                return resp
        # 没找到文件
        return Fail(
            code=404,
            message=f"未找到对应文件。已检查路径: {checked_paths or '无'}"
        ), 404

    except Exception as e:
        traceback.print_exc()
        return Fail(code=500, message=f"文件处理失败：{str(e)}"), 500

    finally:
        if conn:
            await conn.close()