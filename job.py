import json
import traceback
from collections import defaultdict
import aiomysql
from quart import request, Blueprint
import requests
# from flask_pydantic import ValidationError
from quart import jsonify
from app.api.component import save_graph_task
from app.controller import get_job_config, set_job_config
from app.core import CTX_USER_ID, AuthControl
from app.model.job import Job
from app.schema import Fail, Success
from app.settings.setting import DAGSTER_GRAPHQL_URL, get_pg_connection
from app.controller.job import JobController
import asyncio
import shutil
from pathlib import Path
from app.api.file import get_input_dir
from app.api.file_utils import safe_dir_name
from app.api.component_utils import FILE_FIELD_NAMES, parse_json_field

job_router = Blueprint('job_router', __name__)


# @job_router.before_request
# async def before_request():
#     await AuthControl.is_authed()
#     user_id = CTX_USER_ID.get()
#     if not user_id:
#         return Fail(message="用户未登录")

def extract_dirs_from_config_schema(config_schema_raw: str, input_dir: Path):
    """
    从 config_schema 中提取真实文件路径，再反推出：
    - config_dir = .../<cn_job_name>/<config_name>
    - job_dir    = .../<cn_job_name>
    """
    if not config_schema_raw:
        return None, None

    try:
        data = json.loads(config_schema_raw)
    except Exception:
        return None, None

    ops = data.get("ops", {}) or {}
    for _, op_body in ops.items():
        cfg = (op_body or {}).get("config", {}) or {}
        for field_name, field_value in cfg.items():
            if field_name not in FILE_FIELD_NAMES or not field_value:
                continue

            try:
                p = Path(str(field_value)).resolve()
                p.relative_to(input_dir)
            except Exception:
                continue

            # 真实结构: input/<job>/<config>/<node>/<file>
            node_dir = p.parent
            config_dir = node_dir.parent
            job_dir = config_dir.parent

            try:
                config_dir.relative_to(input_dir)
                job_dir.relative_to(input_dir)
            except Exception:
                continue

            return job_dir, config_dir

    return None, None


async def safe_remove_tree(path: Path, root: Path):
    path = path.resolve()
    root = root.resolve()
    try:
        path.relative_to(root)
    except Exception:
        raise ValueError(f"非法删除路径：{path}")
    if not path.exists():
        print("目录不存在，跳过删除：", path)
        return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, shutil.rmtree, str(path))
    if path.exists():
        raise RuntimeError(f"目录删除后仍然存在：{path}")
    print("目录删除完成：", path)


@job_router.route('/get_jobs', methods=['POST'])
async def get_jobs():
    # 从请求的 JSON 数据中获取变量
    data = await request.json
    repository_location_name = data.get("repositoryLocationName")
    repository_name = data.get("repositoryName")
    # 检查必要的参数是否存在
    if not repository_location_name or not repository_name:
        return jsonify({"error": "Missing repositoryLocationName or repositoryName"}), 400
    # GraphQL 查询
    query = """
    query JobsQuery(
      $repositoryLocationName: String!
      $repositoryName: String!
    ) {
      repositoryOrError(
        repositorySelector: {
          repositoryLocationName: $repositoryLocationName
          repositoryName: $repositoryName
        }
      ) {
        ... on Repository {
          jobs {
            name
          }
        }
      }
    }
    """
    # 动态构建变量
    variables = {
        "repositoryLocationName": repository_location_name,
        "repositoryName": repository_name
    }
    # 发送 GraphQL 请求到 Dagster 服务
    response = requests.post(DAGSTER_GRAPHQL_URL, json={'query': query, 'variables': variables})
    # 检查响应是否成功
    if response.status_code == 200:
        data = response.json()
        return jsonify(data), 200  # 返回 JSON 响应
    else:
        return jsonify({"error": "Failed to fetch jobs from Dagster"}), 500


@job_router.route("/job/graph", methods=["POST"])
async def get_graph_by_cn_name():
    """根据中文任务名和配置名获取 graph_config 和 job_name"""
    conn = None
    try:
        data = await request.get_json() or {}
        cn_job_name = (data.get("cn_job_name") or "").strip()
        config_name = (data.get("config_name") or "").strip()
    except Exception:
        return Fail(code=400, message="请求体格式错误，必须为JSON")

    if not cn_job_name:
        return Fail(code=401, message="缺少参数 cn_job_name")
    if not config_name:
        return Fail(code=402, message="缺少参数 config_name")

    conn = await get_pg_connection()
    if conn is None:
        return Fail(code=500, message="数据库连接失败")

    try:
        result = await conn.fetchrow(
            """
            SELECT
                c.graph_by_config,
                c.job_name
            FROM lowcode.config_for_job c
            JOIN lowcode.frontend_job_graph f
              ON c.job_name = f.job_name
            WHERE f.cn_job_name = $1
              AND c.config_name = $2
            LIMIT 1
            """,
            cn_job_name,
            config_name,
        )

        if not result:
            return Fail(
                code=404,
                message=f"未找到 cn_job_name={cn_job_name}、config_name={config_name} 的记录",
            )

        graph_by_config_raw = result["graph_by_config"]
        job_name = result["job_name"]

        if not graph_by_config_raw or not job_name:
            return Fail(code=422, message="图配置或 job_name 字段为空")

        graph_config = parse_json_field(graph_by_config_raw, default={})
        if not graph_config:
            return Fail(code=405, message="graph_config 字段解析失败")

        return Success(
            data={
                "job_name": job_name,
                "graph_config": graph_config,
                "config_name": config_name,
            }
        )

    except Exception as e:
        return Fail(code=501, message=f"查询失败: {str(e)}")

    finally:
        if conn:
            await conn.close()


@job_router.route("/job/list", methods=["GET"])
async def list_jobs():
    """查询全部任务流（frontend_job_graph 表）并附带 config_schema 信息"""
    conn = None
    try:
        conn = await get_pg_connection()
        if conn is None:
            return Fail(code=500, message="数据库连接失败")

        job_rows = await conn.fetch(
            """
            SELECT cn_job_name, job_name, graph_config, created_at
            FROM lowcode.frontend_job_graph
            ORDER BY created_at DESC
            """
        )

        if not job_rows:
            return Success(data={"list": []})

        job_names = [row["job_name"] for row in job_rows if row["job_name"]]

        config_rows = await conn.fetch(
            """
            SELECT job_name, config_name, config_schema
            FROM lowcode.config_for_job
            WHERE job_name = ANY($1::text[])
            ORDER BY id DESC
            """,
            job_names,
        )

        config_map = defaultdict(list)
        for config_row in config_rows:
            config_name = config_row["config_name"] or ""
            raw_schema = config_row["config_schema"]
            parsed_schema = parse_json_field(raw_schema, default={})

            config_map[config_row["job_name"]].append(
                {
                    "config_name": config_name,
                    "config_schema": parsed_schema,
                }
            )

        result = []
        for row in job_rows:
            cn_job_name = row["cn_job_name"] or ""
            job_name = row["job_name"] or ""
            graph_config_raw = row["graph_config"]
            created_at = row["created_at"]

            created_at_str = (
                created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else ""
            )
            graph_config = parse_json_field(graph_config_raw, default={})

            result.append(
                {
                    "cn_job_name": cn_job_name,
                    "job_name": job_name,
                    "created_at": created_at_str,
                    "graph_config": graph_config,
                    "config_schemas": config_map.get(job_name, []),
                }
            )

        return Success(data={"list": result})

    except Exception as e:
        return Fail(code=500, message=f"任务流查询失败：{str(e)}")

    finally:
        if conn:
            await conn.close()


@job_router.route("/job/update", methods=["POST"])
async def update_job():
    """根据旧任务名更新 frontend_job_graph 表中的 cn_job_name"""
    conn = None
    try:
        data = await request.get_json() or {}
        old_cn_job_name = (data.get("old_cn_job_name") or "").strip()
        new_cn_job_name = (data.get("new_cn_job_name") or "").strip()
    except Exception:
        return Fail(code=400, message="请求体格式错误，必须为JSON")

    if not old_cn_job_name or not new_cn_job_name:
        return Fail(code=400, message="缺少参数 old_cn_job_name 或 new_cn_job_name")

    conn = await get_pg_connection()
    if conn is None:
        return Fail(code=500, message="数据库连接失败")

    try:
        async with conn.transaction():
            result = await conn.fetchrow(
                """
                SELECT id
                FROM lowcode.frontend_job_graph
                WHERE cn_job_name = $1
                LIMIT 1
                """,
                old_cn_job_name,
            )

            if not result:
                return Fail(code=404, message=f"未找到任务名为 {old_cn_job_name} 的记录")

            record_id = result["id"]

            conflict = await conn.fetchrow(
                """
                SELECT id
                FROM lowcode.frontend_job_graph
                WHERE cn_job_name = $1
                  AND id != $2
                LIMIT 1
                """,
                new_cn_job_name,
                record_id,
            )

            if conflict:
                return Fail(code=409, message=f"新任务名 {new_cn_job_name} 已存在，请更换")

            await conn.execute(
                """
                UPDATE lowcode.frontend_job_graph
                SET cn_job_name = $1
                WHERE id = $2
                """,
                new_cn_job_name,
                record_id,
            )

        return Success(message=f"任务名称已成功更新为 {new_cn_job_name}")

    except Exception as e:
        return Fail(code=500, message=f"任务名称更新失败：{str(e)}")

    finally:
        if conn:
            await conn.close()


@job_router.route("/job/delete", methods=["DELETE"])
async def delete_job():
    conn = None
    try:
        data = await request.get_json() or {}
        config_name = (data.get("config_name") or "").strip()
    except Exception:
        return Fail(code=400, message="请求体格式错误，必须为JSON")

    if not config_name:
        return Fail(code=401, message="缺少任务配置名 config_name")

    conn = await get_pg_connection()
    if conn is None:
        return Fail(code=500, message="数据库连接失败")

    APP_DIR = Path(__file__).resolve().parents[1]
    input_dir = (APP_DIR / "data" / "input").resolve()

    job_name = None
    job_dir = None
    config_dir = None
    delete_whole_job_dir = False

    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT job_name, graph_by_config, config_schema
                FROM lowcode.config_for_job
                WHERE config_name = $1
                LIMIT 1
                """,
                config_name,
            )

            if not row:
                return Fail(code=404, message=f"未找到 config_name={config_name} 的记录")

            job_name = row["job_name"]
            graph_by_config_raw = row["graph_by_config"] or "{}"
            config_schema_raw = row["config_schema"] or "{}"

            # 1. 优先从 config_schema 中真实文件路径反推目录
            job_dir, config_dir = extract_dirs_from_config_schema(
                config_schema_raw,
                input_dir,
            )

            # 2. 如果当前配置没有文件字段，再退回名字拼目录
            if job_dir is None or config_dir is None:
                graph_by_config = parse_json_field(graph_by_config_raw, default={})
                cn_job_name = (
                    graph_by_config.get("cn_job_name")
                    or graph_by_config.get("cnJobName")
                    or ""
                ).strip()

                if not cn_job_name and "_配置" in config_name:
                    cn_job_name = config_name.rsplit("_配置", 1)[0].strip()

                if not cn_job_name:
                    return Fail(code=500, message="无法确定任务目录：缺少 cn_job_name")

                job_dir = (input_dir / safe_dir_name(cn_job_name)).resolve()
                config_dir = (job_dir / safe_dir_name(config_name)).resolve()

            # 3. 防路径穿越
            try:
                job_dir.relative_to(input_dir)
                config_dir.relative_to(input_dir)
            except Exception:
                return Fail(code=403, message="非法目录路径，拒绝删除")

            # 4. 判断该 job 还有多少配置
            cnt_row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS cnt
                FROM lowcode.config_for_job
                WHERE job_name = $1
                """,
                job_name,
            )
            ref_cnt = int(cnt_row["cnt"] or 0)

            # 5. 先删数据库
            if ref_cnt >= 2:
                await conn.execute(
                    """
                    DELETE FROM lowcode.config_for_job
                    WHERE config_name = $1
                    """,
                    config_name,
                )
            else:
                await conn.execute(
                    """
                    DELETE FROM lowcode.config_for_job
                    WHERE job_name = $1
                    """,
                    job_name,
                )
                await conn.execute(
                    """
                    DELETE FROM lowcode.job
                    WHERE job_name = $1
                    """,
                    job_name,
                )
                await conn.execute(
                    """
                    DELETE FROM lowcode.frontend_job_graph
                    WHERE job_name = $1
                    """,
                    job_name,
                )
                delete_whole_job_dir = True

    except Exception as e:
        return Fail(code=500, message=f"任务删除失败：{str(e)}")

    finally:
        if conn:
            await conn.close()

    # 6. 数据库提交成功后，再删文件目录
    cleanup_errors = []
    target_dir = job_dir if delete_whole_job_dir else config_dir

    try:
        await safe_remove_tree(target_dir, input_dir)

        if (not delete_whole_job_dir) and job_dir.exists() and job_dir.is_dir():
            try:
                next(job_dir.iterdir())
            except StopIteration:
                await asyncio.to_thread(job_dir.rmdir)
                print("empty job_dir removed =", job_dir)

    except Exception as e:
        cleanup_errors.append(str(e))
        print("cleanup error =", repr(e))

    if cleanup_errors:
        if delete_whole_job_dir:
            return Success(
                message=(
                    f"数据库删除成功：job_name={job_name} 及其相关数据已全部删除；"
                    f"但文件目录删除失败：{'；'.join(cleanup_errors)}"
                )
            )

        return Success(
            message=(
                f"数据库删除成功：已移除 config_name={config_name}；"
                f"但文件目录删除失败：{'；'.join(cleanup_errors)}"
            )
        )

    if delete_whole_job_dir:
        return Success(
            message=f"删除成功：job_name={job_name} 及其数据库记录、任务目录已全部删除"
        )

    return Success(
        message=f"删除成功：已移除 config_name={config_name}，并删除对应配置目录"
    )
