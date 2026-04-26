import importlib
import pkgutil
import inspect
import traceback
from pathlib import Path
from dagster import OpDefinition
import os
import ops
import uuid
from collections import defaultdict
from quart import Blueprint, request, jsonify
import re
import json
from app.api.component_utils import (
    trigger_dagster_reload,
    extract_base_label,
    convert_value,
    simplify_dag_for_comparison,
    simplify_ops_for_comparison,
    monitor_and_recover_repository,
    FILE_FIELD_NAMES,
    safe_dir_name,
    is_file_reader_node,
    copy_into_node_dir,
    validate_config_name_strict,
    generate_next_config_name,
    ensure_config_name_available,
    clone_graph_payload_for_new_task,
    build_cloned_ops_and_copy_plan_from_current_page,
    build_dag_from_nodes,
    load_name_mapping,
    load_component_outputs,
    remove_dir_if_exists,
    remove_parent_if_empty,
    safe_target_cfg_dir,
    normalize_source_file_path, parse_json_field,
)
from app.api.file import get_input_dir
from app.schema import Success, Fail
from app.settings import get_pg_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
component_router = Blueprint("component_router", __name__)


@component_router.route("/component/graph_exist", methods=["POST"])
async def is_exist():
    conn = None
    try:
        try:
            data = await request.get_json()
        except Exception:
            return Fail(code=400, message="请求体必须为JSON"), 400

        data = data or {}
        cn_job_name = (data.get("cn_job_name") or "").strip()
        if not cn_job_name:
            return Fail(code=400, message="缺少参数：中文任务名（cn_job_name）"), 400

        conn = await get_pg_connection()
        if not conn:
            return Fail(code=500, message="数据库连接失败"), 500

        row = await conn.fetchrow(
            """
            SELECT 1
            FROM lowcode.frontend_job_graph
            WHERE cn_job_name = $1
            LIMIT 1
            """,
            cn_job_name,
        )

        if row:
            return Success(
                data=False,
                message="该任务名已存在，请勿输入相同的任务名！"
            ), 200

        return Success(
            data=True,
            message="任务名可用"
        ), 200

    except Exception as e:
        return Fail(code=500, message=f"查询失败：{str(e)}"), 500

    finally:
        if conn:
            await conn.close()


@component_router.route("/component/graph_json", methods=["POST"])
async def save_graph_task():
    conn = None
    try:
        # ==================== 1. 读取并校验请求 ====================
        data = await request.get_json() or {}

        cn_job_name = (data.get("cn_job_name") or "").strip()
        if not cn_job_name:
            return jsonify({"error": "缺少参数：中文任务名（cn_job_name）"}), 400

        source_config_name = (data.get("source_config_name") or data.get("sourceConfigName") or "").strip()
        config_name = (data.get("config_name") or data.get("configName") or "").strip()

        nodes = data.get("nodes")
        if not isinstance(nodes, list) or not nodes:
            return jsonify({"error": "缺少参数：nodes，且必须为非空数组"}), 400

        dag = []

        conn = await get_pg_connection()
        if conn is None:
            return jsonify({"error": "无法连接数据库"}), 500

        # ==================== 2. 加载组件输出映射 ====================
        try:
            COMPONENT_OUTPUTS = await load_component_outputs(conn)
        except Exception as e:
            return jsonify({"error": f"加载 component_info 失败: {type(e).__name__}: {e}"}), 500

        # ==================== 3. 加载中文组件名映射 ====================
        try:
            name_mapping = await load_name_mapping(conn)
        except Exception as e:
            return jsonify({"error": f"加载 ops_mapping 失败: {type(e).__name__}: {e}"}), 500

        # ==================== 4. 判断任务是否已存在 ====================
        exists = await conn.fetchrow(
            """
            SELECT 1
            FROM lowcode.frontend_job_graph
            WHERE cn_job_name = $1
            LIMIT 1
            """,
            cn_job_name,
        )

        # ==================== 5. 构建 DAG（新建/编辑公用） ====================
        id_to_node = {node["id"]: node for node in nodes}

        for node in nodes:
            label_cn = extract_base_label(node["label"])
            component_type = name_mapping.get(label_cn)
            if not component_type:
                continue

            node_id = node["id"].replace("node_", "")
            entry = {"name": f"{component_type}_id_{node_id}"}
            inputs = {}

            for field in node.get("formSchema", {}).get("fields", []):
                if field.get("type") != "cascader":
                    continue

                target_node_id = str(field.get("value", "")).replace("node_", "")
                if not target_node_id:
                    continue

                target_node = id_to_node.get(f"node_{target_node_id}")
                if not target_node:
                    continue

                target_label_cn = extract_base_label(target_node["label"])
                target_type = name_mapping.get(target_label_cn)
                if not target_type:
                    continue

                output_val = field.get("output", "")
                if not output_val:
                    if "options" in field:
                        for option in field["options"]:
                            if option.get("value") == f"node_{target_node_id}" and option.get("children"):
                                output_val = option["children"][0].get("value", "")
                                break

                    if not output_val:
                        target_outputs = COMPONENT_OUTPUTS.get(target_type, [])
                        if isinstance(target_outputs, list):
                            output_val = target_outputs[0] if target_outputs else ""
                        else:
                            output_val = target_outputs

                if not output_val:
                    continue

                input_name = field.get("name")
                if not input_name:
                    continue

                inputs[input_name] = {
                    "task": f"{target_type}_id_{target_node_id}",
                    "output": output_val
                }

            if inputs:
                entry["inputs"] = inputs

            dag.append(entry)

        INPUT_ROOT = Path(BASE_DIR) / "app" / "data" / "input"
        input_dir = get_input_dir().resolve()

        # ============================================================
        # ==================== 6. 新建任务分支 ========================
        # ============================================================
        if not exists:
            if source_config_name:
                return jsonify({
                    "error": "新建任务时不应传原配置名（source_config_name）；source_config_name 仅用于编辑已有任务时复用旧配置"
                }), 400

            if not config_name:
                config_name = f"{cn_job_name}_配置1"
            else:
                try:
                    config_name = validate_config_name_strict(cn_job_name, config_name)
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400

            try:
                await ensure_config_name_available(conn, config_name)
            except ValueError as e:
                return jsonify({"error": str(e)}), 409

            data["config_name"] = config_name
            data["configName"] = config_name

            job_dir_name = safe_dir_name(cn_job_name)
            cfg_dir_name = safe_dir_name(config_name)
            cfg_input_dir = (INPUT_ROOT / job_dir_name / cfg_dir_name).resolve()

            ops_config = {}
            copy_plan = []

            for n in nodes:
                label = extract_base_label(n["label"])
                component_type = name_mapping.get(label)
                if not component_type:
                    continue

                node_id_raw = n["id"].replace("node_", "")
                node_id = safe_dir_name(node_id_raw)
                full_node_name = f"{component_type}_id_{node_id_raw}"
                file_reader = is_file_reader_node(n)
                node_input_dir = (cfg_input_dir / node_id).resolve()

                if file_reader:
                    node_input_dir.mkdir(parents=True, exist_ok=True)

                config = {}
                for f in n.get("formSchema", {}).get("fields", []):
                    if f.get("type") == "cascader" or f.get("value") is None:
                        continue

                    name = f.get("name")
                    value = f.get("value")
                    if not name:
                        continue

                    if name in FILE_FIELD_NAMES and value:
                        filename_only = os.path.basename(str(value)).replace("\\", "/").split("/")[-1]
                        dst_path = (node_input_dir / filename_only).resolve()

                        if cfg_input_dir not in dst_path.parents:
                            raise ValueError(f"非法目标路径: {dst_path}")

                        # 情况1：上传流程已将文件放到最终目录
                        if dst_path.exists() and dst_path.is_file():
                            config[name] = dst_path.as_posix()
                            continue

                        # 情况2：兼容旧流程，从 input 根目录复制到最终目录
                        src_path = (INPUT_ROOT / filename_only).resolve()
                        if INPUT_ROOT not in src_path.parents and src_path != INPUT_ROOT:
                            raise ValueError(f"非法文件名: {value}")

                        if src_path.exists() and src_path.is_file():
                            copy_plan.append((src_path, dst_path))
                            config[name] = dst_path.as_posix()
                            continue

                        raise FileNotFoundError(
                            f"未找到上传文件：期望 {dst_path}，也未在根目录找到 {src_path}"
                        )

                    config[name] = convert_value(value)

                ops_config[full_node_name] = {"config": config}

            for src_path, dst_path in copy_plan:
                await copy_into_node_dir(src_path, dst_path)

            job_name = f"job_{uuid.uuid4().hex[:8]}"

            try:
                async with conn.transaction():
                    await conn.execute(
                        """
                        INSERT INTO lowcode.job (job_name, job_graph, repo_id)
                        VALUES ($1, $2, $3)
                        """,
                        job_name,
                        json.dumps({"dag": dag}, ensure_ascii=False),
                        1,
                    )

                    await conn.execute(
                        """
                        INSERT INTO lowcode.config_for_job (job_name, config_schema, config_name, graph_by_config)
                        VALUES ($1, $2, $3, $4)
                        """,
                        job_name,
                        json.dumps({"ops": ops_config}, ensure_ascii=False),
                        config_name,
                        json.dumps(data, ensure_ascii=False),
                    )

                    await conn.execute(
                        """
                        INSERT INTO lowcode.frontend_job_graph (graph_config, job_name, cn_job_name)
                        VALUES ($1, $2, $3)
                        """,
                        json.dumps(data, ensure_ascii=False),
                        job_name,
                        cn_job_name,
                    )
            except Exception as e:
                return jsonify({"error": f"保存新任务失败: {str(e)}"}), 500

            trigger_dagster_reload()
            result = await monitor_and_recover_repository(job_name=job_name, config_name=config_name)
            if result and result.get("code") == 500:
                return jsonify(result.get("message")), 500

            return Success(
                message="成功保存当前任务",
                data={"job_name": job_name, "config_name": config_name}
            )

        # ============================================================
        # ==================== 7. 编辑已有任务分支 ====================
        # ============================================================
        result = await conn.fetchrow(
            """
            SELECT job_name
            FROM lowcode.frontend_job_graph
            WHERE cn_job_name = $1
            LIMIT 1
            """,
            cn_job_name,
        )

        if not result:
            return jsonify({"error": f"未找到任务名“{cn_job_name}”对应的后端 job_name"}), 404

        job_name = result["job_name"]

        if not source_config_name:
            return jsonify({"error": "编辑已有任务时必须传 source_config_name"}), 400

        if not config_name or config_name == source_config_name:
            config_name = await generate_next_config_name(conn, job_name, cn_job_name)
        else:
            try:
                config_name = validate_config_name_strict(cn_job_name, config_name)
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

        try:
            await ensure_config_name_available(conn, config_name)
        except ValueError as e:
            return jsonify({"error": str(e)}), 409

        data["config_name"] = config_name
        data["configName"] = config_name
        data["source_config_name"] = source_config_name
        data["sourceConfigName"] = source_config_name

        graph_result = await conn.fetchrow(
            """
            SELECT job_graph
            FROM lowcode.job
            WHERE job_name = $1
            LIMIT 1
            """,
            job_name,
        )

        if not graph_result:
            return jsonify({"error": f"未找到后端 job_name 为“{job_name}”的任务流数据"}), 404

        job_graph_dict = parse_json_field(graph_result["job_graph"], default={})
        if not isinstance(job_graph_dict, dict):
            return jsonify({"error": f"job_graph 数据格式异常：{job_name}"}), 500

        new_dag_simplified = simplify_dag_for_comparison(dag)
        old_dag_simplified = simplify_dag_for_comparison(job_graph_dict.get("dag", []))
        if old_dag_simplified != new_dag_simplified:
            return jsonify({"error": "同一个任务名的情况下，不能修改图的结构（DAG），请修改任务名！！！"}), 400

        results = await conn.fetch(
            """
            SELECT config_name, config_schema
            FROM lowcode.config_for_job
            WHERE job_name = $1
            """,
            job_name,
        )

        source_row = await conn.fetchrow(
            """
            SELECT config_schema
            FROM lowcode.config_for_job
            WHERE job_name = $1 AND config_name = $2
            LIMIT 1
            """,
            job_name,
            source_config_name,
        )

        if not source_row:
            return jsonify({"error": f"未找到来源配置：{source_config_name}"}), 404

        source_schema = parse_json_field(source_row["config_schema"], default={})
        if not isinstance(source_schema, dict):
            return jsonify({"error": f"来源配置格式异常：{source_config_name}"}), 500

        source_ops = source_schema.get("ops", {})
        if not isinstance(source_ops, dict):
            return jsonify({"error": f"来源配置中的 ops 格式异常：{source_config_name}"}), 500

        job_dir = safe_dir_name(cn_job_name)
        cfg_dir = safe_dir_name(config_name)
        ops_config = {}
        copy_plan = []

        for n in nodes:
            label = extract_base_label(n["label"])
            component_type = name_mapping.get(label)
            if not component_type:
                continue

            node_id_raw = n["id"].replace("node_", "")
            node_dir = safe_dir_name(node_id_raw)
            full_node_name = f"{component_type}_id_{node_id_raw}"
            config = {}

            for f in n.get("formSchema", {}).get("fields", []):
                if f.get("type") == "cascader" or f.get("value") is None:
                    continue

                name = f.get("name")
                value = f.get("value")
                if not name:
                    continue

                if name in FILE_FIELD_NAMES and value:
                    filename_only = os.path.basename(str(value)).replace("\\", "/").split("/")[-1]
                    dst_path = (input_dir / job_dir / cfg_dir / node_dir / filename_only).resolve()

                    try:
                        dst_path.relative_to(input_dir)
                    except Exception:
                        return jsonify({"error": f"非法目标文件路径: {dst_path}"}), 403

                    # 1) 新配置目录已存在文件，说明本次重新上传
                    if dst_path.exists() and dst_path.is_file():
                        config[name] = dst_path.as_posix()
                        continue

                    # 2) 从旧配置中复用
                    source_path = (
                        source_ops.get(full_node_name, {})
                        .get("config", {})
                        .get(name)
                    )
                    if source_path:
                        try:
                            src_path = normalize_source_file_path(source_path, input_dir)
                            if src_path.exists() and src_path.is_file():
                                copy_plan.append((src_path, dst_path))
                                config[name] = dst_path.as_posix()
                                continue
                        except ValueError:
                            pass

                    # 3) 兜底：前端 value 本身就是 input_dir 下旧完整路径
                    try:
                        raw_path = Path(str(value)).resolve()
                        raw_path.relative_to(input_dir)
                        if raw_path.exists() and raw_path.is_file():
                            copy_plan.append((raw_path, dst_path))
                            config[name] = dst_path.as_posix()
                            continue
                    except Exception:
                        pass

                    return jsonify({
                        "error": "未找到可复用文件",
                        "node": full_node_name,
                        "field": name,
                        "target_config": config_name,
                        "source_path": str(source_path) if source_path else "",
                        "frontend_value": str(value),
                        "dst_path": str(dst_path),
                    }), 404

                config[name] = convert_value(value)

            ops_config[full_node_name] = {"config": config}

        new_ops_simplified = simplify_ops_for_comparison(ops_config)

        old_ops_list = []
        for row in results:
            parsed_schema = parse_json_field(row["config_schema"], default={})
            if not isinstance(parsed_schema, dict):
                return jsonify({
                    "error": f"历史配置格式异常：{row['config_name']}"
                }), 500

            old_ops = parsed_schema.get("ops", {})
            if not isinstance(old_ops, dict):
                return jsonify({
                    "error": f"历史配置中的 ops 格式异常：{row['config_name']}"
                }), 500

            old_ops_list.append(simplify_ops_for_comparison(old_ops))

        is_new_config = all(old != new_ops_simplified for old in old_ops_list)

        if not is_new_config:
            return Success(
                message="配置已存在，未保存重复项",
                data={"job_name": job_name}
            )

        for src_path, dst_path in copy_plan:
            await copy_into_node_dir(src_path, dst_path)

        try:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO lowcode.config_for_job (job_name, config_schema, config_name, graph_by_config)
                    VALUES ($1, $2, $3, $4)
                    """,
                    job_name,
                    json.dumps({"ops": ops_config}, ensure_ascii=False),
                    config_name,
                    json.dumps(data, ensure_ascii=False),
                )
        except Exception as e:
            return jsonify({"error": f"保存新配置失败: {str(e)}"}), 500

        return Success(
            message="图任务信息以及图的新配置保存成功",
            data={"job_name": job_name, "config_name": config_name},
        )

    except Exception as e:
        return jsonify({"error": f"处理失败: {type(e).__name__}: {e}"}), 500
    finally:
        if conn:
            await conn.close()


@component_router.route("/component/create_from_existing", methods=["POST"])
async def create_task_from_existing():
    conn = None
    try:
        data = await request.get_json() or {}
        source_cn_job_name = (data.get("source_cn_job_name") or data.get("sourceCnJobName") or "").strip()
        source_config_name = (data.get("source_config_name") or data.get("sourceConfigName") or "").strip()
        target_cn_job_name = (data.get("target_cn_job_name") or data.get("targetCnJobName") or "").strip()
        target_config_name = (data.get("target_config_name") or data.get("targetConfigName") or "").strip()
        nodes = data.get("nodes")

        if not isinstance(nodes, list) or not nodes:
            return jsonify({"error": "缺少参数：nodes，且必须为非空数组"}), 400
        if not source_cn_job_name:
            return jsonify({"error": "缺少参数：source_cn_job_name"}), 400
        if not source_config_name:
            return jsonify({"error": "缺少参数：source_config_name"}), 400
        if not target_cn_job_name:
            return jsonify({"error": "缺少参数：target_cn_job_name"}), 400
        if not target_config_name:
            return jsonify({"error": "缺少参数：target_config_name"}), 400

        if source_cn_job_name == target_cn_job_name:
            return jsonify({"error": "基于此创建时，新任务名不能与原任务名相同"}), 400

        conn = await get_pg_connection()
        if conn is None:
            return jsonify({"error": "无法连接数据库"}), 500

        input_dir = get_input_dir().resolve()

        # 1. 组件输出映射
        try:
            component_outputs = await load_component_outputs(conn)
        except Exception as e:
            return jsonify({"error": f"加载 component_info 失败: {type(e).__name__}: {e}"}), 500

        # 2. 中文组件名映射
        try:
            name_mapping = await load_name_mapping(conn)
        except Exception as e:
            return jsonify({"error": f"加载 ops_mapping 失败: {str(e)}"}), 500

        # 3. 构建 DAG（复用 save_graph_task 的规则）
        dag = build_dag_from_nodes(nodes, name_mapping, component_outputs)

        # 4. 目标任务名必须不存在
        exists = await conn.fetchrow(
            """
            SELECT 1
            FROM lowcode.frontend_job_graph
            WHERE cn_job_name = $1
            LIMIT 1
            """,
            target_cn_job_name,
        )
        if exists:
            return jsonify({"error": f"目标任务名已存在：{target_cn_job_name}"}), 409

        # 5. 校验目标配置名
        try:
            target_config_name = validate_config_name_strict(
                target_cn_job_name,
                target_config_name,
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        try:
            await ensure_config_name_available(conn, target_config_name)
        except ValueError as e:
            return jsonify({"error": str(e)}), 409

        # 6. 校验来源任务存在，并拿到来源 job_name
        source_job_row = await conn.fetchrow(
            """
            SELECT job_name
            FROM lowcode.frontend_job_graph
            WHERE cn_job_name = $1
            LIMIT 1
            """,
            source_cn_job_name,
        )
        if not source_job_row:
            return jsonify({"error": f"未找到来源任务：{source_cn_job_name}"}), 404

        source_job_name = source_job_row["job_name"]

        # 7. 校验来源配置存在
        source_cfg_row = await conn.fetchrow(
            """
            SELECT 1
            FROM lowcode.config_for_job
            WHERE job_name = $1 AND config_name = $2
            LIMIT 1
            """,
            source_job_name,
            source_config_name,
        )
        if not source_cfg_row:
            return jsonify({"error": f"未找到来源配置：{source_config_name}"}), 404

        # 8. 生成新的前端图配置
        cloned_graph_payload = clone_graph_payload_for_new_task(
            data=data,
            source_cn_job_name=source_cn_job_name,
            source_config_name=source_config_name,
            target_cn_job_name=target_cn_job_name,
            target_config_name=target_config_name,
        )

        # 9. 构造 ops_config 与 copy_plan
        try:
            ops_config, copy_plan = build_cloned_ops_and_copy_plan_from_current_page(
                current_payload=cloned_graph_payload,
                input_dir=input_dir,
                source_cn_job_name=source_cn_job_name,
                source_config_name=source_config_name,
                target_cn_job_name=target_cn_job_name,
                target_config_name=target_config_name,
                name_mapping=name_mapping,
            )
        except FileNotFoundError as e:
            return jsonify({"error": str(e)}), 404
        except ValueError as e:
            return jsonify({"error": str(e)}), 403

        # 10. 先复制文件，再写库
        for src_path, dst_path in copy_plan:
            await copy_into_node_dir(src_path, dst_path)

        new_job_name = f"job_{uuid.uuid4().hex[:8]}"

        try:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO lowcode.job (job_name, job_graph, repo_id)
                    VALUES ($1, $2, $3)
                    """,
                    new_job_name,
                    json.dumps({"dag": dag}, ensure_ascii=False),
                    1,
                )

                await conn.execute(
                    """
                    INSERT INTO lowcode.config_for_job (
                        job_name,
                        config_schema,
                        config_name,
                        graph_by_config
                    )
                    VALUES ($1, $2, $3, $4)
                    """,
                    new_job_name,
                    json.dumps({"ops": ops_config}, ensure_ascii=False),
                    target_config_name,
                    json.dumps(cloned_graph_payload, ensure_ascii=False),
                )

                await conn.execute(
                    """
                    INSERT INTO lowcode.frontend_job_graph (
                        graph_config,
                        job_name,
                        cn_job_name
                    )
                    VALUES ($1, $2, $3)
                    """,
                    json.dumps(cloned_graph_payload, ensure_ascii=False),
                    new_job_name,
                    target_cn_job_name,
                )

        except Exception as e:
            return jsonify({"error": f"基于此创建失败: {str(e)}"}), 500

        print("触发Dagster热加载")
        trigger_dagster_reload()

        print("触发Dagster监听")
        result = await monitor_and_recover_repository(
            job_name=new_job_name,
            config_name=target_config_name,
        )
        if result and result.get("code") == 500:
            return jsonify(result.get("message")), 500

        return Success(
            message="基于此创建成功",
            data={
                "job_name": new_job_name,
                "cn_job_name": target_cn_job_name,
                "config_name": target_config_name,
                "source_job_name": source_job_name,
                "source_cn_job_name": source_cn_job_name,
                "source_config_name": source_config_name,
            },
        )

    except Exception as e:
        return jsonify({"error": f"处理失败: {type(e).__name__}: {e}"}), 500
    finally:
        if conn:
            await conn.close()


@component_router.route("/component/discard_unsaved_task", methods=["POST"])
async def discard_unsaved_task():
    conn = None
    try:
        data = await request.get_json() or {}

        cn_job_name = (data.get("cn_job_name") or data.get("cnJobName") or "").strip()
        config_name = (data.get("config_name") or data.get("configName") or "").strip()

        if not cn_job_name:
            return jsonify({"error": "缺少参数：cn_job_name"}), 400
        if not config_name:
            return jsonify({"error": "缺少参数：config_name"}), 400

        conn = await get_pg_connection()
        if conn is None:
            return jsonify({"error": "无法连接数据库"}), 500

        input_dir = get_input_dir().resolve()
        job_path, cfg_path = safe_target_cfg_dir(input_dir, cn_job_name, config_name)

        # 1. 先查这个中文任务名是否已存在
        job_row = await conn.fetchrow(
            """
            SELECT job_name
            FROM lowcode.frontend_job_graph
            WHERE cn_job_name = $1
            LIMIT 1
            """,
            cn_job_name,
        )

        # 情况 A：整个任务还没保存过
        if not job_row:
            remove_dir_if_exists(cfg_path)
            remove_parent_if_empty(job_path, input_dir)

            return Success(
                message="已清理未保存任务的上传目录",
                data={
                    "cn_job_name": cn_job_name,
                    "config_name": config_name,
                    "deleted_cfg_dir": str(cfg_path),
                },
            )

        job_name = job_row["job_name"]

        # 2. 任务已存在，再查这个配置是否已经保存
        cfg_row = await conn.fetchrow(
            """
            SELECT 1
            FROM lowcode.config_for_job
            WHERE job_name = $1 AND config_name = $2
            LIMIT 1
            """,
            job_name,
            config_name,
        )

        # 情况 B：配置已经保存，不能删
        if cfg_row:
            return jsonify({
                "error": f"配置已保存，不能清理目录：{config_name}"
            }), 409

        # 情况 C：任务存在，但当前配置未保存 -> 只删当前配置目录
        remove_dir_if_exists(cfg_path)
        remove_parent_if_empty(job_path, input_dir)

        return Success(
            message="已清理未保存配置的上传目录",
            data={
                "cn_job_name": cn_job_name,
                "config_name": config_name,
                "deleted_cfg_dir": str(cfg_path),
            },
        )

    except Exception as e:
        return jsonify({"error": f"处理失败: {type(e).__name__}: {e}"}), 500
    finally:
        if conn:
            await conn.close()


@component_router.route("/component/component_config", methods=["GET"])
async def get_component_config():
    user_component_name = request.args.get("component_name")
    if not user_component_name:
        return Fail(code=400, message="缺少参数：组件名称（component_name）")

    conn = await get_pg_connection()
    if conn is None:
        return Fail(code=500, message="数据库连接失败")

    try:
        # 第一步：从 ops_mapping 表中通过 user_component_name 匹配获取 dag_func_name
        ops_result = await conn.fetchrow(
            """
            SELECT dag_func_name
            FROM lowcode.ops_mapping
            WHERE user_component_name = $1
            LIMIT 1
            """,
            user_component_name,
        )
        if not ops_result:
            return Fail(code=404, message=f"未找到组件映射：{user_component_name}")

        dag_func_name = ops_result["dag_func_name"]

        # 第二步：根据 dag_func_name 去 component_info 表中查找组件配置
        result = await conn.fetchrow(
            """
            SELECT ins, outs, config_schema, tags
            FROM lowcode.component_info
            WHERE component_name = $1
            LIMIT 1
            """,
            dag_func_name,
        )
        if not result:
            return Fail(code=404, message=f"组件 '{dag_func_name}' 未找到")

        ins = parse_json_field(result["ins"], default={})
        outs = parse_json_field(result["outs"], default={})

        try:
            config_schema = eval(result["config_schema"]) if result["config_schema"] else {}
        except Exception as e:
            return Fail(code=500, message=f"config_schema 字段解析失败：{str(e)}")

        tags = parse_json_field(result["tags"], default={})
        config_frontend_raw = tags.get("config_frontend", "{}")
        config_frontend = parse_json_field(config_frontend_raw, default={})

        json_result = {
            "formName": "参数信息配置",
            "output": [],
            "fields": [],
        }

        for out_key, out_val in outs.items():
            json_result["output"].append({
                "label": out_val.get("description", ""),
                "value": out_key,
            })

        for in_key, in_val in ins.items():
            json_result["fields"].append({
                "type": "cascader",
                "label": in_val.get("description", ""),
                "name": in_key,
                "options": [],
                "value": "",
                "output": "",
            })

        for key, schema_item in config_schema.items():
            frontend_item = config_frontend.get(key, {})
            field = {
                "name": key,
                "label": schema_item.get("description", ""),
                "required": schema_item.get("is_required", ""),
                "placeholder": schema_item.get("placeholder", ""),
                "rows": schema_item.get("rows", ""),
                "type": frontend_item.get("type", "text"),
                "value": frontend_item.get("value", ""),
            }

            if "option" in frontend_item:
                field["options"] = [
                    {"label": str(opt), "value": str(opt)}
                    for opt in frontend_item["option"]
                ]

            for extra_key in ["accept", "limit"]:
                if extra_key in frontend_item:
                    field[extra_key] = frontend_item[extra_key]

            json_result["fields"].append(field)

        return jsonify(json_result)

    except Exception as e:
        return Fail(code=500, message=f"查询失败：{str(e)}")
    finally:
        if conn:
            await conn.close()


# 分类规则：根据 dag_func_name 判断属于哪个分组
@component_router.route("/component/list", methods=["GET"])
async def get_component_list():
    conn = await get_pg_connection()
    if conn is None:
        return Fail(code=500, message="数据库连接失败")

    CATEGORIES = defaultdict(list)

    try:
        rows = await conn.fetch(
            """
            SELECT tags
            FROM lowcode.component_info
            """
        )

        for row in rows:
            try:
                raw_tag = row["tags"]
                tags = parse_json_field(raw_tag, default={})
                classification = tags.get("classification")
                label = tags.get("label")

                if classification and label:
                    if label not in CATEGORIES[classification]:
                        CATEGORIES[classification].append(label)

            except Exception as e:
                print("标签解析错误：", e)
                continue

    except Exception as e:
        return Fail(code=501, message=f"加载分类失败：{str(e)}")

    try:
        rows = await conn.fetch(
            """
            SELECT user_component_name, dag_func_name
            FROM lowcode.ops_mapping
            """
        )

        group_map = defaultdict(list)

        for row in rows:
            label = row["user_component_name"]
            value = row["dag_func_name"]

            for category, label_list in CATEGORIES.items():
                if label in label_list:
                    group_map[category].append({
                        "label": label,
                        "value": value,
                    })
                    break

        result = {
            "ops": [
                {"name": category, "options": group_map[category]}
                for category in CATEGORIES
                if category in group_map
            ]
        }
        return jsonify(result)

    except Exception as e:
        return Fail(code=502, message=f"组件映射失败：{str(e)}")
    finally:
        if conn:
            await conn.close()


@component_router.route("/component/batch_update", methods=["GET"])
async def component_batch_update():
    conn = None
    try:
        op_definitions = {}

        for _, module_name, _ in pkgutil.iter_modules(ops.__path__):
            module = importlib.import_module(f"ops.{module_name}")

            for name, obj in inspect.getmembers(module):
                if isinstance(obj, OpDefinition):
                    config_data = {}

                    try:
                        if (
                                hasattr(obj.config_schema, "config_type")
                                and hasattr(obj.config_schema.config_type, "fields")
                        ):
                            config_data = {
                                k: {
                                    "type": (
                                        v.config_type.__class__.__name__
                                        if hasattr(v, "config_type")
                                        else "Unknown"
                                    ),
                                    "description": getattr(v, "description", None),
                                    "default_value": (
                                        v.default_value
                                        if getattr(v, "default_provided", False)
                                        else None
                                    ),
                                    "is_required": getattr(v, "is_required", True),
                                }
                                for k, v in obj.config_schema.config_type.fields.items()
                            }
                        else:
                            config_data = str(obj.config_schema)
                    except Exception:
                        config_data = str(obj.config_schema)

                    input_data = {
                        k: {
                            "dagster_type": getattr(
                                v.dagster_type,
                                "display_name",
                                str(v.dagster_type),
                            ),
                            "description": v.description,
                        }
                        for k, v in (obj.ins or {}).items()
                    }

                    output_data = {
                        k: {
                            "dagster_type": getattr(
                                v.dagster_type,
                                "display_name",
                                str(v.dagster_type),
                            ),
                            "description": v.description,
                            "is_required": v.is_required,
                            "io_manager_key": v.io_manager_key,
                            "metadata": v.metadata,
                            "code_version": v.code_version,
                        }
                        for k, v in (obj.outs or {}).items()
                    }

                    op_definitions[name] = {
                        "ops_func_name": name,
                        "dag_func_name": obj.name,
                        "description": obj.description,
                        "inputs": input_data,
                        "outputs": output_data,
                        "config_schema": config_data,
                        "required_resources": list(obj.required_resource_keys or []),
                        "tags": obj.tags or {},
                        "version": obj.version or "N/A",
                        "retry_policy": str(obj.retry_policy) if obj.retry_policy else None,
                        "module": module_name,
                    }

        conn = await get_pg_connection()
        if conn is None:
            return jsonify({"code": 500, "error": "数据库连接失败"})

        async with conn.transaction():
            await conn.execute("DELETE FROM lowcode.component_info")

            for v in op_definitions.values():
                await conn.execute(
                    """
                    INSERT INTO lowcode.component_info
                    (
                        component_name,
                        description,
                        ins,
                        outs,
                        config_schema,
                        required_resource,
                        tags,
                        version,
                        retry_policy
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    v["dag_func_name"],
                    v.get("description", ""),
                    json.dumps(v.get("inputs", {}), ensure_ascii=False),
                    json.dumps(v.get("outputs", {}), ensure_ascii=False),
                    str(v.get("config_schema", "")),
                    json.dumps(v.get("required_resources", []), ensure_ascii=False),
                    json.dumps(v.get("tags", {}), ensure_ascii=False),
                    v.get("version", "N/A"),
                    v.get("retry_policy", ""),
                )

            await conn.execute("DELETE FROM lowcode.ops_mapping")

            for v in op_definitions.values():
                label = v.get("tags", {}).get("label", "")
                if not v["dag_func_name"] or not label:
                    continue

                await conn.execute(
                    """
                    INSERT INTO lowcode.ops_mapping (
                        user_component_name,
                        dag_func_name,
                        ops_func_name
                    )
                    VALUES ($1, $2, $3)
                    """,
                    label,
                    v["dag_func_name"],
                    v["ops_func_name"],
                )

        return jsonify({
            "code": 200,
            "message": "所有组件更新成功",
            "count": len(op_definitions),
        })

    except Exception as e:
        return jsonify({"code": 500, "error": str(e)})
    finally:
        if conn:
            await conn.close()


"""
parser_op_to_schema.py 生成 ops.json
json_to_component_info.py 保存进 component_info 表
json_to_ops_mapping.py 保存进 ops_mapping 表
component_mapping_dagfunc.py
从 ops_mapping 表提取 user_component_name 和 dag_func_name 映射，生成 JSON 格式
dagfunc_mapping_opsfunc.py
从 ops_mapping 表提取 dag_func_name 和 ops_func_name 映射，生成 JSON 格式
"""
