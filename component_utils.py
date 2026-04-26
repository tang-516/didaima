import json
from typing import Optional, Tuple
import asyncpg
import requests
from quart import jsonify
from app.settings import DAGSTER_GRAPHQL_URL, get_pg_connection
import re
import os
import asyncio
import shutil
from pathlib import Path

FILE_FIELD_NAMES = {"filename_csv", "filename_txt", "filename_xlsx", "filename_xls"}
APP_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = (APP_DIR / "data" / "input").resolve()


def parse_json_field(value, default=None):
    if value is None:
        return {} if default is None else default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return {} if default is None else default
        return json.loads(value)
    return value


async def safe_remove_tree(path: Path, root: Path):
    path = path.resolve()
    root = root.resolve()

    try:
        path.relative_to(root)
    except Exception:
        raise ValueError(f"非法删除路径：{path}")

    if not path.exists():
        return

    await asyncio.to_thread(shutil.rmtree, str(path))


async def ensure_config_name_available(conn: asyncpg.Connection, config_name: str):
    dup = await conn.fetchval(
        """
        SELECT 1 FROM lowcode.config_for_job WHERE config_name = $1 LIMIT 1
        """,
        config_name,
    )
    if dup:
        raise ValueError(f"配置名已存在：{config_name}")


async def generate_next_config_name(conn: asyncpg.Connection, job_name: str, cn_job_name: str) -> str:
    rows = await conn.fetch(
        """
        SELECT config_name FROM lowcode.config_for_job WHERE job_name = $1
        """,
        job_name,
    )

    max_n = 0
    pattern = re.compile(rf"^{re.escape(cn_job_name)}_配置(\d+)$")
    for row in rows:
        cfg = (row["config_name"] or "").strip()
        m = pattern.fullmatch(cfg)
        if m:
            max_n = max(max_n, int(m.group(1)))

    return f"{cn_job_name}_配置{max_n + 1}"


def validate_config_name_strict(cn_job_name: str, config_name: str) -> str:
    """
    强校验：config_name 必须严格等于 f"{cn_job_name}_配置N" 且 N 为 >=1 的整数
    返回清洗后的 config_name（strip 之后）
    """
    cn = (cn_job_name or "").strip()
    cfg = (config_name or "").strip()
    if not cn:
        raise ValueError("缺少参数：中文任务名（cn_job_name）")
    if not cfg:
        raise ValueError("缺少参数：任务配置名（config_name）")
    if ".." in cn or ".." in cfg:
        raise ValueError("任务名或配置名包含非法片段 '..'")

    m = re.fullmatch(rf"{re.escape(cn)}_配置(\d+)", cfg)
    if not m:
        raise ValueError(f"config_name 格式错误，必须为：{cn}_配置N（例如：{cn}_配置1）")

    n = int(m.group(1))
    if n < 1:
        raise ValueError("config_name 中的配置序号必须 >= 1")
    return cfg


def safe_dir_name(name: str) -> str:
    """
    Windows/Unix 都尽量安全的目录名：保留中文，但替换非法字符，防止 ../
    """
    name = (name or "").strip()
    name = name.replace("..", "_")
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name).strip(". ")
    return name or "unnamed_job"


def is_file_reader_node(node: dict) -> bool:
    fields = node.get("formSchema", {}).get("fields", []) or []
    for f in fields:
        if f.get("type") == "cascader":
            continue
        if f.get("name") in FILE_FIELD_NAMES and f.get("value"):
            return True
    return False


async def copy_into_node_dir(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        shutil.copy2,
        str(src),
        str(dst),
    )


def trigger_dagster_reload():
    query = """
    mutation ReloadWorkspaceMutation {
      reloadWorkspace {
        __typename
        ... on Workspace {
          locationEntries {
            id
            name
            loadStatus
          }
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
    """
    try:
        response = requests.post(
            DAGSTER_GRAPHQL_URL,
            json={"query": query},
            headers={"Content-Type": "application/json"},
        )
        result = response.json()
        if response.status_code == 200 and "errors" not in result:
            print("Dagster workspace reloaded successfully!")
            return True
        else:
            print("Reload failed:", result)
            return False
    except Exception as e:
        print("Error reloading workspace:", str(e))
        return False


async def monitor_and_recover_repository(job_name: str, config_name: str):
    while True:
        try:
            print("正在检查 Dagster 仓库状态...")
            query = """
            {
              workspaceOrError {
                ... on Workspace {
                  locationEntries {
                    name
                    loadStatus
                    locationOrLoadError {
                      __typename
                      ... on PythonError {
                        message
                        stack
                      }
                    }
                  }
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """

            resp = requests.post(DAGSTER_GRAPHQL_URL, json={"query": query})
            data = resp.json()

            if "errors" in data:
                print("GraphQL 查询失败:", data["errors"])
                print("执行删除操作")
                await rollback_failed_job_data(job_name, config_name)
                print("触发Dagster热加载")
                trigger_dagster_reload()
                await asyncio.sleep(5)
                continue

            workspace = data.get("data", {}).get("workspaceOrError", {})
            typename = workspace.get("__typename")
            if typename == "PythonError":
                print("Dagster Workspace 加载失败")
                print("错误信息:", workspace.get("message"))
                await rollback_failed_job_data(job_name, config_name)
                trigger_dagster_reload()
                await asyncio.sleep(5)
                continue

            locations = workspace.get("locationEntries", [])
            for loc in locations:
                name = loc.get("name")
                status = loc.get("loadStatus", "")
                error_obj = loc.get("locationOrLoadError", {})
                error_type = error_obj.get("__typename")

                print(f"location: {name}, status: {status}")

                if error_type == "PythonError":
                    print(f"仓库 {name} 加载失败，错误信息: {error_obj.get('message')}")
                    print("执行删除操作")
                    await rollback_failed_job_data(job_name, config_name)
                    print("触发Dagster热加载")
                    trigger_dagster_reload()
                    await asyncio.sleep(5)
                    return {
                        "message": "您构造的任务图有错误，已删除新建的任务，请重新规划！",
                        "data": error_obj.get("message"),
                        "code": 500
                    }

            failed_locations = [
                loc for loc in locations
                if loc.get("loadStatus", "").lower() != "loaded"
            ]
            print("Failed locations:", failed_locations)
            print("实际返回的仓库状态列表:", [loc["loadStatus"] for loc in locations])
            if not failed_locations:
                print("仓库状态正常 (Loaded)")
                break

        except Exception as e:
            print("监听或恢复异常:", str(e))
            await asyncio.sleep(5)


async def rollback_failed_job_data(job_name: str, config_name: str):
    conn = await get_pg_connection()
    if not conn:
        print("数据库连接失败")
        return

    job_dir = None
    config_dir = None
    delete_whole_job_dir = False

    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT job_name, graph_by_config FROM lowcode.config_for_job WHERE config_name = $1 AND job_name = $2
                LIMIT 1
                """,
                config_name,
                job_name,
            )

            if not row:
                print(f"未找到待回滚记录: job_name={job_name}, config_name={config_name}")
                return

            graph_by_config_raw = row["graph_by_config"] or "{}"

            cn_job_name = ""
            try:
                graph_by_config = parse_json_field(graph_by_config_raw, default={})
                cn_job_name = (
                    graph_by_config.get("cn_job_name")
                    or graph_by_config.get("cnJobName")
                    or ""
                ).strip()
            except Exception:
                cn_job_name = ""

            if not cn_job_name and "_配置" in config_name:
                cn_job_name = config_name.rsplit("_配置", 1)[0].strip()

            if not cn_job_name:
                print(f"无法确定任务目录: job_name={job_name}, config_name={config_name}")
                return

            job_dir = (INPUT_DIR / safe_dir_name(cn_job_name)).resolve()
            config_dir = (job_dir / safe_dir_name(config_name)).resolve()

            try:
                job_dir.relative_to(INPUT_DIR)
                config_dir.relative_to(INPUT_DIR)
            except Exception:
                print(f"非法目录路径，拒绝删除: job_dir={job_dir}, config_dir={config_dir}")
                return

            await conn.execute(
                """
                DELETE FROM lowcode.config_for_job WHERE config_name = $1 AND job_name = $2
                """,
                config_name,
                job_name,
            )

            ref_cnt = await conn.fetchval(
                """
                SELECT COUNT(*) FROM lowcode.config_for_job WHERE job_name = $1
                """,
                job_name,
            )
            ref_cnt = int(ref_cnt or 0)

            if ref_cnt == 0:
                await conn.execute(
                    """
                    DELETE FROM lowcode.job WHERE job_name = $1
                    """,
                    job_name,
                )
                await conn.execute(
                    """
                    DELETE FROM lowcode.frontend_job_graph WHERE job_name = $1
                    """,
                    job_name,
                )
                delete_whole_job_dir = True

        print(f"数据库回滚成功: job_name={job_name}, config_name={config_name}")

    except Exception as e:
        print("回滚失败:", str(e))
        return
    finally:
        try:
            await conn.close()
        except Exception:
            pass

    try:
        if delete_whole_job_dir:
            await safe_remove_tree(job_dir, INPUT_DIR)
            print(f"已删除任务目录: {job_dir}")
        else:
            await safe_remove_tree(config_dir, INPUT_DIR)
            print(f"已删除配置目录: {config_dir}")

            if job_dir.exists() and job_dir.is_dir():
                try:
                    next(job_dir.iterdir())
                except StopIteration:
                    await asyncio.to_thread(job_dir.rmdir)
                    print(f"已删除空任务目录: {job_dir}")

    except Exception as e:
        print(f"文件目录删除失败: job_name={job_name}, config_name={config_name}, error={e}")


def extract_base_label(label: str) -> str:
    return label.rsplit("-", 1)[0]


def convert_value(value):
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            return value
    return value


def simplify_dag_for_comparison(dag_list):
    simplified = []
    for node in dag_list:
        component_type = node["name"].split("_id_")[0]
        simplified_inputs = {}
        for input_key, input_val in node.get("inputs", {}).items():
            source_task_type = input_val["task"].split("_id_")[0]
            simplified_inputs[input_key] = {
                "task": source_task_type,
                "output": input_val["output"]
            }
        simplified.append({
            "name": component_type,
            "inputs": simplified_inputs
        })
    return sorted(simplified, key=lambda x: x["name"])


def simplify_ops_for_comparison(ops: dict) -> dict:
    """
    忽略 ID，只提取组件类型 + 配置信息，标准化排序用于结构比较
    """
    simplified = {}
    for full_key, val in ops.items():
        component_type = full_key.split("_id_")[0]
        config = val.get("config", {})
        simplified[component_type] = dict(sorted(config.items()))
    return dict(sorted(simplified.items()))


def build_full_node_name_from_node(node: dict, name_mapping: dict) -> Optional[str]:
    label_cn = extract_base_label(node.get("label", ""))
    component_type = name_mapping.get(label_cn)
    if not component_type:
        return None

    node_id_raw = str(node.get("id", "")).replace("node_", "").strip()
    if not node_id_raw:
        return None

    return f"{component_type}_id_{node_id_raw}"


async def load_component_outputs(conn: asyncpg.Connection) -> dict:
    component_outputs = {}
    rows = await conn.fetch(
        """
        SELECT component_name, outs
        FROM lowcode.component_info
        """
    )

    for row in rows:
        component_name = row["component_name"]
        outs_raw = row["outs"]
        try:
            outs_dict = parse_json_field(outs_raw, default={})
            if not isinstance(outs_dict, dict):
                continue

            output_keys = list(outs_dict.keys())
            component_outputs[component_name] = (
                output_keys[0] if len(output_keys) == 1 else output_keys
            )
        except Exception as e:
            print(f"无法解析组件 {component_name} 的 outs 字段: {e}")
            continue

    return component_outputs


async def load_name_mapping(conn: asyncpg.Connection) -> dict:
    rows = await conn.fetch(
        """
        SELECT user_component_name, dag_func_name
        FROM lowcode.ops_mapping
        """
    )
    return {row["user_component_name"]: row["dag_func_name"] for row in rows}


def build_dag_from_nodes(nodes: list, name_mapping: dict, component_outputs: dict) -> list:
    dag = []
    id_to_node = {node["id"]: node for node in nodes}

    for node in nodes:
        label_cn = extract_base_label(node.get("label", ""))
        component_type = name_mapping.get(label_cn)
        if not component_type:
            print(f"label '{label_cn}' 未找到组件类型（请检查 name_mapping）")
            continue

        node_id = str(node.get("id", "")).replace("node_", "")
        if not node_id:
            continue

        entry = {"name": f"{component_type}_id_{node_id}"}
        inputs = {}

        for field in node.get("formSchema", {}).get("fields", []):
            if field.get("type") != "cascader":
                continue

            target_node_id = str(field.get("value", "")).replace("node_", "")
            if not target_node_id:
                print(f"字段 {field.get('name')} 缺失目标节点 value")
                continue

            target_node = id_to_node.get(f"node_{target_node_id}")
            if not target_node:
                print(f"未找到目标节点: node_{target_node_id}")
                continue

            target_label_cn = extract_base_label(target_node.get("label", ""))
            target_type = name_mapping.get(target_label_cn)
            if not target_type:
                print(f"无法从目标节点 {target_label_cn} 解析组件类型")
                continue

            output_val = field.get("output", "")
            if not output_val:
                if "options" in field:
                    for option in field["options"]:
                        if option.get("value") == f"node_{target_node_id}" and option.get("children"):
                            output_val = option["children"][0].get("value", "")
                            break

                if not output_val:
                    target_outputs = component_outputs.get(target_type, [])
                    if isinstance(target_outputs, list):
                        output_val = target_outputs[0] if target_outputs else ""
                    else:
                        output_val = target_outputs

            if not output_val:
                print(
                    f"无法确定输出字段: 来自 {target_type}, "
                    f"node_id={target_node_id}, field={field.get('name')}"
                )
                continue

            input_name = field.get("name")
            if not input_name:
                print(f"字段缺失 name 属性: {field}")
                continue

            inputs[input_name] = {
                "task": f"{target_type}_id_{target_node_id}",
                "output": output_val,
            }
            print(f"输入连接：当前组件 {component_type} <- 来自 {target_type} 的 {output_val}")

        if inputs:
            entry["inputs"] = inputs

        print(f"构建 DAG 节点: {entry}")
        dag.append(entry)

    return dag


def clone_graph_payload_for_new_task(
    data: dict,
    source_cn_job_name: str,
    source_config_name: str,
    target_cn_job_name: str,
    target_config_name: str,
) -> dict:
    payload = json.loads(json.dumps(data, ensure_ascii=False))
    payload["cn_job_name"] = target_cn_job_name
    payload["config_name"] = target_config_name
    payload["configName"] = target_config_name
    payload["source_cn_job_name"] = source_cn_job_name
    payload["source_config_name"] = source_config_name
    payload["sourceConfigName"] = source_config_name
    return payload


def try_resolve_input_path(value, input_dir: Path) -> Optional[Path]:
    """
    兼容两种 value:
    1. 纯文件名，如 data10.csv
    2. input_dir 下的完整路径
    """
    if not value:
        return None

    try:
        raw_path = Path(str(value)).resolve()
        raw_path.relative_to(input_dir)
        if raw_path.exists() and raw_path.is_file():
            return raw_path
    except Exception:
        pass

    return None


def build_cloned_ops_and_copy_plan_from_current_page(
    current_payload: dict,
    input_dir: Path,
    source_cn_job_name: str,
    source_config_name: str,
    target_cn_job_name: str,
    target_config_name: str,
    name_mapping: dict,
):
    source_job_dir = safe_dir_name(source_cn_job_name)
    source_cfg_dir = safe_dir_name(source_config_name)
    target_job_dir = safe_dir_name(target_cn_job_name)
    target_cfg_dir = safe_dir_name(target_config_name)

    nodes = current_payload.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("缺少参数：nodes，且必须为非空数组")

    ops_config = {}
    copy_plan = []

    for n in nodes:
        full_node_name = build_full_node_name_from_node(n, name_mapping)
        if not full_node_name:
            continue

        node_id_raw = str(n.get("id", "")).replace("node_", "")
        node_dir = safe_dir_name(node_id_raw)
        config = {}

        for f in n.get("formSchema", {}).get("fields", []):
            if f.get("type") == "cascader" or f.get("value") is None:
                continue

            name = f.get("name")
            value = f.get("value")
            field_type = f.get("type")

            if not name:
                continue

            is_file_field = (name in FILE_FIELD_NAMES) or (field_type == "upload")

            if is_file_field and value:
                filename_only = os.path.basename(str(value)).replace("\\", "/").split("/")[-1]

                dst_path = (
                    input_dir / target_job_dir / target_cfg_dir / node_dir / filename_only
                ).resolve()

                try:
                    dst_path.relative_to(input_dir)
                except Exception:
                    raise ValueError(f"非法目标文件路径: {dst_path}")

                # 1. 优先使用“当前目标目录里已经存在”的文件
                #    适用于：基于此创建时，用户已经在新任务/新配置里重新上传了文件
                if dst_path.exists() and dst_path.is_file():
                    config[name] = dst_path.as_posix()
                    continue

                # 2. 如果前端传来的 value 本身就是 input_dir 下可定位到的文件路径，则直接使用
                raw_path = try_resolve_input_path(value, input_dir)
                if raw_path and raw_path.exists() and raw_path.is_file():
                    raw_path = raw_path.resolve()

                    try:
                        raw_path.relative_to(input_dir)
                    except Exception:
                        raise ValueError(f"非法原始文件路径: {raw_path}")

                    # 如果 raw_path 就是目标路径本身，不需要复制
                    if raw_path == dst_path:
                        config[name] = dst_path.as_posix()
                    else:
                        copy_plan.append((raw_path, dst_path))
                        config[name] = dst_path.as_posix()
                    continue

                # 3. 回退到来源任务/来源配置目录，复用旧文件
                src_path = (
                    input_dir / source_job_dir / source_cfg_dir / node_dir / filename_only
                ).resolve()

                try:
                    src_path.relative_to(input_dir)
                except Exception:
                    raise ValueError(f"非法来源文件路径: {src_path}")

                if src_path.exists() and src_path.is_file():
                    copy_plan.append((src_path, dst_path))
                    config[name] = dst_path.as_posix()
                    continue

                # 4. 三种情况都找不到，才报错
                raise FileNotFoundError(
                    f"未找到可复制文件：节点={full_node_name}, 字段={name}, "
                    f"目标路径={dst_path}, 来源路径={src_path}, 目标配置={target_config_name}"
                )

            config[name] = convert_value(value)

        ops_config[full_node_name] = {"config": config}

    return ops_config, copy_plan


def safe_target_cfg_dir(input_dir: Path, cn_job_name: str, config_name: str) -> Tuple[Path, Path]:
    job_dir = safe_dir_name(cn_job_name)
    cfg_dir = safe_dir_name(config_name)

    job_path = (input_dir / job_dir).resolve()
    cfg_path = (job_path / cfg_dir).resolve()

    try:
        job_path.relative_to(input_dir)
        cfg_path.relative_to(input_dir)
    except Exception:
        raise ValueError("非法目录路径")

    return job_path, cfg_path


def remove_dir_if_exists(path: Path):
    if path.exists() and path.is_dir():
        shutil.rmtree(path, ignore_errors=True)


def remove_parent_if_empty(path: Path, stop_at: Path):
    """
    向上删除空目录，但不会删到 stop_at 之上
    """
    path = path.resolve()
    stop_at = stop_at.resolve()

    while path != stop_at and path.exists() and path.is_dir():
        try:
            next(path.iterdir())
            break
        except StopIteration:
            path.rmdir()
            path = path.parent
        except Exception:
            break


def normalize_source_file_path(old_path, input_dir: Path) -> Path:
    """
    兼容两种情况：
    1. 路径本来就在当前 input_dir 下
    2. 路径是历史项目里的绝对路径（如 MySQL 项目），则截取 data/input 后面的相对部分，
       重新映射到当前项目的 input_dir 下
    """
    raw = str(old_path or "").strip()
    if not raw:
        raise ValueError("空的来源文件路径")

    p = Path(raw).resolve()

    # 情况1：已经在当前项目 input_dir 下
    try:
        p.relative_to(input_dir)
        return p
    except Exception:
        pass

    # 情况2：历史绝对路径，截取 data/input 后面的相对路径
    parts = list(p.parts)
    for i in range(len(parts) - 1):
        if str(parts[i]).lower() == "data" and str(parts[i + 1]).lower() == "input":
            relative_parts = parts[i + 2:]
            if not relative_parts:
                break

            mapped = (input_dir / Path(*relative_parts)).resolve()
            try:
                mapped.relative_to(input_dir)
                return mapped
            except Exception:
                break

    raise ValueError(f"来源文件路径非法: {raw}")


