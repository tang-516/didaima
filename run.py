import json
from dagster import RunRequest
from quart import request, Blueprint
import requests
from quart import jsonify
import httpx
import aiohttp
from app.api.component_utils import monitor_and_recover_repository
from app.api.run_utils import wait_for_run_completion
from app.core import CTX_USER_ID, AuthControl
from app.schema import Fail, Success
from app.settings.setting import DAGSTER_GRAPHQL_URL, get_pg_connection

run_router = Blueprint("run_router", __name__)


# @run_router.before_request
# async def before_request():
#     await AuthControl.is_authed()
#     user_id = CTX_USER_ID.get()
#     if not user_id:
#         return Fail(message="用户未登录")

@run_router.route("/get_run_status", methods=["POST"])
async def get_run_status():
    try:
        data = await request.json
        run_id = data.get("runId")
    except Exception:
        return Fail(code=400, message="请求体格式错误")
    if not run_id:
        return Fail(code=400, message="缺少参数 runId")
# GraphQL 查询模板
    query = """
    query {
      runOrError(runId: "%s") {
        ... on Run {
          status
          startTime
          endTime
          runId
          pipelineName
          jobName
          tags {
            key
            value
          }
        }
      }
    }
    """% run_id
    # 执行GraphQL查询
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DAGSTER_GRAPHQL_URL, json={"query": query}) as response:
                if response.status != 200:
                    return jsonify({"error": f"查询状态失败, status code: {response.status}"}), 500

                # 解析响应的JSON数据
                response_data = await response.json()
                data = response_data.get("data", {}).get("runOrError", None)

                if data is None:
                    return jsonify({"error": "Run not found or invalid runId"}), 404

                # 提取run状态信息
                run_info = {
                    "status": data.get("status"),
                    "startTime": data.get("startTime"),
                    "endTime": data.get("endTime"),
                    "runId": data.get("runId"),
                    "pipelineName": data.get("pipelineName"),
                    "jobName": data.get("jobName"),
                    "tags": data.get("tags", [])
                }

                return jsonify(run_info)

    except Exception as e:
        return Fail(code=500, message=f"查询失败：{str(e)}")


@run_router.route('/get_run', methods=['POST'])
async def get_run():
    query = """
    query RunQuery($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          id
          pipelineName
          status
          startTime
          endTime
          tags {
            key
            value
          }
          eventConnection {
            events {
              __typename
              ... on MessageEvent {
                message
                level
                timestamp
              }
              ... on ExecutionStepInputEvent {
                inputName
                typeCheck {
                  success
                  label
                  description
                }
              }
              ... on ExecutionStepOutputEvent {
                outputName
                typeCheck {
                  success
                  label
                  description
                }
              }
              ... on ExecutionStepFailureEvent {
                error {
                  message
                  stack
                }
                timestamp
              }
              ... on EngineEvent {
                message
                timestamp
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
    # 从请求体的 JSON 数据中获取 runId
    try:
        data = await request.json
        run_id = data.get("runId")
    except Exception:
        return Fail(code=400, message="请求体格式错误")
    if not run_id:
        return Fail(code=400, message="缺少参数 runId")
    # 构造 GraphQL 查询变量
    variables = {"runId": run_id}
    try:
        # 使用 aiohttp 异步请求替代 requests（兼容 async 环境）
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    DAGSTER_GRAPHQL_URL,
                    json={'query': query, 'variables': variables}
            ) as resp:
                if resp.status != 200:
                    return Fail(code=500, message="Dagster 请求失败")

                all_data = await resp.json()
                run_or_error = all_data.get("data", {}).get("runOrError")

                if not run_or_error:
                    return Fail(code=404, message="未找到对应的运行记录")

                typename = run_or_error.get("__typename")
                if typename == "PythonError":
                    return Fail(code=500, message=f"Dagster 错误：{run_or_error.get('message')}")

                events = run_or_error.get("eventConnection", {}).get("events", [])
                info_events = [
                    {
                        "timestamp": evt.get("timestamp"),
                        "message": evt.get("message"),
                        "type": evt.get("__typename")
                    }
                    for evt in events
                    if evt.get("level") == "INFO"
                ]

                # status = wait_for_run_completion(run_id)

                return Success(data={
                    "runId": run_id,
                    "info_events": info_events,
                    # "status": status,
                })
    except Exception as e:
        return Fail(code=500, message=f"查询失败：{str(e)}")


@run_router.route("/launch_run", methods=["POST"])
async def launch_run():
    mutation = """
    mutation LaunchRunMutation(
      $repositoryLocationName: String!
      $repositoryName: String!
      $jobName: String!
      $runConfigData: RunConfigData!
    ) {
      launchRun(
        executionParams: {
          selector: {
            repositoryLocationName: $repositoryLocationName
            repositoryName: $repositoryName
            jobName: $jobName
          }
          runConfigData: $runConfigData
        }
      ) {
        __typename
        ... on LaunchRunSuccess {
          run {
            runId
          }
        }
        ... on RunConfigValidationInvalid {
          errors {
            message
            reason
          }
        }
        ... on PythonError {
          message
        }
      }
    }
    """

    try:
        data = await request.get_json() or {}
        repository_location_name = (data.get("repositoryLocationName") or "").strip()
        repository_name = (data.get("repositoryName") or "").strip()
        job_name = (data.get("jobName") or "").strip()
        config_name = (data.get("configName") or data.get("config_name") or "").strip()
        run_config_data = data.get("runConfigData") or {}
    except Exception:
        return Fail(code=400, message="请求体格式错误")

    if not repository_location_name or not repository_name or not job_name:
        return Fail(
            code=400,
            message="缺少必要参数 repositoryLocationName、repositoryName 或 jobName",
        )

    # runConfigData 为空时，从数据库读取 config_schema
    if not run_config_data:
        if not config_name:
            return Fail(code=400, message="缺少 configName 参数")

        conn = None
        try:
            conn = await get_pg_connection()
            if conn is None:
                return Fail(code=500, message="数据库连接失败")

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
                return Fail(code=404, message=f"未找到 job [{job_name}] 对应的配置")

            raw_config_schema = row["config_schema"]
            if isinstance(raw_config_schema, dict):
                run_config_data = raw_config_schema
            elif isinstance(raw_config_schema, str):
                run_config_data = json.loads(raw_config_schema)
            else:
                return Fail(code=500, message="config_schema 字段类型不支持")

        except Exception as e:
            return Fail(code=500, message=f"配置查询失败：{str(e)}")
        finally:
            if conn:
                await conn.close()

    variables = {
        "repositoryLocationName": repository_location_name,
        "repositoryName": repository_name,
        "jobName": job_name,
        "runConfigData": run_config_data,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                DAGSTER_GRAPHQL_URL,
                json={"query": mutation, "variables": variables},
            ) as resp:
                if resp.status != 200:
                    return Fail(code=500, message="Dagster API 请求失败")

                result = await resp.json()
                launch_run_result = result.get("data", {}).get("launchRun", {}) or {}
                typename = launch_run_result.get("__typename")

                if typename == "LaunchRunSuccess":
                    run_id = launch_run_result["run"]["runId"]

                    conn = None
                    try:
                        conn = await get_pg_connection()
                        if conn is None:
                            return Fail(code=500, message="数据库连接失败")

                        await conn.execute(
                            """
                            INSERT INTO lowcode.run_history (run_id, job_name, config_name)
                            VALUES ($1, $2, $3)
                            """,
                            run_id,
                            job_name,
                            config_name,
                        )
                    except Exception as e:
                        return Fail(code=500, message=f"运行记录写入失败：{str(e)}")
                    finally:
                        if conn:
                            await conn.close()

                    return Success(message="任务已成功提交", data={"runId": run_id})

                elif typename == "RunConfigValidationInvalid":
                    errors = launch_run_result.get("errors", [])
                    messages = "; ".join(
                        e.get("message", "未知错误") for e in errors
                    )
                    return Fail(code=422, message=f"配置验证失败：{messages}")

                elif typename == "PythonError":
                    message = launch_run_result.get("message", "未知异常")
                    return Fail(code=500, message=f"Dagster 执行异常：{message}")

                else:
                    return Fail(code=500, message="未知的 Dagster 返回类型")

    except Exception as e:
        return Fail(code=500, message=f"运行失败：{str(e)}")


@run_router.route('/runs', methods=['GET'])
async def runsOrError():
    query1 = """
    query MyQuery {
      runsOrError {
        ... on Runs {
          results {
            runId
            jobName
            status
            runConfigYaml
            startTime
            endTime
          }
        }
      }
    }
    """
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(DAGSTER_GRAPHQL_URL, json={'query': query1})
            response.raise_for_status()  # 抛出 HTTP 错误（非200）
            data = response.json()  # 注意：这里不需要 await
            return jsonify(data), 200
        except httpx.HTTPStatusError as e:
            return jsonify({"error": f"Dagster HTTP error: {e.response.status_code}"}), e.response.status_code
        except Exception as e:
            return jsonify({"error": f"Internal error: {str(e)}"}), 500
