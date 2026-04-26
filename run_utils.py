import asyncio
import json
from quart import request, Blueprint
import requests
from quart import jsonify
import httpx
import aiohttp
import time
from app.api.component_utils import trigger_dagster_reload
from app.settings.setting import DAGSTER_GRAPHQL_URL, get_pg_connection


async def wait_for_run_completion(run_id):
    query = """
    query GetRunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        __typename
        ... on Run {
          status
          runId
          stepStats {
            stepKey
            status
          }
        }
        ... on RunNotFoundError {
          message
        }
      }
    }
    """
    variables = {"runId": run_id}
    start_time = time.time()
    TIMEOUT = 60  # 1 分钟

    async with aiohttp.ClientSession() as session:
        while True:
            if time.time() - start_time > TIMEOUT:
                print("超时退出")
                return "TIMEOUT"

            async with session.post(
                DAGSTER_GRAPHQL_URL,
                json={"query": query, "variables": variables},
                headers={"Content-Type": "application/json"}
            ) as resp:
                result = await resp.json()
                run_data = result.get("data", {}).get("runOrError", {})
                typename = run_data.get("__typename")
                status = run_data.get("status")

                print(f"当前任务状态: {status}，类型: {typename}")

                if typename == "RunNotFoundError":
                    return "NOT_FOUND"

                if status in ["SUCCESS", "FAILURE", "CANCELED"]:
                    return status
                # trigger_dagster_reload()
                # await asyncio.sleep(6)

