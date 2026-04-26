from quart import Blueprint, request
import requests

from app.controller.job import JobController
from app.core import CTX_USER_ID, AuthControl
from app.model import Job
from app.model.repository import Repository
from app.schema import Fail, Success
from app.schema.repository import RepositoryCreate, RepositoryUpdate
from app.settings.setting import DAGSTER_GRAPHQL_URL
from quart import jsonify
from app.controller.repository import RepositoryController

repository_router = Blueprint('repository_router', __name__)


# @repository_router.before_request
# async def before_request():
#     await AuthControl.is_authed()
#     user_id = CTX_USER_ID.get()
#     if not user_id:
#         return Fail(msg="用户未登录")


@repository_router.route('/repository/get_repo', methods=['GET'])
async def get_repo():
    query1 = """
    query RepositoriesQuery {
  repositoriesOrError {
    ... on RepositoryConnection {
      nodes {
        name
        location {
          name
        }
      }
    }
  }
}
    """
    try:
        # 发送 GraphQL 请求到 Dagster 服务
        response = requests.post(DAGSTER_GRAPHQL_URL, json={'query': query1})
        # 检查响应是否成功
        if response.status_code == 200:
            data = response.json()
            return jsonify(data), 200  # 返回 JSON 响应
        else:
            return jsonify({"error": "Failed to fetch jobs from Dagster"}), 500
    except Exception as e:
        return jsonify({"error:": str(e)})


@repository_router.get("/repository/list")
async def list_repositories():
    """分页查询所有仓库，并附带部分 Job 信息"""
    try:
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 10))
        job_page_size = int(request.args.get("job_page_size", 10))
    except ValueError:
        return Fail(code=400, message="分页参数格式错误，必须为整数")
    try:
        result = await RepositoryController.list_repositories(page, page_size)
        repositories = result["repositories"]
        total = result["total"]
    except Exception as e:
        return Fail(code=500, message=f"获取仓库列表失败：{str(e)}")
    enriched_repos = []
    for repo in repositories:
        try:
            job_result = await JobController.list_jobs(repo_id=repo.id, page=1, page_size=job_page_size)
        except Exception:
            job_result = {"jobs": [], "total": 0}
        enriched_repos.append({
            "id": repo.id,
            "repo_name": repo.repo_name,
            "repo_location": repo.repo_location,
            "created_at": str(repo.created_at),
            "updated_at": str(repo.updated_at),
            "jobs": job_result.get("jobs", []),
            "job_total": job_result.get("total", 0)
        })
    return Success(data={
        "total": total,
        "page": page,
        "page_size": page_size,
        "repos": enriched_repos
    })


@repository_router.post("/repository/create")
async def create_repository():
    """创建仓库"""
    try:
        data = await request.json
        repo_name = data.get("repo_name")
        repo_location = data.get("repo_location")
    except Exception:
        return Fail(code=400, message="请求参数格式错误")
    if not repo_name or not repo_location:
        return Fail(code=401, message="缺少必要参数（repo_name 或 repo_location）")
    try:
        result = await RepositoryController.create_repository(repo_name, repo_location)
    except Exception as e:
        return Fail(code=500, message=f"创建仓库失败：{str(e)}")
    return Success(message="仓库创建成功", data=result)


@repository_router.put("/repository/update")
async def update_repository():
    """更新仓库"""
    try:
        data = await request.json
        repo_id = data.get("repo_id")
        repo_name = data.get("repo_name")
        repo_location = data.get("repo_location")
    except Exception:
        return Fail(code=400, message="请求参数格式错误")

    if not repo_id or not repo_name or not repo_location:
        return Fail(code=401, message="缺少必要参数（repo_id、repo_name 或 repo_location）")
    repo = await RepositoryController.get_by_id(repo_id)
    if not repo:
        return Fail(code=404, message="指定的仓库不存在")
    name_conflict = await RepositoryController.get_by_name(repo_name)
    if name_conflict and name_conflict.id != repo_id:
        return Fail(code=409, message="仓库名称已存在")
    try:
        result = await RepositoryController.update_repository(repo_id, repo_name, repo_location)
    except Exception as e:
        return Fail(code=500, message=f"仓库更新失败：{str(e)}")
    return Success(message="仓库更新成功", data=result)


@repository_router.delete("/repository/delete")
async def delete_repository():
    """删除仓库"""
    repo_id = request.args.get("repo_id")
    if not repo_id:
        return Fail(code=400, message="缺少 repo_id 参数")
    try:
        repo_id = int(repo_id)
    except ValueError:
        return Fail(code=401, message="repo_id 必须为整数")
    repo = await RepositoryController.get_by_id(repo_id)
    if not repo:
        return Fail(code=404, message="指定的仓库不存在")
    try:
        result = await RepositoryController.delete_repository(repo_id)
    except Exception as e:
        return Fail(code=500, message=f"删除仓库失败：{str(e)}")
    return Success(message="仓库删除成功", data=result)



