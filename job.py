import json
import aiomysql
from app.settings import get_pg_connection
from app.model.job import Job
from app.model.repository import Repository


async def set_job_config(job_name, job_graph):
    """保存 job 配置到 PostgreSQL"""
    conn = await get_pg_connection()
    if conn is None:
        print("数据库连接失败，无法插入数据")
        return

    try:
        # 如果传进来的是 dict/list，转成 JSON 字符串再存
        if isinstance(job_graph, (dict, list)):
            job_graph_to_save = json.dumps(job_graph, ensure_ascii=False)
        else:
            job_graph_to_save = job_graph

        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO lowcode.job (job_name, job_graph)
                VALUES ($1, $2)
                """,
                job_name,
                job_graph_to_save,
            )

        print("JSON 数据成功插入到 job 表")

    except Exception as e:
        print(f"插入 JSON 失败: {e}")

    finally:
        await conn.close()


async def get_job_config(job_name):
    """根据 job_name 获取 job_graph JSON 数据"""
    conn = await get_pg_connection()
    if conn is None:
        print("数据库连接失败，无法查询数据")
        return None

    try:
        row = await conn.fetchrow(
            """
            SELECT job_name, job_graph
            FROM lowcode.job
            WHERE job_name = $1
            LIMIT 1
            """,
            job_name,
        )

        if not row:
            return None

        raw_job_graph = row["job_graph"]

        if isinstance(raw_job_graph, (dict, list)):
            parsed_job_graph = raw_job_graph
        elif isinstance(raw_job_graph, str):
            parsed_job_graph = json.loads(raw_job_graph)
        else:
            parsed_job_graph = raw_job_graph

        return row["job_name"], parsed_job_graph

    except Exception as e:
        print(f"查询 job 配置失败: {e}")
        return None

    finally:
        await conn.close()

class JobController:
    @staticmethod
    async def create_job(repo_id: int, job_name: str, job_graph: dict):
        """ 创建任务流（本地模式，无用户维度） """
        repo = await Repository.filter(id=repo_id).first()
        if not repo:
            return {"error": "仓库不存在"}

        existing_job = await Job.filter(job_name=job_name, repo_id=repo_id).first()
        if existing_job:
            return {"error": "任务流已存在"}

        new_job = await Job.create(job_name=job_name, job_graph=job_graph, repo_id=repo_id)
        return {"message": "任务流创建成功", "job_id": new_job.id}

    @staticmethod
    async def get_job(job_id: int):
        """ 获取单个任务流（本地模式） """
        job = await Job.filter(id=job_id).first()
        if not job:
            return {"error": "任务流不存在"}
        return job

    @staticmethod
    async def list_jobs(repo_id: int, page: int = 1, page_size: int = 10):
        """分页查询任务流（本地模式）"""
        repo = await Repository.filter(id=repo_id).first()
        if not repo:
            return {"error": "仓库不存在"}

        total = await Job.filter(repo_id=repo_id).count()
        jobs = await Job.filter(repo_id=repo_id).offset((page - 1) * page_size).limit(page_size).all()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "jobs": jobs
        }

    @staticmethod
    async def update_job(job_id: int, job_graph: dict):
        """ 更新任务流（本地模式） """
        job = await Job.filter(id=job_id).first()
        if not job:
            return {"error": "任务流不存在"}

        job.job_graph = job_graph
        await job.save()
        return {"message": "任务流更新成功"}

    @staticmethod
    async def delete_job(job_id: int):
        """ 删除任务流（本地模式） """
        job = await Job.filter(id=job_id).first()
        if not job:
            return {"error": "任务流不存在"}

        await job.delete()
        return {"message": "任务流删除成功"}


job_controller = JobController()

