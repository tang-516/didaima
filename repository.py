from app.model.repository import Repository
from app.model.job import Job


class RepositoryController:
    @staticmethod
    async def list_repositories(page: int = 1, page_size: int = 10):
        """分页查询所有仓库"""
        total = await Repository.all().count()
        repositories = await Repository.all().offset((page - 1) * page_size).limit(page_size)
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "repositories": repositories
        }

    @staticmethod
    async def create_repository(repo_name: str, repo_location: str):
        """创建仓库，名称不能重复"""
        existing_repo = await Repository.filter(repo_name=repo_name).first()
        if existing_repo:
            return {"error": "仓库名称已存在"}

        new_repo = await Repository.create(repo_name=repo_name, repo_location=repo_location)
        return {"message": "仓库创建成功", "repo_id": new_repo.id}

    @staticmethod
    async def update_repository(repo_id: int, repo_name: str, repo_location: str):
        """更新仓库信息"""
        repo = await Repository.filter(id=repo_id).first()
        if not repo:
            return {"error": "仓库不存在"}

        repo.repo_name = repo_name
        repo.repo_location = repo_location
        await repo.save()
        return {"message": "仓库更新成功"}

    @staticmethod
    async def delete_repository(repo_id: int):
        """删除仓库，必须保证仓库下没有任务"""
        repo = await Repository.filter(id=repo_id).first()
        if not repo:
            return {"error": "仓库不存在"}

        job_count = await Job.filter(repo_id=repo_id).count()
        if job_count > 0:
            return {"error": "无法删除，该仓库下仍有任务"}

        await repo.delete()
        return {"message": "仓库删除成功"}

    @classmethod
    async def get_by_id(cls, repo_id):
        """通过 ID 获取仓库对象"""
        return await Repository.get_or_none(id=repo_id)

    @classmethod
    async def get_by_name(cls, repo_name):
        """通过名称获取仓库对象"""
        return await Repository.get_or_none(name=repo_name)


repository_controller = RepositoryController()
