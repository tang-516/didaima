from datetime import datetime
from typing import Optional
from werkzeug.exceptions import abort
from app.core.crud import CRUDBase
from app.model.admin import User
from app.schema.login import CredentialsSchema
from app.schema.user import UserCreate, UserUpdate
from app.utils.tools.password import get_password_hash, verify_password


class UserController(CRUDBase[User, UserCreate, UserUpdate]):
    def __init__(self):
        super().__init__(model=User)

    async def get_by_email(self, email: str) -> Optional[User]:
        return await self.model.filter(email=email).first()

    async def get_by_username(self, username: str) -> Optional[User]:
        return await self.model.filter(username=username).first()

    async def create_user(self, obj_in: UserCreate) -> User:
        obj_in.password = get_password_hash(password=obj_in.password)
        obj = await self.create(obj_in)
        return obj

    async def update_last_login(self, user_id: int) -> None:
        user = await self.model.get(id=user_id)
        user.last_login = datetime.now()
        await user.save()

    async def authenticate(self, credentials: CredentialsSchema) -> Optional[User]:
        user = await self.model.filter(username=credentials.username).first()
        if not user:
            abort(400, description="无效的用户名")
        verified = verify_password(credentials.password, user.password)
        if not verified:
            abort(400, description="密码错误")
        if not user.is_active:
            abort(400, description="用户已被禁用")
        return user

    async def reset_password(self, user_id: int):
        user_obj = await self.get(id=user_id)
        if user_obj.is_superuser:
            abort(403, description="不允许重置超级管理员密码")
        user_obj.password = get_password_hash(password="123456")
        await user_obj.save()


user_controller = UserController()
