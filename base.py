from datetime import timedelta, timezone
from quart import Blueprint, request, jsonify
from app.controller.user import user_controller
from app.core.ctx import CTX_USER_ID
from app.model.admin import User
from app.schema.base import Fail, Success
from app.schema.login import *
from app.schema.user import UpdatePassword, UserCreate, UserUpdate
from app.settings import setting
from app.utils.tools.jwt import create_access_token
from app.utils.tools.password import get_password_hash, verify_password
from app.schema import CredentialsSchema, JWTOut, JWTPayload

base_router = Blueprint('base_router', __name__)

# @base_router.before_request
# async def before_request():
#     if request.path in ["/user/login", "/user/register"]:  # 这里排除登录,注册接口
#         return
#     await AuthControl.is_authed()


@base_router.post("/user/register")
async def register_user():
    """用户注册"""
    data = await request.json
    user_in = UserCreate(**data)
    existing_user = await user_controller.get_by_email(user_in.email)
    if existing_user:
        return Fail(code=400, message="邮箱已注册，请直接登录")
    existing_username = await user_controller.get_by_username(user_in.username)

    if existing_username:
        return Fail(code=401, message="用户名已被占用，请选择其他用户名")

    new_user = await user_controller.create_user(
        obj_in=UserCreate(
            username=user_in.username,
            email=user_in.email,
            password=user_in.password
        )
    )
    return Success(message="注册成功")


@base_router.post("/user/login")
async def login_user():
    """用户登录"""
    data = await request.json
    try:
        credentials = CredentialsSchema(**data)
    except Exception:
        return Fail(code=400, message="请求参数格式错误")
    user: User = await user_controller.authenticate(credentials)
    if not user:
        return Fail(code=401, message="用户名或密码错误")
    if not user.is_active:
        return Fail(code=403, message="用户已被禁用")
    await user_controller.update_last_login(user.id)
    access_token_expires = timedelta(minutes=setting.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = datetime.now(timezone.utc) + access_token_expires
    data = JWTOut(
        access_token=create_access_token(
            data=JWTPayload(
                user_id=user.id,
                username=user.username,
                is_superuser=user.is_superuser,
                exp=expire,
            )
        ),
        username=user.username,
    )
    return Success(message="登录成功", data=data.model_dump())


@base_router.get("/user/info")
async def get_userinfo():
    """查看当前登录用户信息"""
    user_id = CTX_USER_ID.get()
    if not user_id:
        return Fail(code=401, message="用户未登录")
    user_obj = await user_controller.get(id=user_id)
    if not user_obj:
        return Fail(code=404, message="用户不存在")
    data = await user_obj.to_dict(exclude_fields=["password"])
    return Success(data=data)


@base_router.post("/user/update")
async def update_user():
    """更新当前登录用户信息"""
    user_id = CTX_USER_ID.get()
    if not user_id:
        return Fail(code=401, message="用户未登录")
    user_obj = await user_controller.get(id=user_id)
    if not user_obj:
        return Fail(code=404, message="用户不存在")
    try:
        data = await request.json
        user_update = UserUpdate(**data)
    except Exception:
        return Fail(code=400, message="参数格式错误")
    await user_controller.update(user_id, user_update)
    return Success(message="用户信息更新成功")


@base_router.post("/user/update_password")
async def update_user_password():
    """更新当前登录用户密码"""
    user_id = CTX_USER_ID.get()
    if not user_id:
        return Fail(code=401, message="用户未登录")
    try:
        data = await request.json
        req_in = UpdatePassword(**data)
    except Exception:
        return Fail(code=400, message="请求参数格式错误")
    user = await user_controller.get(user_id)
    if not user:
        return Fail(code=404, message="用户不存在")
    if not verify_password(req_in.old_password, user.password):
        return Fail(code=403, message="旧密码验证错误")
    user.password = get_password_hash(req_in.new_password)
    await user.save()
    return Success(message="密码修改成功")




