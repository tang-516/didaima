# from flask_pydantic import ValidationError
from quart import Blueprint, request, abort
from quart import jsonify
from tortoise.expressions import Q
from app.controller.user import user_controller
from app.core import AuthControl, CTX_USER_ID
from app.schema.base import Fail, Success, SuccessExtra
from app.schema import UserCreate, UserUpdate

user_router = Blueprint('user_router', __name__)


# @user_router.before_request
# async def before_request():
#     user = await AuthControl.is_authed()
#     if not user.is_superuser:
#         abort(403, "必须管理员权限")


@user_router.get("/list")
async def list_user():
    """查看用户信息列表"""
    try:
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 10))
    except ValueError:
        return Fail(code=400, message="分页参数格式错误")
    username = request.args.get("username", "").strip()
    email = request.args.get("email", "").strip()
    q = Q()
    if username:
        q &= Q(username__contains=username)
    if email:
        q &= Q(email__contains=email)
    total, user_objs = await user_controller.list(page=page, page_size=page_size, search=q)
    data = [await obj.to_dict(m2m=True, exclude_fields=["password"]) for obj in user_objs]
    return SuccessExtra(data=data, total=total, page=page, page_size=page_size)


@user_router.get("/get")
async def get_user():
    """根据用户名查看信息"""
    username = request.args.get("username")
    user_obj = await user_controller.get_by_username(username)
    user_dict = await user_obj.to_dict(exclude_fields=["password"])
    return Success(data=user_dict)


@user_router.post("/create")
async def create_user():
    """创建用户，管理员权限"""
    try:
        data = await request.json
        user_in = UserCreate(**data)
    except Exception:
        return Fail(code=400, message="请求参数格式错误")
    user1 = await user_controller.get_by_email(user_in.email)
    user2 = await user_controller.get_by_username(user_in.username)
    if user1 or user2:
        return Fail(code=409, message="该邮箱或用户名已存在")
    new_user = await user_controller.create_user(obj_in=user_in)
    return Success(message="用户创建成功")


@user_router.post("/update")
async def update_user():
    """根据id更新用户，管理员权限"""
    try:
        data = await request.json
        user_in = UserUpdate(**data)
    except Exception:
        return Fail(code=400, message="请求参数格式错误")
    if not getattr(user_in, "id", None):
        return Fail(code=401, message="缺少用户ID")
    user = await user_controller.update(id=user_in.id, obj_in=user_in)
    if not user:
        return Fail(code=404, message="用户不存在")
    return Success(message="用户信息更新成功")


@user_router.delete("/delete")
async def delete_user():
    """根据用户名删除用户，管理员权限"""
    username = request.args.get("username")
    if not username:
        return Fail(code=400, message="缺少用户名参数")
    user_obj = await user_controller.get_by_username(username)
    if not user_obj:
        return Fail(code=404, message="用户不存在")
    await user_obj.delete()
    return Success(message="用户删除成功")


@user_router.post("/reset_password")
async def reset_password():
    """根据id重置密码，管理员权限"""
    try:
        data = await request.json
        user_id = data.get("user_id")
    except Exception:
        return Fail(code=400, message="请求参数格式错误")
    if not user_id:
        return Fail(code=401, message="缺少用户ID")
    user = await user_controller.get(id=user_id)
    if not user:
        return Fail(code=404, message="用户不存在")
    await user_controller.reset_password(user_id)
    return Success(message="密码已重置为 123456")
