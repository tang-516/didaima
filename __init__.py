from quart import Blueprint
from .run import run_router
from .job import job_router
from .repository import repository_router
from .base import base_router
from .user import user_router
from .component import component_router
from .file import file_router


run_blueprint = Blueprint("run_blueprint", __name__)
job_blueprint = Blueprint("job_blueprint", __name__)
repository_blueprint = Blueprint("repository_blueprint", __name__)
base_blueprint = Blueprint("base_blueprint", __name__)
user_blueprint = Blueprint("user_blueprint", __name__)
component_blueprint = Blueprint("component_blueprint", __name__)
file_blueprint = Blueprint("file_blueprint", __name__)

job_blueprint.register_blueprint(job_router)
run_blueprint.register_blueprint(run_router)
repository_blueprint.register_blueprint(repository_router)
base_blueprint.register_blueprint(base_router)
user_blueprint.register_blueprint(user_router)
component_blueprint.register_blueprint(component_router)
file_blueprint.register_blueprint(file_router)


__all__ = [
    'job_blueprint', 'repository_blueprint', 'run_blueprint',
    'base_blueprint', 'user_blueprint', 'component_blueprint',
    'file_blueprint', 'job_router', 'repository_router', 'run_router',
    'base_router', 'user_router', 'component_router', 'file_router']
