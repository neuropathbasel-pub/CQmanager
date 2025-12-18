from CnQuant_utilities.logger import AsyncLogger
from docker.types import LogConfig

from CQmanager.core.config import config

logger_instance = AsyncLogger(
    name=config.app_name,
    log_file=config.log_file_path,
    log_level=config.log_level,
    file_log_level=config.log_level,
)
logger = logger_instance.get_logger()


docker_log_config = LogConfig(
    type="json-file",
    config={
        "max-size": "10m",
        "max-file": "3",
        "compress": "false",
        "labels": "app",
        "tag": "image_name_{{.ImageName}}_container_name_{{.Name}}",
    },
)
