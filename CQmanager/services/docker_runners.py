from CQmanager.core.config import config
from CQmanager.core.logging import docker_log_config, logger
from CQmanager.docker_classes.CQviewersRunner import CQviewersRunner
from CQmanager.docker_classes.DockerRunner import DockerRunner

docker_runner = DockerRunner(config=config, logger=logger)
cq_viewers_runner = CQviewersRunner(
    config=config,
    logger=logger,
    docker_log_config=docker_log_config,
    run_CQviewers_on_remote_server=config.run_CQviewers_on_remote_server,
)
