import logging
import os
import traceback
from typing import TYPE_CHECKING, Optional

from CnQuant_utilities.console_output import print_in_color
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container
from numpy import int16

from CQmanager.core.config import config
from CQmanager.core.logging import logger
from CQmanager.docker_classes.docker_functions import (
    get_docker_client,
    pull_docker_images_if_not_available_locally,
)
from CQmanager.docker_classes.docker_settings import (
    cnviewers_images_and_commands,
    cqviewers_local_environment_variables,
    cqviewers_local_volumes,
    cqviewers_remote_environment_variables,
    cqviewers_remote_volumes,
)

if TYPE_CHECKING:
    from CQmanager.core.config import AppConfig
    from CQmanager.core.logging import LogConfig


class CQviewersRunner:
    """
    Manages the lifecycle of CQviewers containers (CQcase, CQall, and cnquant_redis) on local or remote Docker hosts.

    This class handles initialization, starting, stopping, and monitoring of Docker containers for CQviewers applications.
    It supports both local and remote Docker environments via SSH. On startup, it can automatically initiate containers
    if configured. It also manages Docker networks for container communication.

    Attributes:
        run_CQviewers_on_remote_server (bool): Whether to run containers on a remote server.
        CQviewers_host (Optional[str]): Hostname or IP for remote Docker server.
        CQviewers_user (Optional[str]): SSH username for remote connections.
        CQall_container_name (str): Name of the CQall container.
        CQcase_container_name (str): Name of the CQcase container.
        cnquant_redis_name (str): Name of the cnquant Redis container.
        CQviewers_docker_network_name (str): Name of the Docker network for CQviewers.
        initiate_cqcase_and_cqall_on_startup (bool): Whether to start containers automatically on initialization.
        LOCAL_USER_ID (str): Local user ID for container user mapping.
        LOCAL_GROUP_ID (str): Local group ID for container user mapping.
        REMOTE_USER_ID (str): Remote user ID for container user mapping.
        REMOTE_GROUP_ID (str): Remote group ID for container user mapping.
        cqviewers_local_volumes (dict): Volume mappings for local containers.
        cqviewers_remote_volumes (dict): Volume mappings for remote containers.
        cqviewers_environment_variables (dict): Environment variables for containers.
        cnviewers_images_and_commands (dict): Mapping of container names to their images, commands, and ports.
        cqviewers_names (list[str]): List of CQviewers container names.

    Methods:
        check_if_cqcase_and_cqall_are_running(): Checks if CQviewers containers are running.
        start_cqcase_and_cqall(): Starts CQviewers containers if not already running.
        stop_cqviewers_containers(): Stops running CQviewers containers.
        remove_non_running_containers(): Removes all non-running Docker containers.

    Raises:
        Exception: If Docker client initialization fails or other critical errors occur during operations.

    Note:
        - Requires proper Docker and SSH configuration for remote operations.
        - Automatically pulls required images if not available locally.
        - Manages a custom Docker bridge network for container connectivity.
    """

    def __init__(
        self,
        config: "AppConfig",
        docker_log_config: "LogConfig",
        logger: logging.Logger = logging.getLogger(name=__name__),
        run_CQviewers_on_remote_server: bool = False,
        CQviewers_host: Optional[str] = config.CQviewers_host,
        CQviewers_user: Optional[str] = config.CQviewers_user,
        CQall_container_name: str = config.CQall_container_name,
        CQcase_container_name: str = config.CQcase_container_name,
        cnquant_redis_name: str = config.cnquant_redis_name,
        CQviewers_docker_network_name: str = config.CQviewers_docker_network_name,
        initiate_cqcase_and_cqall_on_startup: bool = (
            config.initiate_cqcase_and_cqall_on_startup
        ),
        cqviewers_local_environment_variables: dict[
            str, str
        ] = cqviewers_local_environment_variables,
        cqviewers_remote_environment_variables: dict[
            str, str
        ] = cqviewers_remote_environment_variables,
        cnviewers_images_and_commands: dict[
            str, dict[str, str | dict[str, int]]
        ] = cnviewers_images_and_commands,
        cqviewers_local_volumes: dict[str, dict[str, str]] = cqviewers_local_volumes,
        cqviewers_remote_volumes: dict[str, dict[str, str]] = cqviewers_remote_volumes,
    ):
        self.logger = logger
        self.docker_log_config = docker_log_config
        self.run_CQviewers_on_remote_server: bool = run_CQviewers_on_remote_server
        self.CQviewers_host: Optional[str] = CQviewers_host
        self.CQviewers_user: Optional[str] = CQviewers_user
        self.CQall_container_name: str = CQall_container_name
        self.CQcase_container_name: str = CQcase_container_name
        self.cnquant_redis_name: str = cnquant_redis_name
        self.CQviewers_docker_network_name: str = CQviewers_docker_network_name
        self.initiate_cqcase_and_cqall_on_startup: bool = (
            initiate_cqcase_and_cqall_on_startup
        )
        self.LOCAL_USER_ID = config.LOCAL_USER_ID
        self.LOCAL_GROUP_ID = config.LOCAL_GROUP_ID
        self.REMOTE_USER_ID = config.REMOTE_USER_ID
        self.REMOTE_GROUP_ID = config.REMOTE_GROUP_ID
        self.cqviewers_local_volumes: dict[str, dict[str, str]] = (
            cqviewers_local_volumes
        )
        self.cqviewers_remote_volumes: dict[str, dict[str, str]] = (
            cqviewers_remote_volumes
        )
        self.cnviewers_images_and_commands: dict[
            str, dict[str, str | dict[str, int]]
        ] = cnviewers_images_and_commands

        if self.run_CQviewers_on_remote_server:
            self.cqviewers_environment_variables = (
                cqviewers_remote_environment_variables
            )
        else:
            self.cqviewers_environment_variables = cqviewers_local_environment_variables

        def check_if_docker_images_are_downloaded(self) -> None:
            CQcase_image_dict = self.cnviewers_images_and_commands.get(
                config.CQcase_container_name, None
            )
            CQall_image_dict = self.cnviewers_images_and_commands.get(
                config.CQall_container_name, None
            )

            try:
                client = get_docker_client(
                    user=self.CQviewers_user,
                    host=self.CQviewers_host,
                    remote_client=self.run_CQviewers_on_remote_server,
                )

            except ImageNotFound:
                client = None

            # Try to download images
            if CQcase_image_dict is not None and client is not None:
                CQcase_image_name = CQcase_image_dict.get("image", None)
                try:
                    pull_docker_images_if_not_available_locally(
                        client=client, image_name=CQcase_image_name
                    )
                except Exception:
                    error = traceback.format_exc()
                    logger.error(
                        msg=f"Unable to download {CQcase_image_name}.\n:{error}"
                    )

            if CQall_image_dict is not None and client is not None:
                CQall_image_name = CQall_image_dict.get("image", None)
                try:
                    pull_docker_images_if_not_available_locally(
                        client=client, image_name=CQall_image_name
                    )
                except Exception:
                    error = traceback.format_exc()
                    logger.error(
                        msg=f"Unable to download {CQall_image_name}.\n:{error}"
                    )
            return None

        check_if_docker_images_are_downloaded(self)

        self.cqviewers_names: list[str] = [
            config.cnquant_redis_name,
            config.CQall_container_name,
            config.CQcase_container_name,
        ]

        if self.initiate_cqcase_and_cqall_on_startup:
            running_containers, status_code = (
                self.check_if_cqcase_and_cqall_are_running()
            )
            if status_code == 200:
                self.start_cqcase_and_cqall()

            else:
                if self.run_CQviewers_on_remote_server:
                    containers_host = config.CQviewers_host

                else:
                    containers_host = os.uname().nodename

                print_in_color(
                    message=f"CQall and CQcase have already been running on {containers_host}",
                    color="green",
                )

    def check_if_cqcase_and_cqall_are_running(self) -> tuple[str, int]:
        """Check if CQcase and CQall containers are running and return their names and status code.

        Returns:
            tuple[str, int]: Comma-separated list of running container names and HTTP status code (200 for success, 500 for error).

        Raises:
            Exception: If Docker client initialization fails.
            DockerException: If container listing fails, logs error and returns error message with status 500.
        """
        # if not self.docker_host_details_available:
        #     return "Docker user and host have not been set properly", 500
        # else:
        try:
            client = get_docker_client(
                user=self.CQviewers_user,
                host=self.CQviewers_host,
                remote_client=self.run_CQviewers_on_remote_server,
            )
        except Exception:
            error = traceback.format_exc()
            logger.error(msg=error)
            raise Exception(error)

        try:
            containers = [
                container.name
                for container in client.containers.list(filters={"status": "running"})
                if container.name in self.cqviewers_names
            ]

            return ",".join([container for container in containers]), 200

        except (DockerException, Exception):
            error_string = traceback.format_exc()
            logger.error(msg=error_string)
            return error_string, 500
        finally:
            client.close()

    def start_cqcase_and_cqall(
        self,
        detach_containers: bool = config.detach_containers,
        autoremove_containers: bool = config.autoremove_containers,
    ) -> list[str]:
        """Start CQcase, CQall, and cnquant_redis containers if not already running.

        Returns:
            list[str]: Names of successfully started containers.

        Raises:
            Exception: If Docker client initialization fails.
            APIError: If network creation or container start fails, logs error.
            NotFound: If Docker network does not exist, creates a new bridge network.
        """
        # if not self.docker_host_details_available:
        #     return []

        containers_to_start: list[str] = [
            config.cnquant_redis_name,
            config.CQall_container_name,
            config.CQcase_container_name,
        ]
        started_containers: list[str] = []
        # Create network
        try:
            client = get_docker_client(
                user=self.CQviewers_user,
                host=self.CQviewers_host,
                remote_client=self.run_CQviewers_on_remote_server,
            )
        except Exception:
            error = traceback.format_exc()
            logger.error(msg=error)
            raise Exception(error)
        try:
            # Check if network exists
            client.networks.get(network_id=self.CQviewers_docker_network_name)
        except NotFound:
            # Create a bridge network with a custom subnet
            client.networks.create(
                name=self.CQviewers_docker_network_name,
                driver="bridge",
            )
        except APIError:
            error_string = traceback.format_exc()
            message = f"Failed to create docker network for CQcase and CQall {self.CQviewers_docker_network_name}: {error_string}"
            logger.error(msg=message)

        for container_name in containers_to_start:
            existing_containers = client.containers.list(all=True)
            existing_names = [container.name for container in existing_containers]
            if container_name not in existing_names:
                container_settings = self.cnviewers_images_and_commands.get(
                    container_name, None
                )
                if container_settings is not None:
                    docker_image = container_settings.get("image", None)
                    execution_command = container_settings.get(
                        "execution_command", None
                    )
                    ports: dict[str, int] | dict = container_settings.get(
                        "ports", None
                    )  ## type: ignore
                else:
                    continue
                try:
                    if self.run_CQviewers_on_remote_server:
                        logger.info(msg="Starting CQ viewers on remote server")
                        user_id = self.REMOTE_USER_ID
                        group_id = self.REMOTE_GROUP_ID
                        volumes = self.cqviewers_remote_volumes
                    else:
                        logger.info(msg="Starting CQ viewers locally")
                        user_id = self.LOCAL_USER_ID
                        group_id = self.LOCAL_GROUP_ID
                        volumes = self.cqviewers_local_volumes

                    new_container: Container = client.containers.run(
                        image=str(docker_image),
                        command=str(execution_command),
                        network=self.CQviewers_docker_network_name,
                        name=container_name,
                        volumes=volumes,
                        detach=detach_containers,
                        auto_remove=autoremove_containers,
                        userns_mode="host",
                        environment=self.cqviewers_environment_variables,
                        ports=ports,
                        user=f"{user_id}:{group_id}",
                    )  ## type: ignore
                    if new_container.name is not None:
                        started_containers += [new_container.name]
                    logger.info(msg=f"Started {container_name}")
                except APIError:
                    error = traceback.format_exc()
                    message = f"Failed to run container {container_name}: {error}"
                    logger.error(msg=message)
                except Exception:
                    error = traceback.format_exc()
                    message = f"Failed to run container {container_name}: {error}"
                    logger.error(msg=message)
            else:
                # Container exists (possibly stopped), remove it to free the name
                try:
                    existing_container = client.containers.get(
                        container_id=container_name
                    )
                    if existing_container.status != "running":
                        existing_container.remove()
                        logger.info(msg=f"Removed stopped container: {container_name}")
                    else:
                        # If running, skip starting (already handled by the original check)
                        continue
                except APIError:
                    error = traceback.format_exc()
                    logger.error(
                        msg=f"Failed to remove existing container {container_name}: {error}"
                    )
                    continue

        client.close()

        return started_containers

    def stop_cqviewers_containers(self) -> tuple[list[str], int]:
        """Stop running CQviewers containers and return their names with status code.

        Returns:
            tuple[list[str], int]: List of stopped container names and HTTP status code (200).

        Raises:
            Exception: If Docker client initialization fails.
            APIError: If stopping a container fails, logs error.
        """
        try:
            client = get_docker_client(
                user=self.CQviewers_user,
                host=self.CQviewers_host,
                remote_client=self.run_CQviewers_on_remote_server,
            )
        except Exception:
            error = traceback.format_exc()
            logger.error(msg=error)
            raise Exception(error)

        running_cqviewers_containers = [
            container
            for container in client.containers.list()
            if container.name in self.cqviewers_names
        ]

        stopped_containers: list[str] = []

        for container in running_cqviewers_containers:
            try:
                container.stop(timeout=10)
                stopped_containers += [str(container.name)]
                logger.info(msg=f"Stopped container: {container.id} ({container.name})")
            except APIError:
                error = traceback.format_exc()
                logger.info(msg=f"Error stopping container {container.id}: {error}")

        client.close()
        return stopped_containers, 200

    def remove_non_running_containers(self) -> tuple[bool, int]:
        """
        Remove all non-running Docker containers using the Docker SDK.
        """
        container_cleanup_successful: bool = False
        removed_count: int = 0
        try:
            client = get_docker_client(
                user=self.CQviewers_user,
                host=self.CQviewers_host,
                remote_client=self.run_CQviewers_on_remote_server,
            )
        except Exception:
            error = traceback.format_exc()
            logger.critical(msg=error)

            # The -1 notifies that there is an error connecting to Docker
            return container_cleanup_successful, -1

        try:
            # List all containers (including stopped ones)
            all_containers = client.containers.list(all=True)
            non_running_containers = [
                c for c in all_containers if c.status != "running"
            ]

            for container in non_running_containers:
                try:
                    logger.info(
                        msg=f"Removing container {container.name} (ID: {container.id}, Status: {container.status})"
                    )
                    container.remove()
                    removed_count += 1
                except (APIError, DockerException, Exception):
                    error = traceback.format_exc()
                    logger.info(
                        msg=f"Failed to remove container {container.name} (ID: {container.id}): {error}"
                    )
            else:  # Runs if loop completes (no exceptions in removals)
                container_cleanup_successful = True

        except (DockerException, Exception):
            error = traceback.format_exc()
            logger.error(msg=f"Error connecting to Docker: {error}")
        finally:
            client.close()

        return container_cleanup_successful, removed_count

    def __str__(self):
        return "CQviewersRunner()"

    def __repr__(self):
        return "CQviewersRunner()"
