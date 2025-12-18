import re
import time
import traceback
from socket import gaierror, gethostbyname

import docker
from CnQuant_utilities.console_output import print_in_color
from docker import DockerClient
from docker.errors import ImageNotFound
from paramiko import SSHClient, SSHException

from CQmanager.core.logging import logger


def validate_host_and_user(
    host: str, user: str, retries: int = 5, delay: int = 1
) -> None:
    """Validate Docker host and user for SSH-based DockerClient connection.

    Args:
        host (str): The hostname or IP address of the Docker host.
        user (str): The SSH username for the connection.
        retries (int): Number of retry attempts for SSH connection (default: 5).
        delay (int): Delay between retries in seconds (default: 1).

    Raises:
        ValueError: If the host is unresolvable or the username contains invalid characters.
        ConnectionError: If the SSH connection fails after all retries.

    """
    # Validate host
    try:
        gethostbyname(host)
    except gaierror:
        error = traceback.format_exc()
        raise gaierror(f"Invalid host: {host}. Error message:\n{error}")
    except Exception:
        error = traceback.format_exc()
        logger.error(msg=error)

    # Validate user
    if not re.match(pattern=r"^[a-zA-Z0-9_-]+$", string=user):
        raise ValueError(f"Invalid username: {user}")

    # Retry SSH connection
    start_time = time.time()
    attempt = 1
    while attempt <= retries:
        try:
            ssh = SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(hostname=host, username=user, timeout=5)
            ssh.close()
            return
        except SSHException as e:
            if time.time() - start_time >= retries * delay:
                raise ConnectionError(
                    f"SSH connection failed after {retries} attempts: {str(e)}"
                )
            time.sleep(delay)
            attempt += 1
        except Exception:
            error = traceback.format_exc()
            logger.error(msg=error)
            raise Exception(error)


def get_docker_client(
    user: str | None, host: str | None, remote_client: bool
) -> docker.client.DockerClient:
    """
    Create and return a Docker client, either by connecting to a remote host via SSH or by using the local Docker environment.

    Args:
        user (str | None): The username for SSH connection if `remote_client` is True. Ignored if `remote_client` is False.
                           Must be a string if `remote_client` is True; passing None may result in an error.
        host (str | None): The hostname or IP address of the remote Docker host if `remote_client` is True.
                           Ignored if `remote_client` is False. Must be a string if `remote_client` is True;
                           passing None may result in an error.
        remote_client (bool): If True, connect to a remote Docker host using SSH. If False, use the local Docker environment.

    Returns:
        docker.client.DockerClient: A Docker client object connected to the specified host or the local environment.

    Raises:
        Exception: If `remote_client` is True and an error occurs while creating the client, such as:
            - "Failed to resolve host: {host}" if the host cannot be resolved.
            - "Authentication failed for user: {user} on host: {host}" if authentication fails.
            - "SSH connection failed to host: {host}" if the SSH connection fails.
            - "Failed to create Docker client: {error}" for other general failures.
        docker.errors.DockerException: If `remote_client` is False and an error occurs while creating the client from the environment.

    Note:
        - The function does not validate `user` or `host`; invalid values may lead to runtime exceptions.
    """
    if (
        remote_client is not None
        and remote_client
        and user is not None
        and host is not None
        and isinstance(user, str)
        and isinstance(host, str)
    ):
        try:
            validate_host_and_user(host=host, user=user)
            client = docker.DockerClient(base_url=f"ssh://{user}@{host}")
            return client

        except Exception:
            error: str = traceback.format_exc()
            logger.error(msg=error)
            raise Exception(error)
    else:
        client = docker.from_env()
        return client


def pull_docker_images_if_not_available_locally(
    client: DockerClient, image_name: str
) -> None:
    """Check if a Docker image exists locally and pull it if not available.

    Args:
        client (DockerClient): Docker client instance for interacting with Docker.
        image_name (str): Docker image name, optionally with tag (e.g., 'image:tag').

    Raises:
        Logs an error if checking or pulling the image fails.

    Returns:
        None
    """
    try:
        client.images.get(name=image_name)

    except ImageNotFound:
        repository, tag = (
            image_name.split(":") if ":" in image_name else (image_name, "latest")
        )
        client.images.pull(repository=repository, tag=tag)
        print_in_color(
            color="yellow",
            message=f"{image_name} has not been found locally and therefore has been downloaded.",
        )
    except Exception:
        error = traceback.format_exc()
        logger.error(
            msg=f"Error while trying to check or download docker images.\n:{error}"
        )
    return None
