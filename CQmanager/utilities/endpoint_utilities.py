from typing import Optional

import paramiko
from fastapi import Request


def detect_cli_client(
    req: Request,
    specified_format: Optional[str] = None,
) -> bool:
    """Detect if the request comes from a CLI tool based on User-Agent header.

    Args:
        req: FastAPI Request object
        specified_format: Optional format specifier, e.g., "text"

    Returns:
        bool: True if request appears to come from a CLI tool
    """
    if specified_format == "text":
        return True
    elif specified_format == "json":
        return False
    else:
        user_agent = req.headers.get("user-agent", default="").lower()
        return any(
            tool in user_agent for tool in ["curl", "wget", "httpie", "python-requests"]
        )


def scp_file(
    host: str,
    username: str,
    local_path: str,
    remote_path: str,
) -> None:
    """Upload a local file to a remote server using SFTP (SCP-like functionality).

    Establishes an SSH connection to the specified host and uploads the file
    from the local path to the remote path using paramiko's SFTP client.

    Args:
        host (str): The hostname or IP address of the remote server.
        username (str): The SSH username for authentication.
        local_path (str): Path to the local file to upload.
        remote_path (str): Destination path on the remote server.

    Returns:
        None

    Raises:
        paramiko.AuthenticationException: If SSH authentication fails.
        paramiko.SSHException: For SSH connection or SFTP errors.
        FileNotFoundError: If the local file does not exist.
        OSError: For file system or network-related errors.

    Note:
        This function automatically accepts unknown host keys for convenience
        and will need to be improved for security if the software will be working in an exposed network.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(policy=paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=username)

    with ssh.open_sftp() as sftp:
        sftp.put(localpath=local_path, remotepath=remote_path)

    ssh.close()
