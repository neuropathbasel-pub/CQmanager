import subprocess
import sys


def main():
    from CQmanager.core.config import config

    cmd = [
        sys.executable,
        "-m",
        "gunicorn",
        f"--workers={config.CQmanager_gunicorn_workers}",
        "-k",
        "uvicorn.workers.UvicornWorker",
        "CQmanager.app:app",
        "--bind",
        f"{config.CQmanager_gunicorn_host_address}:{config.CQmanager_gunicorn_port}",
        f"--timeout={config.CQmanager_gunicorn_timeout}",
    ]

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Gunicorn failed with exit code {e.returncode}")
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()
