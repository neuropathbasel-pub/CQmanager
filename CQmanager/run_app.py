import gunicorn.app.base


class StandaloneApplication(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options: dict[str, str | int]):
        self.options = options
        self.application = app
        super().__init__()

    def load_config(self):
        for key, value in self.options.items():
            self.cfg.set(name=key, value=value)  # type: ignore

    def load(self):
        return self.application


def main():
    from CQmanager.core.config import config

    from .app import app  # Import your FastAPI app

    options = {
        "bind": f"{config.CQmanager_gunicorn_host_address}:{config.CQmanager_gunicorn_port}",
        "workers": 1,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "timeout": 120,
    }
    StandaloneApplication(app=app, options=options).run()


if __name__ == "__main__":
    main()
