"""Configuration loading from CLI args, environment, and config file."""

import os
import sys
import warnings
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Any, cast


class InsecureConfigWarning(UserWarning):
    """Warning for insecure configuration practices."""

    pass

# tomli is in stdlib as tomllib in Python 3.11+
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


ENV_USERNAME = "DATACITE_USERNAME"
ENV_PASSWORD = "DATACITE_PASSWORD"
ENV_REFRESH_INTERVAL = "DATACITE_REFRESH_INTERVAL"

DEFAULT_REFRESH_INTERVAL_MINUTES = 20

DEFAULT_CONFIG_PATHS = [
    Path.home() / ".datacite-data-file-dl.toml",
    Path.home() / ".config" / "datacite-data-file-dl" / "config.toml",
]


@dataclass
class Config:
    """Resolved configuration from all sources."""

    username: str | None
    password: str | None
    output_dir: str

    # Download options
    path: str | None = None
    download_all: bool = False
    list_only: bool = False
    dry_run: bool = False

    # Output options
    json_output: bool = False
    quiet: bool = False
    verbose: bool = False
    log_file: str | None = None

    # Reliability options
    retries: int = 3
    refresh_interval: int = DEFAULT_REFRESH_INTERVAL_MINUTES
    resume: bool = False
    fresh: bool = False
    skip_verify: bool = False

    # Filtering options
    include_patterns: list[str] | None = None
    exclude_patterns: list[str] | None = None
    since: str | None = None
    until: str | None = None
    max_size: str | None = None

    # Interaction
    yes: bool = False

    # Performance
    workers: int = 4

    # Advanced
    bucket: str | None = None


def _load_config_file(path: str | Path | None) -> dict[str, object]:
    if path is not None:
        paths = [Path(path)]
    else:
        paths = DEFAULT_CONFIG_PATHS

    for config_path in paths:
        if config_path.exists():
            with open(config_path, "rb") as f:
                return dict(tomllib.load(f))

    return {}


def load_config(
    cli_username: str | None = None,
    cli_password: str | None = None,
    cli_output_dir: str | None = None,
    config_file: str | None = None,
    prompt_for_missing: bool = False,
    **kwargs: object,
) -> Config:
    """Precedence: CLI > env > config file > prompt."""
    file_config = _load_config_file(config_file)

    # Resolve username: CLI > env > file
    username = cli_username
    if username is None:
        username = os.environ.get(ENV_USERNAME)
    if username is None:
        username = cast(str | None, file_config.get("username"))

    # Resolve password: CLI > env > file (though file storage discouraged)
    password = cli_password
    if password is None:
        password = os.environ.get(ENV_PASSWORD)
    if password is None:
        password = cast(str | None, file_config.get("password"))
        if password is not None:
            warnings.warn(
                "Password loaded from config file. Storing passwords in plaintext "
                "files is insecure. Consider using DATACITE_PASSWORD environment "
                "variable instead.",
                InsecureConfigWarning,
                stacklevel=2,
            )

    if password is None and prompt_for_missing and username is not None:
        password = getpass(f"Password for {username}: ")

    # Resolve output directory: CLI > file > cwd
    output_dir = cli_output_dir
    if output_dir is None:
        output_dir = cast(str | None, file_config.get("output_dir"))
    if output_dir is None:
        output_dir = os.getcwd()

    output_dir = os.path.abspath(os.path.expanduser(output_dir))

    # Resolve refresh_interval: CLI > env > file > default
    refresh_interval = cast(int | None, kwargs.pop("refresh_interval", None))
    if refresh_interval is None:
        env_refresh = os.environ.get(ENV_REFRESH_INTERVAL)
        if env_refresh is not None:
            try:
                refresh_interval = int(env_refresh)
            except ValueError:
                pass  # Ignore invalid env var, use file/default
    if refresh_interval is None:
        refresh_interval = cast(int | None, file_config.get("refresh_interval"))
    if refresh_interval is None:
        refresh_interval = DEFAULT_REFRESH_INTERVAL_MINUTES

    return Config(
        username=username,
        password=password,
        output_dir=output_dir,
        refresh_interval=refresh_interval,
        **cast(dict[str, Any], kwargs),
    )
