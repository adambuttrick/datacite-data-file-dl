"""Tests for configuration loading."""

from unittest.mock import patch


from datacite_data_file_dl.config import load_config


class TestEnvironmentVariables:
    """Test loading config from environment variables."""

    def test_load_username_from_env(self, monkeypatch):
        """Username should be loaded from DATACITE_USERNAME."""
        monkeypatch.setenv("DATACITE_USERNAME", "test-user")
        config = load_config()
        assert config.username == "test-user"

    def test_load_password_from_env(self, monkeypatch):
        """Password should be loaded from DATACITE_PASSWORD."""
        monkeypatch.setenv("DATACITE_PASSWORD", "test-pass")
        config = load_config()
        assert config.password == "test-pass"

    def test_cli_args_override_env(self, monkeypatch):
        """CLI arguments should take precedence over env vars."""
        monkeypatch.setenv("DATACITE_USERNAME", "env-user")
        config = load_config(cli_username="cli-user")
        assert config.username == "cli-user"

    def test_missing_credentials_returns_none(self):
        """Missing credentials should be None, not raise."""
        config = load_config()
        assert config.username is None
        assert config.password is None


class TestConfigFile:
    """Test loading config from TOML file."""

    def test_load_username_from_config_file(self, tmp_path, monkeypatch):
        """Username should be loaded from config file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('username = "file-user"\n')

        config = load_config(config_file=str(config_file))
        assert config.username == "file-user"

    def test_load_output_dir_from_config_file(self, tmp_path, monkeypatch):
        """Output dir should be loaded and expanded from config file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(f'output_dir = "{tmp_path}/data"\n')

        config = load_config(config_file=str(config_file))
        assert config.output_dir == str(tmp_path / "data")

    def test_env_overrides_config_file(self, tmp_path, monkeypatch):
        """Env vars should take precedence over config file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('username = "file-user"\n')
        monkeypatch.setenv("DATACITE_USERNAME", "env-user")

        config = load_config(config_file=str(config_file))
        assert config.username == "env-user"

    def test_missing_config_file_is_ok(self):
        """Missing config file should not raise."""
        config = load_config(config_file="/nonexistent/path/config.toml")
        assert config.username is None


class TestConfigPrecedence:
    """Test the full precedence chain: CLI > env > file."""

    def test_full_precedence(self, tmp_path, monkeypatch):
        """CLI args should override env which overrides file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('username = "file-user"\n')
        monkeypatch.setenv("DATACITE_USERNAME", "env-user")

        config = load_config(
            cli_username="cli-user",
            config_file=str(config_file),
        )
        assert config.username == "cli-user"


class TestPasswordPrompt:
    """Test interactive password prompt."""

    def test_prompt_for_password_when_missing(self, monkeypatch):
        """Should prompt for password if not provided."""
        monkeypatch.setenv("DATACITE_USERNAME", "test-user")

        with patch("datacite_data_file_dl.config.getpass", return_value="prompted-pass"):
            config = load_config(prompt_for_missing=True)

        assert config.password == "prompted-pass"

    def test_no_prompt_when_password_provided(self, monkeypatch):
        """Should not prompt if password already provided."""
        monkeypatch.setenv("DATACITE_USERNAME", "test-user")
        monkeypatch.setenv("DATACITE_PASSWORD", "env-pass")

        with patch("datacite_data_file_dl.config.getpass") as mock_getpass:
            config = load_config(prompt_for_missing=True)

        mock_getpass.assert_not_called()
        assert config.password == "env-pass"

    def test_no_prompt_by_default(self, monkeypatch):
        """Should not prompt by default (for non-interactive use)."""
        monkeypatch.setenv("DATACITE_USERNAME", "test-user")

        with patch("datacite_data_file_dl.config.getpass") as mock_getpass:
            config = load_config()

        mock_getpass.assert_not_called()
        assert config.password is None
