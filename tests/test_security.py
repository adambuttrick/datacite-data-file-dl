"""Security tests for path traversal and credential handling."""

import warnings
from pathlib import Path

import pytest

from datacite_data_file_dl.safe_path import PathTraversalError, safe_join
from datacite_data_file_dl.config import InsecureConfigWarning, load_config


class TestPathTraversalPrevention:
    """Test that path traversal attacks are blocked."""

    # Tests for valid paths that should work

    def test_simple_path(self, tmp_path):
        """Normal paths should work."""
        result = safe_join(tmp_path, "file.json")
        assert result == tmp_path / "file.json"

    def test_nested_path(self, tmp_path):
        """Nested paths should work."""
        result = safe_join(tmp_path, "subdir/file.json")
        assert result == tmp_path / "subdir" / "file.json"

    def test_deeply_nested_path(self, tmp_path):
        """Deeply nested paths should work."""
        result = safe_join(tmp_path, "a/b/c/d/file.json")
        assert result == tmp_path / "a" / "b" / "c" / "d" / "file.json"

    def test_file_with_dots_in_name(self, tmp_path):
        """Files with dots in name should work."""
        result = safe_join(tmp_path, "file.backup.json")
        assert result == tmp_path / "file.backup.json"

    def test_folder_with_dots_in_name(self, tmp_path):
        """Folders with dots (not at start) should work."""
        result = safe_join(tmp_path, "v1.0.0/file.json")
        assert result == tmp_path / "v1.0.0" / "file.json"

    def test_path_with_spaces(self, tmp_path):
        """Paths with spaces should work."""
        result = safe_join(tmp_path, "path with spaces/file.json")
        assert result == tmp_path / "path with spaces" / "file.json"

    def test_unicode_path(self, tmp_path):
        """Unicode paths should work."""
        result = safe_join(tmp_path, "datos/archivo.json")
        assert result == tmp_path / "datos" / "archivo.json"

    def test_navigating_within_allowed_tree(self, tmp_path):
        """Navigating up and back down within allowed tree should work."""
        result = safe_join(tmp_path, "a/b/../c/file.json")
        assert result == tmp_path / "a" / "c" / "file.json"

    # Tests for path traversal attacks that should be blocked

    def test_parent_directory_attack(self, tmp_path):
        """../etc/passwd should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "../etc/passwd")
        assert "cannot start with '.'" in str(exc_info.value)

    def test_multiple_parent_traversal(self, tmp_path):
        """../../etc/passwd should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "../../etc/passwd")
        assert "cannot start with '.'" in str(exc_info.value)

    def test_deep_parent_traversal(self, tmp_path):
        """../../../etc/passwd should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "../../../etc/passwd")
        assert "cannot start with '.'" in str(exc_info.value)

    def test_embedded_parent_traversal(self, tmp_path):
        """foo/../../../etc/passwd should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "foo/../../../etc/passwd")
        assert "escapes base directory" in str(exc_info.value)

    def test_double_dot_in_middle(self, tmp_path):
        """subdir/../../etc/passwd should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "subdir/../../etc/passwd")
        assert "escapes base directory" in str(exc_info.value)

    def test_absolute_path_attack(self, tmp_path):
        """/etc/passwd absolute path should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "/etc/passwd")
        assert "absolute path not allowed" in str(exc_info.value)

    def test_empty_string(self, tmp_path):
        """Empty string should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "")
        assert "empty path" in str(exc_info.value)

    def test_whitespace_only(self, tmp_path):
        """Whitespace-only string should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "   ")
        assert "empty path" in str(exc_info.value)

    def test_single_dot(self, tmp_path):
        """Single dot should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, ".")
        assert "cannot start with '.'" in str(exc_info.value)

    def test_double_dot(self, tmp_path):
        """Double dot should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "..")
        assert "cannot start with '.'" in str(exc_info.value)

    def test_hidden_file(self, tmp_path):
        """Hidden file (starting with .) should be blocked."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, ".hidden")
        assert "cannot start with '.'" in str(exc_info.value)

    def test_path_traversal_error_attributes(self, tmp_path):
        """PathTraversalError should have useful attributes."""
        with pytest.raises(PathTraversalError) as exc_info:
            safe_join(tmp_path, "../etc/passwd")
        error = exc_info.value
        assert error.untrusted_path == "../etc/passwd"
        assert "cannot start with '.'" in error.reason

    def test_base_dir_as_string(self, tmp_path):
        """Base directory can be a string."""
        result = safe_join(str(tmp_path), "file.json")
        assert result == tmp_path / "file.json"


class TestPasswordWarning:
    """Test that insecure password storage triggers warnings."""

    def test_password_from_file_warns(self, tmp_path):
        """Loading password from config file should emit warning."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('password = "secret123"\n')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = load_config(config_file=str(config_file))

        # Filter for our specific warning
        our_warnings = [x for x in w if issubclass(x.category, InsecureConfigWarning)]
        assert len(our_warnings) == 1
        assert "plaintext" in str(our_warnings[0].message).lower()
        assert config.password == "secret123"

    def test_password_from_env_no_warning(self, tmp_path, monkeypatch):
        """Loading password from env var should not warn."""
        monkeypatch.setenv("DATACITE_PASSWORD", "secret123")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = load_config()

        # Filter for our specific warning
        our_warnings = [x for x in w if issubclass(x.category, InsecureConfigWarning)]
        assert len(our_warnings) == 0
        assert config.password == "secret123"

    def test_password_from_cli_no_warning(self):
        """Loading password from CLI should not warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = load_config(cli_password="secret123")

        # Filter for our specific warning
        our_warnings = [x for x in w if issubclass(x.category, InsecureConfigWarning)]
        assert len(our_warnings) == 0
        assert config.password == "secret123"

    def test_cli_password_overrides_file_no_warning(self, tmp_path):
        """CLI password should override file password without warning."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('password = "file-secret"\n')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = load_config(
                cli_password="cli-secret",
                config_file=str(config_file),
            )

        # Filter for our specific warning
        our_warnings = [x for x in w if issubclass(x.category, InsecureConfigWarning)]
        assert len(our_warnings) == 0
        assert config.password == "cli-secret"

    def test_env_password_overrides_file_no_warning(self, tmp_path, monkeypatch):
        """Env password should override file password without warning."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('password = "file-secret"\n')
        monkeypatch.setenv("DATACITE_PASSWORD", "env-secret")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = load_config(config_file=str(config_file))

        # Filter for our specific warning
        our_warnings = [x for x in w if issubclass(x.category, InsecureConfigWarning)]
        assert len(our_warnings) == 0
        assert config.password == "env-secret"
