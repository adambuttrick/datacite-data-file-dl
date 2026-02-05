# DataCite Data File DL

CLI utility to download DataCite monthly data files.

## Installation

```bash
pip install datacite-data-file-dl
```

Or with uv:
```bash
uv tool install datacite-data-file-dl
```

## Quick Start

```bash
# Interactive mode - browse and select files
datacite-data-file-dl -u YOUR_USERNAME -p YOUR_PASSWORD

# Download a specific folder
datacite-data-file-dl -u USER -p PASS --path dois/updated_2024-01/

# Download entire bucket
datacite-data-file-dl -u USER -p PASS --all -o ./data
```

`ddf-dl` is available as a shorter alternative to `datacite-data-file-dl` for all commands.

## Configuration

### Environment Variables

```bash
export DATACITE_USERNAME=your_account_id
export DATACITE_PASSWORD=your_password
datacite-data-file-dl --list
```

### Config File

Create `~/.datacite-data-file-dl.toml` or `~/.config/datacite-data-file-dl/config.toml`:

```toml
username = "your_account_id"
output_dir = "~/data/datacite"
```

Then just run:
```bash
datacite-data-file-dl --list
```

### Credential Precedence

1. CLI arguments (`-u`, `-p`)
2. Environment variables (`DATACITE_USERNAME`, `DATACITE_PASSWORD`)
3. Config file (`~/.datacite-data-file-dl.toml`)
4. Interactive prompt (password only)

## Usage Examples

### List Available Files

```bash
# List root of bucket
datacite-data-file-dl --list

# List specific folder
datacite-data-file-dl --list --path dois/

# JSON output for scripting
datacite-data-file-dl --list --path dois/ --json
```

### Download with Filtering

```bash
# Only JSON files
datacite-data-file-dl --path dois/updated_2024-01/ --include "*.json"

# Exclude large archives
datacite-data-file-dl --all --exclude "*.zip" --max-size 100MB

# Date range
datacite-data-file-dl --all --since 2024-01 --until 2024-06
```

### Additional Options

```bash
# Resume interrupted download
datacite-data-file-dl --all --resume

# Start fresh, ignore previous progress
datacite-data-file-dl --all --fresh

# More retry attempts
datacite-data-file-dl --all --retries 5

# Skip checksum verification (faster)
datacite-data-file-dl --all --skip-verify

# Parallel download with 8 workers (default: 4)
datacite-data-file-dl --all -w 8

# Sequential download (single worker)
datacite-data-file-dl --all -w 1
```

### Using in Automation

```bash
# Quiet mode for scripts
datacite-data-file-dl --all -q

# JSON output
datacite-data-file-dl --all --json > result.json

# Dry run to see what would be downloaded
datacite-data-file-dl --all --dry-run

# Verbose output for debugging
datacite-data-file-dl --all -v

# Write logs to file
datacite-data-file-dl --all --log-file download.log

# Skip confirmation prompts
datacite-data-file-dl --all -y
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Authentication failure |
| 2 | Network/connection error |
| 3 | File not found / invalid path |
| 4 | Partial failure (some files failed) |
| 5 | User cancelled |

## Development

```bash
# Install with dev dependencies
make install

# Run tests
make test

# Run linter and type checker
make check

# Format code
make format
```

## License

MIT
