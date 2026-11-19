#!/usr/bin/env python3
"""
CI/CD helper: replace cross-workspace lakehouse tokens in YAML files.

Tokens supported (exact match, including braces):
  {{source_workspace_id}}
  {{source_lakehouse_id}}
  {{target_workspace_id}}
  {{target_lakehouse_id}}

Values are read from environment variables of the same name
(SOURCE_WORKSPACE_ID, SOURCE_LAKEHOUSE_ID, TARGET_WORKSPACE_ID, TARGET_LAKEHOUSE_ID)
or can be supplied as CLI arguments.

Usage:
  python replace_yaml_tokens.py --path data_product/models
  python replace_yaml_tokens.py --path ./configs --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict

TOKEN_MAP = {
    "{{source_workspace_id}}": "SOURCE_WORKSPACE_ID",
    "{{source_lakehouse_id}}": "SOURCE_LAKEHOUSE_ID",
    "{{target_workspace_id}}": "TARGET_WORKSPACE_ID",
    "{{target_lakehouse_id}}": "TARGET_LAKEHOUSE_ID",
}


def load_replacements(cli_overrides: Dict[str, str] | None = None) -> Dict[str, str]:
    """Build token → value map from env vars (and optional CLI overrides)."""
    replacements: Dict[str, str] = {}
    missing = []

    for token, env_name in TOKEN_MAP.items():
        value = None
        if cli_overrides and env_name.lower() in cli_overrides:
            value = cli_overrides[env_name.lower()]
        else:
            value = os.environ.get(env_name)

        if value:
            replacements[token] = value
        else:
            missing.append(env_name)

    if missing:
        print(
            "WARNING: The following pipeline variables are not set. "
            "Tokens for them will be left unchanged:",
            ", ".join(missing),
            file=sys.stderr,
        )

    return replacements


def replace_in_file(file_path: Path, replacements: Dict[str, str], dry_run: bool) -> int:
    """Replace tokens in a single file. Returns number of replacements made."""
    try:
        original = file_path.read_text(encoding="utf-8")
    except Exception as exc:
        print(f"ERROR reading {file_path}: {exc}", file=sys.stderr)
        return 0

    updated = original
    count = 0
    for token, value in replacements.items():
        occurrences = updated.count(token)
        if occurrences:
            updated = updated.replace(token, value)
            count += occurrences
            print(f"  {file_path}: replaced {occurrences} × {token}")

    if count and not dry_run:
        file_path.write_text(updated, encoding="utf-8")

    return count


def process_path(root: Path, replacements: Dict[str, str], dry_run: bool) -> int:
    """Walk relative path and process all .yaml / .yml files."""
    if not root.exists():
        print(f"ERROR: path does not exist: {root}", file=sys.stderr)
        sys.exit(1)

    yaml_files = list(root.rglob("*.yaml")) + list(root.rglob("*.yml"))
    if not yaml_files:
        print(f"No YAML files found under {root}")
        return 0

    print(f"Scanning {len(yaml_files)} YAML file(s) under {root}")
    if dry_run:
        print("DRY-RUN mode – no files will be modified")

    total = 0
    for yml in sorted(yaml_files):
        total += replace_in_file(yml, replacements, dry_run)

    print(f"Total replacements: {total}")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replace {{source_*}} / {{target_*}} tokens in YAML files"
    )
    parser.add_argument(
        "--path",
        required=True,
        help="Relative (or absolute) path to the directory containing YAML files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be replaced without writing changes",
    )
    # Optional direct overrides (useful for local testing)
    parser.add_argument("--source-workspace-id", dest="source_workspace_id")
    parser.add_argument("--source-lakehouse-id", dest="source_lakehouse_id")
    parser.add_argument("--target-workspace-id", dest="target_workspace_id")
    parser.add_argument("--target-lakehouse-id", dest="target_lakehouse_id")

    args = parser.parse_args()

    cli_overrides = {
        k: v
        for k, v in {
            "source_workspace_id": args.source_workspace_id,
            "source_lakehouse_id": args.source_lakehouse_id,
            "target_workspace_id": args.target_workspace_id,
            "target_lakehouse_id": args.target_lakehouse_id,
        }.items()
        if v
    }

    replacements = load_replacements(cli_overrides)
    if not replacements:
        print("No replacement values available. Exiting.")
        sys.exit(0)

    root = Path(args.path)
    process_path(root, replacements, args.dry_run)


if __name__ == "__main__":
    main()
