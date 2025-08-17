#!/usr/bin/env python3
"""
SQLKit development workflow script
"""

import subprocess
import sys


def run_command(cmd: list[str], exit_on_error: bool = True) -> bool:
    """Run a command and return success status"""
    try:
        subprocess.run(cmd, check=True)
        print(f"✓ {' '.join(cmd)}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {' '.join(cmd)} (exit code: {e.returncode})")
        if exit_on_error:
            sys.exit(1)
        return False


def format_code():
    """Format code with black and ruff"""
    print("🎨 Formatting code...")
    run_command(["uv", "run", "black", "sqlkit/", "examples/", "scripts/"])
    run_command(
        ["uv", "run", "ruff", "format", "sqlkit/", "examples/", "scripts/"]
    )


def lint_code():
    """Lint code with ruff"""
    print("🔍 Linting code...")
    run_command(
        [
            "uv",
            "run",
            "ruff",
            "check",
            "sqlkit/",
            "examples/",
            "scripts/",
            "--fix",
        ]
    )


def type_check():
    """Type check with mypy"""
    print("🧹 Type checking...")
    run_command(["uv", "run", "mypy", "sqlkit/"], exit_on_error=False)


def run_tests():
    """Run tests with pytest"""
    print("🧪 Running tests...")
    run_command(["uv", "run", "pytest", "-v", "--cov=sqlkit"])


def check_all():
    """Run all quality checks"""
    print("🔧 Running all quality checks...")
    format_code()
    lint_code()
    type_check()
    run_tests()
    print("\n✅ All checks completed!")


def main():
    """Main CLI"""
    if len(sys.argv) < 2:
        print("SQLKit Development Script")
        print("\nUsage: uv run python scripts/dev.py <command>")
        print("\nCommands:")
        print("  format    - Format code with black and ruff")
        print("  lint      - Lint code with ruff")
        print("  typecheck - Type check with mypy")
        print("  test      - Run tests with pytest")
        print("  check     - Run all quality checks")
        sys.exit(1)

    command = sys.argv[1]

    if command == "format":
        format_code()
    elif command == "lint":
        lint_code()
    elif command == "typecheck":
        type_check()
    elif command == "test":
        run_tests()
    elif command == "check":
        check_all()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
