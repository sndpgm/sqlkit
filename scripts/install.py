#!/usr/bin/env python3
"""
SQLKit installation and setup script
"""

import subprocess
import sys


def run_command(cmd: list[str]) -> bool:
    """Run a command and return success status"""
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"✓ {' '.join(cmd)}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {' '.join(cmd)}")
        print(f"Error: {e.stderr}")
        return False


def main():
    """Main installation process"""
    print("🔧 Setting up SQLKit development environment...")

    # Check if uv is installed
    if not run_command(["uv", "--version"]):
        print("❌ uv is not installed. Please install uv first:")
        print(
            '   Windows: powershell -c "irm https://astral.sh/uv/install.ps1'
            ' | iex"'
        )
        print(
            "   macOS/Linux: curl -LsSf https://astral.sh/uv/install.sh | sh"
        )
        sys.exit(1)

    # Sync dependencies
    print("\n📦 Installing dependencies...")
    if not run_command(["uv", "sync", "--all-packages"]):
        print("❌ Failed to install dependencies")
        sys.exit(1)

    # Install the package in development mode
    print("\n🔗 Installing sqlkit in development mode...")
    if not run_command(["uv", "pip", "install", "-e", "."]):
        print("❌ Failed to install sqlkit in development mode")
        sys.exit(1)

    print("\n✅ SQLKit development environment setup complete!")
    print("\n📚 Next steps:")
    print("   1. Activate the virtual environment:")
    print("      - Windows: .venv\\Scripts\\Activate.ps1")
    print("      - macOS/Linux: source .venv/bin/activate")
    print("   2. Run tests: uv run pytest")
    print("   3. Run examples: uv run python examples/usage_examples.py")
    print("   4. Format code: uv run black sqlkit/")
    print("   5. Lint code: uv run ruff check sqlkit/")


if __name__ == "__main__":
    main()
