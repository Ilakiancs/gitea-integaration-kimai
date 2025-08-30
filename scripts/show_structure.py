#!/usr/bin/env python3
"""
Project Structure Viewer

This script displays the organized structure of the Gitea to Kimai Integration project.
"""

import os
from pathlib import Path

def get_file_info(file_path):
    """Get basic information about a file."""
    stat = file_path.stat()
    size = stat.st_size
    if size < 1024:
        return f"{size}B"
    elif size < 1024 * 1024:
        return f"{size // 1024}KB"
    else:
        return f"{size // (1024 * 1024)}MB"

def print_directory_tree(directory, prefix="", max_depth=3, current_depth=0):
    """Print a tree representation of the directory structure."""
    if current_depth > max_depth:
        return
    
    items = sorted(directory.iterdir(), key=lambda x: (x.is_file(), x.name.lower()))
    
    for i, item in enumerate(items):
        is_last = i == len(items) - 1
        current_prefix = "└── " if is_last else "├── "
        next_prefix = "    " if is_last else "│   "
        
        if item.is_file():
            size = get_file_info(item)
            print(f"{prefix}{current_prefix}{item.name} ({size})")
        else:
            print(f"{prefix}{current_prefix}{item.name}/")
            if current_depth < max_depth:
                print_directory_tree(item, prefix + next_prefix, max_depth, current_depth + 1)

def main():
    """Main function to display project structure."""
    project_root = Path(__file__).parent.parent
    
    print("=" * 60)
    print("GITEA TO KIMAI INTEGRATION - PROJECT STRUCTURE")
    print("=" * 60)
    print()
    
    print("📁 Project Root:")
    print_directory_tree(project_root, max_depth=2)
    
    print("\n" + "=" * 60)
    print("📋 MODULE DESCRIPTIONS")
    print("=" * 60)
    
    modules = {
        "src/core/": "Core synchronization and task management",
        "src/api/": "API handling and webhook management", 
        "src/data/": "Data processing and manipulation",
        "src/security/": "Security and authentication",
        "src/utils/": "Utility functions and helpers",
        "src/web/": "Web interface components",
        "src/config/": "Configuration management",
        "src/validation/": "Data validation",
        "src/monitoring/": "System monitoring and metrics",
        "src/storage/": "Data storage and caching"
    }
    
    for module, description in modules.items():
        module_path = project_root / module
        if module_path.exists():
            file_count = len(list(module_path.glob("*.py")))
            print(f"📂 {module:<20} {description:<40} ({file_count} files)")
    
    print("\n" + "=" * 60)
    print("🚀 QUICK START")
    print("=" * 60)
    print()
    print("To get started with the organized project:")
    print()
    print("1. Run the main application:")
    print("   python main.py --help")
    print()
    print("2. Run synchronization:")
    print("   python main.py sync")
    print()
    print("3. Run diagnostics:")
    print("   python main.py diagnose")
    print()
    print("4. Start web dashboard:")
    print("   python main.py dashboard")
    print()
    print("5. View project structure:")
    print("   python scripts/show_structure.py")
    print()
    print("📖 For detailed documentation, see:")
    print("   - README.md (main documentation)")
    print("   - PROJECT_STRUCTURE.md (detailed structure)")
    print("   - DIAGNOSTICS.md (troubleshooting guide)")

if __name__ == "__main__":
    main()
