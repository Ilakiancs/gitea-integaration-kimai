#!/usr/bin/env python3
"""
Setup script for Gitea-Kimai Integration System
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="gitea-kimai-integration",
    version="1.0.1",
    author="Your Name",
    author_email="your.email@example.com",
    description="A comprehensive integration system for synchronizing data between Gitea and Kimai",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/gitea-kimai-integration",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
        "web": [
            "flask>=2.0",
            "flask-cors>=3.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "gitea-kimai-sync=main:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
