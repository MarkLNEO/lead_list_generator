"""Setup configuration for Lead Pipeline."""

from setuptools import setup, find_packages

setup(
    name="lead_pipeline",
    version="2.0.0",
    description="Lead List Orchestrator - Production-Ready Lead Generation Pipeline",
    author="Mark Lerner",
    py_modules=["lead_pipeline"],
    python_requires=">=3.9",
    install_requires=[],
    extras_require={
        "test": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "pytest-xdist>=3.5.0",
            "pytest-timeout>=2.2.0",
        ],
    },
)
