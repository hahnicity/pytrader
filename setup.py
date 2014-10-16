#!/usr/bin/env python
from setuptools import setup, find_packages

__version__ = "0.1"


setup(
    name="pytrader",
    author="Gregory Rehm",
    version=__version__,
    description="Python based open source stock trader",
    packages=find_packages(),
    package_data={"*": ["*.html"]},
    install_requires=[
        "cowboycushion",
        "finsymbols",
        "numpy",
        "pandas",
        "redis",
        "ychartspy",
    ],
    tests_require=[
        "mock",
        "nose",
    ],
    entry_points={
        "console_scripts": [
            "pytrader=pytrader.main:main",
        ]
    }
)
