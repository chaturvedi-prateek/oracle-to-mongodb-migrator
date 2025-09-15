from setuptools import setup, find_packages

setup(
    name="oracle-to-mongodb-migrator",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "motor",
        "prometheus_client",
        "structlog",
        "tenacity",
        "pytest",
        "pytest-asyncio"
    ]
)