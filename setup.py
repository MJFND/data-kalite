from setuptools import find_packages, setup

install_requires = []

with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="data-kalite",
    version="1.0.3",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=install_requires,
    python_requires=">3.7.0,<3.11.0",
    author="Junaid Effendi",
    description="Data Quality for PySpark Pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
)
