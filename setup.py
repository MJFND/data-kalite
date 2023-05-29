from setuptools import find_packages, setup

install_requires = []

with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(
    name="data-kalite",
    version="0.1",
    packages=find_packages(),
    install_requires=install_requires,
    python_requires=">=3.7.0",
)
