PROJECT_NAME := $(shell python3 setup.py --name)
PROJECT_VERSION := $(shell python3 setup.py --version)

.PHONY: tests

pre-commit:
	pre-commit run --all-files

install:
	pip3 install -r requirements-dev.txt
	pip3 install -r requirements.txt
	pre-commit install