#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = de-totes-bags
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install flake8
flake:
	$(call execute_in_env, $(PIP) install flake8)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, safety, flake8)
dev-setup: bandit safety flake coverage

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the flake8 code check -- old $(call execute_in_env, flake8  ./src/*/*.py ./test/*/*.py)
# $(call execute_in_env, flake8 ./python/*/*.py ./python/*/*.py)
run-flake:
	$(call execute_in_env, flake8 ./python/*/*/*.py ./python/*/*/*.py)

## Run the unit tests
unit-test:
# $(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v --ignore=/home/mfay/Documents/northcoders/data_engineering/Project/de-totes-bags/deployment/loading_function/src/fp_v3.10/fsspec)
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v --ignore=deployment)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest --ignore=deployment && coverage report -m)

## Run all checks
# run-checks: security-test run-flake unit-test check-coverage
run-checks: run-flake unit-test check-coverage