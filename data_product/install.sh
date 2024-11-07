#!/usr/bin/env bash

pyenv install "$1" -s
pyenv local "$1"
poetry env use "$1"
poetry install
