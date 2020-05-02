"""Common fixtures and configurations for this test directory."""
import os
import pathlib
import shutil

import pytest


@pytest.fixture('module', autouse=True)
def cwd_module_dir():
    """Change current directory to this module's folder to access inputs and write outputs."""
    cwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    yield
    os.chdir(cwd)


@pytest.fixture(scope='module')
def output_dir(cwd_module_dir):
    path = 'output'
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    yield pathlib.Path(path)
    shutil.rmtree(path, ignore_errors=False)
