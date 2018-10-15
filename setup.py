#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
import io
import os
import re


def _read(*parts, **kwargs):
    filepath = os.path.join(os.path.dirname(__file__), *parts)
    encoding = kwargs.pop('encoding', 'utf-8')
    with io.open(filepath, encoding=encoding) as fh:
        text = fh.read()
    return text

def get_version():
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        _read('coltab', '_version.py'),
        re.MULTILINE).group(1)
    return version


long_description = _read('README.md')


setup(
    name='coltab',
    author='Nezar Abdennur',
    author_email='nabdennur@gmail.com',
    version=get_version(),
    description='Columnar tables',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT',
    packages=find_packages(),
    zip_safe=False,
    #url='https://github.com/nvictus/coltab',
    #keywords=[],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=[
        'h5py',
        'numpy',
        'pandas',
    ],    
)
