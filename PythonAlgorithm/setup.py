# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name='PythonAlgorithm',
    version='0.1.0',
    description='Sample package for Python-Guide.org',
    author='Dexter Chan',
    author_email='abcd@abcd.com',
    packages=find_packages(exclude=('tests', 'docs'))
)