#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='encounters',
    version='4.3.2',
    packages=find_packages(exclude=['test*.*', 'tests']),
    install_requires=[
        "apache-beam[gcp]~=2.49",
        "more_itertools~=8.12",
        "s2sphere~=0.2",
        "statistics~=1.0.3",
    ]
)
