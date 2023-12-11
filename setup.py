#!/usr/bin/env python

# Always prefer setuptools over distutils
from setuptools import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    readme = f.read()

# Get the long description from the README file
with open(path.join(here, 'HISTORY.rst'), encoding='utf-8') as f:
    history = f.read()

packages = ['lightstreamer_adapter',
            'lightstreamer_adapter.interfaces']

setup(
    name='lightstreamer_adapter',
    version='1.3.1',
    description='Lightstreamer SDK for Python Adapters',
    long_description=readme + '\n\n' + history,
    url='https://github.com/Lightstreamer/Lightstreamer-lib-python-adapter',
    author='Lightstreamer Srl',
    author_email='support@lightstreamer.com',
    license='Apache License 2.0',
    packages=packages,
    package_dir={'lightstreamer_adapter': 'lightstreamer_adapter'},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Communications',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks'
    ],
    keywords='lightstreamer push realtime real-time',
    test_suite='tests'
)
