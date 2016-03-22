#!/usr/bin/env python


# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

packages = [
    'lightstreamer-adapter',
    'lightstreamer-adapter.lightstreamer',
    'lightstreamer-adapter.lightstreamer.adapter',
    'lightstreamer-adapter.lightstreamer.interfaces',
]

setup(
    name='lightstreamer adapter',
    version='1.0.0a1',
    description='Lighstreamer SDK for Python Adapters',
    long_description=long_description,
    url='https://github.com/Lightstreamer/Lightstreamer-lib-python-adapter',
    author='Lightstreamer Srl',
    author_email='support@lightstreamer.com',
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console'
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
    packages=packages
)
