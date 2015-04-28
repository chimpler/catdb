#!/usr/bin/env python

import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

class PyTestCommand(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name='catdb',
    version='0.0.1',
    description='Utility to copy data from various databases',
    long_description='Utility to import, export and copy data from various databases',
    keywords='catdb',
    license='Apache License 2.0',
    author='Francois Dang Ngoc',
    author_email='francois.dangngoc@gmail.com',
    url='http://github.com/chimpler/catdb/',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    packages=[
        'catdb',
    ],
    install_requires=[
        'pyhocon==0.3.1',
        'psycopg2==2.6',
        'boto3==0.0.16',
        'PyMySQL==0.6.6'
    ] + ['importlib==1.0.3'] if sys.version_info[:2] == (2, 6) else [],
    tests_require=[
        'pytest',
        'mock'
    ],
    entry_points={
        'console_scripts': [
            'catdb=catdb.main:main'
        ]
    },
    test_suite='tests',
    cmdclass={
        'test': PyTestCommand
    },
    package_data={'catdb': ['*.conf']}
)
