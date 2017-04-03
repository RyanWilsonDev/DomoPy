import os

from setuptools import setup


# Manage version in __init__.py


def get_version(version_tuple):
    """version from tuple accounting for possible a,b,rc tags."""
    # in case an a, b, or rc tag is added
    if not isinstance(version_tuple[-1], int):
        return '.'.join(
            map(str, version_tuple[:-1])
        ) + version_tuple[-1]

    return '.'.join(map(str, version_tuple))

# path to __init__ for package
INIT = os.path.join(
    os.path.dirname(__file__), 'domopy',
    '__init__.py'
)

VERSION_LINE = list(
    filter(lambda line: line.startswith('VERSION'), open(INIT))
)[0]

# lotta effort but package might not be importable before
# install is finished so can't just import VERSION
VERSION = get_version(eval(VERSION_LINE.split('=')[-1]))

setup(
    name='domopy',
    version=VERSION,
    author='Ryan Wilson',
    license='MIT',
    url='https://github.com/RyanWilsonDev/DomoPy',
    description="methods for interacting with Domo APIs",
    long_description="""
        Set of classes and methods for interacting with
        the Domo Data APIs and Domo User APIs. Handles
        Authentication, pulling data from domo, creating
        new domo datasets, replace/appending existing
        datasets, etc.
    """,
    packages=[
        'domopy'
    ],
    package_data={'': ['LICENSE'], 'LICENSES': ['NOTICE', 'PANDAS_LICENSE', 'REQUESTS_LICENSE']},
    include_package_data=True,
    install_requires=[
        'pandas',
        'requests',
        'requests_oauthlib'
    ],
    classifiers=(
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython'
    ),
    tests_require=[]
)
