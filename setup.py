import os
import re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))


DEV_REQUIRES = (
    'pytest'
)


def find_version(*file_paths):
    """
    see https://github.com/pypa/sampleproject/blob/master/setup.py
    """
    with open(os.path.join(here, *file_paths), 'r') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string. "
                       "Should be at the first line of __init__.py.")

setup(
    name='fdwpointcloud',
    version=find_version('fdwpointcloud', '__init__.py'),
    description="fdwpointcloud",
    url='',
    author='oslandia',
    author_email='contact@oslandia.com',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
    extras_require={
        'dev': DEV_REQUIRES,
    },
    package_data={'fdwpointcloud': ['schemas/*']}
)
