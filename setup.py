"""
MongoDB Wire Protocol for asyncio
"""

import pathlib

from pkg_resources import get_distribution, DistributionNotFound
from setuptools import setup, find_packages

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')


def get_dist(pkgname):
    try:
        return get_distribution(pkgname)
    except DistributionNotFound:
        return None


install_requires = []
if get_dist('bson') is None and get_dist('pymongo') is None:
    install_requires.append('bson')

setup(
    name='aiomongowire',
    version='0.0.1',
    description='MongoDB Wire Protocol for asyncio',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/upcfrost/aiomongowire',
    author='Petr Belyaev',
    author_email='upcfrost@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='development, mongo, mongodb, asyncio',
    package_dir={'': 'src'},
    packages=find_packages(where='src', exclude=['tests']),
    python_requires='>=3.6, <4',
    install_requires=install_requires,
    extras_require={
        'snappy': ['python-snappy~=0.6'],
        'zstd': ['zstandard~=0.15'],
        'pymongo': ['pymongo~=3.12'],
        'bson': ['bson~=0.5'],
    },
    project_urls={
        'Bug Reports': 'https://github.com/upcFrost/aiomongowire/issues',
        'Source': 'https://github.com/upcFrost/aiomongowire',
    },
)
