"""
MongoDB Wire Protocol for asyncio
"""

import pathlib

from setuptools import setup, find_packages

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')

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
    install_requires=['bson'],
    project_urls={
        'Bug Reports': 'https://github.com/upcFrost/aiomongowire/issues',
        'Source': 'https://github.com/upcFrost/aiomongowire',
    },
)