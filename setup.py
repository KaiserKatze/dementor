#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @see https://github.com/pypa/sampleproject/blob/master/setup.py

from setuptools import setup, find_packages
import os.path

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md'), encoding='utf-8') as file:
    long_description = file.read()

setup(
    name='dementor',
    version='1.0.0',
    description='专注于抓取金融数据的爬虫',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/donizyo/shfe',
    author='KaiserKatze',
    author_email='donizyo@gmail.com',
    # @see https://pypi.org/classifiers/
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        # Audience
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Manufacturing',
        # Pick your license as you wish
        'License :: OSI Approved :: MIT License',
        # Programming language
        'Programming Language :: Python :: 3.6',
        # Topic
        'Topic :: Office/Business :: Financial :: Investment',
    ],
    keywords='economics finance',
    packages=find_packages(exclude=[
        'contrib',
        'docs',
        'tests'
    ]),
    install_requires=[
        'requests',
        'pandas',
        'numpy',
        'matplotlib',
        'flask',
        'beautifulsoup4',
        'lxml',
    ],
    project_urls={
        'Bug Reports': 'https://github.com/donizyo/shfe/issues',
        'Source': 'https://github.com/donizyo/shfe/',
    },
)

