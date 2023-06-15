#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', ]

with open("requirements.txt") as requirements_file:
    requirements = requirements_file.read()


setup(
    author="MasatakaUgumori",
    author_email='mugumori@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Python Boilerplate contains all the boilerplate you need to create a Python package.",
    entry_points={
        'console_scripts': [
            'raspi_gpio_streamer=raspi_gpio_streamer.main:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='raspi_gpio_streamer',
    name='raspi_gpio_streamer',
    packages=find_packages(include=['raspi_gpio_streamer', 'raspi_gpio_streamer.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ugumori/raspi_gpio_streamer',
    version='0.1.0',
    zip_safe=False,
)
