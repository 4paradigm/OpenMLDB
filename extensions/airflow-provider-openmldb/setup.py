"""Setup.py for the OpenMLDB Airflow provider package. Built from datadog provider package for now."""

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-openmldb setup."""
setup(
    name='airflow-provider-openmldb',
    version="0.0.1",
    description='A openmldb provider package built by 4paradigm.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=openmldb_provider.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['openmldb_provider', 'openmldb_provider.hooks',
              'openmldb_provider.operators'],
    install_requires=['apache-airflow>=2.0'],
    setup_requires=['setuptools', 'wheel'],
    author='Huang Wei',
    author_email='huangwei@apache.org',
    url='',
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires='~=3.7',
)
