# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
    license="copyright 4paradigm.com",
    packages=['openmldb_provider', 'openmldb_provider.hooks',
              'openmldb_provider.operators'],
    install_requires=['apache-airflow>=2.0'],
    setup_requires=['setuptools', 'wheel'],
    author='Huang Wei',
    author_email='huangwei@apache.org',
    url='https://github.com/4paradigm/OpenMLDB',
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires='~=3.7',
)
