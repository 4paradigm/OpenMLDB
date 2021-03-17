from setuptools import setup, find_packages

setup(
    name='sqlalchemy-fedb',
    version='1.3.0',
    author='fedb Team',
    author_email=' g_featureengine@4paradigm.com',
    url='https://www.4paradigm.com',
    description='feDB adapter for SQLAlchemy',
    license="copyright 4paradigm.com",
    classifiers=[
        'Programming Language :: Python :: 3',
        ],
    install_requires=[
        "sqlalchemy < 1.4.0"
    ],
    include_package_data=True,
    package_data = {'':['*.so']},
    packages=find_packages(),
    entry_points={
        'sqlalchemy.dialects': [
            'fedb = sqlalchemy_fedb.safedb:FeDBDialect',
        ],
    },
)
