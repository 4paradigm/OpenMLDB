from setuptools import setup

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
        "sqlalchemy"
    ],
    packages=["sqlalchemy_fedb"],
    entry_points={
        'sqlalchemy.dialects': [
            'fedb = sqlalchemy_fedb.safedb:FeDBDialect',
        ],
    },
)
