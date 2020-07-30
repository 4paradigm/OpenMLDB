from setuptools import setup
setup(
        name='rtidb',
        version='2.0.0.0',
        description='This is a rtidb python client',
        author='kongquan',
        author_email='kongquan@4paradigm.com',
        url='https://www.4paradigm.com',
        packages=['rtidb'],
        package_data={'': ['_interclient.so', '_interclient_tools.so']},
        include_package_data=True,
)
