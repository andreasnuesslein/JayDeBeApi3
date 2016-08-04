from setuptools import setup

setup(
    name='JayDeBeApi3',
    version='1.3',
    packages=['jaydebeapi'],
    package_dir={'': 'src'},
    url='https://github.com/nutztherookie/JayDeBeApi3',
    license='LGPL',
    author='Andreas Nüßlein',
    author_email='andreas.nuesslein@amnesty.de',
    install_requires=[
      'py4j<0.10',
    ],
    description='JDBC API for Python3',
    classifiers= [
        'Intended Audience :: Developers',
        'Programming Language :: Java',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
    ]
)
