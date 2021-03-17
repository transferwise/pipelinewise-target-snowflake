#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-snowflake",
      version="1.11.0",
      description="Singer.io target for loading data to Snowflake - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      url='https://github.com/transferwise/pipelinewise-target-snowflake',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_snowflake"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'snowflake-connector-python[pandas]==2.3.10',
          'inflection==0.5.1',
          'joblib==1.0.1',
          'numpy<1.20.0',
          'python-dateutil==2.8.1'
      ],
      extras_require={
          "test": [
              "mock==4.0.3",
              "pylint==2.7.2",
              'pytest==6.2.2',
              'pytest-cov==2.11.1',
              "python-dotenv==0.15.0"
          ]
      },
      entry_points="""
          [console_scripts]
          target-snowflake=target_snowflake:main
      """,
      packages=find_packages(exclude=['tests*']),
      )
