#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-snowflake",
      version="1.6.2",
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
          'idna==2.7',
          'pipelinewise-singer-python==1.*',
          'snowflake-connector-python==2.0.3',
          'boto3==1.10.8',
          'botocore==1.13.8',
          'urllib3==1.24.3',
          'inflection==0.3.1',
          'joblib==0.13.2',
          'python-dateutil==2.8.1'
      ],
      extras_require={
          "test": [
              "nose==1.3.7",
              "mock==3.0.5",
              "pylint==2.4.2"
          ]
      },
      entry_points="""
          [console_scripts]
          target-snowflake=target_snowflake:main
      """,
      packages=["target_snowflake"],
      )
