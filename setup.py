#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
      long_description = f.read()

setup(name="pipelinewise-target-snowflake",
      version="1.1.0",
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
          'singer-python==5.1.1',
          'snowflake-connector-python==2.0.1',
          'boto3==1.9.33',
          'inflection==0.3.1',
          'joblib==0.13.2'
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
      package_data = {},
      include_package_data=True,
)
