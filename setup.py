#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-snowflake",
      version="1.14.1",
      description="Singer.io target for loading data to Snowflake - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Wise",
      url='https://github.com/transferwise/pipelinewise-target-snowflake',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_snowflake"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'snowflake-connector-python[pandas]==2.4.6',
          'inflection==0.5.1',
          'joblib==1.1.0',
          'numpy<1.21.0',
          'python-dateutil==2.8.2'
      ],
      extras_require={
          "test": [
              "pylint==2.11.1",
              'pytest==6.2.5',
              'pytest-cov==3.0.0',
              "python-dotenv==0.19.1"
          ]
      },
      entry_points="""
          [console_scripts]
          target-snowflake=target_snowflake:main
      """,
      packages=find_packages(exclude=['tests*']),
      )
