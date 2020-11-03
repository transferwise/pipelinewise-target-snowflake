#!/usr/bin/env python

from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="pipelinewise-target-snowflake",
    version="1.1.0",
    description="Singer.io target for loading data to Snowflake - PipelineWise compatible",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="TransferWise",
    url="https://github.com/transferwise/pipelinewise-target-snowflake",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
    ],
    py_modules=["target_snowflake"],
    install_requires=[
        "idna==2.8",
        "singer-python==5.9.0",
        "snowflake-connector-python==2.1.1",
        "boto3==1.10.45",
        "botocore==1.13.45",
        "inflection==0.3.1",
        "joblib==0.14.1",
    ],
    entry_points="""
          [console_scripts]
          target-snowflake=target_snowflake:main
      """,
    packages=["target_snowflake"],
    package_data={},
    include_package_data=True,
)
