from setuptools import setup

VERSION = "1.3.1"

setup(
    name="python-openetl",
    version=VERSION,
    author="Tom McCall",
    author_email="thomas.e.mccall@gmail.com",
    packages=["pyopenetl"],
    license="LICENSE",
    url="https://github.com/tmccall8829/python-openetl",
    description="A simple python library that makes it easy to create simple ETL pipelines in code.",
    long_description=open("README.md").read(),
    install_requires=[
        "pandas",
        "pyarrow",
        "google-cloud-bigquery",
        "google-cloud-secret-manager",
        "sqlalchemy-bigquery",
    ],
)
