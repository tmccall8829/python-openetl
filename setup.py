from setuptools import setup

VERSION = "1.0.3"

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
        "pandas >= 1.4.2",
        "pyarrow >= 7.0.0",
        "google-cloud-bigquery >= 3.0.1",
        "google-cloud-secret-manager >= 2.9.2",
        "heroku3 >= 5.1.4",
        "pg8000 >= 1.29.1",
        "marshmallow-sqlalchemy >= 0.26.1",
    ],
)
