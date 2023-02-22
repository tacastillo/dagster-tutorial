from setuptools import find_packages, setup

setup(
    name="tutorial",
    packages=find_packages(exclude=["tutorial_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "pandas",
        "matplotlib",
        "wordcloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
