from setuptools import find_packages, setup

setup(
    name="meu_projeto_dagster",
    packages=find_packages(exclude=["meu_projeto_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
