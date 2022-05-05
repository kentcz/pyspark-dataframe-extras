from setuptools import setup, find_packages

setup(
    name="pyspark_dataframe_extras",
    description='pyspark extras',
    version="0.0.1",
    packages=find_packages(),
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "pyspark"],
    install_requires=[
    ]
)
