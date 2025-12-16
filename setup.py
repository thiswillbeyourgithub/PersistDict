
from setuptools import setup, find_packages

with open("README.md", "r") as readme:
    long_description = readme.read()

setup(
    name="PersistDict",
    version="0.2.14",
    description="Looks like a dict and acts like a dict but is persistent via an LMDB db",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thiswillbeyourgithub/PersistDict",
    packages=find_packages(),

    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    keywords=["dict", "persistence", "persistent", "storage", "lmdb", "db", "compressed", "compression", "metadata", "browniecutter"],
    python_requires=">=3.9",
    install_requires=[
        "lmdb-dict-full >= 1.0.2",
    ],

    entry_points={
        'console_scripts': [
            'PersistDict=PersistDict.__init__:cli_launcher',
        ],
    },
)
