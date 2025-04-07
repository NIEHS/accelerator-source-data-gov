from setuptools import setup, find_packages

setup(
    name="accelerator-source-data-gov",
    version="0.1.0",
    description="accelerator source data.gov ingest",
    author="Mike Conway",
    author_email="mike.conway@nih.gov",
    url="https://github.com/yourusername/accelerator-source-data-gov",
    packages=find_packages(),
    install_requires=[open("requirements.txt").read()],
    license="BSD 3-Clause",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
