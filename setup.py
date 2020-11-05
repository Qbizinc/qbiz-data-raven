import setuptools


with open("README.md", 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name="qbiz-data-raven",
    version="1.0.0",
    author="Alex Broley",
    author_email="alex@qbizinc.com",
    description="A Python framework for building data quality tests",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Qbizinc/qbiz-data-raven",
    packages=setuptools.find_packages(),
    data_files=None,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    python_requires=">=3.6",
    include_package_data=True
)
