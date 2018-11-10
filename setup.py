import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="wurmstore",
    version="0.0.1",
    author="Zaphodious",
    author_email="zaphodious@yu-shan.com",
    description="A naive utility for simple entity storage",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/blakwurm/wurmstore",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)