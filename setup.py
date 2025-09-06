from setuptools import setup, find_packages
from setuptools_rust import Binding, RustExtension

setup(
    name="dataprof",
    version="0.3.1",
    author="Andrea Bozzo",
    author_email="andreabozzo92@gmail.com",
    description="A fast, lightweight CLI tool for CSV data profiling and analysis",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/AndreaBozzo/dataprof",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    rust_extensions=[
        RustExtension(
            "dataprof.dataprof",
            binding=Binding.PyO3,
            path="Cargo.toml",
            features=["python"],
        )
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Rust",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
    keywords="csv data analysis profiling rust performance",
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "black",
            "flake8",
        ],
    },
    zip_safe=False,
    include_package_data=True,
)
