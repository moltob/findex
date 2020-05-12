from setuptools import setup, find_packages

setup(
    name="findex",
    version="1.0.5",
    description="Hash-based indices of directories.",
    author="Mike Pagel",
    author_email="mike@mpagel.de",
    packages=find_packages(exclude=["tests"]),
    install_requires=["click", "colorama", "daiquiri", "tqdm", "xlsxwriter"],
    include_package_data=True,
    entry_points={"console_scripts": ["findex = findex.cli:cli"]},
)
