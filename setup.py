from setuptools import setup,find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="anime-recommendation-system",
    version="0.1",
    author="Jay Rathod",
    packages=find_packages(),
    install_requires = requirements,
)