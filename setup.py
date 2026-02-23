from setuptools import setup, find_packages

with open('VERSION', 'r') as f:
    version = f.read().strip()

setup(
    name='sqlite-gsheet',
    version=version,
    packages=find_packages(),
    url='https://github.com/taylorhickem/sqlite-gsheet.git',
    description='add-on utility for simple python apps to use sqlite as storage and google sheets as user interface',
    author='@taylorhickem',
    long_description=open('README.md').read(),
    install_requires=open("requirements.txt", "r").read().splitlines(),
    include_package_data=True
)
