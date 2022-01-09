from setuptools import setup, find_packages

setup(
    name='sqlite-gsheet',
    packages=find_packages(),
    url='https://github.com/taylorhickem/sqlite-gsheet.git',
    description='add-on utility for simple python apps to use sqlite as storage and google sheets as user interface',
    long_description=open('README.md').read(),
    #install_requires=open('requirements.txt').read(), convert it to list
    install_requires=[
        "google-api-core==1.24.1",
        "google-api-python-client==1.12.8",
        "google-auth==1.24.0",
        "google-auth-httplib2==0.0.4",
        "greenlet==1.1.2",
        "httplib2==0.20.2",
        "importlib-metadata==4.8.3",
        "numpy==1.19.5",
        "oauth2client==4.1.3",
        "pandas==1.1.5",
        "pyasn1==0.4.8",
        "pyasn1-modules==0.2.8",
        "pyparsing==3.0.6",
        "python-dateutil==2.8.2",
        "pytz==2021.3",
        "rsa==4.8",
        "six==1.16.0",
        "SQLAlchemy==1.4.29",
        "typing_extensions==4.0.1",
        "zipp==3.6.0",
        "requests>=2.7.0"
        ],
    include_package_data=True,
)