from setuptools import setup, find_packages

setup(
    name='ctfix',
    packages=find_packages(),  # this must be the same as the name above
    version='0.22',
    description='Ctrader FIX API',
    author='Dmitry Shabanov',
    author_email='dm.skpd@gmail.com',
    url='https://github.com/Skpd/ctrader-fix-api',  # use the URL to the github repo
    download_url='https://github.com/Skpd/ctrader-fix-api/archive/v0.21.tar.gz',  # I'll explain this in a second
    keywords=['ctrader', 'fix', 'ctfix'],  # arbitrary keywords
    classifiers=[],
)
