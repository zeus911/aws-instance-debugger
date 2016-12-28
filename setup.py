#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='aws-instance-debugger',
    version='0.3.5',
    description='CLI to debug AWS instances',
    packages=find_packages(),
    license='MIT',
    author='Marcos Araujo Sobrinho',
    author_email='marcos.sobrinho@vivareal.com.br',
    url='https://github.com/VivaReal/aws-instance-debugger/',
    download_url='https://pypi.python.org/pypi/aws-instance-debugger',
    keywords=['aws', 'boto3', 'ping', 'logs', 'processes', 'amazon web services'],
    long_description=open('README').read(),
    scripts=['vivareal/aws_instance_debugger.py'],
    install_requires=open('requirements.txt').read().strip('\n').split('\n')
)
