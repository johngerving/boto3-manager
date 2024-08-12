from setuptools import setup

setup(
    name='boto3manager',
    version='0.1.3',
    description='A Python package to manage Boto3 file uploads and downloads',
    url='https://gitlab.nrp-nautilus.io/humboldt/boto3-manager.git',
    author='John Gerving',
    author_email='jhg51@humboldt.edu',
    license='MIT',
    packages=['boto3manager'],
    install_requires=[
        'boto3',
        'botocore',
        'tqdm',
        'wakepy'
    ],
    zip_safe=False
)