from setuptools import setup, find_packages


setup(
    name='qsubmit',
    version='0.0.2dev',
    description='Batch engine job submission wrapper',
    author='Ondrej Dusek',
    author_email='o.dusek@hw.ac.uk',
    url='https://github.com/tuetschek/qsubmit',
    download_url='https://github.com/tuetschek/qsubmit.git',
    license='Apache 2.0',
    scripts=['bin/qsubmit'],
    packages=find_packages(),
)

