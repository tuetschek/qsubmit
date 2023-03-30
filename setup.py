from setuptools import setup, find_packages


setup(
    name='qsubmit',
    version='0.0.4dev',
    description='Batch engine job submission wrapper',
    author='Ondrej Dusek',
    author_email='odusek@ufal.mff.cuni.cz',
    url='https://github.com/ufal/qsubmit',
    download_url='https://github.com/ufal/qsubmit.git',
    license='Apache 2.0',
    scripts=['bin/qsubmit','bin/qruncmd'],
    packages=find_packages(),
)

