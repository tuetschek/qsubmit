from setuptools import setup


setup(
    name='qsubmit',
    version='0.0.3dev',
    description='Batch engine job submission wrapper',
    author='Ondrej Dusek',
    author_email='odusek@ufal.mff.cuni.cz',
    url='https://github.com/tuetschek/qsubmit',
    download_url='https://github.com/tuetschek/qsubmit.git',
    license='Apache 2.0',
    scripts=['bin/qsubmit'],
    packages=find_packages(),
)

