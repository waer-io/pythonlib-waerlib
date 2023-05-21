from setuptools import setup, find_packages
setup(
    name='waerlib',
    version='0.0.0',
    description='Python project to facilitate reading from or writing to Dremio',
    install_requires=[
        'numpy',
        'pandas',
        'pyarrow',
        'google-cloud-storage'
    ],
)
