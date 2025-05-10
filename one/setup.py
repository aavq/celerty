from setuptools import setup, find_packages

setup(
    name='one',
    version='0.0.1',
    description='one',
    author='one',
    author_email='one@gmail.com',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'one=one.main:main',
        ],
    },
    install_requires=[
        'aiohttp==3.11.18',
        'tenacity==9.1.2',
    ],
    python_requires='>=3.7',
)
