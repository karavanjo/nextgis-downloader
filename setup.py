from setuptools import setup, find_packages
import sys, os

version = '0.1.0'

requires = [
    'BeautifulSoup',
    'requests',
    'shapely',
    'bs4'
]

entry_points = ''

setup(
    name='nextgis-downloader',
    version=version,
    description="",
    long_description="",
    classifiers=[
        "Programming Language :: Python"
    ],
    author='NextGIS',
    author_email='info@nextgis.ru',
    url='',
    keywords='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    entry_points=entry_points
)
