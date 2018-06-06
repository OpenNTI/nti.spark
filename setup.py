import codecs
from setuptools import setup
from setuptools import find_packages

entry_points = {
    'console_scripts': [
        "nti_spark_drop_table = nti.spark.scripts.drop_table:main",
        "nti_spark_schema_config = nti.spark.scripts.produce_schema:main"
    ],
}

TESTS_REQUIRE = [
    'fudge',
    'nti.schema',
    'nti.testing',
    'zope.dottedname',
    'zope.testrunner',
]


def _read(fname):
    with codecs.open(fname, encoding='utf-8') as f:
        return f.read()


setup(
    name='nti.spark',
    version=_read('version.txt').strip(),
    author='Jason Madden',
    author_email='jason@nextthought.com',
    description="NTI Spark",
    long_description=(
        _read('README.rst') 
        + '\n\n' 
        + _read("CHANGES.rst")
    ),
    license='Apache',
    keywords='spark',
    classifiers=[
        'Framework :: Zope3',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    url="https://github.com/NextThought/nti.spark",
    zip_safe=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    namespace_packages=['nti'],
    tests_require=TESTS_REQUIRE,
    install_requires=[
        'setuptools',
        'isodate',
        'nti.schema',
        'pyspark',
        'pytz',
        'six',
        'zope.cachedescriptors',
        'zope.component',
        'zope.configuration',
        'zope.dottedname',
        'zope.exceptions',
        'zope.interface',
        'zope.schema',
        'zope.security',
    ],
    extras_require={
        'test': TESTS_REQUIRE,
        'docs': [
            'Sphinx',
            'nti.schema',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
        ],
    },
    entry_points=entry_points,
)
