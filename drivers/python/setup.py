# Copyright 2010-2015 RethinkDB, all rights reserved.

import setuptools
import rethinkdb

conditionalPackages = []
try:
    import asyncio
    conditionalPackages = ['rethinkdb.asyncio']
except ImportError: pass

setuptools.setup(
    name="rethinkdb",
    zip_safe=True,
    version=rethinkdb.__version__,
    description="Python driver library for the RethinkDB database server.",
    url="http://rethinkdb.com",
    maintainer="RethinkDB Inc.",
    maintainer_email="bugs@rethinkdb.com",
    packages=['rethinkdb', 'rethinkdb.tornado', 'rethinkdb.twisted', 'rethinkdb.backports.ssl_match_hostname'] + conditionalPackages,
    package_dir={'rethinkdb':'rethinkdb'},
    package_data={ 'rethinkdb':['backports/ssl_match_hostname/*.txt'] },
    entry_points={
        'console_scripts':[
            'rethinkdb-import = rethinkdb._import:main',
            'rethinkdb-dump = rethinkdb._dump:main',
            'rethinkdb-export = rethinkdb._export:main',
            'rethinkdb-restore = rethinkdb._restore:main',
            'rethinkdb-index-rebuild = rethinkdb._index_rebuild:main'
        ]
    }
)
