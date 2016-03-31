#!/usr/bin/env python

import datetime
import os
import random
import re
import sys
import tempfile
import time
import traceback
import unittest
import functools
import socket

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "common"))

import driver
import utils

# -- import the rethinkdb driver

r = utils.import_python_driver()

if 'RDB_DRIVER_PORT' in os.environ:
    sharedServerDriverPort = int(os.environ['RDB_DRIVER_PORT'])
    if 'RDB_SERVER_HOST' in os.environ:
        sharedServerHost = os.environ['RDB_SERVER_HOST']
    else:
        sharedServerHost = 'localhost'
else:
    raise Exception("RDB_DRIVER_PORT must be set.")
    
# -- test classes

class TestPermissionsBase(unittest.TestCase):
    adminConn = None
    userConn = None
    db = None
    tbl = None

    def assertNoPermissions(self, query):
        try:
            query.run(self.userConn)
            raise Exception("Expected permission error when running %s", query)
        except r.ReqlPermissionError:
            pass

    def assertPermissions(self, query):
        query.run(self.userConn)

    def setPermissions(self, scope, permissions):
        scope.grant("user", permissions).run(self.adminConn)

    def setUp(self):
        self.adminConn = r.connect(host=sharedServerHost, port=sharedServerDriverPort, username="admin", password="")

        # Create a regular user account that we will be using during testing
        res = r.db('rethinkdb').table('users').insert({"id": "user", "password": "secret"}).run(self.adminConn)
        if res != {'skipped': 0, 'deleted': 0, 'unchanged': 0, 'errors': 0, 'replaced': 0, 'inserted': 1}:
            raise Exception('Unable to create user `user`, got: %s' % str(res))

        # Also create a test database and table
        res = r.db_create('perm').pluck("dbs_created").run(self.adminConn)
        if res != {'dbs_created': 1}:
            raise Exception('Unable to create database `perm`, got: %s' % str(res))
        self.db = r.db('perm')
        res = self.db.table_create("test").pluck("tables_created").run(self.adminConn)
        if res != {'tables_created': 1}:
            raise Exception('Unable to create table `test`, got: %s' % str(res))
        self.tbl = self.db.table('test')

        self.userConn = r.connect(host=sharedServerHost, port=sharedServerDriverPort, username="user", password="secret")

    def tearDown(self):
        if self.adminConn is not None:
            r.db('rethinkdb').table('permissions').between(["user"], ["user", r.maxval, r.maxval]).delete().run(self.adminConn)
            r.db('rethinkdb').table('users').get("user").delete().run(self.adminConn)
            r.db_drop('perm').run(self.adminConn)
            self.adminConn.close()
        if self.userConn is not None:
            self.userConn.close()

class TestBasicPermissions(TestPermissionsBase):
    def test_read_scopes(self):
        self.assertNoPermissions(self.tbl)

        self.setPermissions(self.tbl, {"read": True})
        self.assertPermissions(self.tbl)
        self.setPermissions(self.tbl, {"read": None})
        self.assertNoPermissions(self.tbl)

        self.setPermissions(self.db, {"read": True})
        self.assertPermissions(self.tbl)
        self.setPermissions(self.tbl, {"read": False})
        self.assertNoPermissions(self.tbl)
        self.setPermissions(self.tbl, {"read": None})
        self.setPermissions(self.db, {"read": None})
        self.assertNoPermissions(self.tbl)

        self.setPermissions(r, {"read": True})
        self.assertPermissions(self.tbl)
        self.setPermissions(self.tbl, {"read": False})
        self.assertNoPermissions(self.tbl)
        self.setPermissions(self.tbl, {"read": None})
        self.setPermissions(self.db, {"read": False})
        self.assertNoPermissions(self.tbl)
        self.setPermissions(self.db, {"read": None})
        self.setPermissions(r, {"read": None})
        self.assertNoPermissions(self.tbl)

    def test_write(self):
        self.assertNoPermissions(self.tbl.insert({"id": "a"}))
        self.assertNoPermissions(self.tbl.update({"f": "v"}))
        self.assertNoPermissions(self.tbl.replace({"id": "a", "f": "v"}))
        self.assertNoPermissions(self.tbl.delete())

        self.setPermissions(self.tbl, {"read": True})
        self.assertNoPermissions(self.tbl.insert({"id": "a"}))
        # These are ok if the table is empty
        self.assertPermissions(self.tbl.update({"f": "v"}))
        self.assertPermissions(self.tbl.replace({"id": "a", "f": "v"}))
        self.assertPermissions(self.tbl.delete())
        # ... but not if they actually have an effect:
        res = self.tbl.insert({"id": "a"}).pluck("inserted").run(self.adminConn)
        if res != {'inserted': 1}:
            raise Exception('Failed to insert data, got: %s' % str(res))
        self.assertNoPermissions(self.tbl.update({"f": "v"}))
        self.assertNoPermissions(self.tbl.replace({"id": "a", "f": "v"}))
        self.assertNoPermissions(self.tbl.delete())
        self.setPermissions(self.tbl, {"read": None})

        self.setPermissions(self.tbl, {"read": True, "write": True})
        self.assertPermissions(self.tbl.insert({"id": "a"}))
        self.assertPermissions(self.tbl.update({"f": "v"}))
        self.assertPermissions(self.tbl.replace({"id": "a", "f": "v"}))
        self.assertPermissions(self.tbl.delete())
        self.setPermissions(self.tbl, {"read": None, "write": None})

    def test_connect(self):
        self.assertNoPermissions(r.http("http://localhost:12345").default(None))

        self.setPermissions(r, {"connect": True})
        self.assertPermissions(r.http("http://localhost:12345").default(None))
        self.setPermissions(r, {"connect": None})

# -- Main function

if __name__ == '__main__':
    print("Running permissions test")
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestBasicPermissions))

    res = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suite)
    if not res.wasSuccessful():
        sys.exit(1)
