#!/usr/bin/env python
# Copyright 2014 RethinkDB, all rights reserved.

'''This test randomly rebalances tables and shards to probabilistically find bugs in the system.'''

from __future__ import print_function

import pprint, os, sys, time, random, threading, itertools, bisect, string

startTime = time.time()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, 'common')))
import driver, scenario_common, utils, vcoptparse

opts = vcoptparse.OptParser()
scenario_common.prepare_option_parser_mode_flags(opts)
opts['random-seed'] = vcoptparse.FloatFlag('--random-seed', random.random())
opts['num-tables'] = vcoptparse.IntFlag('--num-tables', 6) # Number of tables to create
opts['table-scale'] = vcoptparse.IntFlag('--table-scale', 7) # Factor of increasing table size
opts['duration'] = vcoptparse.IntFlag('--duration', 120) # Time to perform fuzzing in seconds
opts['ignore-timeouts'] = vcoptparse.BoolFlag('--ignore-timeouts') # Ignore table_wait timeouts and continue
opts['progress'] = vcoptparse.BoolFlag('--progress') # Write messages every 10 seconds with the time remaining
parsed_opts = opts.parse(sys.argv)
_, command_prefix, serve_options = scenario_common.parse_mode_flags(parsed_opts)

r = utils.import_python_driver()
dbName, tableName = utils.get_test_db_table()

num_threads = 1
data_lock = threading.Lock()
server_names = [ 'War' ]
dbs = set()
tables = set()
indexes = set()

class Db:
    def __init__(self, name):
        self.name = name
        self.tables = set()
        dbs.add(self)
    def unlink(self):
        for table in list(self.tables):
            table.unlink()
        dbs.remove(self)

class Table:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.indexes = set()
        self.count = 0
        db.tables.add(self)
        tables.add(self)
        self.indexes.add(Index(self, 'id'))
    def unlink(self):
        for index in list(self.indexes):
            index.unlink()
        self.db.tables.remove(self)
        tables.remove(self)

class Index:
    def __init__(self, table, name):
        self.table = table
        self.name = name
        table.indexes.add(self)
        indexes.add(self)
    def unlink(self):
        self.table.indexes.remove(self)
        indexes.remove(self)

def make_name():
    return ''.join(random.choice(string.ascii_lowercase) for i in xrange(4))

def rand_shards():
    return random.randint(1, 16)

def rand_replicas():
    return random.randint(1, len(server_names))

def rand_db():
    return random.sample(dbs, 1)[0]

def rand_table():
    return random.sample(tables, 1)[0]

def rand_index():
    return random.sample(indexes, 1)[0]

def weighted_random(weighted_ops):
    ops, weights = zip(*weighted_ops)
    distribution = list(accumulate(weights))

    chosen_weight = random.random() * distribution[-1]
    return ops[bisect.bisect(distribution, chosen_weight)]

def run_random_query(conn, weighted_ops):
    try:
        data_lock.acquire()
        try:
            weighted_ops = [x for x in weighted_ops if x[0].is_valid()]
            op_type = weighted_random(weighted_ops)
            op = op_type()
        finally:
            data_lock.release()

        print('Running op of type: %s' % str(op_type))
        op.run_query(conn)

        data_lock.acquire()
        try:
            op.post_run()
        finally:
            data_lock.release()
    except r.ReqlRuntimeError as ex:
        print('Exception: %s' % repr(ex))

class Query:
    @staticmethod
    def is_valid():
        return True
    def run_query(self, conn):
        self.sub_query(r, conn)
    def post_run(self):
        pass

class DbQuery:
    @staticmethod
    def is_valid():
        return len(dbs) > 0
    def __init__(self):
        self.db = rand_db()
    def run_query(self, conn):
        self.sub_query(r.db(self.db.name), conn)
    def post_run(self):
        pass

class TableQuery:
    @staticmethod
    def is_valid():
        return len(tables) > 0
    def __init__(self):
        self.table = rand_table()
    def run_query(self, conn):
        self.sub_query(r.db(self.table.db.name).table(self.table.name), conn)
    def post_run(self):
        pass

class IndexQuery:
    @staticmethod
    def is_valid():
        return len(indexes) > 0
    def __init__(self):
        self.index = rand_index()
    def run_query(self, conn):
        self.sub_query(r.db(self.index.table.db.name).table(self.index.table.name), conn)
    def post_run(self):
        pass

# No requirements
class db_create(Query):
    def __init__(self):
        self.db = Db(make_name())
    def sub_query(self, q, conn):
        q.db_create(self.db.name).run(conn)

# Requires a DB
class db_drop(DbQuery):
    def run_query(self, conn):
        r.db_drop(self.db.name).run(conn)
    def post_run(self):
        self.db.unlink()

class table_create(DbQuery):
    def __init__(self):
        DbQuery.__init__(self)
        self.table = Table(self.db, make_name())
    def sub_query(self, q, conn):
        q.table_create(self.table.name).run(conn)

# Requires a Table

class wait(TableQuery):
    def sub_query(self, q, conn):
        wait_for = random.choice(['all_replicas_ready',
                                  'ready_for_writes',
                                  'ready_for_reads',
                                  'ready_for_outdated_reads'])
        q.wait(wait_for=wait_for, timeout=30).run(conn)

class reconfigure(TableQuery):
    def sub_query(self, q, conn):
        q.reconfigure(shards=rand_shards(), replicas=rand_replicas()).run(conn)

class rebalance(TableQuery):
    def sub_query(self, q, conn):
        q.rebalance().run(conn)

class config_update(TableQuery):
    def sub_query(self, q, conn):
        shards = []
        for i in xrange(rand_shards()):
            shards.append({'replicas': random.sample(server_names, rand_replicas())})
            shards[-1]['primary_replica'] = random.choice(shards[-1]['replicas'])
        q.config().update({'shards': shards}).run(conn)

class insert(TableQuery):
    def __init__(self):
        TableQuery.__init__(self)
        self.start = self.table.count
        self.num_rows = random.randint(1, 500)
        self.table.count = self.start + self.num_rows
    def sub_query(self, q, conn):
        res = q.insert(r.range(self.start, self.start + self.num_rows).map(lambda x: {'id': x})).run(conn)

class table_drop(TableQuery):
    def sub_query(self, q, conn):
        q.config().delete().run(conn)
    def post_run(self):
        self.table.unlink()

class index_create(TableQuery):
    def __init__(self):
        TableQuery.__init__(self)
        self.index = Index(self.table, make_name())
    def sub_query(self, q, conn):
        q.index_create(self.index.name).run(conn)

# Requires an Index
class index_drop(IndexQuery):
    def sub_query(self, q, conn):
        q.index_drop(self.index.name).run(conn)
    def post_run(self):
        self.index.unlink()

#class index_rename(IndexQuery):
#    def __init__(self):
#        IndexQuery.__init__(self)
#        self.old_name = self.index.name
#        self.index.name = make_name()
#    def sub_query(self, q, conn):
#        q.index_rename(self.old_name, self.index.name).run(conn)

class changefeed(IndexQuery):
    def thread_fn(self, host, port):
        try:
            conn = r.connect(host, port)
            feed = r.db(self.index.table.db.name) \
                    .table(self.index.table.name) \
                    .between(r.minval, r.maxval, index=self.index.name) \
                    .changes().run(conn)
        except Exception as ex:
            print('feed exception: %s' % repr(ex))
    def sub_query(self, q, conn):
        feed_thread = threading.Thread(target=self.thread_fn, args=(conn.host, conn.port))
        feed_thread.start()


def accumulate(iterable):
    it = iter(iterable)
    total = next(it)
    yield total
    for element in it:
        total = total + element
        yield total

def do_fuzz(cluster, stop_event, random_seed):
    random.seed(random_seed)
    weighted_ops = [(db_create, 2),
                    (db_drop, 1),
                    (table_create, 4),
                    (table_drop, 3),
                    (index_create, 8),
                    (index_drop, 2), #(index_rename, 4),
                    (insert, 100),
                    (rebalance, 10),
                    (reconfigure, 10),
                    (config_update, 10),
                    (changefeed, 10),
                    (wait, 10)]

    try:
        server = random.choice(list(cluster.processes))
        conn = r.connect(server.host, server.driver_port)

        while not stop_event.is_set():
            run_random_query(conn, weighted_ops)
        
    finally:
        stop_event.set()

print("Spinning up %d servers (%.2fs)" % (len(server_names), time.time() - startTime))
with driver.Cluster(initial_servers=server_names, output_folder='.', command_prefix=command_prefix,
                    extra_options=serve_options, wait_until_ready=True) as cluster:
    cluster.check()
    random.seed(parsed_opts['random-seed'])

    print("Server driver ports: %s" % (str([x.driver_port for x in cluster])))
    print("Fuzzing shards for %ds, random seed: %s (%.2fs)" %
          (parsed_opts['duration'], repr(parsed_opts['random-seed']), time.time() - startTime))
    stop_event = threading.Event()
    fuzz_threads = []
    for i in xrange(num_threads):
        fuzz_threads.append(threading.Thread(target=do_fuzz, args=(cluster, stop_event, random.random())))
        fuzz_threads[-1].start()

    last_time = time.time()
    end_time = last_time + parsed_opts['duration']
    while (time.time() < end_time) and not stop_event.is_set():
        # TODO: random disconnections / kills during fuzzing
        time.sleep(0.2)
        current_time = time.time()
        if parsed_opts['progress'] and int((end_time - current_time) / 10) < int((end_time - last_time) / 10):
            print("%ds remaining (%.2fs)" % (int(end_time - current_time) + 1, time.time() - startTime))
        last_time = current_time
        if not all([x.is_alive() for x in fuzz_threads]):
            stop_event.set()

    print("Stopping fuzzing (%d of %d threads remain) (%.2fs)" % (len(fuzz_threads), num_threads, time.time() - startTime))
    stop_event.set()
    for thread in fuzz_threads:
        thread.join()

    print("Cleaning up (%.2fs)" % (time.time() - startTime))
print("Done. (%.2fs)" % (time.time() - startTime))

