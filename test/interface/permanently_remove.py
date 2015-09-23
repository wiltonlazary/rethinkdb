#!/usr/bin/env python
# Copyright 2010-2015 RethinkDB, all rights reserved.

'''Show the correct behavior when a server is removed and then returned'''

import os, pprint, socket, sys, time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, 'common')))
import driver, scenario_common, utils, vcoptparse

op = vcoptparse.OptParser()
scenario_common.prepare_option_parser_mode_flags(op)
_, command_prefix, server_options = scenario_common.parse_mode_flags(op.parse(sys.argv))

r = utils.import_python_driver()
dbName, _ = utils.get_test_db_table()

utils.print_with_time("Starting servers PrinceHamlet and KingHamlet")
with driver.Cluster(output_folder='.', initial_servers=['PrinceHamlet', 'KingHamlet', 'Gertrude'], command_prefix=command_prefix, extra_options=server_options) as cluster:
    
    prince_hamlet = cluster[0]
    assert prince_hamlet.name == 'PrinceHamlet', prince_hamlet.name
    king_hamlet = cluster[1]
    assert king_hamlet.name == 'KingHamlet', king_hamlet.name
    gertrude = cluster[2]
    assert gertrude.name == 'Gertrude', gertrude.name
    cluster.check()
    
    utils.print_with_time("Establishing ReQL connection")
    conn = r.connect(prince_hamlet.host, prince_hamlet.driver_port)

    utils.print_with_time("Creating three tables")
    
    if dbName not in r.db_list().run(conn):
        r.db_create(dbName).run(conn)
    
    res = r.db("rethinkdb").table("table_config").insert([
        # Fully avalible, no alteration
        {
            "db": dbName,
            "name": "fully available: majority",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "PrinceHamlet",
                "replicas": ["PrinceHamlet", "KingHamlet", "Gertrude"]
                }],
            "write_acks": "majority"
        },
        {
            "db": dbName,
            "name": "fully available: single",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "PrinceHamlet",
                "replicas": ["PrinceHamlet", "KingHamlet", "Gertrude"]
                }],
            "write_acks": "single"
        },
        
        # Fully avalible, change in primary
        {
            "db": dbName,
            "name": "change primary: majority",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "KingHamlet",
                "replicas": ["PrinceHamlet", "KingHamlet", "Gertrude"]
                }],
            "write_acks": "majority"
        },
        {
            "db": dbName,
            "name": "change primary: single",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "KingHamlet",
                "replicas": ["PrinceHamlet", "KingHamlet", "Gertrude"]
                }],
            "write_acks": "single"
        },
        
        # Avalible for outdated reads only
        {
            "db": dbName,
            "name": "outdated reads: majority",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "KingHamlet",
                "replicas": ["PrinceHamlet", "KingHamlet", "Gertrude"]
                }],
            "write_acks": "majority"
        },
        {
            "db": dbName,
            "name": "outdated reads: single",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "KingHamlet",
                "replicas": ["PrinceHamlet", "KingHamlet", "Gertrude"]
                }],
            "write_acks": "single"
        },
        
        # Completely unavailable
        {
            "db": dbName,
            "name": "unavailable: single",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "KingHamlet",
                "replicas": ["KingHamlet"]
                }],
            "write_acks": "single"
        },
        {
            "db": dbName,
            "name": "unavailable: majority",
            "primary_key": "id",
            "shards": [{
                "primary_replica": "KingHamlet",
                "replicas": ["KingHamlet"]
                }],
            "write_acks": "majority"
        }

        ]).run(conn)
    assert res["inserted"] == 3, res
    r.db(dbName).wait().run(conn)
    
    utils.print_with_time("Inserting data into tables")
    
    res = r.db(dbName).table("test").insert([{}]*100).run(conn)
    assert res["inserted"] == 100
    res = r.db(dbName).table("test2").insert([{}]*100).run(conn)
    assert res["inserted"] == 100
    res = r.db(dbName).table("test3").insert([{}]*100).run(conn)
    assert res["inserted"] == 100

    utils.print_with_time("Killing KingHamlet")
    king_hamlet.stop()
    time.sleep(1)
    
    utils.print_with_time("Checking that all three tables show issues")
    issues = list(r.db("rethinkdb").table("current_issues").run(conn))
    assert len(issues) == 3, pprint.pformat(issues)
    
    
    
    
    assert issues[0]["type"] == "server_disconnected", pprint.pformat(issues[0])
    assert issues[0]["critical"], pprint.pformat(issues[0])
    assert "KingHamlet" in issues[0]["description"], pprint.pformat(issues[0])
    assert issues[0]["info"]["disconnected_server"] == "KingHamlet", pprint.pformat(issues[0])
    assert set(issues[0]["info"]["reporting_servers"]) == set(["PrinceHamlet", "Gertrude"]), pprint.pformat(issues[0])
    
    # identifier_format='uuid'
    issues = list(r.db("rethinkdb").table("current_issues", identifier_format='uuid').run(conn))
    assert issues[0]["info"]["disconnected_server"] == king_hamlet.uuid
    assert set(issues[0]["info"]["reporting_servers"]) == set([prince_hamlet.uuid, gertrude.uuid])

    test_status = r.db(dbName).table("test").status().run(conn)
    test2_status = r.db(dbName).table("test2").status().run(conn)
    test3_status = r.db(dbName).table("test3").status().run(conn)
    assert test_status["status"]["ready_for_writes"], test_status
    assert not test_status["status"]["all_replicas_ready"], test_status
    assert test2_status["status"]["ready_for_outdated_reads"], test2_status
    assert not test2_status["status"]["ready_for_reads"], test2_status
    assert not test3_status["status"]["ready_for_outdated_reads"], test3_status

    utils.print_with_time("Permanently removing KingHamlet")
    res = r.db("rethinkdb").table("server_config").filter({"name": "KingHamlet"}).delete().run(conn)
    assert res["deleted"] == 1
    assert res["errors"] == 0

    utils.print_with_time("Checking the issues that were generated")
    issues = list(r.db("rethinkdb").table("current_issues").run(conn))
    assert len(issues) == 2, issues
    if issues[0]["type"] == "data_lost":
        dl_issue, np_issue = issues
    else:
        np_issue, dl_issue = issues

    assert np_issue["type"] == "table_needs_primary"
    assert np_issue["info"]["table"] == "test2"
    assert "no primary replica" in np_issue["description"]

    assert dl_issue["type"] == "data_lost"
    assert dl_issue["info"]["table"] == "test3"
    assert "Some data has probably been lost permanently" in dl_issue["description"]

    test_status = r.db(dbName).table("test").status().run(conn)
    test2_status = r.db(dbName).table("test2").status().run(conn)
    test3_status = r.db(dbName).table("test3").status().run(conn)
    assert test_status["status"]["all_replicas_ready"]
    assert test2_status["status"]["ready_for_outdated_reads"]
    assert not test2_status["status"]["ready_for_reads"]
    assert not test3_status["status"]["ready_for_outdated_reads"]
    assert r.db(dbName).table("test").config()["shards"].run(conn) == [{
        "primary_replica": "PrinceHamlet",
        "replicas": ["PrinceHamlet"]
        }]
    assert r.db(dbName).table("test2").config()["shards"].run(conn) == [{
        "primary_replica": None,
        "replicas": ["PrinceHamlet"]
        }]
    assert r.db(dbName).table("test3").config()["shards"].run(conn) == [{
        "primary_replica": None,
        "replicas": []
        }]

    utils.print_with_time("Testing that having primary_replica=None doesn't break `table_config`")
    # By changing the table's name, we force a write to `table_config`, which tests the
    # code path that writes `"primary_replica": None`.
    res = r.db(dbName).table("test2").config().update({"name": "test2x"}).run(conn)
    assert res["errors"] == 0
    res = r.db(dbName).table("test2x").config().update({"name": "test2"}).run(conn)
    assert res["errors"] == 0
    assert r.db(dbName).table("test2").config()["shards"].run(conn) == [{
        "primary_replica": None,
        "replicas": ["PrinceHamlet"]
        }]

    utils.print_with_time("Fixing table `test2`")
    r.db(dbName).table("test2").reconfigure(shards=1, replicas=1).run(conn)
    r.db(dbName).table("test2").wait().run(conn)

    utils.print_with_time("Fixing table `test3`")
    r.db(dbName).table("test3").reconfigure(shards=1, replicas=1).run(conn)
    r.db(dbName).table("test3").wait().run(conn)

    utils.print_with_time("Bringing the dead server back")
    king_hamlet.start()
    cluster.check()

    utils.print_with_time("Checking that there is an issue")
    issues = list(r.db("rethinkdb").table("current_issues").run(conn))
    assert len(issues) == 1, issues
    assert issues[0]["type"] == "server_ghost"
    assert not issues[0]["critical"]
    assert issues[0]["info"]["server_id"] == king_hamlet.uuid
    assert issues[0]["info"]["hostname"] == socket.gethostname()
    assert issues[0]["info"]["pid"] == king_hamlet.process.pid

    utils.print_with_time("Checking table contents")
    assert r.db(dbName).table("test").count().run(conn) == 100
    assert r.db(dbName).table("test2").count().run(conn) == 100
    assert r.db(dbName).table("test3").count().run(conn) == 0

    utils.print_with_time("Checking that we can reconfigure despite ghost")
    # This is a regression test for GitHub issue #3627
    res = r.db(dbName).table("test").config().update({
        "shards": [
            {
                "primary_replica": "Gertrude",
                "replicas": ["PrinceHamlet", "Gertrude"]
            },
            {
                "primary_replica": "PrinceHamlet",
                "replicas": ["PrinceHamlet", "Gertrude"]
            }]
        }).run(conn)
    assert res["errors"] == 0, res
    res = r.db(dbName).table("test").wait().run(conn)
    assert res["ready"] == 1, res
    st = r.db(dbName).table("test").status().run(conn)
    assert st["status"]["all_replicas_ready"], st

