import rethinkdb as r
import json

conn = r.connect(port=28022)
ret = conn.server()
print json.dumps(ret)
