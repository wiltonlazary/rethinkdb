#!/bin/bash

set -eu

cd "$(dirname "$0")/.."

# if [[ ! -e RethinkDB.vcxproj.xml ]]; then
    cp mk/RethinkDB.vcxproj.xml .
# fi

cscript mk/gen-vs-project.js
