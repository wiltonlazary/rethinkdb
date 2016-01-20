#!/bin/bash

# This script builds RethinkDB using VC++ from the cygwin command line

set -eu
set -o pipefail

MSBUILD=/cygdrive/c/Program\ Files\ \(x86\)/MSBuild/14.0/Bin/MSBuild.exe

format_output () {
    perl -ne '
      s|([a-zA-Z]):\\|/cygdrive/\1/|g;          # convert to cygwin path
      s|\\|/|g;                                 # mirror slashes
      s|( [^ ]+\.cc){10,}| ...|;                # do not list all cc files on a single line
      print unless m{^         .*/[0-9]/.*\.obj.?$}' # do not list all object files when linking
}

if [[ ! -e RethinkDB.vcxproj ]]; then
    mk/gen-vs-project.sh
fi

"$MSBUILD" /p:Configuration=Debug /p:PlatformToolset=v140 /property:Platform=Win32 /maxcpucount "$@" RethinkDB.vcxproj | format_output
