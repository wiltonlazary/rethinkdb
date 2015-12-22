#!/usr/bin/env bash

# Fetch and build an npm package

# Expects npm_package to be set

npm_conf=$(niceabspath "$conf_dir/npm.conf")

npm () {
    cache_dir=$(mktemp -d "$external_dir/npm_cache-XXXXXXXX")
    "$NPM" --userconfig "$npm_conf" --cache "$cache_dir" "$@"
    rm -rf "$cache_dir"
}

package_json () {
    cat <<EOF
{ "name": "packed-$npm_package",
  "version": "$version",
  "dependencies": { "$npm_package": "$version" } }
EOF
}

pkg_fetch () {
    pkg_make_tmp_fetch_dir
    cp "$pkg_dir/$npm_package.shrinkwrap" "$tmp_dir/npm-shrinkwrap.json"
    package_json > "$tmp_dir/package.json"
    mkdir "$tmp_dir/node_modules"
    in_dir "$tmp_dir" npm install --ignore-scripts
    mkdir "$src_dir"
    mv "$tmp_dir/node_modules" "$src_dir"
    pkg_remove_tmp_fetch_dir
}

pkg_shrinkwrap () {
    pkg_make_tmp_fetch_dir
    package_json > "$tmp_dir/package.json"
    mkdir "$tmp_dir/node_modules"
    in_dir "$tmp_dir" npm install --ignore-scripts
    in_dir "$tmp_dir" npm shrinkwrap
    cp "$tmp_dir/npm-shrinkwrap.json" "$pkg_dir/$npm_package.shrinkwrap"
    pkg_remove_tmp_fetch_dir
}

pkg_install () {
    rm -rf "$install_dir" || : 2>/dev/null
    mkdir -p "$install_dir"
    cp -a "$src_dir/node_modules" "$install_dir"
    in_dir "$install_dir" npm rebuild

    local bin_dir="$install_dir/node_modules/.bin/"
    mkdir -p "$install_dir/bin"
    for bin in "$bin_dir"*; do
        local name
        name=$(basename "$bin")
        local script="$install_dir/bin/$name"
        echo '#!/bin/sh' > "$script"
        echo "export PATH='$(dirname "$NPM")':"'$PATH' >> "$script"
        echo "export NODE_PATH='$install_dir/node_modules'" >> "$script"
        echo "exec $(niceabspath "$bin")"' "$@"' >> "$script"
        chmod +x "$script"
    done
}

pkg_depends () {
    if will_fetch node; then
        echo node
    fi
}
