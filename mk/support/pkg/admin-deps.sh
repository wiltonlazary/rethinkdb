full_npm_package=rethinkdb-webui
version=2.0.3

npm_conf=$(niceabspath "$conf_dir/npm.conf")

include npm-pkg.inc

pkg_shrinkwrap () {
    in_dir "$root_dir/admin" npm shrinkwrap
}

pkg_fetch () {
    pkg_make_tmp_fetch_dir
    cp "$root_dir/admin/npm-shrinkwrap.json" "$tmp_dir"
    cp "$root_dir/admin/package.json" "$tmp_dir"
    mkdir "$tmp_dir/node_modules"
    in_dir "$tmp_dir" npm install --ignore-scripts
    rm -rf "$src_dir"
    mkdir -p "$src_dir"
    mv "$tmp_dir"/node_modules "$src_dir"
    pkg_remove_tmp_fetch_dir
}
