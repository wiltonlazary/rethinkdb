// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/persist/migrate/migrate_v2_1.hpp"

#include <string>

#include "clustering/administration/persist/file_keys.hpp"
#include "clustering/administration/persist/raft_storage_interface.hpp"

template <typename T>
void rewrite_values(const metadata_file_t::key_t<T> &prefix,
                    metadata_file_t::write_txn_t *txn,
                    signal_t *interruptor) {
    txn->read_many<T, cluster_version_t::v2_1>(prefix,
        [&] (const std::string &key_suffix, const T &value) {
            txn->write(prefix.suffix(key_suffix), value, interruptor);
        }, interruptor);
}

void migrate_cluster_metadata_from_v2_1_to_v2_2(metadata_file_t::write_txn_t *txn,
                                                signal_t *interruptor) {
    rewrite_values(mdprefix_table_raft_header(), txn, interruptor);
    rewrite_values(mdprefix_table_raft_snapshot(), txn, interruptor);
    rewrite_values(mdprefix_table_raft_log(), txn, interruptor);
}
