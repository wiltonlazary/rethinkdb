// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_PERSIST_MIGRATE_MIGRATE_V2_1_HPP_
#define CLUSTERING_ADMINISTRATION_PERSIST_MIGRATE_MIGRATE_V2_1_HPP_

#include "clustering/administration/persist/file.hpp"

// This function is used to migrate metadata from v2.1 to the v2.2 format
void migrate_cluster_metadata_from_v2_1_to_v2_2(metadata_file_t::write_txn_t *out,
                                                signal_t *interruptor);

#endif /* CLUSTERING_ADMINISTRATION_PERSIST_MIGRATE_MIGRATE_V2_1_HPP_ */
