// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_PERSIST_V1_16_HPP_
#define CLUSTERING_ADMINISTRATION_PERSIST_V1_16_HPP_

#include <string>

#include "buffer_cache/types.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/persist/file.hpp"
#include "containers/scoped.hpp"
#include "rpc/semilattice/view.hpp"
#include "serializer/types.hpp"

namespace migrate_v1_16 {

// TODO: consider removing structures that haven't changed during migration
//   possibly just `version_t` and `database(s)_semilattice_metadata_t`
class version_t {
public:
    version_t() { }
    branch_id_t branch;
    state_timestamp_t timestamp;
};

RDB_DECLARE_SERIALIZABLE(version_t);

/* `version_range_t` is a pair of `version_t`s. The meta-info, which is stored
   in the superblock of the B-tree, records a `version_range_t` for each range of
   keys. Each key's value is the value it had at some `version_t` in the recorded
   `version_range_t`.

   The reason we store `version_range_t` instead of `version_t` is that if a
   backfill were interrupted, we wouldn't know which keys were up-to-date and
   which were not. All that we would know is that each key's state lay at some
   point between the version the B-tree was at before the backfill started and the
   version that the backfiller is at. */

class version_range_t {
public:
    version_range_t() { }
    version_t earliest, latest;
};

RDB_DECLARE_SERIALIZABLE(version_range_t);

struct branch_birth_certificate_t {
    region_t region;
    state_timestamp_t initial_timestamp;
    region_map_t<version_range_t> origin;
};

RDB_DECLARE_SERIALIZABLE(branch_birth_certificate_t);
RDB_DECLARE_EQUALITY_COMPARABLE(branch_birth_certificate_t);

struct branch_history_t {
    std::map<branch_id_t, branch_birth_certificate_t> branches;
};

RDB_DECLARE_SERIALIZABLE(branch_history_t);

struct server_semilattice_metadata_t {
    versioned_t<name_string_t> name;
    versioned_t<std::set<name_string_t> > tags;
    versioned_t<boost::optional<uint64_t> > cache_size_bytes;
};

RDB_DECLARE_SERIALIZABLE(server_semilattice_metadata_t);

struct servers_semilattice_metadata_t {
    std::map<server_id_t, deletable_t<server_semilattice_metadata_t> > servers;
};

RDB_DECLARE_SERIALIZABLE(servers_semilattice_metadata_t);

struct database_semilattice_metadata_t {
    versioned_t<name_string_t> name;
};

RDB_DECLARE_SERIALIZABLE(database_semilattice_metadata_t);

struct databases_semilattice_metadata_t {
    std::map<database_id_t, deletable_t<database_semilattice_metadata_t> > databases;
};

RDB_DECLARE_SERIALIZABLE(databases_semilattice_metadata_t);

struct write_ack_config_t {
    enum class mode_t { single, majority, complex };
    struct req_t {
        std::set<server_id_t> replicas;
        mode_t mode;   /* must not be `complex` */
    };
    mode_t mode;
    std::vector<req_t> complex_reqs; /* `complex_reqs` must be empty unless `mode` is `complex`. */
};

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(write_ack_config_t::mode_t, int8_t,
                                      write_ack_config_t::mode_t::single,
                                      write_ack_config_t::mode_t::complex);
RDB_DECLARE_SERIALIZABLE(write_ack_config_t::req_t);
RDB_DECLARE_SERIALIZABLE(write_ack_config_t);

struct table_config_t {
    struct shard_t {
        std::set<server_id_t> replicas;
        server_id_t primary_replica;
    };
    std::vector<shard_t> shards;
    write_ack_config_t write_ack_config;
    write_durability_t durability;
};

RDB_DECLARE_SERIALIZABLE(table_config_t);
RDB_DECLARE_SERIALIZABLE(table_config_t::shard_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_config_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_config_t::shard_t);

struct table_shard_scheme_t {
    std::vector<store_key_t> split_points;

    size_t num_shards() const;
    key_range_t get_shard_range(size_t i) const;
    size_t find_shard_for_key(const store_key_t &key) const;

    static table_shard_scheme_t one_shard();
};

RDB_DECLARE_SERIALIZABLE(table_shard_scheme_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_shard_scheme_t);

struct table_replication_info_t {
    table_config_t config;
    table_shard_scheme_t shard_scheme;
};

RDB_DECLARE_SERIALIZABLE(table_replication_info_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_replication_info_t);

struct namespace_semilattice_metadata_t {
    versioned_t<name_string_t> name;
    versioned_t<database_id_t> database;
    versioned_t<std::string> primary_key;
    versioned_t<table_replication_info_t> replication_info;
};

RDB_DECLARE_SERIALIZABLE(namespace_semilattice_metadata_t);

struct namespaces_semilattice_metadata_t {
    std::map<namespace_id_t, deletable_t<namespace_semilattice_metadata_t> > namespaces;
};

RDB_DECLARE_SERIALIZABLE(namespaces_semilattice_metadata_t);

struct cluster_semilattice_metadata_t {
    cluster_semilattice_metadata_t() { }

    namespaces_semilattice_metadata_t rdb_namespaces;
    servers_semilattice_metadata_t servers;
    databases_semilattice_metadata_t databases;
};

RDB_DECLARE_SERIALIZABLE(cluster_semilattice_metadata_t);

void migrate_cluster_metadata(
    txn_t *txn,
    buf_parent_t buf_parent,
    const void *old_superblock,
    metadata_file_t::write_txn_t *destination);

void migrate_auth_file(
    const serializer_filepath_t &path,
    metadata_file_t::write_txn_t *destination);

} // namespace migrate_v1_16

#endif /* CLUSTERING_ADMINISTRATION_PERSIST_V1_16_HPP_ */
