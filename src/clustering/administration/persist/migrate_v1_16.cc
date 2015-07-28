// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/persist/migrate_v1_16.hpp"

#include "buffer_cache/alt.hpp"
#include "buffer_cache/blob.hpp"
#include "clustering/administration/persist/migrate_pre_v1_16.hpp"
#include "clustering/immediate_consistency/history.hpp"

namespace migrate_v1_16 {

table_shard_scheme_t table_shard_scheme_t::one_shard() {
    return table_shard_scheme_t();
}

size_t table_shard_scheme_t::num_shards() const {
    return split_points.size() + 1;
}

key_range_t table_shard_scheme_t::get_shard_range(size_t i) const {
    guarantee(i < num_shards());
    store_key_t left = (i == 0) ? store_key_t::min() : split_points[i-1];
    if (i != num_shards() - 1) {
        return key_range_t(
                           key_range_t::closed, left,
                           key_range_t::open, split_points[i]);
    } else {
        return key_range_t(
                           key_range_t::closed, left,
                           key_range_t::none, store_key_t());
    }
}

size_t table_shard_scheme_t::find_shard_for_key(const store_key_t &key) const {
    size_t ix = 0;
    while (ix < split_points.size() && key >= split_points[ix]) {
        ++ix;
    }
    return ix;
}

struct cluster_metadata_superblock_t {
    block_magic_t magic;

    server_id_t server_id;

    static const int METADATA_BLOB_MAXREFLEN = 1500;
    char metadata_blob[METADATA_BLOB_MAXREFLEN];

    static const int BRANCH_HISTORY_BLOB_MAXREFLEN = 500;
    char rdb_branch_history_blob[BRANCH_HISTORY_BLOB_MAXREFLEN];
};

struct auth_metadata_superblock_t {
    block_magic_t magic;

    static const int METADATA_BLOB_MAXREFLEN = 1500;
    char metadata_blob[METADATA_BLOB_MAXREFLEN];
};

template <cluster_version_t>
struct cluster_metadata_magic_t {
    static const block_magic_t value;
};

template <>
const block_magic_t
    cluster_metadata_magic_t<cluster_version_t::v1_14>::value
        = { { 'R', 'D', 'm', 'e' } };
template <>
const block_magic_t
    cluster_metadata_magic_t<cluster_version_t::v1_15>::value
        = { { 'R', 'D', 'm', 'f' } };
template <>
const block_magic_t
    cluster_metadata_magic_t<cluster_version_t::v1_16>::value
        = { { 'R', 'D', 'm', 'g' } };
template <>
const block_magic_t
    cluster_metadata_magic_t<cluster_version_t::v2_0>::value
        = { { 'R', 'D', 'm', 'h' } };

cluster_version_t cluster_superblock_version(const cluster_metadata_superblock_t *sb) {
    if (sb->magic == cluster_metadata_magic_t<cluster_version_t::v1_14>::value) {
        return cluster_version_t::v1_14;
    } else if (sb->magic == cluster_metadata_magic_t<cluster_version_t::v1_15>::value) {
        return cluster_version_t::v1_15;
    } else if (sb->magic == cluster_metadata_magic_t<cluster_version_t::v1_16>::value) {
        return cluster_version_t::v1_16;
    } else if (sb->magic == cluster_metadata_magic_t<cluster_version_t::v2_0>::value) {
        return cluster_version_t::v2_0;
    } else {
        fail_due_to_user_error("Migration of cluster metadata could not be performed, it "
                               "is only supported for metadata from v1.14.x and later.");
    }
}

template <cluster_version_t>
struct auth_metadata_magic_t {
    static const block_magic_t value;
};

template <>
const block_magic_t auth_metadata_magic_t<cluster_version_t::v1_14>::value =
    { { 'R', 'D', 'm', 'e' } };

template <>
const block_magic_t auth_metadata_magic_t<cluster_version_t::v1_15>::value =
    { { 'R', 'D', 'm', 'f' } };

template <>
const block_magic_t auth_metadata_magic_t<cluster_version_t::v1_16>::value =
    { { 'R', 'D', 'm', 'g' } };

template <>
const block_magic_t auth_metadata_magic_t<cluster_version_t::v2_0>::value =
    { { 'R', 'D', 'm', 'h' } };

cluster_version_t auth_superblock_version(const auth_metadata_superblock_t *sb) {
    if (sb->magic == auth_metadata_magic_t<cluster_version_t::v1_14>::value) {
        return cluster_version_t::v1_14;
    } else if (sb->magic == auth_metadata_magic_t<cluster_version_t::v1_15>::value) {
        return cluster_version_t::v1_15;
    } else if (sb->magic == auth_metadata_magic_t<cluster_version_t::v1_16>::value) {
        return cluster_version_t::v1_16;
    } else if (sb->magic == auth_metadata_magic_t<cluster_version_t::v2_0>::value) {
        return cluster_version_t::v2_0;
    } else {
        fail_due_to_user_error("Migration of auth metadata could not be performed, it "
                               "is only supported for metadata from v1.14.x and later.");
    }
}

static void read_blob(buf_parent_t parent, const char *ref, int maxreflen,
                      const std::function<archive_result_t(read_stream_t *)> &reader) {
    blob_t blob(parent.cache()->max_block_size(),
                const_cast<char *>(ref), maxreflen);
    blob_acq_t acq_group;
    buffer_group_t group;
    blob.expose_all(parent, access_t::read, &group, &acq_group);
    buffer_group_read_stream_t ss(const_view(&group));
    archive_result_t res = reader(&ss);
    guarantee_deserialization(res, "T (template code)");
}

void migrate_server(const cluster_semilattice_metadata_t &metadata,
                    const server_id_t &server_id,
                    metadata_file_t::write_txn_t *out) {
    auto self_it = metadata.servers.servers.find(server_id);
    guarantee(self_it != metadata.servers.servers.end(),
              "Migration of cluster metadata failed, could not find own server config.");

    server_config_versioned_t new_config;
    new_config.version = 1;
    new_config.config.name = self_it->name.get_ref();
    new_config.config.tags = self_it->tags.get_ref();
    new_config.config.cache_size_bytes = self_it->cache_size_bytes.get_ref();

    out->write(mdkey_server_config(), new_config, nullptr);
    out->write(mdkey_server_id(), server_id, nullptr);
}

void migrate_databases(const cluster_semilattice_metadata_t &metadata,
                       metadata_file_t::write_txn_t *out) {
    ::cluster_semilattice_metadata_t new_metadata;
    for (auto const &pair : metadata.databases.databases) {
        if (!pair->second.is_deleted()) {
            ::database_semilattice_metadata_t db;
            db.name = pair->second.name;
            auto res = new_metadata.databases.insert(
                std::make_pair(pair->first, make_deletable(db)));
            guarantee(res->second);
        } else {
            auto res = new_metadata.databases.insert(
                std::make_pair(pair->first, make_deletable(db)));
            guarantee(res->second);
            res->first->second->mark_deleted();
        }
    }
    out->write(mdkey_cluster_semilattices(), new_metadata, nullptr);
}

region_map_t<version_range_t> to_version_range_map(const region_map_t<binary_blob_t> &blob_map) {
    return blob_map.map(blob_map.get_domain(),
        [&] (const binary_blob_t &b) -> version_range_t {
            return binary_blob_t::get<version_range_t>(b);
        });
}

void migrate_tables(io_backender_t *io_backender,
                    serializer_t *serializer,
                    const base_path_t &base_path,
                    bool erase_inconsistent_data,
                    const cluster_semilattice_metadata_t &metadata,
                    const branch_history_t &branch_history,
                    metadata_file_t::write_txn_t *out) {
    dummy_cache_balancer_t balancer(GIGABYTE);
    auto &tables = metadata.rdb_namespaces.namespaces;
    pmap(tables.begin(), tables.end(), [&] (std::pair<namespace_id_t, namespace_semilattice_metadata_t> &info) {
            if (!info->second.is_deleted()) {
                try {
                    rdb_protocol_t::store_t store(region_t::universe(),
                                                  serializer,
                                                  &balancer,
                                                  uuid_to_str(info->first),
                                                  false,
                                                  &get_global_perfmon_collection(),
                                                  nullptr,
                                                  io_backender,
                                                  base_path,
                                                  scoped_ptr_t<outdated_index_report_t>(),
                                                  info->first);
                    migrate_branch_ids(
                        branch_history,
                        info->first,
                        to_version_range_map(
                            store.get_metainfo(order_token_t::ignore,
                                                &token,
                                                store.get_region(),
                                                nullptr)));

                    migrate_active_table(info->first,
                                         info->second.get(),
                                         metadata.servers,
                                         store.sindex_list(nullptr),
                                         out);
                } catch () {
                    // TODO: how does this fail if the table file is missing?
                    // TODO: do we need to do branch ids if the table isn't active on this server?
                    migrate_inactive_table(info->first, info->second.get(), out);
                }
            } else {
                // Nothing is stored on disk for deleted tables
            }
         });

    // Loop over tables again and rewrite the metainfo blob using version_ts instead of version_range_ts
    // This should be the last thing done because afterwards the table files will be incompatible with previous versions
}

void migrate_branch_ids(const branch_history_t &branch_history,
                        const namespace_id_t &table_id,
                        const region_map_t<version_range_t> &versions,
                        bool erase_inconsistent_data,
                        metadata_file_t::write_txn_t *out) {
    std::set<branch_id_t> seen_branches;
    std::queue<branch_id_t> branches_to_save;
    for (auto const &pair : versions) {
        if (!pair.second.is_coherent()) {
            if (!erase_inconsistent_data) {
                fail_due_to_user_error("retry with flag to erase inconsistent data"); // TODO: better message
            } else {
                // The data is in an unrecoverable state, but there should be coherent
                // data elsewhere in the cluster, this may be reset later.
            }
        } else if (seen_branches.count(pair.second.earliest) == 0) {
            seen_branches.insert(pair.second.earliest);
            branches_to_save.push(pair.second.earliest);
        }
    }

    while (!branches_to_save.empty()) {
        auto branch_it = branch_history.branches.find(branches_to_save.front());
        guarantee(branch_it != branch_history.branches.end());
        branches_to_save.pop();

        region_t region = branch_it->second.get_domain();
        guarantee(branch_it->second.region == region);

        ::branch_birth_certificate_t new_birth_certificate;
        new_birth_certificate.initial_timestamp = branch_it->second.initial_timestamp;
        new_birth_certificate.origin = branch_it->second.origin.map(region,
            [&] (const version_range_t &v) -> ::version_t {
                guarantee(v.is_coherent());
                if (seen_branches.count(v.earliest) == 0) {
                    seen_branches.insert(v.earliest);
                    branches_to_save.push(v.earliest);
                }
                return v.earliest();
            });

        out->write(mdprefix_branch_birth_certificate().suffix(
                       uuid_to_str(table_id) + "/" + uuid_to_str(branch_it->first))
                   new_birth_certificate, nullptr);
    }
}

// This function will use a new uuid rather than the timestamp's tiebreaker because
// we are combining multiple timestamps into one - we could potentially lose changes across
// the cluster.  Rather than have conflicting data in the committed raft log under the same
// epoch (on two different servers), we may instead lose a configuration change.  This should
// only realistically happen if configuration changes were made while the cluster was in the
// process of shutting down before the upgrade.
template <typename ...Args>
multi_table_manager_timestamp_t max_versioned_timestamp(const versioned_t<Args> &v...) {
    std::vector<time_t> times = { v.get_timestamp()... };
    auto it = std::max_element(times.begin(), times.end());
    size_t offset = it - times.begin();

    multi_table_manager_timestamp_t res;
    res.epoch.timestamp = times[offset];
    res.epoch.id = generate_uuid();
    res.log_index = 1; // We will always migrate in exactly one log entry for this epoch
    return res;
}

void migrate_inactive_table(const namespace_id_t &table_id,
                            const namespace_semilattice_metadata_t &table_metadata,
                            metadata_file_t::write_txn_t *out) {
    // We want to stamp this with the maximum timestamp seen in the table's metadata
    table_inactive_persistent_state_t state;
    state.timestamp = max_versioned_timestamp(table_metadata.name,
                                              table_metadata.database,
                                              table_metadata.primary_key);
    state.second_hand_config.name = table_metadata.name.get();
    state.second_hand_config.database = table_metadata.database.get();
    state.second_hand_config.primary_key = table_metadata.primary_key.get();

    out->write(mdprefix_table_inactive().suffix(uuid_to_str(table_id)), state, nullptr);

}

void migrate_active_table(const server_id_t &this_server_id,
                          const namespace_id_t &table_id,
                          const namespace_semilattice_metadata_t &table_metadata,
                          const servers_semilattice_metadata_t &servers_metadata,
                          const std::map<std::string, std::pair<sindex_config_t, sindex_status_t> > &sindexes,
                          metadata_file_t::write_txn_t *out) {
    const table_replication_info_t &old_config = table_metadata.replication_info.get();
    raft_member_id_t own_raft_id(generate_uuid());
    table_active_persistent_state_t active_state;
    active_state.epoch = max_versioned_timestamp(table_metadata.name,
                                                 table_metadata.database,
                                                 table_metadata.primary_key,
                                                 table_metadata.replication_info).epoch;
    active_state.raft_member_id = own_raft_id;
    out->write(mdprefix_table_active().suffix(uuid_to_str(table_id)), active_state, nullptr);

    table_raft_state_t raft_state;
    raft_state.config.config.basic.name = table_metadata.name.get();
    raft_state.config.config.basic.database = table_metadata.database.get();
    raft_state.config.config.basic.primary_key = table_metadata.primary_key.get();
    raft_state.config.config.write_ack_config =
        old_config.config.write_ack_config.mode == write_ack_config_t::mode_t::single ?
            ::write_ack_config_t::SINGLE : ::write_ack_config_t::MAJORITY;
    raft_state.config.config.durability = old_config.durability;
    raft_state.config.shard_scheme.split_points = old_config.shard_scheme.split_points;
    raft_state.config.server_names = ;

    std::set<server_id_t> used_servers;
    for (auto const &s : old_config.config.shards) {
        ::table_config_t::shard_t new_shard;
        new_shard.all_replicas = s.replicas;
        new_shard.primary_replica = s.primary_replica;
        raft_state.config.config.shards.push_back(std::move(new_shard));
        used_servers.insert(s.replicas.begin(), s.replicas.end());
    }

    for (auto const &pair : sindexes) {
        raft_state.config.config.sindexes.insert(std::make_pair(pair.first, pair.second.first));
    }

    for (auto const &serv_id : used_servers) {
        auto serv_it = servers_metadata.servers.find(serv_id);
        if (serv_it != servers_metadata.servers.end() && !serv_it->second.is_deleted()) {
            raft_state.config.server_names.emplace(
                serv_id, std::make_pair(1, serv_it->second.get().name.get()));
        }

        raft_member_id_t new_raft_id(generate_uuid());
        raft_state.member_ids.emplace(
            serv_id,
            serv_id == this_server_id ?
                own_raft_id : raft_member_id_t(generate_uuid()));
    }

    raft_config_t raft_config;
    config.voting_members.insert(own_raft_id)

    raft_persistent_state_t<table_raft_state_t> persistent_state = 
        raft_persistent_state_t<table_raft_state_t>::make_initial(raft_state, raft_config);

    // TODO: Is it necessary to have an initial log entry?
    persistent_state.log.append();

    // The `table_raft_storage_interface_t` constructor will persist the header, snapshot, and logs
    table_raft_storage_interface_t storage_interface(nullptr, out, table_id, persistent_state, nullptr)
}

void migrate_cluster_metadata(io_backender_t *io_backender,
                              serializer_t *serializer,
                              const base_path_t &base_path,
                              bool erase_inconsistent_data,
                              txn_t *txn,
                              buf_parent_t buf_parent,
                              const void *old_superblock,
                              metadata_file_t::write_txn_t *new_output) {
    const cluster_metadata_superblock_t *sb =
        static_cast<const cluster_metadata_superblock_t *>(old_superblock);
    cluster_version_t v = cluster_superblock_version(sb);
    cluster_semilattice_metadata_t metadata;

    if (v == cluster_version_t::v1_14 || v == cluster_version_t::v1_15) {
        pre_v1_16::cluster_semilattice_metadata_t old_metadata;
        read_blob(buf_parent, sb->metadata_blob,
                  cluster_metadata_superblock_t::METADATA_BLOB_MAXREFLEN,
                  [&](read_stream_t *s) -> archive_result_t {
                      switch(v) {
                      case cluster_version_t::v1_14:
                          return deserialize<cluster_version_t::v1_14>(s, &old_metadata);
                      case cluster_version_t::v1_15:
                          return deserialize<cluster_version_t::v1_15>(s, &old_metadata);
                      case cluster_version_t::v1_16:
                      case cluster_version_t::v2_0:
                      case cluster_version_t::v2_1_is_latest:
                      default:
                        unreachable();
                      }
                  });
        metadata = migrate_cluster_metadata_to_v1_16(old_metadata);
    } else if (v == cluster_version_t::v1_16 || v == cluster_version_t::v2_0) {
        read_blob(buf_parent, sb->metadata_blob,
                  cluster_metadata_superblock_t::METADATA_BLOB_MAXREFLEN,
                  [&](read_stream_t *s) -> archive_result_t {
                      switch(v) {
                      case cluster_version_t::v1_16:
                          return deserialize<cluster_version_t::v1_16>(s, &metadata);
                      case cluster_version_t::v2_0:
                          return deserialize<cluster_version_t::v2_0>(s, &metadata);
                      case cluster_version_t::v1_14:
                      case cluster_version_t::v1_15:
                      case cluster_version_t::v2_1_is_latest:
                      default:
                        unreachable();
                      }
                  });
    } else {
        unreachable();
    }

    branch_history_t branch_history;
    read_blob(buf_parent, sb->rdb_branch_history_blob,
              cluster_metadata_superblock_t::BRANCH_HISTORY_BLOB_MAXREFLEN,
              [&](read_stream_t *s) -> archive_result_t {
                  return deserialize_for_version(v, s, &branch_history);
              });

    migrate_server(sb->server_id, metadata, new_output);
    migrate_databases(metadata, new_output);
    migrate_tables(io_backender, serializer, base_path, erase_inconsistent_data, metadata, branch_history, new_output);
}

void migrate_auth_file(io_backender_t *io_backender,
                       const serializer_filepath_t &path,
                       metadata_file_t::write_txn_t *destination) {
    perfmon_collection_t dummy_stats;
    serializer_file_opener_t file_opener(path, io_backender)
    standard_serializer_t serializer(standard_serializer_t::dynamic_config_t(), &file_opener, &dummy_stats);

    if (!serializer->coop_lock_and_check()) {
        throw file_in_use_exc_t();
    }

    dummy_cache_balancer_t balancer(MEGABYTE);
    cache_t cache(&serializer, &balancer, &dummy_stats);
    cache_conn_t cache_conn(&cache);

    txn_t read_txn(&cache_conn, read_access_t::read);
    buf_lock_t superblock(buf_parent_t(&read_txn), SUPERBLOCK_ID, access_t::read);
    buf_read_t sb_read(&superblock);

    const auth_metadata_superblock_t *sb =
        static_cast<const auth_metadata_superblock_t *>(sb_read.get_data_read());
    cluster_version_t v = auth_superblock_version(sb);
    auth_metadata_t metadata;

    if (v == cluster_version_t::v1_14 || v == cluster_version_t::v1_15) {
        pre_v1_16::auth_semilattice_metadata_t old_metadata;
        read_blob(buf_parent_t(&superblock),
                  sb->metadata_blob,
                  auth_metadata_superblock_t::METADATA_BLOB_MAXREFLEN,
                  [&](read_stream_t *s) -> archive_result_t {
                      switch (v) {
                      case cluster_version_t::v1_14:
                          return deserialize<cluster_version_t::v1_14>(s, &old_metadata);
                      case cluster_version_t::v1_15:
                          return deserialize<cluster_version_t::v1_15>(s, &old_metadata);
                      case cluster_version_t::v1_16:
                      case cluster_version_t::v2_0:
                      case cluster_version_t::v2_1_is_latest:
                      default:
                          unreachable();
                      }
                  });
        metadata = migrate_auth_metadata_to_v1_16(old_metadata);
    } else  if (v == cluster_version_t::v1_16 || v == cluster_version_t::v2_0) {
        read_blob(buf_parent_t(&superblock),
                  sb->metadata_blob,
                  auth_metadata_superblock_t::METADATA_BLOB_MAXREFLEN,
                  [&](read_stream_t *s) -> archive_result_t {
                      switch (v) {
                      case cluster_version_t::v1_16:
                          return deserialize<cluster_version_t::v1_16>(s, &metadata);
                      case cluster_version_t::v2_0:
                          return deserialize<cluster_version_t::v2_0>(s, &metadata);
                      case cluster_version_t::v1_14:
                      case cluster_version_t::v1_15:
                      case cluster_version_t::v2_1_is_latest:
                      default:
                          unreachable();
                      }
                  });
    } else {
        unreachable();
    }

    destination->write(mdkey_auth_semilattices(), metadata, nullptr);
}

}  // namespace migrate_v1_16

