// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/persist/migrate_v1_16.hpp"
#include "clustering/administration/persist/migrate_pre_v1_16.hpp"

#include "buffer_cache/alt.hpp"
#include "buffer_cache/blob.hpp"

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

void migrate_tables(const cluster_semilattice_metadata_t &metadata,
                    const branch_history_t &branch_history,
                    metadata_file_t::write_txn_t *out) {
    for (auto const &pair : metadata.rdb_namespaces.namespaces) {
        if (!pair->second.is_deleted()) {

        } else {

        }
    }
}

void migrate_cluster_metadata (
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
    migrate_tables(metadata, branch_history, new_output);
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

