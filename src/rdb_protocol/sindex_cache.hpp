// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_SINDEX_CACHE_HPP_
#define RDB_PROTOCOL_SINDEX_CACHE_HPP_

#include <memory>
#include <map>

#include "btree/btree_sindex_cache.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/func.hpp"

// Extends `sindex_disk_info_t` by a compiled mapping function
struct sindex_cached_info_t : public sindex_disk_info_t {
    counted_t<const ql::func_t> compiled_mapping;
};

// This is an extension to `btree_sindex_cache_t` that adds ReQL-specific
// caching values.
class sindex_cache_t : public btree_sindex_cache_t {
public:
    sindex_cache_t() { }

    std::shared_ptr<const sindex_cached_info_t> get_sindex_info(
        const uuid_u &index_id,
        const std::vector<char> &sindex_mapping_data);

    virtual void invalidate(buf_lock_t *sindex_block);

private:
    std::map<uuid_u, std::shared_ptr<const sindex_cached_info_t> >
        cached_sindex_infos;

    DISABLE_COPYING(sindex_cache_t);
};

#endif  // RDB_PROTOCOL_SINDEX_CACHE_HPP_
