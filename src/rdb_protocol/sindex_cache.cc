// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/sindex_cache.hpp"

#include "rdb_protocol/btree.hpp"

std::shared_ptr<const sindex_cached_info_t> sindex_cache_t::get_sindex_info(
        const uuid_u &index_id,
        const std::vector<char> &sindex_mapping_data) {

    new_mutex_acq_t acq(&cache_mutex);
    auto it = cached_sindex_infos.find(index_id);
    if (it == cached_sindex_infos.end()) {
        try {
            sindex_cached_info_t info;
            deserialize_sindex_info_or_crash(sindex_mapping_data, &info);
            info.compiled_mapping = info.mapping.compile_wire_func();
            cached_sindex_infos[index_id] = std::shared_ptr<const sindex_cached_info_t>(
                new sindex_cached_info_t(std::move(info)));
        } catch (const archive_exc_t &e) {
            crash("%s", e.what());
        }
        it = cached_sindex_infos.find(index_id);
    }

    rassert(it != cached_sindex_infos.end());
    return it->second;
}

void sindex_cache_t::invalidate(buf_lock_t *sindex_block) {
    btree_sindex_cache_t::invalidate(sindex_block);

    new_mutex_acq_t acq(&cache_mutex);
    cached_sindex_infos.clear();
}
