// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "btree/btree_sindex_cache.hpp"

#include "buffer_cache/alt.hpp"

std::shared_ptr<const std::map<sindex_name_t, secondary_index_t> >
btree_sindex_cache_t::get_sindex_map(buf_lock_t *sindex_block) {
    sindex_block->read_acq_signal()->wait_lazily_unordered();

    new_mutex_acq_t acq(&cache_mutex);
    if (!static_cast<bool>(cached_sindex_map)) {
        std::map<sindex_name_t, secondary_index_t> map;
        get_secondary_indexes_from_block(sindex_block, &map);
        cached_sindex_map.reset(
            new std::map<sindex_name_t, secondary_index_t>(std::move(map)));
    }

#ifndef NDEBUG
    std::map<sindex_name_t, secondary_index_t> reference;
    get_secondary_indexes_from_block(sindex_block, &reference);
    rassert(reference == *cached_sindex_map);
#endif

    return cached_sindex_map;
}

void btree_sindex_cache_t::invalidate(buf_lock_t *sindex_block) {
    sindex_block->write_acq_signal()->wait_lazily_unordered();

    new_mutex_acq_t acq(&cache_mutex);
    {
        std::shared_ptr<const std::map<sindex_name_t, secondary_index_t> > tmp;
        cached_sindex_map.swap(tmp);
    }
}
