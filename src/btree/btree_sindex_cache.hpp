// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef BTREE_BTREE_SINDEX_CACHE_HPP_
#define BTREE_BTREE_SINDEX_CACHE_HPP_

#include <memory>
#include <map>

#include "btree/secondary_operations.hpp"
#include "concurrency/new_mutex.hpp"

class btree_sindex_cache_t {
public:
    btree_sindex_cache_t() { }
    virtual ~btree_sindex_cache_t() { }

    std::shared_ptr<const std::map<sindex_name_t, secondary_index_t> >
    get_sindex_map(buf_lock_t *sindex_block);

    virtual void invalidate(buf_lock_t *sindex_block);

protected:
    new_mutex_t cache_mutex;

private:
    std::shared_ptr<const std::map<sindex_name_t, secondary_index_t> >
        cached_sindex_map;

    DISABLE_COPYING(btree_sindex_cache_t);
};

#endif  // BTREE_BTREE_SINDEX_CACHE_HPP_
