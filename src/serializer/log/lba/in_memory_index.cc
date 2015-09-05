// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "serializer/log/lba/in_memory_index.hpp"

#include <inttypes.h>

#include "serializer/log/lba/disk_format.hpp"

in_memory_index_t::in_memory_index_t()
    : end_block_id_(0), end_aux_block_id_(AUX_BLOCK_BIT) { }

block_id_t in_memory_index_t::end_block_id() {
    return end_block_id_;
}

block_id_t in_memory_index_t::end_aux_block_id() {
    return end_aux_block_id_;
}

index_block_info_t in_memory_index_t::get_block_info(block_id_t id) {
    if (is_aux_block(id)) {
        index_aux_block_info_t aux_info = aux_infos_.get(convert_aux_block_id(id));
        return index_block_info_t(aux_info.offset,
                                   repli_timestamp_t::distant_past,
                                   aux_info.ser_block_size);
    } else {
        return infos_.get(id);
    }
}

void in_memory_index_t::set_block_info(block_id_t id, repli_timestamp_t recency,
                                       flagged_off64_t offset, uint16_t ser_block_size) {
    if (is_aux_block(id)) {
        if (id >= end_aux_block_id_) {
            end_aux_block_id_ = id + 1;
        }
        // TODO! Do something about this.
        //rassert(recency == repli_timestamp_t::distant_past);
        index_aux_block_info_t info(offset, ser_block_size);
        aux_infos_.set(convert_aux_block_id(id), info);
    } else {
        if (id >= end_block_id_) {
            end_block_id_ = id + 1;
        }
        index_block_info_t info(offset, recency, ser_block_size);
        infos_.set(id, info);
    }
}

