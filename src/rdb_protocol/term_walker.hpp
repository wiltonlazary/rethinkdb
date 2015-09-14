// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_TERM_WALKER_HPP_
#define RDB_PROTOCOL_TERM_WALKER_HPP_

#include "rapidjson/document.h"

namespace ql {

class backtrace_registry_t;
 
void preprocess_term_tree(rapidjson::Value *query_json,
                          rapidjson::Value::AllocatorType *allocator,
                          backtrace_registry_t *bt_reg);

void preprocess_global_optarg(rapidjson::Value *optarg,
                              rapidjson::Value::AllocatorType *allocator);

} // namespace ql

#endif // RDB_PROTOCOL_TERM_WALKER_HPP_
