// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_TERM_WALKER_HPP_
#define RDB_PROTOCOL_TERM_WALKER_HPP_

namespace ql {

class raw_term_t;

void preprocess_term_tree(rapidjson::Value *src, backtrace_registry_t *bt_reg);

} // namespace ql

#endif // RDB_PROTOCOL_TERM_WALKER_HPP_
