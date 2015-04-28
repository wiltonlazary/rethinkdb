// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_TERM_WALKER_HPP_
#define RDB_PROTOCOL_TERM_WALKER_HPP_

namespace ql {

class raw_term_t;

void validate_term_tree(const raw_term_t *root);

} // namespace ql

#endif // RDB_PROTOCOL_TERM_WALKER_HPP_
