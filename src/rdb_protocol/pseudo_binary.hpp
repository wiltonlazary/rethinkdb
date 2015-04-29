// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_PSEUDO_BINARY_HPP_
#define RDB_PROTOCOL_PSEUDO_BINARY_HPP_

#include <string>
#include <utility>
#include <vector>

#include "rdb_protocol/datum_string.hpp"
#include "rdb_protocol/datum.hpp"

namespace rapidjson {
template <class T>
class Writer;
class StringBuffer;
}

namespace ql {
namespace pseudo {

extern const char *const binary_string;
extern const char *const data_key;

std::string encode_base64(const datum_string_t &data);

// Given a raw data string, encodes it into a `r.binary` pseudotype with base64 encoding
scoped_cJSON_t encode_base64_ptype(const datum_string_t &data);
void encode_base64_ptype(const datum_string_t &data,
                         rapidjson::Writer<rapidjson::StringBuffer> *writer);
void write_binary_to_protobuf(Datum *d, const datum_string_t &data);

// Given a `r.binary` pseudotype with base64 encoding, decodes it into a raw data string
datum_string_t decode_base64_ptype(
    const std::vector<std::pair<datum_string_t, datum_t> > &ptype);

} // namespace pseudo
} // namespace ql

#endif  // RDB_PROTOCOL_PSEUDO_BINARY_HPP_
