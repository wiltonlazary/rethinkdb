// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/query.hpp"

#include "rdb_protocol/func.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/optargs.hpp"
#include "rdb_protocol/pseudo_time.hpp"
#include "rdb_protocol/query_cache.hpp"

namespace ql {

bool static_optarg_as_bool(const rapidjson::Value *global_optargs,
                           const std::string &key, bool default_value);

const char *rapidjson_typestr(rapidjson::Type t);

query_params_t::query_id_t::query_id_t(query_params_t::query_id_t &&other) :
        intrusive_list_node_t(std::move(other)),
        parent(other.parent),
        value_(other.value_) {
    parent->assert_thread();
    other.parent = NULL;
}

query_params_t::query_id_t::query_id_t(query_cache_t *_parent) :
        parent(_parent),
        value_(parent->next_query_id++) {
    // Guarantee correct ordering.
    query_id_t *last_newest = parent->outstanding_query_ids.tail();
    guarantee(last_newest == nullptr || last_newest->value() < value_);
    guarantee(value_ >= parent->oldest_outstanding_query_id.get());

    parent->outstanding_query_ids.push_back(this);
}

query_params_t::query_id_t::~query_id_t() {
    if (parent != nullptr) {
        parent->assert_thread();
    } else {
        rassert(!in_a_list());
    }

    if (in_a_list()) {
        parent->outstanding_query_ids.remove(this);
        if (value_ == parent->oldest_outstanding_query_id.get()) {
            query_id_t *next_outstanding_id = parent->outstanding_query_ids.head();
            if (next_outstanding_id == nullptr) {
                parent->oldest_outstanding_query_id.set_value(parent->next_query_id);
            } else {
                guarantee(next_outstanding_id->value() > value_);
                parent->oldest_outstanding_query_id.set_value(next_outstanding_id->value());
            }
        }
    }
}

uint64_t query_params_t::query_id_t::value() const {
    guarantee(in_a_list());
    return value_;
}

// If the query wants a reply, we can release the query id, which is only used for
// tracking the ordering of noreply queries for the purpose of noreply_wait.
void query_params_t::maybe_release_query_id() {
    if (!noreply) {
        query_id_t destroyer(std::move(id));
    }
}

query_params_t::query_params_t(int64_t _token,
                               ql::query_cache_t *_query_cache,
                               scoped_array_t<char> &&original_data,
                               rapidjson::Document &&_query_json) :
        query_cache(_query_cache),
        term_storage(term_storage_t::from_query(std::move(original_data),
                                                std::move(_query_json))),
        id(query_cache), token(_token), noreply(false), profile(false) {
    const rapidjson::Value &query_json = *term_storage.json();
    if (!query_json.IsArray()) {
        throw bt_exc_t(Response::CLIENT_ERROR, Response::QUERY_LOGIC,
            strprintf("Expected a query to be an array, but found %s.",
                      rapidjson_typestr(query_json.GetType())),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    if (query_json.Size() == 0 || query_json.Size() > 3) {
        throw bt_exc_t(Response::CLIENT_ERROR, Response::QUERY_LOGIC,
            strprintf("Expected 0 to 3 elements in the top-level query, but found %d.",
                      query_json.Size()),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }

    if (!query_json[0].IsNumber()) {
        throw bt_exc_t(Response::CLIENT_ERROR, Response::QUERY_LOGIC,
            strprintf("Expected a query type as a number, but found %s.",
                      rapidjson_typestr(query_json[0].GetType())),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    type = static_cast<Query::QueryType>(query_json[0].GetInt());

    if (query_json.Size() >= 3) {
        const rapidjson::Value *global_optargs = &query_json[2];
        if (!global_optargs->IsObject()) {
            throw bt_exc_t(Response::CLIENT_ERROR, Response::QUERY_LOGIC,
                strprintf("Expected global optargs as an object, but found %s.",
                          rapidjson_typestr(query_json[2].GetType())),
                backtrace_registry_t::EMPTY_BACKTRACE);
        }

        // Parse out optargs that are needed before query evaluation
        noreply = static_optarg_as_bool(global_optargs, "noreply", noreply);
        profile = static_optarg_as_bool(global_optargs, "profile", profile);
    }
}

bool static_optarg_as_bool(const rapidjson::Value *global_optargs,
                           const std::string &key, bool default_value) {
    guarantee(global_optargs != nullptr);
    auto it = global_optargs->FindMember(key.c_str());
    if (it == global_optargs->MemberEnd()) {
        return default_value;
    } else if (it->value.IsBool()) {
        return it->value.GetBool();
    } else if (!it->value.IsArray() ||
               it->value.Size() != 2 ||
               !it->value[0].IsNumber() ||
               static_cast<Term::TermType>(it->value[0].GetInt()) != Term::DATUM) {
        return default_value;
    } else if (!it->value[1].IsBool()) {
        return default_value;
    }
    return it->value[1].GetBool();
}

const char *rapidjson_typestr(rapidjson::Type t) {
    switch (t) {
    case rapidjson::kNullType:   return "NULL";
    case rapidjson::kFalseType:  return "BOOL";
    case rapidjson::kTrueType:   return "BOOL";
    case rapidjson::kObjectType: return "OBJECT";
    case rapidjson::kArrayType:  return "ARRAY";
    case rapidjson::kStringType: return "STRING";
    case rapidjson::kNumberType: return "NUMBER";
    default:                     break;
    }
    unreachable();
}

} // namespace ql
