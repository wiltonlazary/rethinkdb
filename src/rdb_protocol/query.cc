// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/query.hpp"

#include "rdb_protocol/func.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/optargs.hpp"
#include "rdb_protocol/pseudo_time.hpp"
#include "rdb_protocol/query_cache.hpp"

namespace ql {

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

void check_type(const rapidjson::Value &v,
                rapidjson::Type expected_type,
                backtrace_id_t bt) {
    if (v.GetType() != expected_type) {
        throw exc_t(base_exc_t::GENERIC,
            strprintf("Query parse error: expected %s but found %s.",
                      rapidjson_typestr(expected_type),
                      rapidjson_typestr(v.GetType())), bt);
    }
}

void check_term_size(const rapidjson::Value &v, backtrace_id_t bt) {
    if (v.Size() == 0 || v.Size() > 3) {
        throw exc_t(base_exc_t::GENERIC,
            strprintf("Expected an array of 1, 2, or 3 elements, but found %d.",
                      v.Size()), bt);
    }
}

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
                               scoped_array_t<char> &&_original_data,
                               rapidjson::Document &&_query_json) :
        query_cache(_query_cache), query_json(std::move(_query_json)), token(_token),
        id(query_cache), noreply(false), profile(false), root_term_json(nullptr),
        global_optargs_json(nullptr), original_data(std::move(_original_data)) {
    if (!query_json.IsArray()) {
        throw bt_exc_t(Response::CLIENT_ERROR,
            strprintf("Expected a query to be an array, but found %s.",
                      rapidjson_typestr(query_json.GetType())),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    if (query_json.Size() == 0 || query_json.Size() > 3) {
        throw bt_exc_t(Response::CLIENT_ERROR,
            strprintf("Expected 0 to 3 elements in the top-level query, but found %d.",
                      query_json.Size()),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }

    if (!query_json[0].IsNumber()) {
        throw bt_exc_t(Response::CLIENT_ERROR,
            strprintf("Expected a query type as a number, but found %s.",
                      rapidjson_typestr(query_json[0].GetType())),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    type = static_cast<Query::QueryType>(query_json[0].GetInt());

    if (query_json.Size() >= 2) {
        root_term_json = &query_json[1];
    }

    if (query_json.Size() >= 3) {
        if (!query_json[2].IsObject()) {
            throw bt_exc_t(Response::CLIENT_ERROR,
                strprintf("Expected global optargs as an object, but found %s.",
                          rapidjson_typestr(query_json[2].GetType())),
                backtrace_registry_t::EMPTY_BACKTRACE);
        }
        global_optargs_json = &query_json[2];
    }

    // Parse out optargs that are needed before query evaluation
    if (global_optargs_json != nullptr) {
        noreply = static_optarg_as_bool("noreply", noreply);
        profile = static_optarg_as_bool("profile", profile);
    }
}

bool query_params_t::static_optarg_as_bool(const std::string &key, bool default_value) {
    r_sanity_check(global_optargs_json != nullptr);
    auto it = global_optargs_json->FindMember(key.c_str());
    if (it == global_optargs_json->MemberEnd()) {
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

arg_iterator_t::arg_iterator_t(raw_term_t *_parent) :
    parent(_parent), index(0) { }

raw_term_t arg_iterator_t::next() {
    r_sanity_check(has_next());
    return (*args)[index++];
}

bool arg_iterator_t::has_next() {
    return index < parent->num_args();
}

optarg_iterator_t::optarg_iterator_t(rapidjson::Value *_optargs) :
    optargs(_optargs), it(optargs->MemberBegin()) { }

raw_term_t optarg_iterator_t::next() {
    r_sanity_check(has_next());
    raw_term_t res(&it->value, &it->name);
    ++it;
    return res;
}

bool optarg_iterator_t::has_next() {
    switch (source_type) {
    case source_type_t::REAL:
        return real.it != real.source->MemberEnd();
    case source_type_t::GENERATED:
        return generated.it != generated.source->optargs.end();
    default:
        unreachable();
    }
}

raw_term_t::raw_term_t(rapidjson::Value *_src,
                       const char *_optarg_name) :
        args_(nullptr), optargs_(nullptr), datum_(nullptr),
        optarg_name_(_optarg_name) {
    // We require that terms be preprocessed before a raw_term_t can be made
    r_sanity_check(v.IsArray());
    size_t size = src->Size();
    r_sanity_check(size >= 2 && size <= 4);

    rapidjson::Value *raw_type = src[0];
    r_sanity_check(raw_type->IsInt());
    type = static_cast<Term::TermType>(raw_type->AsInt());

    rapidjson::Value *raw_bt = src[size - 1];
    r_sanity_check(raw_bt->IsInt());
    bt = raw_bt->AsInt();

    if (type == Term::DATUM) {
        rcheck_src(bt, size == 3, base_exc_t::GENERIC,
                   strprintf("Expected 2 items in array, but found %d.", size));
        datum_ = &src[1];
    } else if (size == 3) {
        args_ = &src[1];
    } else if (size == 4) {
        args_ = &src[1];
        optargs_ = &rc[2];
    }
}

size_t raw_term_t::num_args() const {
    rassert(type != Term::DATUM);
    if (args_ == nullptr) { return 0; }
    r_sanity_check(args_->IsArray());
    return args_->Size();
}

size_t raw_term_t::num_optargs() const {
    rassert(type != Term::DATUM);
    if (optargs_ == nullptr) { return 0; }
    r_sanity_check(optargs_->IsObject());
    return optargs_->MemberCount();
}

arg_iterator_t raw_term_t::args() const {
    r_sanity_check(type != Term::DATUM);
    r_sanity_check(args_ != nullptr);
    r_sanity_check(args_->IsArray());
    return arg_iterator_t(args_);
}

optarg_iterator_t raw_term_t::optargs() const {
    r_sanity_check(type != Term::DATUM);
    r_sanity_check(optargs_ != nullptr);
    r_sanity_check(optargs_->IsObject());
    return optarg_iterator_t(optargs_);
}

term_storage_t::term_storage_t(rapidjson::Value &&root) :
    root_term_(std::move(root)) { }

raw_term_t term_storage_t::root_term() {
    return raw_term_t(&root_term_);
}

raw_term_t *term_storage_t::parse_internal(const rapidjson::Value &v,
                                           backtrace_registry_t *bt_reg,
                                           backtrace_id_t bt) {
}

void term_storage_t::add_args(const rapidjson::Value &args,
                              intrusive_list_t<raw_term_t> *args_out,
                              backtrace_registry_t *bt_reg,
                              backtrace_id_t bt) {
    check_type(args, rapidjson::kArrayType, bt);
    for (size_t i = 0; i < args.Size(); ++i) {
        backtrace_id_t child_bt = (bt_reg == NULL) ?
            backtrace_id_t::empty() :
            bt_reg->new_frame(bt, ql::datum_t(static_cast<double>(i)));
        raw_term_t *t = parse_internal(args[i], bt_reg, child_bt);
        args_out->push_back(t);
    }
}

void term_storage_t::add_optargs(const rapidjson::Value &optargs,
                                 intrusive_list_t<raw_term_t> *optargs_out,
                                 backtrace_registry_t *bt_reg,
                                 backtrace_id_t bt) {
    check_type(optargs, rapidjson::kObjectType, bt);
    for (auto it = optargs.MemberBegin(); it != optargs.MemberEnd(); ++it) {
        backtrace_id_t child_bt = (bt_reg == NULL) ?
            backtrace_id_t::empty() :
            bt_reg->new_frame(bt,
                ql::datum_t(datum_string_t(it->name.GetStringLength(),
                                           it->name.GetString())));
        raw_term_t *t = parse_internal(it->value, bt_reg, child_bt);
        optargs_out->push_back(t);
        t->optarg_name = std::string(it->name.GetString());
    }
}

template <cluster_version_t W>
archive_result_t term_storage_t::deserialize_term_tree(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version) {
    CT_ASSERT(sizeof(int) == sizeof(int32_t));
    int32_t size;
    archive_result_t res = deserialize_universal(s, &size);
    if (bad(res)) { return res; }
    if (size < 0) { return archive_result_t::RANGE_ERROR; }
    scoped_array_t<char> data(size);
    int64_t read_res = force_read(s, data.data(), data.size());
    if (read_res != size) { return archive_result_t::SOCK_ERROR; }
    Term t;
    t.ParseFromArray(data.data(), data.size());
    *term_out = parse_internal(t, reql_version);
    return archive_result_t::SUCCESS;
}

raw_term_t *term_storage_t::parse_internal(const Term &term,
                                           reql_version_t reql_version) {
    r_sanity_check(term.has_type());
    raw_term_t *raw_term = new_term(term.type(), backtrace_id_t::empty());

    if (term.type() == Term::DATUM) {
        raw_term->mutable_datum() =
            to_datum(&term.datum(), configured_limits_t::unlimited, reql_version);
    } else {
        for (int i = 0; i < term.args_size(); ++i) {
            raw_term->mutable_args().push_back(parse_internal(term.args(i),
                                                              reql_version));
        }
        for (int i = 0; i < term.optargs_size(); ++i) {
            const Term_AssocPair &optarg_term = term.optargs(i);
            raw_term_t *optarg = parse_internal(optarg_term.val(), reql_version);
            optarg->optarg_name = optarg_term.key();
            raw_term->mutable_optargs().push_back(optarg);
        }
    }
    return raw_term;
}

template
archive_result_t term_storage_t::deserialize_term_tree<cluster_version_t::v1_14>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version);
template
archive_result_t term_storage_t::deserialize_term_tree<cluster_version_t::v1_15>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version);
template
archive_result_t term_storage_t::deserialize_term_tree<cluster_version_t::v1_16>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version);
template
archive_result_t term_storage_t::deserialize_term_tree<cluster_version_t::v2_0>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version);

template <>
archive_result_t
term_storage_t::deserialize_term_tree<cluster_version_t::v2_1_is_latest>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version) {
    const cluster_version_t W = cluster_version_t::v2_1_is_latest;
    archive_result_t res;

    int32_t type;
    backtrace_id_t bt;
    res = deserialize<W>(s, &type);
    if (bad(res)) { return res; }
    res = deserialize<W>(s, &bt);
    if (bad(res)) { return res; }
    raw_term_t *term = new_term(static_cast<Term::TermType>(type), bt);

    if (term->type == Term::DATUM) {
        deserialize<W>(s, &term->mutable_datum());
    } else {
        size_t num_args;
        res = deserialize<W>(s, &num_args);
        if (bad(res)) { return res; }
        for (size_t i = 0; i < num_args; ++i) {
            res = deserialize_term_tree<W>(s, term_out, reql_version);
            if (bad(res)) { return res; }
            term->mutable_args().push_back(*term_out);
        }
        std::string optarg_name;
        size_t num_optargs;
        res = deserialize<W>(s, &num_optargs);
        if (bad(res)) { return res; }
        for (size_t i = 0; i < num_optargs; ++i) {
            res = deserialize<W>(s, &optarg_name);
            if (bad(res)) { return res; }
            res = deserialize_term_tree<W>(s, term_out, reql_version);
            if (bad(res)) { return res; }
            (*term_out)->optarg_name = optarg_name;
            term->mutable_optargs().push_back(*term_out);
        }
    }
    *term_out = term;
    return res;
}

template <cluster_version_t W>
void serialize_term_tree(write_message_t *wm,
                         const raw_term_t *term) {
    serialize<W>(wm, static_cast<int32_t>(term->type));
    serialize<W>(wm, term->bt);
    if (term->type == Term::DATUM) {
        serialize<W>(wm, term->datum());
    } else {
        size_t num_args = term->num_args();
        serialize<W>(wm, num_args);
        auto arg_it = term->args();
        while (const raw_term_t *t = arg_it.next()) {
            serialize_term_tree<W>(wm, t);
            --num_args;
        }
        std::string optarg_name;
        size_t num_optargs = term->num_optargs();
        serialize<W>(wm, num_optargs);
        auto optarg_it = term->optargs();
        while (const raw_term_t *t = optarg_it.next()) {
            optarg_name.assign(optarg_it.optarg_name());
            serialize<W>(wm, optarg_name);
            serialize_term_tree<W>(wm, t);
            --num_optargs;
        }
        r_sanity_check(num_args == 0);
        r_sanity_check(num_optargs == 0);
    }
}

template
void serialize_term_tree<cluster_version_t::CLUSTER>(
    write_message_t *wm, const raw_term_t *term);

} // namespace ql
