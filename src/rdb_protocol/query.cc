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
            strprintf("Expected an array of 1, 2, or 3 elements, but found %zu.",
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

query_params_t::query_params_t(int64_t _token,
                               ql::query_cache_t *_query_cache,
                               rapidjson::Document &&query_json) :
        query_cache(_query_cache), doc(std::move(query_json)), token(_token),
        id(query_cache), noreply(false), profile(false),
        root_term_json(nullptr), global_optargs_json(nullptr) {
    if (!doc.IsArray()) {
        throw bt_exc_t(Response::CLIENT_ERROR,
            strprintf("Expected a query to be an array, but found %s.",
                      rapidjson_typestr(doc.GetType())),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    if (doc.Size() == 0 || doc.Size() > 3) {
        throw bt_exc_t(Response::CLIENT_ERROR,
            strprintf("Expected 0 to 3 elements in the top-level query, but found %zu.",
                      doc.Size()),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    if (!doc[0].IsNumber()) {
        throw bt_exc_t(Response::CLIENT_ERROR,
            strprintf("Expected a query type as a number, but found %s.",
                      rapidjson_typestr(doc[0].GetType())),
            backtrace_registry_t::EMPTY_BACKTRACE);
    }
    type = static_cast<Query::QueryType>(doc[0].GetInt());

    for (size_t i = 1; i < doc.Size(); ++i) {
        if (doc[i].IsArray()) {
            if (root_term_json != nullptr) {
                throw bt_exc_t(Response::CLIENT_ERROR,
                    "Two term trees found in the top-level query.",
                    backtrace_registry_t::EMPTY_BACKTRACE);
            }
            root_term_json = &doc[i];
        } else if (doc[i].IsObject()) {
            if (global_optargs_json != nullptr) {
                throw bt_exc_t(Response::CLIENT_ERROR,
                    "Two sets of global optargs found in the top-level query.",
                    backtrace_registry_t::EMPTY_BACKTRACE);
            }
            global_optargs_json = &doc[i];
        } else {
            throw bt_exc_t(Response::CLIENT_ERROR,
                strprintf("Expected an argument tree or global optargs, but found %s.",
                          rapidjson_typestr(doc[i].GetType())),
                backtrace_registry_t::EMPTY_BACKTRACE);
        }
    }

    // Parse out optargs that are needed before query evaluation
    if (global_optargs_json != nullptr) {
        noreply = static_optarg_as_bool("noreply", noreply);
        profile = static_optarg_as_bool("profile", profile);
    }

    // If the query wants a reply, we can release the query id, which is
    // only used for tracking the ordering of noreply queries for the
    // purpose of noreply_wait.
    if (!noreply) {
        query_id_t destroyer(std::move(id));
    }
}

bool query_params_t::static_optarg_as_bool(const std::string &key, bool default_value) {
    r_sanity_check(global_optargs_json != nullptr);
    auto it = global_optargs_json->FindMember(key.c_str());
    if (it == global_optargs_json->MemberEnd() ||
        !it->value.IsArray() || it->value.Size() != 2 ||
        !it->value[0].IsNumber() ||
        static_cast<Term::TermType>(it->value[0].GetInt()) != Term::DATUM) {
        return default_value;
    }

    datum_t res = to_datum(it->value[1],
                           configured_limits_t::unlimited,
                           reql_version_t::LATEST);

    if (res.has() && res.get_type() == datum_t::type_t::R_BOOL) {
        return res.as_bool();
    }
    return default_value;
}

raw_term_iterator_t::raw_term_iterator_t(const intrusive_list_t<raw_term_t> *_list) :
    list(_list), item(list->head()) { }

const raw_term_t *raw_term_iterator_t::next() {
    const raw_term_t *res = item;
    r_sanity_check(res != nullptr, "Tried to read too many args or optargs from a term.");
    item = list->next(item);
    return res;
}

void raw_term_t::set_optarg_name(const std::string &name) {
    optarg_name_ = global_optargs_t::validate_optarg(name, bt);
}

term_storage_t::term_storage_t() { }

void term_storage_t::add_root_term(const rapidjson::Value &v) {
    parse_internal(v, &backtrace_registry, backtrace_id_t::empty());
}

void term_storage_t::add_global_optargs(const rapidjson::Value &optargs) {
    check_type(optargs, rapidjson::kObjectType, backtrace_id_t::empty());
    bool has_db_optarg = false;
    for (auto it = optargs.MemberBegin(); it != optargs.MemberEnd(); ++it) {
        std::string key(it->name.GetString());
        if (key == "db") {
            has_db_optarg = true;
        }
        global_optargs_t::validate_optarg(key, backtrace_id_t::empty());

        minidriver_t r(this, backtrace_id_t::empty());
        raw_term_t *term = parse_internal(it->value, nullptr, backtrace_id_t::empty());

        // Don't do this at home
        const raw_term_t *func_term = r.fun(r.expr(term)).raw_term();
        global_optarg_list.push_back(const_cast<raw_term_t *>(func_term));
    }

    // Add a default 'test' database optarg if none was specified
    if (!has_db_optarg) {
        minidriver_t r(this, backtrace_id_t::empty());
        const raw_term_t *func_term = r.fun(r.db("test")).raw_term();
        global_optarg_list.push_back(const_cast<raw_term_t *>(func_term));
    }
}

datum_t term_storage_t::get_time() {
    if (!start_time.has()) {
        start_time = pseudo::time_now();
    }
    return start_time;
}

raw_term_t *term_storage_t::new_term(Term::TermType type, backtrace_id_t bt) {
    raw_term_t &res = terms.push_back();
    res.type = type;
    res.bt = bt;
    return &res;
}

raw_term_t *term_storage_t::parse_internal(const rapidjson::Value &v,
                                           backtrace_registry_t *bt_reg,
                                           backtrace_id_t bt) {
    bool got_optargs = false;
    if (!v.IsArray()) {
        // This is a literal datum term
        raw_term_t *t = new_term(Term::DATUM, bt);
        t->value = to_datum(v, configured_limits_t::unlimited, reql_version_t::LATEST);
        return t;
    }
    check_term_size(v, bt);
    check_type(v, rapidjson::kNumberType, bt);

    raw_term_t *t = new_term(static_cast<Term::TermType>(v[0].GetInt()), bt);

    if (v.Size() >= 2) {
        if (v[1].IsArray()) {
            add_args(v[1], &t->args_, bt_reg, bt);
        } else if (v[1].IsObject()) {
            got_optargs = true;
            add_optargs(v[1], &t->optargs_, bt_reg, bt);
        } else {
            throw exc_t(base_exc_t::GENERIC,
                strprintf("Expected an ARRAY or arguments or an OBJECT of optional "
                          "arguments, but found a %s.",
                          rapidjson_typestr(v[1].GetType())), bt);
        }
    }

    if (v.Size() >= 3) {
        if (got_optargs) {
            throw exc_t(base_exc_t::GENERIC,
                        "Found two sets of optional arguments.", bt);
        }
        check_type(v, rapidjson::kObjectType, bt);
        add_optargs(v[2], &t->optargs_, bt_reg, bt);
    }

    // Convert NOW terms into a literal datum - so they all have the same value
    if (t->type == Term::NOW && t->num_args() == 0 && t->num_optargs() == 0) {
        t->type = Term::DATUM;
        t->value = get_time();
    }
    return t;
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
        t->set_optarg_name(it->name.GetString());
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
        raw_term->value =
            to_datum(&term.datum(), configured_limits_t::unlimited, reql_version);
    } else {
        for (int i = 0; i < term.args_size(); ++i) {
            raw_term->args_.push_back(parse_internal(term.args(i), reql_version));
        }
        for (int i = 0; i < term.optargs_size(); ++i) {
            const Term_AssocPair &optarg_term = term.optargs(i);
            raw_term_t *optarg = parse_internal(optarg_term.val(), reql_version);
            optarg->set_optarg_name(optarg_term.key());
            raw_term->optargs_.push_back(optarg);
        }
    }
    return raw_term;
}

template
archive_result_t term_storage_t::deserialize_term_tree<cluster_version_t::v1_13>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version);
template
archive_result_t term_storage_t::deserialize_term_tree<cluster_version_t::v1_13_2>(
        read_stream_t *s, raw_term_t **term_out, reql_version_t reql_version);
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
        deserialize<W>(s, &term->value);
    } else {
        size_t num_args;
        res = deserialize<W>(s, &num_args);
        if (bad(res)) { return res; }
        for (size_t i = 0; i < num_args; ++i) {
            res = deserialize_term_tree<W>(s, term_out, reql_version);
            if (bad(res)) { return res; }
            term->args_.push_back(*term_out);
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
            (*term_out)->set_optarg_name(optarg_name);
            term->optargs_.push_back(*term_out);
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
        serialize<W>(wm, term->value);
        r_sanity_check(term->num_args() == 0);
        r_sanity_check(term->num_optargs() == 0);
    } else {
        size_t num_args = term->num_args();
        serialize<W>(wm, num_args);
        auto arg_it = term->args();
        for (const raw_term_t *t = arg_it.next();
             t != nullptr; t = arg_it.next()) {
            serialize_term_tree<W>(wm, t);
            --num_args;
        }
        std::string optarg_name;
        size_t num_optargs = term->num_optargs();
        serialize<W>(wm, num_optargs);
        auto optarg_it = term->optargs();
        for (const raw_term_t *t = optarg_it.next();
             t != nullptr; t = optarg_it.next()) {
            optarg_name.assign(t->optarg_name());
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
