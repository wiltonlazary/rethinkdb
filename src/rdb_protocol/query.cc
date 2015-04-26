#include "rdb_protocol/query.hpp"

#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/pseudo_time.hpp"

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


bool query_params_t::static_optarg_as_bool(const std::string &key, bool default_value) {
    guarantee(global_optargs_json != nullptr);
    auto it = global_optargs_json->FindMember(key.c_str());
    if (it == global_optargs_json->MemberEnd() ||
        !it->IsArray() || it->Size() != 2 ||
        !(*it)[0].IsNumber() || (*it)[0].AsInt() != Term::DATUM) {
        return default_value;
    }

    datum_t res = to_datum((*it)[1],
                           configured_limits_t::unlimited(),
                           reql_version_t::LATEST);

    if (res.has() && res.get_type() == datum_t::type_t::R_BOOL) {
        return res.as_bool();
    }
    return default_value;
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
    type = doc[0].AsInt();

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
}

raw_term_iterator_t::raw_term_iterator_t(const intrusive_list_t<raw_term_t> *_list) :
    list(_list), item(list->head()) { }

const raw_term_t *raw_term_iterator_t::next() {
    const raw_term_t *res = item;
    r_sanity_check(res != nullptr, "Tried to read too many args or optargs from a term.");
    item = list->next(item);    
    return res;
}

datum_t term_storage_t::get_time() {
    if (!start_time.has()) {
        start_time = pseudo::time_now();
    }
    return start_time;
}

raw_term_t *term_storage_t::new_term(int type, backtrace_id_t bt) {
    raw_term_t &res = terms.push_back(raw_term_t());
    res.type = type;
    res.bt = bt;
    return &res;
}


void term_storage_t::add_global_optargs(const rapidjson::Value &optargs) {
    check_type(optargs, rapidjson::kObjectType, backtrace_id_t::empty());
    for (auto it = optargs.MemberBegin(); it != optargs.MemberEnd(); ++it) {
        add_global_optarg(it->name, it->value);
    }

    if (global_optargs.count("db") == 0) {
        // Add a default 'test' database optarg if none was specified
        add_global_optarg("db", /* TODO */);
    }
}

void term_storage_t::add_global_optarg(const std::string &key, const rapidjson::Value &v) {
    rcheck_toplevel(global_optargs.count(key) == 0, base_exc_t::GENERIC,
                    strprintf("Duplicate global optarg: %s.", key.c_str()));

    const raw_term_t *term = parse_internal(v, nullptr, backtrace_id_t::empty());

    minidriver_t r(this, backtrace_id_t::empty());
    term = r.fun(term).raw_term();

    compile_env_t env((var_visibility_t()), this);
    counted_t<const func_t> func =
        make_counted<func_term_t>(&env, term)->eval_to_func(var_scoped_t());
    global_optargs[key] = wire_func_t(func);
}

const raw_term_t *term_storage_t::parse_internal(const rapidjson::Value &v,
                                          backtrace_registry_t *bt_reg,
                                          backtrace_id_t bt) {
    bool got_optargs = false;
    if (!v.IsArray()) {
        // This is a literal datum term
        raw_term_t *t = new_term(Term::DATUM, bt);
        t->datum = to_datum(v, configured_limits_t::unlimited(), reql_version_t::LATEST);
        return t;
    }
    check_term_size(v, bt);
    check_type(v, rapidjson::kNumberType, bt);

    raw_term_t *t = new_term(v[0].GetInteger(), bt);

    if (v.Size() >= 2) {
        if (v[1].IsArray()) {
            add_args(v[1], &t->args, bt_reg, bt);
        } else if (v[1].IsObject()) {
            got_optargs = true;
            add_optargs(v[1], &t->optargs, bt_reg, bt);
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
        add_optargs(v[2], &t->optargs, bt_reg, bt);
    }

    // Convert NOW terms into a literal datum - so they all have the same value
    if (t->type == Term::NOW && t->num_args() == 0 && t->num_optargs() == 0) {
        t->type = Term::DATUM;
        t->datum = get_time();
    }
}

void term_storage_t::add_args(const rapidjson::Value &args,
                       intrusive_list_t<raw_term_t> *args_out,
                       backtrace_registry_t *bt_reg,
                       backtrace_id_t bt) {
    check_type(args, rapidjson::kArrayType, bt);
    for (size_t i = 0; i < args.Size(); ++i) {
        backtrace_id_t child_bt = (bt_reg == NULL) ?
            backtrace_id_t::empty() : bt_reg->new_frame(i);
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
            backtrace_id_t::empty() : bt_reg->new_frame(it->name);
        raw_term_t *t = parse_internal(it->value, bt_reg, child_bt);
        optargs_out->push_back(t);
        t->name = it->name;
    }
}

} // namespace ql
