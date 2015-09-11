// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_TERM_STORAGE_HPP_
#define RDB_PROTOCOL_TERM_STORAGE_HPP_

#include <map>
#include <string>
#include <vector>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "containers/counted.hpp"
#include "containers/scoped.hpp"
#include "rapidjson/rapidjson.h"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/ql2.pb.h"

// Used to deserialize protobuf-serialized terms
class Term;
class Datum;
class Backtrace;

namespace ql {

struct generated_term_t;

using maybe_generated_term_t = boost::variant<const rapidjson::Value *,
                                              counted_t<generated_term_t> >;

// TODO: generated terms MUST NOT move after a raw_term_t gets a reference to them
struct generated_term_t : public slow_atomic_countable_t<generated_term_t> {
    generated_term_t(Term::TermType _type, backtrace_id_t _bt);

    const Term::TermType type;
    std::vector<maybe_generated_term_t> args;
    std::map<std::string, maybe_generated_term_t> optargs;
    datum_t datum;
    const backtrace_id_t bt;
};

class raw_term_t {
public:
    raw_term_t(const counted_t<generated_term_t> &source);
    raw_term_t(const raw_term_t &other) = default;

    size_t num_args() const;
    size_t num_optargs() const;
    raw_term_t arg(size_t index) const;
    boost::optional<raw_term_t> optarg(const std::string &name) const;

    template <typename callable_t>
    void each_optarg(callable_t &&cb) const {
        visit_optargs(
            [&] (const rapidjson::Value *optargs) {
                auto it = optargs->MemberBegin();
                while (it != optargs->MemberEnd()) {
                    cb(raw_term_t(&it->value, it->name.GetString()));
                }
            },
            [&] (const std::map<std::string, maybe_generated_term_t> &optargs) {
                auto it = optargs.begin();
                while (it != optargs.end()) {
                    cb(raw_term_t(it->second, it->first));
                }
            });
    }

    // This parses the datum each time it is called - keep calls to a minimum
    datum_t datum(const configured_limits_t &limits, reql_version_t version) const;
    
    // Parses the datum using the latest version and with no limits
    // TODO: make sure this isn't being used in the wrong place
    datum_t datum() const;

    Term::TermType type() const;
    backtrace_id_t bt() const;
    const std::string &optarg_name() const;
    maybe_generated_term_t get_src() const;

private:
    friend class term_storage_t;
    friend struct generated_term_t;
    raw_term_t(const rapidjson::Value *source, std::string _optarg_name);
    raw_term_t(const maybe_generated_term_t &source, std::string _optarg_name);
    raw_term_t();
    void init_json(const rapidjson::Value *src);

    template <typename json_cb_t, typename generated_cb_t>
    void visit_args(json_cb_t &&json_cb, generated_cb_t &&generated_cb) const;

    template <typename json_cb_t, typename generated_cb_t>
    void visit_optargs(json_cb_t &&json_cb, generated_cb_t &&generated_cb) const {
        if (auto json = boost::get<json_data_t>(&info)) {
            json_cb(json->optargs);
        } else if (auto gen = boost::get<counted_t<generated_term_t> >(&info)) {
            generated_cb((*gen)->optargs);
        } else {
            unreachable();
        }
    }

    template <typename json_cb_t, typename generated_cb_t>
    void visit_datum(json_cb_t &&json_cb, generated_cb_t &&generated_cb) const;

    std::string optarg_name_;

    struct json_data_t {
        Term::TermType type;
        backtrace_id_t bt;
        const rapidjson::Value *args;
        const rapidjson::Value *optargs;
        const rapidjson::Value *datum;
        const rapidjson::Value *source;
    };

    boost::variant<json_data_t, counted_t<generated_term_t> > info;
};

class term_storage_t {
public:
    term_storage_t() { }

    static term_storage_t from_query(scoped_array_t<char> &&_wire_str,
                                     rapidjson::Value &&_query_json);

    static term_storage_t from_wire_func(scoped_array_t<char> &&_wire_str,
                                         rapidjson::Value &&_query_json);

    term_storage_t(term_storage_t &&other) = default;
    term_storage_t &operator=(term_storage_t &&other) = default;

    raw_term_t root_term() const;
    global_optargs_t global_optargs() const;

private:
    scoped_array_t<char> wire_str;
    rapidjson::Value query_json;
    rapidjson::Value *root_term_;
};

template <typename pb_t>
MUST_USE archive_result_t deserialize_protobuf(read_stream_t *s, pb_t *pb_out);

template <cluster_version_t W>
void serialize_term_tree(write_message_t *wm,
                         const raw_term_t &root_term);

template <cluster_version_t W>
MUST_USE archive_result_t deserialize_term_tree(read_stream_t *s,
                                                scoped_array_t<char> *buffer_out,
                                                rapidjson::Value *json_out);

} // namespace ql

#endif // RDB_PROTOCOL_TERM_STORAGE_HPP_
