// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_MINIDRIVER_HPP_
#define RDB_PROTOCOL_MINIDRIVER_HPP_

#include <string>
#include <vector>
#include <utility>
#include <algorithm>

#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/pb_utils.hpp"
#include "rdb_protocol/ql2.pb.h"
#include "rdb_protocol/query.hpp"
#include "rdb_protocol/sym.hpp"

namespace ql {

class minidriver_t {
public:
    /** reql_t
     *
     * A class that allows building raw_term_ts using the ReQL syntax. The
     * raw_term_ts will be added to the term_storage_t that the
     * minidriver_t was instantiated with.  If the term is used as
     * an arg or optarg in multiple places, a reference will be added in
     * the term_storage_t rather than recursively copying the term.
     *
     **/
    class reql_t {
    public:
        const raw_term_t *raw_term() const;
        void copy_optargs_from_term(const raw_term_t *from);
        void copy_args_from_term(const raw_term_t *from, size_t start_index);

#define REQL_METHOD(name, termtype)  \
    template <class... T>            \
    reql_t name(T &&... a)           \
    { return reql_t(r, Term::termtype, *this, std::forward<T>(a)...); }

        REQL_METHOD(operator +, ADD)
        REQL_METHOD(operator /, DIV)
        REQL_METHOD(operator ==, EQ)
        REQL_METHOD(operator (), FUNCALL)
        REQL_METHOD(operator >, GT)
        REQL_METHOD(operator <, LT)
        REQL_METHOD(operator >=, GE)
        REQL_METHOD(operator <=, LE)
        REQL_METHOD(operator &&, AND)
        REQL_METHOD(count, COUNT)
        REQL_METHOD(map, MAP)
        REQL_METHOD(concat_map, CONCAT_MAP)
        REQL_METHOD(operator [], GET_FIELD)
        REQL_METHOD(nth, NTH)
        REQL_METHOD(bracket, BRACKET)
        REQL_METHOD(pluck, PLUCK)
        REQL_METHOD(has_fields, HAS_FIELDS)
        REQL_METHOD(coerce_to, COERCE_TO)
        REQL_METHOD(get_, GET)
        REQL_METHOD(get_all, GET_ALL)
        REQL_METHOD(replace, REPLACE)
        REQL_METHOD(insert, INSERT)
        REQL_METHOD(delete_, DELETE)
        REQL_METHOD(slice, SLICE)
        REQL_METHOD(filter, FILTER)
        REQL_METHOD(contains, CONTAINS)
        REQL_METHOD(merge, MERGE)
        REQL_METHOD(default_, DEFAULT)
        REQL_METHOD(table, TABLE)

        reql_t operator !();
        reql_t do_(pb::dummy_var_t arg, const reql_t &body);

        template <class... T>
        reql_t call(Term::TermType type, T &&... args) {
            return reql_t(r, type, *this, std::forward<T>(args)...);
        }

        reql_t(const reql_t &other);
        reql_t &operator= (const reql_t &other);

        template <class... T>
        void add_args(T &&... args) {
            UNUSED int _[] = { (add_arg(std::forward<T>(args)), 1)... };
        }

        template <class T>
        void add_arg(T &&a) {
            reql_t new_term(r, std::forward<T>(a));
            raw_term_->mutable_args().push_back(new_term.raw_term_);
        }

    private:
        friend class minidriver_t;
        reql_t(minidriver_t *_r, const raw_term_t *term_);
        reql_t(minidriver_t *_r, const reql_t &other);
        reql_t(minidriver_t *_r, const double val);
        reql_t(minidriver_t *_r, const std::string &val);
        reql_t(minidriver_t *_r, const datum_t &d);
        reql_t(minidriver_t *_r, std::vector<reql_t> &&val);
        reql_t(minidriver_t *_r, pb::dummy_var_t var);

        template <class... T>
        reql_t(minidriver_t *_r, Term::TermType type, T &&... args) :
                r(_r), raw_term_(r->new_term(type)) {
            // TODO: set datum value if datum - or disallow datum terms in this constructor
            add_args(std::forward<T>(args)...);
        }

        minidriver_t *r;
        raw_term_t *raw_term_;
    };

    minidriver_t(term_storage_t *_term_storage, backtrace_id_t bt);

    template <class T>
    reql_t expr(T &&d) {
        return reql_t(this, std::forward<T>(d));
    }

    reql_t boolean(bool b);

    // Takes n r.var reql_ts, plus a function that takes n arguments
    reql_t fun(const reql_t &body);
    reql_t fun(pb::dummy_var_t a, const reql_t &body);
    reql_t fun(pb::dummy_var_t a, pb::dummy_var_t b, const reql_t &body);

    template <class... T>
    reql_t array(T &&... args) {
        return reql_t(this, Term::MAKE_ARRAY, std::forward<T>(args)...);
    }

    template <class... T>
    reql_t object(T &&... args) {
        return reql_t(this, Term::MAKE_OBJ, std::forward<T>(args)...);
    }

    reql_t null();

    template <class T>
    std::pair<std::string, reql_t> optarg(const std::string &key, T &&value) {
        return std::pair<std::string, reql_t>(key, reql_t(this, std::forward<T>(value)));
    }

    reql_t db(const std::string &name);

    template <class T>
    reql_t error(T &&message) {
        return reql_t(Term::ERROR, std::forward<T>(message));
    }

    template <class Cond, class Then, class Else>
    reql_t branch(Cond &&a, Then &&b, Else &&c) {
        return reql_t(this, Term::BRANCH,
                      std::forward<Cond>(a),
                      std::forward<Then>(b),
                      std::forward<Else>(c));
    }

    reql_t var(pb::dummy_var_t var);
    reql_t var(const sym_t &var);

private:
    friend class reql_t;
    raw_term_t *new_term(Term::TermType type);

    // We may need to make a reference to this object if it is already in use
    // by a different term - we wouldn't want to overwrite its optarg name or
    // intrusive list pointers.
    raw_term_t *new_ref(const raw_term_t *source);

    term_storage_t *term_storage;
    backtrace_id_t bt;
};


// This template-specialization of add_arg is actually adding an optarg
template <>
inline void minidriver_t::reql_t::add_arg(
        std::pair<std::string, minidriver_t::reql_t> &&optarg) {
    raw_term_t *optarg_term = r->new_ref(optarg.second.raw_term_);
    optarg_term->optarg_name = optarg.first;
    raw_term_->mutable_optargs().push_back(optarg_term);
}

template <>
inline void minidriver_t::reql_t::add_arg(minidriver_t::reql_t &&arg) {
    raw_term_->mutable_args().push_back(r->new_ref(arg.raw_term_));
}

}  // namespace ql

#endif  // RDB_PROTOCOL_MINIDRIVER_HPP_
