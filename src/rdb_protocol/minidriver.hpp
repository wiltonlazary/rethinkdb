#ifndef RDB_PROTOCOL_MINIDRIVER_HPP_
#define RDB_PROTOCOL_MINIDRIVER_HPP_

#include <string>
#include <vector>
#include <utility>
#include <algorithm>

#include "rdb_protocol/counted_term.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/pb_utils.hpp"
#include "rdb_protocol/ql2.pb.h"
#include "rdb_protocol/sym.hpp"

/** RVALUE_THIS
 *
 * This macro is used to annotate methods that treat *this as an
 * rvalue reference. On compilers that support it, it expands to &&
 * and all uses of the method on non-rvlaue *this are reported as
 * errors.
 *
 * The supported compilers are clang >= 2.9 and gcc >= 4.8.1
 *
 **/
#if defined(__clang__)
#if __has_extension(cxx_rvalue_references)
#define RVALUE_THIS &&
#else
#define RVALUE_THIS
#endif
#elif __GNUC__ > 4 || (__GNUC__ == 4 && \
    (__GNUC_MINOR__ > 8 || (__GNUC_MINOR__ == 8 && \
                            __GNUC_PATCHLEVEL__ > 1)))
#define RVALUE_THIS &&
#else
#define RVALUE_THIS
#endif


namespace ql {

class minidriver_context_t {
public:
    /** reql_t
     *
     * A class that allows building raw_term_ts using the ReQL syntax. The
     * raw_term_ts will be added to the term_storage_t that the
     * minidriver_context_t was instantiated with.  If the term is used as
     * an arg or optarg in multiple places, a reference will be added in
     * the term_storage_t rather than recursively copying the term.
     *
     **/
    class reql_t {
    public:
        const raw_term_t *raw_term() const { return raw_term_; }
        void copy_optargs_from_term(const raw_term_t *from);
        void copy_args_from_term(const raw_term_t *from, size_t start_index);
    private:
        friend class minidriver_context_t;
        reql_t(minidriver_context_t *_r, scoped_ptr_t<Term> &&term_);
        reql_t(minidriver_context_t *_r, const double val);
        reql_t(minidriver_context_t *_r, const std::string &val);
        reql_t(minidriver_context_t *_r, const datum_t &d);
        reql_t(minidriver_context_t *_r, std::vector<reql_t> &&val);
        reql_t(minidriver_context_t *_r, pb::dummy_var_t var);

        reql_t(reql_t &&other);
        reql_t &operator= (reql_t &&other);

        template <class... T>
        reql_t(minidriver_context_t *_r, Term_TermType type, T &&... args) :
                : raw_term_(r->new_term()) {
            raw_term_->type = type;
            // TODO: set datum value if datum - or disallow datum terms in this constructor
            add_args(std::forward<T>(args)...);
        }

#define REQL_METHOD(name, termtype)                                     \
    template<class... T>                                                \
    reql_t name(T &&... a)                                              \
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
        void add_args(T &&... args) {
            UNUSED int _[] = { (add_arg(std::forward<T>(args)), 1)... };
        }

        template<class T>
        void add_arg(T &&a) {
            reql_t new_term(std::forward<T>(a));
            term->args->push_back(new_term.term);
        }

    private:
        raw_term_t *raw_term_;
        minidriver_context_t *r;
    };

    minidriver_context_t(term_storage_t *_term_storage,
                         backtrace_id_t bt) :
        term_storage(_term_storage), default_backtrace(bt) { }

    template <class T>
    reql_t expr(T &&d) {
        return reql_t(std::forward<T>(d));
    }

    reql_t boolean(bool b);

    reql_t fun(const reql_t &body);
    reql_t fun(pb::dummy_var_t a, const reql_t &body);
    reql_t fun(pb::dummy_var_t a, pb::dummy_var_t b, const reql_t &body);

    template<class... Ts>
    reql_t array(Ts &&... xs) {
        return reql_t(Term::MAKE_ARRAY, std::forward<Ts>(xs)...);
    }

    template<class... Ts>
    reql_t object(Ts &&... xs) {
        return reql_t(Term::MAKE_OBJ, std::forward<Ts>(xs)...);
    }

    reql_t null();

    template <class T>
    std::pair<std::string, reql_t> optarg(const std::string &key, T &&value) {
        return std::pair<std::string, reql_t>(key, reql_t(std::forward<T>(value)));
    }

    reql_t db(const std::string &name);

    template <class T>
    reql_t error(T &&message) {
        return reql_t(Term::ERROR, std::forward<T>(message));
    }

    template<class Cond, class Then, class Else>
    reql_t branch(Cond &&a, Then &&b, Else &&c) {
        return reql_t(Term::BRANCH,
                      std::forward<Cond>(a),
                      std::forward<Then>(b),
                      std::forward<Else>(c));
    }

    reql_t var(pb::dummy_var_t var);
    reql_t var(const sym_t &var);

private:
    friend class reql_t;
    raw_term_t *new_term(Term::TermType type);

    term_storage_t *term_storage;
    backtrace_id_t default_backtrace;
};


// This template-specialization of add_arg is actually adding an optarg
template <>
inline void reql_t::add_arg(std::pair<std::string, reql_t> &&optarg) {
    raw_term_t *optarg_term = optarg.second.term;
    if (optarg_term->in_a_list()) {
        // We need to make a reference to this object - it is already in use
        // by a different term and we wouldn't want to overwrite its optarg name.
        optarg_term = r->new_ref(term);
    }
    optarg_term->name = optarg.first;
    term->optargs->push_back(optarg_term);
}

}  // namespace ql

#endif  // RDB_PROTOCOL_MINIDRIVER_HPP_
