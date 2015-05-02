// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/minidriver.hpp"

namespace ql {

minidriver_t::minidriver_t(term_storage_t *_term_storage, backtrace_id_t _bt) :
        term_storage(_term_storage), bt(_bt) { }

raw_term_t *minidriver_t::new_term(Term::TermType type) {
    return term_storage->new_term(type, bt);
}

raw_term_t *minidriver_t::new_ref(const raw_term_t *src) {
    return term_storage->new_ref(src);
}

minidriver_t::reql_t &minidriver_t::reql_t::operator=(const minidriver_t::reql_t &other) {
    r = other.r;
    raw_term_ = other.raw_term_;
    return *this;
}

minidriver_t::reql_t::reql_t(const reql_t &other) :
        r(other.r), raw_term_(other.raw_term_) { }

minidriver_t::reql_t::reql_t(minidriver_t *_r, const raw_term_t *term) :
        r(_r), raw_term_(r->new_ref(term)) { }

minidriver_t::reql_t::reql_t(minidriver_t *_r, const reql_t &other) :
        r(_r), raw_term_(r->new_ref(other.raw_term_)) { }

minidriver_t::reql_t::reql_t(minidriver_t *_r, double val) :
        r(_r), raw_term_(r->new_term(Term::DATUM)) {
    raw_term_->mutable_datum() = datum_t(val);
}

minidriver_t::reql_t::reql_t(minidriver_t *_r, const std::string &val) :
        r(_r), raw_term_(r->new_term(Term::DATUM)) {
    raw_term_->mutable_datum() = datum_t(val.c_str());
}

minidriver_t::reql_t::reql_t(minidriver_t *_r, const datum_t &d) :
        r(_r), raw_term_(r->new_term(Term::DATUM)) {
    raw_term_->mutable_datum() = d;
}

minidriver_t::reql_t::reql_t(minidriver_t *_r, std::vector<reql_t> &&val) :
        r(_r), raw_term_(r->new_term(Term::MAKE_ARRAY)) {
    for (auto i = val.begin(); i != val.end(); i++) {
        add_arg(std::move(*i));
    }
}

minidriver_t::reql_t::reql_t(minidriver_t *_r, pb::dummy_var_t var) :
        r(_r), raw_term_(r->new_term(Term::VAR)) {
    raw_term_->mutable_datum() =
        datum_t(static_cast<double>(dummy_var_to_sym(var).value));
}

minidriver_t::reql_t minidriver_t::boolean(bool b) {
    return reql_t(this, datum_t(datum_t::construct_boolean_t(), b));
}

void minidriver_t::reql_t::copy_optargs_from_term(const raw_term_t *from) {
    auto arg_it = from->optargs();
    for (const raw_term_t *t = arg_it.next(); t != nullptr; t = arg_it.next()) {
        add_arg(r->optarg(t->optarg_name(), t));
    }
}

void minidriver_t::reql_t::copy_args_from_term(const raw_term_t *from,
                                               size_t start_index) {
    auto arg_it = from->args();
    for (size_t i = 0; i < start_index; ++i) {
        arg_it.next();
    }
    for (const raw_term_t *arg = arg_it.next(); arg != nullptr; arg = arg_it.next()) {
        add_arg(arg);
    }
}

minidriver_t::reql_t minidriver_t::fun(const minidriver_t::reql_t &body) {
    return reql_t(this, Term::FUNC, array(), std::move(body));
}

minidriver_t::reql_t minidriver_t::fun(pb::dummy_var_t a,
                                       const minidriver_t::reql_t &body) {
    return reql_t(this, Term::FUNC,
                  reql_t(this, Term::MAKE_ARRAY, dummy_var_to_sym(a).value),
                  std::move(body));
}

minidriver_t::reql_t minidriver_t::fun(pb::dummy_var_t a,
                                       pb::dummy_var_t b,
                                       const minidriver_t::reql_t &body) {
    return reql_t(this, Term::FUNC,
                  reql_t(this, Term::MAKE_ARRAY,
                         dummy_var_to_sym(a).value,
                         dummy_var_to_sym(b).value),
                  std::move(body));
}

minidriver_t::reql_t minidriver_t::null() {
    return reql_t(this, datum_t::null());
}

const raw_term_t *minidriver_t::reql_t::raw_term() const {
    guarantee(raw_term_ != nullptr);
    return raw_term_;
}

minidriver_t::reql_t minidriver_t::reql_t::operator !() {
    return std::move(*this).call(Term::NOT);
}

minidriver_t::reql_t minidriver_t::reql_t::do_(pb::dummy_var_t arg,
                                               const minidriver_t::reql_t &body) {
    return r->fun(arg, std::move(body))(std::move(*this));
}

minidriver_t::reql_t minidriver_t::db(const std::string &name) {
    return reql_t(this, Term::DB, expr(name));
}

minidriver_t::reql_t minidriver_t::var(pb::dummy_var_t v) {
    return reql_t(this, Term::VAR, static_cast<double>(dummy_var_to_sym(v).value));
}

minidriver_t::reql_t minidriver_t::var(const sym_t &v) {
    return reql_t(this, Term::VAR, static_cast<double>(v.value));
}

} // namespace ql
