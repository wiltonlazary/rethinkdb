#ifndef RDB_PROTOCOL_QUERY_HPP_
#define RDB_PROTOCOL_QUERY_HPP_
// TODO: change guarantees to sanity checks


// Returns a reference to the JSON Value of the root term, which can
// then be parsed into a raw_term_tree_t.
const rapidjson::Value &parse_query_params(const rapidjson::Value &v,
                                           query_params_t *params_out);

struct query_params_t {
public:
    query_params_t(int64_t _token, const rapidjson::Value &v) :
            token(_token), root_term_json(nullptr), global_optargs_json(nullptr),
            noreply(false), profile(profile_bool_t::DONT_PROFILE) {
        if (!v.IsArray()) {
            throw bt_exc_t();
        }
        if (v.Size() == 0 || v.Size() > 3) {
            throw bt_exc_t();
        }
        if (!v[0].IsNumber()) {
            throw bt_exc_t();
        }
        type = v[0].AsInt();

        for (size_t i = 1; i < v.Size(); ++i) {
            if (v[i].IsArray()) {
                if (root_term_json != nullptr) {
                    throw bt_exc_t();
                }
                root_term_json = &v[i];
            } else if (v[i].IsObject()) {
                if (global_optargs_json != nullptr) {
                    throw bt_exc_t();
                }
                global_optargs_json = &v[i];
            } else {
                throw bt_exc_t();
            }
        }

        // Parse out optargs that are needed before query evaluation
        if (global_optargs_json != nullptr) {
            datum_t noreply_datum = static_optarg("noreply");
            datum_t profile_datum = static_optarg("profile");
            if (noreply_datum.has() && noreply_datum.get_type() == datum_t::type_t::R_BOOL) {
                noreply = noreply_datum.as_bool();
            }
            if (profile_datum.has() && profile_datum.get_type() == datum_t::type_t::R_BOOL) {
                profile = profile_datum.as_bool() ?
                    profile_bool_t::PROFILE : profile_bool_t::DONT_PROFILE;
            }
        }
    }

    int64_t token;
    Query::QueryType type;
    bool noreply;
    profile_bool_t profile;

    const rapidjson::Value *root_term_json;
    const rapidjson::Value *global_optargs_json;
private:
    datum_t static_optarg(const std::string &key) {
        guarantee(global_optargs_json != nullptr);
        auto it = global_optargs_json->FindMember(key.c_str());
        if (it == global_optargs_json->MemberEnd() ||
            !it->IsArray() || it->Size() != 2 ||
            !(*it)[0].IsNumber() || (*it)[0].AsInt() != Term::DATUM) {
            return datum_t();
        }
        return to_datum((*it)[1], configured_limits_t::unlimited(), reql_version_t::LATEST);
    }
}



class term_storage_t {
public:
    // Parse a query from a rapidjson value and attach backtraces
    iterator_t add_term_tree(const rapidjson::Value &v);
    iterator_t add_global_optargs(const rapidjson::Value &v);

    iterator_t root_term() {
        return iterator_t(&root_term);
    }

    static iterator_t end() {
        return iterator_t(nullptr);
    }

    class iterator_t {
    public:
        Term::TermType term_type() const {
            guarantee(item != nullptr);
            return item->type;
        }

        backtrace_id_t backtrace() const {
            guarantee(item != nullptr);
            return item->bt;
        }

        datum_t datum() const {
            guarantee(item != nullptr);
            guarantee(item->type == Term::DATUM);
            return item->datum;
        }

        iterator_t args() {
            guarantee(item != nullptr)
            return iterator_t(&item->args);
        }

        optarg_iterator_t optargs() {
            guarantee(item != nullptr)
            return optarg_iterator_t(&item->optargs);
        }

        // postfix increment
        iterator_t operator ++ (int) {
            iterator_t res(*this);
            item = list->next(item);
            return res;
        }

        // prefix increment
        iterator_t& operator ++ () {
            guarantee(item != nullptr);
            item = list->next(item);
            return *this;
        }

    protected:
        iterator_t(intrusive_list_t<raw_term_t *> *_list) :
            item(list->head()), 

        }
        iterator_t(const iterator_t &other) :
            item(other.item), list(other.list) { }

        raw_term_t *item;
        
    private:
        intrusive_list_t<raw_term_t *> *list;
    };

    class optarg_iterator_t : public iterator_t {
    public:
        datum_string_t optarg_name() const {
            guarantee(item != nullptr);
            return item->name;
        }
    private:
        optarg_iterator_t(intrusive_list_t<raw_term_t *> *_list) :
            iterator_t(_list) { }
    };

private:
    query_t();

    struct raw_term_t : public intrusive_list_node_t<raw_term_t> {
        raw_term_t(Term::TermType _type, backtrace_id_t _bt) : type(_type), bt(_bt) { }
        intrusive_list_t<raw_term_t> args; // Not used for datum type
        intrusive_list_t<raw_term_t> optargs; // Not used for datum type

        Term::TermType type;
        datum_string_t name; // Only used when an optarg
        datum_t value; // Only used for datum type
        backtrace_id_t bt;
    };

    // We use a segmented vector so items won't be reallocated and moved, which allows us
    // to use pointers to other items in the vector.
    segmented_vector_t<raw_term_t, 100> terms;
    intrusive_list_t<raw_term_t> root_term; // Dummy list only to provide an iterator for the root term
    intrusive_list_t<raw_term_t> global_optargs;
    backtrace_registry_t bt_reg;

    raw_term_t *new_term(int type, backtrace_id_t bt);

    void add_global_optargs(const rapidjson::Value &args);

    void add_args(const rapidjson::Value &args,
                  intrusive_list_t<raw_term_t> *args_out,
                  backtrace_registry_t *bt_reg,
                  backtrace_id_t bt);

    void add_optargs(const rapidjson::Value &optargs,
                     intrusive_list_t<raw_term_t> *optargs_out,
                     backtrace_registry_t *bt_reg,
                     backtrace_id_t bt);

    raw_term_t *parse_internal(const rapidjson::Value &v,
                               backtrace_registry_t *bt_reg,
                               backtrace_id_t bt);

};

#endif // RDB_PROTOCOL_QUERY_HPP_
