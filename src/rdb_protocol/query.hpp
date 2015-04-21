

// TODO: change guarantees to sanity checks
class query_t {
public:
    // Parse a query from a rapidjson value and attach backtraces
    static query_t parse(const rapidjson::Value &v,
                         backtrace_registry_t *bt_reg);

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
    Query::QueryType type;

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
