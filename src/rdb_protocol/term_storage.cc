// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/term_storage.hpp"

#include "rdb_protocol/optargs.hpp"
#include "rdb_protocol/term_walker.hpp"

namespace ql {

generated_term_t::generated_term_t(Term::TermType _type, backtrace_id_t _bt) :
        type(_type), bt(_bt) { }

raw_term_t::raw_term_t() { }

void log_raw_term(const raw_term_t &term) {
    datum_t d = term.datum();
    if (d.has()) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d.write_json(&writer);
        debugf("new raw_term_t, type: %d, args: %zu, optargs: %zu, datum: %s\n",
               term.type(), term.num_args(), term.num_optargs(), buffer.GetString());
    } else {
        debugf("new raw_term_t, type: %d, args: %zu, optargs: %zu, no datum\n",
               term.type(), term.num_args(), term.num_optargs());
    }

}

raw_term_t::raw_term_t(const counted_t<generated_term_t> &source) {
    info = source;
    log_raw_term(*this);
}

raw_term_t::raw_term_t(const rapidjson::Value *source, std::string _optarg_name) :
        optarg_name_(std::move(_optarg_name)) {
    init_json(source);
    log_raw_term(*this);
}

raw_term_t::raw_term_t(const maybe_generated_term_t &source, std::string _optarg_name) :
        optarg_name_(std::move(_optarg_name)) {
    if (auto json_source = boost::get<rapidjson::Value *>(&source)) {
        init_json(*json_source);
    } else {
        info = boost::get<counted_t<generated_term_t> >(source);
    }
    log_raw_term(*this);
}

void raw_term_t::init_json(const rapidjson::Value *src) {
    info = json_data_t();
    json_data_t *data = boost::get<json_data_t>(&info);
    data->source = src;

    r_sanity_check(src->IsArray());
    size_t size = src->Size();
    r_sanity_check(size >= 2 && size <= 4);

    const rapidjson::Value *raw_type = &(*src)[0];
    r_sanity_check(raw_type->IsInt());
    data->type = static_cast<Term::TermType>(raw_type->GetInt64());

    const rapidjson::Value *raw_bt = &(*src)[size - 1];
    r_sanity_check(raw_bt->IsInt());
    data->bt = backtrace_id_t(raw_bt->GetUint());

    data->args = nullptr;
    data->optargs = nullptr;
    data->datum = nullptr;

    if (data->type == Term::DATUM) {
        rcheck_src(data->bt, size == 3, base_exc_t::LOGIC,
                   strprintf("Expected 3 items in array, but found %zu.", size));
        data->datum = &(*src)[1];
    } else if (size == 3) {
        data->args = &(*src)[1];
    } else if (size == 4) {
        data->args = &(*src)[1];
        data->optargs = &(*src)[2];
    }
}

size_t raw_term_t::num_args() const {
    size_t res = 0;
    visit_args(
        [&] (const rapidjson::Value *args) {
            if (args != nullptr) {
                res = args->Size();
            }
        },
        [&] (const std::vector<maybe_generated_term_t> &args) {
            res = args.size();
        });
    return res;
}

size_t raw_term_t::num_optargs() const {
    size_t res = 0;
    visit_optargs(
        [&] (const rapidjson::Value *optargs) {
            if (optargs != nullptr) {
                res = optargs->MemberCount();
            }
        },
        [&] (const std::map<std::string, maybe_generated_term_t> &optargs) {
            res = optargs.size();
        });
    return res;
}

raw_term_t raw_term_t::arg(size_t index) const {
    raw_term_t res;
    visit_args(
        [&] (const rapidjson::Value *args) {
            guarantee(args->Size() > index);
            res.init_json(&(*args)[index]);
        },
        [&] (const std::vector<maybe_generated_term_t> &args) {
            guarantee(args.size() > index);
            res = raw_term_t(args[index], std::string());
        });
    return res;
}

boost::optional<raw_term_t> raw_term_t::optarg(const std::string &name) const {
    boost::optional<raw_term_t> res;
    visit_optargs(
        [&] (const rapidjson::Value *optargs) {
            if (optargs != nullptr) {
                auto it = optargs->FindMember(name.c_str());
                if (it != optargs->MemberEnd()) {
                    res = raw_term_t(&it->value, it->name.GetString());
                }
            }
        },
        [&] (const std::map<std::string, maybe_generated_term_t> &optargs) {
            auto it = optargs.find(name);
            if (it != optargs.end()) {
                res = raw_term_t(it->second, it->first);
            }
        });
    return res;
}

datum_t raw_term_t::datum(const configured_limits_t &limits, reql_version_t version) const {
    datum_t res;
    visit_datum(
        [&] (const rapidjson::Value *datum) {
            if (datum != nullptr) {
                res = to_datum(*datum, limits, version);
            }
        },
        [&] (const datum_t &d) {
            res = d;
        });
    return res;
}

datum_t raw_term_t::datum() const {
    return datum(configured_limits_t::unlimited, reql_version_t::LATEST);
}

const std::string &raw_term_t::optarg_name() const {
    return optarg_name_;
}

Term::TermType raw_term_t::type() const {
    if (auto json = boost::get<json_data_t>(&info)) {
        return json->type;
    } else if (auto gen = boost::get<counted_t<generated_term_t> >(&info)) {
        return (*gen)->type;
    }
    unreachable();
}

backtrace_id_t raw_term_t::bt() const {
    if (auto json = boost::get<json_data_t>(&info)) {
        return json->bt;
    } else if (auto gen = boost::get<counted_t<generated_term_t> >(&info)) {
        return (*gen)->bt;
    }
    unreachable();
}

maybe_generated_term_t raw_term_t::get_src() const {
    if (auto json = boost::get<json_data_t>(&info)) {
        return json->source;
    } else if (auto gen = boost::get<counted_t<generated_term_t> >(&info)) {
        return *gen;
    }
    unreachable();
}

template <typename json_cb_t, typename generated_cb_t>
void raw_term_t::visit_args(json_cb_t &&json_cb, generated_cb_t &&generated_cb) const {
    if (auto json = boost::get<json_data_t>(&info)) {
        json_cb(json->args);
    } else if (auto gen = boost::get<counted_t<generated_term_t> >(&info)) {
        generated_cb((*gen)->args);
    } else {
        unreachable();
    }
}

template <typename json_cb_t, typename generated_cb_t>
void raw_term_t::visit_datum(json_cb_t &&json_cb, generated_cb_t &&generated_cb) const {
    if (auto json = boost::get<json_data_t>(&info)) {
        json_cb(json->datum);
    } else if (auto gen = boost::get<counted_t<generated_term_t> >(&info)) {
        generated_cb((*gen)->datum);
    } else {
        unreachable();
    }
}

term_storage_t term_storage_t::from_query(scoped_array_t<char> &&_wire_str,
                                          rapidjson::Value &&_query_json) {
    r_sanity_check(_query_json.IsArray());
    r_sanity_check(_query_json.Size() >= 2);

    term_storage_t res;
    res.wire_str = std::move(_wire_str);
    res.query_json = std::move(_query_json);
    res.root_term_ = &res.query_json[1];
    return res;
}

term_storage_t term_storage_t::from_wire_func(scoped_array_t<char> &&_wire_str,
                                              rapidjson::Value &&_query_json) {
    r_sanity_check(_query_json.IsArray());
    r_sanity_check(_query_json.Size() >= 1);

    term_storage_t res;
    res.wire_str = std::move(_wire_str);
    res.query_json = std::move(_query_json);
    res.root_term_ = &res.query_json;
    return res;
}

raw_term_t term_storage_t::root_term() const {
    return raw_term_t(root_term_, std::string());
}

global_optargs_t term_storage_t::global_optargs() {
    rapidjson::Document doc;
    rapidjson::Value *src;

    global_optargs_t res;
    r_sanity_check(query_json.IsArray());
    if (query_json.Size() >= 3) {
        src = &query_json[2];
        r_sanity_check(src->IsObject());
        for (auto it = src->MemberBegin(); it != src->MemberEnd(); ++it) {
            preprocess_global_optarg(&it->value, &doc.GetAllocator());
            res.add_optarg(raw_term_t(&it->value, it->name.GetString()));
        }
    } else {
        query_json.PushBack(rapidjson::Value(rapidjson::kObjectType),
                            doc.GetAllocator());
        src = &query_json[query_json.Size() - 1];
    }

    // Create a default db global optarg
    if (!res.has_optarg("db")) {
        src->AddMember(rapidjson::Value("db", doc.GetAllocator()),
                       rapidjson::Value(rapidjson::kArrayType),
                       doc.GetAllocator());
        auto it = src->FindMember("db");
        it->value.PushBack(rapidjson::Value(Term::DB), doc.GetAllocator());
        it->value.PushBack(rapidjson::Value(rapidjson::kArrayType), doc.GetAllocator());
        it->value[it->value.Size() - 1].PushBack(rapidjson::Value("test", doc.GetAllocator()),
                                                 doc.GetAllocator());
        preprocess_global_optarg(&it->value, &doc.GetAllocator());
        res.add_optarg(raw_term_t(&it->value, it->name.GetString()));
    }

    return res;
}

template <typename pb_t>
MUST_USE archive_result_t deserialize_protobuf(read_stream_t *s, pb_t *p) {
    CT_ASSERT(sizeof(int) == sizeof(int32_t));
    int32_t size;
    archive_result_t res = deserialize_universal(s, &size);
    if (bad(res)) { return res; }
    if (size < 0) { return archive_result_t::RANGE_ERROR; }
    scoped_array_t<char> data(size);
    int64_t read_res = force_read(s, data.data(), data.size());
    if (read_res != size) { return archive_result_t::SOCK_ERROR; }
    p->ParseFromArray(data.data(), data.size());
    return archive_result_t::SUCCESS;
}

template MUST_USE archive_result_t deserialize_protobuf<Term>(read_stream_t *s, Term *bt);
//template MUST_USE archive_result_t deserialize_protobuf<Datum>(read_stream_t *s, Datum *bt);
template MUST_USE archive_result_t deserialize_protobuf<Backtrace>(read_stream_t *s, Backtrace *bt);

rapidjson::Value convert_datum(const Datum &src,
                               rapidjson::MemoryPoolAllocator<> *allocator) {
    guarantee(src.has_type());
    switch (src.type()) {
    case Datum::R_NULL:
        return rapidjson::Value(rapidjson::kNullType);
    case Datum::R_BOOL:
        guarantee(src.has_r_bool());
        return rapidjson::Value(src.r_bool());
    case Datum::R_NUM:
        guarantee(src.has_r_num());
        return rapidjson::Value(src.r_num());
    case Datum::R_STR:
        guarantee(src.has_r_str());
        return rapidjson::Value(src.r_str(), *allocator);
    case Datum::R_ARRAY: {
        rapidjson::Value dest(rapidjson::kArrayType);
        for (int i = 0; i < src.r_array_size(); ++i) {
            dest.PushBack(convert_datum(src.r_array(i), allocator), *allocator);
        }
        return dest;
    }
    case Datum::R_OBJECT: {
        rapidjson::Value dest(rapidjson::kObjectType);
        for (int i = 0; i < src.r_object_size(); ++i) {
            const Datum_AssocPair &item = src.r_object(i);
            guarantee(item.has_key());
            guarantee(item.has_val());
            dest.AddMember(rapidjson::Value(item.key(), *allocator),
                           convert_datum(item.val(), allocator), *allocator);
        }
        return dest;
    }
    case Datum::R_JSON: {
        guarantee(src.has_r_str());
        rapidjson::Document doc(allocator);
        doc.Parse(src.r_str().c_str());
        return rapidjson::Value(std::move(doc));
    }
    default:
        unreachable();
    }
}

rapidjson::Value convert_term_tree(const Term &src,
                                   rapidjson::MemoryPoolAllocator<> *allocator);

rapidjson::Value convert_optargs(const Term &src,
                                 rapidjson::MemoryPoolAllocator<> *allocator) {
    rapidjson::Value dest(rapidjson::kObjectType);
    for (int i = 0; i < src.optargs_size(); ++i) {
        const Term_AssocPair &optarg = src.optargs(i);
        guarantee(optarg.has_key());
        guarantee(optarg.has_val());
        dest.AddMember(rapidjson::Value(optarg.key(), *allocator),
                       convert_term_tree(optarg.val(), allocator), *allocator);
    }
    return dest;
}

rapidjson::Value convert_term_tree(const Term &src,
                                   rapidjson::MemoryPoolAllocator<> *allocator) {
    rapidjson::Value dest;
    guarantee(src.has_type());
    switch(static_cast<int>(src.type())) {
    case Term::DATUM:
        guarantee(src.has_datum());
        dest = convert_datum(src.datum(), allocator);
        break;
    case Term::MAKE_OBJ:
        dest = convert_optargs(src, allocator);
        break;
    default:
        dest.SetArray();
        dest.PushBack(rapidjson::Value(static_cast<int>(src.type())), *allocator);
        if (src.args_size() > 0) {
            dest.PushBack(rapidjson::Value(rapidjson::kArrayType), *allocator);
            rapidjson::Value *args = &dest[dest.Size() - 1];
            for (int i = 0; i < src.args_size(); ++i) {
                args->PushBack(convert_term_tree(src.args(i), allocator), *allocator);
            }
        }
        if (src.optargs_size() > 0) {
            dest.PushBack(convert_optargs(src, allocator), *allocator);
        }
        dest.PushBack(rapidjson::Value(backtrace_id_t::empty().get()), *allocator);
    }
    return dest;
}

template <cluster_version_t W>
MUST_USE archive_result_t deserialize_term_tree(read_stream_t *s,
                                                scoped_array_t<char> *,
                                                rapidjson::Value *json_out) {
    Term body;
    archive_result_t res = deserialize_protobuf(s, &body);
    if (bad(res)) { return res; }

    rapidjson::Document doc;
    *json_out = convert_term_tree(body, &doc.GetAllocator());
    return res;
}

template MUST_USE archive_result_t deserialize_term_tree<cluster_version_t::v1_14>(
        read_stream_t *, scoped_array_t<char> *, rapidjson::Value *);
template MUST_USE archive_result_t deserialize_term_tree<cluster_version_t::v1_15>(
        read_stream_t *, scoped_array_t<char> *, rapidjson::Value *);
template MUST_USE archive_result_t deserialize_term_tree<cluster_version_t::v1_16>(
        read_stream_t *, scoped_array_t<char> *, rapidjson::Value *);
template MUST_USE archive_result_t deserialize_term_tree<cluster_version_t::v2_0>(
        read_stream_t *, scoped_array_t<char> *, rapidjson::Value *);
template MUST_USE archive_result_t deserialize_term_tree<cluster_version_t::v2_1>(
        read_stream_t *, scoped_array_t<char> *, rapidjson::Value *);

template <>
MUST_USE archive_result_t deserialize_term_tree<cluster_version_t::v2_2_is_latest>(
        read_stream_t *s,
        scoped_array_t<char> *data_out,
        rapidjson::Value *json_out) {
    CT_ASSERT(sizeof(int) == sizeof(int32_t));
    int32_t size;
    archive_result_t res = deserialize_universal(s, &size);
    if (bad(res)) { return res; }
    if (size < 0) { return archive_result_t::RANGE_ERROR; }
    data_out->init(size);
    int64_t read_res = force_read(s, data_out->data(), data_out->size());
    if (read_res != size) { return archive_result_t::SOCK_ERROR; }

    rapidjson::Document doc;
    doc.ParseInsitu(data_out->data());
    json_out->Swap(doc);
    return archive_result_t::SUCCESS;
}

void write_term(rapidjson::Writer<rapidjson::StringBuffer> *writer,
                const raw_term_t &term) {
    maybe_generated_term_t src = term.get_src();
    if (auto json = boost::get<const rapidjson::Value *>(&src)) {
        (*json)->Accept(*writer);
    } else if (boost::get<counted_t<generated_term_t> >(&src)) {
        if (term.type() == Term::DATUM) {
            guarantee(term.num_args() == 0);
            guarantee(term.num_optargs() == 0);
            term.datum().write_json(writer);
        } else {
            writer->StartArray();
            writer->Int(term.type());
            if (term.num_args() > 0) {
                writer->StartArray();
                for (size_t i = 0; i < term.num_args(); ++i) {
                    write_term(writer, term.arg(i));
                }
                writer->EndArray();
            }
            if (term.num_optargs() > 0) {
                writer->StartObject();
                term.each_optarg([&] (const raw_term_t &subterm) {
                        writer->Key(subterm.optarg_name().c_str(),
                                    subterm.optarg_name().size(), true);
                        write_term(writer, subterm);
                    });
                writer->EndObject();
            }
            writer->EndArray();
        }
    } else {
        unreachable();
    }
}

template <cluster_version_t W>
void serialize_term_tree(write_message_t *wm, const raw_term_t &root_term) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    write_term(&writer, root_term);
    guarantee(writer.IsComplete());

    int32_t size = buffer.GetSize() + 1;
    serialize_universal(wm, size);

    wm->append(buffer.GetString(), size);
}

template void serialize_term_tree<cluster_version_t::LATEST_OVERALL>(
        write_message_t *, const raw_term_t &);

} // namespace ql
