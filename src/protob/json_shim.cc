#include "protob/json_shim.hpp"

#include <inttypes.h>

#include "debug.hpp"
#include "http/json.hpp"
#include "rdb_protocol/ql2.pb.h"
#include "utils.hpp"

std::map<std::string, int32_t> resolver;

namespace json_shim {

class exc_t : public std::exception {
public:
    exc_t() { }
    ~exc_t() throw () { }
    const char *what() const throw () { return "json_shim::exc_t"; }
};

void write_json_pb(const Response &r, std::string *s) THROWS_NOTHING {
    // Note: We must keep any existing prefix in `s` intact.
#ifdef NDEBUG
    const size_t start_offset = s->length();
#endif
    try {
        *s += strprintf("{\"t\":%d,\"r\":[", r.type());
        for (int i = 0; i < r.response_size(); ++i) {
            *s += (i == 0) ? "" : ",";
            const Datum *d = &r.response(i);
            if (d->type() == Datum::R_JSON) {
                *s += d->r_str();
            } else if (d->type() == Datum::R_STR) {
                scoped_cJSON_t tmp(cJSON_CreateString(d->r_str().c_str()));
                *s += tmp.PrintUnformatted();
            } else {
                unreachable();
            }
        }
        *s += "],\"n\":[";
        for (int i = 0; i < r.notes_size(); ++i) {
            *s += (i == 0) ? "" : ",";
            *s += strprintf("%d", r.notes(i));
        }
        *s += "]";

        if (r.has_backtrace()) {
            *s += ",\"b\":";
            const Backtrace *bt = &r.backtrace();
            scoped_cJSON_t arr(cJSON_CreateArray());
            for (int i = 0; i < bt->frames_size(); ++i) {
                const Frame *f = &bt->frames(i);
                switch (f->type()) {
                case Frame::POS:
                    arr.AddItemToArray(cJSON_CreateNumber(f->pos()));
                    break;
                case Frame::OPT:
                    arr.AddItemToArray(cJSON_CreateString(f->opt().c_str()));
                    break;
                default:
                    unreachable();
                }
            }
            *s += arr.PrintUnformatted();
        }

        if (r.has_profile()) {
            *s += ",\"p\":";
            const Datum *d = &r.profile();
            guarantee(d->type() == Datum::R_JSON);
            *s += d->r_str();
        }

        *s += "}";
    } catch (...) {
#ifndef NDEBUG
        throw;
#else
        // Erase everything we have added above, then append an error message
        s->erase(start_offset);
        *s += strprintf("{\"t\":%d,\"r\":[\"%s\"]}",
                        Response::RUNTIME_ERROR,
                        "Internal error in `write_json_pb`, please report this.");
#endif // NDEBUG
    }
}


} // namespace json_shim
