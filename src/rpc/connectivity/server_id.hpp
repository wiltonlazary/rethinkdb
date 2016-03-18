// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef RPC_CONNECTIVITY_SERVER_ID_HPP_
#define RPC_CONNECTIVITY_SERVER_ID_HPP_

#include <string>

#include "containers/uuid.hpp"
#include "rpc/serialize_macros.hpp"

/* `server_id_t` is a `uuid_u` together with a flag that determines whether it's
for a proxy or not.
For reasons of backwards-compatibility, it serializes as a `uuid_u`. */
class server_id_t {
public:
    // Creates a new `server_id_t` for a proxy server
    static server_id_t generate_proxy_id();
    // Creates a new `server_id_t` for a regular server
    static server_id_t generate_server_id();

    static server_id_t from_uuid(uuid_u _uuid);

    // Used for deserialization. `uuid` is initialized to unset
    server_id_t() { }

    bool operator<(const server_id_t &p) const {
        return p.uuid < uuid;
    }
    bool operator==(const server_id_t &p) const {
        return p.uuid == uuid;
    }
    bool operator!=(const server_id_t &p) const {
        return p.uuid != uuid;
    }

    uuid_u get_uuid() const {
        return uuid;
    }

    bool is_proxy() const;

    std::string print() const;

    RDB_DECLARE_ME_SERIALIZABLE(server_id_t);

private:
    uuid_u uuid;
};

void serialize_universal(write_message_t *wm, const server_id_t &server_id);
archive_result_t deserialize_universal(read_stream_t *s, server_id_t *server_id);

void debug_print(printf_buffer_t *buf, const server_id_t &server_id);

#endif /* RPC_CONNECTIVITY_SERVER_ID_HPP_ */

