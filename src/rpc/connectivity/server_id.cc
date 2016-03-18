// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rpc/connectivity/server_id.hpp"

server_id_t server_id_t::generate_proxy_id() {
    server_id_t res;
    res.uuid = generate_uuid();
    // We take the UUID and flag it in one of the reserved bits of version 4 UUIDs,
    // depending on whether the ID is for a proxy or not.
    // The reason for this is so that we don't break compatibility with older servers
    // that expect a plain `uuid_u`.
    guarantee((res.uuid.data()[8] & 0x80) == 0x80);
    res.uuid.data()[8] = res.uuid.data()[8] ^ 0x80;
    return res;
}

server_id_t server_id_t::generate_server_id() {
    server_id_t res;
    res.uuid = generate_uuid();
    guarantee((res.uuid.data()[8] & 0x80) == 0x80);
    return res;
}

server_id_t server_id_t::from_uuid(uuid_u _uuid) {
    server_id_t res;
    res.uuid = _uuid;
    return res;
}

bool server_id_t::is_proxy() const {
    return (uuid.data()[8] & 0x80) == 0;
}

std::string server_id_t::print() const {
    if (is_proxy()) {
        return "proxy-" + uuid_to_str(uuid);
    } else {
        return uuid_to_str(uuid);
    }
}

RDB_IMPL_SERIALIZABLE_1_SINCE_v1_13(server_id_t, uuid);

// Universal serialization functions: you MUST NOT change their implementations.
// (You could find a way to remove these functions, though.)
void serialize_universal(write_message_t *wm, const server_id_t &server_id) {
    serialize_universal(wm, server_id.get_uuid());
}
archive_result_t deserialize_universal(read_stream_t *s, server_id_t *server_id) {
    uuid_u uuid;
    archive_result_t res = deserialize_universal(s, &uuid);
    if (bad(res)) { return res; }
    *server_id = server_id_t::from_uuid(uuid);
    return archive_result_t::SUCCESS;
}

void debug_print(printf_buffer_t *buf, const server_id_t &server_id) {
    buf->appendf("%s", server_id.print().c_str());
}


