// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "btree/keys.hpp"

#include "debug.hpp"
#include "utils.hpp"

std::string key_range_t::print() const {
    printf_buffer_t buf;
    debug_print(&buf, *this);
    return buf.c_str();
}

// fast-ish non-null terminated string comparison
int sized_strcmp(const uint8_t *str1, int len1, const uint8_t *str2, int len2) {
    int min_len = std::min(len1, len2);
    int res = memcmp(str1, str2, min_len);
    if (res == 0) {
        res = len1 - len2;
    }
    return res;
}

bool unescaped_str_to_key(const char *str, int len, store_key_t *buf) {
    if (len <= MAX_KEY_SIZE) {
        memcpy(buf->contents(), str, len);
        buf->set_size(uint8_t(len));
        return true;
    } else {
        return false;
    }
}

std::string key_to_unescaped_str(const store_key_t &key) {
    return std::string(reinterpret_cast<const char *>(key.contents()), key.size());
}

std::string key_to_debug_str(const store_key_t &key) {
    std::string s;
    s.push_back('"');
    for (int i = 0; i < key.size(); i++) {
        uint8_t c = key.contents()[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
            s.push_back(c);
        } else {
            s.push_back('\\');
            s.push_back('x');
            s.push_back(int_to_hex((c & 0xf0) >> 4));
            s.push_back(int_to_hex(c & 0x0f));
        }
    }
    s.push_back('"');
    return s;
}

std::string key_to_debug_str(const btree_key_t *key) {
    return key_to_debug_str(store_key_t(key));
}

key_range_t::key_range_t() :
    left(), right(store_key_t()) { }

key_range_t::key_range_t(bound_t lm, const store_key_t& l,
                         bound_t rm, const store_key_t& r) {
    init(lm, l.btree_key(), rm, r.btree_key());
}

key_range_t::key_range_t(bound_t lm, const btree_key_t *l,
                         bound_t rm, const btree_key_t *r) {
    init(lm, l, rm, r);
}

void key_range_t::init(bound_t lm, const btree_key_t *l,
                       bound_t rm, const btree_key_t *r) {
    left.assign(l);
    switch (lm) {
    case closed: break;
    case open:
        if (!left.increment()) {
            // `l` is the largest key, so make sure that the resulting range
            // will be empty.
            rassert(rm == open);
            rassert(btree_key_cmp(l, r) == 0);
        }
        break;
    default: unreachable();
    }

    right.assign(r);
    switch (rm) {
    case closed: {
        bool ok = right.increment();
        guarantee(ok);
        break;
    }
    case open: break;
    default: unreachable();
    }

    rassert(left <= right,
            "left_key(%d)=%.*s, right_key(%d)=%.*s",
            left.size(), left.size(), left.contents(),
            right.size(), right.size(), right.contents());
}

bool key_range_t::is_superset(const key_range_t &other) const {
    /* Special-case empty ranges */
    if (other.is_empty()) return true;
    if (left > other.left) return false;
    if (right < other.right) return false;
    return true;
}

bool key_range_t::overlaps(const key_range_t &other) const {
    // TODO: do we need the `is_empty` checks?
    return left < other.right && other.left < right && !is_empty() && !other.is_empty();
}

key_range_t key_range_t::intersection(const key_range_t &other) const {
    if (!overlaps(other)) {
        return key_range_t::empty();
    }
    key_range_t ixn;
    ixn.left = left < other.left ? other.left : left;
    ixn.right = right > other.right ? other.right : right;
    return ixn;
}

void debug_print(printf_buffer_t *buf, const btree_key_t *k) {
    if (k != nullptr) {
        debug_print_quoted_string(buf, k->contents, k->size);
    } else {
        buf->appendf("NULL");
    }
}

void debug_print(printf_buffer_t *buf, const store_key_t &k) {
    debug_print(buf, k.btree_key());
}

void debug_print(printf_buffer_t *buf, const key_range_t &kr) {
    buf->appendf("[");
    debug_print(buf, kr.left);
    buf->appendf(", ");
    debug_print(buf, kr.right);
    buf->appendf(")");
}

std::string key_range_to_string(const key_range_t &kr) {
    std::string res;
    res += "[";
    res += key_to_debug_str(kr.left);
    res += ", ";
    res += key_to_debug_str(kr.right);
    res += ")";
    return res;
}

void debug_print(printf_buffer_t *buf, const store_key_t *k) {
    if (k) {
        debug_print(buf, *k);
    } else {
        buf->appendf("NULL");
    }
}

bool operator==(key_range_t a, key_range_t b) THROWS_NOTHING {
    return a.left == b.left && a.right == b.right;
}

bool operator!=(key_range_t a, key_range_t b) THROWS_NOTHING {
    return !(a == b);
}

bool operator<(const key_range_t &a, const key_range_t &b) THROWS_NOTHING {
    return (a.left < b.left || (a.left == b.left && a.right < b.right));
}

// It used to be possible for key ranges to have unbounded right bounds.  We now
// use `store_key_t::max()` for this instead, and forbid it as a legal key
// value.  This makes a lot of logic simpler.  We continue to use the old
// serialization format for backward compatibility since it only uses one extra
// byte.
template<cluster_version_t W>
void serialize(write_message_t *wm, const key_range_t &kr) {
    serialize<W>(wm, kr.left);
    serialize<W>(wm, false); // This used to be `true` if the right bound was unbounded.
    serialize<W>(wm, kr.right);
}
template<cluster_version_t W>
archive_result_t deserialize(read_stream_t *s, key_range_t *kr) {
    archive_result_t res = deserialize<W>(s, &kr->left);
    if (bad(res)) return res;
    bool unbounded;
    res = deserialize<W>(s, &unbounded);
    if (bad(res)) return res;
    if (unbounded) {
        // We only enter this branch if we're deserializing an old key range.
        // We convert it to the new convention silently.
        kr->right = store_key_t::max();
        // We used to serialize and deserialize junk data in the old format for
        // some reason.
        store_key_t junk_key;
        res = deserialize<W>(s, &junk_key);
    } else {
        res = deserialize<W>(s, &kr->right);
    }
    return res;
}

void serialize_for_metainfo(write_message_t *wm, const key_range_t &kr) {
    kr.left.serialize_for_metainfo(wm);
    serialize_universal(wm, false);
    kr.right.serialize_for_metainfo(wm);
}

archive_result_t deserialize_for_metainfo(read_stream_t *s, key_range_t *out) {
    archive_result_t res = out->left.deserialize_for_metainfo(s);
    if (bad(res)) { return res; }
    bool unbounded;
    res = deserialize_universal(s, &unbounded);
    if (bad(res)) { return res; }
    if (unbounded) {
        store_key_t junk_key;
        res = junk_key.deserialize_for_metainfo(s);
    } else {
        res = out->left.deserialize_for_metainfo(s);
    }
    return res;
}
