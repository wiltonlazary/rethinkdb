#include "unittest/clustering_utils_raft.hpp"

namespace unittest {

void do_writes_raft(dummy_raft_cluster_t *cluster, int expect, int ms) {
#ifdef ENABLE_RAFT_DEBUG
    RAFT_DEBUG("begin do_writes(%d, %d)\n", expect, ms);
    microtime_t start = current_microtime();
#endif /* ENABLE_RAFT_DEBUG */
    std::set<uuid_u> committed_changes;
    signal_timer_t timer;
    timer.start(ms);
    try {
        while (static_cast<int>(committed_changes.size()) < expect) {
            uuid_u change = generate_uuid();
            raft_member_id_t leader = cluster->find_leader(&timer);
            bool ok = cluster->try_change(leader, change, &timer);
            if (ok) {
                committed_changes.insert(change);
            }
        }
        raft_member_id_t leader = cluster->find_leader(&timer);
        cluster->run_on_member(leader, [&](dummy_raft_member_t *m, signal_t *) {
            std::set<uuid_u> all_changes;
            for (const uuid_u &change : m->get_committed_state()->get().state.state) {
                all_changes.insert(change);
            }
            for (const uuid_u &change : committed_changes) {
                ASSERT_EQ(1, all_changes.count(change));
            }
        });
    } catch (const interrupted_exc_t &) {
        ADD_FAILURE() << "completed only " << committed_changes.size() << "/" << expect
            << " changes in " << ms << "ms";
    }
    RAFT_DEBUG("end do_writes() in %" PRIu64 "ms\n", (current_microtime() - start) / 1000);
}

} // namespace unittest

