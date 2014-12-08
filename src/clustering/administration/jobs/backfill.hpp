// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_JOBS_BACKFILL_HPP_
#define CLUSTERING_ADMINISTRATION_JOBS_BACKFILL_HPP_

#include <iostream> // REMOVE
#include <vector>

#include "clustering/administration/tables/table_metadata.hpp"
#include "concurrency/watchable_map.hpp"
#include "containers/uuid.hpp"
#include "rpc/mailbox/mailbox.hpp"
#include "rpc/connectivity/peer_id.hpp"

class backfill_job_t {
public:
    backfill_job_t(peer_id_t const &_destination,
                   namespace_id_t const &_table,
                   peer_id_t const &_source,
                   reactor_activity_id_t const &_reactor_activity_id,
                   backfill_session_id_t const &_backfill_session_id)
        : destination(_destination),
          table(_table),
          source(_source),
          reactor_activity_id(_reactor_activity_id),
          backfill_session_id(_backfill_session_id) { }

    reactor_activity_id_t const & get_reactor_activity_id() const {
        return reactor_activity_id;
    }

    backfill_session_id_t const & get_backfill_session_id() const {
        return backfill_session_id;
    }

    std::pair<peer_id_t, namespace_id_t> get_source_key() const {
        return std::make_pair(source, table);
    }

    void set_progress(std::pair<int, int> const &progress) {
        progress_numerator = progress.first;
        progress_denominator = progress.second;

        std::cout << progress_numerator << " / " << progress_denominator << std::endl;
    }

private:
    peer_id_t destination;
    namespace_id_t table;
    peer_id_t source;
    reactor_activity_id_t reactor_activity_id;
    backfill_session_id_t backfill_session_id;
    int64_t progress_numerator, progress_denominator;
};

std::vector<backfill_job_t> get_all_backfill_jobs(
    watchable_map_t<std::pair<peer_id_t, namespace_id_t>,
        namespace_directory_metadata_t> *reactor_directory_view);

void get_all_backfill_jobs_progress(
    watchable_map_t<std::pair<peer_id_t, namespace_id_t>,
        namespace_directory_metadata_t> *reactor_directory_view,
    mailbox_manager_t *mailbox_manager,
    std::vector<backfill_job_t> *backfill_jobs_out);

#endif /* CLUSTERING_ADMINISTRATION_JOBS_BACKFILL_HPP_ */
