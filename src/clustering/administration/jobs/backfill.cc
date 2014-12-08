// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/jobs/backfill.hpp"

#include <boost/optional.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>
#include <iostream>  // REMOVE

#include "clustering/reactor/metadata.hpp"
#include "rpc/connectivity/peer_id.hpp"

class extract_backfill_jobs_t
    : public boost::static_visitor<> {
    typedef reactor_business_card_details::primary_when_safe_t primary_when_safe_t;
    typedef reactor_business_card_details::secondary_backfilling_t
        secondary_backfilling_t;

public:
    extract_backfill_jobs_t(peer_id_t const &_destination,
                            namespace_id_t const &_table,
                            std::vector<backfill_job_t> *_backfill_jobs_out)
        : destination(_destination),
          table(_table),
          backfill_jobs_out(_backfill_jobs_out) { }

    template <typename T>
    result_type operator()(T const &) const {
    }

    result_type operator()(primary_when_safe_t const &value) const {
        for (auto const &backfill : value.backfills_waited_on) {
            backfill_jobs_out->emplace_back(destination,
                                            table,
                                            backfill.peer_id,
                                            backfill.activity_id,
                                            backfill.backfill_session_id);
        }
    }

    result_type operator()(secondary_backfilling_t const &value) const {
        backfill_jobs_out->emplace_back(destination,
                                        table,
                                        value.backfill.peer_id,
                                        value.backfill.activity_id,
                                        value.backfill.backfill_session_id);
    }

private:
    peer_id_t const &destination;
    namespace_id_t const &table;
    std::vector<backfill_job_t> *backfill_jobs_out;
};

std::vector<backfill_job_t> get_all_backfill_jobs(
    watchable_map_t<std::pair<peer_id_t, namespace_id_t>,
        namespace_directory_metadata_t> *reactor_directory_view) {
    std::vector<backfill_job_t> backfill_jobs;

    reactor_directory_view->read_all(
        [&](std::pair<peer_id_t, namespace_id_t> const &key,
            namespace_directory_metadata_t const *value) {
        for (auto const &activity_id_entry_pair : value->internal->activities) {
            extract_backfill_jobs_t visitor(key.first, key.second, &backfill_jobs);
            boost::apply_visitor(visitor, activity_id_entry_pair.second.activity);
        }
    });

    return backfill_jobs;
}

class extract_backfiller_business_card_t
    : public boost::static_visitor<boost::optional<backfiller_business_card_t>> {
public:
    typedef reactor_business_card_details::primary_t primary_t;
    typedef reactor_business_card_details::secondary_up_to_date_t secondary_up_to_date_t;
    typedef reactor_business_card_details::secondary_without_primary_t
        secondary_without_primary_t;
    typedef reactor_business_card_details::nothing_when_safe_t nothing_when_safe_t;

    template <typename T>
    result_type operator()(T const &) const {
        return boost::none;
    }

    result_type operator()(primary_t const &value) const {
        if (static_cast<bool>(value.replier)) {
            return value.replier.get().backfiller_bcard;
        }
        return boost::none;
    }

    result_type operator()(secondary_up_to_date_t const &value) const {
        return value.replier.backfiller_bcard;
    }

    result_type operator()(secondary_without_primary_t const &value) {
        return value.backfiller;
    }

    result_type operator()(nothing_when_safe_t const &value) const {
        return value.backfiller;
    }
};

void get_all_backfill_jobs_progress(
    watchable_map_t<std::pair<peer_id_t, namespace_id_t>,
        namespace_directory_metadata_t> *reactor_directory_view,
    mailbox_manager_t *mailbox_manager,
    std::vector<backfill_job_t> *backfill_jobs_out) {
    // TODO `throttled_pmap`
    pmap(backfill_jobs_out->begin(), backfill_jobs_out->end(),
        [&](backfill_job_t &backfill_job) {
        boost::optional<backfiller_business_card_t> backfiller_business_card;

        reactor_directory_view->read_key(backfill_job.get_source_key(),
            [&](namespace_directory_metadata_t const *value) {
            auto activity_it = value->internal->activities.find(
                backfill_job.get_reactor_activity_id());
            if (activity_it != value->internal->activities.end()) {
                extract_backfiller_business_card_t visitor;
                backfiller_business_card =
                    boost::apply_visitor(visitor, activity_it->second.activity);
            }
        });

        if (static_cast<bool>(backfiller_business_card)) {
            cond_t returned_progress;
            // disconnect_watcher_t disconnect_watcher(mailbox_manager, peer.first);

            mailbox_t<void(std::pair<int, int>)> return_mailbox(
                mailbox_manager,
                [&](UNUSED signal_t *, std::pair<int, int> const &progress) {
                    backfill_job.set_progress(progress);
                    returned_progress.pulse();
            });
            send(mailbox_manager,
                 backfiller_business_card.get().request_progress_mailbox,
                 backfill_job.get_backfill_session_id(),
                 return_mailbox.get_address());

            wait_any_t waiter(&returned_progress /*, &disconnect_watcher, interruptor */);
            waiter.wait();
        } else {
            // TODO, error
        }
    });
}
