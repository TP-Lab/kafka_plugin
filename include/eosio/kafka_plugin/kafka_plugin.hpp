/**
 *
 *
 */
#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>

#include <appbase/application.hpp>
#include <eosio/chain/controller.hpp>
#include <memory>

namespace eosio {
    using eosio::chain::controller;
    using std::unique_ptr;
    using namespace appbase;

/**
 * Provides persistence to kafka for:
 * transaction_traces
 * transactions
 *
 *   See data dictionary (DB Schema Definition - EOS API) for description of MongoDB schema.
 *
 *   If cmake -DBUILD_kafka_plugin=true  not specified then this plugin not compiled/included.
 */
    class kafka_plugin : public plugin<kafka_plugin> {
    public:
        APPBASE_PLUGIN_REQUIRES((chain_plugin))

        kafka_plugin();

        virtual ~kafka_plugin();

        virtual void set_program_options(options_description &cli, options_description &cfg) override;

        void plugin_initialize(const variables_map&);
        void plugin_startup();

        void plugin_shutdown();

    private:
        unique_ptr<class kafka_plugin_impl> my;
    };

}

