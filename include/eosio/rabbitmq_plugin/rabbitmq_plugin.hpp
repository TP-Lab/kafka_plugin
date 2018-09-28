/**
 *
 *
 */
#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

namespace eosio {

using rabbitmq_plugin_impl_ptr = std::shared_ptr<class rabbitmq_plugin_impl>;

/**
 * Provides persistence to MongoDB for:
 * accounts
 * actions
 * block_states
 * blocks
 * transaction_traces
 * transactions
 *
 *   See data dictionary (DB Schema Definition - EOS API) for description of MongoDB schema.
 *
 *   If cmake -DBUILD_rabbitmq_plugin=true  not specified then this plugin not compiled/included.
 */
class rabbitmq_plugin : public plugin<rabbitmq_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))

   rabbitmq_plugin();
   virtual ~rabbitmq_plugin();

   virtual void set_program_options(options_description& cli, options_description& cfg) override;

   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   rabbitmq_plugin_impl_ptr my;
};

}

