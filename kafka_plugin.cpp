/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
//
#include <stdlib.h>
#include <eosio/kafka_plugin/kafka_producer.hpp>
#include <eosio/kafka_plugin/kafka_plugin.hpp>

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>
#include <eosio/chain/name.hpp>
#include <eosio/chain/abi_serializer.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <thread>
#include <queue>

namespace fc { class variant; }

namespace eosio {
    using chain::name;
    using chain::account_name;
    using chain::action_name;
    using chain::block_id_type;
    using chain::permission_name;
    using chain::transaction;
    using chain::signed_transaction;
    using chain::signed_block;
    using chain::transaction_id_type;
    using chain::packed_transaction;
    using chain::permission_level;
    static auto _kafka_plugin = application::register_plugin<kafka_plugin>();
    using kafka_producer_ptr = std::shared_ptr<class kafka_producer>;

    class kafka_plugin_impl {
    public:
        kafka_plugin_impl();

        ~kafka_plugin_impl();

        std::optional <boost::signals2::scoped_connection> accepted_block_connection;
        std::optional <boost::signals2::scoped_connection> irreversible_block_connection;
        std::optional <boost::signals2::scoped_connection> accepted_transaction_connection;
        std::optional <boost::signals2::scoped_connection> applied_transaction_connection;
        chain_plugin *chain_plug;
        struct action_info {
            account_name account;
            action_name name;
            vector <permission_level> authorization;
            string data_json;
        };

        struct trasaction_info_st {
            uint64_t block_number;
            fc::time_point block_time;
            std::optional <chain::chain_id_type> chain_id;
            chain::transaction_trace_ptr trace;
            vector <action_info> action_vec;

        };


        void consume_blocks();

        void accepted_block(const chain::block_state_ptr &);

        void applied_irreversible_block(const chain::block_state_ptr &);

        void accepted_transaction(const chain::transaction_metadata_ptr &);

        void applied_transaction(const chain::transaction_trace_ptr &);

        void process_accepted_transaction(const chain::transaction_metadata_ptr &);

        void _process_accepted_transaction(const chain::transaction_metadata_ptr &);

        void process_applied_transaction(const trasaction_info_st &);

        void _process_applied_transaction(const trasaction_info_st &);

        void process_accepted_block(const chain::block_state_ptr &);

        void _process_accepted_block(const chain::block_state_ptr &);

        void process_irreversible_block(const chain::block_state_ptr &);

        void _process_irreversible_block(const chain::block_state_ptr &);

        void kafka_msg_flush();

        void _kafka_msg_flush();

        void init();

        bool configured{false};

        void filter_traction_trace(const chain::transaction_trace_ptr trace, action_name act_name);

        void _process_trace(vector<chain::action_trace>::iterator action_trace_ptr, action_name act_name);

        template<typename Queue, typename Entry>
        void queue(Queue &queue, const Entry &e);

        uint32_t start_block_num = 0;
        bool start_block_reached = false;

        size_t max_queue_size = 10000;
        int queue_sleep_time = 0;
        std::deque <chain::transaction_metadata_ptr> transaction_metadata_queue;
        std::deque <chain::transaction_metadata_ptr> transaction_metadata_process_queue;
        std::deque <trasaction_info_st> transaction_trace_queue;
        std::deque <trasaction_info_st> transaction_trace_process_queue;
        std::deque <chain::block_state_ptr> block_state_queue;
        std::deque <chain::block_state_ptr> block_state_process_queue;
        std::deque <chain::block_state_ptr> irreversible_block_state_queue;
        std::deque <chain::block_state_ptr> irreversible_block_state_process_queue;
        std::mutex mtx;
        std::condition_variable condition;
        std::thread consume_thread;
        std::atomic_bool done{false};
        std::atomic_bool startup{true};
        std::optional <chain::chain_id_type> chain_id;
        fc::microseconds abi_serializer_max_time = fc::seconds(10);;

        static const account_name newaccount;
        static const account_name setabi;

        static const std::string block_states_col;
        static const std::string blocks_col;
        static const std::string trans_col;
        static const std::string trans_traces_col;
        static const std::string actions_col;
        static const std::string accounts_col;
        kafka_producer_ptr producer;
    };

    const account_name kafka_plugin_impl::newaccount = chain::newaccount::get_name();
    const account_name kafka_plugin_impl::setabi = chain::setabi::get_name();

    const std::string kafka_plugin_impl::block_states_col = "block_states";
    const std::string kafka_plugin_impl::blocks_col = "blocks";
    const std::string kafka_plugin_impl::trans_col = "transactions";
    const std::string kafka_plugin_impl::trans_traces_col = "transaction_traces";
    const std::string kafka_plugin_impl::actions_col = "actions";
    const std::string kafka_plugin_impl::accounts_col = "accounts";

    template<typename Queue, typename Entry>
    void kafka_plugin_impl::queue(Queue &queue, const Entry &e) {
        std::unique_lock <std::mutex> lock(mtx);
        auto queue_size = queue.size();
        if (queue_size > max_queue_size) {
            lock.unlock();
            condition.notify_one();
            queue_sleep_time += 10;
            if (queue_sleep_time > 1000)
                wlog("queue size: ${q}", ("q", queue_size));
            std::this_thread::sleep_for(std::chrono::milliseconds(queue_sleep_time));
            lock.lock();
        } else {
            queue_sleep_time -= 10;
            if (queue_sleep_time < 0) queue_sleep_time = 0;
        }
        queue.emplace_back(e);
        lock.unlock();
        condition.notify_one();
    }

    void kafka_plugin_impl::accepted_transaction(const chain::transaction_metadata_ptr &t) {
        try {
            queue(transaction_metadata_queue, t);
        } catch (fc::exception &e) {
            elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while accepted_transaction");
        }
    }

    void kafka_plugin_impl::applied_transaction(const chain::transaction_trace_ptr &t) {
        if (!t->producer_block_id.has_value())
            return;
        try {
            auto &chain = chain_plug->chain();
            trasaction_info_st transactioninfo = trasaction_info_st{
                    .block_number = t->block_num,//chain.pending_block_state()->block_num,
                    .block_time = chain.pending_block_time(),
                    .chain_id = this->chain_id,
                    .trace =chain::transaction_trace_ptr(t),
            };
            transactioninfo.block_time = chain.pending_block_time();
            trasaction_info_st &info_t = transactioninfo;
            //elog("###trxId = ${e}", ("e", t->id));
            queue(transaction_trace_queue, info_t);
        } catch (fc::exception &e) {
            elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while applied_transaction");
        }
    }

    void kafka_plugin_impl::applied_irreversible_block(const chain::block_state_ptr &bs) {
        try {
            queue(irreversible_block_state_queue, bs);
        } catch (fc::exception &e) {
            elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while applied_irreversible_block");
        }
    }


    void kafka_plugin_impl::accepted_block(const chain::block_state_ptr &bs) {
        try {
            queue(block_state_queue, bs);
        } catch (fc::exception &e) {
            elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while accepted_block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while accepted_block");
        }
    }

    void kafka_plugin_impl::consume_blocks() {
        try {

            while (true) {
                std::unique_lock <std::mutex> lock(mtx);
                while (transaction_metadata_queue.empty() &&
                       transaction_trace_queue.empty() &&
                       block_state_queue.empty() &&
                       irreversible_block_state_queue.empty() &&
                       !done) {
                    condition.wait(lock);
                }
                // capture for processing
                size_t transaction_metadata_size = transaction_metadata_queue.size();
                if (transaction_metadata_size > 0) {
                    transaction_metadata_process_queue = move(transaction_metadata_queue);
                    transaction_metadata_queue.clear();
                }
                size_t transaction_trace_size = transaction_trace_queue.size();
                if (transaction_trace_size > 0) {
                    transaction_trace_process_queue = move(transaction_trace_queue);
                    transaction_trace_queue.clear();
                }

                size_t block_state_size = block_state_queue.size();
                if (block_state_size > 0) {
                    block_state_process_queue = move(block_state_queue);
                    block_state_queue.clear();
                }
                size_t irreversible_block_size = irreversible_block_state_queue.size();
                if (irreversible_block_size > 0) {
                    irreversible_block_state_process_queue = move(irreversible_block_state_queue);
                    irreversible_block_state_queue.clear();
                }

                lock.unlock();

                // warn if queue size greater than 75%
                if (transaction_metadata_size > (max_queue_size * 0.75) ||
                    transaction_trace_size > (max_queue_size * 0.75) ||
                    block_state_size > (max_queue_size * 0.75) ||
                    irreversible_block_size > (max_queue_size * 0.75)) {
                    wlog("queue size: ${q}", ("q", transaction_metadata_size + transaction_trace_size ));
                } else if (done) {
                    ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size));
                }

                // process transactions
                while (!transaction_metadata_process_queue.empty()) {
                    const auto &t = transaction_metadata_process_queue.front();
                    process_accepted_transaction(t);
                    transaction_metadata_process_queue.pop_front();
                }

                while (!transaction_trace_process_queue.empty()) {
                    const auto &t = transaction_trace_process_queue.front();
                    process_applied_transaction(t);
                    transaction_trace_process_queue.pop_front();
                }

                // process blocks
                while (!block_state_process_queue.empty()) {
                    const auto &bs = block_state_process_queue.front();
                    process_accepted_block(bs);
                    block_state_process_queue.pop_front();
                }

                // process irreversible blocks
                while (!irreversible_block_state_process_queue.empty()) {
                    const auto &bs = irreversible_block_state_process_queue.front();
                    process_irreversible_block(bs);
                    irreversible_block_state_process_queue.pop_front();
                }

                kafka_msg_flush();

                if (transaction_metadata_size == 0 &&
                    transaction_trace_size == 0 &&
                    block_state_size == 0 &&
                    irreversible_block_size == 0 &&
                    done) {
                    break;
                }
            }
            ilog("kafka_plugin consume thread shutdown gracefully");
        } catch (fc::exception &e) {
            elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while consuming block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while consuming block");
        }
    }


    void kafka_plugin_impl::process_accepted_transaction(const chain::transaction_metadata_ptr &t) {
        try {
            // always call since we need to capture setabi on accounts even if not storing transactions
            _process_accepted_transaction(t);
        } catch (fc::exception &e) {
            elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing accepted transaction metadata");
        }
    }

    void kafka_plugin_impl::process_applied_transaction(const trasaction_info_st &t) {
        try {
            if (start_block_reached) {
                _process_applied_transaction(t);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing applied transaction trace");
        }
    }


    void kafka_plugin_impl::process_irreversible_block(const chain::block_state_ptr &bs) {
        try {
            if (start_block_reached) {
                _process_irreversible_block(bs);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing irreversible block");
        }
    }

    void kafka_plugin_impl::kafka_msg_flush() {
        try {
            if (start_block_reached) {
                _kafka_msg_flush();
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing irreversible block");
        }
    }

    void kafka_plugin_impl::process_accepted_block(const chain::block_state_ptr &bs) {
        try {
            if (!start_block_reached) {
                if (bs->block_num >= start_block_num) {
                    start_block_reached = true;
                }
            }
            if (start_block_reached) {
                _process_accepted_block(bs);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing accepted block trace");
        }
    }

    void kafka_plugin_impl::_process_accepted_transaction(const chain::transaction_metadata_ptr &t) {
        const auto &trx = t->packed_trx();
        string trx_json = fc::json::to_string(trx, fc::time_point::maximum());
        //elog("trx_json: ${e}",("e",trx_json));
        producer->trx_kafka_sendmsg(KAFKA_TRX_ACCEPT, (char *) trx_json.c_str());
    }

    void kafka_plugin_impl::_process_applied_transaction(const trasaction_info_st &t) {
        uint64_t time = (t.block_time.time_since_epoch().count() / 1000);
        //elog("trxId = ${e}", ("e", t.trace->id));
        string transaction_metadata_json =
                "{\"block_number\":" + std::to_string(t.block_number) + ",\"block_time\":" + std::to_string(time) +
                ",\"chain_id\":\"" + t.chain_id->str() +
                "\",\"trace\":" + fc::json::to_string(t.trace, fc::time_point::maximum()).c_str() + "}";
        producer->trx_kafka_sendmsg(KAFKA_TRX_APPLIED, (char *) transaction_metadata_json.c_str());
        // elog("transaction_metadata_json = ${e}",("e",transaction_metadata_json));

        if (producer->trx_kafka_get_topic(KAFKA_TRX_TRANSFER) != NULL) {
            filter_traction_trace(t.trace, name("transfer"));
            if (t.trace->action_traces.size() > 0) {
                string transfer_json =
                        "{\"block_number\":" + std::to_string(t.block_number) + ",\"block_time\":" +
                        std::to_string(time) +
                        ",\"chain_id\":" + "\"" + t.chain_id->str() + "\"" +
                        ",\"trace\":" + fc::json::to_string(t.trace, fc::time_point::maximum()).c_str() + "}";
                producer->trx_kafka_sendmsg(KAFKA_TRX_TRANSFER, (char *) transfer_json.c_str());
                //elog("transfer_json = ${e}",("e",transfer_json));
            }
        }
    }

    void
    kafka_plugin_impl::_process_trace(vector<chain::action_trace>::iterator action_trace_ptr, action_name act_name) {
        /*auto inline_trace_ptr = action_trace_ptr->inline_traces.begin();
        for(;inline_trace_ptr!=action_trace_ptr->inline_traces.end();inline_trace_ptr++){
            //elog("inline action:");
            _process_trace(inline_trace_ptr,act_name);
        }*/

        if (action_trace_ptr->act.name == act_name) {
            auto readonly = chain_plug->get_read_only_api(abi_serializer_max_time);
            chain_apis::read_only::get_code_params get_code_params;
            get_code_params.account_name = action_trace_ptr->act.account;

            auto get_code_results = readonly.get_code(get_code_params, fc::time_point::now() + abi_serializer_max_time);
            if (!get_code_results.abi.has_value()){
                return;
            }
            auto abi = std::make_shared<chain::abi_serializer>(std::move(get_code_results.abi.value()), chain::abi_serializer::create_yield_function(abi_serializer_max_time));
            auto data_variant = abi->binary_to_variant(abi->get_action_type(action_trace_ptr->act.name), action_trace_ptr->act.data, chain::abi_serializer::create_yield_function(abi_serializer_max_time));
            string data_str = fc::json::to_string(data_variant, fc::time_point::maximum());
            action_info action_info1 = {
                    .account = action_trace_ptr->act.account,
                    .name = action_trace_ptr->act.name,
                    .authorization = action_trace_ptr->act.authorization,
                    .data_json = data_str
            };

            action_trace_ptr->act.data.resize(data_str.size());
            action_trace_ptr->act.data.assign(data_str.begin(), data_str.end());
        }
    }

    void kafka_plugin_impl::filter_traction_trace(const chain::transaction_trace_ptr trace, action_name act_name) {
        vector<chain::action_trace>::iterator action_trace_ptr = trace->action_traces.begin();

        for (; action_trace_ptr != trace->action_traces.end();) {
            if (action_trace_ptr->act.name == act_name) {
                _process_trace(action_trace_ptr, act_name);
                action_trace_ptr++;
                continue;
            } else {
                trace->action_traces.erase(action_trace_ptr);
            }
        }
    }


    void kafka_plugin_impl::_process_accepted_block(const chain::block_state_ptr &bs) {
    }

    void kafka_plugin_impl::_process_irreversible_block(const chain::block_state_ptr &bs) {
    }

    void kafka_plugin_impl::_kafka_msg_flush() {
        producer->trx_kafka_flush(KAFKA_TRX_ACCEPT);
        producer->trx_kafka_flush(KAFKA_TRX_APPLIED);
        producer->trx_kafka_flush(KAFKA_TRX_TRANSFER);
    }


    kafka_plugin_impl::kafka_plugin_impl()
            : producer(new kafka_producer) {
    }

    kafka_plugin_impl::~kafka_plugin_impl() {
        if (!startup) {
            try {
                ilog("kafka_db_plugin shutdown in process please be patient this can take a few minutes");
                done = true;
                condition.notify_one();

                consume_thread.join();
                producer->trx_kafka_destroy();
            } catch (std::exception &e) {
                elog("Exception on kafka_plugin shutdown of consume thread: ${e}", ("e", e.what()));
            }
        }
    }

    void kafka_plugin_impl::init() {
        ilog("starting kafka plugin thread");
        consume_thread = std::thread([this] { consume_blocks(); });
        startup = false;
    }

////////////
// kafka_plugin
////////////

    kafka_plugin::kafka_plugin()
            : my(new kafka_plugin_impl) {
    }

    kafka_plugin::~kafka_plugin() {
    }

    void kafka_plugin::set_program_options(options_description &cli, options_description &cfg) {
        cfg.add_options()
                ("accept_trx_topic", bpo::value<std::string>(),
                 "The topic for accepted transaction.")
                ("applied_trx_topic", bpo::value<std::string>(),
                 "The topic for appiled transaction.")
                ("transfer_trx_topic", bpo::value<std::string>(),
                 "The topic for transfer transaction.")
                ("kafka-uri,k", bpo::value<std::string>(),
                 "the kafka brokers uri, as 192.168.31.225:9092")
                ("kafka-queue-size", bpo::value<uint32_t>()->default_value(256),
                 "The target queue size between nodeos and kafka plugin thread.")
                ("kafka-block-start", bpo::value<uint32_t>()->default_value(256),
                 "If specified then only abi data pushed to kafka until specified block is reached.")
                ("kafka-compression-codec", bpo::value<std::string>(),
                 "Compression codec to use for compressing message sets. This is the default value for all topics, may be overriden by the topic configuration property compression.codec.(none, gzip, snappy, lz4)");

    }

    void kafka_plugin::plugin_initialize(const variables_map &options) {
        char *accept_trx_topic = NULL;
        char *applied_trx_topic = NULL;
        char *transfer_trx_topic = NULL;
        char *brokers_str = NULL;
        char *compression_codec = NULL;
        try {
            if (options.count("kafka-uri")) {
                brokers_str = (char *) (options.at("kafka-uri").as<std::string>().c_str());
                elog("brokers_str:${j}", ("j", brokers_str));
                if (options.count("accept_trx_topic") != 0) {
                    accept_trx_topic = (char *) (options.at("accept_trx_topic").as<std::string>().c_str());
                    elog("accept_trx_topic:${j}", ("j", accept_trx_topic));
                }
                if (options.count("applied_trx_topic") != 0) {
                    applied_trx_topic = (char *) (options.at("applied_trx_topic").as<std::string>().c_str());
                    elog("applied_trx_topic:${j}", ("j", applied_trx_topic));
                }
                if (options.count("transfer_trx_topic") != 0) {
                    transfer_trx_topic = (char *) (options.at("transfer_trx_topic").as<std::string>().c_str());
                    elog("transfer_trx_topic:${j}", ("j", transfer_trx_topic));
                }
                if (options.count("kafka-compression-codec") != 0) {
                    compression_codec = (char *) (options.at("kafka-compression-codec").as<std::string>().c_str());
                    elog("kafka-compression-codec:${j}", ("j", compression_codec));
                }

                if (0 !=
                    my->producer->trx_kafka_init(brokers_str, accept_trx_topic, compression_codec, applied_trx_topic,
                                                 transfer_trx_topic)) {
                    elog("trx_kafka_init fail");
                } else {
                    elog("trx_kafka_init ok");
                }
            }

            if (options.count("kafka-uri")) {
                ilog("initializing kafka_plugin");
                my->configured = true;

                if (options.count("kafka-queue-size")) {
                    my->max_queue_size = options.at("kafka-queue-size").as<uint32_t>();
                }
                if (options.count("kafka-block-start")) {
                    my->start_block_num = options.at("kafka-block-start").as<uint32_t>();
                }
                if (my->start_block_num == 0) {
                    my->start_block_reached = true;
                }

                // hook up to signals on controller
                //chain_plugin* chain_plug = app().find_plugiin<chain_plugin>();
                my->chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
                auto &chain = my->chain_plug->chain();
                my->chain_id.emplace(chain.get_chain_id());

                my->accepted_block_connection.emplace(
                        chain.accepted_block.connect([&](const chain::block_state_ptr &bs) {
                            my->accepted_block(bs);
                        }));

                my->irreversible_block_connection.emplace(
                        chain.irreversible_block.connect([&](const chain::block_state_ptr &bs) {
                            my->applied_irreversible_block(bs);
                        }));

                my->accepted_transaction_connection.emplace(
                        chain.accepted_transaction.connect([&](const chain::transaction_metadata_ptr &t) {
                            my->accepted_transaction(t);
                        }));
                my->applied_transaction_connection.emplace(
                        chain.applied_transaction.connect(
                                [&](std::tuple<const std::shared_ptr <chain::transaction_trace> &, const std::shared_ptr<const chain::packed_transaction> &> t) {
                                    my->applied_transaction(std::get<0>(t));
                                }));
                my->init();
            } else {
                wlog("eosio::kafka_plugin configured, but no --kafka-uri specified.");
                wlog("kafka_plugin disabled.");
            }

        }

        FC_LOG_AND_RETHROW()
    }

    void kafka_plugin::plugin_startup() {
    }

    void kafka_plugin::plugin_shutdown() {
        my->accepted_block_connection.reset();
        my->irreversible_block_connection.reset();
        my->accepted_transaction_connection.reset();
        my->applied_transaction_connection.reset();
        my.reset();
    }

} // namespace eosio


