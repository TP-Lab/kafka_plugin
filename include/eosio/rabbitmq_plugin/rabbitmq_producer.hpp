#pragma once
#include <amqp.h>
#include <amqp_ssl_socket.h>
#include <amqp_tcp_socket.h>
#include <memory>
#include <eosio/chain/exceptions.hpp>

namespace eosio {

FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_exception, chain::plugin_exception,
                              3119000, "rabbitmq_plugin exception" )
                              
FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_open_connection_exception, rabbitmq_plugin_exception,
                              3119001, "rabbitmq_plugin open connection exception" )                              

FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_open_channel_exception, rabbitmq_plugin_exception,
                              3119002, "rabbitmq_plugin open channel exception" )
                              
FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_closed_connection_exception, rabbitmq_plugin_exception,
                              3119003, "rabbitmq_plugin closed connection exception" )                              

FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_closed_channel_exception, rabbitmq_plugin_exception,
                              3119004, "rabbitmq_plugin closed channel exception" )
                              
FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_library_exception, rabbitmq_plugin_exception,
                              3119005, "rabbitmq_plugin library exception" )

FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_no_reply_exception, rabbitmq_plugin_exception,
                              3119006, "rabbitmq_plugin missing reply exception" )

FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_unknown_exception, rabbitmq_plugin_exception,
                              3119007, "rabbitmq_plugin unknown exception" )
                              
FC_DECLARE_DERIVED_EXCEPTION( rabbitmq_plugin_publish_exception, rabbitmq_plugin_exception,
                              3119008, "rabbitmq_plugin publish exception" )                              
                              
                              
                              
class rabbitmq_producer {
    public:
        rabbitmq_producer() {


        };
        
        int trx_rabbitmq_init(std::string hostname, int port, std::string username, std::string password);

        void trx_rabbitmq_sendmsg(std::string routingKey, std::string exchange, std::string msgstr);

        void trx_rabbitmq_destroy();

    private:


        amqp_connection_state_t conn;
        amqp_socket_t *socket = NULL;

        std::string m_password = "";
        std::string m_username = "";
        std::string m_hostname = "";
        int m_port = 0;

    };
}

