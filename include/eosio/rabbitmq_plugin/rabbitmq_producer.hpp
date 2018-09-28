#pragma once
#include <amqp.h>
#include <amqp_ssl_socket.h>
#include <amqp_tcp_socket.h>
#include <memory>

// #define KAFKA_STATUS_OK 0
// #define KAFKA_STATUS_INIT_FAIL 1
// #define KAFKA_STATUS_MSG_INVALID 2
// #define KAFKA_STATUS_QUEUE_FULL 3

#define RABBITMQ_TRX_ACCEPT 0
#define RABBITMQ_TRX_APPLIED 1

namespace eosio {

class rabbitmq_producer {
    public:
        rabbitmq_producer() {


        };
        
        int trx_rabbitmq_init(char *brokers, char *acceptopic, char *appliedtopic);

        int trx_rabbitmq_sendmsg(int trxtype, char *msgstr);

        int trx_rabbitmq_destroy(void);

    private:


        amqp_connection_state_t conn;
        amqp_socket_t *socket = NULL;


    };
}

