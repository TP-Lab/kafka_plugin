#pragma once

#include "rdkafka.h"

namespace eosio {
#define KAFKA_STATUS_OK 0
#define KAFKA_STATUS_INIT_FAIL 1
#define KAFKA_STATUS_MSG_INVALID 2
#define KAFKA_STATUS_QUEUE_FULL 3

#define KAFKA_TRX_ACCEPT 0
#define KAFKA_TRX_APPLIED 1

class kafka_producer {
    public:
        kafka_producer() {

            accept_rk = NULL;
            applied_rk = NULL;
            accept_rkt = NULL;
            applied_rkt = NULL;
            accept_conf = NULL;
            applied_conf = NULL;
        };

        int trx_kafka_init(char *brokers, char *acceptopic, char *appliedtopic);

        int trx_kafka_sendmsg(int trxtype, char *msgstr);

        int trx_kafka_destroy(void);

    private:
        rd_kafka_t *accept_rk;            /*Producer instance handle*/
        rd_kafka_t *applied_rk;            /*Producer instance handle*/
        rd_kafka_topic_t *accept_rkt;     /*topic object*/
        rd_kafka_topic_t *applied_rkt;     /*topic object*/
        rd_kafka_conf_t *accept_conf;     /*kafka config*/
        rd_kafka_conf_t *applied_conf;     /*kafka config*/

        static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque){}
    };
}

