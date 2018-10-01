#pragma once

#include "rdkafka.h"

namespace eosio {
#define KAFKA_STATUS_OK 0
#define KAFKA_STATUS_INIT_FAIL 1
#define KAFKA_STATUS_MSG_INVALID 2
#define KAFKA_STATUS_QUEUE_FULL 3

#define KAFKA_ACCOUNT_CREATION 1
#define KAFKA_GENERAL_TRX 0

class kafka_producer {
    public:
        kafka_producer() {

            acc_rk = NULL;
            trx_rk = NULL;
            acc_rkt = NULL;
            trx_rkt = NULL;
            acc_conf = NULL;
            trx_conf = NULL;
        };

        int trx_kafka_init(char *brokers, char *acctopic, char *trxtopic);

        int trx_kafka_sendmsg(int trxtype, char *msgstr);

        int trx_kafka_destroy(void);

    private:
        rd_kafka_t *acc_rk;            /*Producer instance handle*/
        rd_kafka_t *trx_rk;            /*Producer instance handle*/
        rd_kafka_topic_t *acc_rkt;     /*topic object*/
        rd_kafka_topic_t *trx_rkt;     /*topic object*/
        rd_kafka_conf_t *acc_conf;     /*kafka config*/
        rd_kafka_conf_t *trx_conf;     /*kafka config*/

        static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque){}
    };
}

