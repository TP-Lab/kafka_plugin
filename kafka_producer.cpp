#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdexcept>
#include <unistd.h>

#include <eosio/kafka_plugin/kafka_producer.hpp>

 
/*
    The callback function is called once for each message, indicating that the message was successfully delivered (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR)
    Still failed to pass (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR)
    The callback function is triggered by rd_kafka_poll() and executed on the thread of the application.
 */
namespace eosio {

    int kafka_producer::trx_kafka_init(char *brokers, char *acceptopic, char *appliedtopic, MessageCallbackFunctionPtr msgDeliveredCallback) {
        char errstr[512];
        if (brokers == NULL) {
            return KAFKA_STATUS_INIT_FAIL;
        }

        if (acceptopic != NULL) {

            accept_conf = rd_kafka_conf_new();

            if (rd_kafka_conf_set(accept_conf, "bootstrap.servers", brokers, errstr,
                                  sizeof(errstr)) != RD_KAFKA_CONF_OK)
            {
                fprintf(stderr, "%s\n", errstr);
                return KAFKA_STATUS_INIT_FAIL;
            }

            rd_kafka_conf_set_dr_msg_cb(accept_conf, msgDeliveredCallback);

            accept_rk = rd_kafka_new(RD_KAFKA_PRODUCER, accept_conf, errstr, sizeof(errstr));
            if (!accept_rk) {
                fprintf(stderr, "%% Failed to create new producer:%s\n", errstr);
                return KAFKA_STATUS_INIT_FAIL;
            }

            accept_rkt = rd_kafka_topic_new(accept_rk, acceptopic, NULL);
            if (!accept_rkt) {
                fprintf(stderr, "%% Failed to create topic object: %s\n",
                        rd_kafka_err2str(rd_kafka_last_error()));
                rd_kafka_destroy(accept_rk);
                accept_rk = NULL;
                return KAFKA_STATUS_INIT_FAIL;
            }
        }

        if (appliedtopic != NULL) {

            // This pointer is to be freed by rd_kafka_new below
            applied_conf = rd_kafka_conf_new();

            //char bufMsgInFlight[16];
            //snprintf(bufMsgInFlight, sizeof(bufMsgInFlight), "%i", 1);
            //char bufMsgMaxBytes[16];
            //snprintf(bufMsgMaxBytes, sizeof(bufMsgMaxBytes), "%i", 3000000);

            const char* bufCompression = "lz4";
            const char* bufIdempotent = "true";

            if ((rd_kafka_conf_set(applied_conf, "bootstrap.servers", brokers, errstr,
                                  sizeof(errstr)) != RD_KAFKA_CONF_OK) ||
                (rd_kafka_conf_set(applied_conf, "compression.codec", bufCompression, errstr,
                                                  sizeof(errstr)) != RD_KAFKA_CONF_OK) ||
                (rd_kafka_conf_set(applied_conf, "enable.idempotence", bufIdempotent, errstr,
                                                  sizeof(errstr)) != RD_KAFKA_CONF_OK)
                    /*||
                (rd_kafka_conf_set(applied_conf, "message.max.bytes", bufMsgMaxBytes, errstr,
                                                  sizeof(errstr)) != RD_KAFKA_CONF_OK)*/ )
            {
                fprintf(stderr, "%s\n", errstr);
                return KAFKA_STATUS_INIT_FAIL;
            }

            rd_kafka_conf_set_dr_msg_cb(applied_conf, msgDeliveredCallback);


            applied_rk = rd_kafka_new(RD_KAFKA_PRODUCER, applied_conf, errstr, sizeof(errstr));
            if (!applied_rk) {
                fprintf(stderr, "%% Failed to create new producer:%s\n", errstr);
                return KAFKA_STATUS_INIT_FAIL;
            }

            applied_rkt = rd_kafka_topic_new(applied_rk, appliedtopic, NULL);
            if (!applied_rkt) {
                fprintf(stderr, "%% Failed to create topic object: %s\n",
                        rd_kafka_err2str(rd_kafka_last_error()));
                rd_kafka_destroy(applied_rk);
                applied_rk = NULL;
                return KAFKA_STATUS_INIT_FAIL;
            }
        }
        return KAFKA_STATUS_OK;
    }

    int kafka_producer::trx_kafka_sendmsg(int trxtype, char *msgstr, const std::string& msgKey) {
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        if (trxtype == KAFKA_TRX_ACCEPT) {
            rk = accept_rk;
            rkt = accept_rkt;
        } else if (trxtype == KAFKA_TRX_APPLIED) {
            rk = applied_rk;
            rkt = applied_rkt;
        } else {
            return KAFKA_STATUS_MSG_INVALID;
        }

        size_t len = strlen(msgstr);
        if (len == 0) {
            rd_kafka_poll(rk, 0);
            return KAFKA_STATUS_MSG_INVALID;
        }

        bool shouldRetry = false;
        int retryAttemptsLeft = 10;
        unsigned sleepBetweenRetriesMilliSeconds = 1000;

        do {
            int rdKafkaProduceResult = rd_kafka_produce(
                    rkt,
                    RD_KAFKA_PARTITION_UA,
                    RD_KAFKA_MSG_F_COPY,
                    msgstr, len,
                    msgKey.c_str(), msgKey.size(),
                    NULL);

            if(-1 == rdKafkaProduceResult) {
                fprintf(stderr,
                        "%% Failed to produce to topic %s: %s\n",
                        rd_kafka_topic_name(rkt),
                        rd_kafka_err2str(rd_kafka_last_error()));

                if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL &&
                        retryAttemptsLeft > 0) {
                    rd_kafka_poll(rk, 0);
                    shouldRetry = true;
                    --retryAttemptsLeft;
                    usleep(sleepBetweenRetriesMilliSeconds * 1000);
                } else if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE) {
                    fprintf(stderr, "The failed message is: \n %.*s ", static_cast<int>(len), msgstr);
                } else {
                    // Any other error - is not OK
                    throw std::runtime_error("Error on Kafka - send message");
                }
            }
        } while(shouldRetry);

        rd_kafka_poll(rk, 0);
        return KAFKA_STATUS_OK;

    }

    int kafka_producer::trx_kafka_destroy(void) {
        fprintf(stderr, "=== trx_kafka_destroyFlushing final message.. \n");
        if (accept_rk != NULL) {
            rd_kafka_flush(accept_rk, 10 * 1000);
            /* Destroy topic object */
            rd_kafka_topic_destroy(accept_rkt);
            /* Destroy the producer instance */
            rd_kafka_destroy(accept_rk);
            accept_rk = NULL;
            accept_rkt = NULL;
        }
        if (applied_rk != NULL) {
            rd_kafka_flush(applied_rk, 10 * 1000);
            /* Destroy topic object */
            rd_kafka_topic_destroy(applied_rkt);
            /* Destroy the producer instance */
            rd_kafka_destroy(applied_rk);
            applied_rk = NULL;
            applied_rkt = NULL;
        }

        return KAFKA_STATUS_OK;
    }
}


