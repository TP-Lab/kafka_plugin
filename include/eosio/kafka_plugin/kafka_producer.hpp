#pragma once

#define KAFKA_STATUS_OK 0
#define KAFKA_STATUS_INIT_FAIL 1
#define KAFKA_STATUS_MSG_INVALID 2
#define KAFKA_STATUS_QUEUE_FULL 3

#define KAFKA_TRX_ACCEPT 0
#define KAFKA_TRX_APPLIED 1


int trx_kafka_init(char* brokers,char* acceptopic,char*appliedtopic );
int trx_kafka_sendmsg(int trxtype,char* msgstr);
int trx_kafka_destroy(void);
