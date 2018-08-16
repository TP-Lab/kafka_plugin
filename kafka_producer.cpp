#include <stdio.h>
#include <signal.h>
#include <string.h>
 
#include "rdkafka.h"
#include <eosio/kafka_plugin/kafka_producer.hpp>


rd_kafka_t *accept_rk=NULL;            /*Producer instance handle*/
rd_kafka_t *applied_rk=NULL;            /*Producer instance handle*/
rd_kafka_topic_t *accept_rkt=NULL;     /*topic object*/
rd_kafka_topic_t *applied_rkt=NULL;     /*topic object*/
rd_kafka_conf_t *accept_conf=NULL;     /*kafka config*/
rd_kafka_conf_t *applied_conf=NULL;     /*kafka config*/


static int run = 1;
 
static void stop(int sig){
	run = 0;
	fclose(stdin);
}  
 
/*
    æ¯æ¡æ¶ˆæ¯è°ƒç”¨ä¸€æ¬¡è¯¥å›è°ƒå‡½æ•°ï¼Œè¯´æ˜æ¶ˆæ¯æ˜¯ä¼ é€’æˆåŠŸ(rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR)
    è¿˜æ˜¯ä¼ é€’å¤±è´¥(rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR)
    è¯¥å›è°ƒå‡½æ•°ç”±rd_kafka_poll()è§¦å‘ï¼Œåœ¨åº”ç”¨ç¨‹åºçš„çº¿ç¨‹ä¸Šæ‰§è¡Œ
 */
static void dr_msg_cb_accept(rd_kafka_t *rk,
					  const rd_kafka_message_t *rkmessage, void *opaque){
		if(rkmessage->err)
			{
			//fprintf(stderr, "%% Message delivery failed: %s\n", 
			//		rd_kafka_err2str(rkmessage->err));
			}
		else
			{
			//fprintf(stderr,
            //            "%% Message delivered (%zd bytes, "
            //            "partition %d)\n",
            //            rkmessage->len, rkmessage->partition);
			}
        /* rkmessageè¢«librdkafkaè‡ªåŠ¨é”€æ¯*/
}

static void dr_msg_cb_appiled(rd_kafka_t *rk,
					  const rd_kafka_message_t *rkmessage, void *opaque){
		if(rkmessage->err)
			{
			//fprintf(stderr, "%% Message delivery failed: %s\n", 
			//		rd_kafka_err2str(rkmessage->err));
			}
		else
			{
			//fprintf(stderr,
            //            "%% Message delivered (%zd bytes, "
            //            "partition %d)\n",
            //            rkmessage->len, rkmessage->partition);
			}
        /* rkmessageè¢«librdkafkaè‡ªåŠ¨é”€æ¯*/
}


int trx_kafka_init(char* brokers,char* acceptopic,char* appliedtopic )
{
	
	char errstr[512];
	if(brokers==NULL)
	{
		return KAFKA_STATUS_INIT_FAIL;
	}
	


 	if(acceptopic!=NULL)
 	{

		 /* ´´½¨Ò»¸ökafkaÅäÖÃÕ¼Î» */
		accept_conf = rd_kafka_conf_new();
		
		/*´´½¨broker¼¯Èº*/
		if (rd_kafka_conf_set(accept_conf, "bootstrap.servers", brokers, errstr,
					sizeof(errstr)) != RD_KAFKA_CONF_OK){
			fprintf(stderr, "%s\n", errstr);
			return KAFKA_STATUS_INIT_FAIL;
		}

		/*ÉèÖÃ·¢ËÍ±¨¸æ»Øµ÷º¯Êı£¬rd_kafka_produce()½ÓÊÕµÄÃ¿ÌõÏûÏ¢¶¼»áµ÷ÓÃÒ»´Î¸Ã»Øµ÷º¯Êı
	     *Ó¦ÓÃ³ÌĞòĞèÒª¶¨ÆÚµ÷ÓÃrd_kafka_poll()À´·şÎñÅÅ¶ÓµÄ·¢ËÍ±¨¸æ»Øµ÷º¯Êı*/
		rd_kafka_conf_set_dr_msg_cb(accept_conf, dr_msg_cb_accept);
		
	    /*´´½¨accept trascation producerÊµÀı
	      rd_kafka_new()»ñÈ¡conf¶ÔÏóµÄËùÓĞÈ¨,Ó¦ÓÃ³ÌĞòÔÚ´Ëµ÷ÓÃÖ®ºó²»µÃÔÙ´ÎÒıÓÃËü*/
		accept_rk = rd_kafka_new(RD_KAFKA_PRODUCER, accept_conf, errstr, sizeof(errstr));
		if(!accept_rk){
			fprintf(stderr, "%% Failed to create new producer:%s\n", errstr);
			return KAFKA_STATUS_INIT_FAIL;
		}
	 
	    /*ÊµÀı»¯Ò»¸ö»ò¶à¸ötopics(`rd_kafka_topic_t`)À´Ìá¹©Éú²ú»òÏû·Ñ£¬topic
	    ¶ÔÏó±£´ætopicÌØ¶¨µÄÅäÖÃ£¬²¢ÔÚÄÚ²¿Ìî³äËùÓĞ¿ÉÓÃ·ÖÇøºÍleader brokers£¬*/
		accept_rkt = rd_kafka_topic_new(accept_rk, acceptopic, NULL);
		if (!accept_rkt){
			fprintf(stderr, "%% Failed to create topic object: %s\n", 
					rd_kafka_err2str(rd_kafka_last_error()));
			rd_kafka_destroy(accept_rk);
			accept_rk=NULL;
			return KAFKA_STATUS_INIT_FAIL;
		}
 	}
	
	if(appliedtopic!=NULL)
	{

		 /* ´´½¨Ò»¸ökafkaÅäÖÃÕ¼Î» */
		applied_conf = rd_kafka_conf_new();

		/*´´½¨broker¼¯Èº*/
		if (rd_kafka_conf_set(applied_conf, "bootstrap.servers", brokers, errstr,
					sizeof(errstr)) != RD_KAFKA_CONF_OK){
			fprintf(stderr, "%s\n", errstr);
			return KAFKA_STATUS_INIT_FAIL;
		}

		/*ÉèÖÃ·¢ËÍ±¨¸æ»Øµ÷º¯Êı£¬rd_kafka_produce()½ÓÊÕµÄÃ¿ÌõÏûÏ¢¶¼»áµ÷ÓÃÒ»´Î¸Ã»Øµ÷º¯Êı
	     *Ó¦ÓÃ³ÌĞòĞèÒª¶¨ÆÚµ÷ÓÃrd_kafka_poll()À´·şÎñÅÅ¶ÓµÄ·¢ËÍ±¨¸æ»Øµ÷º¯Êı*/
		rd_kafka_conf_set_dr_msg_cb(applied_conf, dr_msg_cb_appiled);

	
		    /*´´½¨applied trascation producerÊµÀı
	      rd_kafka_new()»ñÈ¡conf¶ÔÏóµÄËùÓĞÈ¨,Ó¦ÓÃ³ÌĞòÔÚ´Ëµ÷ÓÃÖ®ºó²»µÃÔÙ´ÎÒıÓÃËü*/
		applied_rk = rd_kafka_new(RD_KAFKA_PRODUCER, applied_conf, errstr, sizeof(errstr));
		if(!applied_rk){
			fprintf(stderr, "%% Failed to create new producer:%s\n", errstr);
			return KAFKA_STATUS_INIT_FAIL;
		}
	 
	    /*ÊµÀı»¯Ò»¸ö»ò¶à¸ötopics(`rd_kafka_topic_t`)À´Ìá¹©Éú²ú»òÏû·Ñ£¬topic
	    ¶ÔÏó±£´ætopicÌØ¶¨µÄÅäÖÃ£¬²¢ÔÚÄÚ²¿Ìî³äËùÓĞ¿ÉÓÃ·ÖÇøºÍleader brokers£¬*/
		applied_rkt = rd_kafka_topic_new(applied_rk, appliedtopic, NULL);
		if (!applied_rkt){
			fprintf(stderr, "%% Failed to create topic object: %s\n", 
					rd_kafka_err2str(rd_kafka_last_error()));
			rd_kafka_destroy(applied_rk);
			applied_rk=NULL;
			return KAFKA_STATUS_INIT_FAIL;
		}
	}
	signal(SIGINT, stop);
	return KAFKA_STATUS_OK;
}

int trx_kafka_sendmsg(int trxtype,char* msgstr)
{
	rd_kafka_t* rk;
	rd_kafka_topic_t* rkt;
	if(trxtype==KAFKA_TRX_ACCEPT)
	{
		rk = accept_rk;
		rkt = accept_rkt;
	}
	else if(trxtype==KAFKA_TRX_APPLIED)
	{
		rk = applied_rk;
		rkt = applied_rkt;
	}
	else
	{
		return KAFKA_STATUS_MSG_INVALID;
	}
	
	size_t len = strlen(msgstr);
 	if(len == 0){
            /*ÂÖÑ¯ÓÃÓÚÊÂ¼şµÄkafka handle£¬
            ÊÂ¼ş½«µ¼ÖÂÓ¦ÓÃ³ÌĞòÌá¹©µÄ»Øµ÷º¯Êı±»µ÷ÓÃ
            µÚ¶ş¸ö²ÎÊıÊÇ×î´ó×èÈûÊ±¼ä£¬Èç¹ûÉèÎª0£¬½«»áÊÇ·Ç×èÈûµÄµ÷ÓÃ*/
     		rd_kafka_poll(rk, 0);
     		return KAFKA_STATUS_MSG_INVALID;
     	}
	retry:
	/*Send/Produce message.
           ÕâÊÇÒ»¸öÒì²½µ÷ÓÃ£¬ÔÚ³É¹¦µÄÇé¿öÏÂ£¬Ö»»á½«ÏûÏ¢ÅÅÈëÄÚ²¿producer¶ÓÁĞ£¬
           ¶ÔbrokerµÄÊµ¼Ê´«µİ³¢ÊÔÓÉºóÌ¨Ïß³Ì´¦Àí£¬Ö®Ç°×¢²áµÄ´«µİ»Øµ÷º¯Êı(dr_msg_cb)
           ÓÃÓÚÔÚÏûÏ¢´«µİ³É¹¦»òÊ§°ÜÊ±ÏòÓ¦ÓÃ³ÌĞò·¢»ØĞÅºÅ*/
     	if (rd_kafka_produce(
                    /* Topic object */
     				rkt,
                    /*Ê¹ÓÃÄÚÖÃµÄ·ÖÇøÀ´Ñ¡Ôñ·ÖÇø*/
     				RD_KAFKA_PARTITION_UA,
                    /*Éú³ÉpayloadµÄ¸±±¾*/
     				RD_KAFKA_MSG_F_COPY,
                    /*ÏûÏ¢ÌåºÍ³¤¶È*/
     				msgstr, len,
                    /*¿ÉÑ¡¼ü¼°Æä³¤¶È*/
     				NULL, 0,
     				NULL) == -1){
     		fprintf(stderr, 
     			"%% Failed to produce to topic %s: %s\n", 
     			rd_kafka_topic_name(rkt),
     			rd_kafka_err2str(rd_kafka_last_error()));
 
     		if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL){
                /*Èç¹ûÄÚ²¿¶ÓÁĞÂú£¬µÈ´ıÏûÏ¢´«ÊäÍê³É²¢retry,
                ÄÚ²¿¶ÓÁĞ±íÊ¾Òª·¢ËÍµÄÏûÏ¢ºÍÒÑ·¢ËÍ»òÊ§°ÜµÄÏûÏ¢£¬
                ÄÚ²¿¶ÓÁĞÊÜÏŞÓÚqueue.buffering.max.messagesÅäÖÃÏî*/
     			rd_kafka_poll(rk, 1000);
     			goto retry;
     		}	
     	} else {
     		fprintf(stderr, "%% Enqueued message (%zd bytes) for topic %s\n", len, rd_kafka_topic_name(rkt));
     	}
 
        /*producerÓ¦ÓÃ³ÌĞòÓ¦²»¶ÏµØÍ¨¹ıÒÔÆµ·±µÄ¼ä¸ôµ÷ÓÃrd_kafka_poll()À´Îª
        ´«ËÍ±¨¸æ¶ÓÁĞÌá¹©·şÎñ¡£ÔÚÃ»ÓĞÉú³ÉÏûÏ¢ÒÔÈ·¶¨ÏÈÇ°Éú³ÉµÄÏûÏ¢ÒÑ·¢ËÍÁËÆä
        ·¢ËÍ±¨¸æ»Øµ÷º¯Êı(ºÍÆäËû×¢²á¹ıµÄ»Øµ÷º¯Êı)ÆÚ¼ä£¬ÒªÈ·±£rd_kafka_poll()
        ÈÔÈ»±»µ÷ÓÃ*/
     	rd_kafka_poll(rk, 0);
		return KAFKA_STATUS_OK;
	
}

int trx_kafka_destroy(void)
{
	 fprintf(stderr, "%% Flushing final message.. \n");
	 /*rd_kafka_flushÊÇrd_kafka_poll()µÄ³éÏó»¯£¬
	 µÈ´ıËùÓĞÎ´Íê³ÉµÄproduceÇëÇóÍê³É£¬Í¨³£ÔÚÏú»ÙproducerÊµÀıÇ°Íê³É
	 ÒÔÈ·±£ËùÓĞÅÅÁĞÖĞºÍÕıÔÚ´«ÊäµÄproduceÇëÇóÔÚÏú»ÙÇ°Íê³É*/
	 if(accept_rk!=NULL)
	 {
	     rd_kafka_flush(accept_rk, 10*1000);
		 /* Destroy topic object */
		 rd_kafka_topic_destroy(accept_rkt);
		 /* Destroy the producer instance */
		 rd_kafka_destroy(accept_rk);
		 accept_rk=NULL;
		 accept_rkt=NULL;
	 }
	 if(applied_rk!=NULL)
	 {
		 rd_kafka_flush(applied_rk, 10*1000); 
		 /* Destroy topic object */
		 rd_kafka_topic_destroy(applied_rkt); 
		 /* Destroy the producer instance */
		 rd_kafka_destroy(applied_rk);
		 applied_rk=NULL;
		 applied_rkt=NULL;
	 }

	 return KAFKA_STATUS_OK;
}
#if 0
int main(int argc, char **argv)
{
	char buf[512]; 
	int kafkastaus=KAFKA_STATUS_OK;
	trx_kafka_init();
	
	
	fprintf(stderr,
                "%% Type some text and hit enter to produce message\n"
                "%% Or just hit enter to only serve delivery reports\n"
                "%% Press Ctrl-C or Ctrl-D to exit\n");
	while(run && fgets(buf, sizeof(buf), stdin))
	{
		do{
			kafkastaus=trx_kafka_sendmsg(buf);
		}while(kafkastaus==KAFKA_STATUS_QUEUE_FULL);
	}

	trx_kafka_destroy();
}
#endif

#if 0
int main(int argc, char **argv){
	rd_kafka_t *rk;            /*Producer instance handle*/
	rd_kafka_topic_t *rkt;     /*topicå¯¹è±¡*/
	rd_kafka_conf_t *conf;     /*ä¸´æ—¶é…ç½®å¯¹è±¡*/
	char errstr[512];          
	char buf[512];             
	const char *brokers;       
	const char *topic;         
 
	if(argc != 3){
		fprintf(stderr, "%% Usage: %s <broker> <topic>\n", argv[0]);
        return 1;
	}
 
	brokers = argv[1];
	topic = argv[2];
 
    /* åˆ›å»ºä¸€ä¸ªkafkaé…ç½®å ä½ */
	conf = rd_kafka_conf_new();
 
    /*åˆ›å»ºbrokeré›†ç¾¤*/
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
				sizeof(errstr)) != RD_KAFKA_CONF_OK){
		fprintf(stderr, "%s\n", errstr);
		return 1;
	}
 
    /*è®¾ç½®å‘é€æŠ¥å‘Šå›è°ƒå‡½æ•°ï¼Œrd_kafka_produce()æ¥æ”¶çš„æ¯æ¡æ¶ˆæ¯éƒ½ä¼šè°ƒç”¨ä¸€æ¬¡è¯¥å›è°ƒå‡½æ•°
     *åº”ç”¨ç¨‹åºéœ€è¦å®šæœŸè°ƒç”¨rd_kafka_poll()æ¥æœåŠ¡æ’é˜Ÿçš„å‘é€æŠ¥å‘Šå›è°ƒå‡½æ•°*/
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
 
    /*åˆ›å»ºproducerå®ä¾‹
      rd_kafka_new()è·å–confå¯¹è±¡çš„æ‰€æœ‰æƒ,åº”ç”¨ç¨‹åºåœ¨æ­¤è°ƒç”¨ä¹‹åä¸å¾—å†æ¬¡å¼•ç”¨å®ƒ*/
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if(!rk){
		fprintf(stderr, "%% Failed to create new producer:%s\n", errstr);
		return 1;
	}
 
    /*å®ä¾‹åŒ–ä¸€ä¸ªæˆ–å¤šä¸ªtopics(`rd_kafka_topic_t`)æ¥æä¾›ç”Ÿäº§æˆ–æ¶ˆè´¹ï¼Œtopic
    å¯¹è±¡ä¿å­˜topicç‰¹å®šçš„é…ç½®ï¼Œå¹¶åœ¨å†…éƒ¨å¡«å……æ‰€æœ‰å¯ç”¨åˆ†åŒºå’Œleader brokersï¼Œ*/
	rkt = rd_kafka_topic_new(rk, topic, NULL);
	if (!rkt){
		fprintf(stderr, "%% Failed to create topic object: %s\n", 
				rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_destroy(rk);
		return 1;
	}
 
    /*ç”¨äºä¸­æ–­çš„ä¿¡å·*/
	signal(SIGINT, stop);
 
	fprintf(stderr,
                "%% Type some text and hit enter to produce message\n"
                "%% Or just hit enter to only serve delivery reports\n"
                "%% Press Ctrl-C or Ctrl-D to exit\n");
 
     while(run && fgets(buf, sizeof(buf), stdin)){
     	size_t len = strlen(buf);
 
     	if(buf[len-1] == '\n')
     		buf[--len] = '\0';
 
     	if(len == 0){
            /*è½®è¯¢ç”¨äºäº‹ä»¶çš„kafka handleï¼Œ
            äº‹ä»¶å°†å¯¼è‡´åº”ç”¨ç¨‹åºæä¾›çš„å›è°ƒå‡½æ•°è¢«è°ƒç”¨
            ç¬¬äºŒä¸ªå‚æ•°æ˜¯æœ€å¤§é˜»å¡æ—¶é—´ï¼Œå¦‚æœè®¾ä¸º0ï¼Œå°†ä¼šæ˜¯éé˜»å¡çš„è°ƒç”¨*/
     		rd_kafka_poll(rk, 0);
     		continue;
     	}
 
     retry:
         /*Send/Produce message.
           è¿™æ˜¯ä¸€ä¸ªå¼‚æ­¥è°ƒç”¨ï¼Œåœ¨æˆåŠŸçš„æƒ…å†µä¸‹ï¼Œåªä¼šå°†æ¶ˆæ¯æ’å…¥å†…éƒ¨produceré˜Ÿåˆ—ï¼Œ
           å¯¹brokerçš„å®é™…ä¼ é€’å°è¯•ç”±åå°çº¿ç¨‹å¤„ç†ï¼Œä¹‹å‰æ³¨å†Œçš„ä¼ é€’å›è°ƒå‡½æ•°(dr_msg_cb)
           ç”¨äºåœ¨æ¶ˆæ¯ä¼ é€’æˆåŠŸæˆ–å¤±è´¥æ—¶å‘åº”ç”¨ç¨‹åºå‘å›ä¿¡å·*/
     	if (rd_kafka_produce(
                    /* Topic object */
     				rkt,
                    /*ä½¿ç”¨å†…ç½®çš„åˆ†åŒºæ¥é€‰æ‹©åˆ†åŒº*/
     				RD_KAFKA_PARTITION_UA,
                    /*ç”Ÿæˆpayloadçš„å‰¯æœ¬*/
     				RD_KAFKA_MSG_F_COPY,
                    /*æ¶ˆæ¯ä½“å’Œé•¿åº¦*/
     				buf, len,
                    /*å¯é€‰é”®åŠå…¶é•¿åº¦*/
     				NULL, 0,
     				NULL) == -1){
     		fprintf(stderr, 
     			"%% Failed to produce to topic %s: %s\n", 
     			rd_kafka_topic_name(rkt),
     			rd_kafka_err2str(rd_kafka_last_error()));
 
     		if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL){
                /*å¦‚æœå†…éƒ¨é˜Ÿåˆ—æ»¡ï¼Œç­‰å¾…æ¶ˆæ¯ä¼ è¾“å®Œæˆå¹¶retry,
                å†…éƒ¨é˜Ÿåˆ—è¡¨ç¤ºè¦å‘é€çš„æ¶ˆæ¯å’Œå·²å‘é€æˆ–å¤±è´¥çš„æ¶ˆæ¯ï¼Œ
                å†…éƒ¨é˜Ÿåˆ—å—é™äºqueue.buffering.max.messagesé…ç½®é¡¹*/
     			rd_kafka_poll(rk, 1000);
     			goto retry;
     		}	
     	}else{
     		fprintf(stderr, "%% Enqueued message (%zd bytes) for topic %s\n", 
     			len, rd_kafka_topic_name(rkt));
     	}
 
        /*produceråº”ç”¨ç¨‹åºåº”ä¸æ–­åœ°é€šè¿‡ä»¥é¢‘ç¹çš„é—´éš”è°ƒç”¨rd_kafka_poll()æ¥ä¸º
        ä¼ é€æŠ¥å‘Šé˜Ÿåˆ—æä¾›æœåŠ¡ã€‚åœ¨æ²¡æœ‰ç”Ÿæˆæ¶ˆæ¯ä»¥ç¡®å®šå…ˆå‰ç”Ÿæˆçš„æ¶ˆæ¯å·²å‘é€äº†å…¶
        å‘é€æŠ¥å‘Šå›è°ƒå‡½æ•°(å’Œå…¶ä»–æ³¨å†Œè¿‡çš„å›è°ƒå‡½æ•°)æœŸé—´ï¼Œè¦ç¡®ä¿rd_kafka_poll()
        ä»ç„¶è¢«è°ƒç”¨*/
     	rd_kafka_poll(rk, 0);
     }
 
     fprintf(stderr, "%% Flushing final message.. \n");
     /*rd_kafka_flushæ˜¯rd_kafka_poll()çš„æŠ½è±¡åŒ–ï¼Œ
     ç­‰å¾…æ‰€æœ‰æœªå®Œæˆçš„produceè¯·æ±‚å®Œæˆï¼Œé€šå¸¸åœ¨é”€æ¯producerå®ä¾‹å‰å®Œæˆ
     ä»¥ç¡®ä¿æ‰€æœ‰æ’åˆ—ä¸­å’Œæ­£åœ¨ä¼ è¾“çš„produceè¯·æ±‚åœ¨é”€æ¯å‰å®Œæˆ*/
     rd_kafka_flush(rk, 10*1000);
 
     /* Destroy topic object */
     rd_kafka_topic_destroy(rkt);
 
     /* Destroy the producer instance */
     rd_kafka_destroy(rk);
 
     return 0;
}
#endif

