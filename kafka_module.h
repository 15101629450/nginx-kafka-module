
#ifndef __ADX_KAFKA_H__
#define __ADX_KAFKA_H__

#include <nginx.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <librdkafka/rdkafka.h>

typedef struct {
	ngx_queue_t           next;
	ngx_str_t             broker;
} kafka_broker;

typedef struct kafka_topic_t kafka_topic_t;
struct kafka_topic_t {
	char                  *key;
	rd_kafka_topic_conf_t *conf;
	rd_kafka_topic_t      *conn;
	kafka_topic_t         *rb_right;
	kafka_topic_t         *rb_left;
    unsigned long         rb_parent_color;
};

typedef struct {
	kafka_topic_t         *rb_node;
} kafka_topic_list;

typedef struct {
	rd_kafka_conf_t       *conf;
	rd_kafka_t            *conn;
	ngx_queue_t           brokers;
	kafka_topic_list      topics;
    ngx_pool_t            *pool;
} kafka_conf_t;

void kafka_send(char *topic_name, void *buffer, size_t size);

#endif


