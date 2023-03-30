
#include <kafka_module.h>

static void *kafka_conf_create(ngx_conf_t *cf);
static char *kafka_brokers(ngx_conf_t *cf, ngx_command_t *cmd, void *_conf);
static ngx_int_t kafka_process(ngx_cycle_t *cycle);
static kafka_conf_t *kafka_conf = NULL;
static ngx_command_t kafka_commands[] = {
	{
		ngx_string("kafka"),
		NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE,
        kafka_brokers,
		NGX_HTTP_MAIN_CONF_OFFSET,
		0,
		NULL },
	ngx_null_command
};

static ngx_http_module_t kafka_module_ctx = {
	NULL,                             /* pre conf */
	NULL,                             /* post conf */
	kafka_conf_create,  /* create main conf */
	NULL,                             /* init main conf */

	NULL,                             /* create server conf */
	NULL,                             /* merge server conf */

	NULL,   /* create local conf */
	NULL,    /* merge location conf */
};

ngx_module_t kafka_module = {
	NGX_MODULE_V1,
	&kafka_module_ctx,   /* module context */
	kafka_commands,      /* module directives */
	NGX_HTTP_MODULE,              /* module type */

	NULL,                         /* init master */
	NULL,                         /* init module */

    kafka_process,   /* init process */
	NULL,                         /* init thread */

	NULL,                         /* exit thread */
	NULL,   /* exit process */
	NULL,                         /* exit master */

	NGX_MODULE_V1_PADDING
};

#define rb_is_red(r) (!rb_color(r))
#define rb_set_red(r) do { (r)->rb_parent_color &= ~1; } while (0)
#define rb_set_black(r) do { (r)->rb_parent_color |= 1; } while (0)
#define rb_parent(r) ((kafka_topic_t *)((r)->rb_parent_color & ~3))
#define rb_color(r) ((r)->rb_parent_color & 1)

void kafka_rbtree_set_parent(kafka_topic_t *rb, kafka_topic_t *p)
{
	rb->rb_parent_color = (rb->rb_parent_color & 3) | (unsigned long)p;
}

void kafka_rbtree_link_node(kafka_topic_t *node, kafka_topic_t *parent, kafka_topic_t **rb_link)
{

	node->rb_parent_color = (unsigned long )parent;
	node->rb_left = node->rb_right = NULL;
	*rb_link = node;
}

void kafka_rbtree_rotate_left(kafka_topic_t *node, kafka_topic_list *head)
{
	kafka_topic_t *right = node->rb_right;
	kafka_topic_t *parent = rb_parent(node);

	if ((node->rb_right = right->rb_left))
		kafka_rbtree_set_parent(right->rb_left, node);

	right->rb_left = node;
	kafka_rbtree_set_parent(right, parent);

	if (parent) {

		if (node == parent->rb_left)
			parent->rb_left = right;
		else
			parent->rb_right = right;

	} else {

		head->rb_node = right;
	}

	kafka_rbtree_set_parent(node, right);
}

void kafka_rbtree_rotate_right(kafka_topic_t *node, kafka_topic_list *head)
{
	kafka_topic_t *left = node->rb_left;
	kafka_topic_t *parent = rb_parent(node);

	if ((node->rb_left = left->rb_right))
		kafka_rbtree_set_parent(left->rb_right, node);

	left->rb_right = node;
	kafka_rbtree_set_parent(left, parent);

	if (parent) {

		if (node == parent->rb_right)
			parent->rb_right = left;
		else
			parent->rb_left = left;

	} else {

		head->rb_node = left;
	}

	kafka_rbtree_set_parent(node, left);
}

void kafka_rbtree_insert_color(kafka_topic_t *node, kafka_topic_list *head)
{

	kafka_topic_t *parent, *gparent;

	while ((parent = rb_parent(node)) && rb_is_red(parent)) {

		gparent = rb_parent(parent);

		if (parent == gparent->rb_left) {

			register kafka_topic_t *uncle = gparent->rb_right;
			if (uncle && rb_is_red(uncle)) {

				rb_set_black(uncle);
				rb_set_black(parent);
				rb_set_red(gparent);
				node = gparent;
				continue;
			}

			if (parent->rb_right == node) {

				register kafka_topic_t *tmp;
				kafka_rbtree_rotate_left(parent, head);
				tmp = parent;
				parent = node;
				node = tmp;
			}

			rb_set_black(parent);
			rb_set_red(gparent);
			kafka_rbtree_rotate_right(gparent, head);

		} else {

			register kafka_topic_t *uncle = gparent->rb_left;
			if (uncle && rb_is_red(uncle)) {

				rb_set_black(uncle);
				rb_set_black(parent);
				rb_set_red(gparent);
				node = gparent;
				continue;
			}

			if (parent->rb_left == node) {

				register kafka_topic_t *tmp;
				kafka_rbtree_rotate_right(parent, head);
				tmp = parent;
				parent = node;
				node = tmp;
			}

			rb_set_black(parent);
			rb_set_red(gparent);
			kafka_rbtree_rotate_left(gparent, head);
		}
	}

	rb_set_black(head->rb_node);
}

kafka_topic_t *kafka_topic_add(kafka_topic_list *head, kafka_topic_t *new_node)
{
	kafka_topic_t *parent = NULL;
	kafka_topic_t **p = &head->rb_node;
	kafka_topic_t *node = NULL;

	while (*p) {

		parent = *p;
		node = parent;
		int retval = strcmp(node->key, new_node->key);
		if (retval < 0) {
			p = &(*p)->rb_left;

		} else if (retval > 0) {
			p = &(*p)->rb_right;

		} else {
			return NULL;
		}
	}

	kafka_rbtree_link_node(new_node, parent, p);
	kafka_rbtree_insert_color(new_node, head);
	return node;
}

kafka_topic_t *kafka_topic_init(const char *key)
{
    size_t len = strlen(key);
    kafka_topic_t *topic = ngx_pcalloc(kafka_conf->pool, sizeof (kafka_topic_t));
    topic->key = ngx_pcalloc(kafka_conf->pool, len + 1);
    memcpy(topic->key, key, len);
    topic->key[len] = 0;

    topic->conf = rd_kafka_topic_conf_new();
    topic->conn = rd_kafka_topic_new(kafka_conf->conn,topic->key,topic->conf);
    if (!topic->conn) {
        return NULL;
    }

    kafka_topic_add(&kafka_conf->topics, topic);
    return topic;
}

kafka_topic_t *kafka_topic_find(const char *key)
{
    if (!kafka_conf) {
        return NULL;
    }

    kafka_topic_list *head = &kafka_conf->topics;
    kafka_topic_t *p = head->rb_node;
	kafka_topic_t *node = NULL;

	while (p) {

		node = (kafka_topic_t *)p;
		int retval = strcmp(node->key, key);
		if (retval < 0) {
			p = p->rb_left;

		} else if (retval > 0) {
			p = p->rb_right;

		} else {
			return node;
		}
	}

	return kafka_topic_init(key);
}


static void *kafka_conf_create(ngx_conf_t *cf)
{
    kafka_conf = ngx_pcalloc(cf->pool, sizeof(kafka_conf_t));
    if (kafka_conf == NULL) {
        return NGX_CONF_ERROR;
    }

    kafka_conf->conf = rd_kafka_conf_new();
    ngx_queue_init(&kafka_conf->brokers);
    kafka_conf->topics.rb_node = NULL;
    return kafka_conf;
}

static char *kafka_brokers(ngx_conf_t *cf, ngx_command_t *cmd, void *_conf)
{
    ngx_uint_t i;
    kafka_conf_t *conf = _conf;
    ngx_str_t *value = cf->args->elts;
    for (i = 1; i < cf->args->nelts; i++) {
        kafka_broker *node = ngx_pcalloc(cf->pool, sizeof(kafka_broker));
        node->broker = value[i];
        node->broker.data[node->broker.len] = 0;
        ngx_queue_insert_tail(&conf->brokers, &node->next);
    }

    return NGX_OK;
}

static ngx_int_t kafka_process(ngx_cycle_t *cycle)
{
    kafka_conf_t *conf = ngx_http_cycle_get_module_main_conf(cycle, kafka_module);
    if (ngx_queue_empty(&conf->brokers)) {
        return NGX_OK;
    }

    conf->conn = rd_kafka_new(RD_KAFKA_PRODUCER, conf->conf, NULL, 0);
    if (!conf->conn) {
        return NGX_ERROR;
    }

    ngx_queue_t *p = NULL;
    for (p = ngx_queue_head(&conf->brokers);
         p != ngx_queue_sentinel(&conf->brokers);
         p = ngx_queue_next(p)) {

        kafka_broker *node = (kafka_broker *)p;
        int ret = rd_kafka_brokers_add(conf->conn, (char *)node->broker.data);
        // fprintf(stdout, "[kafka][%s][%d]\n", node->broker.data, ret);
        if (ret == 0) {
            return NGX_ERROR;
        }
    }

    conf->pool = cycle->pool;
    return NGX_OK;
}

void kafka_send(char *topic_name, void *buffer, size_t size) {
    kafka_topic_t *topic = kafka_topic_find(topic_name);
    if (topic) {
        rd_kafka_produce(topic->conn,
                         RD_KAFKA_PARTITION_UA,
                         RD_KAFKA_MSG_F_COPY,
                         buffer,
                         size,
                         NULL,
                         0,
                         NULL);
    }
}


