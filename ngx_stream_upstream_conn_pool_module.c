

/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>


typedef struct {
    ngx_uint_t                           max_conns;

    ngx_rbtree_t                         rbtree;
    ngx_rbtree_node_t                    sentinel;

    ngx_stream_upstream_init_pt          original_init_upstream;
    ngx_stream_upstream_init_peer_pt     original_init_peer;

} ngx_stream_upstream_conn_pool_srv_conf_t;


typedef struct {
    ngx_str_node_t                       sn;

    ngx_queue_t                          queue;

} ngx_stream_upstream_conn_pool_node_t;


typedef struct {
    ngx_event_t                            timer;
    ngx_queue_t                            queue;
    ngx_peer_connection_t                  peer;
    ngx_stream_upstream_conn_pool_node_t  *cn;

} ngx_stream_upstream_conn_pool_ctx_t;


typedef struct {
    /* the round robin data must be first */
    ngx_stream_upstream_rr_peer_data_t         rrp;

    ngx_event_get_peer_pt                      original_get_peer;
    ngx_event_free_peer_pt                     original_free_peer;
    ngx_stream_upstream_conn_pool_srv_conf_t  *conf;

} ngx_stream_upstream_conn_pool_peer_data_t;


static ngx_int_t ngx_stream_upstream_init_conn_pool_peer(
    ngx_stream_session_t *s, ngx_stream_upstream_srv_conf_t *us);
static ngx_int_t ngx_stream_upstream_get_conn_pool_peer(
    ngx_peer_connection_t *pc, void *data);
static void ngx_stream_upstream_free_conn_pool_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static char *ngx_stream_upstream_conn_pool(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static void *ngx_stream_upstream_conn_pool_create_conf(ngx_conf_t *cf);
static ngx_int_t ngx_stream_upstream_conn_pool_init_process(
    ngx_cycle_t *cycle);
static void ngx_stream_upstream_conn_pool_connect(
    ngx_stream_upstream_conn_pool_ctx_t *ctx);
static void ngx_stream_upstream_conn_pool_connect_handler(ngx_event_t *ev);
static void ngx_stream_upstream_conn_pool_close_handler(ngx_event_t *ev);
static void ngx_stream_upstream_conn_pool_reconnect_handler(ngx_event_t *ev);
static void ngx_stream_upstream_conn_pool_dummy_handler(ngx_event_t *ev);
static ngx_int_t ngx_stream_upstream_conn_pool_test_connect(
    ngx_connection_t *c);


static ngx_command_t  ngx_stream_upstream_conn_pool_commands[] = {

    { ngx_string("conn_pool"),
      NGX_STREAM_UPS_CONF|NGX_CONF_TAKE1,
      ngx_stream_upstream_conn_pool,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_stream_module_t  ngx_stream_upstream_conn_pool_module_ctx = {
    NULL,                                      /* preconfiguration */
    NULL,                                      /* postconfiguration */

    NULL,                                      /* create main configuration */
    NULL,                                      /* init main configuration */

    ngx_stream_upstream_conn_pool_create_conf, /* create server configuration */
    NULL                                       /* merge server configuration */
};


ngx_module_t  ngx_stream_upstream_conn_pool_module = {
    NGX_MODULE_V1,
    &ngx_stream_upstream_conn_pool_module_ctx, /* module context */
    ngx_stream_upstream_conn_pool_commands,    /* module directives */
    NGX_STREAM_MODULE,                         /* module type */
    NULL,                                      /* init master */
    NULL,                                      /* init module */
    ngx_stream_upstream_conn_pool_init_process,/* init process */
    NULL,                                      /* init thread */
    NULL,                                      /* exit thread */
    NULL,                                      /* exit process */
    NULL,                                      /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_stream_upstream_init_conn_pool(ngx_conf_t *cf,
    ngx_stream_upstream_srv_conf_t *us)
{
    uint32_t                                    hash;
    ngx_str_t                                  *name;
    ngx_uint_t                                  i, j, k;
    ngx_str_node_t                             *sn;
    ngx_stream_upstream_server_t               *server;
    ngx_stream_upstream_conn_pool_ctx_t        *ctx;
    ngx_stream_upstream_conn_pool_node_t       *cnode;
    ngx_stream_upstream_conn_pool_srv_conf_t   *cpcf;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, cf->log, 0,
                   "init upstream conn pool");

    cpcf = ngx_stream_conf_upstream_srv_conf(us,
                                          ngx_stream_upstream_conn_pool_module);

    if (cpcf->original_init_upstream(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    if (!us->servers) {
        return NGX_OK;
    }

    ngx_rbtree_init(&cpcf->rbtree, &cpcf->sentinel, 
                    ngx_str_rbtree_insert_value);

    server = us->servers->elts;
    for (i = 0; i < us->servers->nelts; i++) {
        if (server[i].backup) {
            continue;
        }

        for (j = 0; j < server[i].naddrs; j++) {
            name = &server[i].addrs[j].name;
            hash = ngx_crc32_long(name->data, name->len);

            sn = ngx_str_rbtree_lookup(&cpcf->rbtree, name, hash);
            if (sn) {
                continue;
            }

            cnode = ngx_pcalloc(cf->pool,
                                sizeof(ngx_stream_upstream_conn_pool_node_t));
            if (cnode == NULL) {
                return NGX_ERROR;
            }

            cnode->sn.str = *name;
            cnode->sn.node.key = hash;

            ngx_rbtree_insert(&cpcf->rbtree, &cnode->sn.node);

            ngx_queue_init(&cnode->queue);

            for(k = 0; k < cpcf->max_conns; k++) {
                ctx = ngx_pcalloc(cf->pool,
                                 sizeof(ngx_stream_upstream_conn_pool_ctx_t));
                if (ctx == NULL) {
                    return NGX_ERROR;
                }

                ctx->cn = cnode;
                ctx->peer.sockaddr = server[i].addrs[j].sockaddr;
                ctx->peer.socklen = server[i].addrs[j].socklen;
                ctx->peer.name = &server[i].addrs[j].name;
                ctx->peer.get = ngx_event_get_peer;
                ctx->peer.log = &cf->cycle->new_log;
                ctx->peer.log_error = NGX_ERROR_ERR;

                ctx->timer.data = ctx;
                ctx->timer.handler =
                                ngx_stream_upstream_conn_pool_reconnect_handler;
                ctx->timer.log = &cf->cycle->new_log;
                ctx->timer.cancelable = 1;

                ngx_queue_insert_head(&cnode->queue, &ctx->queue);
            }
        }
    }

    cpcf->original_init_peer = us->peer.init;

    us->peer.init = ngx_stream_upstream_init_conn_pool_peer;

    return NGX_OK;
}


static ngx_int_t
ngx_stream_upstream_init_conn_pool_peer(ngx_stream_session_t *s,
    ngx_stream_upstream_srv_conf_t *us)
{
    ngx_stream_upstream_conn_pool_srv_conf_t   *cpcf;
    ngx_stream_upstream_conn_pool_peer_data_t  *cpp;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                   "init conn pool peer");

    cpp = ngx_palloc(s->connection->pool,
                     sizeof(ngx_stream_upstream_conn_pool_peer_data_t));
    if (cpp == NULL) {
        return NGX_ERROR;
    } 

    cpcf = ngx_stream_conf_upstream_srv_conf(us,
                                          ngx_stream_upstream_conn_pool_module);

    s->upstream->peer.data = &cpp->rrp;

    if (cpcf->original_init_peer(s, us) != NGX_OK) {
        return NGX_ERROR;
    }

    cpp->conf = cpcf;
    cpp->original_get_peer = s->upstream->peer.get;
    cpp->original_free_peer = s->upstream->peer.free;

    s->upstream->peer.get = ngx_stream_upstream_get_conn_pool_peer;
    s->upstream->peer.free = ngx_stream_upstream_free_conn_pool_peer;

    return NGX_OK;
}


static ngx_int_t
ngx_stream_upstream_get_conn_pool_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_stream_upstream_conn_pool_srv_conf_t   *cpcf;
    ngx_stream_upstream_conn_pool_peer_data_t  *cpp = data;


    uint32_t                              hash;
    ngx_int_t                             rc;
    ngx_queue_t                          *q;
    ngx_str_node_t                       *sn;
    ngx_stream_upstream_conn_pool_ctx_t  *ctx;
    ngx_stream_upstream_conn_pool_node_t *cn;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, pc->log, 0, "get conn pool peer");

    rc = cpp->original_get_peer(pc, &cpp->rrp);
    if (rc != NGX_OK) {
        return rc;
    }

    cpcf = cpp->conf;
    hash = ngx_crc32_long(pc->name->data, pc->name->len);

    sn = ngx_str_rbtree_lookup(&cpcf->rbtree, pc->name, hash);
    if (sn == NULL) {
        return rc;
    }

    cn = (ngx_stream_upstream_conn_pool_node_t *) sn;

    if (ngx_queue_empty(&cn->queue)) {
        ngx_log_error(NGX_LOG_INFO, pc->log, 0,
                       "get conn pool peer: pool is empty");
        return rc;
    }

    q = ngx_queue_head(&cn->queue);
    ngx_queue_remove(q);
    ctx = ngx_queue_data(q, ngx_stream_upstream_conn_pool_ctx_t,
                         queue);

    pc->connection = ctx->peer.connection;
    ctx->peer.connection = NULL;

    pc->connection->idle = 0;
    pc->connection->log = pc->log;
    pc->connection->read->log = pc->log;
    pc->connection->write->log = pc->log;

    ngx_log_error(NGX_LOG_INFO, pc->log, 0,
                   "get conn pool peer: using connection %p", pc->connection);

    ngx_stream_upstream_conn_pool_connect(ctx);

    return NGX_DONE;
}


static void
ngx_stream_upstream_free_conn_pool_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_stream_upstream_conn_pool_peer_data_t *cpp = data;

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, pc->log, 0,
                   "free conn pool peer, state: %ui", state);

    cpp->original_free_peer(pc, &cpp->rrp, state);
}


static ngx_int_t
ngx_stream_upstream_conn_pool_init_process(ngx_cycle_t *cycle)
{
    ngx_uint_t                                  i;
    ngx_queue_t                                *q;
    ngx_rbtree_node_t                          *node;
    ngx_stream_upstream_conn_pool_node_t       *cn;
    ngx_stream_upstream_conn_pool_ctx_t        *ctx;
    ngx_stream_upstream_srv_conf_t            **uscfp;
    ngx_stream_upstream_main_conf_t            *umcf;
    ngx_stream_upstream_conn_pool_srv_conf_t   *cpcf;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, cycle->log, 0,
                   "conn pool init process");

    umcf = ngx_stream_cycle_get_module_main_conf(cycle,
                                                 ngx_stream_upstream_module);
    if (umcf == NULL) {
        return NGX_OK;
    }

    uscfp = umcf->upstreams.elts;
    for (i = 0; i < umcf->upstreams.nelts; i++) {
        cpcf = ngx_stream_conf_upstream_srv_conf(uscfp[i],
                                          ngx_stream_upstream_conn_pool_module);
        if (cpcf == NULL) {
            return NGX_OK;
        }

        if (cpcf->max_conns == 0) {
            return NGX_OK;
        }

        for (node = ngx_rbtree_min(cpcf->rbtree.root, &cpcf->sentinel);
             node;
             node = ngx_rbtree_next(&cpcf->rbtree, node))
        {
            cn = (ngx_stream_upstream_conn_pool_node_t *) node;

            while (!ngx_queue_empty(&cn->queue)) {
                q = ngx_queue_head(&cn->queue);
                ngx_queue_remove(q);
                ctx = ngx_queue_data(q, ngx_stream_upstream_conn_pool_ctx_t,
                                     queue);

                ngx_stream_upstream_conn_pool_connect(ctx);
                ngx_msleep(10);
            }
        }
    }

    return NGX_OK;
}

static void
ngx_stream_upstream_conn_pool_connect(ngx_stream_upstream_conn_pool_ctx_t *ctx)
{
    ngx_int_t                rc;
    ngx_connection_t        *c;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ngx_cycle->log, 0,
                   "upstream conn pool connect");

    rc = ngx_event_connect_peer(&ctx->peer);

    if (rc == NGX_ERROR || rc == NGX_BUSY || rc == NGX_DECLINED) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "upstream conn pool connect error: %V", ctx->peer.name);

        ngx_add_timer(&ctx->timer, 3000);
        return;
    }

    c = ctx->peer.connection;

    c->data = ctx;
    c->read->handler = ngx_stream_upstream_conn_pool_connect_handler;
    c->write->handler = ngx_stream_upstream_conn_pool_connect_handler;

    if (rc == NGX_OK) {
        ngx_stream_upstream_conn_pool_connect_handler(c->write);
        return;
    }

    ngx_add_timer(c->write, 3000);
    return;
}


static void
ngx_stream_upstream_conn_pool_close_handler(ngx_event_t *ev)
{
    ssize_t                              n;
    u_char                               buf[1];

    ngx_connection_t                    *c;
    ngx_stream_upstream_conn_pool_ctx_t *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ev->log, 0,
                   "upstream conn pool close handler");

    c = ev->data;
    ctx = c->data;

    if (c->close) {
        goto close;
    }

    n = recv(c->fd, buf, 1, MSG_PEEK);

    if (n == -1 && ngx_socket_errno == NGX_EAGAIN) {
        ev->ready = 0;

        if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            goto close;
        }

        return;
    }

close:

    ngx_queue_remove(&ctx->queue);

    if (ctx->peer.connection) {
        ngx_close_connection(ctx->peer.connection);
        ctx->peer.connection = NULL;
    }

    ngx_add_timer(&ctx->timer, 10);
}


static void
ngx_stream_upstream_conn_pool_connect_handler(ngx_event_t *ev)
{
    ngx_connection_t                    *c;
    ngx_stream_upstream_conn_pool_ctx_t *ctx;

    c = ev->data;
    ctx = c->data;

    if (ev->timedout) {
        ngx_log_error(NGX_LOG_ERR, c->log, NGX_ETIMEDOUT,
                      "upstream conn pool timed out: %V", ctx->peer.name);
        goto failed;
    }

    if (c->write->timer_set) {
        ngx_del_timer(c->write);
    }

    if (ngx_stream_upstream_conn_pool_test_connect(c) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, c->log, NGX_ETIMEDOUT,
                      "upstream conn pool connect error: %V", ctx->peer.name);
        goto failed;
    }

    ngx_log_error(NGX_LOG_INFO, c->log, 0,
                  "upstream conn pool connect sucess: %V, %p",
                  ctx->peer.name, c);

    c->idle = 1;

    if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        goto failed;
    }

    c->read->handler = ngx_stream_upstream_conn_pool_close_handler;
    c->write->handler = ngx_stream_upstream_conn_pool_dummy_handler;

    ngx_queue_insert_tail(&ctx->cn->queue, &ctx->queue);

    if (c->read->ready) {
        ngx_stream_upstream_conn_pool_close_handler(c->read);
    }

    return;

failed:

    if (ctx->peer.connection) {
        ngx_close_connection(ctx->peer.connection);
        ctx->peer.connection = NULL;
    }

    ngx_add_timer(&ctx->timer, 3000);
}


static ngx_int_t
ngx_stream_upstream_conn_pool_test_connect(ngx_connection_t *c)
{
    int        err;
    socklen_t  len;

#if (NGX_HAVE_KQUEUE)

    if (ngx_event_flags & NGX_USE_KQUEUE_EVENT)  {
        err = c->write->kq_errno ? c->write->kq_errno : c->read->kq_errno;

        if (err) {
            (void) ngx_connection_error(c, err,
                                    "kevent() reported that connect() failed");
            return NGX_ERROR;
        }

    } else
#endif
    {
        err = 0;
        len = sizeof(int);

        /*
         * BSDs and Linux return 0 and set a pending error in err
         * Solaris returns -1 and sets errno
         */

        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len)
            == -1)
        {
            err = ngx_socket_errno;
        }

        if (err) {
            (void) ngx_connection_error(c, err, "connect() failed");
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static void
ngx_stream_upstream_conn_pool_reconnect_handler(ngx_event_t *ev)
{
    ngx_stream_upstream_conn_pool_ctx_t *ctx;

    ctx = ev->data;

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ev->log, 0,
                   "upstream conn pool reconnect: %V", ctx->peer.name);

    ngx_stream_upstream_conn_pool_connect(ctx);
}


static void
ngx_stream_upstream_conn_pool_dummy_handler(ngx_event_t *ev)
{
    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ev->log, 0,
                   "upstream conn pool dummy handler");
}


static void *
ngx_stream_upstream_conn_pool_create_conf(ngx_conf_t *cf)
{
    ngx_stream_upstream_conn_pool_srv_conf_t *conf;

    conf = ngx_pcalloc(cf->pool,
                      sizeof(ngx_stream_upstream_conn_pool_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    return conf;
}


static char *
ngx_stream_upstream_conn_pool(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_stream_upstream_srv_conf_t           *uscf;
    ngx_stream_upstream_conn_pool_srv_conf_t *cpcf;

    ngx_int_t    n;
    ngx_str_t   *value;

    uscf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_upstream_module);
    cpcf = ngx_stream_conf_upstream_srv_conf(uscf,
                                          ngx_stream_upstream_conn_pool_module);
    if (cpcf->max_conns) {
        return "is duplicate";
    }

    value = cf->args->elts;

    n = ngx_atoi(value[1].data, value[1].len);

    if (n == NGX_ERROR || n == 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid value \"%V\" in \"%V\" directive",
                           &value[1], &cmd->name);
        return NGX_CONF_ERROR;
    }

    cpcf->max_conns = n;

    uscf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_upstream_module);

    cpcf->original_init_upstream = uscf->peer.init_upstream
                                   ? uscf->peer.init_upstream
                                   : ngx_stream_upstream_init_round_robin;

    uscf->peer.init_upstream = ngx_stream_upstream_init_conn_pool;

    uscf->flags = NGX_STREAM_UPSTREAM_CREATE
                  |NGX_STREAM_UPSTREAM_WEIGHT
                  |NGX_STREAM_UPSTREAM_MAX_CONNS
                  |NGX_STREAM_UPSTREAM_MAX_FAILS
                  |NGX_STREAM_UPSTREAM_FAIL_TIMEOUT
                  |NGX_STREAM_UPSTREAM_DOWN
                  |NGX_STREAM_UPSTREAM_BACKUP;

    return NGX_CONF_OK;
}
