/*
 *  Beansdb - A high available distributed key-value storage system:
 *
 *      http://beansdb.googlecode.com
 *
 *  Copyright 2010 Douban Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Davies Liu <davies.liu@gmail.com>
 *      Hurricane Lee <hurricane1026@gmail.com>
 *
 */

#include "beansdb.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include <pthread.h>
#include "util.h"
#include "log.h"

typedef struct EventLoop
{
    conn* conns[AE_SETSIZE];
    int   fired[AE_SETSIZE];
    int   nready;
    void* apidata;
} EventLoop;

/* Lock for connection freelist */
static pthread_mutex_t conn_lock;

/* Lock for item buffer freelist */
static pthread_mutex_t ibuffer_lock;

static EventLoop loop;
static pthread_mutex_t leader;

/*
 * Pulls a conn structure from the freelist, if one is available.
 */
conn *mt_conn_from_freelist()
{
    conn *c;
    pthread_mutex_lock(&conn_lock);
    c = do_conn_from_freelist();
    pthread_mutex_unlock(&conn_lock);
    return c;
}

/*
 * Adds a conn structure to the freelist.
 *
 * Returns 0 on success, 1 if the structure couldn't be added.
 */
bool mt_conn_add_to_freelist(conn *c)
{
    bool result;

    pthread_mutex_lock(&conn_lock);
    result = do_conn_add_to_freelist(c);
    pthread_mutex_unlock(&conn_lock);

    return result;
}

/*
 * Pulls a item buffer from the freelist, if one is available.
 */

item *mt_item_from_freelist(void)
{
    item *it;
    pthread_mutex_lock(&ibuffer_lock);
    it = do_item_from_freelist();
    pthread_mutex_unlock(&ibuffer_lock);
    return it;
}

/*
 * Adds a item buffer to the freelist.
 *
 * Returns 0 on success, 1 if the buffer couldn't be added.
 */
int mt_item_add_to_freelist(item *it)
{
    int result;

    pthread_mutex_lock(&ibuffer_lock);
    result = do_item_add_to_freelist(it);
    pthread_mutex_unlock(&ibuffer_lock);

    return result;
}

/******************************* GLOBAL STATS ******************************/

void mt_stats_lock()
{
}

void mt_stats_unlock()
{
}

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EPOLL
#include "ae_epoll.c"
#else
#ifdef HAVE_KQUEUE
#include "ae_kqueue.c"
#else
#include "ae_select.c"
#endif
#endif

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of event handler threads to spawn
 */
void thread_init(int nthreads)
{
    int         i;
    pthread_mutex_init(&ibuffer_lock, NULL);
    pthread_mutex_init(&conn_lock, NULL);
    pthread_mutex_init(&leader, NULL);

    memset(&loop, 0, sizeof(loop));
    if (aeApiCreate(&loop) == -1)
    {
        exit(1);
    }
}

int add_event(int fd, int mask, conn *c)
{
    if (fd >= AE_SETSIZE)
    {
        PDLOG(ERROR, "fd is too large: %d", fd);
        return AE_ERR;
    }
    if (loop.conns[fd] != NULL)
    {
        PDLOG(ERROR, "fd is used: %d", fd);
        return AE_ERR;
    }
    loop.conns[fd] = c;
    if (aeApiAddEvent(&loop, fd, mask) == -1)
    {
        loop.conns[fd] = NULL;
        return AE_ERR;
    }
    return AE_OK;
}

int update_event(int fd, int mask, conn *c)
{
    loop.conns[fd] = c;
    if (aeApiUpdateEvent(&loop, fd, mask) == -1)
    {
        loop.conns[fd] = NULL;
        return AE_ERR;
    }
    return AE_OK;
}

int delete_event(int fd)
{
    if (fd >= AE_SETSIZE) return -1;
    loop.conns[fd] = NULL;
    if (aeApiDelEvent(&loop, fd) == -1)
        return -1;
    return 0;
}

static void *worker_main(void *arg)
{
    pthread_setcanceltype (PTHREAD_CANCEL_ASYNCHRONOUS, 0);

    struct timeval tv = {1, 0};
    while (!daemon_quit)
    {
        pthread_mutex_lock(&leader);

AGAIN:
        while(loop.nready == 0 && daemon_quit == 0)
            loop.nready = aeApiPoll(&loop, &tv);
        if (daemon_quit)
        {
            pthread_mutex_unlock(&leader);
            break;
        }

        loop.nready --;
        int fd = loop.fired[loop.nready];
        conn *c = loop.conns[fd];
        if (c == NULL)
        {
            PDLOG(ERROR, "Bug: conn %d should not be NULL", fd);
            delete_event(fd);
            close(fd);
            goto AGAIN;
        }
        //loop.conns[fd] = NULL;
        pthread_mutex_unlock(&leader);

        if (drive_machine(c))
        {
            if (update_event(fd, c->ev_flags, c)) conn_close(c);
        }
    }
    return NULL;
}

void loop_run(int nthread)
{
    int i, ret;
    pthread_attr_t  attr;
    pthread_attr_init(&attr);
    pthread_t *tids = (pthread_t*)safe_malloc(sizeof(pthread_t) * nthread);

    for (i = 0; i < nthread - 1; i++)
    {
        if ((ret = pthread_create(tids + i, &attr, worker_main, NULL)) != 0)
        {
            PDLOG(ERROR, "Can't create thread: %s",
                    strerror(ret));
            exit(1);
        }
    }

    worker_main(NULL);

    // wait workers to stop
    for (i = 0; i < nthread - 1; i++)
    {
        (void) pthread_join(tids[i], NULL);
        pthread_detach(tids[i]);
    }
    free(tids);

    aeApiFree(&loop);
}
