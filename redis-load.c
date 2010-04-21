/* Redis load utility.
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * This software is NOT released under a free software license.
 * It is a commercial tool, under the terms of the license you can find in
 * the COPYING file in the Redis-Tools distribution.
 */

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <signal.h>
#include <math.h>
#include <limits.h>

#include "ae.h"
#include "anet.h"
#include "sds.h"
#include "adlist.h"
#include "zmalloc.h"
#include "rc4rand.h"

#define REPLY_UNKNOWN 0
#define REPLY_INT 1
#define REPLY_RETCODE 2
#define REPLY_BULK 3
#define REPLY_MBULK 4

#define REDIS_IDLE 0
#define REDIS_GET 1
#define REDIS_SET 2
#define REDIS_DEL 3
#define REDIS_SWAPIN 4
#define REDIS_LPUSH 6
#define REDIS_LPOP 7

#define CLIENT_CONNECTING 0
#define CLIENT_SENDQUERY 1
#define CLIENT_READREPLY 2

#define MAX_LATENCY 5000
#define DEFAULT_KEYSPACE 100000 /* 100k */

#define REDIS_NOTUSED(V) ((void) V)

static struct config {
    int debug;
    int numclients;
    int requests;
    int liveclients;
    int donerequests;
    int datasize_min;
    int datasize_max;
    int keyspace;
    int set_perc;
    int del_perc;
    int swapin_perc;
    int lpush_perc;
    int lpop_perc;
    int check;
    int rand;
    int longtail;
    int longtail_order;
    aeEventLoop *el;
    char *hostip;
    int hostport;
    int keepalive;
    long long start;
    long long totlatency;
    int *latency;
    list *clients;
    int quiet;
    int loop;
    int idlemode;
    int ctrlc; /* Ctrl + C pressed */
    unsigned int prngseed;
    /* The following "optab" array is used in order to randomize the different
     * kind of operations, like GET, SET, LPUSH, LPOP, SADD, and so forth.
     * For every query the client will pick a random bucket from 0 to 99
     * and will check what is the kind of operation to perform, also it will
     * use the bucket ID in order to make sure this kind of operation is
     * always executed against the right data type, so for instance if bucket
     * 7 is a LPUSH the operation will be performed against key xxxxxx07
     * and so forth. */
    unsigned char optab[100];
} config;

#define OPTAB_GET 0
#define OPTAB_SET 1
#define OPTAB_DEL 2
#define OPTAB_SWAPIN 3
#define OPTAB_LPUSH 4
#define OPTAB_LPOP 5

typedef struct _client {
    int state;
    int fd;
    int reqtype;        /* request type. REDIS_GET, REDIS_SET, ... */
    sds obuf;
    sds ibuf;
    int mbulk;          /* Number of elements in an mbulk reply */
    int readlen;        /* readlen == -1 means read a single line */
    int totreceived;
    unsigned int written;        /* bytes of 'obuf' already written */
    int replytype;
    long long start;    /* start time in milliseconds */
    long keyid;          /* the key name for this request is "key:<keyid>" */
} *client;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(void);

/* Implementation */
static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

/* Return a pseudo random number between min and max both inclusive */
static long randbetween(long min, long max) {
    return min+(random()%(max-min+1));
}

/* PRNG biased accordingly to the power law (Long Tail alike) */
unsigned long longtailprng(unsigned long min, unsigned long max, int n) {
    unsigned long pl;
    double r = (double)(random()&(INT_MAX-1))/INT_MAX;

    max += 1;
    pl = pow((pow(max,n+1) - pow(min,n+1))*r + pow(min,n+1),1.0/(n+1));
    return (max-1-pl)+min;
}

static void freeClient(client c) {
    listNode *ln;

    aeDeleteFileEvent(config.el,c->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->fd,AE_READABLE);
    sdsfree(c->ibuf);
    sdsfree(c->obuf);
    close(c->fd);
    zfree(c);
    config.liveclients--;
    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void checkDataIntegrity(client c) {
    if (c->reqtype == REDIS_GET) {
        size_t l;
        unsigned char *data;
        unsigned long datalen;

        l = sdslen(c->ibuf);
        if (l == 5 &&
            c->ibuf[0] == '$' && c->ibuf[1] == '-' && c->ibuf[2] == '1')
            return;

        rc4rand_seed(c->keyid);
        datalen = rc4rand_between(config.datasize_min,config.datasize_max);
        data = zmalloc(datalen+3);
        rc4rand_set(data,datalen);
        data[datalen] = '\r';
        data[datalen+1] = '\n';
        data[datalen+2] = '\0';

        if (l && l-2 != datalen) {
            printf("*** Len mismatch for KEY key:%ld\n", c->keyid);
            printf("*** %lu instead of %lu\n", l, datalen);
            printf("*** '%s' instead of '%s'\n", c->ibuf, data);
            exit(1);
        }
        if (memcmp(data,c->ibuf,datalen) != 0) {
            printf("*** Data mismatch for KEY key:%ld\n", c->keyid);
            printf("*** '%s' instead of '%s'\n", c->ibuf, data);
            exit(1);
        }
        zfree(data);
    }
}

static void resetClient(client c) {
    aeDeleteFileEvent(config.el,c->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->fd,AE_READABLE);
    aeCreateFileEvent(config.el,c->fd, AE_WRITABLE,writeHandler,c);
    sdsfree(c->ibuf);
    c->ibuf = sdsempty();
    c->mbulk = -1;
    c->readlen = 0;
    c->written = 0;
    c->totreceived = 0;
    c->state = CLIENT_SENDQUERY;
    c->start = mstime();
    c->replytype = REPLY_UNKNOWN;
    createMissingClients();
}

static void prepareClientForReply(client c, int type) {
    if (type == REPLY_BULK) {
        c->replytype = REPLY_BULK;
        c->readlen = -1;
    } else if (type == REPLY_MBULK) {
        c->replytype = REPLY_MBULK;
        c->readlen = -1;
        c->mbulk = -1;
    } else {
        c->replytype = type;
        c->readlen = 0;
    }
}

static void prepareClientForQuery(client c) {
    long r = random() % 100;
    long key;
    int op = config.optab[r];
    
    if (config.longtail) {
        key = longtailprng(0,config.keyspace-1,config.longtail_order);
    } else {
        key = random() % config.keyspace;
    }

    sdsfree(c->obuf);
    c->keyid = key;
    if (config.idlemode) {
        c->reqtype = REDIS_IDLE;
        c->obuf = sdsempty();
    } else if (op == OPTAB_SET) {
        unsigned char *data;
        unsigned long datalen;
        
        /* Set */
        c->reqtype = REDIS_SET;
        if (config.check) {
            rc4rand_seed(key);
            datalen = rc4rand_between(config.datasize_min,config.datasize_max);
        } else {
            datalen = randbetween(config.datasize_min,config.datasize_max);
        }
        data = zmalloc(datalen+2);
        c->obuf = sdscatprintf(sdsempty(),"SET key:%ld %lu\r\n",key,datalen);
        /* We use the key number as seed of the PRNG, so we'll be able to
         * check if a given key contains the right data later, without the
         * use of additional memory */
        if (config.check) {
            rc4rand_set(data,datalen);
        } else {
            if (config.rand) {
                rc4rand_seed(key);
                rc4rand_set(data,datalen);
            } else {
                memset(data,'x',datalen);
            }
        }
        data[datalen] = '\r';
        data[datalen+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,datalen+2);
        zfree(data);
    } else if (op == OPTAB_LPUSH) {
        unsigned char *data;
        unsigned long datalen;
        
        /* Lpush */
        c->reqtype = REDIS_LPUSH;
        datalen = randbetween(config.datasize_min,config.datasize_max);
        data = zmalloc(datalen+2);
        c->obuf = sdscatprintf(sdsempty(),"LPUSH key:%ld %lu\r\n",key,datalen);

        if (config.rand) {
            rc4rand_seed(key);
            rc4rand_set(data,datalen);
        } else {
            memset(data,'x',datalen);
        }
        data[datalen] = '\r';
        data[datalen+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,datalen+2);
        zfree(data);
    } else if (op == OPTAB_DEL) {
        /* Del */
        c->reqtype = REDIS_DEL;
        c->obuf = sdscatprintf(sdsempty(),"DEL key:%ld\r\n",key);
    } else if (op == OPTAB_SWAPIN) {
        /* Debug Swapin -- useful to stress test the VM subsystem */
        c->reqtype = REDIS_SWAPIN;
        c->obuf = sdscatprintf(sdsempty(),"DEBUG SWAPIN key:%ld\r\n",key);
    } else if (op == OPTAB_LPOP) {
        /* Lpop */
        c->reqtype = REDIS_LPOP;
        c->obuf = sdscatprintf(sdsempty(),"LPOP key:%ld\r\n",key);
    } else if (op == OPTAB_GET) {
        /* Get */
        c->reqtype = REDIS_GET;
        c->obuf = sdscatprintf(sdsempty(),"GET key:%ld\r\n",key);
    }
}

static void clientDone(client c) {
    static int last_tot_received = 1;

    long long latency;
    config.donerequests ++;
    latency = mstime() - c->start;
    if (latency > MAX_LATENCY) latency = MAX_LATENCY;
    config.latency[latency]++;

    if (config.check) checkDataIntegrity(c);

    if (config.debug && last_tot_received != c->totreceived) {
        printf("Tot bytes received: %d\n", c->totreceived);
        last_tot_received = c->totreceived;
    }
    if (config.donerequests == config.requests || config.ctrlc) {
        freeClient(c);
        aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
        prepareClientForQuery(c);
    } else {
        config.liveclients--;
        createMissingClients();
        config.liveclients++;
        freeClient(c);
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    char buf[1024];
    int nread;
    client c = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(mask);

    nread = read(c->fd, buf, 1024);
    if (nread == -1) {
        fprintf(stderr, "Reading from socket: %s\n", strerror(errno));
        freeClient(c);
        return;
    }
    if (nread == 0) {
        fprintf(stderr, "EOF from client\n");
        freeClient(c);
        return;
    }
    c->totreceived += nread;
    c->ibuf = sdscatlen(c->ibuf,buf,nread);
    if (c->replytype == REPLY_UNKNOWN) {
        int rtype;

        switch(c->ibuf[0]) {
        case '*': rtype = REPLY_MBULK; break;
        case '$': rtype = REPLY_BULK; break;
        case ':': rtype = REPLY_INT; break;
        default: rtype = REPLY_RETCODE; break;
        }
        prepareClientForReply(c,rtype);
    }

processdata:
    /* Are we waiting for the first line of the command of for  sdf 
     * count in bulk or multi bulk operations? */
    if (c->replytype == REPLY_INT ||
        c->replytype == REPLY_RETCODE ||
        (c->replytype == REPLY_BULK && c->readlen == -1) ||
        (c->replytype == REPLY_MBULK && c->readlen == -1) ||
        (c->replytype == REPLY_MBULK && c->mbulk == -1)) {
        char *p;

        /* Check if the first line is complete. This is only true if
         * there is a newline inside the buffer. */
        if ((p = strchr(c->ibuf,'\n')) != NULL) {
            if (c->replytype == REPLY_BULK ||
                (c->replytype == REPLY_MBULK && c->mbulk != -1))
            {
                /* Read the count of a bulk reply (being it a single bulk or
                 * a multi bulk reply). "$<count>" for the protocol spec. */
                *p = '\0';
                *(p-1) = '\0';
                c->readlen = atoi(c->ibuf+1)+2;
                /* Handle null bulk reply "$-1" */
                if (c->readlen-2 == -1) {
                    clientDone(c);
                    return;
                }
                /* Leave all the rest in the input buffer */
                c->ibuf = sdsrange(c->ibuf,(p-c->ibuf)+1,-1);
                /* fall through to reach the point where the code will try
                 * to check if the bulk reply is complete. */
            } else if (c->replytype == REPLY_MBULK && c->mbulk == -1) {
                /* Read the count of a multi bulk reply. That is, how many
                 * bulk replies we have to read next. "*<count>" protocol. */
                *p = '\0';
                *(p-1) = '\0';
                c->mbulk = atoi(c->ibuf+1);
                /* Handle null bulk reply "*-1" */
                if (c->mbulk == -1) {
                    clientDone(c);
                    return;
                }
                // printf("%p) %d elements list\n", c, c->mbulk);
                /* Leave all the rest in the input buffer */
                c->ibuf = sdsrange(c->ibuf,(p-c->ibuf)+1,-1);
                goto processdata;
            } else {
                c->ibuf = sdstrim(c->ibuf,"\r\n");
                clientDone(c);
                return;
            }
        }
    }
    /* bulk read, did we read everything? */
    if (((c->replytype == REPLY_MBULK && c->mbulk != -1) || 
         (c->replytype == REPLY_BULK)) && c->readlen != -1 &&
          (unsigned)c->readlen <= sdslen(c->ibuf))
    {
        if (c->replytype == REPLY_BULK) {
            clientDone(c);
        } else if (c->replytype == REPLY_MBULK) {
            if (--c->mbulk == 0) {
                clientDone(c);
            } else {
                c->ibuf = sdsrange(c->ibuf,c->readlen,-1);
                c->readlen = -1;
                goto processdata;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    client c = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(mask);

    if (c->state == CLIENT_CONNECTING) {
        c->state = CLIENT_SENDQUERY;
        c->start = mstime();
    }
    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        int len = sdslen(c->obuf) - c->written;
        int nwritten = write(c->fd, ptr, len);
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(config.el,c->fd,AE_WRITABLE);
            aeCreateFileEvent(config.el,c->fd,AE_READABLE,readHandler,c);
            c->state = CLIENT_READREPLY;
        }
    }
}

static client createClient(void) {
    client c = zmalloc(sizeof(struct _client));
    char err[ANET_ERR_LEN];

    c->fd = anetTcpNonBlockConnect(err,config.hostip,config.hostport);
    if (c->fd == ANET_ERR) {
        zfree(c);
        fprintf(stderr,"Connect: %s\n",err);
        return NULL;
    }
    anetTcpNoDelay(NULL,c->fd);
    c->obuf = sdsempty();
    c->ibuf = sdsempty();
    c->mbulk = -1;
    c->readlen = 0;
    c->written = 0;
    c->totreceived = 0;
    c->state = CLIENT_CONNECTING;
    c->reqtype = REDIS_IDLE;
    aeCreateFileEvent(config.el, c->fd, AE_WRITABLE, writeHandler, c);
    config.liveclients++;
    listAddNodeTail(config.clients,c);
    return c;
}

static void createMissingClients(void) {
    while(config.liveclients < config.numclients) {
        client new = createClient();
        if (!new) continue;
        prepareClientForQuery(new);
        prepareClientForReply(new,new->replytype);
    }
}

static void showLatencyReport(char *title) {
    int j, seen = 0;
    float perc, reqpersec;

    reqpersec = (float)config.donerequests/((float)config.totlatency/1000);
    if (!config.quiet) {
        printf("====== %s ======\n", title);
        printf("  %d requests completed in %.2f seconds\n", config.donerequests,
            (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d min bytes payload\n", config.datasize_min);
        printf("  %d max bytes payload\n", config.datasize_max);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");
        for (j = 0; j <= MAX_LATENCY; j++) {
            if (config.latency[j]) {
                seen += config.latency[j];
                perc = ((float)seen*100)/config.donerequests;
                printf("%.2f%% <= %d milliseconds\n", perc, j);
            }
        }
        printf("%.2f requests per second\n\n", reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", title, reqpersec);
    }
}

static void prepareForBenchmark(void)
{
    memset(config.latency,0,sizeof(int)*(MAX_LATENCY+1));
    config.start = mstime();
    config.donerequests = 0;
}

static void endBenchmark(char *title) {
    config.totlatency = mstime()-config.start;
    showLatencyReport(title);
    freeAllClients();
}

static void usage(char *wrong) {
    if (wrong)
        printf("Wrong option '%s' or option argument missing\n\n",wrong);
    printf(
"Usage: redis-load ... options ...\n\n"
" host <hostname>      Server hostname (default 127.0.0.1)\n"
" port <hostname>      Server port (default 6379)\n"
" clients <clients>    Number of parallel connections (default 50)\n"
" requests <requests>  Total number of requests (default 10k)\n"
" mindatasize <size>   Min data size of string values in bytes (default 1)\n"
" maxdatasize <size>   Min data size of string values in bytes (default 64)\n"
" datasize <size>      Set both min and max data size to the same value\n"
" keepalive            1=keep alive 0=reconnect (default 1)\n"
" keyspace             The number of different keys to use (default 100k)\n"
" rand                 Use random data payload (incompressible)\n"
" check                Check integrity where reading data back (implies rand)\n"
" longtail             Use long tail alike key access pattern distribution\n"
" longtailorder        A value of 2: 20%% keys get 49%% accesses.\n"
"                                 3: 20%% keys get 59%% accesses.\n"
"                                 4: 20%% keys get 67%% accesses.\n"
"                                 5: 20%% keys get 74%% accesses.\n"
"                                 6: 20%% keys get 79%% accesses (default).\n"
"                                 7: 20%% keys get 83%% accesses.\n"
"                                 8: 20%% keys get 86%% accesses.\n"
"                                 9: 20%% keys get 89%% accesses.\n"
"                                10: 20%% keys get 91%% accesses.\n"
"                                20: 20%% keys get 99%% accesses.\n"
" seed <seed>          PRNG seed for deterministic load\n"
" big                  alias for keyspace 1000000 requests 1000000\n"
" verybig              alias for keyspace 10000000 requests 10000000\n"
" quiet                Quiet mode, less verbose\n"
" loop                 Loop. Run the tests forever\n"
" idle                 Idle mode. Just open N idle connections and wait.\n"
" debug                Debug mode. more verbose.\n"
"\n"
"Type of operations (use percentages without trailing %%):\n"
"\n"
" set <percentage>    Percentage of SETs (default 50)\n"
" del <percentage>    Percentage of DELs (default 0)\n"
" lpush <percentage>  Percentage of LPUSHs (default 0)\n"
" lpop <percentage>   Percentage of LPOPs (default 0)\n"
" swapin <percentage> Percentage of DEBUG SWAPINs (default 0)\n"
"\n"
" All the free percantege (in order to reach 100%%) will be used for GETs\n"
);
    exit(1);
}

static void parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;
        
        if (!strcmp(argv[i],"clients") && !lastarg) {
            config.numclients = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"requests") && !lastarg) {
            config.requests = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"set") && !lastarg) {
            config.set_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"del") && !lastarg) {
            config.del_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"swapin") && !lastarg) {
            config.swapin_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"lpush") && !lastarg) {
            config.lpush_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"lpop") && !lastarg) {
            config.lpop_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"keepalive") && !lastarg) {
            config.keepalive = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"host") && !lastarg) {
            char *ip = zmalloc(32);
            if (anetResolve(NULL,argv[i+1],ip) == ANET_ERR) {
                printf("Can't resolve %s\n", argv[i]);
                exit(1);
            }
            config.hostip = ip;
            i++;
        } else if (!strcmp(argv[i],"port") && !lastarg) {
            config.hostport = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"datasize") && !lastarg) {
            config.datasize_max = config.datasize_min = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"mindatasize") && !lastarg) {
            config.datasize_min = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"maxdatasize") && !lastarg) {
            config.datasize_max = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"keyspace") && !lastarg) {
            config.keyspace = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"seed") && !lastarg) {
            config.prngseed = strtol(argv[i+1],NULL,10);
            i++;
        } else if (!strcmp(argv[i],"big")) {
            config.keyspace = 1000000;
            config.requests = 1000000;
        } else if (!strcmp(argv[i],"verybig")) {
            config.keyspace = 10000000;
            config.requests = 10000000;
        } else if (!strcmp(argv[i],"quiet")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"check")) {
            config.check = 1;
        } else if (!strcmp(argv[i],"rand")) {
            config.rand = 1;
        } else if (!strcmp(argv[i],"longtail")) {
            config.longtail = 1;
        } else if (!strcmp(argv[i],"longtailorder") && !lastarg) {
            config.longtail_order = atoi(argv[i+1]);
            i++;
            if (config.longtail_order < 2 || config.longtail_order > 100) {
                printf("Value out of range for 'longtailorder' option");
                exit(1);
            }
        } else if (!strcmp(argv[i],"loop")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"debug")) {
            config.debug = 1;
        } else if (!strcmp(argv[i],"idle")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i],"help")) {
            usage(NULL);
        } else {
            usage(argv[i]);
        }
    }
    /* Sanitize options */
    if (config.datasize_min < 1) config.datasize_min = 1;
    if (config.datasize_min > 1024*1024) config.datasize_min = 1024*1024;
    if (config.datasize_max < 1) config.datasize_max = 1;
    if (config.datasize_max > 1024*1024) config.datasize_max = 1024*1024;
    if (config.keyspace < 0) config.keyspace = 0;
}

static void ctrlc(int sig) {
    REDIS_NOTUSED(sig);

    config.ctrlc++;
    if (config.ctrlc == 1) {
        printf("Ctrl+C, return as soon as the running requests terminate\n");
    } else {
        printf("If you insist... exiting\n");
        exit(1);
    }
}

static void fillOpTab(int *i, int op, int perc) {
    int j;

    for (j = 0; j < perc; j++) {
        if (*i < 100) {
            config.optab[*i] = op;
            (*i)++;
        }
    }
}

int main(int argc, char **argv) {
    client c;
    int i;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.debug = 0;
    config.numclients = 50;
    config.requests = 10000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop();
    config.keepalive = 1;
    config.set_perc = 50;
    config.del_perc = 0;
    config.swapin_perc = 0;
    config.lpush_perc = 0;
    config.lpop_perc = 0;
    config.donerequests = 0;
    config.datasize_min = 1;
    config.datasize_max = 64;
    config.keyspace = DEFAULT_KEYSPACE; /* 100k */
    config.check = 0;
    config.rand = 0;
    config.longtail = 0;
    config.quiet = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.clients = listCreate();
    config.latency = zmalloc(sizeof(int)*(MAX_LATENCY+1));
    config.ctrlc = 0;
    config.prngseed = (unsigned int) (mstime()^getpid());

    config.hostip = "127.0.0.1";
    config.hostport = 6379;

    parseOptions(argc,argv);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
    }

    /* Setup the operation table. Start with a table with just GETs operations
     * and overwrite it with the other kind of ops as needed. */
    memset(config.optab,OPTAB_GET,100);
    i = 0;
    fillOpTab(&i,OPTAB_SET,config.set_perc);
    fillOpTab(&i,OPTAB_DEL,config.del_perc);
    fillOpTab(&i,OPTAB_SWAPIN,config.swapin_perc);
    fillOpTab(&i,OPTAB_LPUSH,config.lpush_perc);
    fillOpTab(&i,OPTAB_LPOP,config.lpop_perc);

    signal(SIGINT,ctrlc);
    srandom(config.prngseed);
    printf("PRNG seed is: %u - use the 'seed' option to reproduce the same sequence\n", config.prngseed);
    do {
        prepareForBenchmark();
        c = createClient();
        prepareClientForQuery(c);
        prepareClientForReply(c,c->replytype);
        createMissingClients();
        aeMain(config.el);
        endBenchmark("Report");
        printf("\n");
    } while(config.loop);

    return 0;
}
