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
#include <signal.h>
#include <assert.h>
#include <signal.h>
#include <math.h>
#include <limits.h>

#include "hiredis.h"
#include "adapters/ae.h"
#include "adlist.h"
#include "zmalloc.h"
#include "rc4rand.h"
#include "utils.h"

#define REDIS_IDLE 0
#define REDIS_GET 1
#define REDIS_SET 2
#define REDIS_DEL 3
#define REDIS_SWAPIN 4
#define REDIS_LPUSH 6
#define REDIS_LPOP 7
#define REDIS_HSET 8
#define REDIS_HGET 9
#define REDIS_HGETALL 10

#define MAX_LATENCY 5000
#define DEFAULT_KEYSPACE 100000 /* 100k */
#define DEFAULT_HASHKEYSPACE 1000 /* 1k */

#define REDIS_NOTUSED(V) ((void) V)

static struct config {
    aeEventLoop *el;
    int debug;
    int done;
    list *clients;
    int num_clients;
    int num_requests;
    int issued_requests;

    int datasize_min;
    int datasize_max;
    unsigned char *databuf;

    int keyspace;
    int hashkeyspace;

    int set_perc;
    int del_perc;
    int swapin_perc;
    int lpush_perc;
    int lpop_perc;
    int hset_perc;
    int hget_perc;
    int hgetall_perc;

    int check;
    int rand;
    int longtail;
    int longtail_order;
    char *hostip;
    int hostport;
    int keepalive;
    long long start;
    long long totlatency;
    int *latency;
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

typedef struct _client {
    redisAsyncContext *context;
    int state;
    int reqtype;        /* request type. REDIS_GET, REDIS_SET, ... */
    long long start;    /* start time in milliseconds */
    long keyid;          /* the key name for this request is "key:<keyid>" */
} *client;

/* Prototypes */
static void issueRequest(client c);
static void createMissingClients(void);

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

static void clientDisconnected(const redisAsyncContext *context, int status) {
    listNode *ln;
    client c = (client)context->data;

    if (status != REDIS_OK) {
        fprintf(stderr,"Disconnected: %s\n",c->context->errstr);
        exit(1);
    }

    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
    zfree(c);

    /* The run was not done, create new client(s). */
    if (!config.done) {
        createMissingClients();
    }

    /* Stop the event loop when all clients were disconnected */
    if (!listLength(config.clients)) {
        aeStop(config.el);
    }
}

static client createClient(void) {
    client c = zmalloc(sizeof(struct _client));

    c->context = redisAsyncConnect(config.hostip,config.hostport);
    c->context->data = c;
    redisAsyncSetDisconnectCallback(c->context,clientDisconnected);
    if (c->context->err) {
        fprintf(stderr,"Connect: %s\n",c->context->errstr);
        exit(1);
    }

    redisAeAttach(config.el,c->context);
    listAddNodeTail(config.clients,c);
    issueRequest(c);
    return c;
}

static void createMissingClients(void) {
    while(listLength(config.clients) < (size_t)config.num_clients) {
        createClient();
    }
}

static void checkDataIntegrity(client c, redisReply *reply) {
    if (c->reqtype == REDIS_GET && reply->type == REDIS_REPLY_STRING) {
        unsigned char *data;
        unsigned int datalen;

        rc4rand_seed(c->keyid);
        datalen = rc4rand_between(config.datasize_min,config.datasize_max);
        data = zmalloc(datalen);
        rc4rand_set(data,datalen);

        if (reply->len != (int)datalen) {
            fprintf(stderr, "*** Len mismatch for KEY key:%ld\n", c->keyid);
            fprintf(stderr, "*** %d instead of %d\n", reply->len, datalen);
            fprintf(stderr, "*** '%s' instead of '%s'\n", reply->str, data);
            exit(1);
        }
        if (memcmp(reply->str,data,datalen) != 0) {
            fprintf(stderr, "*** Data mismatch for KEY key:%ld\n", c->keyid);
            fprintf(stderr, "*** '%s' instead of '%s'\n", reply->str, data);
            exit(1);
        }
        zfree(data);
    }
}

static void handleReply(redisAsyncContext *context, void *_reply, void *privdata) {
    REDIS_NOTUSED(privdata);
    redisReply *reply = (redisReply*)_reply;
    client c = (client)context->data;
    long long latency = (microseconds() - c->start) / 1000;

    if (reply == NULL && context->err) {
        fprintf(stderr,"Error: %s\n", context->errstr);
        exit(1);
    } else {
        assert(reply != NULL);
        if (reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr,"Error: %s\n", reply->str);
            exit(1);
        }
    }

    if (latency > MAX_LATENCY) latency = MAX_LATENCY;
    config.latency[latency]++;

    if (config.check) checkDataIntegrity(c,reply);
    freeReplyObject(reply);

    if (config.done || config.ctrlc) {
        redisAsyncDisconnect(c->context);
        return;
    }

    if (config.keepalive) {
        issueRequest(c);
    } else {
        /* createMissingClients will be called in the disconnection callback */
        redisAsyncDisconnect(c->context);
    }
}

static unsigned long randomData(long seed) {
    unsigned long datalen;

    /* We use the key number as seed of the PRNG, so we'll be able to check if
     * a given key contains the right data later, without the use of additional
     * memory. */
    if (config.check) {
        rc4rand_seed(seed);
        datalen = rc4rand_between(config.datasize_min,config.datasize_max);
        rc4rand_set(config.databuf,datalen);
    } else {
        datalen = randbetween(config.datasize_min,config.datasize_max);
        if (config.rand) {
            rc4rand_seed(seed);
            rc4rand_set(config.databuf,datalen);
        } else {
            memset(config.databuf,'x',datalen);
        }
    }

    return datalen;
}

static void issueRequest(client c) {
    int op = config.optab[random() % 100];
    long key, hashkey;
    unsigned long datalen;

    config.issued_requests++;
    if (config.issued_requests == config.num_requests) config.done = 1;

    c->start = microseconds();
    if (config.longtail) {
        key = longtailprng(0,config.keyspace-1,config.longtail_order);
        hashkey = longtailprng(0,config.hashkeyspace-1,config.longtail_order);
    } else {
        key = random() % config.keyspace;
        hashkey = random() % config.hashkeyspace;
    }

    c->keyid = key;
    c->reqtype = op;

    if (op == REDIS_IDLE) {
        /* Idle */
    } else if (op == REDIS_SET) {
        datalen = randomData(key);
        redisAsyncCommand(c->context,handleReply,NULL,"SET string:%ld %b",key,config.databuf,datalen);
    } else if (op == REDIS_GET) {
        redisAsyncCommand(c->context,handleReply,NULL,"GET string:%ld",key);
    } else if (op == REDIS_DEL) {
        redisAsyncCommand(c->context,handleReply,NULL,"DEL string:%ld list:%ld hash:%ld",key,key,key);
    } else if (op == REDIS_LPUSH) {
        datalen = randomData(key);
        redisAsyncCommand(c->context,handleReply,NULL,"LPUSH list:%ld %b",key,config.databuf,datalen);
    } else if (op == REDIS_LPOP) {
        redisAsyncCommand(c->context,handleReply,NULL,"LPOP list:%ld",key);
    } else if (op == REDIS_HSET) {
        datalen = randomData(key);
        redisAsyncCommand(c->context,handleReply,NULL,"HSET hash:%ld key:%ld %b",key,hashkey,config.databuf,datalen);
    } else if (op == REDIS_HGET) {
        redisAsyncCommand(c->context,handleReply,NULL,"HGET hash:%ld key:%ld",key,hashkey);
    } else if (op == REDIS_HGETALL) {
        redisAsyncCommand(c->context,handleReply,NULL,"HGETALL hash:%ld",key);
    } else if (op == REDIS_SWAPIN) {
        /* Only accepts a single argument, so for now only works with string keys. */
        redisAsyncCommand(c->context,handleReply,NULL,"DEBUG SWAPIN string:%ld",key);
    } else {
        assert(NULL);
    }
}

static void showLatencyReport(void) {
    int j, seen = 0;
    float perc, reqpersec;

    reqpersec = (float)config.issued_requests/((float)config.totlatency/1000);
    if (!config.quiet) {
        printf("====== Report ======\n");
        printf("  %d requests in %.3f seconds\n", config.issued_requests,
            (float)config.totlatency/1000);
        printf("  %.2f requests per second\n", reqpersec);
        printf("  %d parallel clients\n", config.num_clients);
        printf("  payload: %d..%d bytes\n", config.datasize_min, config.datasize_max);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");
        for (j = 0; j <= MAX_LATENCY; j++) {
            if (config.latency[j]) {
                seen += config.latency[j];
                perc = ((float)seen*100)/config.issued_requests;
                printf("%6.2f%% < %d ms\n", perc, j+1);
            }
        }
    } else {
        printf("%.2f requests per second\n", reqpersec);
    }
}

static void prepareForBenchmark(void) {
    memset(config.latency,0,sizeof(int)*(MAX_LATENCY+1));
    config.start = microseconds();
    config.issued_requests = 0;
}

static void endBenchmark(void) {
    config.totlatency = (microseconds()-config.start)/1000;
    showLatencyReport();
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
" set <percentage>     Percentage of SETs (default 50)\n"
" del <percentage>     Percentage of DELs (default 0)\n"
" lpush <percentage>   Percentage of LPUSHs (default 0)\n"
" lpop <percentage>    Percentage of LPOPs (default 0)\n"
" hset <percentage>    Percentage of HSETs (default 0)\n"
" hget <percentage>    Percentage of HGETs (default 0)\n"
" hgetall <percentage> Percentage of HGETs (default 0)\n"
" swapin <percentage>  Percentage of DEBUG SWAPINs (default 0)\n"
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
            config.num_clients = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"requests") && !lastarg) {
            config.num_requests = atoi(argv[i+1]);
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
        } else if (!strcmp(argv[i],"hset") && !lastarg) {
            config.hset_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"hget") && !lastarg) {
            config.hget_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"hgetall") && !lastarg) {
            config.hgetall_perc = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"keepalive") && !lastarg) {
            config.keepalive = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"host") && !lastarg) {
            config.hostip = argv[i+1];
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
        } else if (!strcmp(argv[i],"hashkeyspace") && !lastarg) {
            config.hashkeyspace = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"seed") && !lastarg) {
            config.prngseed = strtol(argv[i+1],NULL,10);
            i++;
        } else if (!strcmp(argv[i],"big")) {
            config.keyspace = 1000000;
            config.num_requests = 1000000;
        } else if (!strcmp(argv[i],"verybig")) {
            config.keyspace = 10000000;
            config.num_requests = 10000000;
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
    if (config.keyspace < 1) config.keyspace = DEFAULT_KEYSPACE;
    if (config.hashkeyspace < 1) config.hashkeyspace = DEFAULT_HASHKEYSPACE;
}

static void ctrlc(int sig) {
    REDIS_NOTUSED(sig);

    if (config.idlemode) {
        exit(1);
    } else {
        config.ctrlc++;
        if (config.ctrlc == 1) {
            config.done = 1;
            printf("\nWaiting for pending requests to complete...\n");
        } else {
            printf("\nForcing exit...\n");
            exit(1);
        }
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
    int i = 0;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.el = aeCreateEventLoop();
    config.debug = 0;
    config.done = 0;
    config.clients = listCreate();
    config.num_clients = 50;
    config.num_requests = 10000;
    config.issued_requests = 0;

    config.keepalive = 1;
    config.set_perc = 50;
    config.del_perc = 0;
    config.swapin_perc = 0;
    config.lpush_perc = 0;
    config.lpop_perc = 0;
    config.hset_perc = 0;
    config.hget_perc = 0;
    config.hgetall_perc = 0;
    config.datasize_min = 1;
    config.datasize_max = 64;
    config.keyspace = DEFAULT_KEYSPACE; /* 100k */
    config.hashkeyspace = DEFAULT_HASHKEYSPACE; /* 1k */
    config.check = 0;
    config.rand = 0;
    config.longtail = 0;
    config.quiet = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.latency = zmalloc(sizeof(int)*(MAX_LATENCY+1));
    config.ctrlc = 0;
    config.prngseed = (unsigned int) (microseconds()^getpid());

    config.hostip = "127.0.0.1";
    config.hostport = 6379;

    parseOptions(argc,argv);
    config.databuf = zmalloc(config.datasize_max);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.num_clients);
        memset(config.optab,REDIS_IDLE,100);
    } else {
        /* Setup the operation table. Start with a table with just GET
         * operations and overwrite it with others as needed. */
        memset(config.optab,REDIS_GET,100);
        fillOpTab(&i,REDIS_SET,config.set_perc);
        fillOpTab(&i,REDIS_DEL,config.del_perc);
        fillOpTab(&i,REDIS_LPUSH,config.lpush_perc);
        fillOpTab(&i,REDIS_LPOP,config.lpop_perc);
        fillOpTab(&i,REDIS_HSET,config.hset_perc);
        fillOpTab(&i,REDIS_HGET,config.hget_perc);
        fillOpTab(&i,REDIS_HGETALL,config.hgetall_perc);
        fillOpTab(&i,REDIS_SWAPIN,config.swapin_perc);
    }

    signal(SIGINT,ctrlc);
    srandom(config.prngseed);
    printf("PRNG seed is: %u - use the 'seed' option to reproduce the same sequence\n", config.prngseed);
    do {
        prepareForBenchmark();
        createMissingClients();
        aeMain(config.el);
        endBenchmark();
    } while(config.loop);

    return 0;
}
