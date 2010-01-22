/* Redis stat utility.
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
#include <stdarg.h>

#include "sds.h"
#include "zmalloc.h"
#include "hiredis.h"
#include "anet.h"

#define REDIS_NOTUSED(V) ((void) V)

#define STAT_VMSTAT 0

static struct config {
    char *hostip;
    int hostport;
    int delay;
    int stat; /* The kind of output to produce: STAT_* */
} config;

void usage(char *wrong) {
    if (wrong)
        printf("Wrong option '%s' or option argument missing\n\n",wrong);
    printf(
"Usage: redis-stat <type> ... options ...\n\n"
"Statistic types:\n"
" vmstat               Print information about Redis VM activity.\n"
"\n"
"Options:\n"
" host <hostname>      Server hostname (default 127.0.0.1)\n"
" port <hostname>      Server port (default 6379)\n"
" delay <seconds>      Delay between requests (default: 1 second).\n"
);
    exit(1);
}

static int parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;
        
        if (!strcmp(argv[i],"host") && !lastarg) {
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
        } else if (!strcmp(argv[i],"help")) {
            usage(NULL);
        } else {
            usage(argv[i]);
        }
    }
    return i;
}

void vmstat(int fd) {
    redisReply *r;

    while(1) {
        r = redisCommand(fd,"INFO");
        if (r->type == REDIS_REPLY_ERROR) {
            printf("ERROR: %s\n", r->reply);
        } else if (r->type == REDIS_REPLY_STRING) {
            printf("%s\n", r->reply);
        }
        freeReplyObject(r);
        sleep(config.delay);
    }
}

int main(int argc, char **argv) {
    redisReply *r;
    int fd;

    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.stat = STAT_VMSTAT;
    config.delay = 1;

    parseOptions(argc,argv);

    r = redisConnect(&fd,config.hostip,config.hostport);
    if (r != NULL) {
        printf("Error connecting to Redis server: %s\n", r->reply);
        freeReplyObject(r);
        exit(1);
    }

    switch(config.stat) {
    case STAT_VMSTAT:
        vmstat(fd);
        break;
    }
    return 0;
}
