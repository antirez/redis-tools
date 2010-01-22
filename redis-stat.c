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
#include <limits.h>

#include "sds.h"
#include "zmalloc.h"
#include "hiredis.h"
#include "anet.h"
#include "utils.h"

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

/* Return the specified INFO field from the INFO command output "info".
 * The result must be released calling sdsfree().
 *
 * If the field is not found NULL is returned. */
static sds getInfoField(char *info, char *field) {
    char *p = strstr(info,field);
    char *n;

    if (!p) return NULL;
    p = strchr(p,':')+1;
    n = strchr(p,'\r');
    return sdsnewlen(p,(n-p)+1);
}

/* Like the above function but automatically convert the result into
 * a long. On error (missing field) LONG_MIN is returned. */
static long getLongInfoField(char *info, char *field) {
    sds val = getInfoField(info,field);
    long l;

    if (!val) return LONG_MIN;
    l = strtol(val,NULL,10);
    sdsfree(val);
    return l;
}

void vmstat(int fd) {
    redisReply *r;
    int c = 0;
    long aux, pagein = 0, pageout = 0, usedpages = 0, usedmemory = 0;
    long swapped = 0;

    while(1) {
        char buf[64];

        r = redisCommand(fd,"INFO");
        if (r->type == REDIS_REPLY_ERROR) {
            printf("ERROR: %s\n", r->reply);
            exit(1);
        }

        if ((c % 20) == 0) {
            printf(
" --------------- pages -------------- ----- objects ------ ----- memory -----\n");
            printf(
" in       out      used     delta     swapped   delta      used     delta    \n");
        }

        /* pagein */
        aux = getLongInfoField(r->reply,"vm_stats_swappin_count");
        sprintf(buf,"%ld",aux-pagein);
        pagein = aux;
        printf(" %-9s",buf);

        /* pageout */
        aux = getLongInfoField(r->reply,"vm_stats_swappout_count");
        sprintf(buf,"%ld",aux-pageout);
        pageout = aux;
        printf("%-9s",buf);

        /* used pages */
        aux = getLongInfoField(r->reply,"vm_stats_used_pages");
        sprintf(buf,"%ld",aux);
        printf("%-9s",buf);

        sprintf(buf,"%ld",aux-usedpages);
        usedpages = aux;
        printf("%-9s",buf);

        /* Swapped objects */
        aux = getLongInfoField(r->reply,"vm_stats_swapped_objects");
        sprintf(buf,"%ld",aux);
        printf(" %-10s",buf);

        sprintf(buf,"%ld",aux-swapped);
        swapped = aux;
        printf("%-10s",buf);

        /* Used memory */
        aux = getLongInfoField(r->reply,"used_memory");
        bytesToHuman(buf,aux);
        printf(" %-9s",buf);

        bytesToHuman(buf,aux-usedmemory);
        usedmemory = aux;
        printf("%-9s",buf);

        printf("\n");
        freeReplyObject(r);
        sleep(config.delay);
        c++;
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
