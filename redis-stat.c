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
#include <math.h>

#include "sds.h"
#include "zmalloc.h"
#include "hiredis.h"
#include "anet.h"
#include "utils.h"

#define REDIS_NOTUSED(V) ((void) V)

#define STAT_VMSTAT 0
#define STAT_VMPAGE 1

static struct config {
    char *hostip;
    int hostport;
    int delay;
    int stat; /* The kind of output to produce: STAT_* */
    int samplesize;
} config;

void usage(char *wrong) {
    if (wrong)
        printf("Wrong option '%s' or option argument missing\n\n",wrong);
    printf(
"Usage: redis-stat <type> ... options ...\n\n"
"Statistic types:\n"
" vmstat               Print information about Redis VM activity.\n"
" vmpage               Try to guess the best vm-page-size for your dataset.\n"
"\n"
"Options:\n"
" host <hostname>      Server hostname (default 127.0.0.1)\n"
" port <hostname>      Server port (default 6379)\n"
" delay <milliseconds> Delay between requests (default: 1000 ms, 1 second).\n"
" samplesize <keys>    Number of keys to sample for 'vmpage' stat.\n"
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
        } else if (!strcmp(argv[i],"delay") && !lastarg) {
            config.delay = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"samplesize") && !lastarg) {
            config.samplesize = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"vmstat")) {
            config.stat = STAT_VMSTAT;
        } else if (!strcmp(argv[i],"vmpage")) {
            config.stat = STAT_VMPAGE;
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

static void vmstat(int fd) {
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
" --------------- objects --------------- ------ pages ------ ----- memory -----\n");
            printf(
" load-in  swap-out  swapped   delta      used     delta      used     delta    \n");
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

        /* Swapped objects */
        aux = getLongInfoField(r->reply,"vm_stats_swapped_objects");
        sprintf(buf,"%ld",aux);
        printf(" %-10s",buf);

        sprintf(buf,"%ld",aux-swapped);
        if (aux-swapped == 0) printf(" ");
        else if (aux-swapped > 0) printf("+");
        swapped = aux;
        printf("%-10s",buf);

        /* used pages */
        aux = getLongInfoField(r->reply,"vm_stats_used_pages");
        sprintf(buf,"%ld",aux);
        printf("%-9s",buf);

        sprintf(buf,"%ld",aux-usedpages);
        if (aux-usedpages == 0) printf(" ");
        else if (aux-usedpages > 0) printf("+");
        usedpages = aux;
        printf("%-9s",buf);

        /* Used memory */
        aux = getLongInfoField(r->reply,"used_memory");
        bytesToHuman(buf,aux);
        printf(" %-9s",buf);

        bytesToHuman(buf,aux-usedmemory);
        if (aux-usedmemory == 0) printf(" ");
        else if (aux-usedmemory > 0) printf("+");
        usedmemory = aux;
        printf("%-9s",buf);

        printf("\n");
        freeReplyObject(r);
        usleep(config.delay*1000);
        c++;
    }
}

size_t getSerializedLen(int fd, char *key) {
    redisReply *r;
    size_t sl = 0;

    /* The value may be swapped out, try to load it */
    r = redisCommand(fd,"GET %s",key);
    freeReplyObject(r);

    r = redisCommand(fd,"DEBUG OBJECT %s",key);
    if (r->type == REDIS_REPLY_STRING) {
        char *p;

        p = strstr(r->reply,"serializedlength:");
        if (p) sl = strtol(p+17,NULL,10);
    } else {
        printf("%s\n", r->reply);
    }
    freeReplyObject(r);
    return sl;
}

/* The following function implements the "vmpage" statistic, that is,
 * it tries to perform a few simulations with data gained from the dataset
 * in order to understand what's the best vm-page-size for your dataset.
 *
 * How dows it work? We use VMPAGE_PAGES pages of different sizes, and simulate
 * adding data with sizes sampled from the real dataset, in a random way.
 * While doing this we take stats about how efficiently our application is
 * using the swap file with every given page size. The best will win.
 *
 * Why the thing we take "fixed" is the number of pages? It's our constant
 * as Redis will use one bit of RAM for every page in the swap file, so we
 * want to optimized page size while the number of pages is taken fixed. */
#define VMPAGE_PAGES 1000000
static void vmpage(int fd) {
    redisReply *r;
    size_t *samples = zmalloc(config.samplesize*sizeof(size_t));
    size_t totsl = 0;
    int j, pagesize, bestpagesize = 0;
    double bestscore = 0;

    printf("Sampling %d random keys from DB 0...\n", config.samplesize);
    for (j = 0; j < config.samplesize; j++) {
        size_t sl;

        /* Select a RANDOM key */
        r = redisCommand(fd,"RANDOMKEY");
        if (r->type == REDIS_REPLY_NIL) {
            printf("Sorry but DB 0 is empty\n");
            exit(1);
        } else if (r->type == REDIS_REPLY_ERROR) {
            printf("Error: %s\n", r->reply);
            exit(1);
        }
        /* Store the lenght of this object in our sampling vector */
        sl = getSerializedLen(fd, r->reply);
        freeReplyObject(r);

        if (sl == 0) {
            /* Problem getting the  length of this object, don't count
             * this try. */
            j--;
            continue;
        }
        samples[j] = sl;
        totsl += sl;
    }
    printf("Average serialized value size is: %zu\n", totsl/config.samplesize);
    /* Compute the standard deviation */
    {
        size_t deltasum = 0, avg = totsl/config.samplesize;

        for (j = 0; j < config.samplesize; j++) {
            long delta = avg-samples[j];

            deltasum += delta*delta;
        }
        printf("Standard deviation: %.2lf\n",
            sqrt(deltasum/config.samplesize));
    }
    printf("Simulate fragmentation with different page sizes...\n");
    for (pagesize = 8; pagesize <= 1024*64; pagesize*=2) {
        size_t totpages = VMPAGE_PAGES;
        unsigned char *pages = zmalloc(totpages);
        size_t stored_bytes, used_pages;
        double score;

        printf("%d: ",pagesize);
        fflush(stdout);
        stored_bytes = used_pages = 0;
        memset(pages,0,totpages);
        while(1) {
            int r = random() % config.samplesize;
            size_t bytes_needed = samples[r];
            size_t pages_needed = (bytes_needed+(pagesize-1))/pagesize;

            for(j = 0; j < 200; j++) {
                size_t off = random()%(totpages-(pages_needed-1));
                size_t i;

                for (i = off; i < off+pages_needed; i++)
                    if (pages[i] != 0) break;
                if (i == off+pages_needed) {
                    memset(pages+off,1,pages_needed);
                    used_pages += pages_needed;
                    stored_bytes += bytes_needed;
                    break;
                }
            }
            if (j == 200) break;
        }
        printf("bytes per page: %.2lf, space efficiency: %.2lf%%\n",
            (double)stored_bytes/totpages,
            ((double)stored_bytes*100)/(totpages*pagesize));
        score = ((double)stored_bytes/totpages)*
                (((double)stored_bytes*100)/(totpages*pagesize));
        if (bestpagesize == 0 || bestscore < score) {
            bestpagesize = pagesize;
            bestscore = score;
        }
        zfree(pages);
    }
    printf("\nThe best compromise between bytes per page and swap file size: %d\n", bestpagesize);
}

int main(int argc, char **argv) {
    redisReply *r;
    int fd;

    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.stat = STAT_VMSTAT;
    config.delay = 1000;
    config.samplesize = 10000;

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
    case STAT_VMPAGE:
        vmpage(fd);
        break;
    }
    return 0;
}
