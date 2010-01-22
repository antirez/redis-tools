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

#include "anet.h"
#include "sds.h"
#include "adlist.h"
#include "zmalloc.h"

#define REDIS_NOTUSED(V) ((void) V)

#define REDIS_REPLY_ERROR 0
#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4

#define STAT_VMSTAT 0

static struct config {
    char *hostip;
    int hostport;
    int delay;
    int stat; /* The kind of output to produce: STAT_* */
} config;

/* This is the reply object returned by redisCommand() */
typedef struct redisReply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    char *reply; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReply;

static redisReply *createReplyObject(int type, sds reply);
static redisReply *redisReadReply(int fd);

/* Connect to a Redis instance. On success NULL is returned and *fd is set
 * to the socket file descriptor. On error a redisReply object is returned
 * with reply->type set to REDIS_REPLY_ERROR and reply->string containing
 * the error message. This replyObject must be freed with redisFreeReply(). */
static redisReply *redisConnect(int *fd) {
    char err[ANET_ERR_LEN];

    *fd = anetTcpConnect(err,config.hostip,config.hostport);
    if (*fd == ANET_ERR)
        return createReplyObject(REDIS_REPLY_ERROR,sdsnew(err));
    anetTcpNoDelay(NULL,*fd);
    return NULL;
}

/* Create a reply object */
static redisReply *createReplyObject(int type, sds reply) {
    redisReply *r = zmalloc(sizeof(*r));

    r->type = type;
    r->reply = reply;
    return r;
}

/* Free a reply object */
static void freeReplyObject(redisReply *r) {
    size_t j;

    switch(r->type) {
    case REDIS_REPLY_INTEGER:
        break; /* Nothing to free */
    case REDIS_REPLY_ARRAY:
        for (j = 0; j < r->elements; j++)
            freeReplyObject(r->element[j]);
        zfree(r->element);
        break;
    default:
        sdsfree(r->reply);
        break;
    }
    zfree(r);
}

static redisReply *redisIOError(void) {
    return createReplyObject(REDIS_REPLY_ERROR,sdsnew("I/O error"));
}

/* In a real high performance C client this should be bufferized */
static sds redisReadLine(int fd) {
    sds line = sdsempty();

    while(1) {
        char c;
        ssize_t ret;

        ret = read(fd,&c,1);
        if (ret == -1) {
            sdsfree(line);
            return NULL;
        } else if ((ret == 0) || (c == '\n')) {
            break;
        } else {
            line = sdscatlen(line,&c,1);
        }
    }
    return sdstrim(line,"\r\n");
}

static redisReply *redisReadSingleLineReply(int fd, int type) {
    sds buf = redisReadLine(fd);
    
    if (buf == NULL) return redisIOError();
    return createReplyObject(type,buf);
}

static redisReply *redisReadIntegerReply(int fd) {
    sds buf = redisReadLine(fd);
    redisReply *r = zmalloc(sizeof(*r));
    
    if (buf == NULL) return redisIOError();
    r->type = REDIS_REPLY_INTEGER;
    r->integer = strtoll(buf,NULL,10);
    return r;
}

static redisReply *redisReadBulkReply(int fd) {
    sds replylen = redisReadLine(fd);
    sds buf;
    char crlf[2];
    int bulklen;

    if (replylen == NULL) return redisIOError();
    bulklen = atoi(replylen);
    sdsfree(replylen);
    if (bulklen == -1)
        return createReplyObject(REDIS_REPLY_NIL,sdsempty());

    buf = sdsnewlen(NULL,bulklen);
    anetRead(fd,buf,bulklen);
    anetRead(fd,crlf,2);
    return createReplyObject(REDIS_REPLY_STRING,buf);
}

static redisReply *redisReadMultiBulkReply(int fd) {
    sds replylen = redisReadLine(fd);
    long elements, j;
    redisReply *r;

    if (replylen == NULL) return redisIOError();
    elements = strtol(replylen,NULL,10);
    sdsfree(replylen);

    if (elements == -1)
        return createReplyObject(REDIS_REPLY_NIL,sdsempty());

    r = zmalloc(sizeof(*r));
    r->type = REDIS_REPLY_ARRAY;
    r->elements = elements;
    r->element = zmalloc(sizeof(*r)*elements);
    for (j = 0; j < elements; j++)
        r->element[j] = redisReadReply(fd);
    return r;
}

static redisReply *redisReadReply(int fd) {
    char type;

    if (anetRead(fd,&type,1) <= 0) return redisIOError();
    switch(type) {
    case '-':
        return redisReadSingleLineReply(fd,REDIS_REPLY_ERROR);
    case '+':
        return redisReadSingleLineReply(fd,REDIS_REPLY_STRING);
    case ':':
        return redisReadIntegerReply(fd);
    case '$':
        return redisReadBulkReply(fd);
    case '*':
        return redisReadMultiBulkReply(fd);
    default:
        printf("protocol error, got '%c' as reply type byte\n", type);
        exit(1);
    }
}

/* Execute a command. This function is printf alike where
 * %s a plain string such as a key, %b a bulk payload. For instance:
 *
 * redisCommand("GET %s", mykey);
 * redisCommand("SET %s %b", mykey, somevalue, somevalue_len);
 *
 * The returned value is a redisReply object that must be freed using the
 * redisFreeReply() function.
 *
 * given a redisReply "reply" you can test if there was an error in this way:
 *
 * if (reply->type == REDIS_REPLY_ERROR) {
 *     printf("Error in request: %s\n", reply->reply);
 * }
 *
 * The replied string itself is in reply->reply if the reply type is
 * a REDIS_REPLY_STRING. If the reply is a multi bulk reply then
 * reply->type is REDIS_REPLY_ARRAY and you can access all the elements
 * in this way:
 *
 * for (i = 0; i < reply->elements; i++)
 *     printf("%d: %s\n", i, reply->element[i]);
 *
 * Finally when type is REDIS_REPLY_INTEGER the long long integer is
 * stored at reply->integer.
 */
redisReply *redisCommand(int fd, char *format, ...) {
    va_list ap;
    size_t size;
    char *arg, *c = format;
    sds cmd = sdsempty();

    /* Build the command string accordingly to protocol */
    va_start(ap,format);
    while(*c != '\0') {
        if (*c != '%' || c[1] == '\0') {
            cmd = sdscatlen(cmd,c,1);
        } else {
            switch(c[1]) {
            case 's':
                arg = va_arg(ap,char*);
                cmd = sdscat(cmd,arg);
                break;
            case 'b':
                arg = va_arg(ap,char*);
                size = va_arg(ap,size_t);
                cmd = sdscatprintf(cmd,"%zu\r\n",size);
                cmd = sdscatlen(cmd,arg,size);
                break;
            case '%':
                cmd = sdscat(cmd,"%");
                break;
            }
        }
        c++;
    }
    cmd = sdscat(cmd,"\r\n");
    va_end(ap);

    /* Send the command via socket */
    anetWrite(fd,cmd,sdslen(cmd));
    sdsfree(cmd);
    return redisReadReply(fd);
}

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

    r = redisConnect(&fd);
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
