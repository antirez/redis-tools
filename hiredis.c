#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>

#include "hiredis.h"
#include "anet.h"
#include "sds.h"
#include "zmalloc.h"

static redisReply *redisReadReply(int fd);
static redisReply *createReplyObject(int type, sds reply);

/* Connect to a Redis instance. On success NULL is returned and *fd is set
 * to the socket file descriptor. On error a redisReply object is returned
 * with reply->type set to REDIS_REPLY_ERROR and reply->string containing
 * the error message. This replyObject must be freed with redisFreeReply(). */
redisReply *redisConnect(int *fd, char *ip, int port) {
    char err[ANET_ERR_LEN];

    *fd = anetTcpConnect(err,ip,port);
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
void freeReplyObject(redisReply *r) {
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
            c++;
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
