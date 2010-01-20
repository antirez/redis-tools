/* rc4-based pseudo-random number generator for hping.
 * Copyright (C) 2003-2010 Salvatore Sanfilippo
 * All rights reserved */

#include <sys/types.h>
#include <string.h>

/* The rc4 sbox */
static unsigned char rc4_sbox[256];
static unsigned int rc4_i = 0, rc4_j = 0;

/* Initialize the sbox with an user provided seed. In this implementation
 * we don't care about security, what we need is the ability to reproduce
 * the same sequence starting from the same seed for deterministic testing. */
void rc4rand_seed(unsigned long seed) {
    int i;
    unsigned char *s = (unsigned char*) &seed;

    memcpy(rc4_sbox,"<j$;~1+K`rp_oeTCAGJQbej7`5O>sl/Y/SEg:{6wj1~l,Q/6Eah,Ymh%D?'%DOS+EdW)O](lc9$Wwh*m#AgsjWxX*`HXt?o-Xt^#+&Eb<.cLGe`|.}:cODM0Pt*2|LT$yn6v?>-3:Fpt](_yuo'=g<j]4t*dtq_Z07UaC.1pplWtxrvtLDo437jt-zqvBb{_/,,)ly>*R]r0aizJ)yBbP=b5;w3@8tGkK3LGf0>;0cl?k/JYtbmVNHFM]RlR3=MR",256);
    for (i = 0; i < 256; i++)
        rc4_sbox[i] ^= s[i%(sizeof(seed))];
    rc4_i = rc4_j = 0;
}

/* Set 'len' bytes starting at 'dest' to random bytes */
void rc4rand_set(void *d, size_t len) {
    unsigned char *dest = d;
    unsigned int si, sj, x;
    int i = rc4_i;
    int j = rc4_j;

    for (x = 0; x < len; x++) {
        i = (i+1) & 0xff;
        si = rc4_sbox[i];
        j = (j + si) & 0xff;
        sj = rc4_sbox[j];
        rc4_sbox[i] = sj;
        rc4_sbox[j] = si;
        *dest++ = rc4_sbox[(si+sj)&0xff];
    }
    rc4_i = i;
    rc4_j = j;
}

/* Emite a random unsigned long */
unsigned long rc4rand(void) {
    unsigned long l;

    rc4rand_set(&l,sizeof(l));
    return l;
}

long rc4rand_between(long min, long max) {
    return min+(rc4rand()%(max-min+1));
}

#ifdef RC4RAND_TEST_MAIN
#include <stdio.h>
#include <time.h>
int main(void) {
    int i;
    rc4rand_seed(time(NULL));

    for (i=0;i<10;i++) printf("%lu\n", rc4rand());
    rc4rand_seed(49992);
    printf("%ld\n", rc4rand_between(1,64));
    return 0;
}
#endif
