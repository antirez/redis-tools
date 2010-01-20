/* rc4-based pseudo-random number generator for hping.
 * Copyright (C) 2003-2010 Salvatore Sanfilippo
 * All rights reserved */

#ifndef __RC4RAND_H
#define __RC4RAND_H

void rc4rand_seed(unsigned long seed);
void rc4rand_set(unsigned char *dest, size_t len);
unsigned long rc4rand(void);
long rc4rand_between(long min, long max);

#endif
