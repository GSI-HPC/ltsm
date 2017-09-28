#include "test_utils.h"

const char ALPHANUM[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

void rnd_str(char *s, const uint16_t len)
{
 	for (uint16_t l = 0; l < len; l++)
		s[l] = ALPHANUM[rand() % (sizeof(ALPHANUM) - 1)];
	s[len] = 0;
}
