/* Representation check for borrowed UTF-8 text; no allocation or I/O. */
#ifndef SEMLOOM_SEM_TEXT_H
#define SEMLOOM_SEM_TEXT_H

#include <stdbool.h>
#include <stdint.h>

extern bool semloom_text_is_utf8_no_nul(const uint8_t *data, uint32_t length);

#endif
