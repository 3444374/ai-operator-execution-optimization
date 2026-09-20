#ifndef SEMLOOM_SEMANTIC_IMAGE_CONTRACT_H
#define SEMLOOM_SEMANTIC_IMAGE_CONTRACT_H

#include <stdbool.h>
#include <stdint.h>

#define SEMLOOM_IMAGE_PLAN_SCHEMA_VERSION 5
#define SEMLOOM_IMAGE_MAX_INPUT_BYTES 262144
#define SEMLOOM_IMAGE_MAX_PIXELS 16777216
#define SEMLOOM_IMAGE_MAX_DIMENSION 4096
#define SEMLOOM_IMAGE_SPEC_ID "semloom.semantic.image_embed.clip.v1"
#define SEMLOOM_IMAGE_PARSER_ID "semloom.image_embed.finite_float4.v1"
#define SEMLOOM_IMAGE_PREPARE_ID "semloom.image_embed.clip_rgb_fp32.v1"

/* Network-order IEEE float4 values, without a PostgreSQL array header. */
extern bool semloom_image_vector_valid(const uint8_t *bytes, uint32_t length, uint32_t dimension);

#endif
