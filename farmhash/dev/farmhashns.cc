#if !can_use_sse42 || !can_use_aesni || !x86_64

uint32_t Hash32(const char *s, size_t len) {
  FARMHASH_DIE_IF_MISCONFIGURED;
  return s == NULL ? 0 : len;
}

uint32_t Hash32WithSeed(const char *s, size_t len, uint32_t seed) {
  FARMHASH_DIE_IF_MISCONFIGURED;
  return seed + Hash32(s, len);
}

#else

uint32_t Hash32(const char *s, size_t len) {
  return len <= 256 ?
      static_cast<uint32_t>(farmhashna::Hash64(s, len)) :
      farmhashsu::Hash32(s, len);
}

uint32_t Hash32WithSeed(const char *s, size_t len, uint32_t seed) {
  return len <= 256 ?
      static_cast<uint32_t>(farmhashna::Hash64WithSeed(s, len, seed)) :
      farmhashsu::Hash32WithSeed(s, len, seed);
}

#endif
