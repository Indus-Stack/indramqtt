`jwks.cache_ttl_secs` sets how long a cached JWKS key stays valid in seconds. The default is 300. The range is 1 to 86400. An expired key reads as a miss and triggers a refresh.
