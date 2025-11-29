# Kafka HTTP Emitter (Rust)

## Running

The program will work without jemalloc config, but we highly recommend setting
it in order to prevent memory issues.

```
JEMALLOC_SYS_WITH_MALLOC_CONF="background_thread:true,narenas:1,tcache:false,dirty_decay_ms:1,muzzy_decay_ms:1,abort_conf:true,retain:false" cargo r --release
```
