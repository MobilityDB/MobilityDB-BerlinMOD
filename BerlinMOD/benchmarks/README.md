# BerlinMOD Benchmarks

The MobilityDB ecosystem runs the same BerlinMOD spatial predicates on **six
platforms** through a single shared kernel (MEOS). The platforms split into two
families by operational mode — choose based on your use case, not on which
predicates are available:

| Family | Platforms | Query model | Data model | Metric |
|---|---|---|---|---|
| **[Batch SQL](batch/)** | MobilityDB · MobilityDuck · MobilitySpark | SQL over stored data; GiST / SP-GiST indexed | Complete trips, stored and indexed | Query latency (ms – s) |
| **[Stream](stream/)** | MobilityFlink · MobilityKafka · MobilityNebula | Continuous / windowed / snapshot over arriving events | Instant stream, no prior storage required | Throughput (events/s) |

The **snapshot** streaming form bridges the two families: by contract, the stream
snapshot at watermark `T` equals the batch result on the same data up to `T`.
