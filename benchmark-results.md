# Benchmark results

Run on 2026-09-16

## 1. DB smaller than RAM

### Machine specifications

- Provider/model: Vultr VX, KVM virtual machine
- CPU: 4 vCPUs, AMD EPYC-Turin Processor, 1 socket, 2 cores, 2 threads/core
- Memory: 30 GiB RAM
- Swap: 8 GiB
- Storage: 240 GB virtual disk
- OS: Ubuntu 26.04 LTS, Linux 7.0.0-31-generic, x86-64

### Run configuration

- Workflow: 1
- Profile: `enhanced`
- Items: 11,000,000
- Verification: every 1,000 reads
- Database directory deleted before and after each engine run


| Measured item                       | Pogreb           | HashTableDB      | bbolt              | LMDB               | RocksDB            | ForestDB           | BadgerDB           | SQLite             | Wiredtiger         | LevelDB            | Pebble             |
| ----------------------------------- | ---------------: | ---------------: | -----------------: | -----------------: | -----------------: | -----------------: | -----------------: | -----------------: | -----------------: | -----------------: | -----------------: |
| Total                               | 2m41.355s        | 2m44.773s        | 51m54.629s         | 10m23.434s         | 18m33.626s         | 20m15.843s         | 20m14.059s         | 40m36.713s         | 38m47.542s         | 44m0.516s          | 41m48.834s         |
| cold_random_read                    | 12.863s / 855.2k | 16.068s / 684.6k | 32.678s / 336.6k   | 33.208s / 331.2k   | 2m8.564s / 85.6k   | 2m13.423s / 82.4k  | 2m38.448s / 69.4k  | 5m1.407s / 36.5k   | 5m42.903s / 32.1k  | 5m44.632s / 31.9k  | 6m8.155s / 29.9k   |
| warm_random_read                    | 10.441s / 1.054M | 12.495s / 880.4k | 32.651s / 336.9k   | 27.845s / 395.0k   | 1m32.228s / 119.3k | 1m20.923s / 135.9k | 1m53.873s / 96.6k  | 2m16.511s / 80.6k  | 5m11.844s / 35.3k  | 4m34.125s / 40.1k  | 3m59.832s / 45.9k  |
| cold_sequential_read                | 6.574s / 1.673M  | 8.77s / 1.254M   | 31.385s / 350.5k   | 32.167s / 342.0k   | 3m46.452s / 48.6k  | 49.094s / 224.1k   | 1m57.525s / 93.6k  | 4m21.226s / 42.1k  | 5m10.035s / 35.5k  | 3m50.947s / 47.6k  | 7m42.753s / 23.8k  |
| warm_sequential_read                | 5.975s / 1.841M  | 6.855s / 1.605M  | 31.354s / 350.8k   | 26.753s / 411.2k   | 2m2.869s / 89.5k   | 48.133s / 228.5k   | 1m55.651s / 95.1k  | 2m15.704s / 81.1k  | 4m59.36s / 36.7k   | 2m46.786s / 66.0k  | 3m39.702s / 50.1k  |
| cold_concurrent_readers             | 24.619s / 2.234M | 20.628s / 2.666M | 1m7.744s / 811.9k  | 1m2.829s / 875.4k  | 3m42.372s / 247.3k | 3m0.882s / 304.1k  | 3m53.458s / 235.6k | 5m44.137s / 159.8k | 5m54.003s / 155.4k | 11m36.102s / 79.0k | 9m21.183s / 98.0k  |
| warm_concurrent_readers             | 23.898s / 2.301M | 19.474s / 2.824M | 1m7.748s / 811.8k  | 1m1.788s / 890.1k  | 3m24.058s / 269.5k | 2m47.444s / 328.5k | 3m45.187s / 244.2k | 5m10.354s / 177.2k | 5m22.781s / 170.4k | 11m23.644s / 80.5k | 8m30.546s / 107.7k |
| cold_full_iteration                 | 14.373s / 939.3k | 7.675s / 1.759M  | 1m32.527s / 145.9k | 19.702s / 685.2k   | 16.905s / 798.6k   | 4m35.877s / 48.9k  | 12.734s / 1.060M   | 2m46.827s / 80.9k  | 33.655s / 401.1k   | 14.084s / 958.5k   | 13.629s / 990.5k   |
| warm_full_iteration                 | 9.973s / 1.354M  | 5.559s / 2.428M  | 3.807s / 3.546M    | 11.097s / 1.217M   | 18.809s / 717.7k   | 41.69s / 323.8k    | 10.801s / 1.250M   | 23.163s / 582.8k   | 28.069s / 481.0k   | 8.376s / 1.612M    | 10.332s / 1.307M   |
| cold_tx_write                       | 5.088s / 98.3k   | 8.491s / 58.9k   | 21.573s / 23.2k    | 33.37s / 15.0k     | 10.021s / 49.9k    | 16.459s / 30.4k    | 6.078s / 82.3k     | 2m5.738s / 4.0k    | 52.959s / 9.4k     | 48.197s / 10.4k    | 17.487s / 28.6k    |
| bulk_set_phase_1                    | 4.974s / 442.3k  | 4.88s / 450.8k   | 22.528s / 97.7k    | 15.954s / 137.9k   | 5.81s / 378.6k     | 22.44s / 98.0k     | 8.32s / 264.4k     | 1m6.142s / 33.3k   | 12.836s / 171.4k   | 7.068s / 311.2k    | 5.069s / 434.0k    |
| bulk_set_phase_2                    | 5.521s / 398.5k  | 4.797s / 458.7k  | 29.558s / 74.4k    | 24.094s / 91.3k    | 6.244s / 352.4k    | 26.754s / 82.2k    | 10.102s / 217.8k   | 1m34.414s / 23.3k  | 36.069s / 61.0k    | 15.003s / 146.6k   | 7.101s / 309.8k    |
| bulk_set_phase_3                    | 5.685s / 387.0k  | 5.177s / 425.0k  | 36.174s / 60.8k    | 28.891s / 76.1k    | 6.318s / 348.2k    | 30.229s / 72.8k    | 21.949s / 100.2k   | 1m40.061s / 22.0k  | 42.94s / 51.2k     | 14.555s / 151.1k   | 8.79s / 250.3k     |
| bulk_set_phase_4                    | 5.972s / 368.4k  | 5.208s / 422.4k  | 39.305s / 56.0k    | 32.326s / 68.1k    | 6.076s / 362.1k    | 31.456s / 69.9k    | 47.272s / 46.5k    | 1m43.413s / 21.3k  | 48.395s / 45.5k    | 19.291s / 114.0k   | 9.583s / 229.6k    |
| bulk_set_phase_5                    | 5.931s / 370.9k  | 5.47s / 402.2k   | 42.002s / 52.4k    | 35.105s / 62.7k    | 6.083s / 361.7k    | 34.205s / 64.3k    | 21.219s / 103.7k   | 1m43.51s / 21.3k   | 45.513s / 48.3k    | 18.032s / 122.0k   | 7.44s / 295.7k     |
| tx_write_only                       | 1.29s / 387.7k   | 1.601s / 312.3k  | 12.916s / 38.7k    | 28.046s / 17.8k    | 1.365s / 366.3k    | 8.552s / 58.5k     | 2.308s / 216.7k    | 24.094s / 20.8k    | 10.9s / 45.9k      | 1.042s / 479.8k    | 1.249s / 400.2k    |
| tx_write_recent_read                | 1.714s / 291.7k  | 1.96s / 255.1k   | 13.452s / 37.2k    | 22.747s / 22.0k    | 6.921s / 72.2k     | 11.946s / 41.9k    | 24.014s / 20.8k    | 32.606s / 15.3k    | 13.204s / 37.9k    | 21.347s / 23.4k    | 11.261s / 44.4k    |
| tx_write_random_read                | 2.232s / 224.1k  | 2.269s / 220.4k  | 13.157s / 38.0k    | 22.818s / 21.9k    | 13.108s / 38.1k    | 16.649s / 30.0k    | 21.976s / 22.8k    | 33.139s / 15.1k    | 13.653s / 36.6k    | 20.933s / 23.9k    | 12.815s / 39.0k    |
| tx_write_under_readers              | 8.018s / 62.4k   | 10.264s / 48.7k  | 23.895s / 20.9k    | 1m37.708s / 5.1k   | 13.25s / 37.7k     | 31.274s / 16.0k    | 23.606s / 21.2k    | 1m4.281s / 7.8k    | 34.844s / 14.3k    | 46.025s / 10.9k    | 27.061s / 18.5k    |
| reads_under_tx_writes               | 8.018s / 317.0k  | 10.264s / 2.617M | 23.895s / 296.4k   | 1m37.708s / 805.2k | 13.25s / 49.3k     | 31.274s / 291.5k   | 23.606s / 52.5k    | 1m4.281s / 134.1k  | 34.844s / 263.3k   | 46.025s / 21.4k    | 27.061s / 44.3k    |
| close_after_writes                  | 161ms            | 298ms            | 171ms              | 135ms              | 74ms               | 48ms               | 29.128s            | 68ms               | 3.02s              | 27ms               | 627ms              |
| close_after_reopen                  | 1ms              | 3ms              | 259ms              | 0s                 | 2ms                | 33ms               | 74ms               | 0s                 | 1ms                | 1ms                | 26ms               |
| cache_reset_cold_tx_write           | 1.899s           | 1.395s           | 1.554s             | 1.035s             | 573ms              | 2.902s             | 1.102s             | 1.318s             | 946ms              | 1.568s             | 1.527s             |
| cache_reset_cold_random_read        | 1.452s           | 1.836s           | 15.889s            | 1.44s              | 730ms              | 1.155s             | 1.059s             | 2.053s             | 2.965s             | 1.669s             | 1.588s             |
| cache_reset_cold_sequential_read    | 753ms            | 668ms            | 15.937s            | 998ms              | 638ms              | 833ms              | 1.251s             | 1.895s             | 1.664s             | 1.884s             | 2.865s             |
| cache_reset_cold_concurrent_readers | 73ms             | 138ms            | 16.002s            | 983ms              | 1.289s             | 198ms              | 823ms              | 1.68s              | 1.517s             | 1.935s             | 2.962s             |
| cache_reset_cold_full_iteration     | 619ms            | 645ms            | 16.021s            | 1.019s             | 1.443s             | 886ms              | 937ms              | 1.805s             | 2.161s             | 1.874s             | 3.145s             |
| size_after_bulk                     | 8.40 GB          | 8.62 GB          | 13.53 GB           | 15.21 GB           | 8.85 GB            | 22.37 GB           | 8.94 GB            | 10.55 GB           | 8.90 GB            | —                  | 8.30 GB            |
| size_after_close                    | 13.39 GB         | 13.98 GB         | 19.34 GB           | 21.73 GB           | 13.15 GB           | 35.66 GB           | 13.39 GB           | 15.46 GB           | 14.67 GB           | 13.19 GB           | 13.36 GB           |
| size_after_reopen_close             | 13.39 GB         | 13.18 GB         | 19.34 GB           | 21.73 GB           | 13.15 GB           | 35.66 GB           | 13.39 GB           | 15.46 GB           | 14.67 GB           | 13.19 GB           | 13.17 GB           |
| final_size                          | 17.23 GB         | 17.01 GB         | 24.96 GB           | 27.45 GB           | 16.99 GB           | 41.70 GB           | 17.38 GB           | 19.76 GB           | 29.29 GB           | 17.05 GB           | 17.02 GB           |



## 2. DB bigger than RAM

### Machine specifications

- Provider/model: Linode Compute Instance
- CPU: 2 vCPUs, AMD EPYC 7713 64-Core Processor
- Memory: 3.8 GiB RAM
- Swap: 512 MiB
- Storage: 79.5 GB `sda` root disk; 512 MiB `sdb` swap disk
- OS: Ubuntu 26.04 LTS, Linux 7.0.0-22-generic, x86-64

### Run configuration

- Profile: `constrained`
- Items: default Workflow 1 size (2,000,000)
- Verification: every 1,000 reads
- Database directory deleted before and after each engine run


| Measured item                       | HashTableDB        | ForestDB          | bbolt             | LMDB              | Pogreb             | BadgerDB           | LevelDB           | RocksDB            | Wiredtiger        | SQLite             | Pebble             |
| ----------------------------------- | -----------------: | ----------------: | ----------------: | ----------------: | -----------------: | -----------------: | ----------------: | -----------------: | ----------------: | -----------------: | -----------------: |
| Total                               | 12m42.74s          | 49m22.701s        | 3h47m15.332s      | 1h11m26.55s       | 1h14m18.601s       | 2h10m15.761s       | 1h34m19.272s      | 1h47m2.161s        | 1h46m45.415s      | 2h17m16.789s       | 2h5m46.043s        |
| cold_random_read                    | 2m40.518s / 12.5k  | 3m23.977s / 9.8k  | 3m24.278s / 9.8k  | 3m41.817s / 9.0k  | 3m54.229s / 8.5k   | 6m4.463s / 5.5k    | 7m5.138s / 4.7k   | 7m58.763s / 4.2k   | 8m20.274s / 4.0k  | 8m22.775s / 4.0k   | 10m20.684s / 3.2k  |
| warm_random_read                    | 2m0.772s / 16.6k   | 2m46.825s / 12.0k | 3m21.267s / 9.9k  | 3m16.788s / 10.2k | 3m46.814s / 8.8k   | 5m59.013s / 5.6k   | 6m3.084s / 5.5k   | 7m24.896s / 4.5k   | 8m12.179s / 4.1k  | 7m45.258s / 4.3k   | 9m38.481s / 3.5k   |
| cold_sequential_read                | 17.675s / 367.8k   | 1m5.379s / 99.4k  | 13m27.594s / 8.0k | 14m15.586s / 7.6k | 12.481s / 520.8k   | 39m49.934s / 2.7k  | 28m14.091s / 3.8k | 32m29.235s / 3.3k  | 24m19.642s / 4.5k | 28m18.536s / 3.8k  | 38m10.131s / 2.8k  |
| warm_sequential_read                | 13.404s / 484.9k   | 1m3.384s / 102.6k | 13m28.113s / 8.0k | 13m0.341s / 8.3k  | 12.88s / 504.6k    | 40m15.621s / 2.7k  | 27m30.493s / 3.9k | 31m31.787s / 3.4k  | 23m54.265s / 4.5k | 27m51.324s / 3.9k  | 37m19.904s / 2.9k  |
| cold_concurrent_readers             | 2m57.587s / 56.3k  | 5m50.183s / 28.6k | 8m54.068s / 18.7k | 4m24.087s / 37.9k | 10m16.095s / 16.2k | 15m20.913s / 10.9k | 9m59.066s / 16.7k | 10m38.821s / 15.7k | 8m52.697s / 18.8k | 10m53.008s / 15.3k | 13m10.891s / 12.6k |
| warm_concurrent_readers             | 2m46.053s / 60.2k  | 5m40.243s / 29.4k | 8m52.85s / 18.8k  | 4m22.229s / 38.1k | 10m13.328s / 16.3k | 15m17.526s / 10.9k | 9m59.937s / 16.7k | 10m31.528s / 15.8k | 8m56.56s / 18.6k  | 10m35.332s / 15.7k | 12m54.72s / 12.9k  |
| cold_full_iteration                 | 12.117s / 618.9k   | 13m19.318s / 9.4k | 6m29.04s / 19.3k  | 7m43.593s / 16.2k | 22m2.852s / 5.7k   | 53.903s / 139.1k   | 21.642s / 346.5k  | 27.617s / 271.6k   | 1m37.69s / 76.8k  | 5m59.732s / 20.8k  | 12.772s / 587.2k   |
| warm_full_iteration                 | 11.383s / 658.9k   | 12m31.77s / 10.0k | 6m32.777s / 19.1k | 6m45.865s / 18.5k | 22m1.439s / 5.7k   | 56.331s / 133.1k   | 19.553s / 383.6k  | 24.612s / 304.7k   | 1m27.084s / 86.1k | 5m13.873s / 23.9k  | 12.64s / 593.3k    |
| cold_tx_write                       | 8.584s / 23.3k     | 13.163s / 15.2k   | 50.162s / 4.0k    | 1m4.573s / 3.1k   | 5.365s / 37.3k     | 7.24s / 27.6k      | 49.43s / 4.0k     | 1m4.411s / 3.1k    | 1m6.166s / 3.0k   | 2m5.909s / 1.6k    | 24.636s / 8.1k     |
| bulk_set_phase_1                    | 5.308s / 244.9k    | 14.073s / 92.4k   | 19.659s / 66.1k   | 15.331s / 84.8k   | 8.137s / 159.8k    | 13.846s / 93.9k    | 13.38s / 97.2k    | 5.12s / 253.9k     | 36.921s / 35.2k   | 1m21.776s / 15.9k  | 7.907s / 164.4k    |
| bulk_set_phase_2                    | 5.769s / 225.4k    | 19.058s / 68.2k   | 49.761s / 26.1k   | 29.145s / 44.6k   | 8.422s / 154.4k    | 32.407s / 40.1k    | 20.936s / 62.1k   | 9.41s / 138.1k     | 51.34s / 25.3k    | 2m7.574s / 10.2k   | 13.01s / 99.9k     |
| bulk_set_phase_3                    | 7.536s / 172.5k    | 22.642s / 57.4k   | 2m18.543s / 9.4k  | 2m9.422s / 10.0k  | 8.526s / 152.5k    | 46.389s / 28.0k    | 29.346s / 44.3k   | 15.182s / 85.6k    | 2m2.243s / 10.6k  | 3m32.074s / 6.1k   | 18.023s / 72.1k    |
| bulk_set_phase_4                    | 9.463s / 137.4k    | 33.431s / 38.9k   | 2m49.359s / 7.7k  | 2m43.509s / 8.0k  | 8.914s / 145.8k    | 52.095s / 25.0k    | 31.014s / 41.9k   | 21.057s / 61.7k    | 5m18.35s / 4.1k   | 6m31.459s / 3.3k   | 20.295s / 64.1k    |
| bulk_set_phase_5                    | 10.728s / 121.2k   | 34.93s / 37.2k    | 3m45.109s / 5.8k  | 3m3.266s / 7.1k   | 9.401s / 138.3k    | 1m1.977s / 21.0k   | 30.942s / 42.0k   | 14.689s / 88.5k    | 6m0.625s / 3.6k   | 8m49.193s / 2.5k   | 22.484s / 57.8k    |
| tx_write_only                       | 2.83s / 70.7k      | 5.813s / 34.4k    | 40.417s / 4.9k    | 54.446s / 3.7k    | 1.511s / 132.3k    | 9.857s / 20.3k     | 7.409s / 27.0k    | 7.86s / 25.4k      | 54.572s / 3.7k    | 1m25.072s / 2.4k   | 5.134s / 39.0k     |
| tx_write_recent_read                | 3.317s / 60.3k     | 7.787s / 25.7k    | 56.644s / 3.5k    | 1m5.012s / 3.1k   | 1.746s / 114.5k    | 32.408s / 6.2k     | 12.541s / 15.9k   | 51.675s / 3.9k     | 1m22.606s / 2.4k  | 2m6.053s / 1.6k    | 12.796s / 15.6k    |
| tx_write_random_read                | 18.607s / 10.7k    | 42.335s / 4.7k    | 50.492s / 4.0k    | 1m4.345s / 3.1k   | 33.165s / 6.0k     | 1m4.507s / 3.1k    | 42.038s / 4.8k    | 1m19.314s / 2.5k   | 1m37.554s / 2.1k  | 2m5.144s / 1.6k    | 37.726s / 5.3k     |
| tx_write_under_readers              | 6.835s / 29.3k     | 19.444s / 10.3k   | 48.838s / 4.1k    | 1m0.963s / 3.3k   | 7.334s / 27.3k     | 8.828s / 22.7k     | 42.617s / 4.7k    | 1m1.294s / 3.3k    | 1m7.124s / 3.0k   | 2m5.994s / 1.6k    | 57.551s / 3.5k     |
| reads_under_tx_writes               | 6.835s / 6.3k      | 19.444s / 14.2k   | 48.838s / 12.4k   | 1m0.963s / 22.7k  | 7.334s / 70.1k     | 8.828s / 32.3k     | 42.617s / 4.7k    | 1m1.294s / 3.1k    | 1m7.124s / 6.8k   | 2m5.994s / 7.5k    | 57.551s / 5.3k     |
| close_after_writes                  | 159ms              | 10ms              | 125ms             | 69ms              | 120ms              | 201ms              | 7ms               | 4ms                | 759ms             | 194ms              | 82ms               |
| close_after_reopen                  | 2ms                | 4ms               | 154ms             | 0s                | 1ms                | 44ms               | 0s                | 8ms                | 2ms               | 0s                 | 96ms               |
| cache_reset_cold_tx_write           | 1.028s             | 1.52s             | 1.265s            | 629ms             | 1.195s             | 485ms              | 1.115s            | 451ms              | 874ms             | 959ms              | 1.083s             |
| cache_reset_cold_random_read        | 919ms              | 940ms             | 14m33.944s        | 574ms             | 685ms              | 584ms              | 929ms             | 51ms               | 1.434s            | 1.277s             | 756ms              |
| cache_reset_cold_sequential_read    | 786ms              | 1.144s            | 14m14.345s        | 1.341s            | 1.189s             | 1.36s              | 1.214s            | 1.081s             | 1.068s            | 1.22s              | 1.068s             |
| cache_reset_cold_concurrent_readers | 161ms              | 650ms             | 14m16.425s        | 1.113s            | 315ms              | 1.409s             | 956ms             | 1.068s             | 962ms             | 944ms              | 931ms              |
| cache_reset_cold_full_iteration     | 955ms              | 1.319s            | 14m13.757s        | 1.32s             | 1.246s             | 1.415s             | 992ms             | 1.087s             | 1.049s            | 1.073s             | 945ms              |
| size_after_bulk                     | 5.08 GB            | 10.08 GB          | 8.11 GB           | 7.86 GB           | 4.96 GB            | 5.12 GB            | 4.85 GB           | 5.15 GB            | 5.34 GB           | 6.47 GB            | 4.89 GB            |
| size_after_close                    | 6.94 GB            | 14.21 GB          | 12.20 GB          | 10.73 GB          | 6.96 GB            | 6.96 GB            | 6.83 GB           | 6.83 GB            | 7.66 GB           | 8.05 GB            | 6.90 GB            |
| size_after_reopen_close             | 6.81 GB            | 14.21 GB          | 12.20 GB          | 10.73 GB          | 6.96 GB            | 6.96 GB            | 6.83 GB           | 6.83 GB            | 7.66 GB           | 8.05 GB            | 6.84 GB            |
| final_size                          | 8.35 GB            | 16.27 GB          | 14.40 GB          | 13.02 GB          | 8.50 GB            | 8.53 GB            | 8.37 GB           | 8.37 GB            | 13.75 GB          | 9.75 GB            | 8.38 GB            |
