# MP3 deployment and demo scripts

Same style as the MP2 scripts (`run_mp`, `kill_mp`, `deploy_mp`, `cmd_mp`, `show_mp`), driven from
your laptop over ssh to `fa26-cs425-4801 … 4810.cs.illinois.edu`. What is different from MP2:
MP3 nodes are daemons (no stdin REPL), so commands run through `./cmd_mp <vm> "<command>"`,
which runs the client on that VM, and every node starts with `--bootstrap` the first time and
`--recover` afterwards (that is what makes a `kill -9`'d node come back with its data).

**Never run these against the real VMs without the team's go-ahead.** Practice locally first:

```bash
export MP3_TARGET=local          # ten processes on this machine, same scripts
export MP3_LOCAL_ROOT=$HOME/mp3_local
```

## One-time setup (real VMs)

```bash
export MP3_USER=<your netid>     # default is kbalaji4, as in MP2
./deploy_mp all                  # copies code, manifest, inventory to ~/mp3_deploy on each VM
./push_data /path/to/dataset     # business_1..business_20 onto every VM (~/mp3_deploy/data)
```

Needs `python3` ≥ 3.10 on the VMs (`./check_prep` reports the version) and `tmux` (optional; without it
the node runs under nohup). Ports 65441–65444 on each VM (kv, membership, log query, control).

## Before every demo / practice run

```bash
./reset_mp all                   # fresh databases and logs (nodes must be stopped)
./run_mp all                     # all ten start in Strong mode and join membership
./check_prep                     # python, dataset, node up, 10 members, no keys: must print ALL TEN VMs READY
```

## Everyday commands

| Script | What it does |
|---|---|
| `./run_mp <vm…\|all>` | start, or restart after a crash (recovers the existing database) |
| `./kill_mp <vm…\|all>` | `kill -9` the node process only (never the VM) |
| `./stop_mp <vm…\|all>` | graceful stop (SIGTERM) |
| `./cmd_mp <vm> "<command>"` | run a client command on that VM; `@data/` and `@out/` name its dataset and output dirs |
| `./status_mp [vm…]` | up/down and pid per VM |
| `./show_mp <vm> [-f]` | node console output |
| `./logs_mp <vm> [pattern] [n]` | the VM's `machine.<vm>.log` filtered by a pattern |
| `./attach_mp <vm>` | tmux session `mp3` on the VM: window `node` (output), window `cli` (shell; use `./mp3 put k f`) |
| `./reset_mp <vm…\|all>` | delete databases, logs and outputs (only when stopped; reset all ten together) |

Commands for `cmd_mp`: `put key file`, `get key file`, `ls key`, `liststore`,
`getfromreplica vmNN key file`, `list_mem_ids`, `multiput(key, VM1, VM2, …)`,
`consistency_mode strong|eventual-noRR|eventual-RR`.

## Demo tests (all parameters are arguments; `<file>` is a dataset name such as `business_7`)

```bash
./test1.sh put <writer_vm> k1=business_3 k2=business_9 …      # Test 1: five puts
./test1.sh get <reader_vm> <key> <file>                        #   get + tail + diff
./test1.sh replicas <key> <liststore_vm>                       #   ls, list_mem_ids, liststore, logs

./test2.sh kill <key>                                          # Test 2: kill -9 two random replicas
./test2.sh put <writer_vm> <key> <file>                        #   put with two down
./test2.sh restart <vmA> <vmB>                                 #   bring them back (recover)
./test2.sh kill <key> --not <vmA> <vmB>                        #   crash two of the other three
./test2.sh precheck <key> <vmA> <vmB>                          #   getfromreplica + ALIVE with same ring ids
./test2.sh get <reader_vm> <key> <file>
./test2.sh gfr <key> <file> <vm> <vm> <vm>                     #   getfromreplica on the 3 live replicas + diff + logs

./test3.sh mode                                                # Test 3: all ten -> eventual-RR (R=2, W=2)
./test3.sh put <writer_vm> <key> <file>;  ./test3.sh get <reader_vm> <key> <file>
./test3.sh kill <key>                                          #   crash one replica
./test3.sh multiput <key> <vm> <vm> … (≥7 live VMs)            #   concurrent puts; each uses its business_<vm>
./test3.sh restart <vm>;  ./test3.sh gfr <key> <vm>            #   recovered replica's value + version
./test3.sh readrepair <key>                                    #   read-repair log events
./test3.sh verify <key>                                        #   five replicas: same version, same bytes, matches an input file
```

`./rehearse.sh [1|2|3]` runs the whole flow with default choices and times each test (the
demo allows about 12 minutes for Tests 1–3). Before Test 3, all ten processes must be running.

## Notes

- Each VM's `multiput` input is its own `business_<N>` (found when the node starts, so run
  `push_data` before `run_mp`); with no dataset it uses random bytes. Values are therefore distinct.
- Logs: `~/mp3_deploy/run/machine.<N>.log` (JSON lines, no value bytes). The MP1 `server.py <N>`
  works when started in that directory.
- In Eventual mode a put overtaken by a newer concurrent put at the replicas reports an error
  (by design); the newest version wins on every replica. `test3.sh multiput` prints each client's result.
- MP1/MP2 scripts are unchanged and are not used here; this folder reuses their conventions
  (hostnames 4801–4810, `kill -9` for crashes, a restart that never deletes data, `reset` only
  when stopped, parallel ssh per VM).
