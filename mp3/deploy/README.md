# MP3 deployment and demo scripts

The scripts can target either ten local processes or the course VMs. Local rehearsal must
select its target explicitly; ordinary deployment commands default to `remote`, so a missed
environment setting can contact university hosts.

The remote host pattern (`fa26-cs425-48NN.cs.illinois.edu`, NN 01–10), default SSH user
(`kbalaji4`), and port allocation below are inherited from the existing MP1/MP2 deployment
scripts and this repository's MP3 manifest generator. They are not confirmed current course
instructions. Obtain the instructor's current hostnames/IPs, account, port/firewall policy,
Python version, and demo procedure before the authorized VM deployment. No actual VM has been
contacted or validated by the local checks described here.

MP3 nodes are daemons (no stdin REPL), so commands run through `./cmd_mp <vm> "<command>"`,
which runs the client on that VM. `./deploy_mp` copies the original, unchanged `mp1/server.py`
and `mp1/protocol.py` into each node's `run/` directory. `./run_mp` starts that MP1 server
from `run/` on the node's `log_query` manifest port, so it reads that node's
`machine.<N>.log`; it also starts the MP3 node. `./stop_mp` stops both services. `./kill_mp`
crashes only the MP3 node and leaves the log server running. The first MP3 process uses
`--bootstrap`; later starts recover its existing database.

**Never run these against the real VMs without the team's go-ahead.** Practice locally first:

```bash
MP3_TARGET=local MP3_LOCAL_ROOT="$HOME/mp3_local" ./deploy_mp all
MP3_TARGET=local MP3_LOCAL_ROOT="$HOME/mp3_local" ./run_mp all
MP3_TARGET=local MP3_LOCAL_ROOT="$HOME/mp3_local" ./status_mp all
```

For a completely disposable end-to-end rehearsal (creates and removes only its own unique
temporary directory), run `MP3_TARGET=local ./local_release_rehearsal.sh`. It deploys all ten
processes, checks readiness, exercises all eight commands and all three modes, kills and
recovers a node, queries logs through the original MP1 client, then stops and removes its
temporary tree. It refuses to run unless `MP3_TARGET=local` is explicitly set.

## One-time setup (real VMs)

```bash
export MP3_USER=<your netid>     # default is kbalaji4, as in MP2
./deploy_mp all                  # copies code, manifest, inventory to ~/mp3_deploy on each VM
./push_data /path/to/dataset     # business_1..business_20 onto every VM (~/mp3_deploy/data)
```

The code currently declares Python 3.10+ syntax; `check_prep` checks that floor and prints the
actual interpreter. The UIUC VM Python and SQLite versions remain unverified. `tmux` is optional;
without it the node runs under `nohup`. The generated remote manifest allocates ports
65441–65444 on each VM (KV, membership, MP1 log query, control). Confirm those ports are free
and permitted through the course VM firewall before deployment.

## Before every demo / practice run

```bash
./reset_mp all                   # fresh databases and logs (nodes must be stopped)
./run_mp all                     # all ten start in Strong mode and join membership
./check_prep                     # Python, files, both services, dataset, 10 members, no keys
```

## Everyday commands

| Script | What it does |
|---|---|
| `./run_mp <vm…\|all>` | start/recover MP3 and start the original MP1 server on the manifest log-query port |
| `./kill_mp <vm…\|all>` | `kill -9` the node process only (never the VM) |
| `./stop_mp <vm…\|all>` | graceful stop of the node and MP1 log server (SIGTERM) |
| `./cmd_mp <vm> "<command>"` | run a client command on that VM; `@data/` and `@out/` name its dataset and output dirs |
| `./status_mp [vm…]` | node and MP1 server status/pids plus configured log-query port |
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
- Logs: `~/mp3_deploy/run/machine.<N>.log` (JSON lines, no value bytes); the MP1 server is
  launched with this directory as its working directory.
- In Eventual mode a put overtaken by a newer concurrent put at the replicas reports an error
  (by design); the newest version wins on every replica. `test3.sh multiput` prints each client's result.
- MP1 and MP2 source files are unchanged; MP3 deploy copies only the original MP1 server and
  framing files. MP3 deployment scripts use repository precedents for hostnames (4801–4810),
  `kill -9` crash simulation, restart without data deletion, reset only when stopped, and
  parallel SSH. Confirm those precedents against current course instructions before any VM use.

## Locally verified versus still requiring instructor/VM confirmation

The local release rehearsal verifies script behavior on loopback, including all ten generated
identities, manifest-derived service ports, bootstrap/recovery, separate service lifecycles,
the eight commands, three modes, and MP1 queries. It does not verify DNS, SSH authentication,
host-key policy, remote Python/SQLite compatibility, firewall rules, course datasets, process
limits, clocks, or the instructor's demo procedure. The original MP1 scripts use a different
historical project layout and are not suitable as current MP3 deployment instructions without
confirmation. Do not run the destructive `reset_mp` against existing node data unless the team
explicitly intends a fresh cluster and has preserved any needed data.
