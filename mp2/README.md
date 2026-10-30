# MP2

## Setup

1. Clone the repo:

```bash
git clone https://github.com/illinois-cs-coursework/fa26_cs425_.group-mp_.team-48.git
cd fa26_cs425_.group-mp_.team-48/mp2
```

2. Make sure `machines.txt` contains the 10 VM hostnames.

3. Make the scripts executable:

```bash
chmod +x run_mp kill_mp deploy_mp cmd_mp show_mp reset_logs
```

## Dependencies

- Python 3
- `matplotlib`

## Running the MP

Before a fresh demo or practice run, clear old logs:

```bash
./reset_logs all
```

Start the introducer:

```bash
./deploy_mp 1
```

Start multiple VMs:

```bash
./deploy_mp 2 3 4 5
```

Start all VMs:

```bash
./deploy_mp all
```

Start or rejoin one VM:

```bash
./run_mp <vm_number>
```

Force-kill one VM:

```bash
./kill_mp <vm_number>
```

Send a command to a running VM:

```bash
./cmd_mp <vm_number> "<command>"
```

Examples:

```bash
./cmd_mp 1 list_mem
./cmd_mp 1 display_protocol
./cmd_mp 1 "switch suspect"
./cmd_mp 1 "set_drop_rate 10"
```

View a VM's output:

```bash
./show_mp <vm_number>
```

## Running the tests

Run the local self-test:

```bash
python3 selftest.py
```

Check the Python files:

```bash
python3 -m py_compile node.py membership.py protocol.py
```

## Experiments

Set the NetID and source path:

```bash
export MP2_NETID=<your net_id>
export MP2_SRC=$(pwd)
```

Push the harness code to all 10 VMs before running anything:

```bash
python3 experiments.py push
```

Run the three experiments (`--trials 5` shown as an example):

```bash
python3 experiments.py e1 --trials 5
python3 experiments.py e2 --trials 5 --rates 20,24,27,30,33
python3 experiments.py e3 --trials 5
```

Generate the plots:

```bash
python3 plot.py
```

Running Experiment (false positive rate vs. drop rate)

```bash
python3 experiments.py e2 --trials 1 --rates 0 --window 60
```

If something goes wrong in the middle of the run, stop everything with:

```bash
python3 experiments.py stop
```
