# MP1

## Setup

1. Clone the repo on every VM:

```bash
git clone https://github.com/illinois-cs-coursework/fa26_cs425_.group-mp_.team-48.git
cd fa26_cs425_.group-mp_.team-48/mp1
```

2. Place each VM's log file, `machine.<vm_number>.log`, in this folder.

3. Edit `machines.txt` with each VM's hostname and port, one per line.

4. Make the scripts executable:

```bash
chmod +x run_mp kill_mp deploy_mp
```

## Dependencies

- Python 3
- `pytest` (`pip install pytest --break-system-packages`)
- `grep`
- `matplotlib`

## Running the MP

Start a server on one VM:

```bash
./run_mp <vm_number>
```

Start servers on all VMs:

```bash
./deploy_mp
```

Stop a server:

```bash
./kill_mp <vm_number>
```

Run a query from any VM:

```bash
python3 client.py
```

## Running the tests

```bash
pytest test/test_suite.py -v
```

## Performance Experiment

Generate a performance log on each VM:

```bash
python3 generate_perf_logs.py <vm_number>
```

Start the server on each VM:

```bash
python3 server.py <vm_number>
```

Create `perf_machines.txt` with the four VM hostnames and ports.

Run the experiment:

```bash
python3 run_timing_experiment.py perf_machines.txt 5
```

Generate the performance graph:

```bash
python3 analyze_and_plot.py
```
