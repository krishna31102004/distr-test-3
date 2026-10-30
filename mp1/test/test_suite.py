import pytest
import subprocess
import random

from client import fan_out_query

USER = "kbalaji4"
REPO_DIR = "fa26_cs425_.group-mp_.team-48/mp1"
PORT = 65432
CONFIG_FILE = "machines.txt"

TEST_VMS = []
for vm_number in range(1, 11):
    host_suffix = 4800 + vm_number
    hostname = f"fa26-cs425-{host_suffix}.cs.illinois.edu"
    TEST_VMS.append((hostname, vm_number))


# runs a command on one of the VMs through ssh
def ssh_run(host, command):
    subprocess.run(["ssh", f"{USER}@{host}", command], check=True)


# writes some content directly into a file on a VM
def ssh_write_file(host, remote_path, content):
    subprocess.run(
        ["ssh", f"{USER}@{host}", f"cat > {remote_path}"],
        input=content.encode(),
        check=True
    )


# creates log contents with known matches mixed in with normal lines
def generate_log_content(total_lines, patterns):
    lines = []

    for pattern, count in patterns.items():
        for i in range(count):
            lines.append(f"{i} {pattern}: something went wrong")

    matching_lines = len(lines)

    for i in range(total_lines - matching_lines):
        lines.append(f"{i} normal log entry, nothing special here")

    random.shuffle(lines)

    return "\n".join(lines) + "\n"

    # Add known matching lines
    for pattern, count in patterns.items():
        for i in range(count):
            lines.append(
                f"{i} {pattern}: something went wrong"
            )

    # Add normal random lines
    matching_lines = len(lines)

    for i in range(total_lines - matching_lines):
        lines.append(
            f"{i} normal log entry, nothing special here"
        )

    random.shuffle(lines)

    with open(filename, "w") as f:
        f.write("\n".join(lines) + "\n")


# ---------------------------------------------------------
# Point machines.txt at the local test servers
# ---------------------------------------------------------

@pytest.fixture(autouse=True, scope="module")
def machines_file():
    """
    Back up machines.txt, point it at the local test servers
    for the duration of this file, then restore it.
    """

    with open(CONFIG_FILE) as f:
        original = f.read()

    with open(CONFIG_FILE, "w") as f:
        for host, port in MACHINES:
            f.write(f"{host}:{port}\n")

    yield

    with open(CONFIG_FILE, "w") as f:
        f.write(original)


# ---------------------------------------------------------
# Start servers
# ---------------------------------------------------------

# creates the test log for a specific VM
def write_remote_log(host, vm_number, total_lines, patterns):
    content = generate_log_content(total_lines, patterns)
    remote_path = f"~/{REPO_DIR}/machine.{vm_number}.log"
    ssh_write_file(host, remote_path, content)


# temporarily changes machines.txt to use the actual 10 VMs
@pytest.fixture(scope="module", autouse=True)
def machines_file():
    with open(CONFIG_FILE) as f:
        original = f.read()

    with open(CONFIG_FILE, "w") as f:
        for host, vm_number in TEST_VMS:
            f.write(f"{host}:{PORT}\n")

    yield

    # restore the original machines.txt after all the tests finish
    with open(CONFIG_FILE, "w") as f:
        f.write(original)


# back up the current logs before replacing them with test logs
@pytest.fixture(scope="module", autouse=True)
def backup_real_logs():
    for host, vm_number in TEST_VMS:
        ssh_run(
            host,
            f"cp ~/{REPO_DIR}/machine.{vm_number}.log "
            f"~/{REPO_DIR}/machine.{vm_number}.log.bak"
        )

    yield

    # put the original logs back once the tests are done
    for host, vm_number in TEST_VMS:
        ssh_run(
            host,
            f"mv ~/{REPO_DIR}/machine.{vm_number}.log.bak "
            f"~/{REPO_DIR}/machine.{vm_number}.log"
        )


# checks that every machine returned the count we expected
def check_results(results, expected):
    assert len(results) == len(expected)

    for i, (result, expected_count) in enumerate(
        zip(results, expected),
        start=1
    ):
        assert result["success"] is True
        assert result["count"] == expected_count
        assert result["filename"] == f"machine.{i}.log"

#------ RARE PATTERN ---------
# only machine 1 has the rare pattern
def test_rare_pattern_one_machine():
    write_remote_log(TEST_VMS[0][0], 1, 1000, {"rare": 2})

    for host, vm_number in TEST_VMS[1:]:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("rare")
    expected = [2] + [0] * 9

    check_results(results, expected)


# the rare pattern is on machines 1 through 3
def test_rare_pattern_some_machines():
    for host, vm_number in TEST_VMS[:3]:
        write_remote_log(host, vm_number, 1000, {"rare": 2})

    for host, vm_number in TEST_VMS[3:]:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("rare")
    expected = [2, 2, 2] + [0] * 7

    check_results(results, expected)


# every machine has the rare pattern
def test_rare_pattern_all_machines():
    for host, vm_number in TEST_VMS:
        write_remote_log(host, vm_number, 1000, {"rare": 2})

    results = fan_out_query("rare")
    expected = [2] * 10

    check_results(results, expected)

#------ INFREQUENT PATTERN ---------
# only machine 1 has the medium pattern
def test_medium_pattern_one_machine():
    write_remote_log(TEST_VMS[0][0], 1, 1000, {"medium": 50})

    for host, vm_number in TEST_VMS[1:]:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("medium")
    expected = [50] + [0] * 9

    check_results(results, expected)


# the medium pattern is on machines 1 through 3
def test_medium_pattern_some_machines():
    for host, vm_number in TEST_VMS[:3]:
        write_remote_log(host, vm_number, 1000, {"medium": 50})

    for host, vm_number in TEST_VMS[3:]:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("medium")
    expected = [50, 50, 50] + [0] * 7

    check_results(results, expected)


# every machine has the medium pattern
def test_medium_pattern_all_machines():
    for host, vm_number in TEST_VMS:
        write_remote_log(host, vm_number, 1000, {"medium": 50})

    results = fan_out_query("medium")
    expected = [50] * 10

    check_results(results, expected)

#------ FREQUENT PATTERN ---------
# only machine 1 has the frequent pattern
def test_frequent_pattern_one_machine():
    write_remote_log(TEST_VMS[0][0], 1, 1000, {"frequent": 500})

    for host, vm_number in TEST_VMS[1:]:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("frequent")
    expected = [500] + [0] * 9

    check_results(results, expected)


# the frequent pattern is on machines 1 through 3
def test_frequent_pattern_some_machines():
    for host, vm_number in TEST_VMS[:3]:
        write_remote_log(host, vm_number, 1000, {"frequent": 500})

    for host, vm_number in TEST_VMS[3:]:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("frequent")
    expected = [500, 500, 500] + [0] * 7

    check_results(results, expected)


# every machine has the frequent pattern
def test_frequent_pattern_all_machines():
    for host, vm_number in TEST_VMS:
        write_remote_log(host, vm_number, 1000, {"frequent": 500})

    results = fan_out_query("frequent")
    expected = [500] * 10

    check_results(results, expected)

#------ NO MATCHES ---------
# none of the machines should find this pattern
def test_no_matches():
    for host, vm_number in TEST_VMS:
        write_remote_log(host, vm_number, 1000, {})

    results = fan_out_query("does_not_exist")
    expected = [0] * 10

    check_results(results, expected)

#------ REGEX PATTERN ---------
# checks that grep regex options like -E work across the real VMs
def test_regex():
    for host, vm_number in TEST_VMS:
        write_remote_log(host, vm_number, 1000, {"error": 4})

    results = fan_out_query("-E err.*wrong")
    expected = [4] * 10

    check_results(results, expected)

#------ CLIENT FROM DIFFERENT VMS ---------
# runs client.py from a few different VMs to make sure queries work from anywhere
def test_query_from_different_vms():
    marker = "queryfromanyvm"

    for host, vm_number in TEST_VMS:
        write_remote_log(host, vm_number, 1000, {marker: 3})

    querying_vms = [TEST_VMS[0], TEST_VMS[4], TEST_VMS[9]]

    for querying_host, querying_vm_number in querying_vms:
        result = subprocess.run(
            [
                "ssh",
                f"{USER}@{querying_host}",
                f"cd ~/{REPO_DIR} && echo '{marker}' | python3 client.py"
            ],
            capture_output=True,
            text=True,
            check=True
        )

        output = result.stdout

        assert "10 machines responded, 0 failed" in output
        assert "Total matches across all machines: 30" in output
