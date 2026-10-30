import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from mp3.cli import main, parse_command, smoke
from mp3.models import Command, Status
from mp3.pending import execute


class CliTests(unittest.TestCase):
    def test_valid_commands_and_guard(self):
        commands = ("put key input.bin", "get key output.bin", "ls key", "liststore",
                    "getfromreplica localhost key out.bin", "list_mem_ids",
                    "multiput key VM1 VM10", "multiput(key, VM1, VM10)",
                    "consistency_mode strong", "consistency_mode eventual-noRR",
                    "consistency_mode eventual-RR")
        for line in commands:
            with self.subTest(line=line):
                result = execute(parse_command(line))
                self.assertEqual(result.status, Status.NOT_IMPLEMENTED)
                self.assertIn("KD-", result.message)
                self.assertIsNone(result.value)

    def test_quotes_and_multiput_forms(self):
        self.assertEqual(parse_command('put "my key" "my file"'),
                         Command("put", ("my key", "my file")))
        self.assertEqual(parse_command("multiput(key, VM1, VM10)"),
                         parse_command("multiput key VM1 VM10"))

    def test_invalid_commands(self):
        for line in ("", "put", "put key", "get key file extra", "ls", "liststore x",
                     "list_mem_ids x", "getfromreplica host key", "bogus key",
                     "consistency_mode eventual", "multiput key", "multiput key VM0",
                     "multiput key VM11", "multiput key VM1 VM1", "multiput(key,)",
                     "multiput(key, VM1", "multiput(key, VM1) trailing", 'put "bad',
                     'put "" file', "get key \x00"):
            with self.subTest(line=line), self.assertRaises(ValueError):
                parse_command(line)

    def test_main_exit_codes_and_json(self):
        for argv, code, status in ((["put", "k", "file"], 3, "NOT_IMPLEMENTED"),
                                    (["put"], 2, "ERROR"),
                                    (["multiput(k, VM1)"], 3, "NOT_IMPLEMENTED")):
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                self.assertEqual(main(argv), code)
            self.assertEqual(json.loads(stream.getvalue())["status"], status)

    def test_pending_commands_do_not_touch_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "value.bin"
            path.write_bytes(b"keep me")
            for name in ("get", "put"):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main([name, "key", str(path)]), 3)
                self.assertEqual(path.read_bytes(), b"keep me")

    def test_help_and_smoke(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--help"]), 0)
            self.assertEqual(main(["--smoke"]), 0)

    def test_smoke(self):
        smoke()
