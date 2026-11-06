import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mp3.cli import main, parse_command, smoke, split_options
from mp3.models import Command


class CliTests(unittest.TestCase):
    def test_every_command_parses(self):
        commands = ("put key input.bin", "get key output.bin", "ls key", "liststore",
                    "getfromreplica localhost key out.bin", "list_mem_ids",
                    "multiput key VM1 VM10", "multiput(key, VM1, VM10)",
                    "consistency_mode strong", "consistency_mode eventual-noRR",
                    "consistency_mode eventual-RR")
        for line in commands:
            with self.subTest(line=line):
                self.assertIsInstance(parse_command(line), Command)

    def test_multiput_accepts_vm_names_in_any_common_form(self):
        for label in ("VM3", "vm3", "vm03", "VM03", "VM10"):
            self.assertEqual(parse_command(f"multiput key {label}").args, ("key", label))

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
                     "multiput key VM3 vm03", "multiput key vm011", "multiput key VM00",
                     "multiput(key, VM1", "multiput(key, VM1) trailing", 'put "bad',
                     'put "" file', "get key \x00"):
            with self.subTest(line=line), self.assertRaises(ValueError):
                parse_command(line)

    def test_main_exit_codes_and_json(self):
        for argv, message in ((["ls", "k"], "needs the manifest"),
                              (["put", "k", "file"], "needs a node"),
                              (["put"], "unknown command or incorrect number"),
                              (["multiput(k, VM1)"], "needs a node"),
                              (["liststore"], "needs a node"),
                              (["list_mem_ids"], "needs a node"),
                              (["getfromreplica", "vm01", "k", "f"], "needs a node"),
                              (["consistency_mode", "strong"], "needs a node")):
            stream = io.StringIO()
            with self.subTest(argv=argv), contextlib.redirect_stdout(stream), \
                    mock.patch.dict(os.environ, {}, clear=True):
                self.assertEqual(main(argv), 2)
            result = json.loads(stream.getvalue())
            self.assertEqual(result["status"], "ERROR")
            self.assertIn(message, result["message"])

    def test_put_and_get_without_a_node_fail_and_do_not_touch_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "value.bin"
            path.write_bytes(b"keep me")
            for name in ("get", "put"):
                stream = io.StringIO()
                with contextlib.redirect_stdout(stream), mock.patch.dict(
                        os.environ, {}, clear=True):
                    self.assertEqual(main([name, "key", str(path)]), 2)
                self.assertIn("needs a node", json.loads(stream.getvalue())["message"])
                self.assertEqual(path.read_bytes(), b"keep me")

    def test_option_parsing_and_bad_manifest(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(split_options(["--manifest", "m", "--vm-id", "vm01", "ls", "k"]),
                             ("m", "vm01", ["ls", "k"]))
            with self.assertRaises(ValueError):
                split_options(["--manifest"])
        with tempfile.TemporaryDirectory() as directory:
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                code = main(["--manifest", str(Path(directory) / "missing.json"),
                             "--vm-id", "vm01", "put", "k", "f"])
            self.assertEqual(code, 2)
            self.assertEqual(json.loads(stream.getvalue())["status"], "ERROR")

    def test_help_and_smoke(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--help"]), 0)
            self.assertEqual(main(["--smoke"]), 0)

    def test_smoke(self):
        smoke()
