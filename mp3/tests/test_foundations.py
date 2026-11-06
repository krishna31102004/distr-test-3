import io
import json
import logging
from pathlib import Path
import tempfile
import unittest

from mp3.config import load_inventory, parse_inventory, validate_quorums
from mp3.eventlog import configure_logging
from mp3.ring import RingNode, select_successors


class RingTests(unittest.TestCase):
    def setUp(self):
        self.nodes = [RingNode(str(i), i * 10) for i in range(1, 7)]

    def test_wraparound_five_distinct(self):
        result = select_successors(reversed(self.nodes), 55, include_equal=True)
        self.assertEqual([node.node_id for node in result], ["6", "1", "2", "3", "4"])
        self.assertEqual(len(set(result)), 5)

    def test_supplied_equality_policy(self):
        for equal, expected in ((True, "2"), (False, "3")):
            self.assertEqual(select_successors(self.nodes, 20, count=1,
                                              include_equal=equal)[0].node_id, expected)

    def test_all_nodes_and_far_past_last(self):
        self.assertEqual(select_successors(self.nodes, 9999, count=6, include_equal=False),
                         tuple(self.nodes))

    def test_invalid_rings(self):
        cases = ([], [RingNode("a", 1), RingNode("a", 2)],
                 [RingNode("a", 1), RingNode("b", 1)], [RingNode("a", -1)],
                 [RingNode("", 1)], [RingNode("a", True)])
        for nodes in cases:
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                select_successors(nodes, 1, count=1, include_equal=True)

    def test_invalid_key_count_and_equality(self):
        for kwargs in ({"key_id": -1}, {"key_id": True}, {"count": 0},
                       {"count": 7}, {"count": True}, {"include_equal": 1}):
            options = {"key_id": 1, "count": 5, "include_equal": True, **kwargs}
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                select_successors(self.nodes, **options)

    def test_equality_policy_has_no_default(self):
        with self.assertRaises(TypeError):
            select_successors(self.nodes, 1)


class ConfigTests(unittest.TestCase):
    def test_deterministic_order_and_comments(self):
        a = "b localhost 2 # second\n\n# comment\na localhost 1"
        b = "a localhost 1\nb localhost 2"
        self.assertEqual(parse_inventory(a), parse_inventory(b))

    def test_invalid_inventory(self):
        for text in ("", "# empty", "a host", "a host 0", "a host 65536",
                     "a host nope", "a host 1\na other 2", "a host 1\nb host 1",
                     "a host １", "a host 1 extra"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_inventory(text)

    def test_load_local_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "machines"
            path.write_text("a ::1 42001", encoding="utf-8")
            self.assertEqual(load_inventory(path)[0].host, "::1")

    def test_independent_eventual_quorums(self):
        for reads in range(1, 6):
            for writes in range(1, 6):
                validate_quorums(reads, writes)
        for values in ((0, 2), (2, 6), (True, 1), (1.5, 2)):
            with self.assertRaises(ValueError):
                validate_quorums(*values)


class LogTests(unittest.TestCase):
    def test_json_lines_append_and_console(self):
        stream = io.StringIO()
        logger = logging.Logger("phase0.test")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "machine.1.log"
            path.write_text('{"previous": true}\n', encoding="utf-8")
            try:
                configure_logging(logger, path, stream=stream)
                logger.info("PUT_RECEIVED\nkey=λ", extra={"node_id": "vm1", "request_id": "r1"})
                logger.info("PUT_RESPONSE status=NOT_IMPLEMENTED")
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                self.assertTrue(rows[0]["previous"])
                self.assertEqual(rows[1]["request_id"], "r1")
                self.assertEqual(rows[1]["node_id"], "vm1")
                self.assertEqual(rows[1]["event"], "PUT_RECEIVED\nkey=λ")
                self.assertEqual(len(stream.getvalue().splitlines()), 2)
                self.assertEqual(json.loads(stream.getvalue().splitlines()[1]), rows[2])
                with self.assertRaises(ValueError):
                    configure_logging(logger, path, stream=stream)
            finally:
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                    handler.close()
