import unittest

from sluice.canonical import request_hash
from sluice.money import AmountError, to_human, to_units
from sluice.states import CLAIMABLE, TERMINAL, TRANSITIONS, OpState, can_transition
from sluice.tron import address


class TestMoney(unittest.TestCase):
    def test_basic_conversion(self):
        self.assertEqual(to_units("12.5", 6), 12_500_000)
        self.assertEqual(to_units("0.000001", 6), 1)
        self.assertEqual(to_units("1000000", 6), 10**12)

    def test_normalization_is_exact(self):
        # "1.5" and "1.50" are the same instruction
        self.assertEqual(to_units("1.5", 6), to_units("1.50", 6))

    def test_floats_are_shown_the_door(self):
        with self.assertRaises(AmountError):
            to_units(12.5, 6)  # type: ignore[arg-type]

    def test_garbage(self):
        for bad in ["", "-1", "1e6", "1.", ".5", "1.0000001", "12,5", "0x10", None]:
            with self.assertRaises(AmountError, msg=bad):
                to_units(bad, 6)  # type: ignore[arg-type]

    def test_unicode_digits_are_not_money(self):
        # \d would happily match these and int() would happily parse them;
        # the regex is pinned to [0-9] for exactly this reason
        for bad in ["١٢", "๕.๕", "1٢", "１２"]:
            with self.assertRaises(AmountError, msg=bad):
                to_units(bad, 6)

    def test_roundtrip(self):
        self.assertEqual(to_human(12_500_000, 6), "12.5")
        self.assertEqual(to_human(10**12, 6), "1000000")
        self.assertEqual(to_human(1, 6), "0.000001")


class TestCanonicalHash(unittest.TestCase):
    def test_semantically_equal_payloads_collide_on_purpose(self):
        a = request_hash("Taddr", to_units("1.5", 6), "USDT-TRC20")
        b = request_hash("Taddr", to_units("1.50", 6), "USDT-TRC20")
        self.assertEqual(a, b)

    def test_any_semantic_difference_changes_hash(self):
        base = request_hash("Taddr", 1_500_000, "USDT-TRC20")
        self.assertNotEqual(base, request_hash("Taddr", 1_500_001, "USDT-TRC20"))
        self.assertNotEqual(base, request_hash("Taddr2", 1_500_000, "USDT-TRC20"))
        self.assertNotEqual(base, request_hash("Taddr", 1_500_000, "USDT"))


class TestAddress(unittest.TestCase):
    def test_real_mainnet_address_validates(self):
        # the USDT contract itself
        self.assertTrue(address.is_valid("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"))

    def test_checksum_actually_checks(self):
        self.assertFalse(address.is_valid("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6u"))

    def test_junk(self):
        for bad in ["", "T", "0x41deadbeef", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6tXX", 42]:
            self.assertFalse(address.is_valid(bad))  # type: ignore[arg-type]

    def test_minted_addresses_validate(self):
        self.assertTrue(address.is_valid(address.from_payload(b"\x01" * 20)))


class TestStateTable(unittest.TestCase):
    def test_terminal_states_have_no_exits(self):
        for s in TERMINAL:
            self.assertEqual(TRANSITIONS[s], frozenset())
            for dst in OpState:
                if dst != s:
                    self.assertFalse(can_transition(s, dst))

    def test_same_state_writes_always_allowed(self):
        for s in OpState:
            self.assertTrue(can_transition(s, s))

    def test_claimable_is_exactly_non_terminal(self):
        self.assertEqual(CLAIMABLE, frozenset(OpState) - TERMINAL)

    def test_every_live_state_can_eventually_terminate(self):
        # BFS: from any state there is a path to some terminal state
        for start in OpState:
            seen, frontier = {start}, [start]
            while frontier:
                cur = frontier.pop()
                for nxt in TRANSITIONS[cur]:
                    if nxt not in seen:
                        seen.add(nxt)
                        frontier.append(nxt)
            self.assertTrue(seen & TERMINAL, f"{start} can never finish")

    def test_the_exact_shape_of_the_machine(self):
        # A change here must be deliberate: this doubles as the parity anchor
        # for the SQL trigger in migrations/001_schema.sql.
        S = OpState
        self.assertEqual(TRANSITIONS[S.SIGNED], {S.BROADCASTING, S.SIGNING})
        self.assertEqual(TRANSITIONS[S.BROADCASTING], {S.BROADCAST, S.SIGNING, S.FAILED})
        self.assertEqual(TRANSITIONS[S.BROADCAST], {S.CONFIRMED, S.SIGNING, S.FAILED})

    def test_sql_trigger_matches_this_table_verbatim(self):
        """The transition table exists twice - here and in the migration's
        trigger - and the pg tier can only compare them when Postgres is
        around. This test parses the SQL instead, so drift gets caught on
        every plain `make test` too, no database required."""
        import re
        from pathlib import Path

        sql = (Path(__file__).parent.parent / "migrations" / "001_schema.sql").read_text()

        trigger = sql.split("CREATE FUNCTION op_transition_guard", 1)[1]
        guard = trigger.split("NOT IN (", 1)[1].split(") THEN")[0]  # the pair list
        sql_pairs = set(re.findall(r"\('(\w+)',\s*'(\w+)'\)", guard))
        py_pairs = {(a.value, b.value) for a, dsts in TRANSITIONS.items() for b in dsts}
        self.assertEqual(sql_pairs, py_pairs)

        enum_src = sql.split("CREATE TYPE op_state AS ENUM (", 1)[1].split(");")[0]
        sql_states = set(re.findall(r"'(\w+)'", enum_src))
        self.assertEqual(sql_states, {s.value for s in OpState})

        # terminal set is spelled out in three places in the SQL (claim index,
        # release guard, trigger immutability check); all must agree with us
        terminals = {s.value for s in TERMINAL}
        for m in re.findall(r"IN \('(\w+)', '(\w+)', '(\w+)'\)", sql):
            self.assertEqual(set(m), terminals)


if __name__ == "__main__":
    unittest.main()
