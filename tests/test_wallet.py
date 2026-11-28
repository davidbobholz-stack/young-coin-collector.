#!/usr/bin/env python3
"""Wallet transaction parsing. No network and no wallet list."""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import wallet_log

W1 = "Wallet111111111111111111111111111111111111111"
W2 = "Wallet222222222222222222222222222222222222222"
MINT = "Mint1111111111111111111111111111111111111111"


def _tx(pre_tok, post_tok, pre_sol, post_sol, transfer=None):
    keys = [W1, W2]
    instr = []
    if transfer:
        instr.append({"program": "system", "parsed": {"type": "transfer", "info": {
            "source": transfer[0], "destination": transfer[1], "lamports": transfer[2]}}})
    return {
        "slot": 99,
        "blockTime": 1700000000,
        "transaction": {"signatures": ["sig1"], "message": {"accountKeys": keys, "instructions": instr}},
        "meta": {
            "err": None,
            "preBalances": [pre_sol, 0],
            "postBalances": [post_sol, 0],
            "preTokenBalances": pre_tok,
            "postTokenBalances": post_tok,
            "innerInstructions": [],
        },
    }


def _bal(amount, owner=W1):
    return [{"owner": owner, "mint": MINT, "uiTokenAmount": {"uiAmount": amount, "amount": str(int(amount * 1e6)), "decimals": 6}}]


class ParseTests(unittest.TestCase):
    def test_buy_sell_and_slot(self):
        buy = wallet_log.parse_transaction(_tx(_bal(1), _bal(3), 2_000_000_000, 1_000_000_000), {W1, W2})
        kinds = [e["kind"] for e in buy]
        self.assertIn("buy", kinds)
        b = next(e for e in buy if e["kind"] == "buy")
        self.assertEqual(b["token_amount"], 2)
        self.assertEqual(b["slot"], 99)
        self.assertAlmostEqual(b["sol_amount"], 1.0)
        sell = wallet_log.parse_transaction(_tx(_bal(3), _bal(1), 1_000_000_000, 1_500_000_000), {W1, W2})
        s = next(e for e in sell if e["kind"] == "sell")
        self.assertEqual(s["token_amount"], 2)

    def test_sol_transfer_between_tracked_only(self):
        tx = _tx([], [], 5_000_000_000, 4_000_000_000, transfer=(W1, W2, 1_000_000_000))
        ev = wallet_log.parse_transaction(tx, {W1, W2})
        x = [e for e in ev if e["kind"] == "sol_transfer"]
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0]["sender"], W1)
        self.assertEqual(x[0]["receiver"], W2)
        self.assertAlmostEqual(x[0]["sol_amount"], 1.0)
        self.assertEqual(x[0]["slot"], 99)
        outsider = wallet_log.parse_transaction(
            _tx([], [], 5_000_000_000, 4_000_000_000, transfer=(W1, "SomeoneElse", 1_000_000_000)), {W1, W2})
        self.assertFalse([e for e in outsider if e["kind"] == "sol_transfer"])

    def test_unset_wallet_list_skips(self):
        os.environ.pop("TRACKED_WALLETS", None)
        wallet_log._skipped_logged[0] = False
        self.assertEqual(wallet_log.load_wallets("/tmp/does-not-exist-tracked-wallets.json"), [])

    def test_file_fallback_when_secret_unset(self):
        import json, tempfile
        os.environ.pop("TRACKED_WALLETS", None)
        wallet_log._loaded_logged[0] = False
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(["AddrOne", "AddrTwo"], fh)
            path = fh.name
        try:
            self.assertEqual(wallet_log.load_wallets(path), ["AddrOne", "AddrTwo"])
        finally:
            os.remove(path)

    def test_secret_overrides_file(self):
        os.environ["TRACKED_WALLETS"] = '["FromSecret"]'
        wallet_log._loaded_logged[0] = False
        try:
            self.assertEqual(wallet_log.load_wallets("/tmp/does-not-exist-tracked-wallets.json"), ["FromSecret"])
        finally:
            os.environ.pop("TRACKED_WALLETS", None)


if __name__ == "__main__":
    unittest.main()
