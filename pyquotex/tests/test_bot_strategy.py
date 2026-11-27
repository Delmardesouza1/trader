"""Testes offline da estratégia e do ranking de bot_quotex.py."""

from dataclasses import replace
from typing import Dict, List

import pytest

import bot_quotex
from bot_quotex import (
    BotConfig,
    TradeMemory,
    adverse_wick_ratio,
    atr_regime,
    calculate_atr,
    evaluate_signal,
)

pytestmark = pytest.mark.unit


def _call_setup() -> List[Dict[str, float]]:
    """40 velas de queda e 6 de alta: gera um CALL que passa em todos os filtros."""
    candles: List[Dict[str, float]] = []
    price = 100.0
    for i in range(40):
        o = price
        price -= 0.1 + 0.05 * (i % 3)
        candles.append({"time": i * 300, "open": o, "close": price,
                        "high": o + 0.09, "low": price - 0.09})
    for j in range(6):
        o = price
        price += 0.5
        candles.append({"time": (40 + j) * 300, "open": o, "close": price,
                        "high": price + 0.02, "low": o - 0.05})
    return candles


def test_baseline_call_signal_passes():
    signal, reason = evaluate_signal(_call_setup(), BotConfig())
    assert reason == "ok"
    assert signal is not None
    assert signal.direction == "call"
    assert 0.0 <= signal.wick_ratio < 0.40
    assert signal.atr_ratio > 0.0


def test_large_adverse_wick_rejects_call():
    candles = _call_setup()
    last = candles[-1]
    # Pavio superior = 3x o corpo: rejeição forte contra o CALL.
    last["high"] = last["close"] + 1.5
    signal, reason = evaluate_signal(candles, BotConfig())
    assert signal is None
    assert reason.startswith("pavio contra")


def test_atr_out_of_range_rejects():
    cfg = replace(BotConfig(), atr_ratio_range=(5.0, 10.0))
    signal, reason = evaluate_signal(_call_setup(), cfg)
    assert signal is None
    assert reason.startswith("ATR baixo")

    cfg = replace(BotConfig(), atr_ratio_range=(0.0, 0.5))
    signal, reason = evaluate_signal(_call_setup(), cfg)
    assert signal is None
    assert reason.startswith("ATR alto")


def test_adverse_wick_ratio_by_direction():
    candle = {"open": 1.0, "close": 2.0, "high": 3.0, "low": 0.0}
    assert adverse_wick_ratio(candle, "call") == pytest.approx(1 / 3)
    assert adverse_wick_ratio(candle, "put") == pytest.approx(1 / 3)
    flat = {"open": 1.0, "close": 1.0, "high": 1.0, "low": 1.0}
    assert adverse_wick_ratio(flat, "call") == 0.0


def test_atr_constant_range_is_regime_one():
    candles = [{"open": 1.0, "close": 1.0, "high": 1.5, "low": 0.5} for _ in range(30)]
    assert calculate_atr(candles, 14) == pytest.approx(1.0)
    assert atr_regime(candles, 14) == pytest.approx(1.0)
    assert calculate_atr(candles[:10], 14) is None


def _record(mem: TradeMemory, asset: str, status: str, ts: int) -> None:
    mem.record_trade(ts, asset, "call", 5.0, 1.0, 1.0, status=status)


def test_asset_record_uses_recent_window(tmp_path):
    mem = TradeMemory(tmp_path / "t.db")
    for i in range(10):
        _record(mem, "EURUSD", "WIN", i)
    for i in range(10, 15):
        _record(mem, "EURUSD", "LOSS", i)
    assert mem.asset_record("EURUSD", 5) == (0, 5)
    assert mem.asset_record("EURUSD", 100) == (10, 15)
    mem.conn.close()


def test_metrics_columns_added_to_old_db(tmp_path):
    import sqlite3

    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT NOT NULL,
            candle_ts INTEGER NOT NULL, asset TEXT NOT NULL, direction TEXT NOT NULL,
            amount REAL NOT NULL, sma_fast REAL NOT NULL, sma_slow REAL NOT NULL,
            status TEXT NOT NULL, profit REAL DEFAULT 0.0, error_reason TEXT DEFAULT '',
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    mem = TradeMemory(path)
    mem.record_trade(1, "X", "put", 5.0, 1.0, 1.0, status="WIN",
                     rsi=40.0, wick_ratio=0.1, atr_ratio=1.2)
    row = mem.conn.execute("SELECT rsi, wick_ratio, atr_ratio FROM trades").fetchone()
    assert row == (40.0, 0.1, 1.2)
    mem.conn.close()


def test_merge_candles_keeps_high_low(tmp_path, monkeypatch):
    monkeypatch.setattr(bot_quotex, "DB_PATH", tmp_path / "m.db")
    bot = bot_quotex.QuotexSmartBot.__new__(bot_quotex.QuotexSmartBot)
    bot.cfg = BotConfig()
    bot.history_cache = {}
    raw = [
        {"time": 600, "open": 1.0, "close": 2.0, "high": 2.5, "low": 0.8},
        {"time": 900, "open": 2.0, "close": 1.5, "max": 2.2, "min": 1.1},
        {"time": 1200, "open": 1.5, "close": 1.7},
    ]
    bot._merge_candles("X", raw, last_closed=1200)
    cache = bot.history_cache["X"]
    assert (cache[600]["high"], cache[600]["low"]) == (2.5, 0.8)
    assert (cache[900]["high"], cache[900]["low"]) == (2.2, 1.1)
    assert (cache[1200]["high"], cache[1200]["low"]) == (1.7, 1.5)


def test_is_login_block_classifies_errors():
    assert bot_quotex.is_login_block(Exception(
        "HTTP 403: blocked by Cloudflare challenge at https://quotex.com/pt/sign-in/modal/"))
    assert bot_quotex.is_login_block(ConnectionError(
        "falha ao conectar: Login failed. E-mail ou senha inválidos"))
    assert not bot_quotex.is_login_block(ConnectionError("Websocket connection rejected."))
    assert not bot_quotex.is_login_block(TimeoutError("Timeout waiting for authorization"))


def test_single_instance_lock_blocks_second_process(tmp_path):
    import subprocess
    import sys

    lock = tmp_path / "bot.lock"
    assert bot_quotex.acquire_single_instance_lock(lock)
    code = (
        "import sys, bot_quotex; from pathlib import Path; "
        f"sys.exit(0 if bot_quotex.acquire_single_instance_lock(Path(r'{lock}')) else 3)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True)
    assert result.returncode == 3


def test_session_file_anchored_to_script_dir():
    import pyquotex.config as qx_config

    assert qx_config.resource_path("session.json") == bot_quotex.BASE_DIR / "session.json"


def test_import_does_not_attach_log_handlers():
    assert not bot_quotex.logger.handlers


async def test_start_closes_client_when_connect_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(bot_quotex, "DB_PATH", tmp_path / "s.db")
    bot = bot_quotex.QuotexSmartBot.__new__(bot_quotex.QuotexSmartBot)
    closed = []

    class FakeClient:
        async def connect(self):
            raise RuntimeError("HTTP 403: blocked by Cloudflare challenge")

        async def close(self):
            closed.append(True)
            return True

    bot.client = FakeClient()
    with pytest.raises(RuntimeError):
        await bot.start()
    assert closed == [True]
