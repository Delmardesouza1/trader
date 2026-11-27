"""
Bot Quotex - cruzamento de EMA 9/21 com filtro anti-lateralização,
gestão de risco e seleção dinâmica de ativos.

Seleção de ativos:
1. Candidatos: ativos com payout >= min_payout (no timeframe configurado),
   até pool_size, ordenados pelo payout.
2. Ranking: taxa de acerto medida menos o breakeven do payout, só para ativos com
   min_sample operações e resultado acima do breakeven.
3. Coleta: se faltarem ativos ranqueados, a lista é completada com os ativos menos testados.
4. A lista é reavaliada a cada reselect_seconds.

Configuração obrigatória por variáveis de ambiente:
    QUOTEX_EMAIL
    QUOTEX_PASSWORD

Use somente conta DEMO. Execute:
    python bot_quotex.py
"""

import asyncio
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import pyquotex.config as qx_config
from pyquotex.global_value import AuthStatus, WebsocketStatus
from pyquotex.stable_api import Quotex
from pyquotex.utils.account_type import AccountType

# ---------------------------------------------------------------------------
# Caminhos (ancorados ao diretório do script, não ao diretório de execução)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
DB_PATH = LOG_DIR / "operacoes.db"
LOCK_PATH = LOG_DIR / "bot.lock"

# A pyquotex grava session.json no diretório de execução (cwd). Ancorar na pasta do
# script evita um login novo só porque o bot foi iniciado de outra pasta.
qx_config.base_dir = BASE_DIR

logger = logging.getLogger("QuotexBot")


def setup_logging() -> None:
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    logger.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler = RotatingFileHandler(
        LOG_DIR / "execucao.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    # Console do Windows (cp1252) embaralha acentos; força UTF-8 quando possível.
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure is not None:
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    # Avisos da biblioteca (login, websocket, sessão) também vão para o arquivo,
    # para diagnosticar por que uma sessão foi rejeitada.
    for name in ("pyquotex", "Browser"):
        lib_logger = logging.getLogger(name)
        lib_logger.setLevel(logging.WARNING)
        lib_logger.addHandler(file_handler)



# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class BotConfig:
    timeframe: int = 300                 # 300 = M5 (5 min) | 60 = M1 (1 min)
    fast_ema: int = 9
    slow_ema: int = 21
    rsi_period: int = 14
    min_candles_history: int = 45        # histórico mínimo para estabilizar EMA21 e RSI14
    # Filtros anti-lateralização e exaustão
    min_body_ratio: float = 0.50         # corpo da vela de sinal >= 50% da média das últimas 5 velas
    rsi_call_range: Tuple[float, float] = (50.0, 75.0)
    rsi_put_range: Tuple[float, float] = (25.0, 50.0)
    # Cruzamento vale por até N velas fechadas (1 = só a última). Com 2, um cruzamento
    # cuja vela falhou em algum filtro ainda pode ser confirmado pela vela seguinte.
    cross_lookback: int = 2
    # Pavio contra a direção da vela de sinal (CALL: pavio superior; PUT: inferior),
    # como fração do range (máxima - mínima). Pavio grande = rejeição contra a entrada.
    max_wick_ratio: float = 0.40
    # Regime de volatilidade: ATR(atr_period) dividido pela média do true range de
    # toda a série. Abaixo do mínimo = mercado morto; acima do máximo = pico/notícia.
    atr_period: int = 14
    atr_ratio_range: Tuple[float, float] = (0.60, 2.00)
    # Supervisor: espera entre tentativas quando a conexão cai de vez.
    restart_min_wait: int = 30
    restart_max_wait: int = 300
    # Login bloqueado (Cloudflare 403, "Login failed"): cada tentativa nova piora o
    # bloqueio do IP. Espera longa, dobrando até o máximo.
    login_block_min_wait: int = 900
    login_block_max_wait: int = 3600
    # Gestão de risco
    risk_per_trade_pct: float = 0.02     # 2% do saldo por entrada
    max_daily_loss_pct: float = 0.03     # stop loss diário: 3% do saldo inicial do dia
    daily_target_pct: float = 0.03       # stop win diário: 3% do saldo inicial do dia
    max_consecutive_losses: int = 2      # sequência que ativa o cooldown (por ativo)
    cooldown_seconds: int = 900          # 15 min de pausa
    min_amount: float = 5.0              # stake mínima aceita pela conta
    # Temporização e rede
    max_entry_delay: int = 20            # s após a abertura da vela para ainda operar
    candle_fetch_attempts: int = 3       # tentativas rápidas para evitar estourar delay
    candle_fetch_interval: float = 0.5   # s entre tentativas
    expiry_extra: int = 3                # s de folga após a expiração antes de ler saldo
    balance_stable_reads: int = 2        # leituras iguais seguidas para considerar saldo estável
    balance_max_reads: int = 10
    # Seleção de ativos
    min_payout: float = 0.80             # payout mínimo para o ativo ser candidato
    pool_size: int = 10                  # candidatos avaliados, pelos de maior payout
    top_n: int = 5                       # ativos na lista de operação
    min_sample: int = 15                 # operações mínimas por ativo antes de ranquear
    rank_window: int = 30                # ranking usa só as últimas N operações WIN/LOSS do ativo
    reselect_seconds: int = 1800         # reavaliação da lista de ativos (30 min)
    scan_concurrency: int = 3            # ativos buscados em paralelo por vela
    reconnect_wait: int = 45             # s aguardando a reconexão automática antes de recriar a conexão
    result_extra_wait: int = 60          # s além da expiração aguardando o resultado da ordem


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------
class TradeMemory:
    """Banco SQLite local para auditoria e controles de risco."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_sessions (
                    trade_date TEXT PRIMARY KEY,
                    initial_balance REAL NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT NOT NULL,
                    candle_ts INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    amount REAL NOT NULL,
                    sma_fast REAL NOT NULL,
                    sma_slow REAL NOT NULL,
                    status TEXT NOT NULL,
                    profit REAL DEFAULT 0.0,
                    error_reason TEXT DEFAULT '',
                    created_at INTEGER NOT NULL
                )
                """
            )
            # Métricas da vela de sinal, para medir depois quais filtros ajudam.
            # Bancos antigos recebem as colunas por ALTER TABLE.
            existing = {row[1] for row in self.conn.execute("PRAGMA table_info(trades)")}
            for column in ("rsi", "wick_ratio", "atr_ratio"):
                if column not in existing:
                    self.conn.execute(f"ALTER TABLE trades ADD COLUMN {column} REAL")
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_asset_status ON trades(asset, status)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_date_asset ON trades(trade_date, asset)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_asset_candle ON trades(asset, candle_ts)"
            )

    def get_or_set_daily_balance(self, current_balance: float) -> float:
        """Mantém o saldo inicial do dia mesmo se o script reiniciar."""
        today = datetime.now().strftime("%Y-%m-%d")
        row = self.conn.execute(
            "SELECT initial_balance FROM daily_sessions WHERE trade_date = ?", (today,)
        ).fetchone()
        if row is not None:
            return float(row[0])
        with self.conn:
            self.conn.execute(
                "INSERT INTO daily_sessions (trade_date, initial_balance) VALUES (?, ?)",
                (today, current_balance),
            )
        return current_balance

    def already_traded_candle(self, asset: str, candle_ts: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM trades WHERE asset = ? AND candle_ts = ? AND status != 'REJECTED'",
            (asset, candle_ts),
        ).fetchone()
        return row is not None

    def asset_record(self, asset: str, window: int) -> Tuple[int, int]:
        """Retorna (vitórias, total) das últimas `window` operações WIN/LOSS do ativo."""
        row = self.conn.execute(
            """
            SELECT COALESCE(SUM(status = 'WIN'), 0), COUNT(*)
            FROM (
                SELECT status FROM trades
                WHERE asset = ? AND status IN ('WIN', 'LOSS')
                ORDER BY id DESC LIMIT ?
            )
            """,
            (asset, window),
        ).fetchone()
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])

    def is_in_cooldown(
        self, asset: str, max_losses: int, cooldown_sec: int, timeframe: int
    ) -> Tuple[bool, int]:
        """Pausa o ativo após N losses seguidos. DRAW interrompe a sequência."""
        today = datetime.now().strftime("%Y-%m-%d")
        rows = self.conn.execute(
            """
            SELECT status, candle_ts FROM trades
            WHERE trade_date = ? AND asset = ? AND status IN ('WIN', 'LOSS', 'DRAW')
            ORDER BY id DESC LIMIT ?
            """,
            (today, asset, max_losses),
        ).fetchall()
        if len(rows) < max_losses:
            return False, 0
        if all(status == "LOSS" for status, _ in rows):
            result_time = int(rows[0][1]) + timeframe
            elapsed = int(time.time()) - result_time
            if elapsed < cooldown_sec:
                return True, cooldown_sec - elapsed
        return False, 0

    def record_trade(
        self,
        candle_ts: int,
        asset: str,
        direction: str,
        amount: float,
        ema_fast: float,
        ema_slow: float,
        status: str,
        profit: float = 0.0,
        error_reason: str = "",
        rsi: Optional[float] = None,
        wick_ratio: Optional[float] = None,
        atr_ratio: Optional[float] = None,
    ) -> int:
        today = datetime.now().strftime("%Y-%m-%d")
        with self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO trades (
                    trade_date, candle_ts, asset, direction, amount,
                    sma_fast, sma_slow, status, profit, error_reason, created_at,
                    rsi, wick_ratio, atr_ratio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    today, candle_ts, asset, direction, amount,
                    ema_fast, ema_slow, status, profit, error_reason, int(time.time()),
                    rsi, wick_ratio, atr_ratio,
                ),
            )
        return int(cur.lastrowid or 0)

    def update_trade_result(self, trade_id: int, status: str, profit: float) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE trades SET status = ?, profit = ? WHERE id = ?",
                (status, profit, trade_id),
            )

    def reconcile_open_trades(self) -> int:
        """Operações EXECUTED sem resultado (queda do bot) viram UNRESOLVED."""
        with self.conn:
            cur = self.conn.execute(
                """
                UPDATE trades
                SET status = 'UNRESOLVED',
                    error_reason = 'encerrada sem resultado registrado (queda do bot)'
                WHERE status = 'EXECUTED'
                """
            )
        return cur.rowcount


# ---------------------------------------------------------------------------
# Seleção de ativos (funções puras)
# ---------------------------------------------------------------------------
def normalize_payout(raw: Any) -> Optional[float]:
    """Converte o payout para fração (0.85). Aceita 85 ou 0.85."""
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value > 1.0:
        value /= 100.0
    return value if value > 0.0 else None


def breakeven(payout: float) -> float:
    """Taxa de acerto mínima para não perder dinheiro com o payout informado."""
    return 1.0 / (1.0 + payout)


def rank_assets(
    payouts: Dict[str, float],
    stats: Dict[str, Tuple[int, int]],
    top_n: int,
    min_sample: int,
) -> List[str]:
    qualified: List[Tuple[float, str]] = []
    collecting: List[Tuple[int, str]] = []
    for asset, payout in payouts.items():
        wins, total = stats.get(asset, (0, 0))
        if total >= min_sample:
            edge = wins / total - breakeven(payout)
            if edge > 0.0:
                qualified.append((edge, asset))
        else:
            collecting.append((total, asset))

    qualified.sort(reverse=True)
    chosen = [asset for _, asset in qualified[:top_n]]
    if len(chosen) < top_n:
        collecting.sort()
        chosen += [asset for _, asset in collecting[: top_n - len(chosen)]]
    return chosen


# ---------------------------------------------------------------------------
# Indicadores e Sinal (EMA 9/21 + Filtro Anti-Lateralização + RSI)
# ---------------------------------------------------------------------------
def ema_series(values: List[float], period: int) -> List[float]:
    """Calcula a série completa da Média Móvel Exponencial."""
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema_vals: List[float] = [sum(values[:period]) / period]
    for price in values[period:]:
        ema_vals.append(price * k + ema_vals[-1] * (1.0 - k))
    return ema_vals


def calculate_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Calcula o RSI (Wilder) do último fechamento."""
    if len(closes) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def true_ranges(candles: List[Dict[str, float]]) -> List[float]:
    """True range de cada vela a partir da segunda (usa o fechamento anterior)."""
    trs: List[float] = []
    for prev, c in zip(candles, candles[1:]):
        prev_close = prev["close"]
        trs.append(
            max(c["high"] - c["low"], abs(c["high"] - prev_close), abs(c["low"] - prev_close))
        )
    return trs


def calculate_atr(candles: List[Dict[str, float]], period: int = 14) -> Optional[float]:
    """ATR de Wilder da última vela."""
    trs = true_ranges(candles)
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def atr_regime(candles: List[Dict[str, float]], period: int = 14) -> Optional[float]:
    """ATR atual dividido pela média do true range da série. 1.0 = volatilidade normal."""
    atr = calculate_atr(candles, period)
    trs = true_ranges(candles)
    if atr is None or not trs:
        return None
    mean_tr = sum(trs) / len(trs)
    if mean_tr <= 0.0:
        return None
    return atr / mean_tr


def adverse_wick_ratio(candle: Dict[str, float], direction: str) -> float:
    """Pavio contra a direção como fração do range da vela (0 = sem rejeição)."""
    rng = candle["high"] - candle["low"]
    if rng <= 0.0:
        return 0.0
    if direction == "call":
        wick = candle["high"] - max(candle["open"], candle["close"])
    else:
        wick = min(candle["open"], candle["close"]) - candle["low"]
    return max(wick, 0.0) / rng


class Signal(NamedTuple):
    direction: str
    ema_fast: float
    ema_slow: float
    rsi: float
    cross_ts: int
    wick_ratio: float
    atr_ratio: float


def evaluate_signal(
    candles: List[Dict[str, float]], cfg: BotConfig
) -> Tuple[Optional[Signal], str]:
    """
    Avalia cruzamento EMA 9 x EMA 21 com filtros de qualidade:
    1. Cruzamento ocorrido nas últimas cross_lookback velas e ainda válido.
    2. Distância entre as EMAs aumentando (cruzamento ganhando força, não morrendo).
    3. EMA 21 inclinada a favor da operação (evita mercado lateral).
    4. Vela de sinal fechando na direção, além da EMA 9, com corpo relevante.
    5. Pavio contra a direção pequeno (sem rejeição forte na vela de sinal).
    6. Volatilidade (ATR) dentro da faixa normal do ativo.
    7. RSI dentro da faixa de impulso sem exaustão.
    Retorna (sinal, motivo). Sinal é None quando algum filtro reprova; motivo diz qual.
    """
    if len(candles) < cfg.min_candles_history:
        return None, "histórico curto"

    closes = [c["close"] for c in candles]
    fast_list = ema_series(closes, cfg.fast_ema)
    slow_list = ema_series(closes, cfg.slow_ema)

    lookback = max(1, cfg.cross_lookback)
    if len(fast_list) < lookback + 1 or len(slow_list) < lookback + 1:
        return None, "histórico curto"

    curr_fast = fast_list[-1]
    curr_slow, prev_slow = slow_list[-1], slow_list[-2]
    diff = [f - s for f, s in zip(fast_list[-(lookback + 1):], slow_list[-(lookback + 1):])]

    # Direção atual e vela do cruzamento mais recente dentro da janela.
    direction = "call" if diff[-1] > 0 else "put" if diff[-1] < 0 else ""
    cross_idx: Optional[int] = None
    if direction:
        for k in range(1, lookback + 1):
            before, after = diff[-k - 1], diff[-k]
            if (direction == "call" and before <= 0 < after) or (
                direction == "put" and before >= 0 > after
            ):
                cross_idx = len(candles) - k
                break
    if cross_idx is None:
        return None, "sem cruzamento"
    # Cruzamento mais recente na direção atual: a EMA 9 seguiu do mesmo lado desde então.
    sign = 1 if direction == "call" else -1
    cross_ts = int(candles[cross_idx]["time"])

    if sign * (diff[-1] - diff[-2]) <= 0:
        return None, "EMAs convergindo"

    if sign * (curr_slow - prev_slow) <= 0:
        return None, "EMA21 contra"

    last_candle = candles[-1]
    last_open = last_candle["open"]
    last_close = last_candle["close"]
    last_body = abs(last_close - last_open)
    if sign * (last_close - last_open) <= 0 or sign * (last_close - curr_fast) <= 0:
        return None, "vela contra"

    # Média do corpo das últimas 5 velas para descartar dojis/mercado morto
    recent_bodies = [abs(c["close"] - c["open"]) for c in candles[-6:-1]]
    avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else 0.0
    if avg_body <= 0.0 or last_body < (avg_body * cfg.min_body_ratio):
        return None, "corpo pequeno"

    wick = adverse_wick_ratio(last_candle, direction)
    if wick > cfg.max_wick_ratio:
        return None, f"pavio contra {wick:.0%}"

    regime = atr_regime(candles, cfg.atr_period)
    if regime is None:
        return None, "histórico curto"
    atr_lo, atr_hi = cfg.atr_ratio_range
    if regime < atr_lo:
        return None, f"ATR baixo {regime:.2f}"
    if regime > atr_hi:
        return None, f"ATR alto {regime:.2f}"

    rsi = calculate_rsi(closes, cfg.rsi_period)
    if rsi is None:
        return None, "histórico curto"
    lo, hi = cfg.rsi_call_range if direction == "call" else cfg.rsi_put_range
    if not lo <= rsi <= hi:
        return None, f"RSI {rsi:.0f}"

    return Signal(direction, curr_fast, curr_slow, rsi, cross_ts, wick, regime), "ok"


def crossover_signal(candles: List[Dict[str, float]], cfg: BotConfig) -> Optional[Signal]:
    return evaluate_signal(candles, cfg)[0]


def _align_candle_ts(raw_ts: Any, timeframe: int) -> int:
    """Normaliza timestamp (s ou ms) para o início exato da vela no timeframe."""
    ts = int(float(raw_ts))
    if ts > 10_000_000_000:
        ts //= 1000
    return ts - (ts % timeframe)


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------
class QuotexSmartBot:
    def __init__(self, email: str, password: str, config: BotConfig) -> None:
        self.client = Quotex(
            email=email,
            password=password,
            lang="pt",
            host="quotex.com",
            # session.json ancorado ao diretório do script, não ao diretório de execução.
            root_path=str(BASE_DIR),
            user_data_dir=str(BASE_DIR),
        )
        self.cfg = config
        self.memory = TradeMemory()
        self.initial_balance: float = 0.0
        self.session_date: str = ""
        self.paused_date: Optional[str] = None   # dia em que stop win/loss foi atingido
        self._backoff = 5
        self.active: List[str] = []
        self.payouts: Dict[str, float] = {}
        self.last_refresh: float = 0.0
        self.history_cache: Dict[str, Dict[int, Dict[str, float]]] = {}
        # Último cruzamento já operado por ativo: com cross_lookback > 1 o mesmo
        # cruzamento segue visível na vela seguinte e não pode gerar 2ª entrada.
        self.traded_cross: Dict[str, int] = {}

    # -- conexão -----------------------------------------------------------
    async def _set_demo(self) -> None:
        await self.client.change_account(str(AccountType.DEMO.value))

    def _healthy(self) -> bool:
        api = self.client.api
        return (
            api is not None
            and api.state.status == WebsocketStatus.CONNECTED
            and api.state.auth_status == AuthStatus.AUTHENTICATED
        )

    async def _ensure_connected(self) -> None:
        if self._healthy():
            return
        api = self.client.api
        # 1) A camada WS reconecta sozinha: espera em vez de abrir uma 2ª conexão
        #    com o mesmo SSID (o servidor rejeita as duas).
        if api is not None and api.state.auth_status != AuthStatus.FAILED:
            logger.warning("Conexão caiu. Aguardando reconexão automática...")
            deadline = time.monotonic() + self.cfg.reconnect_wait
            while time.monotonic() < deadline:
                await asyncio.sleep(1)
                if self._healthy():
                    logger.info("Reconectado.")
                    await self._set_demo()
                    return
                if api.state.auth_status == AuthStatus.FAILED:
                    break
        # 2) Sem sucesso: recria a conexão (connect() fecha a anterior antes).
        logger.warning("Recriando a conexão...")
        connected, reason = await self.client.connect()
        if not connected:
            raise ConnectionError(f"reconexão falhou: {reason}")
        await self._set_demo()

    async def _balance(self) -> float:
        return float(await self.client.get_balance())

    # -- sessão diária -------------------------------------------------------
    async def _roll_day(self) -> None:
        """Renova o saldo base quando o dia muda (bot rodando após meia-noite)."""
        today = datetime.now().strftime("%Y-%m-%d")
        if today == self.session_date:
            return
        balance = await self._balance()
        self.initial_balance = self.memory.get_or_set_daily_balance(balance)
        self.session_date = today
        if self.paused_date and self.paused_date != today:
            logger.info("Novo dia: bloqueio diário liberado.")
            self.paused_date = None
        logger.info("Sessão %s | saldo base R$ %.2f", today, self.initial_balance)

    # -- seleção de ativos -------------------------------------------------
    async def _refresh_assets(self) -> None:
        cfg = self.cfg
        names = list(await self.client.get_all_assets())
        tf_minutes = str(max(1, cfg.timeframe // 60))

        candidates: Dict[str, float] = {}
        for name in names:
            payout = normalize_payout(self.client.get_payout_by_asset(name, tf_minutes))
            if payout is not None and payout >= cfg.min_payout:
                candidates[name] = payout

        pool = dict(
            sorted(candidates.items(), key=lambda kv: kv[1], reverse=True)[: cfg.pool_size]
        )

        stats = {asset: self.memory.asset_record(asset, cfg.rank_window) for asset in pool}
        previous = set(self.active)
        self.payouts = pool
        self.active = rank_assets(pool, stats, cfg.top_n, cfg.min_sample)
        self.last_refresh = time.time()

        # Cancela streams e cache dos ativos que saíram da lista.
        for asset in previous - set(self.active):
            self.history_cache.pop(asset, None)
            try:
                await self.client.stop_candles_stream(asset)
            except Exception as exc:
                logger.debug("%s: falha ao cancelar stream (%s)", asset, exc)

        if not self.active:
            logger.warning(
                "Nenhum ativo elegível (payout >= %.0f%% em M%s). Aguardando próxima reavaliação.",
                cfg.min_payout * 100,
                tf_minutes,
            )
            return

        for asset in self.active:
            wins, total = stats[asset]
            taxa = f"{wins / total:.1%}" if total else "sem dados"
            logger.info(
                "Ativo %s (M%s) | payout %.0f%% | breakeven %.1f%% | amostra %d/%d | taxa %s",
                asset, tf_minutes, pool[asset] * 100, breakeven(pool[asset]) * 100,
                total, cfg.min_sample, taxa,
            )

        # Pré-carrega o histórico fora da janela de entrada (max_entry_delay), que é curta
        # demais para o fallback get_historical_candles.
        now = int(time.time())
        candle_open = now - (now % cfg.timeframe)
        for asset in self.active:
            if len(self.history_cache.get(asset, {})) >= cfg.min_candles_history:
                continue
            try:
                await self._load_history(asset, candle_open)
            except Exception as exc:
                logger.warning("%s: falha ao pré-carregar histórico (%s)", asset, exc)

    # -- ciclo principal ---------------------------------------------------
    async def start(self) -> None:
        try:
            connected, reason = await self.client.connect()
            if not connected:
                raise ConnectionError(f"falha ao conectar: {reason}")
            await self._set_demo()
            await self._roll_day()
            balance = await self._balance()
        except BaseException:
            # Qualquer falha no início (ex.: HTTP 403 no login) fecha o cliente. Sem isso
            # o websocket e o laço de reconexão da tentativa anterior seguiam vivos,
            # fazendo logins por conta própria enquanto o supervisor criava outro bot.
            await self.client.close()
            raise

        orphans = self.memory.reconcile_open_trades()
        if orphans:
            logger.warning(
                "%d operação(ões) sem resultado de execução anterior marcadas como UNRESOLVED",
                orphans,
            )

        logger.info(
            "Bot iniciado (M%d | EMA %d/%d) | Saldo base: R$ %.2f | Atual: R$ %.2f",
            self.cfg.timeframe // 60,
            self.cfg.fast_ema,
            self.cfg.slow_ema,
            self.initial_balance,
            balance,
        )
        try:
            await self.run()
        finally:
            await self.client.close()
            logger.info("Conexão encerrada.")

    async def run(self) -> None:
        last_processed: Optional[int] = None
        tf = self.cfg.timeframe
        while True:
            try:
                await self._ensure_connected()
                await self._roll_day()
                if self.paused_date:
                    await asyncio.sleep(30)  # stop diário: espera o próximo dia
                    continue
                if time.time() - self.last_refresh >= self.cfg.reselect_seconds:
                    await self._refresh_assets()
                now = int(time.time())
                candle_open = now - (now % tf)
                if candle_open != last_processed:
                    last_processed = candle_open
                    await self._on_new_candle(candle_open)
                self._backoff = 5
            except Exception:
                logger.exception("Erro no ciclo. Nova tentativa em %ss", self._backoff)
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 60)
                continue
            await asyncio.sleep(0.5)

    async def _scan(self, candle_open: int) -> Optional[Tuple[str, Signal]]:
        """Busca velas dos ativos em paralelo; retorna o primeiro sinal na ordem do ranking."""
        cfg = self.cfg
        eligible: List[str] = []
        for asset in self.active:
            if self.memory.already_traded_candle(asset, candle_open):
                continue
            in_cooldown, remaining = self.memory.is_in_cooldown(
                asset, cfg.max_consecutive_losses, cfg.cooldown_seconds, cfg.timeframe
            )
            if in_cooldown:
                logger.info("%s em cooldown por sequência de loss (%ss restantes).", asset, remaining)
                continue
            eligible.append(asset)
        if not eligible:
            return None

        sem = asyncio.Semaphore(max(1, cfg.scan_concurrency))

        async def fetch(asset: str) -> Optional[List[Dict[str, float]]]:
            async with sem:
                if time.time() - candle_open > cfg.max_entry_delay:
                    return None
                return await self._fetch_candles(asset, candle_open)

        results = await asyncio.gather(*(fetch(a) for a in eligible), return_exceptions=True)
        verdicts: List[str] = []
        for asset, candles in zip(eligible, results):
            short = asset.replace("_otc", "")
            if isinstance(candles, BaseException):
                logger.warning("%s: erro na varredura (%s)", asset, candles)
                verdicts.append(f"{short}=erro")
                continue
            if not candles:
                verdicts.append(f"{short}=sem velas")
                continue
            signal, reason = evaluate_signal(candles, cfg)
            if signal is not None:
                if self.traded_cross.get(asset) == signal.cross_ts:
                    verdicts.append(f"{short}=cruzamento já operado")
                    continue
                return asset, signal
            verdicts.append(f"{short}={reason}")
        # Batimento por vela: silêncio no log passa a significar bot parado, não "sem sinal".
        logger.info(
            "Vela %s sem entrada | %s",
            datetime.fromtimestamp(candle_open).strftime("%H:%M"),
            ", ".join(verdicts),
        )
        return None

    async def _on_new_candle(self, candle_open: int) -> None:
        cfg = self.cfg
        if time.time() - candle_open > cfg.max_entry_delay:
            logger.info("Vela iniciada há muito tempo. Sinal desta vela ignorado.")
            return

        balance = await self._balance()
        allowed, amount = self.check_risk(balance)
        if not allowed:
            return

        if not self.active:
            logger.info("Nenhum ativo na lista. Sem entrada nesta vela.")
            return

        hit = await self._scan(candle_open)
        if hit is None:
            return

        asset, signal = hit
        direction, ema_f, ema_s = signal.direction, signal.ema_fast, signal.ema_slow
        if time.time() - candle_open > cfg.max_entry_delay:
            logger.info("Entrada cancelada: atraso acima do limite após análise.")
            return
        self.traded_cross[asset] = signal.cross_ts

        logger.info(
            "ENTRADA %s em %s | R$ %.2f | EMA%d=%.5f EMA%d=%.5f | RSI=%.1f | pavio=%.0f%% | ATR=%.2f",
            direction.upper(), asset, amount, cfg.fast_ema, ema_f, cfg.slow_ema, ema_s,
            signal.rsi, signal.wick_ratio * 100, signal.atr_ratio,
        )
        ok, order = await self.client.buy(amount, asset, direction, cfg.timeframe)
        if not ok:
            # "NotSent": a ordem não saiu da máquina. Timeout: pode ter sido executada
            # e só a confirmação se perdeu; não é rejeição.
            reason = str(order)
            status = "UNCONFIRMED" if reason == "Timeout" else "REJECTED"
            if status == "UNCONFIRMED":
                logger.error("Ordem sem confirmação (pode ter sido executada): %s", reason)
            else:
                logger.error("Ordem rejeitada: %s", reason)
            self.memory.record_trade(
                candle_open, asset, direction, amount, ema_f, ema_s,
                status=status, error_reason=reason,
                rsi=signal.rsi, wick_ratio=signal.wick_ratio, atr_ratio=signal.atr_ratio,
            )
            return

        order_id = order.get("id") if isinstance(order, dict) else None
        trade_id = self.memory.record_trade(
            candle_open, asset, direction, amount, ema_f, ema_s, status="EXECUTED",
            rsi=signal.rsi, wick_ratio=signal.wick_ratio, atr_ratio=signal.atr_ratio,
        )
        try:
            status, profit = await self._await_result(order_id, balance)
        except Exception as exc:
            logger.exception("Falha ao obter resultado da ordem %s", order_id)
            self.memory.update_trade_result(trade_id, "UNRESOLVED", 0.0)
            raise ConnectionError(f"resultado indisponível: {exc}") from exc
        self.memory.update_trade_result(trade_id, status, profit)
        label = {"WIN": "GAIN", "LOSS": "LOSS", "DRAW": "EMPATE"}.get(status, status)
        logger.info("RESULTADO %s: %s | valor: R$ %+.2f", asset, label, profit)

    def check_risk(self, balance: float) -> Tuple[bool, float]:
        cfg = self.cfg
        pnl = balance - self.initial_balance

        if pnl <= -(self.initial_balance * cfg.max_daily_loss_pct):
            logger.critical("BLOQUEIO DIÁRIO: stop loss atingido (PnL R$ %.2f). Pausado até amanhã.", pnl)
            self.paused_date = self.session_date
            return False, 0.0

        if pnl >= self.initial_balance * cfg.daily_target_pct:
            logger.info("BLOQUEIO DIÁRIO: stop win atingido (PnL R$ %.2f). Pausado até amanhã.", pnl)
            self.paused_date = self.session_date
            return False, 0.0

        stake = round(balance * cfg.risk_per_trade_pct, 2)
        if stake < cfg.min_amount:
            logger.warning(
                "Stake de %.0f%% (R$ %.2f) abaixo do mínimo (R$ %.2f). Entrada ignorada.",
                cfg.risk_per_trade_pct * 100, stake, cfg.min_amount,
            )
            return False, 0.0
        return True, stake

    async def _load_history(self, asset: str, candle_open: int) -> None:
        """
        Carrega e atualiza o histórico de velas fechadas com tolerância
        aos diferentes formatos de retorno da pyquotex e tipagem estrita.
        """
        cfg = self.cfg
        last_closed = candle_open - cfg.timeframe
        cache = self.history_cache.setdefault(asset, {})

        has_base = len(cache) >= cfg.min_candles_history and max(cache) >= last_closed - (cfg.timeframe * 3)
        fetch_count = 10 if has_base else (cfg.min_candles_history + 15)
        offset_sec = fetch_count * cfg.timeframe

        # end_from_time = agora: a biblioteca descarta o último grupo (vela em
        # formação). Com candle_open, o descartado seria a última vela FECHADA.
        raw: Any = await self.client.get_candles(
            asset,
            end_from_time=time.time(),
            offset=offset_sec,
            period=cfg.timeframe,
        )
        self._merge_candles(asset, raw, last_closed)

        # get_candles às vezes devolve só as 2-3 velas mais recentes (stream), não vazio.
        # Sem este fallback o ativo ficaria horas sem histórico suficiente.
        if len(cache) < cfg.min_candles_history:
            hist: Any = await self.client.get_historical_candles(
                asset, offset_sec, cfg.timeframe
            )
            self._merge_candles(asset, hist, last_closed)
            logger.info("%s: histórico completado (%d velas em cache).", asset, len(cache))

        keep = cfg.min_candles_history + 25
        for t in sorted(cache)[:-keep]:
            del cache[t]

    def _merge_candles(self, asset: str, raw: Any, last_closed: int) -> None:
        """Junta velas fechadas (até last_closed) no cache, aceitando os formatos da pyquotex."""
        cfg = self.cfg
        cache = self.history_cache.setdefault(asset, {})
        if not isinstance(raw, list) or not raw:
            logger.warning("%s: API retornou lista de velas vazia.", asset)
            return

        added = 0
        for c in raw:
            if not isinstance(c, dict):
                continue

            raw_t: Any = c.get("time")
            if raw_t is None:
                raw_t = c.get("from")
            if raw_t is None:
                raw_t = c.get("timestamp")

            close_val: Any = c.get("close")
            if raw_t is None or close_val is None:
                continue

            raw_open: Any = c.get("open")
            open_val: Any = close_val if raw_open is None else raw_open
            raw_high: Any = c.get("high", c.get("max"))
            raw_low: Any = c.get("low", c.get("min"))

            try:
                aligned_t = _align_candle_ts(raw_t, cfg.timeframe)
                if aligned_t <= last_closed:
                    o, cl = float(open_val), float(close_val)
                    # Sem máxima/mínima, usa o corpo: pavio 0 e ATR medido pelos corpos.
                    hi = max(o, cl) if raw_high is None else max(float(raw_high), o, cl)
                    lo = min(o, cl) if raw_low is None else min(float(raw_low), o, cl)
                    cache[aligned_t] = {
                        "time": float(aligned_t),
                        "open": o,
                        "high": hi,
                        "low": lo,
                        "close": cl,
                    }
                    added += 1
            except (TypeError, ValueError):
                continue

        if added == 0:
            logger.warning("%s: formato de vela não reconhecido: %s", asset, raw[0])

    async def _fetch_candles(
        self, asset: str, candle_open: int
    ) -> Optional[List[Dict[str, float]]]:
        """
        Retorna as velas fechadas ordenadas até last_closed.
        Exige que a última e a penúltima vela estejam presentes e sem lacunas
        nas últimas 3 velas (onde ocorre o cruzamento e medição de inclinação).
        """
        cfg = self.cfg
        last_closed = candle_open - cfg.timeframe
        needed = cfg.min_candles_history

        for _ in range(cfg.candle_fetch_attempts):
            try:
                await self._load_history(asset, candle_open)
            except Exception as exc:
                logger.warning("%s: erro ao buscar velas (%s)", asset, exc)
                await asyncio.sleep(cfg.candle_fetch_interval)
                continue

            cache = self.history_cache.get(asset, {})
            if last_closed in cache and (last_closed - cfg.timeframe) in cache:
                closed_keys = [t for t in sorted(cache) if t <= last_closed]
                if len(closed_keys) >= needed:
                    series_keys = closed_keys[-needed:]
                    recent_keys = series_keys[-3:]
                    recent_ok = all(
                        b - a == cfg.timeframe for a, b in zip(recent_keys, recent_keys[1:])
                    )
                    if recent_ok:
                        return [cache[t] for t in series_keys]

            await asyncio.sleep(cfg.candle_fetch_interval)

        cache = self.history_cache.get(asset, {})
        max_ts = max(cache) if cache else 0
        max_str = datetime.fromtimestamp(max_ts).strftime("%H:%M:%S") if max_ts else "nenhuma"
        logger.warning(
            "%s: velas insuficientes (%d/%d) ou última vela %s ausente (última recebida: %s).",
            asset,
            len(cache),
            needed,
            datetime.fromtimestamp(last_closed).strftime("%H:%M:%S"),
            max_str,
        )
        return None

    async def _await_result(
        self, order_id: Any, balance_before: float
    ) -> Tuple[str, float]:
        """Espera o fechamento da ordem (evento do servidor) e mede o resultado no saldo."""
        cfg = self.cfg
        timeout = cfg.timeframe + cfg.result_extra_wait
        if order_id is not None:
            win, _ = await self.client.check_win(order_id, cfg.timeframe, timeout=timeout)
            if win == "unknown":
                logger.warning("Resultado da ordem %s não recebido; usando variação do saldo.", order_id)
            else:
                await asyncio.sleep(cfg.expiry_extra)  # saldo atualiza logo após o fechamento
        else:
            await asyncio.sleep(cfg.timeframe + cfg.expiry_extra)

        last: Optional[float] = None
        stable = 0
        balance_now = balance_before
        for _ in range(cfg.balance_max_reads):
            balance_now = await self._balance()
            if last is not None and abs(balance_now - last) < 0.005:
                stable += 1
                if stable >= cfg.balance_stable_reads:
                    break
            else:
                stable = 0
            last = balance_now
            await asyncio.sleep(1)

        profit = round(balance_now - balance_before, 2)
        if abs(profit) < 0.005:
            return "DRAW", 0.0
        return ("WIN" if profit > 0 else "LOSS"), profit


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------
def load_credentials() -> Tuple[str, str]:
    email = os.getenv("QUOTEX_EMAIL")
    password = os.getenv("QUOTEX_PASSWORD")
    if not email or not password:
        raise SystemExit("Defina QUOTEX_EMAIL e QUOTEX_PASSWORD nas variáveis de ambiente.")
    return email, password


_LOGIN_BLOCK_MARKERS = ("http 403", "cloudflare", "login failed", "authentication failed")


def is_login_block(exc: BaseException) -> bool:
    """True quando o erro indica login recusado/bloqueado, e não queda de rede."""
    text = str(exc).lower()
    return any(marker in text for marker in _LOGIN_BLOCK_MARKERS)


_lock_handle: Any = None


def acquire_single_instance_lock(path: Path = LOCK_PATH) -> bool:
    """Impede duas instâncias do bot: cada uma faria login e uma derrubaria a outra."""
    global _lock_handle
    handle = open(path, "a+")
    try:
        # `== "win32"` (não startswith): o Pylance/Pyright só reconhece esta forma e
        # então ignora o ramo do outro sistema (fcntl não existe no Windows).
        if sys.platform == "win32":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return False
    _lock_handle = handle  # mantém aberto (e travado) enquanto o processo vive
    return True


async def main() -> None:
    """Supervisor: se o bot cair (conexão, autorização), recria e tenta de novo para sempre."""
    email, password = load_credentials()
    if not acquire_single_instance_lock():
        raise SystemExit("Outra instância do bot já está rodando. Encerre-a antes de iniciar outra.")
    cfg = BotConfig()
    wait = cfg.restart_min_wait
    block_wait = cfg.login_block_min_wait
    while True:
        started = time.monotonic()
        bot = QuotexSmartBot(email=email, password=password, config=cfg)
        blocked = False
        try:
            await bot.start()
            logger.warning("Bot encerrou sem erro. Reiniciando em %ss.", wait)
        except Exception as exc:
            blocked = is_login_block(exc)
            if blocked:
                logger.error(
                    "Login bloqueado: %s. Nova tentativa em %d min (tentar antes piora o bloqueio).",
                    exc, block_wait // 60,
                )
            else:
                logger.error("Bot caiu: %s. Reiniciando em %ss.", exc, wait)
        finally:
            bot.memory.conn.close()
        if blocked:
            await asyncio.sleep(block_wait)
            block_wait = min(block_wait * 2, cfg.login_block_max_wait)
            continue
        # Rodou bastante antes de cair: queda pontual, volta a esperar pouco.
        if time.monotonic() - started > 600:
            wait = cfg.restart_min_wait
            block_wait = cfg.login_block_min_wait
        await asyncio.sleep(wait)
        wait = min(wait * 2, cfg.restart_max_wait)


if __name__ == "__main__":
    # Só ao rodar o bot: importar o módulo (ex.: nos testes) não pode escrever no log real.
    setup_logging()
    if sys.platform.startswith("win"):
        # curl_cffi precisa de add_reader, que o loop Proactor (padrão no Windows) não tem.
        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    else:
        asyncio.run(main())
