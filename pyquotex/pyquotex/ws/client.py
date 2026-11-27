"""Async WebSocket client for the Quotex API using curl_cffi.

Reader first
------------
The reader task (``recv`` loop) is started *before* the Engine.IO/Socket.IO
handshake, so the handshake response is actually read while we wait for it.
Control frames are answered in the reader: ``"2"`` -> ``"3"`` and
``"2probe"`` -> ``"3probe"``. An active ping ``"2"`` is sent every
:data:`ENGINEIO_PING_INTERVAL` seconds by an asyncio task.

Decoupled reception
-------------------
The reader only routes frames into a bounded ``asyncio.Queue``. A separate
consumer task calls ``api._on_message``, so slow processing never delays a
pong. When a connection ends, the queue is drained (with a timeout) before
the consumer is stopped, so frames received right before a disconnect (for
example an order result) are not lost.

Resilience layer
----------------
Auto-reconnect uses exponential backoff governed by
:class:`pyquotex.qxtypes.ReconnectPolicy`. The backoff only resets after a
connection has stayed open for :data:`STABLE_UPTIME` seconds. When the server
rejects the authorization (expired SSID), the client asks the API to log in
again (``api._reauthenticate``) before reconnecting, instead of retrying
forever with the same invalid token.

Engine.IO v3 handshake
----------------------
On every new connection the client sends the Socket.IO CONNECT frame ``"40"``
and waits for the server's open (``"0{...}"``) or connect (``"40"`` /
``"40{...}"``) frame before authorizing.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any

from curl_cffi.requests import CurlWsFlag

from pyquotex._api._waits import backoff_sleep
from pyquotex.global_value import AuthStatus, WebsocketStatus
from pyquotex.qxtypes import ReconnectPolicy

logger = logging.getLogger(__name__)

ENGINEIO_PING_INTERVAL = 20.0  # segundos entre pings ativos "2"
STABLE_UPTIME = 60.0           # segundos aberta para considerar a conexão estável
FRAME_LOG_LIMIT = 5            # quantos frames iniciais são registrados para diagnóstico
HANDSHAKE_TIMEOUT = 10.0       # segundos para aguardar o handshake do servidor
QUEUE_MAXSIZE = 10_000         # limite da fila de mensagens pendentes
DRAIN_TIMEOUT = 5.0            # segundos para esvaziar a fila ao fim de uma conexão
MAX_REAUTH_ATTEMPTS = 3        # novos logins seguidos após rejeição antes de desistir


class WebsocketClient:
    """Pure-async WebSocket client with heartbeat and optional auto-reconnect."""

    def __init__(
        self,
        api: Any,
        reconnect_policy: ReconnectPolicy | None = None,
    ) -> None:
        """Initialize the WebSocket client.

        Args:
            api: The :class:`QuotexAPI` instance this client belongs to.
            reconnect_policy: Resilience configuration. Defaults to
                :class:`ReconnectPolicy` with auto-reconnect enabled.
        """
        self.api = api
        self.state = api.state
        self.policy = reconnect_policy or ReconnectPolicy()
        self._ws: Any = None
        self._closing = False
        self._watchdog_task: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[Any] | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        # Referências fortes para tasks avulsas (evita coleta pelo GC no meio da execução).
        self._background: set[asyncio.Task[Any]] = set()
        self._frames_logged = 0
        # Counter of successful opens; the very first open does NOT
        # replay subscriptions (there are none yet).
        self._open_count = 0
        self._handshake_event: asyncio.Event | None = None

    @property
    def wss(self) -> "WebsocketClient":
        """Returns the low-level WebSocket wrapper (self)."""
        return self

    def _spawn(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def send(self, data: str | bytes) -> None:
        """Send a frame.

        Raises:
            ConnectionError: if the socket is closed or the send fails. The
                caller decides what to do; a silent failure would make an
                order look "sent" when it never left the machine.
        """
        ws = self._ws
        if ws is None or getattr(ws, "closed", False):
            raise ConnectionError("WebSocket não está aberto")
        try:
            async with self._send_lock:
                if isinstance(data, str):
                    await ws.send(data.encode("utf-8"), flags=CurlWsFlag.TEXT)
                else:
                    await ws.send(data, flags=CurlWsFlag.BINARY)
            logger.debug("Sent: %s", data)
        except Exception as e:
            logger.error("Error sending WebSocket message: %s", e)
            raise ConnectionError(f"falha ao enviar frame: {e}") from e

    async def _send_quiet(self, data: str) -> None:
        """Send used by control frames (ping/pong): never raises."""
        try:
            await self.send(data)
        except ConnectionError:
            pass

    async def run_forever(
        self,
        url: str,
        extra_headers: dict[str, str] | None = None,
        ssl: Any = None,
        **kwargs: Any,
    ) -> None:
        """Connect to the WebSocket and stay connected with auto-reconnect."""
        attempt = 0
        reauth_attempts = 0
        while True:
            started = time.monotonic()
            try:
                await self._connect_once(url, extra_headers)
            except Exception as e:
                logger.error("WebSocket error (%s): %s", type(e).__name__, e)
                self.api._on_error(e)

            if self._closing:
                return

            # Só zera o backoff se a conexão ficou aberta tempo suficiente.
            if time.monotonic() - started >= STABLE_UPTIME:
                attempt = 0
                reauth_attempts = 0

            if not self.policy.enabled or self._closing:
                return

            # SSID rejeitado: reconectar com o mesmo token nunca funciona.
            if getattr(self.state, "auth_status", None) == AuthStatus.FAILED:
                reauth = getattr(self.api, "_reauthenticate", None)
                if reauth is None or reauth_attempts >= MAX_REAUTH_ATTEMPTS:
                    logger.error("Autorização rejeitada; novo login indisponível ou esgotado. Parando.")
                    return
                reauth_attempts += 1
                try:
                    ok, msg = await reauth()
                except Exception as e:  # pragma: no cover - depende da rede
                    ok, msg = False, str(e)
                if not ok:
                    logger.error("Novo login falhou (%s). Parando reconexão.", msg)
                    return

            if self.policy.max_attempts and attempt >= self.policy.max_attempts:
                logger.error(
                    "WebSocket auto-reconnect giving up after %d attempts",
                    attempt,
                )
                return

            logger.info("WebSocket reconnecting (attempt #%d)", attempt + 1)
            await backoff_sleep(
                attempt,
                base=self.policy.base_delay,
                cap=self.policy.max_delay,
                jitter=self.policy.jitter,
            )
            attempt += 1

    async def _connect_once(
        self,
        url: str,
        extra_headers: dict[str, str] | None,
    ) -> None:
        """One ``connect()`` cycle. Returns when the connection ends."""
        headers = extra_headers or {}

        session = getattr(self.api.browser, "_client", None)
        if session is None or getattr(session, "_closed", False) is True:
            session = await self.api.browser.ensure_client()

        self._ws = await session.ws_connect(url, headers=headers)
        self.api.last_message_at = time.monotonic()
        on_connecting = getattr(self.api, "_on_connecting", None)
        if on_connecting is not None:
            on_connecting()
        self._frames_logged = 0

        # Reader e consumer sobem ANTES do handshake, senão ninguém lê a resposta.
        self._start_consumer()
        self._handshake_event = asyncio.Event()
        reader = asyncio.create_task(self._reader_loop())
        try:
            await self._do_engineio_handshake(reader)
            if self._closing:
                return

            await self.api._on_open()
            self._open_count += 1
            if self._open_count > 1:
                self._spawn(self._replay_subscriptions())

            self._start_ping()
            self._start_watchdog()
            await reader
        finally:
            self._handshake_event = None
            self._stop_ping()
            self._stop_watchdog()
            if not reader.done():
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await reader
            await self._stop_consumer(drain=True)
            if self._ws and not getattr(self._ws, "closed", False):
                with contextlib.suppress(Exception):
                    await self._ws.close()

    async def _reader_loop(self) -> None:
        try:
            while not self._closing:
                msg, _flags = await self._ws.recv()
                if not msg and getattr(self._ws, "closed", False):
                    break
                await self._route(msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._handle_close_exception(e)

    async def _do_engineio_handshake(self, reader: asyncio.Task[None]) -> None:
        """Send Socket.IO CONNECT ``"40"`` and wait for the server handshake.

        Returns early if the reader stops (socket closed) or on timeout.
        """
        assert self._handshake_event is not None
        logger.debug("Sending Engine.IO handshake frame: 40")
        await self.send("40")

        waiter = asyncio.create_task(self._handshake_event.wait())
        try:
            done, _ = await asyncio.wait(
                {waiter, reader},
                timeout=HANDSHAKE_TIMEOUT,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if waiter in done:
                logger.debug("Engine.IO handshake completed successfully")
            elif reader in done:
                logger.debug("Socket closed during handshake")
            else:
                logger.warning(
                    "Engine.IO handshake timed out after %.1fs; proceeding anyway. "
                    "Frames received during wait: %d",
                    HANDSHAKE_TIMEOUT,
                    self._frames_logged,
                )
        finally:
            if not waiter.done():
                waiter.cancel()

    # ------------------------------------------------------------------
    # Routing: controle no leitor, dados para a fila
    # ------------------------------------------------------------------
    async def _route(self, msg: str | bytes) -> None:
        text = msg.decode("utf-8", errors="ignore") if isinstance(msg, bytes) else str(msg)
        self._log_frame(text)

        # Engine.IO control frames — answer immediately, before any queueing
        if text == "2":
            await self._send_quiet("3")
            return
        if text == "2probe":
            await self._send_quiet("3probe")
            return

        hs = self._handshake_event
        if hs is not None and not hs.is_set():
            # Engine.IO OPEN "0{sid,...}" ou Socket.IO CONNECT "40" / "40{...}"
            if text == "40" or text == "0":
                hs.set()
                return
            if text.startswith("40{") or text.startswith("0{"):
                payload = text[2:] if text.startswith("40") else text[1:]
                try:
                    json.loads(payload)
                    hs.set()
                    return
                except json.JSONDecodeError:
                    pass

        self._enqueue(msg)

    def _enqueue(self, msg: Any) -> None:
        q = self._queue
        if q is None:
            return
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            logger.warning("Fila de mensagens cheia (%d); descartando a mais antiga.", QUEUE_MAXSIZE)
            with contextlib.suppress(asyncio.QueueEmpty):
                q.get_nowait()
                q.task_done()
            q.put_nowait(msg)

    def _log_frame(self, text: str) -> None:
        """Registra só o início dos primeiros frames, para diagnóstico do protocolo."""
        if self._frames_logged < FRAME_LOG_LIMIT:
            self._frames_logged += 1
            logger.debug("Frame recebido (%d): %s", self._frames_logged, text[:200])

    def _start_consumer(self) -> None:
        self._queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._consumer_task = asyncio.create_task(self._consume(self._queue))

    async def _consume(self, queue: asyncio.Queue[Any]) -> None:
        while True:
            msg = await queue.get()
            try:
                await self.api._on_message(msg)
            except Exception as e:
                logger.error("Erro ao processar mensagem: %s", e)
            finally:
                queue.task_done()

    async def _stop_consumer(self, drain: bool = False) -> None:
        task = self._consumer_task
        queue = self._queue
        if drain and task and not task.done() and queue is not None:
            try:
                await asyncio.wait_for(queue.join(), timeout=DRAIN_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning("Fila não esvaziou em %.0fs; %d mensagens descartadas.",
                               DRAIN_TIMEOUT, queue.qsize())
        self._consumer_task = None
        self._queue = None
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # ------------------------------------------------------------------
    # Ping ativo "2" a cada ENGINEIO_PING_INTERVAL segundos
    # ------------------------------------------------------------------
    def _start_ping(self) -> None:
        self._stop_ping()
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        try:
            while self.is_alive():
                await asyncio.sleep(ENGINEIO_PING_INTERVAL)
                if self.is_alive():
                    await self._send_quiet("2")
        except asyncio.CancelledError:
            pass

    def _stop_ping(self) -> None:
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = None

    def _handle_close_exception(self, exc: Exception) -> None:
        logger.info("WebSocket closed: reason=%s", str(exc))
        self.api._on_close(1006, str(exc))

    # ------------------------------------------------------------------
    # Stale-connection watchdog
    # ------------------------------------------------------------------
    def _start_watchdog(self) -> None:
        if self.policy.stale_timeout <= 0:
            return
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    def _stop_watchdog(self) -> None:
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
        self._watchdog_task = None

    async def _watchdog_loop(self) -> None:
        timeout = self.policy.stale_timeout
        try:
            while self._ws is not None and not getattr(self._ws, "closed", False):
                await asyncio.sleep(min(timeout / 3.0, 10.0))
                silent_for = time.monotonic() - self.api.last_message_at
                if silent_for > timeout:
                    logger.warning(
                        "WebSocket idle for %.1fs (>%ds); recycling.",
                        silent_for,
                        timeout,
                    )
                    with contextlib.suppress(Exception):
                        await self._ws.close()
                    return
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Subscription replay after reconnect
    # ------------------------------------------------------------------
    async def _replay_subscriptions(self) -> None:
        """Re-issue every tracked subscription after a successful reconnect."""
        # Espera a autorização: assinar antes dela é ignorado pelo servidor.
        for _ in range(100):  # ~5 s
            if self.state.status == WebsocketStatus.CONNECTED and getattr(
                self.state, "auth_status", AuthStatus.AUTHENTICATED
            ) != AuthStatus.NOT_AUTHENTICATED:
                break
            await asyncio.sleep(0.05)

        subs = list(getattr(self.api, "_subscriptions", {}).values())
        for sub in subs:
            try:
                await self._replay_one(sub)
            except Exception as e:
                logger.warning(
                    "Failed to replay subscription kind=%s asset=%s: %s",
                    sub.kind,
                    sub.asset,
                    e,
                )

    async def _replay_one(self, sub: Any) -> None:
        api = self.api
        if sub.kind == "candle":
            await api.subscribe_realtime_candle(sub.asset, sub.period or 0)
            await api.chart_notification(sub.asset)
            await api.follow_candle(sub.asset)
        elif sub.kind == "candle_all_size":
            await api.subscribe_all_size(sub.asset)
        elif sub.kind == "mood":
            instrument = sub.extra.get("instrument", "turbo-option")
            await api.subscribe_Traders_mood(sub.asset, instrument)
        elif sub.kind == "realtime_price":
            await api.subscribe_realtime_candle(sub.asset, sub.period or 0)

    async def close(self) -> None:
        """Close the websocket gracefully and stop auto-reconnect."""
        self._closing = True
        self.policy = ReconnectPolicy(enabled=False)
        self._stop_ping()
        self._stop_watchdog()
        for task in list(self._background):
            task.cancel()
        if self._ws and not getattr(self._ws, "closed", False):
            with contextlib.suppress(Exception):
                await self._ws.close()
        await self._stop_consumer(drain=False)

    def is_alive(self) -> bool:
        """Return True iff the underlying socket is currently OPEN."""
        return self._ws is not None and not getattr(self._ws, "closed", False)
