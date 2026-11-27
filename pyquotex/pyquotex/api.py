import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable

from .config import update_session
from .global_value import AuthStatus, ConnectionState, WebsocketStatus
from .network.history import GetHistory
from .network.login import Login
from .network.logout import Logout
from .network.navigator import USER_AGENT_DEFAULT, Browser
from .network.settings import Settings
from .utils import json_utils as json
from .utils.account_type import AccountType
from .utils.async_utils import EventRegistry
from .ws.channels.buy import Buy
from .ws.channels.candles import GetCandles
from .ws.channels.sell_option import SellOption
from .ws.channels.ssid import Ssid
from .ws.client import WebsocketClient
from .ws.objects.candles import Candles
from .ws.objects.listinfodata import ListInfoData
from .ws.objects.profile import Profile
from .ws.objects.timesync import TimeSync

logger = logging.getLogger(__name__)


class QuotexAPI:
    """Class for communication with Quotex API."""

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        lang: str,
        proxies: dict[str, str] | None = None,
        resource_path: str | None = None,
        user_data_dir: str = ".",
        on_otp_callback: Callable | None = None,
        reconnect_policy: Any = None,
        wss_url_override: str | None = None,
    ):
        """
        :param str host: The hostname or ip address of a Quotex server.
        :param str username: The username of a Quotex server.
        :param str password: The password of a Quotex server.
        :param str lang: The lang of a Quotex platform.
        :param proxies: The proxies of a Quotex server.
        :param user_data_dir: The path browser user data dir.
        :param on_otp_callback: Callback function for OTP (2FA) input.
        :param wss_url_override: Replace the computed ``wss://ws2.{host}``
            URL with this value. Primarily a test hook so the offline
            integration tests can point at a local replay server, but
            also useful for routing through a custom proxy.
        """
        self.state = ConnectionState()
        self.on_otp_callback = on_otp_callback
        self.reconnect_policy = reconnect_policy
        self._ws_send_lock = asyncio.Lock()

        self.socket_option_opened: dict[str, Any] = {}
        self.buy_id: str | int | None = None
        self.pending_id: str | int | None = None
        self.trace_ws: bool = False
        self.buy_expiration: int | None = None
        self.current_asset: str | None = None
        self.current_period: int | None = None
        self.buy_successful: bool | None = None
        self.pending_successful: bool | None = None
        self.account_balance: dict[str, Any] | None = None
        self.account_type: AccountType | int | None = AccountType.DEMO
        self.tournament_id: int = 0
        self.instruments: list[Any] = []
        self.training_balance_edit_request: dict[str, Any] | None = None
        self.profit_in_operation: float | None = None
        self.sold_options_respond: Any = None
        self.sold_digital_options_respond: Any = None
        self.listinfodata = ListInfoData()
        self.timesync = TimeSync()
        self.candles = Candles()
        self.profile = Profile()

        self.host = host
        self.https_url = f"https://{host}"
        self.wss_url = (
            wss_url_override
            if wss_url_override
            else f"wss://ws2.{host}/socket.io/?EIO=3&transport=websocket"
        )
        self.wss_message: str | None = None
        self.websocket_client: WebsocketClient | None = None
        self._websocket_task: asyncio.Task | None = None
        self.set_ssid: Any = None
        self.object_id: Any = None
        self.token_login2fa: str | None = None
        self.is_logged: bool = False
        self._temp_status: str = ""
        self.username = username
        self.password = password
        self.resource_path = resource_path
        self.user_data_dir = user_data_dir
        self.proxies = proxies
        self.lang = lang
        self.settings_list: dict[str, Any] = {}
        self.signal_data: dict[str, Any] = {}
        self.get_candle_data: dict[str, Any] = {}
        self.historical_candles: dict[str, Any] = {}
        self.candle_v2_data: dict[str, Any] = {}
        self.realtime_price: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.realtime_price_data: list[Any] = []
        self.realtime_candles: dict[str, Any] = {}
        self.realtime_sentiment: dict[str, Any] = {}
        self.traders_mood: dict[str, Any] = {}
        self.candle_generated_check = defaultdict(lambda: defaultdict(dict))
        self.candle_generated_all_size_check = defaultdict(dict)
        self.top_list_leader: dict[str, Any] = {}
        self.session_data: dict[str, Any] = {}
        self.browser = Browser()
        self.browser.set_headers()
        self.settings = Settings(self)
        self.event_registry = EventRegistry()
        from pyquotex._api._waits import SlotRegistry

        self.slots = SlotRegistry()
        self.profit_today: float | None = None
        self._auth_retry: bool = False

        # Last time an inbound frame arrived; used by the stale watchdog
        # in :class:`pyquotex.ws.client.WebsocketClient` to decide when
        # to recycle a silent connection.
        self.last_message_at: float = time.monotonic()

        # Active stream subscriptions, replayed after auto-reconnect.
        from pyquotex.qxtypes import Subscription

        self._subscriptions: dict[str, Subscription] = {}

        # Dispatch table for "control" events: maps Socket.IO event name
        # to an async handler taking the event payload.
        self._control_handlers: dict[str, Callable[[Any], Awaitable[None]]] = {
            "s_authorization": self._h_auth_ok,
            "instruments/list": self._h_instruments_list,
            "trader/history": self._h_trader_history,
            "balance": self._h_balance,
            "candle-generated": self._h_candle_generated,
            "sentiment": self._h_sentiment,
        }

    # ------------------------------------------------------------------
    # Subscription tracking (replayed by WebsocketClient after reconnect)
    # ------------------------------------------------------------------
    def _track_subscription(
        self,
        kind: str,
        asset: str,
        period: int | None = None,
        **extra: Any,
    ) -> None:
        """Record an active stream so it can be replayed after reconnect."""
        from pyquotex.qxtypes import Subscription

        key = f"{kind}:{asset}:{period or 0}"
        self._subscriptions[key] = Subscription(
            kind=kind,  # type: ignore[arg-type]
            asset=asset,
            period=period,
            extra=dict(extra),
        )

    def _forget_subscription(self, kind: str, asset: str, period: int | None = None) -> None:
        key = f"{kind}:{asset}:{period or 0}"
        self._subscriptions.pop(key, None)

    # ------------------------------------------------------------------
    # Control-event handlers (dispatch table targets)
    # ------------------------------------------------------------------
    async def _h_auth_ok(self, data: Any) -> None:
        self.state.auth_status = AuthStatus.AUTHENTICATED
        self._auth_retry = False
        await self.event_registry.set_event("auth_changed", self.state.auth_status)

    async def _h_instruments_list(self, data: Any) -> None:
        if isinstance(data, dict) and data.get("_placeholder"):
            self._temp_status = f'451-["instruments/list",{json.dumps_str(data)}]'
        elif isinstance(data, list):
            self.instruments = data
            await self.event_registry.set_event("instruments_ready", data)
        elif isinstance(data, dict) and isinstance(data.get("list"), list):
            self.instruments = data["list"]
            await self.event_registry.set_event("instruments_ready", self.instruments)

    async def _h_trader_history(self, data: Any) -> None:
        await self.event_registry.set_event("history_ready", data)

    async def _h_balance(self, data: Any) -> None:
        self.account_balance = data
        if data is not None:
            self.slots.balance.set(data)
        await self.event_registry.set_event("balance_ready", data)

    async def _h_candle_generated(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        asset = data.get("asset")
        period = data.get("period")
        if asset and period:
            self.candle_generated_check[str(asset)][int(period)] = data
            self.candle_generated_all_size_check[str(asset)] = data

    async def _h_sentiment(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        asset = data.get("asset")
        if asset:
            self.traders_mood[asset] = data
            self.realtime_sentiment[asset] = data

    # ------------------------------------------------------------------
    # WebSocket lifecycle
    # ------------------------------------------------------------------
    async def _on_open(self) -> None:
        """Called when WebSocket connection is established (after Engine.IO handshake)."""
        logger.info("Websocket client connected.")
        self.state.status = WebsocketStatus.CONNECTED
        await self.event_registry.set_event("status_changed", self.state.status)

        ws = self.websocket
        if ws is None:
            return

        # Send authorization packet FIRST before any other command
        if self.state.SSID:
            is_demo_val = int(self.account_type) if self.account_type is not None else 1
            payload = {
                "session": self.state.SSID,
                "isDemo": is_demo_val,
                "tournamentId": self.tournament_id,
            }
            auth_frame = f'42["authorization",{json.dumps_str(payload)}]'
            logger.debug(
                "Sending authorization: session=%s... isDemo=%s",
                str(self.state.SSID)[:10],
                is_demo_val,
            )
            await ws.send(auth_frame)
        else:
            logger.warning("No SSID available for authorization!")

        # Send initial requests — Engine.IO ping/pong in WebsocketClient keeps alive
        await ws.send('42["indicator/list"]')
        await ws.send('42["drawing/load"]')
        await ws.send('42["pending/list"]')
        await ws.send('42["chart_notification/get"]')
        await ws.send('42["instruments/get"]')

    def _on_connecting(self) -> None:
        """Chamado pelo cliente WS logo após abrir o socket, antes de ler frames.

        Nova conexão = nova autorização. Sem isso o estado AUTHENTICATED da
        conexão anterior faz o chamador enviar ordens antes do s_authorization.
        """
        if self.state.auth_status != AuthStatus.FAILED:
            self.state.auth_status = AuthStatus.NOT_AUTHENTICATED

    async def _on_message(self, msg: bytes | str) -> None:
        """Called for every WebSocket message received."""
        self.last_message_at = time.monotonic()
        try:
            message: Any = None
            msg_str = msg.decode("utf-8", errors="ignore") if isinstance(msg, bytes) else str(msg)

            if self.state.auth_status != AuthStatus.AUTHENTICATED:
                logger.debug("WS received while not authenticated: %s", msg_str[:200])

            if "authorization/reject" in msg_str:
                safe = msg_str.replace(str(self.state.SSID), "<SSID>") if self.state.SSID else msg_str
                logger.error("=== AUTH REJECTED ===")
                logger.error("Frame completo: %s", safe)
                logger.error("SSID usado: %s...", str(self.state.SSID)[:10] if self.state.SSID else "None")
                logger.error("account_type (isDemo): %s", self.account_type)
                logger.error("Cookies na sessão: %s", "presente" if self.session_data.get("cookies") else "ausente")

                self.state.websocket_error_reason = f"Websocket connection rejected ({safe})."
                self.state.auth_status = AuthStatus.FAILED
                self._invalidate_session()
                # Fecha o socket: o loop de reconexão faz novo login antes de tentar de novo.
                if self.websocket_client is not None and self.websocket_client._ws is not None:
                    self.websocket_client._spawn(self._close_ws_quietly())
                self.state.status = WebsocketStatus.ERROR
                await self.event_registry.set_event("auth_changed", self.state.auth_status)
                await self.event_registry.set_event("status_changed", self.state.status)
                return
            elif "s_authorization" in msg_str:
                logger.debug("Websocket authorization succeeded")
                self.state.auth_status = AuthStatus.AUTHENTICATED
                self.state.status = WebsocketStatus.CONNECTED
                await self.event_registry.set_event("auth_changed", self.state.auth_status)
                await self.event_registry.set_event("status_changed", self.state.status)
                return

            # Detect Socket.IO prefix
            is_control = bool(msg_str and msg_str[0].isdigit())

            # Clean JSON extraction
            try:
                start_idx = -1
                for idx, char in enumerate(msg_str):
                    if char in ("[", "{"):
                        start_idx = idx
                        break

                if start_idx != -1:
                    clean_json = msg_str[start_idx:]
                    data_json = json.loads(clean_json)
                    message = data_json
            except Exception as e:
                logger.debug("Failed to parse raw data payload: %s", e)

            # 1. Handle Control Messages (Placeholders)
            if is_control:
                if "51-" in msg_str and "_placeholder" in msg_str:
                    self._temp_status = msg_str
                    return

                if isinstance(message, list) and len(message) > 1 and isinstance(message[0], str):
                    event = message[0]
                    data = message[1]
                    handler = self._control_handlers.get(event)
                    if handler is not None:
                        await handler(data)

            # 2. Handle Data Payloads (Placeholder fulfillment)
            elif message is not None and not is_control:
                data = message[0] if isinstance(message, list) and len(message) == 1 else message

                if self._temp_status and "instruments/list" in self._temp_status:
                    if isinstance(data, list):
                        self.instruments = data
                    elif isinstance(data, dict) and "list" in data:
                        self.instruments = data["list"]

                    if self.instruments:
                        await self.event_registry.set_event("instruments_ready", self.instruments)

                elif any(x in self._temp_status for x in ["history/list/v2", "history/load"]) or (
                    isinstance(data, dict) and (data.get("candles") or data.get("data"))
                ):
                    if isinstance(data, dict) and data.get("asset"):
                        asset = data["asset"]
                        self.candle_v2_data[asset] = data
                        self.slots.candle_v2(asset).set(data)
                        await self.event_registry.set_event(f"candles_ready_{asset}", data)
                        if data.get("index") is not None:
                            # Só notifica se houver alguém esperando este index;
                            # criar uma chave por resposta vaza memória.
                            await self.event_registry.set_existing(
                                f"candles_ready_{asset}_{data['index']}", data
                            )
                    elif isinstance(data, list):
                        await self.event_registry.set_event("history_ready", data)

                elif self._temp_status and any(
                    x in self._temp_status
                    for x in [
                        "orders/open",
                        "orders/close",
                        "orders/opened",
                        "pending/create",
                        "pending/opened",
                    ]
                ):
                    logger.debug("Order event via placeholder! status=%s", self._temp_status)

                    orders_to_process: list[dict[str, Any]] = []
                    if isinstance(data, list):
                        orders_to_process = data
                    elif isinstance(data, dict):
                        if data.get("deals"):
                            orders_to_process = data["deals"]
                        else:
                            orders_to_process = [data]

                    for order in orders_to_process:
                        order_id = order.get("id")
                        if order_id:
                            profit = order.get("profit", 0)
                            win = "win" if profit > 0 else "loss"
                            is_closed = (
                                any(x in self._temp_status for x in ["closed", "close"])
                                or order.get("status") == "closed"
                            )
                            game_state = 1 if is_closed else 0

                            logger.debug(
                                "Processing order %s: win=%s, state=%s, profit=%s",
                                order_id,
                                win,
                                game_state,
                                profit,
                            )
                            self.listinfodata.set(win, game_state, order_id, profit)
                            self.listinfodata.set(win, game_state, str(order_id), profit)
                            if game_state == 1:
                                self.slots.win_result(str(order_id)).set(
                                    {"win": win, "profit": profit}
                                )

                    if any(
                        x in self._temp_status for x in ["orders/open", "pending/create"]
                    ) and isinstance(data, dict):
                        if "pending" in self._temp_status:
                            self.pending_id = data.get("id")
                            self.pending_successful = True
                            if self.pending_id is not None:
                                self.slots.pending_confirm.set({"id": self.pending_id})
                            await self.event_registry.set_event("pending_confirmed", data)
                        else:
                            self.buy_id = data.get("id")
                            self.buy_successful = True
                            if self.buy_id is not None:
                                self.slots.buy_confirm.set({"id": self.buy_id})
                            await self.event_registry.set_event("buy_confirmed", data)

                self._temp_status = ""

            # 3. Handle Real-time and Profile Dicts
            if isinstance(message, dict):
                if message.get("liveBalance") or message.get("demoBalance"):
                    self.account_balance = message
                    self.slots.balance.set(message)
                    await self.event_registry.set_event("balance_ready", message)
                elif message.get("deals"):
                    for order in message["deals"]:
                        order_id = order.get("id")
                        if order_id:
                            profit = order.get("profit", 0)
                            win = "win" if profit > 0 else "loss"
                            logger.debug(
                                "Real-time deal update for %s: win=%s, profit=%s",
                                order_id,
                                win,
                                profit,
                            )
                            self.listinfodata.set(win, 1, order_id, profit)
                            self.listinfodata.set(win, 1, str(order_id), profit)
                            self.slots.win_result(str(order_id)).set({"win": win, "profit": profit})
                    await self.event_registry.set_event("history_ready", message)
                elif "id" in message and ("asset" in message or "amount" in message):
                    self.buy_id = message.get("id")
                    if self.buy_id is not None:
                        self.slots.buy_confirm.set({"id": self.buy_id})
                    await self.event_registry.set_event("buy_confirmed", message)

            elif isinstance(message, list) and len(message) > 1 and message[0] == "order":
                data = message[1]
                order_id = data.get("id")
                self.buy_id = order_id
                if self.buy_id is not None:
                    self.slots.buy_confirm.set({"id": self.buy_id})

                if "profit" in data and "status" in data:
                    profit = data.get("profit", 0)
                    win = "win" if profit > 0 else "loss"
                    game_state = 1 if data.get("status") == "closed" else 0
                    self.listinfodata.set(win, game_state, str(order_id), profit)
                    if game_state == 1 and order_id is not None:
                        self.slots.win_result(str(order_id)).set({"win": win, "profit": profit})

                await self.event_registry.set_event("buy_confirmed", data)

            elif isinstance(message, list) and len(message) > 0 and isinstance(message[0], list):
                if len(message[0]) == 4:
                    asset, ts, price = (message[0][0], message[0][1], message[0][2])
                    self.timesync.server_timestamp = ts

                    price_list = self.realtime_price[asset]
                    price_list.append({"time": ts, "price": price})
                    if len(price_list) > 1000:
                        price_list.pop(0)

                    self.realtime_candles[asset] = message[0]

        except Exception as e:
            logger.error("Error in _on_message: %s", e)

    def _on_error(self, error: Exception | str) -> None:
        """Handles WebSocket errors."""
        logger.error(error)
        self.state.websocket_error_reason = str(error)
        self.state.status = WebsocketStatus.ERROR
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self.event_registry.set_event("status_changed", self.state.status))
        except RuntimeError:
            pass

    def _on_close(self, code: int, msg: str) -> None:
        """Handles WebSocket connection closure."""
        logger.info("Websocket connection closed. code=%s reason=%s", code, msg)

        self.state.status = WebsocketStatus.DISCONNECTED
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self.event_registry.set_event("status_changed", self.state.status))
        except RuntimeError:
            pass

    @property
    def websocket(self) -> Any:
        """Returns the active WebSocket instance."""
        return self.websocket_client.wss if self.websocket_client else None

    async def get_instruments(self) -> None:
        """Sends a request to the WebSocket to retrieve the list of available instruments."""
        ws = self.websocket
        if ws:
            await ws.send('42["instruments/get"]')

    async def authenticate(self) -> tuple[bool, str]:
        """Authenticates the user using the provided credentials."""
        login_obj = self.login
        status, msg = await login_obj(self.username, self.password, self.user_data_dir)
        if status:
            previous = self.browser
            self.browser = login_obj  # type: ignore[assignment]
            if previous is not None and previous is not login_obj:
                try:
                    await previous.close()
                except Exception:  # pragma: no cover
                    pass
            self.state.SSID = self.session_data.get("token")
            self.is_logged = True
            if (
                "cookies" in self.session_data
                and self.session_data["cookies"]
                and self.browser._client is not None
            ):
                cookie_str = self.session_data["cookies"]
                for item in cookie_str.split("; "):
                    if "=" in item:
                        k, v = item.split("=", 1)
                        self.browser._client.cookies.set(k, v, domain=self.host)

            self.browser.headers.update(
                {
                    "User-Agent": self.session_data.get("user_agent", ""),
                    "Referer": f"{self.https_url}/{self.lang}/trade",
                    "Cookie": self.session_data.get("cookies", ""),
                }
            )
        else:
            await login_obj.close()
        return status, msg

    async def send_http_request_v1(self, method: str, url: str, **kwargs: Any) -> Any:
        """Sends an HTTP request using the internal browser client (v1)."""
        return await self.browser.send_request(method, url, **kwargs)

    async def send_http_request_v2(self, method: str, url: str, **kwargs: Any) -> Any:
        """Sends an HTTP request using the internal browser client (v2)."""
        return await self.browser.send_request(method, url, **kwargs)

    async def send_websocket_request(self, data: str) -> None:
        """Sends a raw string request to the WebSocket."""
        async with self._ws_send_lock:
            ws = self.websocket
            if ws is None:
                raise ConnectionError("WebSocket não inicializado")
            await ws.send(data)

    async def check_connect(self) -> bool:
        """Checks if the WebSocket is currently connected."""
        return self.state.status == WebsocketStatus.CONNECTED

    async def settings_apply(
        self,
        asset: str,
        period: int,
        is_fast_option: bool = False,
        end_time: int | None = None,
        deal: float | int = 5,
        percent_mode: bool = False,
        percent_deal: float | int = 1,
    ) -> None:
        """Apply asset and time settings before placing an order."""
        payload: dict[str, Any] = {
            "chartId": "graph",
            "settings": {
                "chartId": "graph",
                "chartType": 2,
                "currentExpirationTime": int(time.time()) if not is_fast_option else end_time,
                "isFastOption": is_fast_option,
                "isFastAmountOption": percent_mode,
                "isIndicatorsMinimized": False,
                "isIndicatorsShowing": True,
                "isShortBetElement": False,
                "chartPeriod": 4,
                "currentAsset": {"symbol": asset},
                "dealValue": deal,
                "dealPercentValue": percent_deal,
                "isVisible": True,
                "timePeriod": period,
                "gridOpacity": 8,
                "isAutoScrolling": 1,
                "isOneClickTrade": True,
                "upColor": "#0FAF59",
                "downColor": "#FF6251",
            },
        }
        if end_time:
            payload["endTime"] = end_time

        data = f'42["settings/apply", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def subscribe_realtime_candle(self, asset: str, period: int) -> None:
        """Subscribes to real-time price updates for a specific asset and period."""
        payload = {"asset": asset, "period": period}
        data = f'42["instruments/update", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def chart_notification(self, asset: str) -> None:
        """Requests chart notifications for a specific asset."""
        payload = {"asset": asset, "version": "1.0.0"}
        payload_json = json.dumps_str(payload)
        data = f'42["chart_notification/get", {payload_json}]'
        await self.send_websocket_request(data)

    async def follow_candle(self, asset: str) -> None:
        """Starts following the depth of market for a specific asset."""
        data = f'42["depth/follow", {json.dumps_str(asset)}]'
        await self.send_websocket_request(data)

    async def unfollow_candle(self, asset: str) -> None:
        """Stops following the depth of market for a specific asset."""
        data = f'42["depth/unfollow", {json.dumps_str(asset)}]'
        await self.send_websocket_request(data)

    async def signals_subscribe(self) -> None:
        """Subscribes to real-time trading signals from the platform."""
        await self.send_websocket_request('42["signal/subscribe"]')

    async def change_account(self, account_type: AccountType, tournament_id: int = 0) -> None:
        """Change active trading account."""
        self.account_type = account_type
        self.tournament_id = tournament_id

        payload = {"demo": int(account_type), "tournamentId": tournament_id}
        data = f'42["account/change",{json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def edit_training_balance(self, amount: float | int) -> None:
        """Refills the demo account balance."""
        data = f'42["demo/refill",{json.dumps_str(amount)}]'
        await self.send_websocket_request(data)

    async def change_time_offset(self, time_offset: int) -> dict[str, Any]:
        """Changes the account time offset."""
        return await self.settings.set_time_offset(time_offset)

    async def unsubscribe_realtime_candle(self, asset: str) -> None:
        """Unsubscribes from real-time price updates for a specific asset."""
        payload = {"asset": asset}
        data = f'42["instruments/unsubscribe", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def subscribe_Traders_mood(self, asset: str, instrument: str) -> None:
        """Subscribes to traders' mood/sentiment for a specific asset."""
        payload = {"asset": asset, "instrument": instrument}
        data = f'42["sentiment/subscribe", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def subscribe_all_size(self, asset: str) -> None:
        """Subscribes to all candle sizes for a specific asset."""
        payload = {"asset": asset}
        data = f'42["history/subscribe_all", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def get_history_line(self, asset: str, index: int, time_from: float, offset: int) -> None:
        """Requests historical price line data."""
        payload = {"asset": asset, "index": index, "time": time_from, "offset": offset}
        data = f'42["history/load", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def open_pending(
        self, amount: float | int, asset: str, direction: str, duration: int, open_time: int
    ) -> None:
        """Places a pending order to be executed at a specific future time."""
        payload = {
            "asset": asset,
            "amount": amount,
            "action": direction,
            "time": duration,
            "openTime": open_time,
            "isDemo": int(self.account_type) if self.account_type is not None else AccountType.DEMO,
            "tournamentId": self.tournament_id,
            "requestId": int(time.time()),
        }
        data = f'42["pending/create", {json.dumps_str(payload)}]'
        await self.send_websocket_request(data)

    async def instruments_follow(
        self, amount: float | int, asset: str, direction: str, duration: int, open_time: int
    ) -> None:
        """Alias for open_pending or similar follow request."""
        await self.open_pending(amount, asset, direction, duration, open_time)

    async def start_websocket(self) -> tuple[bool, str]:
        """Initializes and starts the WebSocket connection."""
        self.state.status = WebsocketStatus.CONNECTING
        self.state.auth_status = AuthStatus.NOT_AUTHENTICATED
        await self.event_registry.set_event("status_changed", self.state.status)
        if not self.state.SSID:
            status, msg = await self.authenticate()
            if not status:
                return False, f"Authentication failed: {msg}"

        self.websocket_client = WebsocketClient(self, reconnect_policy=self.reconnect_policy)

        ua = self.session_data.get("user_agent") or USER_AGENT_DEFAULT

        self.browser.headers.update(
            {
                "User-Agent": ua,
                "Referer": f"{self.https_url}/{self.lang}/trade",
                "Cookie": self.session_data.get("cookies", ""),
            }
        )

        extra_headers = {
            "User-Agent": ua,
            "Origin": self.https_url,
            "Referer": f"{self.https_url}/{self.lang}/trade",
            "Cookie": self.session_data.get("cookies", ""),
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        ssl_ctx = self.browser._ssl_context if self.wss_url.startswith("wss://") else None
        self._websocket_task = asyncio.create_task(
            self.websocket_client.run_forever(
                url=self.wss_url,
                extra_headers=extra_headers,
                ssl=ssl_ctx,
            )
        )
        # Handshake (até 10 s) + autorização: 20 s no total.
        for _ in range(200):
            if self.state.auth_status == AuthStatus.FAILED:
                await self._stop_websocket()
                return False, self.state.websocket_error_reason or "Authorization rejected"
            if (
                self.state.status == WebsocketStatus.CONNECTED
                and self.state.auth_status == AuthStatus.AUTHENTICATED
            ):
                return True, "Connected"
            if self._websocket_task is not None and self._websocket_task.done():
                break
            await asyncio.sleep(0.1)
        reason = self.state.websocket_error_reason or "Timeout waiting for authorization"
        await self._stop_websocket()
        return False, reason

    async def _stop_websocket(self) -> None:
        """Encerra o cliente WS e o loop de reconexão (sem fechar a sessão HTTP)."""
        if self.websocket_client is not None:
            await self.websocket_client.close()
        task = self._websocket_task
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(task, timeout=5)
            except (asyncio.TimeoutError, Exception):
                task.cancel()

    async def _close_ws_quietly(self) -> None:
        ws_client = self.websocket_client
        if ws_client is not None and ws_client._ws is not None:
            try:
                await ws_client._ws.close()
            except Exception:
                pass

    def _invalidate_session(self) -> None:
        """Apaga o token rejeitado da memória e do disco (session.json)."""
        self.state.SSID = None
        if self.session_data.get("token"):
            self.session_data["token"] = None
            try:
                update_session(self.username, self.session_data)
            except Exception as e:  # pragma: no cover - erro de disco
                logger.warning("Não foi possível atualizar session.json: %s", e)

    async def _reauthenticate(self) -> tuple[bool, str]:
        """Novo login HTTP para obter SSID válido (usado pelo loop de reconexão)."""
        self._invalidate_session()
        logger.info("Fazendo novo login para renovar o SSID...")
        ok, msg = await self.authenticate()
        if ok:
            self.state.auth_status = AuthStatus.NOT_AUTHENTICATED
            self.state.websocket_error_reason = None
        return ok, msg

    async def send_ssid(self) -> bool:
        """Sends the SSID token to the WebSocket to authorize the connection."""
        if not self.state.SSID:
            return False
        await self.ssid(self.state.SSID)
        return True

    async def connect(self, is_demo: bool) -> tuple[bool, str]:
        """Connects to the Quotex platform."""
        self.account_type = AccountType.DEMO if is_demo else AccountType.REAL
        ok, reason = await self.start_websocket()
        if ok and self.state.auth_status != AuthStatus.AUTHENTICATED and not self.state.SSID:
            await self.send_ssid()
        return ok, reason

    async def close(self) -> bool:
        """Closes the WebSocket connection and the HTTP client session."""
        if self.websocket_client:
            await self._stop_websocket()
            self._on_close(1000, "Graceful closure")
        if self.browser:
            await self.browser.close()
        try:
            await self.settings.close()
        except Exception:  # pragma: no cover
            pass
        return True

    @property
    def logout(self) -> Logout:
        """Returns the Logout action handler."""
        return Logout(self)

    @property
    def login(self) -> Login:
        """Returns the Login action handler."""
        return Login(self)

    @property
    def ssid(self) -> Ssid:
        """Returns the SSID authorization handler."""
        return Ssid(self)

    @property
    def buy(self) -> Buy:
        """Returns the Buy order handler."""
        return Buy(self)

    @property
    def sell_option(self) -> SellOption:
        """Returns the Sell Option handler."""
        return SellOption(self)

    @property
    def get_candles(self) -> GetCandles:
        """Returns the Candles retrieval handler."""
        return GetCandles(self)

    @property
    def get_history(self) -> GetHistory:
        """Returns the Trade History retrieval handler."""
        return GetHistory(self)

    async def get_profile(self) -> Profile:
        """Retrieves and parses the user profile data."""
        user_settings = await self.settings.get_settings()
        d = user_settings.get("data", {})
        self.profile.nick_name = d.get("nickname")
        self.profile.profile_id = d.get("id")
        self.profile.demo_balance = float(d.get("demoBalance", 0))
        self.profile.live_balance = float(d.get("liveBalance", 0))
        self.profile.currency_code = d.get("currencyCode")
        self.profile.currency_symbol = d.get("currencySymbol")
        self.profile.country_name = d.get("countryName")
        self.profile.offset = d.get("timeOffset")
        return self.profile

    async def get_trader_history(self, account_type: int, page: int) -> dict[str, Any]:
        """Retrieves the trade history for a specific account and page."""
        history = await self.get_history(account_type, page)
        return history.get("data", {})
