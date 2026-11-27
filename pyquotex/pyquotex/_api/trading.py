"""Trading-related methods extracted from Quotex.

This mixin is composed into Quotex via multiple inheritance. It uses
self.api, self.account_is_demo, etc. — all set up in Quotex.__init__
inside pyquotex/stable_api.py.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from pyquotex import expiration
from pyquotex._api._constants import DEFAULT_TIMEOUT
from pyquotex._api._waits import wait_until
from pyquotex.utils.account_type import AccountType

logger = logging.getLogger(__name__)

BUY_PRICE_WAIT = 5        # s esperando ticks do ativo antes de enviar a ordem
BUY_CONFIRM_TIMEOUT = 15  # s esperando a confirmação (id) da ordem


class TradingMixin:
    """Methods related to placing trades and reading their results."""

    async def buy(
            self, amount: float, asset: str, direction: str, duration: int, time_mode: str = "TIME"
    ) -> tuple[bool, Any]:
        """
        Places a buy order for a specified asset, direction, and duration.
        Waits for WebSocket confirmation of the buy and returns the result.
        """
        if self.api is None:
            return False, "API not initialized"

        self.api.buy_id = None
        self.api.buy_successful = None
        request_id = expiration.get_timestamp()
        is_fast_option = time_mode.upper() == "TIME"

        # Clear slot state before requesting buy to prevent
        # race with WS response
        self.api.slots.buy_confirm.clear()

        # Garante fluxo de preço (sem travar a ordem: no máximo BUY_PRICE_WAIT s)
        # e sincroniza o relógio. settings_apply é enviado pelo canal Buy.
        try:
            await self.start_realtime_price(asset, duration, timeout=BUY_PRICE_WAIT)
        except TimeoutError:
            logger.warning("Sem ticks de %s em %ss; enviando a ordem mesmo assim.", asset, BUY_PRICE_WAIT)
        try:
            # Só atualiza timesync (exibição); a expiração usa o relógio local.
            await self.get_server_time()
        except Exception as e:
            logger.warning("Falha ao sincronizar horário do servidor: %s", e)

        try:
            await self.api.buy(
                amount, asset, direction, duration, request_id, is_fast_option, time_mode
            )
        except ConnectionError as e:
            # A ordem não saiu da máquina: falha certa, não "timeout".
            return False, f"NotSent: {e}"

        # A confirmação chega em poucos segundos; esperar a duração inteira
        # bloqueia o chamador sem motivo.
        timeout = BUY_CONFIRM_TIMEOUT

        if self.api.buy_id is None:
            try:
                event_data = await self.api.slots.buy_confirm.wait(timeout=timeout)
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for buy confirmation.")
                return False, "Timeout"
        else:
            event_data = {"id": self.api.buy_id}

        if self.api.state.check_websocket_if_error:
            return False, self.api.state.websocket_error_reason

        if event_data and isinstance(event_data, dict) and "error" in event_data:
            return False, event_data["error"]

        return True, event_data

    async def open_pending(
            self, amount: float, asset: str, direction: str, duration: int, open_time: str | None = None
    ) -> tuple[bool, Any]:
        """Places a pending order to be executed at a specific future time."""
        if self.api is None:
            return False, "API not initialized"

        self.api.pending_id = None
        self.api.slots.pending_confirm.clear()
        user_settings = await self.get_profile()
        offset_zone = user_settings.offset if user_settings else 0
        open_time_int = int(
            expiration.get_next_timeframe(int(time.time()), offset_zone, duration, open_time)
        )
        await self.api.open_pending(amount, asset, direction, duration, open_time_int)
        if self.api.pending_id is None:
            try:
                await self.api.slots.pending_confirm.wait(timeout=DEFAULT_TIMEOUT)
            except asyncio.TimeoutError:
                logger.error("Timeout pending order.")
                return False, "Timeout waiting for pending ID"

        if self.api.state.check_websocket_if_error:
            return False, self.api.state.websocket_error_reason

        # pending_id was set (success path).
        status_buy = False
        if self.api.pending_id is not None:
            status_buy = True
            await self.api.instruments_follow(amount, asset, direction, duration, open_time_int)

        return status_buy, self.api.pending_successful

    async def sell_option(
            self, options_ids: list[str] | str, timeout: int = DEFAULT_TIMEOUT
    ) -> dict[str, Any]:
        """Sells active options back to the broker before expiration."""
        if self.api is None:
            raise RuntimeError("API not initialized")

        # Reset sentinel BEFORE sending the request — if the WS response
        # arrives before the next line, it must not be wiped out.
        self.api.sold_options_respond = None
        await self.api.sell_option(options_ids)
        # We don't yet have an identified WS event that populates
        # sold_options_respond, so we still settle via predicate-polling —
        # but through wait_until() (event-loop friendly, hard timeout,
        # configurable poll interval) instead of an ad-hoc while/sleep.
        try:
            await wait_until(
                lambda: self.api.sold_options_respond is not None,
                timeout=float(timeout),
                poll_interval=0.2,
            )
        except asyncio.TimeoutError:
            raise TimeoutError("Timeout waiting for sell option response.")
        return self.api.sold_options_respond

    async def check_win(
            self, order_id: str | int, duration: int = 0, timeout: float | None = None
    ) -> tuple[str, float]:
        """Checks if a trade operation resulted in a win based on its ID.

        Returns ``("unknown", 0.0)`` when the result does not arrive in time:
        a missing result is NOT a loss.
        """
        if self.api is None:
            return "unknown", 0.0
        if timeout is None:
            timeout = max(300.0, float(duration) + 60.0)

        # Fast path: result may already be cached (e.g. closed deal arrived
        # before this method was called).
        cached = self.api.listinfodata.get(order_id)
        if cached and cached.get("game_state") == 1:
            self.api.listinfodata.delete(order_id)
            return (
                cached.get("win", "loss"),
                float(cached.get("profit", 0)),
            )

        # Event-driven path: wait on the keyed win_result slot fired by
        # _on_message when the matching order closes.
        key = str(order_id)
        slot = self.api.slots.win_result(key)
        try:
            result = await slot.wait(timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Resultado da ordem %s não chegou em %.0fs.", order_id, timeout)
            return "unknown", 0.0
        finally:
            self.api.slots.release_win_result(key)

        # Clean up the listinfodata cache to match prior behavior.
        self.api.listinfodata.delete(order_id)
        self.api.listinfodata.delete(key)

        win = result.get("win", "loss") if result else "loss"
        profit = float(result.get("profit", 0)) if result else 0.0
        return win, profit

    async def get_result(self, operation_id: str) -> tuple[str | None, Any]:
        """Check if the trade is a win based on its ID."""
        data_history = await self.get_history()
        for item in data_history:
            if str(item.get("ticket")) == operation_id:
                profit = float(item.get("profitAmount", 0))
                status = "win" if profit > 0 else "loss"
                return status, item

        return None, "OperationID Not Found."

    def get_profit(self) -> float:
        """Retrieves the profit amount from the current active operation."""
        if self.api:
            return self.api.profit_in_operation or 0.0
        return 0.0

    async def get_history(self) -> list[dict[str, Any]]:
        """Get the trader's history based on account type."""
        if self.api is None:
            return []

        account_type = AccountType.DEMO if self.account_is_demo else AccountType.REAL
        history = await self.api.get_trader_history(account_type, page=1)
        return list(history)
