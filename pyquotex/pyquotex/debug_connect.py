import asyncio
import logging
import sys
import traceback

from pyquotex.config import credentials
from pyquotex.qxtypes import ReconnectPolicy
from pyquotex.stable_api import Quotex

# Evita "RuntimeError: Event loop is closed" ao encerrar WebSockets no Windows
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main() -> None:
    # Credenciais via QUOTEX_EMAIL/QUOTEX_PASSWORD (ou prompt); nunca no código.
    email, password = credentials()

    client = Quotex(
        email=email,
        password=password,
        lang="en",  # Evita HTTP 405 na rota localizada /pt/sign-in/
        reconnect_policy=ReconnectPolicy(enabled=False),
    )

    try:
        ok, msg = await client.connect()
        print("\n=== RESULTADO ===")
        print(f"Conectou: {ok}")
        print(f"Mensagem: {msg}")

        if ok:
            # Define conta Demo ("PRACTICE") ou Real ("REAL")
            await client.change_account("PRACTICE")

            api = getattr(client, "api", None)
            state = getattr(api, "state", None)
            if state is not None:
                print(f"Auth status: {getattr(state, 'auth_status', 'N/A')}")
                print(f"WS status: {getattr(state, 'status', 'N/A')}")

            balance = await client.get_balance()
            print(f"Saldo (Demo): {balance}")

    except Exception as e:
        print("\n=== ERRO ===")
        print(f"{type(e).__name__}: {e}")
        traceback.print_exc()

    finally:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            res = close_fn()
            if asyncio.iscoroutine(res):
                await res


if __name__ == "__main__":
    asyncio.run(main())
