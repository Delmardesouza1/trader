import asyncio
import os
import time

from pyquotex.stable_api import Quotex


def load_credentials() -> tuple[str, str]:
    email = os.getenv("QUOTEX_EMAIL", "").strip()
    password = os.getenv("QUOTEX_PASSWORD", "")
    if not email or not password:
        raise SystemExit("Erro: Defina QUOTEX_EMAIL e QUOTEX_PASSWORD nas variáveis de ambiente.")
    return email, password


async def main() -> None:
    email, password = load_credentials()
    client = Quotex(
        email=email,
        password=password,
        lang="pt",
        host="quotex.com",  # Alternativas em caso de bloqueio: "qxbroker.io", "quotex.io"
    )

    try:
        print("Conectando à Quotex...")
        check, reason = await client.connect()
        if not check:
            print(f"Falha na conexão: {reason}")
            return

        print("✅ Conectado com sucesso!")

        # Correção: Passa a string literal esperada pelo type hint e pela lógica interna
        await client.change_account("PRACTICE")

        saldo = await client.get_balance()
        print(f"Saldo Demo: R$ {float(saldo):.2f}")

        asset = "AUDCHF_otc"
        current_time = int(time.time())
        
        print(f"Buscando histórico de candles para {asset}...")
        candles = await client.get_candles(
            asset=asset,
            end_from_time=current_time,  # Timestamp atual para dados recentes
            offset=6000,                 # 6000 segundos = ~100 candles de 1 minuto (period=60)
            period=60
        )

        if candles:
            print(f"✅ Sucesso: {len(candles)} candles carregados.")
            ultimo_candle = candles[-1]
            print(f"Último candle -> Abertura: {ultimo_candle.get('open')}, Fechamento: {ultimo_candle.get('close')}")
        else:
            print("⚠️ Nenhum candle retornado. O ativo pode estar fechado ou o nome do par está incorreto.")

    except Exception as e:
        print(f"❌ Ocorreu um erro durante a execução: {e}")
    finally:
        print("Encerrando conexão...")
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())