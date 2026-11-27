"""pyquotex CLI entry point. Run with `python -m pyquotex` or via app.py."""

import asyncio
import sys

from rich.console import Console

from pyquotex.cli.commands import COMMAND_REGISTRY
from pyquotex.cli.parser import make_parser
from pyquotex.cli.runtime import on_otp
from pyquotex.config import credentials
from pyquotex.stable_api import Quotex

console = Console()


async def main() -> None:
    parser = make_parser()
    args = parser.parse_args()

    if not getattr(args, "command", None):
        parser.print_help()
        return

    email, password = credentials()
    client = Quotex(
        email=email,
        password=password,
        on_otp_callback=on_otp,
    )

    handler = COMMAND_REGISTRY.get(args.command)
    if handler is None:
        console.print(f"[red]Unknown command: {args.command}[/]")
        parser.print_help()
        sys.exit(2)

    try:
        await handler(client, args)
    finally:
        try:
            await client.close()
        except Exception:
            pass  # best-effort cleanup


def _force_utf8_stdio() -> None:
    """Console do Windows (cp1252) não imprime emoji e derruba a CLI."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, OSError):  # pragma: no cover
                pass


def cli_main() -> None:
    _force_utf8_stdio()
    asyncio.run(main())


if __name__ == "__main__":
    cli_main()
