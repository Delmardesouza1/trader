import configparser
import getpass
import json
import os
import sys
import threading
from pathlib import Path
from typing import Any

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

base_dir = Path.cwd()
# Arquivo legado. Não é mais criado: a senha nunca é gravada em disco.
config_path = Path(os.path.join(base_dir, "settings/config.ini"))
config = configparser.ConfigParser(interpolation=None)

session_lock = threading.Lock()

# Chaves de metadados que podem aparecer soltas no topo de session.json (estrutura inconsistente).
_SESSION_META_KEYS = {"cookies", "token", "user_agent"}


def write_private_file(path: Path, text: str) -> None:
    """Grava um arquivo sensível de forma atômica e restringe a permissão.

    Escreve num arquivo temporário e troca com ``os.replace``: uma queda no meio
    da escrita nunca deixa ``session.json`` corrompido (o que apagaria a sessão).
    A permissão 0600 tem efeito pleno em Linux, Termux e macOS.
    """
    path.parent.mkdir(exist_ok=True, parents=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def credentials() -> tuple[str, str]:
    """
    Obtém e-mail e senha, nesta ordem:
    1. variáveis de ambiente QUOTEX_EMAIL e QUOTEX_PASSWORD;
    2. settings/config.ini, somente se já existir (legado, com aviso);
    3. prompt interativo (a senha não aparece na tela).
    Nunca grava a senha em disco.
    """
    email = os.getenv("QUOTEX_EMAIL", "").strip()
    password = os.getenv("QUOTEX_PASSWORD", "")

    if (not email or not password) and config_path.exists():
        config.read(config_path, encoding="utf-8")
        email = email or config.get("settings", "email", fallback="").strip()
        legacy_password = config.get("settings", "password", fallback="")
        if legacy_password and not password:
            password = legacy_password
            print(
                "Aviso: senha lida de settings/config.ini (formato antigo). "
                "Migre para QUOTEX_PASSWORD e apague esse arquivo."
            )

    if not email:
        email = input("E-mail da conta Quotex: ").strip()
    if not password:
        password = getpass.getpass("Senha da conta Quotex: ")

    if not email or not password:
        print("E-mail e senha não podem ficar em branco.")
        sys.exit(1)

    return email, password


def host() -> str:
    """Get the broker host configured in settings/config.ini."""
    if not config_path.exists():
        return "qxbroker.io"

    config.read(config_path, encoding="utf-8")
    return config.get("settings", "host", fallback="qxbroker.io").strip()


def resource_path(relative_path: str | Path) -> Path:
    """Get absolute path to resource, works for dev and for PyInstaller"""
    global base_dir
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base_dir = Path(getattr(sys, "_MEIPASS"))  # atributo só existe no PyInstaller
    return base_dir / relative_path


def load_all_sessions() -> dict[str, Any]:
    """Lê session.json e descarta chaves soltas de metadados no topo."""
    output_file = Path(resource_path("session.json"))
    if not output_file.exists():
        return {}
    try:
        data = json.loads(output_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        # Não sobrescreve um arquivo ilegível em silêncio: guarda uma cópia.
        backup = output_file.with_suffix(".json.corrupt")
        try:
            output_file.replace(backup)
        except OSError:
            pass
        print(f"Aviso: session.json ilegível; cópia salva em {backup.name}.")
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if k not in _SESSION_META_KEYS and isinstance(v, dict)}


def load_session(email: str, user_agent: str | None = None) -> dict[str, Any]:
    """Load session data for a specific email."""
    if user_agent is None:
        user_agent = USER_AGENT

    output_file = Path(resource_path("session.json"))
    with session_lock:
        all_sessions = load_all_sessions()
        if email not in all_sessions:
            all_sessions[email] = {"cookies": None, "token": None, "user_agent": user_agent}
            write_private_file(output_file, json.dumps(all_sessions, indent=4))

        return all_sessions[email]


def update_session(email: str, d: dict[str, Any]) -> dict[str, Any]:
    """Update and persist session data for a specific email."""
    output_file = Path(resource_path("session.json"))
    with session_lock:
        current_sessions = load_all_sessions()
        current_sessions[email] = d
        write_private_file(output_file, json.dumps(current_sessions, indent=4))
        return current_sessions[email]
