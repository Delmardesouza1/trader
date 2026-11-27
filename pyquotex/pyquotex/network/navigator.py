"""Async HTTP browser client using curl_cffi for Quotex API communication."""
import logging
from typing import Any, Literal, Optional, cast

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession, Response
from typing_extensions import Self

logger = logging.getLogger("Browser")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)

from pyquotex.network.ssl_utils import (
    create_ssl_context,
    CIPHER_SUITE_CHROME
)

USER_AGENT_DEFAULT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def impersonate_for(user_agent: str) -> str:
    """Escolhe o perfil TLS do curl_cffi compatível com o User-Agent."""
    return "firefox133" if "Firefox/" in (user_agent or "") else "chrome120"

# Compatibilidade: adiciona is_success / is_redirect à Response do curl_cffi, se ausentes.
# setattr evita o erro de atribuição estática no type checker; o comportamento é o mesmo.
if not hasattr(Response, "is_success"):
    setattr(Response, "is_success", property(lambda self: 200 <= self.status_code < 300))
if not hasattr(Response, "is_redirect"):
    setattr(
        Response,
        "is_redirect",
        property(
            lambda self: 300 <= self.status_code < 400 or self.status_code in (301, 302, 303, 307, 308)
        ),
    )


# Mesmos métodos aceitos pelo curl_cffi (tipo HttpMethod da biblioteca).
HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH", "QUERY"]
_HTTP_METHODS: frozenset[str] = frozenset({"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH", "QUERY"})

_SENSITIVE_HEADERS = {"cookie", "set-cookie", "authorization", "proxy-authorization"}


def _redact_headers(headers: Any) -> dict[str, str]:
    """Remove os valores de cabeçalhos sensíveis antes de registrar em log."""
    return {
        k: ("<redigido>" if str(k).lower() in _SENSITIVE_HEADERS else v)
        for k, v in dict(headers).items()
    }


class Browser:
    """Async HTTP client wrapping curl_cffi.requests.AsyncSession with TLS impersonation, 
    cookies, and proxy support."""

    def __init__(self, *args: Any, **kwargs: Any):
        self.response: Optional[Response] = None
        self.default_headers: Optional[dict[str, str]] = None
        self.source_address: Any = kwargs.pop('source_address', None)
        self.server_hostname: Optional[str] = kwargs.pop('server_hostname', None)
        self.proxies: Optional[dict[str, str] | str] = kwargs.pop('proxies', None)
        self.debug: bool = kwargs.pop('debug', False)

        # Build SSL context
        self._ssl_context = create_ssl_context(cipher_suite=CIPHER_SUITE_CHROME)

        # server_hostname é ignorado de propósito: a verificação de hostname permanece ativa.

        self.headers = self.get_headers() or {}

        self._client: Optional[AsyncSession] = None

        if self.debug:
            logger.setLevel(logging.DEBUG)

    async def ensure_client(self) -> AsyncSession:
        """Ensures that a curl_cffi AsyncSession instance is available and configured."""
        if self._client is None or getattr(self._client, "_closed", False) is True or getattr(self._client, "is_closed",
                                                                                              False) is True:
            client_kwargs: dict[str, Any] = {
                "headers": self.headers,
                "timeout": 30.0,
                "allow_redirects": True,
                # O fingerprint TLS precisa bater com o User-Agent enviado;
                # UA Firefox com TLS Chrome é bloqueado pelo Cloudflare.
                "impersonate": impersonate_for(self.headers.get("User-Agent", "")),
                # curl_cffi uses 'verify=False' for insecure requests, but we manage our own ssl context in websocket.
                # For HTTP, curl_cffi's impersonate handles the TLS handshake automatically.
            }

            if self.proxies:
                if isinstance(self.proxies, str):
                    client_kwargs["proxies"] = {"http": self.proxies, "https": self.proxies}
                else:
                    client_kwargs["proxies"] = self.proxies

            cookie_header = self.headers.get("Cookie")
            if cookie_header:
                cookies_dict = {}
                for item in cookie_header.split(";"):
                    if "=" in item:
                        k, v = item.strip().split("=", 1)
                        if k.strip():
                            cookies_dict[k.strip()] = v.strip()
                client_kwargs["cookies"] = cookies_dict

            self._client = AsyncSession(**client_kwargs)

        return self._client

    def __enter__(self) -> Self:
        return self

    def __exit__(
            self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None:
        pass

    async def __aenter__(self) -> Self:
        await self.ensure_client()
        return self

    async def __aexit__(
            self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying client."""
        if self._client and not getattr(self._client, "_closed", False):
            try:
                await self._client.close()
            except Exception as e:
                logger.debug(f"Error closing AsyncSession: {e}")
            finally:
                self._client = None

    def get_headers(self) -> dict[str, str]:
        self.default_headers = {
            "User-Agent": USER_AGENT_DEFAULT,
        }
        return self.default_headers

    def set_headers(self, headers: dict[str, str] | None = None) -> None:
        if self.default_headers:
            self.headers.update(self.default_headers)
        if headers:
            cleaned_headers = {k: v for k, v in headers.items() if v is not None}
            self.headers.update(cleaned_headers)

        if self._client and not getattr(self._client, "_closed", False):
            self._client.headers.update(self.headers)

    def get_cookies(self) -> str:
        """Get cookies as semicolon-separated string from the client jar."""
        if not self._client:
            return ""
        return '; '.join(
            f'{cookie.name}={cookie.value}'
            for cookie in self._client.cookies.jar
        )

    def is_cloudflare_challenge(self) -> bool:
        """True when the last response is a Cloudflare JS/managed challenge."""
        if self.response is None or self.response.status_code not in (403, 503):
            return False
        if self.response.headers.get("cf-mitigated") == "challenge":
            return True
        return "Just a moment" in (self.response.text or "")[:2048]

    def _raise_for_error(self) -> None:
        if self.response is None or self.response.status_code < 400:
            return
        if self.is_cloudflare_challenge():
            raise RuntimeError(
                f"HTTP {self.response.status_code}: blocked by Cloudflare "
                f"challenge at {self.response.url}. Your IP may be temporarily "
                "flagged; wait a few minutes or use a different IP/proxy."
            )
        raise RuntimeError(
            f"HTTP {self.response.status_code}: "
            f"{self.response.reason or ''} ({self.response.url})"
        )

    def get_soup(self) -> BeautifulSoup:
        """Parse the last response content with BeautifulSoup."""
        self._raise_for_error()
        return BeautifulSoup(
            self.response.content if self.response else b"",
            "html.parser"
        )

    def get_json(self) -> Any:
        """Parse the last response as JSON."""
        self._raise_for_error()
        try:
            return self.response.json() if self.response else None
        except Exception:
            return None

    async def send_request(
            self,
            method: str,
            url: str,
            headers: dict[str, str] | None = None,
            **kwargs: Any
    ) -> Response:
        """Send an async HTTP request using curl_cffi.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Target URL
            headers: Optional additional headers
            **kwargs: Additional request arguments (data, json, params, etc.)

        Returns:
            curl_cffi.requests.Response object
        """
        client = await self.ensure_client()

        merged_headers = self.headers.copy() if self.headers else {}
        if headers:
            merged_headers.update(headers)

        if self.debug:
            logger.debug("Using proxies: %s", self.proxies)

        method_str = method.upper()
        if method_str not in _HTTP_METHODS:
            raise ValueError(f"Método HTTP não suportado: {method}")
        method_typed = cast(HttpMethod, method_str)

        # Adapt redirect kwargs syntax
        if "follow_redirects" in kwargs:
            kwargs["allow_redirects"] = kwargs.pop("follow_redirects")

        self.response = await client.request(
            method_typed,
            url,
            headers=merged_headers,
            **kwargs,
        )

        if self.debug:
            logger.debug(f"→ {method} {url}")
            logger.debug(f"Status: {self.response.status_code}")
            logger.debug("Headers enviados: %s", _redact_headers(merged_headers))
            logger.debug("Headers recebidos: %s", _redact_headers(self.response.headers))
            cookie_names = sorted({c.split("=", 1)[0] for c in self.get_cookies().split("; ") if c})
            logger.debug("Cookies (nomes): %s", cookie_names)
            content_preview = (
                self.response.text[:250].strip().replace('\n', '')
            )
            logger.debug(f"Body (preview): {content_preview} [...]")

        return self.response
