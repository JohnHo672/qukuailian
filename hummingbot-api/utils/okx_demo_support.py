"""Register isolated OKX demo connectors for Hummingbot Core 20260920.

OKX uses the production REST hostname for demo trading, but requires the
``x-simulated-trading: 1`` header on authenticated requests and separate
``wspap`` websocket endpoints.  The installed core already contains most of
the perpetual-demo URL table, but does not register demo connectors and does
not add the required REST header.  Keep the compatibility shim here, in the
API image, so live connectors remain untouched and upstream drift fails fast.
"""

from __future__ import annotations

from decimal import Decimal
from importlib.metadata import version
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.client.settings import AllConnectorSettings


SUPPORTED_HUMMINGBOT_VERSION = "20260920"
SPOT_DEMO_CONNECTOR = "okx_demo"
PERPETUAL_DEMO_CONNECTOR = "okx_perpetual_demo"

_installed = False


class OkxDemoConfigMap(BaseConnectorConfigMap):
    connector: str = SPOT_DEMO_CONNECTOR
    okx_demo_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the API key created in OKX Demo Trading",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    okx_demo_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the secret key created in OKX Demo Trading",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    okx_demo_passphrase: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the passphrase for this OKX Demo Trading key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="okx")


class OkxPerpetualDemoConfigMap(BaseConnectorConfigMap):
    connector: str = PERPETUAL_DEMO_CONNECTOR
    okx_perpetual_demo_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the API key created in OKX Demo Trading",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    okx_perpetual_demo_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the secret key created in OKX Demo Trading",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    okx_perpetual_demo_passphrase: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the passphrase for this OKX Demo Trading key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="okx_perpetual")


def _require_supported_core() -> None:
    installed = version("hummingbot")
    if installed != SUPPORTED_HUMMINGBOT_VERSION:
        raise RuntimeError(
            "OKX demo connector compatibility check failed: "
            f"expected hummingbot {SUPPORTED_HUMMINGBOT_VERSION}, found {installed}. "
            "Review the demo shim before upgrading the core package."
        )


def _patch_spot_connector() -> None:
    from hummingbot.connector.exchange.okx import okx_constants as constants
    from hummingbot.connector.exchange.okx.okx_auth import OkxAuth
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange

    original_get_base_url = constants.get_okx_base_url
    original_get_ws_url = constants.get_ws_url
    original_auth_init = OkxAuth.__init__
    original_authentication_headers = OkxAuth.authentication_headers
    original_exchange_init = OkxExchange.__init__

    def get_okx_base_url(sub_domain: str) -> str:
        if sub_domain == "demo":
            return "https://www.okx.com/"
        return original_get_base_url(sub_domain)

    def get_ws_url(sub_domain: str) -> str:
        if sub_domain == "demo":
            return "wss://wspap.okx.com:8443"
        return original_get_ws_url(sub_domain)

    def auth_init(
        self,
        api_key: str,
        secret_key: str,
        passphrase: str,
        time_provider,
        simulated: bool = False,
    ) -> None:
        original_auth_init(self, api_key, secret_key, passphrase, time_provider)
        self._simulated = simulated

    def authentication_headers(self, request) -> Dict[str, Any]:
        headers = original_authentication_headers(self, request)
        if self._simulated:
            headers["x-simulated-trading"] = "1"
        return headers

    def exchange_init(
        self,
        okx_api_key: str,
        okx_secret_key: str,
        okx_passphrase: str,
        balance_asset_limit=None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs=None,
        trading_required: bool = True,
        okx_registration_sub_domain: str = "www",
        domain: str | None = None,
    ) -> None:
        registration_sub_domain = "demo" if domain == "demo" else okx_registration_sub_domain
        original_exchange_init(
            self,
            okx_api_key=okx_api_key,
            okx_secret_key=okx_secret_key,
            okx_passphrase=okx_passphrase,
            balance_asset_limit=balance_asset_limit,
            rate_limits_share_pct=rate_limits_share_pct,
            trading_pairs=trading_pairs,
            trading_required=trading_required,
            okx_registration_sub_domain=registration_sub_domain,
        )

    def authenticator(self):
        return OkxAuth(
            api_key=self.okx_api_key,
            secret_key=self.okx_secret_key,
            passphrase=self.okx_passphrase,
            time_provider=self._time_synchronizer,
            simulated=self.okx_registration_sub_domain == "demo",
        )

    constants.get_okx_base_url = get_okx_base_url
    constants.get_ws_url = get_ws_url
    OkxAuth.__init__ = auth_init
    OkxAuth.authentication_headers = authentication_headers
    OkxExchange.__init__ = exchange_init
    OkxExchange.authenticator = property(authenticator)


def _patch_perpetual_connector() -> None:
    from hummingbot.connector.derivative.okx_perpetual import okx_perpetual_constants as constants
    from hummingbot.connector.derivative.okx_perpetual import okx_perpetual_web_utils as web_utils
    from hummingbot.connector.derivative.okx_perpetual.okx_perpetual_api_order_book_data_source import (
        OkxPerpetualAPIOrderBookDataSource,
    )
    from hummingbot.connector.derivative.okx_perpetual.okx_perpetual_auth import OkxPerpetualAuth
    from hummingbot.connector.derivative.okx_perpetual.okx_perpetual_derivative import OkxPerpetualDerivative

    original_auth_init = OkxPerpetualAuth.__init__
    original_authentication_headers = OkxPerpetualAuth.authentication_headers

    def auth_init(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        time_provider,
        simulated: bool = False,
    ) -> None:
        original_auth_init(self, api_key, api_secret, passphrase, time_provider)
        self._simulated = simulated

    def authentication_headers(self, request) -> Dict[str, Any]:
        headers = original_authentication_headers(self, request)
        if self._simulated:
            headers["x-simulated-trading"] = "1"
        return headers

    def authenticator(self):
        return OkxPerpetualAuth(
            self.okx_perpetual_api_key,
            self.okx_perpetual_secret_key,
            self.okx_perpetual_passphrase,
            self._time_synchronizer,
            simulated=self._domain == constants.DEMO_DOMAIN,
        )

    async def connected_websocket_assistant(self):
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.wss_linear_public_url(self._domain),
            message_timeout=constants.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
        )
        return ws

    OkxPerpetualAuth.__init__ = auth_init
    OkxPerpetualAuth.authentication_headers = authentication_headers
    OkxPerpetualDerivative.authenticator = property(authenticator)
    OkxPerpetualAPIOrderBookDataSource._connected_websocket_assistant = connected_websocket_assistant


def _register_connector_variants() -> None:
    settings = AllConnectorSettings.get_connector_settings()

    spot = settings["okx"]
    settings[SPOT_DEMO_CONNECTOR] = spot._replace(
        name=SPOT_DEMO_CONNECTOR,
        config_keys=OkxDemoConfigMap.model_construct(),
        is_sub_domain=True,
        parent_name="okx",
        domain_parameter="demo",
    )

    perpetual = settings["okx_perpetual"]
    settings[PERPETUAL_DEMO_CONNECTOR] = perpetual._replace(
        name=PERPETUAL_DEMO_CONNECTOR,
        config_keys=OkxPerpetualDemoConfigMap.model_construct(),
        is_sub_domain=True,
        parent_name="okx_perpetual",
        domain_parameter="okx_perpetual_demo",
    )


def install_okx_demo_support() -> None:
    """Install the version-gated demo variants exactly once."""
    global _installed
    if _installed:
        return
    _require_supported_core()
    _patch_spot_connector()
    _patch_perpetual_connector()
    _register_connector_variants()
    _installed = True

