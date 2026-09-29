from decimal import Decimal

import pytest

from hummingbot.client.config.config_helpers import get_connector_class
from hummingbot.client.settings import AllConnectorSettings
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from utils.okx_demo_support import install_okx_demo_support


@pytest.fixture(scope="module", autouse=True)
def install_demo_connectors():
    install_okx_demo_support()


def _connector(name: str):
    setting = AllConnectorSettings.get_connector_settings()[name]
    fields = setting.config_keys.__class__.model_fields
    keys = {
        field: ("www" if field.endswith("registration_sub_domain") else "test-value")
        for field in fields
        if field != "connector"
    }
    params = setting.conn_init_parameters(
        trading_pairs=[],
        trading_required=True,
        api_keys=keys,
        rate_limits_share_pct=Decimal("100"),
    )
    return get_connector_class(name)(**params)


def _auth_headers(connector):
    request = RESTRequest(
        method=RESTMethod.GET,
        url="https://www.okx.com/api/v5/account/balance",
        is_auth_required=True,
    )
    return connector.authenticator.authentication_headers(request)


def test_demo_connectors_are_registered_with_isolated_fields():
    settings = AllConnectorSettings.get_connector_settings()

    assert "okx_demo" in settings
    assert "okx_perpetual_demo" in settings
    assert set(settings["okx_demo"].config_keys.__class__.model_fields) == {
        "connector",
        "okx_demo_api_key",
        "okx_demo_secret_key",
        "okx_demo_passphrase",
    }
    assert set(settings["okx_perpetual_demo"].config_keys.__class__.model_fields) == {
        "connector",
        "okx_perpetual_demo_api_key",
        "okx_perpetual_demo_secret_key",
        "okx_perpetual_demo_passphrase",
    }


def test_demo_and_live_rest_headers_are_separated():
    assert _auth_headers(_connector("okx_demo"))["x-simulated-trading"] == "1"
    assert _auth_headers(_connector("okx_perpetual_demo"))["x-simulated-trading"] == "1"
    assert "x-simulated-trading" not in _auth_headers(_connector("okx"))
    assert "x-simulated-trading" not in _auth_headers(_connector("okx_perpetual"))


def test_demo_rest_and_websocket_endpoints_are_selected():
    from hummingbot.connector.exchange.okx import okx_constants as spot_constants
    from hummingbot.connector.derivative.okx_perpetual import okx_perpetual_web_utils as perpetual_web_utils

    spot = _connector("okx_demo")
    perpetual = _connector("okx_perpetual_demo")

    assert spot.domain == "https://www.okx.com/"
    assert spot_constants.get_okx_ws_uri_public(spot.okx_registration_sub_domain).startswith(
        "wss://wspap.okx.com:8443/"
    )
    assert spot_constants.get_okx_ws_uri_private(spot.okx_registration_sub_domain).startswith(
        "wss://wspap.okx.com:8443/"
    )
    assert perpetual_web_utils.wss_linear_public_url(perpetual.domain).startswith(
        "wss://wspap.okx.com:8443/"
    )
    assert perpetual_web_utils.wss_linear_private_url(perpetual.domain).startswith(
        "wss://wspap.okx.com:8443/"
    )


@pytest.mark.asyncio
async def test_perpetual_public_stream_uses_demo_endpoint():
    from hummingbot.connector.derivative.okx_perpetual.okx_perpetual_api_order_book_data_source import (
        OkxPerpetualAPIOrderBookDataSource,
    )

    class FakeWebsocket:
        def __init__(self):
            self.url = None

        async def connect(self, ws_url, message_timeout):
            self.url = ws_url

    class FakeFactory:
        def __init__(self):
            self.websocket = FakeWebsocket()

        async def get_ws_assistant(self):
            return self.websocket

    source = object.__new__(OkxPerpetualAPIOrderBookDataSource)
    source._api_factory = FakeFactory()
    source._domain = "okx_perpetual_demo"

    websocket = await source._connected_websocket_assistant()

    assert websocket.url.startswith("wss://wspap.okx.com:8443/")

