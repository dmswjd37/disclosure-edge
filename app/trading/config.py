import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None or not value.strip():
        return default

    return int(value.replace(",", "").strip())


@dataclass(frozen=True)
class KiwoomSettings:
    auto_buy_enabled: bool
    dry_run: bool
    is_paper_trading: bool
    real_app_key: str | None
    real_app_secret: str | None
    real_host_url: str | None
    real_socket_url: str | None
    paper_app_key: str | None
    paper_app_secret: str | None
    paper_host_url: str | None
    paper_socket_url: str | None

    @property
    def app_key(self) -> str | None:
        return self.paper_app_key if self.is_paper_trading else self.real_app_key

    @property
    def app_secret(self) -> str | None:
        return self.paper_app_secret if self.is_paper_trading else self.real_app_secret

    @property
    def host_url(self) -> str | None:
        return self.paper_host_url if self.is_paper_trading else self.real_host_url

    @property
    def socket_url(self) -> str | None:
        return self.paper_socket_url if self.is_paper_trading else self.real_socket_url

    @property
    def account_type(self) -> str:
        return "paper" if self.is_paper_trading else "real"

    @property
    def configured(self) -> bool:
        return bool(self.host_url and self.app_key and self.app_secret)


@dataclass(frozen=True)
class TradingRiskSettings:
    order_budget: int
    min_cash_balance: int
    daily_max_buy_amount: int
    state_path: Path


@dataclass(frozen=True)
class TradingSettings:
    kiwoom: KiwoomSettings
    risk: TradingRiskSettings


def get_trading_settings() -> TradingSettings:
    order_budget = _env_int("KIWOOM_ORDER_BUDGET", 4_000_000)

    return TradingSettings(
        kiwoom=KiwoomSettings(
            auto_buy_enabled=_env_flag("KIWOOM_AUTO_BUY_ENABLED", default=False),
            dry_run=_env_flag("KIWOOM_DRY_RUN", default=True),
            is_paper_trading=_env_flag("KIWOOM_IS_PAPER_TRADING", default=True),
            real_app_key=os.getenv("REAL_APP_KEY"),
            real_app_secret=os.getenv("REAL_APP_SECRET"),
            real_host_url=os.getenv("REAL_HOST_URL"),
            real_socket_url=os.getenv(
                "REAL_SOCKET_URL",
                "wss://api.kiwoom.com:10000/api/dostk/websocket",
            ),
            paper_app_key=os.getenv("PAPER_APP_KEY"),
            paper_app_secret=os.getenv("PAPER_APP_SECRET"),
            paper_host_url=os.getenv("PAPER_HOST_URL"),
            paper_socket_url=os.getenv(
                "PAPER_SOCKET_URL",
                "wss://mockapi.kiwoom.com:10000/api/dostk/websocket",
            ),
        ),
        risk=TradingRiskSettings(
            order_budget=order_budget,
            min_cash_balance=_env_int("KIWOOM_MIN_CASH_BALANCE", 5_000_000),
            daily_max_buy_amount=_env_int("KIWOOM_DAILY_MAX_BUY_AMOUNT", order_budget),
            state_path=Path(os.getenv("KIWOOM_ORDER_STATE_PATH", "data/trading/orders.json")),
        ),
    )
