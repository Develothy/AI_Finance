"""숫자/가격 포맷"""


def format_krw(value: float) -> str:
    if value is None:
        return "-"
    return f"{value:,.0f}"


def format_usd(value: float) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def format_volume(value: int) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def format_price(value: float, market: str) -> str:
    if market in ("NYSE", "NASDAQ"):
        return format_usd(value)
    return format_krw(value)
