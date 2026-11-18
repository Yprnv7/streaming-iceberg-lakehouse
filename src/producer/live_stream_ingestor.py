"""Simulated high-throughput live stream generator with chaos late-event injection."""

from __future__ import annotations

import datetime
import random
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional


@dataclass
class TradeEvent:
    """Individual trade event payload modeled after financial market feeds."""

    event_id: str
    symbol: str
    price: float
    quantity: float
    trade_side: str  # 'BUY' | 'SELL'
    user_id: str
    event_time: datetime.datetime
    ingested_at: datetime.datetime
    is_late: bool = False
    delay_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "trade_side": self.trade_side,
            "user_id": self.user_id,
            "event_time": self.event_time.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "is_late": self.is_late,
            "delay_seconds": self.delay_seconds,
        }


class LiveStreamIngestor:
    """Generates continuous trade stream with configurable chaos late-arrival injection."""

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        late_event_probability: float = 0.08,
        max_delay_minutes: float = 15.0,
    ) -> None:
        self.symbols = symbols or ["BTC/USDT", "ETH/USDT", "SOL/USDT", "EUR/USD"]
        self.late_event_probability = late_event_probability
        self.max_delay_minutes = max_delay_minutes
        self.random = random.Random(42)

    def generate_event(self, current_time: Optional[datetime.datetime] = None) -> TradeEvent:
        """Synthesizes a trade event with optional delayed timestamp."""
        now = current_time or datetime.datetime.now(datetime.timezone.utc)
        symbol = self.random.choice(self.symbols)
        base_price = {"BTC/USDT": 65000.0, "ETH/USDT": 3400.0, "SOL/USDT": 150.0, "EUR/USD": 1.08}[symbol]
        price = round(base_price * (1.0 + self.random.uniform(-0.015, 0.015)), 2)
        qty = round(self.random.uniform(0.01, 2.5), 4)

        is_late = self.random.random() < self.late_event_probability
        delay_sec = 0.0
        event_time = now

        if is_late:
            # Late arrival between 1 and max_delay_minutes
            delay_sec = self.random.uniform(60.0, self.max_delay_minutes * 60.0)
            event_time = now - datetime.timedelta(seconds=delay_sec)

        return TradeEvent(
            event_id=f"ev_{uuid.uuid4().hex[:12]}",
            symbol=symbol,
            price=price,
            quantity=qty,
            trade_side=self.random.choice(["BUY", "SELL"]),
            user_id=f"usr_{self.random.randint(100, 999)}",
            event_time=event_time,
            ingested_at=now,
            is_late=is_late,
            delay_seconds=round(delay_sec, 2),
        )

    def stream(self, count: int = 100) -> Generator[TradeEvent, None, None]:
        """Yields a sequence of trade events."""
        now = datetime.datetime.now(datetime.timezone.utc)
        for i in range(count):
            t = now + datetime.timedelta(milliseconds=i * 50)
            yield self.generate_event(current_time=t)
