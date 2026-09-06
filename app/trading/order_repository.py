import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class OrderRecord:
    rcp_no: str
    stock_code: str
    stock_name: str
    report_name: str
    order_no: str
    order_date: str
    order_amount: int
    quantity: int
    price: int
    status: str
    created_at: str
    filled_quantity: int = 0
    filled_price: int = 0
    filled_amount: int = 0
    filled_at: str = ""


class OrderRepository:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def list_records(self) -> list[OrderRecord]:
        if not self.path.exists():
            return []

        with self.path.open("r", encoding="utf-8") as file:
            rows = json.load(file)

        if not isinstance(rows, list):
            return []

        return [
            OrderRecord(
                rcp_no=row.get("rcp_no", ""),
                stock_code=row.get("stock_code", ""),
                stock_name=row.get("stock_name", ""),
                report_name=row.get("report_name", ""),
                order_no=row.get("order_no", ""),
                order_date=row.get("order_date", ""),
                order_amount=int(row.get("order_amount") or 0),
                quantity=int(row.get("quantity") or 0),
                price=int(row.get("price") or 0),
                status=row.get("status", ""),
                created_at=row.get("created_at", ""),
                filled_quantity=int(row.get("filled_quantity") or 0),
                filled_price=int(row.get("filled_price") or 0),
                filled_amount=int(row.get("filled_amount") or 0),
                filled_at=row.get("filled_at", ""),
            )
            for row in rows
            if isinstance(row, dict)
        ]

    def append(self, record: OrderRecord) -> None:
        records = self.list_records()
        records.append(record)

        with self.path.open("w", encoding="utf-8") as file:
            json.dump(
                [asdict(item) for item in records],
                file,
                ensure_ascii=False,
                indent=2,
            )

    def has_receipt(self, rcp_no: str) -> bool:
        return any(record.rcp_no == rcp_no for record in self.list_records())

    def has_stock_on_date(self, stock_code: str, order_date: str) -> bool:
        return any(
            record.stock_code == stock_code and record.order_date == order_date
            for record in self.list_records()
        )

    def bought_amount_on_date(self, order_date: str) -> int:
        return sum(
            record.order_amount
            for record in self.list_records()
            if record.order_date == order_date and record.status in {"ordered", "filled", "dry_run"}
        )

    def recent_records(self, limit: int = 20) -> list[OrderRecord]:
        return sorted(
            self.list_records(),
            key=lambda record: record.created_at,
            reverse=True,
        )[:limit]

    def mark_filled(
        self,
        order_no: str,
        filled_quantity: int,
        filled_price: int,
        filled_amount: int,
    ) -> None:
        records = []
        filled_at = datetime.now().isoformat(timespec="seconds")

        for record in self.list_records():
            if record.order_no == order_no:
                next_filled_quantity = record.filled_quantity + filled_quantity
                next_filled_amount = record.filled_amount + filled_amount
                next_filled_price = (
                    next_filled_amount // next_filled_quantity
                    if next_filled_quantity
                    else filled_price
                )
                record = OrderRecord(
                    rcp_no=record.rcp_no,
                    stock_code=record.stock_code,
                    stock_name=record.stock_name,
                    report_name=record.report_name,
                    order_no=record.order_no,
                    order_date=record.order_date,
                    order_amount=record.order_amount,
                    quantity=record.quantity,
                    price=record.price,
                    status="filled",
                    created_at=record.created_at,
                    filled_quantity=next_filled_quantity,
                    filled_price=next_filled_price,
                    filled_amount=next_filled_amount,
                    filled_at=filled_at,
                )

            records.append(record)

        with self.path.open("w", encoding="utf-8") as file:
            json.dump(
                [asdict(item) for item in records],
                file,
                ensure_ascii=False,
                indent=2,
            )


def new_order_record(
    disclosure: dict,
    order_no: str,
    order_date: str,
    order_amount: int,
    quantity: int,
    price: int,
    status: str,
) -> OrderRecord:
    return OrderRecord(
        rcp_no=disclosure.get("rcp_no", ""),
        stock_code=disclosure.get("stock_code", ""),
        stock_name=disclosure.get("stock_name", ""),
        report_name=disclosure.get("report_name", ""),
        order_no=order_no,
        order_date=order_date,
        order_amount=order_amount,
        quantity=quantity,
        price=price,
        status=status,
        created_at=datetime.now().isoformat(timespec="seconds"),
    )
