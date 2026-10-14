import csv
import os
from typing import Iterator, Dict, Any
from bank_system.iterators import AccountRangeIterator

def generate_large_dataset(filepath: str, total_records: int = 100_000) -> None:
    """Генерує тестовий CSV-файл із банківськими операціями (> 100 000 записів)."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    clients = ["Client_A", "Client_B", "Client_C", "Client_D", "Client_E"]
    operations = ["deposit", "withdraw"]
    
    # Використання власного ітератора
    id_iter = AccountRangeIterator(start_id=1000, count=500)
    account_pool = list(id_iter)

    with open(filepath, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["account_number", "client", "balance", "operation", "amount"])
        
        for i in range(total_records):
            acc = f"ACC-{account_pool[i % len(account_pool)]}"
            client = clients[i % len(clients)]
            balance = round(1000.0 + (i * 7) % 50000.0, 2)
            operation = operations[i % 2]
            amount = round(10.0 + (i * 3) % 500.0, 2)
            writer.writerow([acc, client, balance, operation, amount])


def stream_csv_reader(filepath: str) -> Iterator[Dict[str, str]]:
    """Streaming reader: читає CSV-файл рядок за рядком за допомогою yield."""
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def parse_and_validate_records(rows: Iterator[Dict[str, str]]) -> Iterator[Dict[str, Any]]:
    """Парсить дані, перевіряє валідність та фільтрує пошкоджені записи."""
    for row in rows:
        try:
            account_number = str(row["account_number"]).strip()
            client = str(row["client"]).strip()
            balance = float(row["balance"])
            operation = str(row["operation"]).strip().lower()
            amount = float(row["amount"])

            if not account_number or not client or balance < 0 or amount < 0:
                continue
            if operation not in ["deposit", "withdraw"]:
                continue

            yield {
                "account_number": account_number,
                "client": client,
                "balance": balance,
                "operation": operation,
                "amount": amount
            }
        except (ValueError, KeyError):
            continue


def filter_accounts(records: Iterator[Dict[str, Any]], target_client: str = None, min_balance: float = 0.0) -> Iterator[Dict[str, Any]]:
    """Фільтрація рахунків за клієнтом або порогом балансу."""
    for record in records:
        if target_client and record["client"] != target_client:
            continue
        if record["balance"] < min_balance:
            continue
        yield record


def process_account_operations(records: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """Потокове застосування операцій з перевіркою на недопущення від'ємного балансу."""
    for record in records:
        bal = record["balance"]
        amt = record["amount"]
        if record["operation"] == "deposit":
            bal += amt
        elif record["operation"] == "withdraw":
            if bal - amt >= 0:
                bal -= amt
            else:
                continue
        
        updated_record = record.copy()
        updated_record["balance"] = round(bal, 2)
        yield updated_record


def run_lazy_pipeline(filepath: str, target_client: str = None, min_balance: float = 0.0) -> Iterator[Dict[str, Any]]:
    """Збирання повного lazy pipeline потокової обробки."""
    raw_lines = stream_csv_reader(filepath)
    parsed = parse_and_validate_records(raw_lines)
    filtered = filter_accounts(parsed, target_client=target_client, min_balance=min_balance)
    processed = process_account_operations(filtered)
    return processed