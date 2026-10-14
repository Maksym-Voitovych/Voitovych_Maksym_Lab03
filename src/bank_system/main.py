import time
import tracemalloc
import os
from itertools import islice, groupby, accumulate, pairwise, chain

from bank_system.streaming import generate_large_dataset, run_lazy_pipeline

DATA_PATH = os.path.join("data", "input.csv")

def measure_eager_processing(filepath: str):
    """Eager evaluation: завантаження всього файлу в пам'ять (наприклад, через pandas)."""
    import pandas as pd
    start_time = time.time()
    tracemalloc.start()
    
    df = pd.read_csv(filepath)
    df = df[(df["balance"] >= 0) & (df["amount"] >= 0)]
    df["balance"] = df.apply(
        lambda row: row["balance"] + row["amount"] if row["operation"] == "deposit" 
        else (row["balance"] - row["amount"] if row["balance"] - row["amount"] >= 0 else row["balance"]), 
        axis=1
    )
    total_sum = df["balance"].sum()
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    elapsed = time.time() - start_time
    return elapsed, peak, total_sum


def measure_lazy_processing(filepath: str):
    """Lazy evaluation: потокова обробка без матеріалізації колекцій."""
    start_time = time.time()
    tracemalloc.start()
    
    pipeline = run_lazy_pipeline(filepath, min_balance=100.0)
    total_sum = sum(rec["balance"] for rec in pipeline)
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    elapsed = time.time() - start_time
    return elapsed, peak, total_sum


if __name__ == "__main__":
    print("=== БАНКІВСЬКА СИСТЕМА: ПОТОКОВА ОБРОБКА ДАНИХ (ЛР №3) ===")
    
    # 1. Генерація датасету (100 000+ записів)
    print("[1] Генерація тестового набору даних (100 000 записів)...")
    generate_large_dataset(DATA_PATH, total_records=100_000)
    print(f"   Файл створено: {DATA_PATH} (розмір: {os.path.getsize(DATA_PATH) / 1024 / 1024:.2f} MB)\n")

    # 2. Вибірка перших записів через islice
    print("[2] Демонстрація роботи lazy pipeline та вибірки перших 5 записів (islice):")
    pipeline = run_lazy_pipeline(DATA_PATH)
    first_records = list(islice(pipeline, 5))
    for idx, rec in enumerate(first_records, 1):
        print(f"   {idx}. Рахунок: {rec['account_number']} | Клієнт: {rec['client']} | Баланс: {rec['balance']} | Операція: {rec['operation']} {rec['amount']}")
    print()

    # 3. Використання itertools (groupby, accumulate, pairwise, chain)
    print("[3] Демонстрація інструментів itertools:")
    pipeline_it = run_lazy_pipeline(DATA_PATH)
    rec_list = sorted(list(pipeline_it), key=lambda x: x["client"])
    
    # groupby
    client_groups = {client: len(list(group)) for client, group in groupby(rec_list, key=lambda x: x["client"])}
    print(f"   Кількість записів по клієнтах (groupby): {client_groups}")

    # accumulate та pairwise
    balances = [r["balance"] for r in rec_list[:10]]
    acc_balances = list(accumulate(balances))
    diffs = [round(curr - prev, 2) for prev, curr in pairwise(balances)]
    print(f"   Накопичувальні баланси (accumulate): {acc_balances[:5]}")
    print(f"   Різниці сусідніх транзакцій (pairwise): {diffs[:5]}")
    print()

    # 4. Порівняння Eager та Lazy підходів
    print("[4] Порівняння продуктивності (Eager vs Lazy):")
    eager_time, eager_peak, eager_sum = measure_eager_processing(DATA_PATH)
    lazy_time, lazy_peak, lazy_sum = measure_lazy_processing(DATA_PATH)

    print(f"   [Eager] Час: {eager_time:.4f} сек | Пікова пам'ять: {eager_peak / 1024 / 1024:.2f} MB | Сума: {eager_sum}")
    print(f"   [Lazy]  Час: {lazy_time:.4f} сек | Пікова пам'ять: {lazy_peak / 1024 / 1024:.2f} MB | Сума: {lazy_sum}")
    print("\nВиконання програми завершено успішно!")