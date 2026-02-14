import pandas as pd
import numpy as np

total_rows = 100000 

print(f"[*] Membuat {total_rows} data dummy...")

df = pd.DataFrame({
    'order_id': range(1, total_rows + 1),
    'product_name': np.random.choice(['Laptop', 'Mouse', 'Monitor', 'Keyboard'], total_rows),
    'category': np.random.choice(['Electronics', 'Accessories'], total_rows),
    'quantity': np.random.randint(1, 10, total_rows),
    'price': np.random.randint(100000, 5000000, total_rows),
    'order_date': pd.date_range(start='2024-01-01', periods=total_rows, freq='min')
})

df.to_csv('transaction.csv', index=False)
print("[+] Selesai! File 'transaction.csv' sudah siap")