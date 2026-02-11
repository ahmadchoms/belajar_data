import os
import psycopg2
import random
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

NUM_USERS = 100
NUM_PRODUCTS = 20
NUM_ORDERS = 500

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT")
    )
    return conn

def generate_products(cursor):
    print("[*] Menyiapkan stok gudang")
    products = [
        ('Laptop Gaming', 'Electronics', 15000000),
        ('Mouse Wireless', 'Electronics', 150000),
        ('Keyboard Mechanical', 'Electronics', 500000),
        ('Monitor 24 inch', 'Electronics', 2000000),
        ('Meja Kerja', 'Furniture', 1200000),
        ('Kursi Ergonomis', 'Furniture', 2500000),
        ('Lampu Belajar', 'Furniture', 300000),
        ('Kopi Arabika', 'F&B', 75000),
        ('Tumbler Keren', 'Merchandise', 100000),
        ('Kaos Developer', 'Apparel', 120000)
    ]
    
    for prod in products:
        cursor.execute(
            "insert into products (product_name, category, price) values (%s, %s, %s)", prod
        )
        
        print(f"[*] {len(products)} jenis produk siap dijual.")
        
def generate_orders(cursor):
    print("[*] Simulasi transaksi belanja sedang berlangsung...")
    
    # 1. ambil semua id user dan producr
    cursor.execute("select user_id from users")
    user_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("select product_id, price from products")
    products = cursor.fetchall()
    
    if not user_ids or not products:
        print("[*] Error: Tabel users atau products kosong!")
        return
    
    for _ in range(NUM_ORDERS):
        # pilih user acak buat belanja
        user_id = random.choice(user_ids)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choice(['Completed', 'Pending', 'Cancelled'])
        
        # buat order
        cursor.execute(
            "insert into orders (user_id, order_date, status, total_amount) values (%s, %s, %s, 0) returning order_id", (user_id, order_date, status)
        )
        order_id = cursor.fetchone()[0]
        
        total_order_price = 0
        num_items = random.randint(1, 5) #random belajar 1-5
        
        for _ in range(num_items):
            product = random.choice(products)
            p_id, p_price = product
            qty = random.randint(1, 3)
            subtotal = p_price * qty
            
            cursor.execute(
                "insert into order_items (order_id, product_id, quantity, price_at_purchase) values (%s, %s, %s, %s)", (order_id, p_id, qty, subtotal)
            )
            total_order_price += subtotal
            
        cursor.execute(
            "update orders set total_amount = %s where order_id = %s", (total_order_price, order_id)
        )
        
    print(f"[*] {NUM_ORDERS} transaksi berhasil dibuat")
    
def main():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        generate_products(cursor)
        generate_orders(cursor)
        
        conn.commit()
        print("[*] Semua data dummy berhasil masuk!")
    except Exception as error:
        print(f"Terjadi error: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == '__main__':
    main()