import os
import logging
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# 1. setup logging agar tahu apa yang robot lakukan
# file log akan disimpan di folder logs
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etl_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# 2. load env variables
load_dotenv()

def get_db_connection():
    """Fungsi pembantu untuk koneksi ke database"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT")
    )
    
def extract():
    """Ambil data mentah dari database"""
    logger.info("[*] Memulai proses EXTRACT...")
    
    conn = get_db_connection()
    
    # query untuk mengambil data join yang siap diolah
    # ambil data orders yang status nya 'Completed' saja
    query = """
        select
            o.order_date,
            o.total_amount,
            o.order_id,
            p.category
        from orders o
        join order_items oi on o.order_id = oi.order_id
        join products p on oi.product_id = p.product_id
        where o.status = 'Completed'
    """
    
    # pandas membaca sql langsung jadi DF
    df = pd.read_sql(query, conn)
    
    conn.close()
    logger.info(f"[*] Extract selesai. {len(df)} baris data diambil.")
    return df

def transform(df):
    """Hitung agregasi"""
    logger.info("[*] Memulai proses TRANSFORM...")
    
    if df.empty:
        logger.warning("[*] Tidak ada data untuk diolah!")
        return pd.DataFrame()
    
    # pastikan kolom order_date bertipe datetime
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # buat kolom baru 'report_date' (hanya tanggal, jam dibuang)
    df['report_date'] = df['order_date'].dt.date
    
    # agregasi 1: hitung total revenue & total order per tanggal
    daily_stats = df.groupby('report_date').agg({
        'total_amount': 'sum',
        'order_id': 'nunique' # hitung unique order_id
    }).reset_index()
    
    # rename kolom biar sesuai tabel tujuan
    daily_stats.rename(columns={
        'total_amount': 'total_revenue',
        'order_id': 'total_orders'
    }, inplace=True)
    
    # agregasi 2: cari kategori paling laku per tanggal
    # hitung frek kategori per tanggal
    category_counts = df.groupby(['report_date', 'category']).size().reset_index(name='count')
    # ambil kategori dengan count tertinggi di setiap tanggal
    top_categories = category_counts.sort_values(['report_date', 'count'], ascending=[True, False]).drop_duplicates('report_date')[['report_date', 'category']]
    
    # gabung kedua hasil agregasi
    final_report = pd.merge(daily_stats, top_categories, on='report_date', how='left')
    
    # rename kolom kategori
    final_report.rename(columns={'category': 'top_selling_category'}, inplace=True)
    
    logger.info(f"Transform selesai. {len(final_report)} baris laporan harian siap digunakan.")
    return final_report

def load(df):
    """Masukan data matang ke tabel laporan"""
    logger.info("Memulai proses LOAD ke DB...")
    
    if df.empty:
        logger.warning("Tidak ada data untuk disimpan.")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # loop setiap baris data di df dan masukan ke sql
        # di prod, pakai 'UPSERT', tapi untuk sekarang insert biasa dulu
        # pakai ON CONFLICT DO NOTHING agar kalau dijalankan 2x tidak error duplikat
        
        insert_query = """
            insert into daily_sales_summary
            (report_date, total_revenue, total_orders, top_selling_category)
            values (%s, %s, %s, %s)
            on conflict (report_date)
            do update set
                total_revenue = excluded.total_revenue,
                total_orders = excluded.total_orders,
                top_selling_category = excluded.top_selling_category;
        """
        
        for index, row in df.iterrows():
            cursor.execute(insert_query, (
                row['report_date'],
                row['total_revenue'],
                row['total_orders'],
                row['top_selling_category']
            ))
            
        conn.commit()
        logger.info("[+] Load selesai. Data laporan berhasil disimpan!")
        
    except Exception as e:
        logger.error(f"Terjadi error saat load: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    # ORCHESTRATION: Mengatur urutan jalan
    logger.info("=== START ETL PIPELINE ===")
    
    try:
        raw_data = extract()
        clean_data = transform(raw_data)
        load(clean_data)
        logger.info("=== ETL PIPELINE SELESAI === ")
    except Exception as e:
        logger.error(f"Pipeline berhenti karena error: {e}")