import pandas as pd
import logging

# setup logging agar bisa dibaca di airflow ui
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, df):
        self.df = df
        self.results = {
            'passed': True,
            'details': []
        }
        
    def check_no_nulls(self, columns):
        """Cek apakah ada nilai NULL di kolom penting"""
        for col in columns:
            null_count = self.df[col].isnull().sum()
            status = null_count == 0
            
            self.results['details'].append({
                'check': f'check_no_nulls_{col}',
                'passed': status,
                'info': f'{null_count} nulls found'
            })
            
            if not status:
                self.results['passed'] = False
                logger.error(f"[-] Data Quality Fail: Kolom '{col} mengandung NULL!'")
                
    def check_positive_values(self, columns):
        """Cek apakah angka bernilai positif (Revenue ga boleh minus)"""
        for col in columns:
            negative_count = (self.df[col] < 0).sum()
            status = negative_count == 0
            
            self.results['details'].append({
                'check': f'check_positive_{col}',
                'passed': status,
                'info': f'{negative_count} negative values found'
            })
            
            if not status:
                self.results['passed'] = False
                logger.error(f"[-] Data Quality Fail: Kolom '{col}' ada nilai minus (negative)!")
                
    def validate(self):
        """Mengembalikan TRUE jika SEMUA cek lulus"""
        return self.results['passed']