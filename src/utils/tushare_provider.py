# src/utils/tushare_provider.py
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta

class TushareProvider:
    """
    Tushare Pro Data Provider
    """
    def __init__(self, token=None):
        # Default to a placeholder token if none provided. User must replace this.
        self.token = token
        if self.token:
            ts.set_token(self.token)
            self.pro = ts.pro_api()
        else:
            self.pro = None
            print("⚠️ Warning: Tushare Token not provided. Please initialize with a valid token.")

    def set_token(self, token):
        self.token = token
        ts.set_token(self.token)
        self.pro = ts.pro_api()

    def get_latest_bar(self, code):
        """
        Get the latest real-time quote for a stock.
        Returns a dict in the standard format.
        """
        try:
            # Tushare 'get_realtime_quotes' is from the old package, but still works for free.
            # However, for stable production, 'pro' interfaces are preferred if available.
            # But Tushare Pro doesn't have a 1-second delay real-time tick API for free usually.
            # Let's use the standard tushare.get_realtime_quotes which scrapes Sina/Sohu.
            
            df = ts.get_realtime_quotes(code)
            if df is None or df.empty:
                return None
                
            row = df.iloc[0]
            
            # Normalize format
            # Tushare RT columns: name, open, pre_close, price, high, low, bid, ask, volume, amount, date, time
            
            # Combine date and time
            dt_str = f"{row['date']} {row['time']}"
            dt = pd.to_datetime(dt_str)
            
            return {
                'code': code,
                'dt': dt,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['price']), # Current price
                'vol': float(row['volume']), # Check unit: usually shares?
                'amount': float(row['amount']) # Check unit: usually Yuan?
            }
        except Exception as e:
            print(f"Error fetching Tushare RT data: {e}")
            return None

    def fetch_minute_data(self, code, start_time, end_time):
        """
        Fetch historical minute data via Tushare Pro (requires points/permission).
        Interface: pro.stk_mins or standard ts.pro_bar
        """
        if not self.pro:
            return pd.DataFrame()
            
        # Format dates: YYYYMMDD
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # pro_bar is a wrapper that handles pagination automatically
            df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_str, end_date=end_str, freq='1min')
            
            if df is None or df.empty:
                return pd.DataFrame()
                
            # Tushare returns date as string, trade_time as string
            # Columns: ts_code, trade_time, open, close, high, low, vol, amount
            
            # Rename
            df = df.rename(columns={
                'ts_code': 'code',
                'trade_time': 'dt'
            })
            
            # Ensure datetime
            df['dt'] = pd.to_datetime(df['dt'])
            
            # Sort ascending (Tushare usually returns descending)
            df = df.sort_values('dt').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            print(f"Error fetching Tushare history: {e}")
            return pd.DataFrame()
