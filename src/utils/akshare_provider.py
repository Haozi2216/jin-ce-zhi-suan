# src/utils/akshare_provider.py
import akshare as ak
import pandas as pd
from datetime import datetime

class AkshareProvider:
    """
    Akshare Data Provider (Open Source, Free)
    """
    def __init__(self):
        pass

    def get_latest_bar(self, code):
        """
        Get the latest real-time quote for a stock using Akshare.
        Returns a dict in the standard format.
        """
        try:
            # Akshare stock_zh_a_spot_em returns real-time data from EastMoney
            # It returns a large DataFrame for all stocks, which might be slow.
            # Optimized: stock_bid_ask_em for specific stock? Or stock_zh_a_spot_em with filter?
            # stock_zh_a_spot_em is the standard real-time interface.
            
            # Normalize code: 600036.SH -> 600036
            symbol = code.split('.')[0]
            
            # Use stock_bid_ask_em to get real-time price for a single stock (faster)
            # It returns trade levels, but also current price?
            # Better: stock_zh_a_spot_em returns all, we filter.
            # Alternative: stock_individual_info_em (basic info)
            
            # Let's try `stock_zh_a_spot_em` but it might be heavy.
            # Actually `stock_zh_a_hist_min_em` gets 1-min history, latest row is recent.
            # But for RT, we want sub-minute latency.
            
            # Use `stock_zh_a_spot_em` but filtered locally.
            # Or `stock_zh_a_tick_tx_js` (Tencent)
            
            # Let's use `stock_zh_a_spot_em` for now, assuming user machine can handle it.
            # Or better: `stock_zh_a_hist_min_em` with current time to get the latest 1-min bar.
            # This is safer for our system which runs on 1-min bars anyway.
            
            df = ak.stock_zh_a_hist_min_em(symbol=symbol, period='1', adjust='qfq')
            if df is None or df.empty:
                return None
                
            row = df.iloc[-1]
            
            # Columns: 时间, 开盘, 收盘, 最高, 最低, 成交量, 成交额, ...
            # Format: "2024-03-20 15:00:00"
            dt = pd.to_datetime(row['时间'])
            
            return {
                'code': code,
                'dt': dt,
                'open': float(row['开盘']),
                'high': float(row['最高']),
                'low': float(row['最低']),
                'close': float(row['收盘']),
                'vol': float(row['成交量']),
                'amount': float(row['成交额'])
            }
        except Exception as e:
            print(f"Error fetching Akshare RT data: {e}")
            return None

    def fetch_minute_data(self, code, start_time, end_time):
        """
        Fetch historical minute data via Akshare (EastMoney).
        """
        symbol = code.split('.')[0]
        # Akshare expects "YYYY-MM-DD HH:MM:SS" for minute data
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"📡 Requesting Akshare data for {symbol} from {start_str} to {end_str}...")
        
        try:
            # Akshare stock_zh_a_hist_min_em
            df = ak.stock_zh_a_hist_min_em(
                symbol=symbol, 
                period='1', 
                adjust='qfq', 
                start_date=start_str, 
                end_date=end_str
            )
            
            if df is None or df.empty:
                print(f"⚠️ Akshare returned empty data for {symbol}.")
                return pd.DataFrame()
                
            # Rename columns to standard format
            # Akshare columns: "时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"
            df = df.rename(columns={
                '时间': 'dt',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'vol',
                '成交额': 'amount'
            })
            
            df['code'] = code
            df['dt'] = pd.to_datetime(df['dt'])
            
            # Filter exact range (API might return slightly different range)
            mask = (df['dt'] >= start_time) & (df['dt'] <= end_time)
            df = df.loc[mask]
            
            # Sort
            df = df.sort_values('dt').reset_index(drop=True)
            
            # Ensure float types
            cols = ['open', 'high', 'low', 'close', 'vol', 'amount']
            for col in cols:
                df[col] = df[col].astype(float)
                
            return df[['code', 'dt', 'open', 'high', 'low', 'close', 'vol', 'amount']]
            
        except Exception as e:
            print(f"❌ Error fetching Akshare history: {e}")
            return pd.DataFrame()
