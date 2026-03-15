# src/utils/data_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_mock_data(stock_code, start_date, end_date):
    """
    Generate mock 1-minute K-line data for a single stock.
    """
    # Create a simple date range for minutes
    dates = pd.date_range(start=start_date, end=end_date, freq='1min')
    
    # Filter for trading hours (simplified)
    # Only keep 9:30-11:30 and 13:00-15:00 on weekdays
    mask = (dates.weekday < 5) & (
        ((dates.time >= pd.Timestamp('09:30').time()) & (dates.time <= pd.Timestamp('11:30').time())) |
        ((dates.time >= pd.Timestamp('13:00').time()) & (dates.time <= pd.Timestamp('15:00').time()))
    )
    dates = dates[mask]
    n = len(dates)
    
    if n == 0:
        return pd.DataFrame()

    # Random walk for price starting at 100
    price_base = 100.0
    returns = np.random.normal(0, 0.0005, n) # 0.05% per minute volatility
    price_path = price_base * np.exp(np.cumsum(returns))
    
    data = {
        'dt': dates,
        'code': [stock_code] * n,
        'name': [f"Stock_{stock_code}"] * n,
        'open': price_path,
        'high': price_path * (1 + abs(np.random.normal(0, 0.0002, n))),
        'low': price_path * (1 - abs(np.random.normal(0, 0.0002, n))),
        'close': price_path * (1 + np.random.normal(0, 0.0001, n)),
        'vol': np.random.randint(100, 5000, n),
        'amount': np.random.randint(10000, 500000, n),
        'zf': returns, # Percentage change approximation
        'limit_up': 0, # Placeholder
        'limit_down': 0 # Placeholder
    }
    
    df = pd.DataFrame(data)
    
    # Ensure high is highest and low is lowest
    df['high'] = df[['open', 'close', 'high']].max(axis=1)
    df['low'] = df[['open', 'close', 'low']].min(axis=1)
    
    return df
