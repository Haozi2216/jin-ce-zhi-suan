# test_data_provider.py
from src.utils.data_provider import DataProvider
from datetime import datetime

def test_fetch():
    provider = DataProvider()
    code = "000001.SZ"
    
    ranges = [
        (datetime(2023, 1, 1), datetime(2023, 1, 31)),
        (datetime(2015, 1, 1), datetime(2015, 1, 31)),
        (datetime(2010, 1, 1), datetime(2010, 1, 31)),
    ]
    
    for start, end in ranges:
        print(f"Fetching {code} from {start} to {end}...")
        df = provider.fetch_minute_data(code, start, end)
        if not df.empty:
            print(f"Found {len(df)} rows.")
            print(f"Date range: {df['dt'].min()} to {df['dt'].max()}")
            break
        else:
            print("No data found.")

if __name__ == "__main__":
    test_fetch()
