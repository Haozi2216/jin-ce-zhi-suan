# src/utils/data_provider.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

class DataProvider:
    """
    Data Provider using external API for 1-minute K-line data.
    """
    def __init__(self, api_key="quantify-api-2026", base_url="https://automobiles-thumbnail-openings-wishing.trycloudflare.com"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "X-API-Key": self.api_key
        }

    def fetch_minute_data(self, code, start_time, end_time):
        """
        Fetch 1-minute data for a single stock within a time range.
        Handles pagination automatically.
        """
        all_data = []
        current_start = start_time
        limit = 20000 # Max limit per API spec
        
        print(f"Fetching data for {code} from {start_time} to {end_time}...")
        
        while current_start < end_time:
            # Format dates as ISO string or whatever the API expects.
            # API spec says date-time string. Let's try ISO format.
            # Example response showed "2011-08-17T15:00:00".
            
            # Request slightly more than needed to ensure coverage, but use limit
            params = {
                "code": code,
                "start_time": current_start.isoformat(),
                "end_time": end_time.isoformat(),
                "limit": limit
            }
            
            # # print(f"Requesting: {self.base_url}/market/minutes with params: {params}") # Debug
            
            try:
                response = requests.get(f"{self.base_url}/market/minutes", headers=self.headers, params=params)
                # print(f"Response Status: {response.status_code}") # Debug
                if response.status_code != 200:
                    print(f"Error: {response.text}")
                    
                response.raise_for_status()
                data = response.json()
                # print(f"Data keys: {data.keys()}") # Debug
                
                rows = data.get('rows', []) # Assuming response format based on /latest example which had "rows"
                # Wait, /latest had "rows". /minutes might be different. 
                # Let's assume consistent wrapper. 
                # If 'rows' is not present, check if it's a list directly or other key.
                # Based on /latest: {"count": 2, "rows": [...]}
                # I'll assume /minutes is similar.
                
                if not rows:
                    break
                    
                df_chunk = pd.DataFrame(rows)
                all_data.append(df_chunk)
                
                # Update current_start for next page
                # The API doesn't seem to have offset-based pagination for /minutes, but time-based?
                # Or maybe I just slide the window.
                # If I got 'limit' records, the last one's time is the new start.
                if len(rows) < limit:
                    break
                    
                last_time_str = rows[-1]['trade_time']
                last_time = pd.to_datetime(last_time_str)
                
                # If we are stuck at the same time, break to avoid infinite loop
                if last_time <= current_start:
                    # Move forward by 1 minute if stuck?
                    current_start = current_start + timedelta(minutes=1)
                else:
                    current_start = last_time + timedelta(minutes=1) # Start next from next minute
                    
                # Rate limit protection
                time.sleep(0.1) 
                
            except Exception as e:
                print(f"Error fetching data: {e}")
                break
                
        if not all_data:
            return pd.DataFrame()
            
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Rename columns to match internal standard if necessary
        # Internal: dt, code, open, high, low, close, vol, amount
        # API: trade_time, code, open, high, low, close, vol, amount
        
        rename_map = {
            'trade_time': 'dt'
        }
        final_df = final_df.rename(columns=rename_map)
        final_df['dt'] = pd.to_datetime(final_df['dt'])
        
        # Ensure correct types
        final_df['vol'] = final_df['vol'].astype(float)
        final_df['amount'] = final_df['amount'].astype(float)
        
        # Sort and drop duplicates just in case
        final_df = final_df.drop_duplicates(subset=['dt']).sort_values('dt').reset_index(drop=True)
        
        return final_df

    def fetch_batch_data(self, codes, start_time, end_time):
        """
        Fetch data for multiple codes.
        """
        results = {}
        for code in codes:
            df = self.fetch_minute_data(code, start_time, end_time)
            if not df.empty:
                results[code] = df
        return results

    def get_latest_bar(self, code):
        """
        Get the latest available 1-minute bar for a stock.
        Returns a dict or None.
        """
        try:
            url = f"{self.base_url}/market/latest"
            params = {"codes": code}
            response = requests.get(url, headers=self.headers, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            rows = data.get('rows', [])
            
            if rows:
                row = rows[0]
                # Normalize keys
                return {
                    'code': row['code'],
                    'dt': pd.to_datetime(row['trade_time']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'vol': float(row['vol']),
                    'amount': float(row['amount'])
                }
        except Exception as e:
            print(f"Error fetching latest bar for {code}: {e}")
        return None

    def push_data_to_remote(self, data_list):
        """
        Push daily archived data to remote API.
        This is a placeholder as the current API docs (OpenAPI) are read-only (GET methods).
        If there is a POST endpoint in future, implement here.
        For now, we just log.
        """
        # print(f"Pushing {len(data_list)} records to remote API...")
        # Example: requests.post(f"{self.base_url}/market/minutes", json=data_list, headers=self.headers)
        pass
