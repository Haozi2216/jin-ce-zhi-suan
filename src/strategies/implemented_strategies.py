# src/strategies/implemented_strategies.py
from src.strategies.base_strategy import BaseStrategy
from src.utils.indicators import Indicators
import pandas as pd
import numpy as np

class BaseImplementedStrategy(BaseStrategy):
    """
    Base class for implemented strategies with common utilities.
    """
    def __init__(self, strategy_id, name):
        super().__init__(strategy_id)
        self.name = name
        self.bars_held = {} # Code -> Count of bars held
        self.entry_price = {} # Code -> Entry Price
        self.highest_high = {} # Code -> Highest High since entry
        self.trailing_stop_level = {} # Code -> Trailing Stop Price

    def update_holding_time(self, code):
        if code in self.positions and self.positions[code] > 0:
            self.bars_held[code] = self.bars_held.get(code, 0) + 1
        else:
            self.bars_held[code] = 0
            self.highest_high[code] = 0.0
            self.trailing_stop_level[code] = 0.0

    def check_max_holding_time(self, code, max_bars):
        if self.bars_held.get(code, 0) >= max_bars:
            return True
        return False
        
    def create_exit_signal(self, kline, qty, reason):
        return {
            'strategy_id': self.id,
            'code': kline['code'],
            'dt': kline['dt'],
            'direction': 'SELL',
            'price': kline['close'],
            'qty': qty,
            'reason': reason
        }

# Strategy 01: 三周期共振波段策略
class Strategy01(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("01", "三周期共振波段")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        self.update_holding_time(code)
        
        if code not in self.history: self.history[code] = pd.DataFrame()
        new_row = pd.DataFrame([kline])
        self.history[code] = pd.concat([self.history[code], new_row], ignore_index=True)
        
        if len(self.history[code]) > 30000:
             self.history[code] = self.history[code].iloc[-30000:]
             
        df = self.history[code]
        if len(df) < 500: return None 
        
        qty = self.positions.get(code, 0)
        
        # Optimization: Use subsets for resampling
        df_d_subset = df.iloc[-6000:]
        df_d = Indicators.resample(df_d_subset, 'D')
        if len(df_d) < 20: return None
        df_d['ma10'] = Indicators.MA(df_d['close'], 10)
        df_d['ma20'] = Indicators.MA(df_d['close'], 20)
        curr_d = df_d.iloc[-1]
        
        if qty > 0:
            if curr_d['close'] < curr_d['ma20']:
                return self.create_exit_signal(kline, qty, "Daily MA20 Breakdown")
            return None

        # Weekly
        df_w = Indicators.resample(df, 'W')
        if len(df_w) < 20: return None
        df_w['ma20'] = Indicators.MA(df_w['close'], 20)

        # 60 Min
        df_recent = df.iloc[-2000:]
        df_60m = Indicators.resample(df_recent, '60min')
        if len(df_60m) < 35: return None
        df_60m['dif'], df_60m['dea'], df_60m['macd'] = Indicators.MACD(df_60m['close'])
        
        curr_60m = df_60m.iloc[-1]
        prev_60m = df_60m.iloc[-2]
            
        # Entry
        if len(df_w) >= 2 and df_w.iloc[-1]['ma20'] > df_w.iloc[-2]['ma20']:
            if abs(curr_d['low'] - curr_d['ma10']) / curr_d['ma10'] < 0.02:
                 if curr_60m['dif'] > curr_60m['dea'] and prev_60m['dif'] <= prev_60m['dea']:
                     vol_ma5 = df_60m['vol'].rolling(5).mean().iloc[-1]
                     if curr_60m['vol'] > vol_ma5:
                         return {
                            'strategy_id': self.id,
                            'code': code,
                            'dt': kline['dt'],
                            'direction': 'BUY',
                            'price': kline['close'],
                            'qty': 1000,
                            'stop_loss': kline['close'] * 0.97,
                            'take_profit': None
                        }
        return None

# Strategy 02: 短线弱转强烂板战法
class Strategy02(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("02", "短线弱转强烂板")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        self.update_holding_time(code)
        if code not in self.history: self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(500)
        
        qty = self.positions.get(code, 0)
        if qty > 0:
            if self.check_max_holding_time(code, 240): 
                return self.create_exit_signal(kline, qty, "Next Day Exit")
            return None
        return None

# Strategy 03: ETF行业轮动
class Strategy03(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("03", "ETF行业轮动")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        if code not in self.history: self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(2400)
        df = self.history[code]
        
        if len(df) < 240: return None
        
        df_d = Indicators.resample(df, 'D')
        if len(df_d) < 10: return None
        df_d['ma10'] = Indicators.MA(df_d['close'], 10)
        curr_d = df_d.iloc[-1]
        
        qty = self.positions.get(code, 0)
        if qty > 0:
            if curr_d['close'] < curr_d['ma10']:
                return self.create_exit_signal(kline, qty, "Break MA10")
            return None
            
        if curr_d['close'] > curr_d['ma10']:
             return {
                'strategy_id': self.id,
                'code': code,
                'dt': kline['dt'],
                'direction': 'BUY',
                'price': kline['close'],
                'qty': 1000,
                'stop_loss': kline['close'] * 0.95,
                'take_profit': None
            }
        return None

# Strategy 04: 龙头首阴反包
class Strategy04(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("04", "龙头首阴反包")
    def on_bar(self, kline):
        return None

# Strategy 05: 3N法则主升浪
class Strategy05(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("05", "3N法则主升浪")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        self.update_holding_time(code)
        if code not in self.history: self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(5000)
        df = self.history[code]
        
        qty = self.positions.get(code, 0)
        
        df_30m = Indicators.resample(df, '30min')
        if len(df_30m) < 20: return None
        df_30m['dif'], df_30m['dea'], df_30m['macd'] = Indicators.MACD(df_30m['close'])
        curr = df_30m.iloc[-1]
        
        if qty > 0:
             if self.check_max_holding_time(code, 1200): 
                 return self.create_exit_signal(kline, qty, "Time 5 Days")
             return None
             
        if curr['dif'] > curr['dea'] and curr['dif'] > 0:
             if curr['vol'] > df_30m['vol'].rolling(5).mean().iloc[-1] * 1.5:
                 return {
                    'strategy_id': self.id,
                    'code': code,
                    'dt': kline['dt'],
                    'direction': 'BUY',
                    'price': kline['close'],
                    'qty': 1000,
                    'stop_loss': kline['close'] * 0.95,
                    'take_profit': kline['close'] * 1.10
                }
        return None

# Strategy 06: 海豚交易法
class Strategy06(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("06", "海豚交易法")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        self.update_holding_time(code)
        if code not in self.history: self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(500)
        df = self.history[code]
        if len(df) < 50: return None
        
        # MA26 for Trend
        df['ma26'] = Indicators.MA(df['close'], 26)
        # MACD
        df['dif'], df['dea'], df['macd'] = Indicators.MACD(df['close'])
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        qty = self.positions.get(code, 0)
        
        # Trailing Stop Logic (1% below High)
        if qty > 0:
            if curr['high'] > self.highest_high.get(code, 0.0):
                self.highest_high[code] = curr['high']
                # Update Trailing Stop: 1% below new high
                self.trailing_stop_level[code] = self.highest_high[code] * 0.99
            
            # Check Trailing Stop
            if curr['low'] <= self.trailing_stop_level.get(code, 0.0):
                return self.create_exit_signal(kline, qty, f"Trailing Stop (High {self.highest_high[code]:.2f})")
                
            return None
            
        # Entry Long
        # Price > MA26 + MACD Gold Cross above Zero
        if curr['close'] > curr['ma26']:
            # Gold Cross: DIF > DEA now, DIF <= DEA before
            if curr['dif'] > curr['dea'] and prev['dif'] <= prev['dea']:
                # "Water Top" (Above Zero)
                if curr['dif'] > 0 and curr['dea'] > 0:
                    self.highest_high[code] = curr['close']
                    self.trailing_stop_level[code] = curr['close'] * 0.99
                    return {
                        'strategy_id': self.id,
                        'code': code,
                        'dt': kline['dt'],
                        'direction': 'BUY',
                        'price': kline['close'],
                        'qty': 1000,
                        'stop_loss': kline['close'] * 0.99, # Initial SL
                        'take_profit': None # Trailing Stop only
                    }
        return None

# Strategy 07: 跳空交易系统
class Strategy07(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("07", "跳空交易系统")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        self.update_holding_time(code)
        if code not in self.history: self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(500)
        df = self.history[code]
        
        # Use 15min timeframe as per description
        df_15m = Indicators.resample(df, '15min')
        if len(df_15m) < 20: return None
        
        curr = df_15m.iloc[-1]
        prev = df_15m.iloc[-2]
        
        qty = self.positions.get(code, 0)
        
        # Trailing Stop Logic (Similar to Dolphin: 1% below High)
        if qty > 0:
            # Check Stop
            current_price = kline['close']
            if current_price > self.highest_high.get(code, 0.0):
                self.highest_high[code] = current_price
                self.trailing_stop_level[code] = self.highest_high[code] * 0.99
            
            if current_price <= self.trailing_stop_level.get(code, 0.0):
                 return self.create_exit_signal(kline, qty, "Trailing Stop")
            return None
            
        # Entry Long
        # Gap Down 0.2% + Yang Line
        if curr['open'] < prev['close'] * 0.998:
            if curr['close'] > curr['open']: # Yang Line
                self.highest_high[code] = curr['close']
                self.trailing_stop_level[code] = curr['close'] * 0.99
                return {
                    'strategy_id': self.id,
                    'code': code,
                    'dt': kline['dt'],
                    'direction': 'BUY',
                    'price': kline['close'],
                    'qty': 1000,
                    'stop_loss': kline['close'] * 0.99,
                    'take_profit': None
                }
        return None

# Strategy 08: 神奇九转 (Magic 9)
class Strategy08(BaseImplementedStrategy):
    def __init__(self):
        super().__init__("08", "神奇九转")
        self.history = {}

    def on_bar(self, kline):
        code = kline['code']
        self.update_holding_time(code)
        
        # History Management
        if code not in self.history: self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(500)
        df = self.history[code]
        
        # Need enough data: 9 bars + 4 lag = 13 minimum
        if len(df) < 13: return None
        
        qty = self.positions.get(code, 0)
        curr_close = kline['close']
        
        # --- Exit Logic: Trailing Stop (1%) ---
        if qty > 0:
            # Update Highest High
            if kline['high'] > self.highest_high.get(code, 0.0):
                self.highest_high[code] = kline['high']
                self.trailing_stop_level[code] = self.highest_high[code] * 0.99
            
            # Check Stop
            if curr_close <= self.trailing_stop_level.get(code, 0.0):
                 return self.create_exit_signal(kline, qty, f"Trailing Stop (High {self.highest_high[code]:.2f})")
            return None

        # --- Entry Logic: Magic 9 (Buy Setup) ---
        # Condition: 9 consecutive bars where Close > Close[i-4]
        # Check last 9 bars
        
        # Get close prices as numpy array for speed
        closes = df['close'].values
        
        is_setup = True
        for i in range(1, 10): # 1 to 9 (checking last 9 bars)
            # Index from end: -i
            # closes[-i] vs closes[-i-4]
            if not (closes[-i] > closes[-i-4]):
                is_setup = False
                break
        
        # Specific Validation from Rules:
        # 1st (-9) > Prev 4 (-13) (Already checked in loop i=9)
        # 5th (-5) > 1st (-9)
        # 9th (-1) > 5th (-5)
        if is_setup:
            price_1st = closes[-9]
            price_5th = closes[-5]
            price_9th = closes[-1]
            
            if price_5th > price_1st and price_9th > price_5th:
                 # Initialize Trailing Stop state
                 self.highest_high[code] = curr_close
                 self.trailing_stop_level[code] = curr_close * 0.99
                 
                 return {
                    'strategy_id': self.id,
                    'code': code,
                    'dt': kline['dt'],
                    'direction': 'BUY',
                    'price': curr_close,
                    'qty': 1000,
                    'stop_loss': curr_close * 0.99, # Initial SL
                    'take_profit': None
                }
            
        return None
