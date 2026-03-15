# src/core/menxia_sheng.py

from src.ministries.xing_bu_justice import XingBuJustice
from src.utils.constants import *

class MenxiaSheng:
    """
    门下省 (Chancellery): 对每一个信号做强制风控审核，一票否决
    """
    def __init__(self, xing_bu: XingBuJustice):
        self.xing_bu = xing_bu
        self.daily_losses = {} # Strategy ID -> Daily Loss
        self.consecutive_losses = {} # Strategy ID -> Consecutive Loss Count
        self.max_drawdowns = {} # Strategy ID -> Max Drawdown
        self.positions = {} # Strategy ID -> Current Positions

    def check_signal(self, signal, current_portfolio_value, current_positions, daily_pnl):
        """
        Check if a signal passes risk control.
        Returns: (bool, reason)
        """
        strategy_id = signal['strategy_id']
        direction = signal['direction']
        entry_price = signal['price']
        stop_loss_price = signal.get('stop_loss')
        
        # Rule 1: Single trade stop loss <= 1%
        if stop_loss_price:
            sl_pct = abs(entry_price - stop_loss_price) / entry_price
            if sl_pct > MAX_STOP_LOSS_PCT:
                self.xing_bu.record_rejection(strategy_id, 'R1', f"Stop loss {sl_pct:.2%} > {MAX_STOP_LOSS_PCT:.2%}", signal['dt'])
                return False, f"止损幅度 {sl_pct:.2%} 超过限制 ({MAX_STOP_LOSS_PCT:.2%})"

        # Rule 2: Single stock max position 10%
        # Calculate position size based on signal amount/qty
        # Assuming signal['amount'] is the value or quantity * price
        # This requires context of current portfolio value.
        position_value = signal['qty'] * entry_price
        if position_value / current_portfolio_value > MAX_POS_PER_STOCK:
             self.xing_bu.record_rejection(strategy_id, 'R2', f"Position size {position_value/current_portfolio_value:.2%} > {MAX_POS_PER_STOCK:.2%}", signal['dt'])
             return False, f"单票仓位 {position_value/current_portfolio_value:.2%} 超过限制 ({MAX_POS_PER_STOCK:.2%})"

        # Rule 3: Total position <= 50%
        current_total_pos_value = sum([p['market_value'] for p in current_positions.values()])
        if (current_total_pos_value + position_value) / current_portfolio_value > MAX_TOTAL_POS:
             self.xing_bu.record_rejection(strategy_id, 'R3', f"Total position {(current_total_pos_value + position_value)/current_portfolio_value:.2%} > {MAX_TOTAL_POS:.2%}", signal['dt'])
             return False, f"总仓位 {(current_total_pos_value + position_value)/current_portfolio_value:.2%} 超过上限 ({MAX_TOTAL_POS:.2%})"

        # Rule 4: Daily max loss 2% (Circuit Breaker)
        if daily_pnl / current_portfolio_value < -MAX_DAILY_LOSS_PCT:
             self.xing_bu.record_circuit_break(strategy_id, f"Daily loss {daily_pnl/current_portfolio_value:.2%} < -{MAX_DAILY_LOSS_PCT:.2%}", signal['dt'])
             return False, f"单日亏损 {daily_pnl/current_portfolio_value:.2%} 触发熔断 ({MAX_DAILY_LOSS_PCT:.2%})"

        # Rule 5: Consecutive 3 losses -> Stop opening today
        # This state needs to be tracked externally or passed in.
        # Assuming we track it in strategy state or pass it here.
        if self.consecutive_losses.get(strategy_id, 0) >= CONSECUTIVE_LOSS_LIMIT:
             self.xing_bu.record_rejection(strategy_id, 'R5', f"Consecutive losses {self.consecutive_losses.get(strategy_id)} >= {CONSECUTIVE_LOSS_LIMIT}", signal['dt'])
             return False, f"连续亏损 {self.consecutive_losses.get(strategy_id)} 次，暂停开仓"
        
        # Rule 6: Max Drawdown > 10% -> Reduce frequency (implementation detail: reject if frequent?)
        # This is harder to implement as a strict rule without frequency definition. 
        # For now, we log it.

        # Rule 7: Banned stocks (ST, etc.) - Should be handled by Crown Prince, but double check here?
        # Rule 8: Limit Up/Down - Should be handled by Crown Prince.
        
        # Rule 9: 1-min single order per stock
        # This implies we shouldn't have multiple signals for the same stock in the same minute.
        
        return True, "批准执行"

    def update_loss_count(self, strategy_id, is_loss):
        if is_loss:
            self.consecutive_losses[strategy_id] = self.consecutive_losses.get(strategy_id, 0) + 1
        else:
            self.consecutive_losses[strategy_id] = 0
