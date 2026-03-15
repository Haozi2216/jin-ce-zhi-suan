# src/core/zhongshu_sheng.py

class ZhongshuSheng:
    """
    中书省 (Secretariat): 为每套策略独立生成买卖信号
    """
    def __init__(self, strategies):
        self.strategies = strategies # List of strategy instances

    def generate_signals(self, kline):
        """
        Generate signals for all strategies for the current bar.
        """
        signals = []
        for strategy in self.strategies:
            signal = strategy.on_bar(kline)
            if signal:
                signals.append(signal)
        return signals

    def update_strategy_state(self, strategy_id, code, position_qty):
        """
        Update strategy state after execution.
        """
        for strategy in self.strategies:
            if strategy.id == strategy_id:
                strategy.update_position(code, position_qty)
                break
