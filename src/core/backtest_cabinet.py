
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from src.core.crown_prince import CrownPrince
from src.core.zhongshu_sheng import ZhongshuSheng
from src.core.menxia_sheng import MenxiaSheng
from src.core.shangshu_sheng import ShangshuSheng
from src.ministries.li_bu_personnel import LiBuPersonnel
from src.ministries.hu_bu_revenue import HuBuRevenue
from src.ministries.li_bu_rites import LiBuRites
from src.ministries.bing_bu_war import BingBuWar
from src.ministries.xing_bu_justice import XingBuJustice
from src.ministries.gong_bu_works import GongBuWorks
import src.strategies.strategy_factory as strategy_factory_module
from src.utils.data_provider import DataProvider
from src.utils.tushare_provider import TushareProvider
from src.utils.akshare_provider import AkshareProvider
from src.utils.config_loader import ConfigLoader

class BacktestCabinet:
    def __init__(self, stock_code, strategy_id='all', initial_capital=1000000.0, event_callback=None):
        self.stock_code = stock_code
        self.strategy_id = strategy_id
        self.initial_capital = initial_capital
        self.event_callback = event_callback
        
        self.config = ConfigLoader()
        
        # Initialize Ministries
        self.personnel = LiBuPersonnel()
        self.revenue = HuBuRevenue(initial_capital)
        self.rites = LiBuRites()
        self.war = BingBuWar()
        self.justice = XingBuJustice()
        self.works = GongBuWorks()

        # Initialize Departments
        self.prince = CrownPrince()
        self.chancellery = MenxiaSheng(self.justice)
        self.state_affairs = ShangshuSheng(self.revenue, self.war, self.justice)

        # Initialize Strategies
        # Get the latest strategies every time we start a backtest
        all_strategies = strategy_factory_module.create_strategies()
        if strategy_id == 'all':
            self.strategies = all_strategies
        else:
            self.strategies = [s for s in all_strategies if s.id == strategy_id]
            
        self.secretariat = ZhongshuSheng(self.strategies)
        
        for s in self.strategies:
            self.personnel.register_strategy(s)

    async def _emit(self, event_type, data):
        if self.event_callback:
            await self.event_callback(event_type, data)

    async def run(self, start_date=None, end_date=None):
        if not start_date:
            start_date = datetime.now() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now()

        await self._emit('system', {'msg': f"开始回测 {self.stock_code} ({start_date.date()} - {end_date.date()})..."})
        
        # 1. Fetch Data
        provider_source = self.config.get("data_provider.source", "default")
        if provider_source == 'tushare':
            provider = TushareProvider(token=self.config.get("data_provider.tushare_token"))
        elif provider_source == 'akshare':
            provider = AkshareProvider()
        else:
            provider = DataProvider()
            
        df = provider.fetch_minute_data(self.stock_code, start_date, end_date)
        if df.empty:
            await self._emit('system', {'msg': f"❌ 无法获取 {self.stock_code} 的历史数据，回测终止。"})
            return

        df = self.works.clean_data(df)
        total_bars = len(df)
        await self._emit('system', {'msg': f"已获取 {total_bars} 条K线数据，正在初始化策略..."})

        # 2. Warm up strategies with initial data (optional, or just run)
        # Here we just run bar by bar
        
        # 3. Main Loop
        report_interval = max(1, total_bars // 50) # Report 50 times total
        
        for i, row in df.iterrows():
            # Check for cancellation (how? maybe checking a flag if running in task)
            # For now, just yield control
            if i % 100 == 0:
                await asyncio.sleep(0) # Yield
            
            if i % report_interval == 0:
                progress = int((i / total_bars) * 100)
                await self._emit('backtest_progress', {'progress': progress, 'current_date': str(row['dt'])})

            kline = row
            
            # Generate Signals
            signals = self.secretariat.generate_signals(kline)
            
            for signal in signals:
                sid = signal['strategy_id']
                
                # Check Risk
                # Approx fund value for backtest speed
                current_fund_value = self.revenue.cash + self.state_affairs.update_holdings_value({kline['code']: kline['close']})
                current_positions = self.state_affairs.positions.get(sid, {})
                
                approved, reason = self.chancellery.check_signal(signal, current_fund_value, current_positions, 0.0)
                
                if approved:
                    executed = self.state_affairs.execute_order(sid, signal, kline)
                    if executed:
                        new_qty = self.state_affairs.positions[sid][signal['code']]['qty'] if signal['code'] in self.state_affairs.positions.get(sid, {}) else 0
                        self.secretariat.update_strategy_state(sid, signal['code'], new_qty)
                        
                        # Emit trade log occasionally or accumulate?
                        # Sending every trade might flood WS if high freq.
                        # Let's send major trades.
                        await self._emit('backtest_trade', {
                            'dt': str(kline['dt']),
                            'strategy': sid,
                            'code': signal['code'],
                            'dir': signal['direction'],
                            'price': signal['price'],
                            'qty': signal['qty']
                        })

            # Check Stops
            triggered_orders = self.state_affairs.check_stops(kline)
            for order in triggered_orders:
                self.state_affairs.execute_order(order['strategy_id'], order, kline)
                self.secretariat.update_strategy_state(order['strategy_id'], order['code'], 0)
                await self._emit('backtest_trade', {
                            'dt': str(kline['dt']),
                            'strategy': order['strategy_id'],
                            'code': order['code'],
                            'dir': order['direction'],
                            'price': order['price'],
                            'qty': order['qty'],
                            'reason': 'STOP'
                        })

        # 4. Generate Report
        await self._emit('backtest_progress', {'progress': 100, 'current_date': 'Done'})
        
        reports = []
        for s in self.strategies:
            report = self.rites.generate_report(s.id, self.revenue, self.justice, self.initial_capital/len(self.strategies))
            reports.append(report)
            
        ranking = self.rites.generate_ranking(reports)
        
        # Convert ranking to dict for JSON
        ranking_dict = ranking.to_dict('records')
        
        await self._emit('backtest_result', {
            'stock': self.stock_code,
            'period': f"{start_date.date()} - {end_date.date()}",
            'ranking': ranking_dict,
            'total_trades': sum(r['total_trades'] for r in reports)
        })
        
        await self._emit('system', {'msg': f"回测完成。"})
