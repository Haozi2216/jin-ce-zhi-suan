"""
优化版风险监控系统
通过缓存、批处理、异步计算等方式提升性能
"""
import time
import threading
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import deque
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools
import hashlib
from collections import OrderedDict
try:
    from cachetools import TTLCache, LRUCache
except ImportError:
    class TTLCache(dict):
        def __init__(self, maxsize=128, ttl=300):
            super().__init__()
            self.maxsize = max(1, int(maxsize or 128))
            self.ttl = float(ttl or 0)
            self._expires_at = {}

        def _purge(self):
            now = time.time()
            expired_keys = [key for key, expires_at in list(self._expires_at.items()) if expires_at <= now]
            for key in expired_keys:
                super().pop(key, None)
                self._expires_at.pop(key, None)
            while len(self) > self.maxsize:
                oldest_key = next(iter(self))
                super().pop(oldest_key, None)
                self._expires_at.pop(oldest_key, None)

        def __contains__(self, key):
            self._purge()
            return super().__contains__(key)

        def __getitem__(self, key):
            self._purge()
            return super().__getitem__(key)

        def __setitem__(self, key, value):
            self._purge()
            super().__setitem__(key, value)
            self._expires_at[key] = time.time() + self.ttl
            self._purge()

        def clear(self):
            super().clear()
            self._expires_at.clear()

    class LRUCache(OrderedDict):
        def __init__(self, maxsize=128):
            super().__init__()
            self.maxsize = max(1, int(maxsize or 128))

        def __getitem__(self, key):
            value = super().__getitem__(key)
            self.move_to_end(key)
            return value

        def __setitem__(self, key, value):
            if key in self:
                self.move_to_end(key)
            super().__setitem__(key, value)
            while len(self) > self.maxsize:
                self.popitem(last=False)

        def clear(self):
            super().clear()
import multiprocessing as mp
from queue import Queue, Empty


class RiskMetricType(Enum):
    """风险指标类型"""
    POSITION_RISK = "position_risk"
    MARKET_RISK = "market_risk"
    LIQUIDITY_RISK = "liquidity_risk"
    CONCENTRATION_RISK = "concentration_risk"
    DRAWDOWN_RISK = "drawdown_risk"
    VOLATILITY_RISK = "volatility_risk"
    LEVERAGE_RISK = "leverage_risk"


@dataclass
class CachedRiskSnapshot:
    """缓存的风险快照"""
    strategy_id: str
    timestamp: datetime
    total_value: float
    net_asset: float
    cash: float
    positions_hash: str  # 持仓数据哈希，用于缓存比较
    risk_metrics: Dict[str, float] = field(default_factory=dict)
    computed_metrics: set = field(default_factory=set)  # 已计算的指标


class PerformanceOptimizedRiskCalculator:
    """性能优化的风险计算器"""
    
    def __init__(self):
        # 计算结果缓存，TTL 5分钟
        self.calculation_cache = TTLCache(maxsize=1000, ttl=300)
        
        # 中间结果缓存，LRU策略
        self.intermediate_cache = LRUCache(maxsize=500)
        
        # 预计算的权重矩阵
        self._precompute_weights()
        
        # 批处理队列
        self.batch_queue = Queue(maxsize=1000)
        self.batch_processor = None
        self._start_batch_processor()
    
    def _precompute_weights(self):
        """预计算权重矩阵"""
        self.position_weights = np.array([0.7, 0.3])  # 持仓风险权重
        self.concentration_weights = np.array([0.4, 0.6])  # 集中度风险权重
        self.volatility_weights = np.array([0.6, 0.4])  # 波动率风险权重
    
    @functools.lru_cache(maxsize=256)
    def _calculate_position_hash(self, positions_data: str) -> str:
        """计算持仓数据哈希（带缓存）"""
        return hashlib.md5(positions_data.encode()).hexdigest()
    
    def calculate_position_risk_optimized(self, snapshot: CachedRiskSnapshot) -> float:
        """优化的持仓风险计算"""
        # 生成缓存键
        positions_str = json.dumps(snapshot.positions, sort_keys=True)
        cache_key = f"position_risk_{snapshot.positions_hash}"
        
        # 检查缓存
        if cache_key in self.calculation_cache:
            return self.calculation_cache[cache_key]
        
        if not snapshot.positions or snapshot.total_value == 0:
            return 0.0
        
        # 向量化计算
        position_values = np.array([pos.get('value', 0) for pos in snapshot.positions])
        position_ratios = position_values / snapshot.total_value
        
        # 使用numpy向量化操作
        max_position_ratio = np.max(position_ratios)
        position_count = len(snapshot.positions)
        count_risk = max(0, (10 - position_count) / 10)
        
        # 矩阵运算计算风险
        risk_vector = np.array([max_position_ratio, count_risk])
        position_risk = np.dot(risk_vector, self.position_weights) * 100
        
        result = min(100, position_risk)
        
        # 缓存结果
        self.calculation_cache[cache_key] = result
        
        return result
    
    def calculate_concentration_risk_optimized(self, snapshot: CachedRiskSnapshot) -> float:
        """优化的集中度风险计算"""
        cache_key = f"concentration_risk_{snapshot.positions_hash}"
        
        if cache_key in self.calculation_cache:
            return self.calculation_cache[cache_key]
        
        if not snapshot.positions or snapshot.total_value == 0:
            return 0.0
        
        # 使用pandas进行高效分组计算
        positions_df = pd.DataFrame(snapshot.positions)
        
        # 计算行业集中度
        sector_exposure = positions_df.groupby('sector')['value'].sum()
        max_sector_ratio = sector_exposure.max() / snapshot.total_value
        
        # 计算个股集中度
        position_values = positions_df['value'].values
        max_stock_ratio = np.max(position_values) / snapshot.total_value
        
        # 向量化计算
        risk_vector = np.array([max_sector_ratio, max_stock_ratio])
        concentration_risk = np.dot(risk_vector, self.concentration_weights) * 100
        
        result = min(100, concentration_risk)
        
        # 缓存结果
        self.calculation_cache[cache_key] = result
        
        return result
    
    def calculate_volatility_risk_optimized(self, snapshot: CachedRiskSnapshot) -> float:
        """优化的波动率风险计算"""
        risk_metrics = snapshot.risk_metrics
        metrics_key = f"vol_metrics_{hash(tuple(sorted(risk_metrics.items())))}"
        
        if metrics_key in self.intermediate_cache:
            portfolio_volatility, downside_volatility = self.intermediate_cache[metrics_key]
        else:
            portfolio_volatility = risk_metrics.get('portfolio_volatility', 0.15)
            downside_volatility = risk_metrics.get('downside_volatility', 0.1)
            self.intermediate_cache[metrics_key] = (portfolio_volatility, downside_volatility)
        
        # 向量化计算
        vol_risk = portfolio_volatility * 200
        downside_risk = downside_volatility * 300
        
        risk_vector = np.array([vol_risk, downside_risk])
        total_volatility_risk = np.dot(risk_vector, self.volatility_weights)
        
        return min(100, total_volatility_risk)
    
    def batch_calculate_metrics(self, snapshots: List[CachedRiskSnapshot]) -> Dict[str, Dict[str, float]]:
        """批量计算风险指标"""
        results = {}
        
        # 按指标类型分组计算
        for snapshot in snapshots:
            strategy_id = snapshot.strategy_id
            results[strategy_id] = {}
            
            # 并行计算不同指标
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(self.calculate_position_risk_optimized, snapshot): 'position_risk',
                    executor.submit(self.calculate_concentration_risk_optimized, snapshot): 'concentration_risk',
                    executor.submit(self.calculate_volatility_risk_optimized, snapshot): 'volatility_risk'
                }
                
                for future in as_completed(futures):
                    metric_type = futures[future]
                    try:
                        results[strategy_id][metric_type] = future.result(timeout=5)
                    except Exception as e:
                        logging.error(f"批量计算失败 {metric_type}: {e}")
                        results[strategy_id][metric_type] = 0.0
        
        return results
    
    def _start_batch_processor(self):
        """启动批处理器"""
        self.batch_processor = threading.Thread(target=self._batch_process_loop, daemon=True)
        self.batch_processor.start()
    
    def _batch_process_loop(self):
        """批处理循环"""
        batch_size = 10
        batch_timeout = 1.0  # 1秒超时
        
        while True:
            batch = []
            start_time = time.time()
            
            # 收集批次数据
            while len(batch) < batch_size and (time.time() - start_time) < batch_timeout:
                try:
                    item = self.batch_queue.get(timeout=0.1)
                    batch.append(item)
                except Empty:
                    continue
            
            # 处理批次
            if batch:
                try:
                    self._process_batch(batch)
                except Exception as e:
                    logging.error(f"批处理失败: {e}")
    
    def _process_batch(self, batch: List[Tuple]):
        """处理批次数据"""
        # 这里可以实现具体的批处理逻辑
        pass
    
    def clear_cache(self):
        """清理缓存"""
        self.calculation_cache.clear()
        self.intermediate_cache.clear()
        # 清理LRU缓存
        self._calculate_position_hash.cache_clear()


class AsyncRiskMonitor:
    """异步风险监控器"""
    
    def __init__(self, strategy_id: str):
        self.strategy_id = strategy_id
        self.calculator = PerformanceOptimizedRiskCalculator()
        
        # 异步执行器
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"risk_{strategy_id}")
        
        # 快照缓存
        self.snapshot_cache = TTLCache(maxsize=100, ttl=60)  # 1分钟缓存
        
        # 计算任务队列
        self.calculation_queue = asyncio.Queue(maxsize=50)
        
        # 事件循环
        self.loop = None
        self._running = False
        
        # 性能统计
        self.performance_stats = {
            'calculations': 0,
            'cache_hits': 0,
            'avg_calculation_time': 0.0,
            'total_calculation_time': 0.0
        }
    
    async def start(self):
        """启动异步监控"""
        self._running = True
        self.loop = asyncio.get_event_loop()
        
        # 启动计算任务处理器
        asyncio.create_task(self._calculation_worker())
    
    async def stop(self):
        """停止异步监控"""
        self._running = False
        self.executor.shutdown(wait=True)
    
    async def update_snapshot_async(self, snapshot_data: Dict[str, Any]) -> Dict[str, float]:
        """异步更新快照并计算风险"""
        start_time = time.time()
        
        # 创建缓存快照
        positions_str = json.dumps(snapshot_data.get('positions', []), sort_keys=True)
        positions_hash = self.calculator._calculate_position_hash(positions_str)
        
        cached_snapshot = CachedRiskSnapshot(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(),
            total_value=snapshot_data.get('total_value', 0),
            net_asset=snapshot_data.get('net_asset', 0),
            cash=snapshot_data.get('cash', 0),
            positions=snapshot_data.get('positions', []),
            positions_hash=positions_hash,
            risk_metrics=snapshot_data.get('risk_metrics', {})
        )
        
        # 检查缓存
        cache_key = f"{self.strategy_id}_{positions_hash}"
        if cache_key in self.snapshot_cache:
            self.performance_stats['cache_hits'] += 1
            return self.snapshot_cache[cache_key]
        
        # 异步计算风险指标
        risk_metrics = await self._calculate_risk_metrics_async(cached_snapshot)
        
        # 更新缓存
        self.snapshot_cache[cache_key] = risk_metrics
        
        # 更新性能统计
        calculation_time = time.time() - start_time
        self.performance_stats['calculations'] += 1
        self.performance_stats['total_calculation_time'] += calculation_time
        self.performance_stats['avg_calculation_time'] = (
            self.performance_stats['total_calculation_time'] / 
            self.performance_stats['calculations']
        )
        
        return risk_metrics
    
    async def _calculate_risk_metrics_async(self, snapshot: CachedRiskSnapshot) -> Dict[str, float]:
        """异步计算风险指标"""
        # 使用线程池执行CPU密集型计算
        loop = asyncio.get_event_loop()
        
        tasks = [
            loop.run_in_executor(
                self.executor, 
                self.calculator.calculate_position_risk_optimized, 
                snapshot
            ),
            loop.run_in_executor(
                self.executor, 
                self.calculator.calculate_concentration_risk_optimized, 
                snapshot
            ),
            loop.run_in_executor(
                self.executor, 
                self.calculator.calculate_volatility_risk_optimized, 
                snapshot
            )
        ]
        
        # 并行执行计算
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        risk_metrics = {}
        metric_names = ['position_risk', 'concentration_risk', 'volatility_risk']
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"风险计算失败 {metric_names[i]}: {result}")
                risk_metrics[metric_names[i]] = 0.0
            else:
                risk_metrics[metric_names[i]] = result
        
        return risk_metrics
    
    async def _calculation_worker(self):
        """计算任务工作器"""
        while self._running:
            try:
                # 等待计算任务
                task = await asyncio.wait_for(
                    self.calculation_queue.get(), 
                    timeout=1.0
                )
                
                # 处理任务
                await self._process_calculation_task(task)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"计算任务处理失败: {e}")
    
    async def _process_calculation_task(self, task: Dict[str, Any]):
        """处理计算任务"""
        task_type = task.get('type')
        
        if task_type == 'update_snapshot':
            snapshot_data = task.get('data')
            await self.update_snapshot_async(snapshot_data)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        cache_hit_rate = (
            self.performance_stats['cache_hits'] / 
            max(1, self.performance_stats['calculations'] + self.performance_stats['cache_hits'])
        ) * 100
        
        return {
            'strategy_id': self.strategy_id,
            'total_calculations': self.performance_stats['calculations'],
            'cache_hits': self.performance_stats['cache_hits'],
            'cache_hit_rate': f"{cache_hit_rate:.2f}%",
            'avg_calculation_time': f"{self.performance_stats['avg_calculation_time']:.4f}s",
            'total_calculation_time': f"{self.performance_stats['total_calculation_time']:.2f}s"
        }


class OptimizedRiskMonitoringSystem:
    """优化的风险监控系统"""
    
    def __init__(self):
        self.monitors: Dict[str, AsyncRiskMonitor] = {}
        self.batch_calculator = PerformanceOptimizedRiskCalculator()
        
        # 全局性能统计
        self.global_stats = {
            'total_requests': 0,
            'total_calculation_time': 0.0,
            'cache_hits': 0,
            'active_monitors': 0
        }
        
        # 定期清理任务
        self.cleanup_task = None
        self._start_cleanup_task()
    
    async def add_strategy(self, strategy_id: str) -> AsyncRiskMonitor:
        """添加策略监控"""
        if strategy_id not in self.monitors:
            monitor = AsyncRiskMonitor(strategy_id)
            await monitor.start()
            self.monitors[strategy_id] = monitor
            self.global_stats['active_monitors'] += 1
            
            logging.info(f"添加优化风险监控: {strategy_id}")
        
        return self.monitors[strategy_id]
    
    async def remove_strategy(self, strategy_id: str) -> bool:
        """移除策略监控"""
        if strategy_id in self.monitors:
            monitor = self.monitors[strategy_id]
            await monitor.stop()
            del self.monitors[strategy_id]
            self.global_stats['active_monitors'] -= 1
            
            logging.info(f"移除优化风险监控: {strategy_id}")
            return True
        return False
    
    async def update_strategy_snapshot(self, strategy_id: str, 
                                     snapshot_data: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """更新策略快照"""
        if strategy_id not in self.monitors:
            await self.add_strategy(strategy_id)
        
        start_time = time.time()
        self.global_stats['total_requests'] += 1
        
        try:
            risk_metrics = await self.monitors[strategy_id].update_snapshot_async(snapshot_data)
            
            # 更新全局统计
            calculation_time = time.time() - start_time
            self.global_stats['total_calculation_time'] += calculation_time
            
            return risk_metrics
            
        except Exception as e:
            logging.error(f"更新策略快照失败 {strategy_id}: {e}")
            return None
    
    async def batch_update_snapshots(self, snapshots: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        """批量更新快照"""
        tasks = []
        
        for strategy_id, snapshot_data in snapshots.items():
            task = asyncio.create_task(
                self.update_strategy_snapshot(strategy_id, snapshot_data)
            )
            tasks.append((strategy_id, task))
        
        results = {}
        
        for strategy_id, task in tasks:
            try:
                result = await task
                if result:
                    results[strategy_id] = result
            except Exception as e:
                logging.error(f"批量更新失败 {strategy_id}: {e}")
        
        return results
    
    def get_system_performance_stats(self) -> Dict[str, Any]:
        """获取系统性能统计"""
        # 聚合所有监控器的性能统计
        monitor_stats = []
        for monitor in self.monitors.values():
            monitor_stats.append(monitor.get_performance_stats())
        
        # 计算全局统计
        avg_calculation_time = (
            self.global_stats['total_calculation_time'] / 
            max(1, self.global_stats['total_requests'])
        )
        
        return {
            'global_stats': {
                'total_requests': self.global_stats['total_requests'],
                'active_monitors': self.global_stats['active_monitors'],
                'avg_calculation_time': f"{avg_calculation_time:.4f}s",
                'total_calculation_time': f"{self.global_stats['total_calculation_time']:.2f}s"
            },
            'monitor_stats': monitor_stats,
            'cache_stats': {
                'calculation_cache_size': len(self.batch_calculator.calculation_cache),
                'intermediate_cache_size': len(self.batch_calculator.intermediate_cache)
            }
        }
    
    def _start_cleanup_task(self):
        """启动清理任务"""
        self.cleanup_task = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_task.start()
    
    def _cleanup_loop(self):
        """清理循环"""
        while True:
            try:
                # 每10分钟清理一次缓存
                time.sleep(600)
                
                # 清理计算器缓存
                self.batch_calculator.clear_cache()
                
                # 清理监控器缓存
                for monitor in self.monitors.values():
                    monitor.snapshot_cache.clear()
                
                logging.info("风险监控系统缓存清理完成")
                
            except Exception as e:
                logging.error(f"缓存清理失败: {e}")
    
    async def shutdown(self):
        """关闭系统"""
        # 停止所有监控器
        for monitor in self.monitors.values():
            await monitor.stop()
        
        self.monitors.clear()
        logging.info("优化风险监控系统已关闭")


# 全局优化风险监控系统实例
optimized_risk_monitoring_system = OptimizedRiskMonitoringSystem()