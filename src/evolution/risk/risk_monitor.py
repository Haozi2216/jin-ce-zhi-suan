"""
风险暴露度实时监控系统
提供策略运行过程中的实时风险监控和预警
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
from concurrent.futures import ThreadPoolExecutor


class RiskAlertLevel(Enum):
    """风险预警等级"""
    NORMAL = "normal"       # 正常
    ATTENTION = "attention" # 关注
    WARNING = "warning"     # 警告
    CRITICAL = "critical"   # 严重
    EMERGENCY = "emergency" # 紧急


class RiskMetricType(Enum):
    """风险指标类型"""
    POSITION_RISK = "position_risk"       # 持仓风险
    MARKET_RISK = "market_risk"           # 市场风险
    LIQUIDITY_RISK = "liquidity_risk"     # 流动性风险
    CONCENTRATION_RISK = "concentration_risk" # 集中度风险
    DRAWDOWN_RISK = "drawdown_risk"       # 回撤风险
    VOLATILITY_RISK = "volatility_risk"   # 波动率风险
    LEVERAGE_RISK = "leverage_risk"       # 杠杆风险


@dataclass
class RiskThreshold:
    """风险阈值"""
    metric_type: RiskMetricType
    attention_threshold: float  # 关注阈值
    warning_threshold: float    # 警告阈值
    critical_threshold: float   # 严重阈值
    emergency_threshold: float  # 紧急阈值
    description: str = ""


@dataclass
class RiskAlert:
    """风险预警"""
    alert_id: str
    strategy_id: str
    metric_type: RiskMetricType
    level: RiskAlertLevel
    current_value: float
    threshold_value: float
    message: str
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskSnapshot:
    """风险快照"""
    strategy_id: str
    timestamp: datetime
    total_value: float
    net_asset: float
    cash: float
    positions: List[Dict[str, Any]]
    risk_metrics: Dict[str, float]
    alerts: List[RiskAlert]
    market_data: Dict[str, Any]


class RiskCalculator:
    """风险计算器"""
    
    @staticmethod
    def calculate_position_risk(snapshot: RiskSnapshot) -> float:
        """计算持仓风险"""
        if not snapshot.positions or snapshot.total_value == 0:
            return 0.0
        
        # 计算持仓集中度（最大持仓比例）
        max_position_ratio = max(
            pos.get('value', 0) / snapshot.total_value 
            for pos in snapshot.positions
        )
        
        # 计算持仓数量风险
        position_count = len(snapshot.positions)
        count_risk = max(0, (10 - position_count) / 10)  # 持仓越少风险越高
        
        # 综合持仓风险
        position_risk = (max_position_ratio * 0.7 + count_risk * 0.3) * 100
        
        return min(100, position_risk)
    
    @staticmethod
    def calculate_market_risk(snapshot: RiskSnapshot) -> float:
        """计算市场风险"""
        market_data = snapshot.market_data
        
        # 获取市场波动率
        market_volatility = market_data.get('market_volatility', 0.2)
        
        # 获取Beta暴露
        beta_exposure = market_data.get('portfolio_beta', 1.0)
        
        # 计算市场风险分数
        market_risk = abs(beta_exposure - 1.0) * 50 + market_volatility * 100
        
        return min(100, market_risk)
    
    @staticmethod
    def calculate_liquidity_risk(snapshot: RiskSnapshot) -> float:
        """计算流动性风险"""
        if snapshot.total_value == 0:
            return 0.0
        
        # 现金比例
        cash_ratio = snapshot.cash / snapshot.total_value
        
        # 计算平均流动性（基于成交额）
        total_liquidity_score = 0
        total_weight = 0
        
        for position in snapshot.positions:
            value = position.get('value', 0)
            liquidity_score = position.get('liquidity_score', 0.5)  # 0-1之间
            weight = value / snapshot.total_value
            
            total_liquidity_score += liquidity_score * weight
            total_weight += weight
        
        avg_liquidity = total_liquidity_score / total_weight if total_weight > 0 else 0.5
        
        # 流动性风险（现金比例越低、平均流动性越差，风险越高）
        liquidity_risk = (1 - cash_ratio) * 30 + (1 - avg_liquidity) * 70
        
        return min(100, liquidity_risk)
    
    @staticmethod
    def calculate_concentration_risk(snapshot: RiskSnapshot) -> float:
        """计算集中度风险"""
        if not snapshot.positions or snapshot.total_value == 0:
            return 0.0
        
        # 计算行业集中度
        sector_exposure = {}
        for position in snapshot.positions:
            sector = position.get('sector', '其他')
            value = position.get('value', 0)
            sector_exposure[sector] = sector_exposure.get(sector, 0) + value
        
        max_sector_ratio = max(
            exposure / snapshot.total_value 
            for exposure in sector_exposure.values()
        )
        
        # 计算个股集中度
        max_stock_ratio = max(
            pos.get('value', 0) / snapshot.total_value 
            for pos in snapshot.positions
        )
        
        # 综合集中度风险
        concentration_risk = (max_sector_ratio * 0.4 + max_stock_ratio * 0.6) * 100
        
        return min(100, concentration_risk)
    
    @staticmethod
    def calculate_drawdown_risk(snapshot: RiskSnapshot) -> float:
        """计算回撤风险"""
        risk_metrics = snapshot.risk_metrics
        
        current_drawdown = risk_metrics.get('current_drawdown', 0)
        max_drawdown = risk_metrics.get('max_drawdown', 0)
        drawdown_duration = risk_metrics.get('drawdown_duration', 0)
        
        # 回撤风险评分
        dd_risk = current_drawdown * 100  # 当前回撤权重100%
        
        # 如果接近历史最大回撤，增加风险
        if max_drawdown > 0:
            dd_ratio = current_drawdown / max_drawdown
            if dd_ratio > 0.8:
                dd_risk += 20
        
        # 回撤持续时间风险
        duration_risk = min(20, drawdown_duration / 30)  # 按月计算，最高20分
        
        total_drawdown_risk = dd_risk + duration_risk
        
        return min(100, total_drawdown_risk)
    
    @staticmethod
    def calculate_volatility_risk(snapshot: RiskSnapshot) -> float:
        """计算波动率风险"""
        risk_metrics = snapshot.risk_metrics
        
        portfolio_volatility = risk_metrics.get('portfolio_volatility', 0.15)
        downside_volatility = risk_metrics.get('downside_volatility', 0.1)
        
        # 波动率风险评分
        vol_risk = portfolio_volatility * 200  # 年化波动率转换为风险分数
        downside_risk = downside_volatility * 300  # 下行波动率权重更高
        
        total_volatility_risk = (vol_risk * 0.6 + downside_risk * 0.4)
        
        return min(100, total_volatility_risk)
    
    @staticmethod
    def calculate_leverage_risk(snapshot: RiskSnapshot) -> float:
        """计算杠杆风险"""
        if snapshot.net_asset <= 0:
            return 100.0  # 净资产为负，风险极高
        
        leverage = snapshot.total_value / snapshot.net_asset
        
        # 杠杆风险评分
        if leverage <= 1.0:
            leverage_risk = 0
        elif leverage <= 1.5:
            leverage_risk = (leverage - 1.0) * 40
        elif leverage <= 2.0:
            leverage_risk = 20 + (leverage - 1.5) * 60
        else:
            leverage_risk = 50 + (leverage - 2.0) * 100
        
        return min(100, leverage_risk)


class RiskMonitor:
    """风险监控器"""
    
    def __init__(self, strategy_id: str):
        self.strategy_id = strategy_id
        self.thresholds = self._initialize_thresholds()
        self.alerts: List[RiskAlert] = []
        self.snapshots: deque = deque(maxlen=1000)  # 保留最近1000个快照
        self.alert_callbacks: List[Callable[[RiskAlert], None]] = []
        self._lock = threading.Lock()
        
        # 风险计算器映射
        self.calculators = {
            RiskMetricType.POSITION_RISK: RiskCalculator.calculate_position_risk,
            RiskMetricType.MARKET_RISK: RiskCalculator.calculate_market_risk,
            RiskMetricType.LIQUIDITY_RISK: RiskCalculator.calculate_liquidity_risk,
            RiskMetricType.CONCENTRATION_RISK: RiskCalculator.calculate_concentration_risk,
            RiskMetricType.DRAWDOWN_RISK: RiskCalculator.calculate_drawdown_risk,
            RiskMetricType.VOLATILITY_RISK: RiskCalculator.calculate_volatility_risk,
            RiskMetricType.LEVERAGE_RISK: RiskCalculator.calculate_leverage_risk
        }
    
    def _initialize_thresholds(self) -> Dict[RiskMetricType, RiskThreshold]:
        """初始化风险阈值"""
        return {
            RiskMetricType.POSITION_RISK: RiskThreshold(
                metric_type=RiskMetricType.POSITION_RISK,
                attention_threshold=30,
                warning_threshold=50,
                critical_threshold=70,
                emergency_threshold=85,
                description="持仓集中度过高"
            ),
            RiskMetricType.MARKET_RISK: RiskThreshold(
                metric_type=RiskMetricType.MARKET_RISK,
                attention_threshold=25,
                warning_threshold=40,
                critical_threshold=60,
                emergency_threshold=80,
                description="市场风险暴露过高"
            ),
            RiskMetricType.LIQUIDITY_RISK: RiskThreshold(
                metric_type=RiskMetricType.LIQUIDITY_RISK,
                attention_threshold=30,
                warning_threshold=50,
                critical_threshold=70,
                emergency_threshold=90,
                description="流动性不足"
            ),
            RiskMetricType.CONCENTRATION_RISK: RiskThreshold(
                metric_type=RiskMetricType.CONCENTRATION_RISK,
                attention_threshold=35,
                warning_threshold=55,
                critical_threshold=75,
                emergency_threshold=90,
                description="集中度风险过高"
            ),
            RiskMetricType.DRAWDOWN_RISK: RiskThreshold(
                metric_type=RiskMetricType.DRAWDOWN_RISK,
                attention_threshold=20,
                warning_threshold=35,
                critical_threshold=50,
                emergency_threshold=70,
                description="回撤风险过高"
            ),
            RiskMetricType.VOLATILITY_RISK: RiskThreshold(
                metric_type=RiskMetricType.VOLATILITY_RISK,
                attention_threshold=30,
                warning_threshold=45,
                critical_threshold=65,
                emergency_threshold=85,
                description="波动率风险过高"
            ),
            RiskMetricType.LEVERAGE_RISK: RiskThreshold(
                metric_type=RiskMetricType.LEVERAGE_RISK,
                attention_threshold=20,
                warning_threshold=40,
                critical_threshold=60,
                emergency_threshold=80,
                description="杠杆率过高"
            )
        }
    
    def add_alert_callback(self, callback: Callable[[RiskAlert], None]) -> None:
        """添加预警回调函数"""
        self.alert_callbacks.append(callback)
    
    def remove_alert_callback(self, callback: Callable[[RiskAlert], None]) -> bool:
        """移除预警回调函数"""
        try:
            self.alert_callbacks.remove(callback)
            return True
        except ValueError:
            return False
    
    def update_snapshot(self, snapshot: RiskSnapshot) -> None:
        """更新风险快照"""
        with self._lock:
            self.snapshots.append(snapshot)
        
        # 计算风险指标并检查预警
        self._check_risk_alerts(snapshot)
    
    def _check_risk_alerts(self, snapshot: RiskSnapshot) -> None:
        """检查风险预警"""
        new_alerts = []
        
        for metric_type, calculator in self.calculators.items():
            try:
                current_value = calculator(snapshot)
                threshold = self.thresholds[metric_type]
                
                # 确定预警等级
                alert_level = self._determine_alert_level(current_value, threshold)
                
                if alert_level != RiskAlertLevel.NORMAL:
                    alert = RiskAlert(
                        alert_id=f"{self.strategy_id}_{metric_type.value}_{int(time.time())}",
                        strategy_id=self.strategy_id,
                        metric_type=metric_type,
                        level=alert_level,
                        current_value=current_value,
                        threshold_value=self._get_threshold_value(current_value, threshold),
                        message=self._generate_alert_message(metric_type, alert_level, current_value),
                        timestamp=datetime.now(),
                        details={
                            "snapshot_time": snapshot.timestamp.isoformat(),
                            "total_value": snapshot.total_value,
                            "net_asset": snapshot.net_asset
                        }
                    )
                    new_alerts.append(alert)
                    
            except Exception as e:
                logging.error(f"风险指标计算失败 {metric_type.value}: {e}")
        
        # 添加新预警
        if new_alerts:
            with self._lock:
                self.alerts.extend(new_alerts)
            
            # 触发回调
            for alert in new_alerts:
                for callback in self.alert_callbacks:
                    try:
                        callback(alert)
                    except Exception as e:
                        logging.error(f"预警回调执行失败: {e}")
    
    def _determine_alert_level(self, value: float, threshold: RiskThreshold) -> RiskAlertLevel:
        """确定预警等级"""
        if value >= threshold.emergency_threshold:
            return RiskAlertLevel.EMERGENCY
        elif value >= threshold.critical_threshold:
            return RiskAlertLevel.CRITICAL
        elif value >= threshold.warning_threshold:
            return RiskAlertLevel.WARNING
        elif value >= threshold.attention_threshold:
            return RiskAlertLevel.ATTENTION
        else:
            return RiskAlertLevel.NORMAL
    
    def _get_threshold_value(self, value: float, threshold: RiskThreshold) -> float:
        """获取触发的阈值"""
        if value >= threshold.emergency_threshold:
            return threshold.emergency_threshold
        elif value >= threshold.critical_threshold:
            return threshold.critical_threshold
        elif value >= threshold.warning_threshold:
            return threshold.warning_threshold
        elif value >= threshold.attention_threshold:
            return threshold.attention_threshold
        else:
            return threshold.attention_threshold
    
    def _generate_alert_message(self, metric_type: RiskMetricType, 
                               level: RiskAlertLevel, value: float) -> str:
        """生成预警消息"""
        level_names = {
            RiskAlertLevel.ATTENTION: "关注",
            RiskAlertLevel.WARNING: "警告",
            RiskAlertLevel.CRITICAL: "严重",
            RiskAlertLevel.EMERGENCY: "紧急"
        }
        
        metric_names = {
            RiskMetricType.POSITION_RISK: "持仓风险",
            RiskMetricType.MARKET_RISK: "市场风险",
            RiskMetricType.LIQUIDITY_RISK: "流动性风险",
            RiskMetricType.CONCENTRATION_RISK: "集中度风险",
            RiskMetricType.DRAWDOWN_RISK: "回撤风险",
            RiskMetricType.VOLATILITY_RISK: "波动率风险",
            RiskMetricType.LEVERAGE_RISK: "杠杆风险"
        }
        
        return f"{level_names[level]}：{metric_names[metric_type]}达到{value:.2f}分"
    
    def get_current_risk_status(self) -> Dict[str, Any]:
        """获取当前风险状态"""
        with self._lock:
            if not self.snapshots:
                return {"status": "no_data"}
            
            latest_snapshot = self.snapshots[-1]
            
            # 计算当前风险指标
            current_metrics = {}
            for metric_type, calculator in self.calculators.items():
                try:
                    current_metrics[metric_type.value] = calculator(latest_snapshot)
                except Exception as e:
                    current_metrics[metric_type.value] = None
            
            # 统计预警情况
            active_alerts = [a for a in self.alerts if not a.resolved]
            alert_counts = {}
            for alert in active_alerts:
                level = alert.level.value
                alert_counts[level] = alert_counts.get(level, 0) + 1
            
            return {
                "strategy_id": self.strategy_id,
                "timestamp": latest_snapshot.timestamp.isoformat(),
                "total_value": latest_snapshot.total_value,
                "net_asset": latest_snapshot.net_asset,
                "risk_metrics": current_metrics,
                "active_alerts": len(active_alerts),
                "alert_counts": alert_counts,
                "latest_alerts": [
                    {
                        "metric_type": alert.metric_type.value,
                        "level": alert.level.value,
                        "message": alert.message,
                        "timestamp": alert.timestamp.isoformat()
                    }
                    for alert in sorted(active_alerts, key=lambda x: x.timestamp, reverse=True)[:5]
                ]
            }
    
    def get_risk_history(self, hours: int = 24) -> Dict[str, Any]:
        """获取风险历史"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self._lock:
            # 过滤快照
            filtered_snapshots = [
                s for s in self.snapshots 
                if s.timestamp >= cutoff_time
            ]
            
            # 过滤预警
            filtered_alerts = [
                a for a in self.alerts 
                if a.timestamp >= cutoff_time
            ]
            
            # 构建时间序列数据
            timeline = []
            for snapshot in filtered_snapshots:
                metrics = {}
                for metric_type, calculator in self.calculators.items():
                    try:
                        metrics[metric_type.value] = calculator(snapshot)
                    except Exception:
                        metrics[metric_type.value] = None
                
                timeline.append({
                    "timestamp": snapshot.timestamp.isoformat(),
                    "total_value": snapshot.total_value,
                    "net_asset": snapshot.net_asset,
                    "risk_metrics": metrics
                })
            
            return {
                "strategy_id": self.strategy_id,
                "period_hours": hours,
                "snapshot_count": len(filtered_snapshots),
                "alert_count": len(filtered_alerts),
                "timeline": timeline,
                "alerts": [
                    {
                        "alert_id": alert.alert_id,
                        "metric_type": alert.metric_type.value,
                        "level": alert.level.value,
                        "message": alert.message,
                        "current_value": alert.current_value,
                        "timestamp": alert.timestamp.isoformat(),
                        "resolved": alert.resolved
                    }
                    for alert in sorted(filtered_alerts, key=lambda x: x.timestamp, reverse=True)
                ]
            }
    
    def resolve_alert(self, alert_id: str) -> bool:
        """解决预警"""
        with self._lock:
            for alert in self.alerts:
                if alert.alert_id == alert_id and not alert.resolved:
                    alert.resolved = True
                    alert.resolved_at = datetime.now()
                    return True
        return False
    
    def update_threshold(self, metric_type: RiskMetricType, 
                        attention: Optional[float] = None,
                        warning: Optional[float] = None,
                        critical: Optional[float] = None,
                        emergency: Optional[float] = None) -> bool:
        """更新风险阈值"""
        if metric_type not in self.thresholds:
            return False
        
        with self._lock:
            threshold = self.thresholds[metric_type]
            
            if attention is not None:
                threshold.attention_threshold = attention
            if warning is not None:
                threshold.warning_threshold = warning
            if critical is not None:
                threshold.critical_threshold = critical
            if emergency is not None:
                threshold.emergency_threshold = emergency
        
        return True


class RiskMonitoringSystem:
    """风险监控系统"""
    
    def __init__(self):
        self.monitors: Dict[str, RiskMonitor] = {}
        self.global_callbacks: List[Callable[[RiskAlert], None]] = []
        self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="risk_monitor")
        self._running = False
        self._monitor_thread = None
    
    def add_strategy(self, strategy_id: str) -> RiskMonitor:
        """添加策略监控"""
        if strategy_id not in self.monitors:
            monitor = RiskMonitor(strategy_id)
            
            # 添加全局回调
            for callback in self.global_callbacks:
                monitor.add_alert_callback(callback)
            
            self.monitors[strategy_id] = monitor
            logging.info(f"添加策略风险监控: {strategy_id}")
        
        return self.monitors[strategy_id]
    
    def remove_strategy(self, strategy_id: str) -> bool:
        """移除策略监控"""
        if strategy_id in self.monitors:
            del self.monitors[strategy_id]
            logging.info(f"移除策略风险监控: {strategy_id}")
            return True
        return False
    
    def add_global_alert_callback(self, callback: Callable[[RiskAlert], None]) -> None:
        """添加全局预警回调"""
        self.global_callbacks.append(callback)
        
        # 为现有监控器添加回调
        for monitor in self.monitors.values():
            monitor.add_alert_callback(callback)
    
    def update_strategy_snapshot(self, strategy_id: str, snapshot: RiskSnapshot) -> None:
        """更新策略快照"""
        if strategy_id not in self.monitors:
            self.add_strategy(strategy_id)
        
        self.monitors[strategy_id].update_snapshot(snapshot)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        total_alerts = 0
        active_alerts = 0
        strategy_statuses = {}
        
        for strategy_id, monitor in self.monitors.items():
            status = monitor.get_current_risk_status()
            strategy_statuses[strategy_id] = status
            
            total_alerts += len(monitor.alerts)
            active_alerts += status.get("active_alerts", 0)
        
        return {
            "monitoring_strategies": len(self.monitors),
            "total_alerts": total_alerts,
            "active_alerts": active_alerts,
            "system_running": self._running,
            "strategy_statuses": strategy_statuses
        }
    
    def get_cross_strategy_risk_summary(self) -> Dict[str, Any]:
        """获取跨策略风险摘要"""
        if not self.monitors:
            return {"status": "no_strategies"}
        
        # 聚合所有策略的风险指标
        all_metrics = {}
        total_value = 0
        total_net_asset = 0
        
        for monitor in self.monitors.values():
            status = monitor.get_current_risk_status()
            
            if status.get("status") != "no_data":
                total_value += status.get("total_value", 0)
                total_net_asset += status.get("net_asset", 0)
                
                metrics = status.get("risk_metrics", {})
                for metric_name, value in metrics.items():
                    if value is not None:
                        if metric_name not in all_metrics:
                            all_metrics[metric_name] = []
                        all_metrics[metric_name].append(value)
        
        # 计算平均风险指标
        avg_metrics = {}
        for metric_name, values in all_metrics.items():
            if values:
                avg_metrics[metric_name] = np.mean(values)
        
        # 计算系统级风险
        system_leverage = total_value / total_net_asset if total_net_asset > 0 else 0
        
        return {
            "total_strategies": len(self.monitors),
            "total_value": total_value,
            "total_net_asset": total_net_asset,
            "system_leverage": system_leverage,
            "average_risk_metrics": avg_metrics,
            "risk_distribution": {
                metric_name: {
                    "min": min(values),
                    "max": max(values),
                    "avg": np.mean(values),
                    "std": np.std(values)
                }
                for metric_name, values in all_metrics.items() if values
            }
        }


# 全局风险监控系统实例
risk_monitoring_system = RiskMonitoringSystem()