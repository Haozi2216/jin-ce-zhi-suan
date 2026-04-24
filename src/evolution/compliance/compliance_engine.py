"""
合规性规则引擎
确保策略生成和交易执行符合监管要求和内部风控规定
"""
import re
import ast
import json
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum
import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod


class ComplianceLevel(Enum):
    """合规等级"""
    PASS = "pass"           # 通过
    WARNING = "warning"     # 警告
    VIOLATION = "violation" # 违规
    CRITICAL = "critical"   # 严重违规


class ComplianceCategory(Enum):
    """合规类别"""
    TRADING_RULES = "trading_rules"       # 交易规则
    RISK_LIMITS = "risk_limits"           # 风险限制
    POSITION_LIMITS = "position_limits"   # 持仓限制
    MARKET_MANIPULATION = "market_manipulation" # 市场操纵
    INSIDER_TRADING = "insider_trading"   # 内幕交易
    CAPITAL_REQUIREMENTS = "capital_requirements" # 资本要求
    REPORTING = "reporting"               # 报告要求
    DATA_PRIVACY = "data_privacy"         # 数据隐私


@dataclass
class ComplianceRule:
    """合规规则"""
    rule_id: str
    name: str
    category: ComplianceCategory
    description: str
    level: ComplianceLevel
    enabled: bool = True
    parameters: Dict[str, Any] = field(default_factory=dict)
    check_function: Optional[str] = None  # 检查函数名


@dataclass
class ComplianceResult:
    """合规检查结果"""
    rule_id: str
    rule_name: str
    category: ComplianceCategory
    level: ComplianceLevel
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ComplianceReport:
    """合规报告"""
    strategy_id: str
    check_time: datetime
    overall_level: ComplianceLevel
    passed_rules: int
    total_rules: int
    results: List[ComplianceResult]
    recommendations: List[str]
    required_actions: List[str]


class ComplianceChecker(ABC):
    """合规检查器基类"""
    
    @abstractmethod
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """执行合规检查"""
        pass
    
    @abstractmethod
    def get_rule(self) -> ComplianceRule:
        """获取规则定义"""
        pass


class TPlusOneChecker(ComplianceChecker):
    """T+1交易规则检查器"""
    
    def __init__(self):
        self.rule = ComplianceRule(
            rule_id="TPLUS001",
            name="A股T+1交易规则",
            category=ComplianceCategory.TRADING_RULES,
            description="禁止当日回转交易，买入后T+1日才能卖出",
            level=ComplianceLevel.CRITICAL
        )
    
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """检查T+1规则"""
        trades = data.get('trades', [])
        violations = []
        
        # 按股票代码分组
        stock_trades = {}
        for trade in trades:
            symbol = trade.get('symbol', '')
            if symbol not in stock_trades:
                stock_trades[symbol] = []
            stock_trades[symbol].append(trade)
        
        # 检查每只股票的交易
        for symbol, symbol_trades in stock_trades.items():
            # 按时间排序
            symbol_trades.sort(key=lambda x: x.get('timestamp', ''))
            
            holdings = 0  # 持仓数量
            for trade in symbol_trades:
                trade_type = trade.get('type', '')  # buy/sell
                quantity = trade.get('quantity', 0)
                
                if trade_type == 'buy':
                    holdings += quantity
                elif trade_type == 'sell':
                    if holdings < quantity:
                        violations.append(f"{symbol} 卖出数量超过持仓")
                    holdings -= quantity
        
        passed = len(violations) == 0
        level = ComplianceLevel.PASS if passed else ComplianceLevel.CRITICAL
        message = "T+1规则检查通过" if passed else f"T+1规则违规: {', '.join(violations)}"
        
        return ComplianceResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            category=self.rule.category,
            level=level,
            passed=passed,
            message=message,
            details={"violations": violations}
        )
    
    def get_rule(self) -> ComplianceRule:
        return self.rule


class PriceLimitChecker(ComplianceChecker):
    """涨跌停价格限制检查器"""
    
    def __init__(self):
        self.rule = ComplianceRule(
            rule_id="PRICE001",
            name="涨跌停价格限制",
            category=ComplianceCategory.TRADING_RULES,
            description="禁止在涨停板买入，跌停板卖出",
            level=ComplianceLevel.CRITICAL,
            parameters={
                "limit_up_threshold": 0.098,  # 涨停阈值（考虑ST股票）
                "limit_down_threshold": -0.098  # 跌停阈值
            }
        )
    
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """检查涨跌停价格限制"""
        trades = data.get('trades', [])
        market_data = data.get('market_data', {})
        violations = []
        
        for trade in trades:
            symbol = trade.get('symbol', '')
            price = trade.get('price', 0)
            trade_type = trade.get('type', '')
            
            # 获取市场数据
            symbol_market = market_data.get(symbol, {})
            prev_close = symbol_market.get('prev_close', 0)
            
            if prev_close > 0:
                price_change_pct = (price - prev_close) / prev_close
                
                # 检查涨停买入
                if (trade_type == 'buy' and 
                    price_change_pct >= self.rule.parameters["limit_up_threshold"]):
                    violations.append(f"{symbol} 在涨停板附近买入")
                
                # 检查跌停卖出
                elif (trade_type == 'sell' and 
                      price_change_pct <= self.rule.parameters["limit_down_threshold"]):
                    violations.append(f"{symbol} 在跌停板附近卖出")
        
        passed = len(violations) == 0
        level = ComplianceLevel.PASS if passed else ComplianceLevel.CRITICAL
        message = "涨跌停限制检查通过" if passed else f"涨跌停限制违规: {', '.join(violations)}"
        
        return ComplianceResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            category=self.rule.category,
            level=level,
            passed=passed,
            message=message,
            details={"violations": violations}
        )
    
    def get_rule(self) -> ComplianceRule:
        return self.rule


class PositionLimitChecker(ComplianceChecker):
    """持仓限制检查器"""
    
    def __init__(self):
        self.rule = ComplianceRule(
            rule_id="POS001",
            name="单只股票持仓限制",
            category=ComplianceCategory.POSITION_LIMITS,
            description="单只股票持仓不得超过总资产的特定比例",
            level=ComplianceLevel.VIOLATION,
            parameters={
                "max_single_position": 0.10,  # 单只股票最大持仓比例10%
                "max_sector_position": 0.30    # 单个行业最大持仓比例30%
            }
        )
    
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """检查持仓限制"""
        positions = data.get('positions', [])
        total_value = data.get('total_value', 0)
        sector_mapping = data.get('sector_mapping', {})
        
        violations = []
        sector_positions = {}
        
        # 检查单只股票持仓
        for position in positions:
            symbol = position.get('symbol', '')
            value = position.get('value', 0)
            
            if total_value > 0:
                position_ratio = value / total_value
                
                if position_ratio > self.rule.parameters["max_single_position"]:
                    violations.append(f"{symbol} 持仓比例 {position_ratio:.2%} 超过限制")
            
            # 统计行业持仓
            sector = sector_mapping.get(symbol, '其他')
            sector_positions[sector] = sector_positions.get(sector, 0) + value
        
        # 检查行业持仓
        for sector, sector_value in sector_positions.items():
            if total_value > 0:
                sector_ratio = sector_value / total_value
                
                if sector_ratio > self.rule.parameters["max_sector_position"]:
                    violations.append(f"{sector} 行业持仓比例 {sector_ratio:.2%} 超过限制")
        
        passed = len(violations) == 0
        level = ComplianceLevel.PASS if passed else ComplianceLevel.VIOLATION
        message = "持仓限制检查通过" if passed else f"持仓限制违规: {', '.join(violations)}"
        
        return ComplianceResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            category=self.rule.category,
            level=level,
            passed=passed,
            message=message,
            details={"violations": violations}
        )
    
    def get_rule(self) -> ComplianceRule:
        return self.rule


class LeverageLimitChecker(ComplianceChecker):
    """杠杆限制检查器"""
    
    def __init__(self):
        self.rule = ComplianceRule(
            rule_id="LEV001",
            name="杠杆率限制",
            category=ComplianceCategory.RISK_LIMITS,
            description="总体杠杆率不得超过规定限制",
            level=ComplianceLevel.VIOLATION,
            parameters={
                "max_leverage": 2.0  # 最大杠杆率2倍
            }
        )
    
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """检查杠杆限制"""
        total_value = data.get('total_value', 0)
        net_asset = data.get('net_asset', 0)
        
        violations = []
        
        if net_asset > 0:
            leverage = total_value / net_asset
            
            if leverage > self.rule.parameters["max_leverage"]:
                violations.append(f"杠杆率 {leverage:.2f} 超过限制 {self.rule.parameters['max_leverage']}")
        
        passed = len(violations) == 0
        level = ComplianceLevel.PASS if passed else ComplianceLevel.VIOLATION
        message = "杠杆限制检查通过" if passed else f"杠杆限制违规: {', '.join(violations)}"
        
        return ComplianceResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            category=self.rule.category,
            level=level,
            passed=passed,
            message=message,
            details={"violations": violations, "leverage": total_value / net_asset if net_asset > 0 else 0}
        )
    
    def get_rule(self) -> ComplianceRule:
        return self.rule


class FutureFunctionChecker(ComplianceChecker):
    """未来函数检查器"""
    
    def __init__(self):
        self.rule = ComplianceRule(
            rule_id="FUT001",
            name="未来函数检查",
            category=ComplianceCategory.TRADING_RULES,
            description="禁止在策略中使用未来函数（回看偏差）",
            level=ComplianceLevel.CRITICAL,
            parameters={
                "forbidden_functions": [
                    "future", "shift(-1)", "shift(-2)", "shift(-3)",
                    "lead", "lookahead", "peek"
                ]
            }
        )
    
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """检查未来函数"""
        strategy_code = data.get('strategy_code', '')
        violations = []
        
        # 检查禁用的函数
        for func in self.rule.parameters["forbidden_functions"]:
            if func in strategy_code:
                violations.append(f"使用未来函数: {func}")
        
        # 检查shift负数
        shift_pattern = r'\.shift\(-\d+\)'
        if re.search(shift_pattern, strategy_code):
            violations.append("使用负数shift（未来数据）")
        
        passed = len(violations) == 0
        level = ComplianceLevel.PASS if passed else ComplianceLevel.CRITICAL
        message = "未来函数检查通过" if passed else f"未来函数违规: {', '.join(violations)}"
        
        return ComplianceResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            category=self.rule.category,
            level=level,
            passed=passed,
            message=message,
            details={"violations": violations}
        )
    
    def get_rule(self) -> ComplianceRule:
        return self.rule


class TradingTimeChecker(ComplianceChecker):
    """交易时间检查器"""
    
    def __init__(self):
        self.rule = ComplianceRule(
            rule_id="TIME001",
            name="交易时间限制",
            category=ComplianceCategory.TRADING_RULES,
            description="禁止在非交易时间下单",
            level=ComplianceLevel.WARNING,
            parameters={
                "morning_start": "09:30",
                "morning_end": "11:30",
                "afternoon_start": "13:00",
                "afternoon_end": "15:00"
            }
        )
    
    def check(self, data: Dict[str, Any]) -> ComplianceResult:
        """检查交易时间"""
        trades = data.get('trades', [])
        violations = []
        
        # 解析交易时间
        morning_start = time.fromisoformat(self.rule.parameters["morning_start"])
        morning_end = time.fromisoformat(self.rule.parameters["morning_end"])
        afternoon_start = time.fromisoformat(self.rule.parameters["afternoon_start"])
        afternoon_end = time.fromisoformat(self.rule.parameters["afternoon_end"])
        
        for trade in trades:
            trade_time_str = trade.get('timestamp', '')
            if trade_time_str:
                try:
                    # 解析时间部分
                    trade_time = datetime.fromisoformat(trade_time_str).time()
                    
                    # 检查是否在交易时间内
                    is_trading_time = (
                        (morning_start <= trade_time <= morning_end) or
                        (afternoon_start <= trade_time <= afternoon_end)
                    )
                    
                    if not is_trading_time:
                        violations.append(f"{trade_time_str} 非交易时间交易")
                        
                except ValueError:
                    violations.append(f"{trade_time_str} 时间格式错误")
        
        passed = len(violations) == 0
        level = ComplianceLevel.PASS if passed else ComplianceLevel.WARNING
        message = "交易时间检查通过" if passed else f"交易时间违规: {', '.join(violations)}"
        
        return ComplianceResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            category=self.rule.category,
            level=level,
            passed=passed,
            message=message,
            details={"violations": violations}
        )
    
    def get_rule(self) -> ComplianceRule:
        return self.rule


class ComplianceEngine:
    """合规性规则引擎"""
    
    def __init__(self):
        self.checkers: List[ComplianceChecker] = [
            TPlusOneChecker(),
            PriceLimitChecker(),
            PositionLimitChecker(),
            LeverageLimitChecker(),
            FutureFunctionChecker(),
            TradingTimeChecker()
        ]
        
        # 合规等级权重
        self.level_weights = {
            ComplianceLevel.PASS: 0,
            ComplianceLevel.WARNING: 1,
            ComplianceLevel.VIOLATION: 3,
            ComplianceLevel.CRITICAL: 5
        }
    
    def add_checker(self, checker: ComplianceChecker) -> None:
        """添加合规检查器"""
        self.checkers.append(checker)
    
    def remove_checker(self, rule_id: str) -> bool:
        """移除合规检查器"""
        for i, checker in enumerate(self.checkers):
            if checker.get_rule().rule_id == rule_id:
                del self.checkers[i]
                return True
        return False
    
    def check_strategy(self, strategy_id: str, data: Dict[str, Any]) -> ComplianceReport:
        """检查策略合规性"""
        results = []
        
        for checker in self.checkers:
            try:
                result = checker.check(data)
                results.append(result)
            except Exception as e:
                logging.error(f"合规检查失败 {checker.get_rule().rule_id}: {e}")
                # 创建失败结果
                rule = checker.get_rule()
                results.append(ComplianceResult(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    category=rule.category,
                    level=ComplianceLevel.CRITICAL,
                    passed=False,
                    message=f"检查执行失败: {str(e)}"
                ))
        
        # 计算总体合规等级
        overall_level = self._calculate_overall_level(results)
        
        # 统计通过率
        passed_rules = sum(1 for r in results if r.passed)
        total_rules = len(results)
        
        # 生成建议和必要行动
        recommendations = self._generate_recommendations(results)
        required_actions = self._generate_required_actions(results)
        
        return ComplianceReport(
            strategy_id=strategy_id,
            check_time=datetime.now(),
            overall_level=overall_level,
            passed_rules=passed_rules,
            total_rules=total_rules,
            results=results,
            recommendations=recommendations,
            required_actions=required_actions
        )
    
    def _calculate_overall_level(self, results: List[ComplianceResult]) -> ComplianceLevel:
        """计算总体合规等级"""
        if not results:
            return ComplianceLevel.PASS
        
        # 如果有任何严重违规，总体为严重违规
        if any(r.level == ComplianceLevel.CRITICAL for r in results):
            return ComplianceLevel.CRITICAL
        
        # 如果有任何违规，总体为违规
        if any(r.level == ComplianceLevel.VIOLATION for r in results):
            return ComplianceLevel.VIOLATION
        
        # 如果有任何警告，总体为警告
        if any(r.level == ComplianceLevel.WARNING for r in results):
            return ComplianceLevel.WARNING
        
        # 否则为通过
        return ComplianceLevel.PASS
    
    def _generate_recommendations(self, results: List[ComplianceResult]) -> List[str]:
        """生成合规建议"""
        recommendations = []
        
        for result in results:
            if not result.passed:
                if result.category == ComplianceCategory.TRADING_RULES:
                    if "T+1" in result.message:
                        recommendations.append("建议引入持仓管理模块，确保T+1规则")
                    elif "涨跌停" in result.message:
                        recommendations.append("建议增加价格限制检查，避免在极端价格交易")
                    elif "未来函数" in result.message:
                        recommendations.append("建议重构策略代码，消除未来函数")
                
                elif result.category == ComplianceCategory.POSITION_LIMITS:
                    recommendations.append("建议增加分散化投资逻辑，控制单只股票持仓")
                
                elif result.category == ComplianceCategory.RISK_LIMITS:
                    recommendations.append("建议增加风险控制模块，限制杠杆率")
        
        return list(set(recommendations))  # 去重
    
    def _generate_required_actions(self, results: List[ComplianceResult]) -> List[str]:
        """生成必要行动"""
        required_actions = []
        
        for result in results:
            if result.level in [ComplianceLevel.VIOLATION, ComplianceLevel.CRITICAL]:
                if result.category == ComplianceCategory.TRADING_RULES:
                    required_actions.append(f"必须修复{result.rule_name}问题才能实盘")
                elif result.category == ComplianceCategory.RISK_LIMITS:
                    required_actions.append(f"必须调整{result.rule_name}参数")
        
        return list(set(required_actions))  # 去重
    
    def get_compliance_summary(self, report: ComplianceReport) -> Dict[str, Any]:
        """获取合规摘要"""
        level_counts = {}
        for result in report.results:
            level = result.level.value
            level_counts[level] = level_counts.get(level, 0) + 1
        
        return {
            "strategy_id": report.strategy_id,
            "check_time": report.check_time.isoformat(),
            "overall_level": report.overall_level.value,
            "pass_rate": report.passed_rules / report.total_rules if report.total_rules > 0 else 0,
            "level_distribution": level_counts,
            "critical_issues": [
                r.rule_name for r in report.results 
                if r.level == ComplianceLevel.CRITICAL
            ],
            "violations": [
                r.rule_name for r in report.results 
                if r.level == ComplianceLevel.VIOLATION
            ],
            "warnings": [
                r.rule_name for r in report.results 
                if r.level == ComplianceLevel.WARNING
            ],
            "recommendations": report.recommendations,
            "required_actions": report.required_actions
        }
    
    def export_report(self, report: ComplianceReport, format: str = "json") -> str:
        """导出合规报告"""
        if format == "json":
            return json.dumps(self.get_compliance_summary(report), 
                            indent=2, ensure_ascii=False)
        else:
            raise ValueError(f"不支持的导出格式: {format}")


# 全局合规引擎实例
compliance_engine = ComplianceEngine()