"""
A股合规性约束验证模块
确保生成的策略符合A股交易规则和监管要求
"""
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import numpy as np
from datetime import datetime, timedelta


class ComplianceRule(Enum):
    """合规规则枚举"""
    T_PLUS_ONE = "t_plus_one"                    # T+1交易规则
    PRICE_LIMIT = "price_limit"                  # 涨跌停限制
    POSITION_LIMIT = "position_limit"            # 持仓限制
    TRADING_TIME = "trading_time"                # 交易时间限制
    SHORT_SELLING = "short_selling"              # 融资融券限制
    IPO_RESTRICTION = "ipo_restriction"          # 新股交易限制
    RISK_MANAGEMENT = "risk_management"          # 风险管理要求
    CAPITAL_ADEQUACY = "capital_adequacy"        # 资本充足率


@dataclass
class ComplianceViolation:
    """合规违规记录"""
    rule: ComplianceRule
    severity: str  # 'low', 'medium', 'high', 'critical'
    description: str
    suggestion: str
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule": self.rule.value,
            "severity": self.severity,
            "description": self.description,
            "suggestion": self.suggestion,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class ComplianceResult:
    """合规检查结果"""
    is_compliant: bool
    violations: List[ComplianceViolation]
    score: float  # 合规评分 0-100
    warnings: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_compliant": self.is_compliant,
            "violations": [v.to_dict() for v in self.violations],
            "score": self.score,
            "warnings": self.warnings
        }


class AShareComplianceValidator:
    """A股合规性验证器"""
    
    def __init__(self):
        self.price_limit_ratio = 0.10  # 10%涨跌停
        self.max_position_ratio = 0.10  # 单只股票最大持仓10%
        self.trading_hours = {
            "morning": (9.5, 11.5),    # 9:30-11:30
            "afternoon": (13.0, 15.0)  # 13:00-15:00
        }
        self.min_capital_ratio = 0.5   # 最小资本充足率50%
    
    def validate_strategy_gene(self, gene: Dict[str, Any]) -> ComplianceResult:
        """验证策略基因的合规性"""
        violations = []
        warnings = []
        
        # 1. T+1交易规则验证
        t1_violations = self._validate_t_plus_one(gene)
        violations.extend(t1_violations)
        
        # 2. 涨跌停限制验证
        price_violations = self._validate_price_limits(gene)
        violations.extend(price_violations)
        
        # 3. 持仓限制验证
        position_violations = self._validate_position_limits(gene)
        violations.extend(position_violations)
        
        # 4. 交易时间验证
        time_violations = self._validate_trading_time(gene)
        violations.extend(time_violations)
        
        # 5. 融资融券限制验证
        short_violations = self._validate_short_selling(gene)
        violations.extend(short_violations)
        
        # 6. 风险管理要求验证
        risk_violations = self._validate_risk_management(gene)
        violations.extend(risk_violations)
        
        # 7. 资本充足率验证
        capital_violations = self._validate_capital_adequacy(gene)
        violations.extend(capital_violations)
        
        # 生成警告
        warnings = self._generate_warnings(gene)
        
        # 计算合规评分
        score = self._calculate_compliance_score(violations, warnings)
        
        # 判断是否合规
        critical_violations = [v for v in violations if v.severity == 'critical']
        is_compliant = len(critical_violations) == 0
        
        return ComplianceResult(
            is_compliant=is_compliant,
            violations=violations,
            score=score,
            warnings=warnings
        )
    
    def _validate_t_plus_one(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证T+1交易规则"""
        violations = []
        
        # 检查策略是否包含当日回转交易逻辑
        gene_structure = gene.get("gene_structure", {})
        signal_type = gene_structure.get("signal_type", "")
        
        # 检查是否有当日买卖逻辑
        if "intraday" in signal_type.lower() or "day_trading" in signal_type.lower():
            violations.append(ComplianceViolation(
                rule=ComplianceRule.T_PLUS_ONE,
                severity="critical",
                description="策略包含当日回转交易逻辑，违反A股T+1规则",
                suggestion="移除当日买卖逻辑，改为隔日交易",
                timestamp=datetime.now()
            ))
        
        # 检查持仓时间参数
        parameters = gene.get("parameters", {})
        holding_period = parameters.get("holding_period", 0)
        
        if holding_period and holding_period < 1:
            violations.append(ComplianceViolation(
                rule=ComplianceRule.T_PLUS_ONE,
                severity="high",
                description=f"持仓时间{holding_period}天小于T+1要求",
                suggestion="设置最小持仓时间为1天",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _validate_price_limits(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证涨跌停限制"""
        violations = []
        
        parameters = gene.get("parameters", {})
        
        # 检查是否有突破涨跌停的逻辑
        gene_structure = gene.get("gene_structure", {})
        conditions = gene_structure.get("conditions", [])
        
        if any("limit_up" in str(cond).lower() or "limit_down" in str(cond).lower() for cond in conditions):
            violations.append(ComplianceViolation(
                rule=ComplianceRule.PRICE_LIMIT,
                severity="high",
                description="策略包含涨跌停突破逻辑",
                suggestion="移除涨跌停突破逻辑，或添加涨跌停检查",
                timestamp=datetime.now()
            ))
        
        # 检查止损设置是否考虑涨跌停
        stop_loss = parameters.get("stop_loss", 0)
        if stop_loss and abs(stop_loss) > self.price_limit_ratio:
            violations.append(ComplianceViolation(
                rule=ComplianceRule.PRICE_LIMIT,
                severity="medium",
                description=f"止损比例{stop_loss:.2%}超过涨跌停限制",
                suggestion=f"调整止损比例在{self.price_limit_ratio:.1%}以内",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _validate_position_limits(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证持仓限制"""
        violations = []
        
        parameters = gene.get("parameters", {})
        position_size = parameters.get("position_size", 0)
        
        if position_size > self.max_position_ratio:
            violations.append(ComplianceViolation(
                rule=ComplianceRule.POSITION_LIMIT,
                severity="medium",
                description=f"仓位大小{position_size:.1%}超过单只股票最大限制{self.max_position_ratio:.1%}",
                suggestion=f"调整仓位大小至{self.max_position_ratio:.1%}以内",
                timestamp=datetime.now()
            ))
        
        # 检查是否有集中持仓风险
        gene_structure = gene.get("gene_structure", {})
        if "concentrated_position" in str(gene_structure).lower():
            violations.append(ComplianceViolation(
                rule=ComplianceRule.POSITION_LIMIT,
                severity="low",
                description="策略可能存在集中持仓风险",
                suggestion="考虑分散化投资，降低单只股票集中度",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _validate_trading_time(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证交易时间"""
        violations = []
        
        # 检查是否有非交易时间交易逻辑
        gene_structure = gene.get("gene_structure", {})
        signal_type = gene_structure.get("signal_type", "")
        
        if "overnight" in signal_type.lower() or "after_hours" in signal_type.lower():
            violations.append(ComplianceViolation(
                rule=ComplianceRule.TRADING_TIME,
                severity="medium",
                description="策略包含非交易时间交易逻辑",
                suggestion="调整为交易时间内的交易逻辑",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _validate_short_selling(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证融资融券限制"""
        violations = []
        
        gene_structure = gene.get("gene_structure", {})
        conditions = gene_structure.get("conditions", [])
        
        # 检查是否有做空逻辑
        if any("short" in str(cond).lower() or "sell_short" in str(cond).lower() for cond in conditions):
            violations.append(ComplianceViolation(
                rule=ComplianceRule.SHORT_SELLING,
                severity="high",
                description="策略包含做空逻辑，需要融资融券资格",
                suggestion="移除做空逻辑或确保有融资融券资格",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _validate_risk_management(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证风险管理要求"""
        violations = []
        
        parameters = gene.get("parameters", {})
        gene_structure = gene.get("gene_structure", {})
        
        # 检查是否有止损机制
        risk_management = gene_structure.get("risk_management", [])
        if "stop_loss" not in risk_management and "trailing_stop" not in risk_management:
            violations.append(ComplianceViolation(
                rule=ComplianceRule.RISK_MANAGEMENT,
                severity="medium",
                description="策略缺乏止损机制",
                suggestion="添加止损或跟踪止损机制",
                timestamp=datetime.now()
            ))
        
        # 检查止损比例是否合理
        stop_loss = parameters.get("stop_loss", 0)
        if stop_loss == 0:
            violations.append(ComplianceViolation(
                rule=ComplianceRule.RISK_MANAGEMENT,
                severity="low",
                description="策略未设置止损比例",
                suggestion="设置合理的止损比例，建议5%-10%",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _validate_capital_adequacy(self, gene: Dict[str, Any]) -> List[ComplianceViolation]:
        """验证资本充足率"""
        violations = []
        
        parameters = gene.get("parameters", {})
        position_size = parameters.get("position_size", 0)
        
        # 简单的资本充足率检查
        if position_size > 0.8:  # 单策略占用80%以上资金
            violations.append(ComplianceViolation(
                rule=ComplianceRule.CAPITAL_ADEQUACY,
                severity="medium",
                description=f"策略仓位{position_size:.1%}过高，可能影响资本充足率",
                suggestion="降低仓位大小，保留充足的风险准备金",
                timestamp=datetime.now()
            ))
        
        return violations
    
    def _generate_warnings(self, gene: Dict[str, Any]) -> List[str]:
        """生成警告信息"""
        warnings = []
        
        parameters = gene.get("parameters", {})
        gene_structure = gene.get("gene_structure", {})
        
        # 复杂度警告
        complexity_score = gene.get("complexity_score", 0)
        if complexity_score > 0.8:
            warnings.append("策略复杂度较高，可能增加实施难度和风险")
        
        # 频率警告
        signal_type = gene_structure.get("signal_type", "")
        if "high_frequency" in signal_type.lower():
            warnings.append("高频交易策略需要更强的技术支持和风控措施")
        
        # 依赖性警告
        indicators = gene_structure.get("indicators", [])
        if len(indicators) > 5:
            warnings.append("策略依赖指标较多，需要确保数据质量和稳定性")
        
        return warnings
    
    def _calculate_compliance_score(self, violations: List[ComplianceViolation], warnings: List[str]) -> float:
        """计算合规评分"""
        base_score = 100.0
        
        # 根据违规严重程度扣分
        for violation in violations:
            if violation.severity == "critical":
                base_score -= 50
            elif violation.severity == "high":
                base_score -= 20
            elif violation.severity == "medium":
                base_score -= 10
            elif violation.severity == "low":
                base_score -= 5
        
        # 根据警告数量扣分
        base_score -= len(warnings) * 2
        
        return max(0.0, base_score)
    
    def validate_backtest_compliance(self, backtest_results: Dict[str, Any]) -> ComplianceResult:
        """验证回测结果的合规性"""
        violations = []
        warnings = []
        
        # 检查回测中的异常交易
        trades = backtest_results.get("trades", [])
        
        for trade in trades:
            # 检查是否有当日买卖
            if trade.get("buy_date") == trade.get("sell_date"):
                violations.append(ComplianceViolation(
                    rule=ComplianceRule.T_PLUS_ONE,
                    severity="critical",
                    description=f"回测中发现当日买卖交易：{trade.get('symbol')}在{trade.get('buy_date')}",
                    suggestion="检查回测逻辑，确保符合T+1规则",
                    timestamp=datetime.now()
                ))
        
        # 检查收益率异常
        total_return = backtest_results.get("total_return", 0)
        if total_return > 5.0:  # 年化收益率超过500%
            warnings.append("回测收益率异常高，请检查策略逻辑和回测设置")
        
        # 检查交易频率
        trade_count = len(trades)
        backtest_days = backtest_results.get("backtest_days", 252)
        avg_trades_per_day = trade_count / backtest_days
        
        if avg_trades_per_day > 10:
            warnings.append(f"交易频率过高（日均{avg_trades_per_day:.1f}笔），可能产生过高交易成本")
        
        # 计算合规评分
        score = self._calculate_compliance_score(violations, warnings)
        
        # 判断是否合规
        critical_violations = [v for v in violations if v.severity == 'critical']
        is_compliant = len(critical_violations) == 0
        
        return ComplianceResult(
            is_compliant=is_compliant,
            violations=violations,
            score=score,
            warnings=warnings
        )


# 全局合规验证器实例
a_share_compliance_validator = AShareComplianceValidator()