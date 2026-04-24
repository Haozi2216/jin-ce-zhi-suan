"""
策略风险评级体系
为进化策略提供全面的风险评估和等级划分
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
import json
import math
from abc import ABC, abstractmethod


class RiskLevel(Enum):
    """风险等级"""
    LOW = "low"           # 低风险
    MEDIUM = "medium"     # 中等风险
    HIGH = "high"         # 高风险
    EXTREME = "extreme"   # 极高风险
    UNKNOWN = "unknown"   # 未知风险


class RiskCategory(Enum):
    """风险类别"""
    MARKET_RISK = "market_risk"           # 市场风险
    CREDIT_RISK = "credit_risk"           # 信用风险
    LIQUIDITY_RISK = "liquidity_risk"     # 流动性风险
    OPERATIONAL_RISK = "operational_risk" # 操作风险
    MODEL_RISK = "model_risk"             # 模型风险
    CONCENTRATION_RISK = "concentration_risk" # 集中度风险
    VOLATILITY_RISK = "volatility_risk"   # 波动率风险
    DRAWDOWN_RISK = "drawdown_risk"       # 回撤风险


@dataclass
class RiskMetrics:
    """风险指标"""
    # 收益风险指标
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    information_ratio: float = 0.0
    
    # 波动率指标
    volatility: float = 0.0
    downside_volatility: float = 0.0
    upside_volatility: float = 0.0
    
    # 回撤指标
    max_drawdown: float = 0.0
    avg_drawdown: float = 0.0
    drawdown_duration: int = 0
    recovery_time: int = 0
    
    # 尾部风险指标
    var_95: float = 0.0      # 95% VaR
    var_99: float = 0.0      # 99% VaR
    cvar_95: float = 0.0     # 95% CVaR
    cvar_99: float = 0.0     # 99% CVaR
    
    # 其他风险指标
    beta: float = 0.0
    correlation: float = 0.0
    tracking_error: float = 0.0
    turnover_rate: float = 0.0
    
    # 交易相关指标
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_win_loss_ratio: float = 0.0
    max_consecutive_losses: int = 0
    
    # 集中度指标
    position_concentration: float = 0.0
    sector_concentration: float = 0.0
    time_concentration: float = 0.0


@dataclass
class RiskScore:
    """风险评分"""
    category: RiskCategory
    score: float  # 0-100分，分数越高风险越大
    level: RiskLevel
    weight: float = 1.0
    description: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskAssessment:
    """风险评估结果"""
    strategy_id: str
    assessment_time: datetime
    overall_score: float
    overall_level: RiskLevel
    category_scores: List[RiskScore]
    risk_metrics: RiskMetrics
    recommendations: List[str]
    risk_factors: List[str]
    mitigation_strategies: List[str]


class RiskCalculator(ABC):
    """风险计算器基类"""
    
    @abstractmethod
    def calculate(self, data: Dict[str, Any]) -> float:
        """计算风险分数"""
        pass
    
    @abstractmethod
    def get_category(self) -> RiskCategory:
        """获取风险类别"""
        pass


class MarketRiskCalculator(RiskCalculator):
    """市场风险计算器"""
    
    def __init__(self):
        self.weights = {
            'volatility': 0.3,
            'beta': 0.2,
            'correlation': 0.2,
            'var': 0.3
        }
    
    def calculate(self, data: Dict[str, Any]) -> float:
        """计算市场风险分数"""
        metrics = data.get('risk_metrics', RiskMetrics())
        
        # 波动率评分 (0-100)
        vol_score = min(100, metrics.volatility * 100)
        
        # Beta评分 (0-100)
        beta_score = min(100, abs(metrics.beta - 1) * 50)
        
        # 相关性评分 (0-100)
        corr_score = min(100, abs(metrics.correlation) * 100)
        
        # VaR评分 (0-100)
        var_score = min(100, abs(metrics.var_95) * 100)
        
        # 加权平均
        total_score = (
            vol_score * self.weights['volatility'] +
            beta_score * self.weights['beta'] +
            corr_score * self.weights['correlation'] +
            var_score * self.weights['var']
        )
        
        return total_score
    
    def get_category(self) -> RiskCategory:
        return RiskCategory.MARKET_RISK


class DrawdownRiskCalculator(RiskCalculator):
    """回撤风险计算器"""
    
    def calculate(self, data: Dict[str, Any]) -> float:
        """计算回撤风险分数"""
        metrics = data.get('risk_metrics', RiskMetrics())
        
        # 最大回撤评分 (0-100)
        max_dd_score = min(100, metrics.max_drawdown * 100)
        
        # 平均回撤评分 (0-100)
        avg_dd_score = min(100, metrics.avg_drawdown * 100)
        
        # 回撤持续时间评分 (0-100)
        duration_score = min(100, metrics.drawdown_duration / 252 * 100)  # 按年化计算
        
        # 恢复时间评分 (0-100)
        recovery_score = min(100, metrics.recovery_time / 30 * 100)  # 按月计算
        
        # 加权平均
        total_score = (
            max_dd_score * 0.4 +
            avg_dd_score * 0.3 +
            duration_score * 0.2 +
            recovery_score * 0.1
        )
        
        return total_score
    
    def get_category(self) -> RiskCategory:
        return RiskCategory.DRAWDOWN_RISK


class VolatilityRiskCalculator(RiskCalculator):
    """波动率风险计算器"""
    
    def calculate(self, data: Dict[str, Any]) -> float:
        """计算波动率风险分数"""
        metrics = data.get('risk_metrics', RiskMetrics())
        
        # 总波动率评分
        vol_score = min(100, metrics.volatility * 100)
        
        # 下行波动率评分
        downside_score = min(100, metrics.downside_volatility * 100)
        
        # 波动率比率评分
        if metrics.upside_volatility > 0:
            vol_ratio = metrics.downside_volatility / metrics.upside_volatility
            ratio_score = min(100, vol_ratio * 50)
        else:
            ratio_score = 100
        
        # 加权平均
        total_score = (
            vol_score * 0.4 +
            downside_score * 0.4 +
            ratio_score * 0.2
        )
        
        return total_score
    
    def get_category(self) -> RiskCategory:
        return RiskCategory.VOLATILITY_RISK


class ConcentrationRiskCalculator(RiskCalculator):
    """集中度风险计算器"""
    
    def calculate(self, data: Dict[str, Any]) -> float:
        """计算集中度风险分数"""
        metrics = data.get('risk_metrics', RiskMetrics())
        
        # 持仓集中度评分
        position_score = min(100, metrics.position_concentration * 100)
        
        # 行业集中度评分
        sector_score = min(100, metrics.sector_concentration * 100)
        
        # 时间集中度评分
        time_score = min(100, metrics.time_concentration * 100)
        
        # 加权平均
        total_score = (
            position_score * 0.5 +
            sector_score * 0.3 +
            time_score * 0.2
        )
        
        return total_score
    
    def get_category(self) -> RiskCategory:
        return RiskCategory.CONCENTRATION_RISK


class ModelRiskCalculator(RiskCalculator):
    """模型风险计算器"""
    
    def calculate(self, data: Dict[str, Any]) -> float:
        """计算模型风险分数"""
        strategy_data = data.get('strategy_data', {})
        
        # 模型复杂度评分
        complexity_score = self._calculate_complexity_score(strategy_data)
        
        # 过拟合风险评分
        overfitting_score = self._calculate_overfitting_score(data)
        
        # 参数稳定性评分
        stability_score = self._calculate_stability_score(data)
        
        # 样本外表现评分
        oos_score = self._calculate_oos_score(data)
        
        # 加权平均
        total_score = (
            complexity_score * 0.2 +
            overfitting_score * 0.3 +
            stability_score * 0.3 +
            oos_score * 0.2
        )
        
        return total_score
    
    def _calculate_complexity_score(self, strategy_data: Dict[str, Any]) -> float:
        """计算模型复杂度评分"""
        # 基于策略代码行数、参数数量等计算复杂度
        code_lines = len(strategy_data.get('code', '').split('\n'))
        param_count = len(strategy_data.get('parameters', {}))
        
        complexity = (code_lines / 100 * 50) + (param_count / 10 * 50)
        return min(100, complexity)
    
    def _calculate_overfitting_score(self, data: Dict[str, Any]) -> float:
        """计算过拟合风险评分"""
        metrics = data.get('risk_metrics', RiskMetrics())
        
        # 基于夏普比率稳定性、样本内外表现差异等
        sharpe_stability = abs(metrics.sharpe_ratio)  # 简化计算
        
        # 如果夏普比率过高，可能存在过拟合
        if metrics.sharpe_ratio > 3:
            overfitting_risk = 80
        elif metrics.sharpe_ratio > 2:
            overfitting_risk = 50
        else:
            overfitting_risk = 20
        
        return overfitting_risk
    
    def _calculate_stability_score(self, data: Dict[str, Any]) -> float:
        """计算参数稳定性评分"""
        # 简化实现，实际应该基于参数敏感性分析
        return 30  # 默认中等风险
    
    def _calculate_oos_score(self, data: Dict[str, Any]) -> float:
        """计算样本外表现评分"""
        # 简化实现，实际应该基于样本外测试结果
        return 25  # 默认中低风险
    
    def get_category(self) -> RiskCategory:
        return RiskCategory.MODEL_RISK


class RiskRatingSystem:
    """风险评级系统"""
    
    def __init__(self):
        self.calculators: List[RiskCalculator] = [
            MarketRiskCalculator(),
            DrawdownRiskCalculator(),
            VolatilityRiskCalculator(),
            ConcentrationRiskCalculator(),
            ModelRiskCalculator()
        ]
        
        # 风险等级阈值
        self.level_thresholds = {
            RiskLevel.LOW: 20,
            RiskLevel.MEDIUM: 40,
            RiskLevel.HIGH: 70,
            RiskLevel.EXTREME: 85
        }
        
        # 类别权重
        self.category_weights = {
            RiskCategory.MARKET_RISK: 0.25,
            RiskCategory.DRAWDOWN_RISK: 0.25,
            RiskCategory.VOLATILITY_RISK: 0.20,
            RiskCategory.CONCENTRATION_RISK: 0.15,
            RiskCategory.MODEL_RISK: 0.15
        }
    
    def assess_strategy_risk(self, 
                           strategy_id: str,
                           backtest_results: Dict[str, Any],
                           strategy_data: Dict[str, Any]) -> RiskAssessment:
        """评估策略风险"""
        
        # 计算风险指标
        risk_metrics = self._calculate_risk_metrics(backtest_results)
        
        # 准备评估数据
        assessment_data = {
            'risk_metrics': risk_metrics,
            'strategy_data': strategy_data,
            'backtest_results': backtest_results
        }
        
        # 计算各类别风险分数
        category_scores = []
        for calculator in self.calculators:
            score = calculator.calculate(assessment_data)
            category = calculator.get_category()
            level = self._determine_risk_level(score)
            weight = self.category_weights.get(category, 1.0)
            
            risk_score = RiskScore(
                category=category,
                score=score,
                level=level,
                weight=weight,
                description=self._get_risk_description(category, level)
            )
            
            category_scores.append(risk_score)
        
        # 计算总体风险分数
        overall_score = self._calculate_overall_score(category_scores)
        overall_level = self._determine_risk_level(overall_score)
        
        # 生成建议和风险因子
        recommendations = self._generate_recommendations(category_scores)
        risk_factors = self._identify_risk_factors(category_scores)
        mitigation_strategies = self._generate_mitigation_strategies(category_scores)
        
        return RiskAssessment(
            strategy_id=strategy_id,
            assessment_time=datetime.now(),
            overall_score=overall_score,
            overall_level=overall_level,
            category_scores=category_scores,
            risk_metrics=risk_metrics,
            recommendations=recommendations,
            risk_factors=risk_factors,
            mitigation_strategies=mitigation_strategies
        )
    
    def _calculate_risk_metrics(self, backtest_results: Dict[str, Any]) -> RiskMetrics:
        """计算风险指标"""
        equity_curve = backtest_results.get('equity_curve', [])
        returns = backtest_results.get('returns', [])
        trades = backtest_results.get('trades', [])
        
        if not returns:
            return RiskMetrics()
        
        returns_df = pd.Series(returns)
        
        # 基础统计指标
        volatility = returns_df.std() * np.sqrt(252)  # 年化波动率
        
        # 夏普比率
        risk_free_rate = 0.03  # 假设无风险利率3%
        excess_returns = returns_df.mean() * 252 - risk_free_rate
        sharpe_ratio = excess_returns / volatility if volatility > 0 else 0
        
        # 下行波动率
        negative_returns = returns_df[returns_df < 0]
        downside_volatility = negative_returns.std() * np.sqrt(252) if len(negative_returns) > 0 else 0
        
        # 最大回撤
        equity_series = pd.Series(equity_curve)
        rolling_max = equity_series.expanding().max()
        drawdown = (equity_series - rolling_max) / rolling_max
        max_drawdown = abs(drawdown.min())
        
        # VaR和CVaR
        var_95 = np.percentile(returns_df, 5)
        var_99 = np.percentile(returns_df, 1)
        cvar_95 = returns_df[returns_df <= var_95].mean()
        cvar_99 = returns_df[returns_df <= var_99].mean()
        
        # 交易相关指标
        if trades:
            win_trades = [t for t in trades if t.get('pnl', 0) > 0]
            lose_trades = [t for t in trades if t.get('pnl', 0) < 0]
            
            win_rate = len(win_trades) / len(trades) if trades else 0
            
            total_win = sum(t.get('pnl', 0) for t in win_trades)
            total_lose = abs(sum(t.get('pnl', 0) for t in lose_trades))
            profit_factor = total_win / total_lose if total_lose > 0 else float('inf')
            
            avg_win = np.mean([t.get('pnl', 0) for t in win_trades]) if win_trades else 0
            avg_loss = np.mean([t.get('pnl', 0) for t in lose_trades]) if lose_trades else 0
            avg_win_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        else:
            win_rate = 0
            profit_factor = 0
            avg_win_loss_ratio = 0
        
        return RiskMetrics(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            downside_volatility=downside_volatility,
            max_drawdown=max_drawdown,
            var_95=var_95,
            var_99=var_99,
            cvar_95=cvar_95,
            cvar_99=cvar_99,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_win_loss_ratio=avg_win_loss_ratio
        )
    
    def _calculate_overall_score(self, category_scores: List[RiskScore]) -> float:
        """计算总体风险分数"""
        weighted_sum = sum(score.score * score.weight for score in category_scores)
        total_weight = sum(score.weight for score in category_scores)
        return weighted_sum / total_weight if total_weight > 0 else 0
    
    def _determine_risk_level(self, score: float) -> RiskLevel:
        """确定风险等级"""
        if score <= self.level_thresholds[RiskLevel.LOW]:
            return RiskLevel.LOW
        elif score <= self.level_thresholds[RiskLevel.MEDIUM]:
            return RiskLevel.MEDIUM
        elif score <= self.level_thresholds[RiskLevel.HIGH]:
            return RiskLevel.HIGH
        else:
            return RiskLevel.EXTREME
    
    def _get_risk_description(self, category: RiskCategory, level: RiskLevel) -> str:
        """获取风险描述"""
        descriptions = {
            RiskCategory.MARKET_RISK: {
                RiskLevel.LOW: "市场风险较低，对市场波动不敏感",
                RiskLevel.MEDIUM: "市场风险适中，需要关注市场变化",
                RiskLevel.HIGH: "市场风险较高，建议加强市场监控",
                RiskLevel.EXTREME: "市场风险极高，需要严格控制敞口"
            },
            RiskCategory.DRAWDOWN_RISK: {
                RiskLevel.LOW: "回撤风险较低，资金曲线稳定",
                RiskLevel.MEDIUM: "回撤风险适中，需要设置止损",
                RiskLevel.HIGH: "回撤风险较高，建议降低仓位",
                RiskLevel.EXTREME: "回撤风险极高，需要暂停交易"
            },
            RiskCategory.VOLATILITY_RISK: {
                RiskLevel.LOW: "波动率风险较低，收益稳定",
                RiskLevel.MEDIUM: "波动率风险适中，收益有一定波动",
                RiskLevel.HIGH: "波动率风险较高，收益波动大",
                RiskLevel.EXTREME: "波动率风险极高，收益极不稳定"
            },
            RiskCategory.CONCENTRATION_RISK: {
                RiskLevel.LOW: "集中度风险较低，分散化良好",
                RiskLevel.MEDIUM: "集中度风险适中，有一定集中",
                RiskLevel.HIGH: "集中度风险较高，分散化不足",
                RiskLevel.EXTREME: "集中度风险极高，过度集中"
            },
            RiskCategory.MODEL_RISK: {
                RiskLevel.LOW: "模型风险较低，模型稳健",
                RiskLevel.MEDIUM: "模型风险适中，需要验证",
                RiskLevel.HIGH: "模型风险较高，可能过拟合",
                RiskLevel.EXTREME: "模型风险极高，不建议使用"
            }
        }
        
        return descriptions.get(category, {}).get(level, "未知风险")
    
    def _generate_recommendations(self, category_scores: List[RiskScore]) -> List[str]:
        """生成风险建议"""
        recommendations = []
        
        for score in category_scores:
            if score.level in [RiskLevel.HIGH, RiskLevel.EXTREME]:
                if score.category == RiskCategory.MARKET_RISK:
                    recommendations.append("建议降低市场敞口，增加对冲工具")
                elif score.category == RiskCategory.DRAWDOWN_RISK:
                    recommendations.append("建议设置更严格的止损规则，降低仓位")
                elif score.category == RiskCategory.VOLATILITY_RISK:
                    recommendations.append("建议增加低波动率资产，平滑收益曲线")
                elif score.category == RiskCategory.CONCENTRATION_RISK:
                    recommendations.append("建议增加投资标的数量，分散风险")
                elif score.category == RiskCategory.MODEL_RISK:
                    recommendations.append("建议简化模型参数，进行更多样本外测试")
        
        return recommendations
    
    def _identify_risk_factors(self, category_scores: List[RiskScore]) -> List[str]:
        """识别风险因子"""
        risk_factors = []
        
        for score in category_scores:
            if score.score > 60:
                risk_factors.append(f"{score.category.value}: {score.description}")
        
        return risk_factors
    
    def _generate_mitigation_strategies(self, category_scores: List[RiskScore]) -> List[str]:
        """生成风险缓解策略"""
        strategies = []
        
        for score in category_scores:
            if score.level in [RiskLevel.HIGH, RiskLevel.EXTREME]:
                if score.category == RiskCategory.MARKET_RISK:
                    strategies.extend([
                        "使用股指期货对冲市场风险",
                        "增加债券等低相关性资产",
                        "动态调整仓位规模"
                    ])
                elif score.category == RiskCategory.DRAWDOWN_RISK:
                    strategies.extend([
                        "实施动态止损策略",
                        "分批建仓降低冲击成本",
                        "设置最大回撤预警线"
                    ])
                elif score.category == RiskCategory.VOLATILITY_RISK:
                    strategies.extend([
                        "使用波动率目标策略",
                        "增加期权等衍生品保护",
                        "实施风险平价配置"
                    ])
                elif score.category == RiskCategory.CONCENTRATION_RISK:
                    strategies.extend([
                        "增加行业和个股分散度",
                        "设置单个标的持仓上限",
                        "定期再平衡投资组合"
                    ])
                elif score.category == RiskCategory.MODEL_RISK:
                    strategies.extend([
                        "进行蒙特卡洛模拟验证",
                        "实施模型集成策略",
                        "建立模型监控预警机制"
                    ])
        
        # 去重
        return list(set(strategies))
    
    def get_risk_summary(self, assessment: RiskAssessment) -> Dict[str, Any]:
        """获取风险摘要"""
        return {
            "strategy_id": assessment.strategy_id,
            "overall_score": assessment.overall_score,
            "overall_level": assessment.overall_level.value,
            "assessment_time": assessment.assessment_time.isoformat(),
            "category_breakdown": {
                score.category.value: {
                    "score": score.score,
                    "level": score.level.value,
                    "weight": score.weight,
                    "description": score.description
                }
                for score in assessment.category_scores
            },
            "key_metrics": {
                "sharpe_ratio": assessment.risk_metrics.sharpe_ratio,
                "max_drawdown": assessment.risk_metrics.max_drawdown,
                "volatility": assessment.risk_metrics.volatility,
                "win_rate": assessment.risk_metrics.win_rate
            },
            "recommendations": assessment.recommendations,
            "risk_factors": assessment.risk_factors,
            "mitigation_strategies": assessment.mitigation_strategies
        }


# 全局风险评级系统实例
risk_rating_system = RiskRatingSystem()