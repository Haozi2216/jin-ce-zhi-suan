"""
智能错误恢复和重试策略
为策略进化系统提供自动错误恢复、重试机制和降级策略
"""
import time
import threading
import logging
import traceback
import random
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, Future
import json
import hashlib
from functools import wraps


class ErrorType(Enum):
    """错误类型"""
    NETWORK_ERROR = "network_error"
    TIMEOUT_ERROR = "timeout_error"
    DATA_ERROR = "data_error"
    AUTH_ERROR = "auth_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    SYSTEM_ERROR = "system_error"
    UNKNOWN_ERROR = "unknown_error"


class RecoveryAction(Enum):
    """恢复动作"""
    RETRY = "retry"
    FALLBACK = "fallback"
    CIRCUIT_BREAK = "circuit_break"
    DEGRADE = "degrade"
    IGNORE = "ignore"
    ESCALATE = "escalate"


@dataclass
class ErrorInfo:
    """错误信息"""
    error_type: ErrorType
    error_message: str
    exception: Optional[Exception] = None
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    source: str = ""
    operation: str = ""


@dataclass
class RetryConfig:
    """重试配置"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    
    def get_delay(self, attempt: int) -> float:
        """计算重试延迟"""
        delay = min(self.base_delay * (self.exponential_base ** (attempt - 1)), self.max_delay)
        
        if self.jitter:
            jitter_amount = delay * self.jitter_factor
            delay += random.uniform(-jitter_amount, jitter_amount)
        
        return max(0, delay)


@dataclass
class RecoveryPolicy:
    """恢复策略"""
    error_type: ErrorType
    actions: List[RecoveryAction]
    retry_config: Optional[RetryConfig] = None
    fallback_sources: List[str] = field(default_factory=list)
    max_recovery_time: float = 300.0  # 最大恢复时间（秒）
    escalation_threshold: int = 5  # 升级阈值


class ErrorClassifier:
    """错误分类器"""
    
    @staticmethod
    def classify_error(exception: Exception, context: Dict[str, Any] = None) -> ErrorType:
        """分类错误类型"""
        if context is None:
            context = {}
        
        error_message = str(exception).lower()
        exception_type = type(exception).__name__.lower()
        
        # 网络错误
        if any(keyword in error_message for keyword in [
            "connection", "network", "dns", "socket", "host"
        ]) or any(keyword in exception_type for keyword in [
            "connection", "network", "socket"
        ]):
            return ErrorType.NETWORK_ERROR
        
        # 超时错误
        if "timeout" in error_message or "timeout" in exception_type:
            return ErrorType.TIMEOUT_ERROR
        
        # 认证错误
        if any(keyword in error_message for keyword in [
            "auth", "unauthorized", "forbidden", "token", "credential"
        ]):
            return ErrorType.AUTH_ERROR
        
        # 限流错误
        if any(keyword in error_message for keyword in [
            "rate limit", "too many requests", "quota", "throttle"
        ]):
            return ErrorType.RATE_LIMIT_ERROR
        
        # 数据错误
        if any(keyword in error_message for keyword in [
            "data", "format", "parse", "invalid", "corrupt"
        ]):
            return ErrorType.DATA_ERROR
        
        # 系统错误
        if any(keyword in error_message for keyword in [
            "system", "memory", "disk", "resource"
        ]):
            return ErrorType.SYSTEM_ERROR
        
        return ErrorType.UNKNOWN_ERROR


class CircuitBreaker:
    """熔断器"""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def __call__(self, func: Callable) -> Callable:
        """装饰器用法"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """通过熔断器调用函数"""
        with self._lock:
            if self.state == "OPEN":
                if self._should_attempt_reset():
                    self.state = "HALF_OPEN"
                else:
                    raise Exception("熔断器已打开")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise e
    
    def _should_attempt_reset(self) -> bool:
        """是否应该尝试重置熔断器"""
        return (self.last_failure_time and 
                (datetime.now() - self.last_failure_time).seconds >= self.recovery_timeout)
    
    def _on_success(self) -> None:
        """成功时的处理"""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def _on_failure(self) -> None:
        """失败时的处理"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
    
    def reset(self) -> None:
        """重置熔断器"""
        with self._lock:
            self.failure_count = 0
            self.state = "CLOSED"
            self.last_failure_time = None


class ErrorRecoveryManager:
    """错误恢复管理器"""
    
    def __init__(self):
        self.recovery_policies: Dict[ErrorType, RecoveryPolicy] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.error_history: List[ErrorInfo] = []
        self.recovery_stats: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="recovery")
        
        self._initialize_default_policies()
    
    def _initialize_default_policies(self) -> None:
        """初始化默认恢复策略"""
        default_policies = [
            RecoveryPolicy(
                error_type=ErrorType.NETWORK_ERROR,
                actions=[RecoveryAction.RETRY, RecoveryAction.FALLBACK],
                retry_config=RetryConfig(max_attempts=3, base_delay=2.0),
                fallback_sources=["secondary_api", "local_cache"]
            ),
            RecoveryPolicy(
                error_type=ErrorType.TIMEOUT_ERROR,
                actions=[RecoveryAction.RETRY, RecoveryAction.DEGRADE],
                retry_config=RetryConfig(max_attempts=2, base_delay=1.0, max_delay=10.0)
            ),
            RecoveryPolicy(
                error_type=ErrorType.AUTH_ERROR,
                actions=[RecoveryAction.ESCALATE],
                max_recovery_time=60.0
            ),
            RecoveryPolicy(
                error_type=ErrorType.RATE_LIMIT_ERROR,
                actions=[RecoveryAction.RETRY, RecoveryAction.DEGRADE],
                retry_config=RetryConfig(max_attempts=2, base_delay=5.0, max_delay=30.0)
            ),
            RecoveryPolicy(
                error_type=ErrorType.DATA_ERROR,
                actions=[RecoveryAction.FALLBACK, RecoveryAction.IGNORE],
                fallback_sources=["backup_data", "cached_data"]
            ),
            RecoveryPolicy(
                error_type=ErrorType.SYSTEM_ERROR,
                actions=[RecoveryAction.CIRCUIT_BREAK, RecoveryAction.ESCALATE],
                escalation_threshold=3
            ),
            RecoveryPolicy(
                error_type=ErrorType.UNKNOWN_ERROR,
                actions=[RecoveryAction.RETRY],
                retry_config=RetryConfig(max_attempts=1, base_delay=1.0)
            )
        ]
        
        for policy in default_policies:
            self.recovery_policies[policy.error_type] = policy
    
    def add_recovery_policy(self, policy: RecoveryPolicy) -> None:
        """添加恢复策略"""
        with self._lock:
            self.recovery_policies[policy.error_type] = policy
        logging.info(f"添加恢复策略: {policy.error_type.value}")
    
    def get_circuit_breaker(self, name: str, **kwargs) -> CircuitBreaker:
        """获取或创建熔断器"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(**kwargs)
        return self.circuit_breakers[name]
    
    def execute_with_recovery(self,
                            func: Callable,
                            args: tuple = (),
                            kwargs: Optional[Dict[str, Any]] = None,
                            context: Optional[Dict[str, Any]] = None,
                            source: str = "",
                            operation: str = "") -> Any:
        """
        带错误恢复的函数执行
        
        Args:
            func: 要执行的函数
            args: 函数参数
            kwargs: 函数关键字参数
            context: 执行上下文
            source: 数据源名称
            operation: 操作名称
            
        Returns:
            函数执行结果
            
        Raises:
            最后一次执行的异常
        """
        if kwargs is None:
            kwargs = {}
        if context is None:
            context = {}
        
        start_time = time.time()
        last_exception = None
        
        try:
            # 直接执行函数
            result = func(*args, **kwargs)
            self._record_success(source, operation, time.time() - start_time)
            return result
            
        except Exception as e:
            last_exception = e
            
            # 分类错误
            error_type = ErrorClassifier.classify_error(e, context)
            
            # 创建错误信息
            error_info = ErrorInfo(
                error_type=error_type,
                error_message=str(e),
                exception=e,
                context=context,
                source=source,
                operation=operation
            )
            
            # 记录错误
            self._record_error(error_info)
            
            # 获取恢复策略
            policy = self.recovery_policies.get(error_type)
            if not policy:
                raise e
            
            # 执行恢复策略
            recovery_result = self._execute_recovery_policy(
                policy, func, args, kwargs, error_info
            )
            
            if recovery_result is not None:
                self._record_recovery_success(source, operation, time.time() - start_time)
                return recovery_result
            
            # 恢复失败，抛出原始异常
            raise e
    
    def _execute_recovery_policy(self,
                               policy: RecoveryPolicy,
                               func: Callable,
                               args: tuple,
                               kwargs: Dict[str, Any],
                               error_info: ErrorInfo) -> Any:
        """执行恢复策略"""
        recovery_start_time = time.time()
        
        for action in policy.actions:
            try:
                # 检查恢复时间限制
                if (time.time() - recovery_start_time) > policy.max_recovery_time:
                    logging.warning(f"恢复策略超时，停止恢复: {error_info.operation}")
                    break
                
                result = self._execute_recovery_action(
                    action, policy, func, args, kwargs, error_info
                )
                
                if result is not None:
                    logging.info(f"恢复成功: {action.value} for {error_info.operation}")
                    return result
                    
            except Exception as e:
                logging.warning(f"恢复动作失败: {action.value} - {str(e)}")
                error_info.retry_count += 1
                continue
        
        return None
    
    def _execute_recovery_action(self,
                               action: RecoveryAction,
                               policy: RecoveryPolicy,
                               func: Callable,
                               args: tuple,
                               kwargs: Dict[str, Any],
                               error_info: ErrorInfo) -> Any:
        """执行单个恢复动作"""
        if action == RecoveryAction.RETRY:
            return self._execute_retry(policy, func, args, kwargs, error_info)
        
        elif action == RecoveryAction.FALLBACK:
            return self._execute_fallback(policy, error_info)
        
        elif action == RecoveryAction.CIRCUIT_BREAK:
            return self._execute_circuit_break(error_info)
        
        elif action == RecoveryAction.DEGRADE:
            return self._execute_degrade(func, args, kwargs, error_info)
        
        elif action == RecoveryAction.IGNORE:
            return None
        
        elif action == RecoveryAction.ESCALATE:
            return self._execute_escalate(error_info)
        
        else:
            logging.warning(f"未知的恢复动作: {action.value}")
            return None
    
    def _execute_retry(self,
                      policy: RecoveryPolicy,
                      func: Callable,
                      args: tuple,
                      kwargs: Dict[str, Any],
                      error_info: ErrorInfo) -> Any:
        """执行重试"""
        if not policy.retry_config:
            return None
        
        retry_config = policy.retry_config
        max_attempts = min(retry_config.max_attempts, 5)  # 限制最大重试次数
        
        for attempt in range(1, max_attempts + 1):
            try:
                delay = retry_config.get_delay(attempt)
                if delay > 0:
                    time.sleep(delay)
                
                logging.info(f"重试第 {attempt} 次: {error_info.operation}")
                result = func(*args, **kwargs)
                
                # 重试成功
                self._update_recovery_stats(error_info.source, "retry_success")
                return result
                
            except Exception as e:
                error_info.retry_count += 1
                logging.warning(f"重试第 {attempt} 次失败: {str(e)}")
                
                # 如果是最后一次重试，记录失败
                if attempt == max_attempts:
                    self._update_recovery_stats(error_info.source, "retry_failed")
        
        return None
    
    def _execute_fallback(self, policy: RecoveryPolicy, error_info: ErrorInfo) -> Any:
        """执行降级到备用数据源"""
        for fallback_source in policy.fallback_sources:
            try:
                logging.info(f"尝试备用数据源: {fallback_source}")
                
                # 这里应该调用备用数据源的实现
                # 暂时返回None表示未实现
                self._update_recovery_stats(error_info.source, "fallback_success")
                return None
                
            except Exception as e:
                logging.warning(f"备用数据源 {fallback_source} 失败: {str(e)}")
                continue
        
        self._update_recovery_stats(error_info.source, "fallback_failed")
        return None
    
    def _execute_circuit_break(self, error_info: ErrorInfo) -> None:
        """执行熔断"""
        circuit_breaker = self.get_circuit_breaker(error_info.source)
        
        # 增加失败计数
        circuit_breaker.failure_count += 1
        circuit_breaker.last_failure_time = datetime.now()
        
        if circuit_breaker.failure_count >= circuit_breaker.failure_threshold:
            circuit_breaker.state = "OPEN"
            logging.warning(f"数据源 {error_info.source} 熔断器已打开")
        
        self._update_recovery_stats(error_info.source, "circuit_break_triggered")
        return None
    
    def _execute_degrade(self,
                        func: Callable,
                        args: tuple,
                        kwargs: Dict[str, Any],
                        error_info: ErrorInfo) -> Any:
        """执行降级服务"""
        try:
            # 修改参数以降低服务质量
            degraded_kwargs = kwargs.copy()
            
            # 例如：减少数据量、降低精度等
            if "limit" in degraded_kwargs:
                degraded_kwargs["limit"] = min(degraded_kwargs["limit"], 100)
            
            logging.info(f"执行降级服务: {error_info.operation}")
            result = func(*args, **degraded_kwargs)
            
            self._update_recovery_stats(error_info.source, "degrade_success")
            return result
            
        except Exception as e:
            logging.warning(f"降级服务失败: {str(e)}")
            self._update_recovery_stats(error_info.source, "degrade_failed")
            return None
    
    def _execute_escalate(self, error_info: ErrorInfo) -> None:
        """执行错误升级"""
        logging.error(f"错误需要升级处理: {error_info.error_message}")
        
        # 这里可以实现告警、通知等功能
        # 例如发送邮件、短信、推送到监控系统等
        
        self._update_recovery_stats(error_info.source, "escalated")
        return None
    
    def _record_error(self, error_info: ErrorInfo) -> None:
        """记录错误"""
        with self._lock:
            self.error_history.append(error_info)
            
            # 限制历史记录大小
            if len(self.error_history) > 1000:
                self.error_history = self.error_history[-500:]
    
    def _record_success(self, source: str, operation: str, duration: float) -> None:
        """记录成功"""
        with self._lock:
            if source not in self.recovery_stats:
                self.recovery_stats[source] = {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "total_duration": 0.0,
                    "recoveries": {}
                }
            
            stats = self.recovery_stats[source]
            stats["total_requests"] += 1
            stats["successful_requests"] += 1
            stats["total_duration"] += duration
    
    def _record_recovery_success(self, source: str, operation: str, duration: float) -> None:
        """记录恢复成功"""
        with self._lock:
            if source not in self.recovery_stats:
                self.recovery_stats[source] = {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "total_duration": 0.0,
                    "recoveries": {}
                }
            
            stats = self.recovery_stats[source]
            stats["total_requests"] += 1
            stats["successful_requests"] += 1
            stats["total_duration"] += duration
    
    def _update_recovery_stats(self, source: str, recovery_type: str) -> None:
        """更新恢复统计"""
        with self._lock:
            if source not in self.recovery_stats:
                self.recovery_stats[source] = {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "total_duration": 0.0,
                    "recoveries": {}
                }
            
            if "recoveries" not in self.recovery_stats[source]:
                self.recovery_stats[source]["recoveries"] = {}
            
            recoveries = self.recovery_stats[source]["recoveries"]
            recoveries[recovery_type] = recoveries.get(recovery_type, 0) + 1
    
    def get_error_statistics(self, 
                           source: Optional[str] = None,
                           hours: int = 24) -> Dict[str, Any]:
        """获取错误统计"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # 过滤错误历史
        filtered_errors = [
            error for error in self.error_history
            if error.timestamp >= cutoff_time and
            (source is None or error.source == source)
        ]
        
        # 统计错误类型
        error_types = {}
        for error in filtered_errors:
            error_type = error.error_type.value
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        # 统计错误来源
        error_sources = {}
        for error in filtered_errors:
            error_sources[error.source] = error_sources.get(error.source, 0) + 1
        
        return {
            "total_errors": len(filtered_errors),
            "error_types": error_types,
            "error_sources": error_sources,
            "recovery_stats": self.recovery_stats,
            "circuit_breakers": {
                name: {
                    "state": cb.state,
                    "failure_count": cb.failure_count,
                    "last_failure": cb.last_failure_time.isoformat() if cb.last_failure_time else None
                }
                for name, cb in self.circuit_breakers.items()
            }
        }
    
    def clear_error_history(self, hours: int = 24) -> None:
        """清理错误历史"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self._lock:
            self.error_history = [
                error for error in self.error_history
                if error.timestamp >= cutoff_time
            ]
    
    def reset_all_circuit_breakers(self) -> None:
        """重置所有熔断器"""
        for circuit_breaker in self.circuit_breakers.values():
            circuit_breaker.reset()
        logging.info("所有熔断器已重置")
    
    def __del__(self):
        """析构函数"""
        self._executor.shutdown(wait=True)


# 全局错误恢复管理器实例
error_recovery_manager = ErrorRecoveryManager()


def with_recovery(source: str = "", operation: str = ""):
    """装饰器：为函数添加错误恢复功能"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return error_recovery_manager.execute_with_recovery(
                func, args, kwargs, {}, source, operation
            )
        return wrapper
    return decorator