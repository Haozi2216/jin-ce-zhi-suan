"""
增强的数据源管理器
为策略进化系统提供更可靠的多级数据源备份和故障转移机制
"""
import os
import json
import time
import threading
import logging
import requests
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import hashlib
import pickle
import gzip

from src.utils.config_loader import ConfigLoader
from .multi_level_backup import (
    DataSourceType, DataSourceConfig, BackupStatus, 
    BackupTask, BackupPolicy
)


class HealthStatus(Enum):
    """健康状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    source_name: str
    status: HealthStatus
    response_time: float
    error_message: str = ""
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class DataFetchResult:
    """数据获取结果"""
    success: bool
    data: Any = None
    source_used: str = ""
    error_message: str = ""
    fetch_time: float = 0.0
    cache_hit: bool = False
    fallback_used: bool = False


class EnhancedDataSourceManager:
    """增强的数据源管理器"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.data_sources: Dict[str, DataSourceConfig] = {}
        self.active_source: Optional[str] = None
        self.source_health: Dict[str, HealthStatus] = {}
        self.source_stats: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._health_check_interval = 30  # 秒
        self._health_check_thread = None
        self._running = False
        self._config_path = config_path
        self._executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="datasource")
        
        # 缓存机制
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._cache_ttl = 300  # 5分钟缓存
        
        # 重试配置
        self._max_retry_attempts = 3
        self._retry_delay_base = 1.0
        self._circuit_breaker_threshold = 5  # 连续失败次数触发熔断
        self._circuit_breaker_timeout = 300  # 熔断恢复时间
        
        self._load_config()
        self._initialize_stats()
        
    def _load_config(self) -> None:
        """加载配置"""
        try:
            cfg = ConfigLoader.reload()
            datasource_config = cfg.get("evolution.data_sources", {})
            
            # 默认数据源配置
            default_sources = [
                {
                    "name": "primary_api",
                    "source_type": "primary",
                    "connection_params": {
                        "url": cfg.get("data_provider.default_api_url", ""),
                        "api_key": cfg.get("data_provider.default_api_key", ""),
                        "timeout": 30
                    },
                    "priority": 1
                },
                {
                    "name": "tushare_backup",
                    "source_type": "secondary", 
                    "connection_params": {
                        "token": cfg.get("data_provider.tushare_token", ""),
                        "timeout": 30
                    },
                    "priority": 2
                },
                {
                    "name": "local_cache",
                    "source_type": "cache",
                    "connection_params": {
                        "cache_dir": cfg.get("data_provider.local_cache_dir", "data/history/cache")
                    },
                    "priority": 3
                }
            ]
            
            # 合并用户配置
            sources_to_add = datasource_config.get("sources", default_sources)
            
            for source_config in sources_to_add:
                config = DataSourceConfig(
                    source_type=DataSourceType(source_config["source_type"]),
                    name=source_config["name"],
                    connection_params=source_config["connection_params"],
                    priority=source_config.get("priority", 999),
                    enabled=source_config.get("enabled", True),
                    read_only=source_config.get("read_only", False),
                    timeout=source_config.get("timeout", 30),
                    retry_count=source_config.get("retry_count", 3),
                    retry_delay=source_config.get("retry_delay", 1.0)
                )
                self.add_data_source(config)
                
            # 加载其他配置
            self._health_check_interval = datasource_config.get("health_check_interval", 30)
            self._cache_ttl = datasource_config.get("cache_ttl", 300)
            self._max_retry_attempts = datasource_config.get("max_retry_attempts", 3)
            
        except Exception as e:
            logging.error(f"加载数据源配置失败: {e}")
            # 使用最小可用配置
            self._add_fallback_sources()
    
    def _add_fallback_sources(self) -> None:
        """添加备用数据源"""
        fallback_config = DataSourceConfig(
            source_type=DataSourceType.BACKUP,
            name="fallback_local",
            connection_params={"cache_dir": "data/history/cache"},
            priority=999,
            enabled=True
        )
        self.add_data_source(fallback_config)
    
    def _initialize_stats(self) -> None:
        """初始化统计信息"""
        with self._lock:
            for name in self.data_sources:
                self.source_stats[name] = {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "total_response_time": 0.0,
                    "last_success": None,
                    "last_failure": None,
                    "consecutive_failures": 0,
                    "circuit_breaker_open": False,
                    "circuit_breaker_opened_at": None
                }
                self.source_health[name] = HealthStatus.UNKNOWN
    
    def add_data_source(self, config: DataSourceConfig) -> None:
        """添加数据源"""
        with self._lock:
            self.data_sources[config.name] = config
            self.source_health[config.name] = HealthStatus.UNKNOWN
            self.source_stats[config.name] = {
                "total_requests": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "total_response_time": 0.0,
                "last_success": None,
                "last_failure": None,
                "consecutive_failures": 0,
                "circuit_breaker_open": False,
                "circuit_breaker_opened_at": None
            }
            
            # 如果这是第一个启用的主数据源，设为活跃
            if (self.active_source is None and config.enabled and 
                config.source_type == DataSourceType.PRIMARY):
                self.active_source = config.name
                
        logging.info(f"添加数据源: {config.name} ({config.source_type.value})")
    
    def fetch_data_with_fallback(self, 
                                operation: str,
                                params: Dict[str, Any],
                                timeout: Optional[float] = None) -> DataFetchResult:
        """
        带故障转移的数据获取
        
        Args:
            operation: 操作类型 (如 'fetch_minute_data', 'get_latest_bar')
            params: 操作参数
            timeout: 超时时间
            
        Returns:
            DataFetchResult: 数据获取结果
        """
        start_time = time.time()
        
        # 生成缓存键
        cache_key = self._generate_cache_key(operation, params)
        
        # 检查缓存
        cached_result = self._check_cache(cache_key)
        if cached_result:
            return DataFetchResult(
                success=True,
                data=cached_result,
                cache_hit=True,
                fetch_time=time.time() - start_time
            )
        
        # 获取可用数据源列表（按优先级排序）
        available_sources = self._get_available_sources()
        
        if not available_sources:
            return DataFetchResult(
                success=False,
                error_message="没有可用的数据源",
                fetch_time=time.time() - start_time
            )
        
        last_error = ""
        
        # 尝试每个数据源
        for source_name in available_sources:
            if self._is_circuit_breaker_open(source_name):
                logging.warning(f"数据源 {source_name} 熔断器已打开，跳过")
                continue
                
            result = self._fetch_from_source(source_name, operation, params, timeout)
            
            if result.success:
                # 更新活跃数据源
                with self._lock:
                    if self.active_source != source_name:
                        logging.info(f"切换活跃数据源: {self.active_source} -> {source_name}")
                        self.active_source = source_name
                
                # 缓存成功的结果
                self._cache_result(cache_key, result.data)
                
                return DataFetchResult(
                    success=True,
                    data=result.data,
                    source_used=source_name,
                    fetch_time=time.time() - start_time,
                    fallback_used=(source_name != available_sources[0])
                )
            else:
                last_error = result.error_message
                self._handle_source_failure(source_name, result.error_message)
        
        # 所有数据源都失败
        return DataFetchResult(
            success=False,
            error_message=f"所有数据源都失败。最后错误: {last_error}",
            fetch_time=time.time() - start_time
        )
    
    def _fetch_from_source(self, 
                          source_name: str,
                          operation: str,
                          params: Dict[str, Any],
                          timeout: Optional[float] = None) -> DataFetchResult:
        """从指定数据源获取数据"""
        start_time = time.time()
        
        try:
            config = self.data_sources[source_name]
            actual_timeout = timeout or config.timeout
            
            # 更新统计信息
            with self._lock:
                self.source_stats[source_name]["total_requests"] += 1
            
            # 根据数据源类型执行相应的操作
            if config.source_type == DataSourceType.PRIMARY:
                result = self._fetch_from_primary_api(config, operation, params, actual_timeout)
            elif config.source_type == DataSourceType.SECONDARY:
                result = self._fetch_from_secondary_source(config, operation, params, actual_timeout)
            elif config.source_type == DataSourceType.CACHE:
                result = self._fetch_from_cache(config, operation, params, actual_timeout)
            else:
                result = DataFetchResult(success=False, error_message=f"不支持的数据源类型: {config.source_type}")
            
            # 更新统计信息
            fetch_time = time.time() - start_time
            with self._lock:
                stats = self.source_stats[source_name]
                stats["total_response_time"] += fetch_time
                
                if result.success:
                    stats["successful_requests"] += 1
                    stats["last_success"] = datetime.now()
                    stats["consecutive_failures"] = 0
                    self.source_health[source_name] = HealthStatus.HEALTHY
                else:
                    stats["failed_requests"] += 1
                    stats["last_failure"] = datetime.now()
                    stats["consecutive_failures"] += 1
                    self.source_health[source_name] = HealthStatus.UNHEALTHY
                    
                    # 检查是否需要打开熔断器
                    if stats["consecutive_failures"] >= self._circuit_breaker_threshold:
                        stats["circuit_breaker_open"] = True
                        stats["circuit_breaker_opened_at"] = datetime.now()
                        logging.warning(f"数据源 {source_name} 熔断器已打开")
            
            result.fetch_time = fetch_time
            return result
            
        except Exception as e:
            error_msg = f"数据源 {source_name} 异常: {str(e)}"
            logging.error(error_msg)
            
            with self._lock:
                stats = self.source_stats[source_name]
                stats["failed_requests"] += 1
                stats["last_failure"] = datetime.now()
                stats["consecutive_failures"] += 1
                self.source_health[source_name] = HealthStatus.UNHEALTHY
            
            return DataFetchResult(success=False, error_message=error_msg)
    
    def _fetch_from_primary_api(self, 
                               config: DataSourceConfig,
                               operation: str,
                               params: Dict[str, Any],
                               timeout: float) -> DataFetchResult:
        """从主API获取数据"""
        try:
            # 这里应该调用实际的数据提供器
            # 为了演示，我们模拟一个API调用
            url = config.connection_params.get("url", "")
            api_key = config.connection_params.get("api_key", "")
            
            if not url:
                return DataFetchResult(success=False, error_message="主API URL未配置")
            
            headers = {"X-API-Key": api_key} if api_key else {}
            
            # 根据操作类型构建请求
            if operation == "fetch_minute_data":
                endpoint = "/market/minutes"
                request_params = {
                    "code": params.get("code", ""),
                    "start_time": params.get("start_time", ""),
                    "end_time": params.get("end_time", ""),
                    "limit": params.get("limit", 10000)
                }
            elif operation == "get_latest_bar":
                endpoint = "/market/latest"
                request_params = {"codes": params.get("code", "")}
            else:
                return DataFetchResult(success=False, error_message=f"不支持的操作: {operation}")
            
            response = requests.get(
                f"{url}{endpoint}",
                params=request_params,
                headers=headers,
                timeout=timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return DataFetchResult(success=True, data=data)
            else:
                return DataFetchResult(
                    success=False, 
                    error_message=f"API请求失败: HTTP {response.status_code}"
                )
                
        except requests.exceptions.Timeout:
            return DataFetchResult(success=False, error_message="请求超时")
        except requests.exceptions.ConnectionError:
            return DataFetchResult(success=False, error_message="连接错误")
        except Exception as e:
            return DataFetchResult(success=False, error_message=f"API调用异常: {str(e)}")
    
    def _fetch_from_secondary_source(self,
                                   config: DataSourceConfig,
                                   operation: str,
                                   params: Dict[str, Any],
                                   timeout: float) -> DataFetchResult:
        """从备用数据源获取数据"""
        # 这里可以实现Tushare、Akshare等备用数据源
        # 暂时返回未实现
        return DataFetchResult(success=False, error_message="备用数据源暂未实现")
    
    def _fetch_from_cache(self,
                         config: DataSourceConfig,
                         operation: str,
                         params: Dict[str, Any],
                         timeout: float) -> DataFetchResult:
        """从缓存获取数据"""
        try:
            cache_dir = config.connection_params.get("cache_dir", "data/history/cache")
            cache_key = self._generate_cache_key(operation, params)
            cache_file = os.path.join(cache_dir, f"{cache_key}.json")
            
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return DataFetchResult(success=True, data=data)
            else:
                return DataFetchResult(success=False, error_message="缓存文件不存在")
                
        except Exception as e:
            return DataFetchResult(success=False, error_message=f"缓存读取失败: {str(e)}")
    
    def _get_available_sources(self) -> List[str]:
        """获取可用的数据源列表（按优先级排序）"""
        with self._lock:
            available = [
                (name, config) for name, config in self.data_sources.items()
                if config.enabled and self.source_health.get(name) != HealthStatus.UNHEALTHY
            ]
        
        # 按优先级排序
        available.sort(key=lambda x: x[1].priority)
        return [name for name, _ in available]
    
    def _is_circuit_breaker_open(self, source_name: str) -> bool:
        """检查熔断器是否打开"""
        with self._lock:
            stats = self.source_stats.get(source_name, {})
            if not stats.get("circuit_breaker_open", False):
                return False
            
            # 检查熔断器是否应该恢复
            opened_at = stats.get("circuit_breaker_opened_at")
            if opened_at and (datetime.now() - opened_at).seconds > self._circuit_breaker_timeout:
                stats["circuit_breaker_open"] = False
                stats["circuit_breaker_opened_at"] = None
                stats["consecutive_failures"] = 0
                logging.info(f"数据源 {source_name} 熔断器已恢复")
                return False
            
            return True
    
    def _handle_source_failure(self, source_name: str, error_message: str) -> None:
        """处理数据源失败"""
        logging.error(f"数据源 {source_name} 失败: {error_message}")
        
        # 如果是活跃数据源失败，尝试故障转移
        if self.active_source == source_name:
            if self.failover_to_next_source():
                logging.info(f"已从 {source_name} 故障转移到新数据源")
    
    def failover_to_next_source(self) -> bool:
        """故障转移到下一个数据源"""
        with self._lock:
            current_source = self.active_source
            if current_source:
                self.source_health[current_source] = HealthStatus.UNHEALTHY
            
            new_source = self._find_best_source()
            if new_source and new_source != current_source:
                self.active_source = new_source
                logging.info(f"数据源故障转移: {current_source} -> {new_source}")
                return True
            return False
    
    def _find_best_source(self) -> Optional[str]:
        """找到最佳数据源"""
        available_sources = [
            (name, config) for name, config in self.data_sources.items()
            if (config.enabled and 
                self.source_health.get(name) != HealthStatus.UNHEALTHY and
                not self._is_circuit_breaker_open(name))
        ]
        
        if not available_sources:
            return None
        
        # 按优先级排序
        available_sources.sort(key=lambda x: x[1].priority)
        
        # 优先选择主数据源
        for name, config in available_sources:
            if config.source_type == DataSourceType.PRIMARY:
                return name
        
        # 其次选择备用数据源
        for name, config in available_sources:
            if config.source_type == DataSourceType.SECONDARY:
                return name
        
        return available_sources[0][0] if available_sources else None
    
    def _generate_cache_key(self, operation: str, params: Dict[str, Any]) -> str:
        """生成缓存键"""
        key_data = f"{operation}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _check_cache(self, cache_key: str) -> Optional[Any]:
        """检查缓存"""
        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if (datetime.now() - timestamp).seconds < self._cache_ttl:
                return data
            else:
                del self._cache[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, data: Any) -> None:
        """缓存结果"""
        self._cache[cache_key] = (data, datetime.now())
        
        # 限制缓存大小
        if len(self._cache) > 1000:
            # 删除最旧的缓存项
            oldest_key = min(self._cache.keys(), 
                           key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
    
    def start_health_check(self) -> None:
        """启动健康检查"""
        if self._health_check_thread is None or not self._health_check_thread.is_alive():
            self._running = True
            self._health_check_thread = threading.Thread(target=self._health_check_loop)
            self._health_check_thread.daemon = True
            self._health_check_thread.start()
            logging.info("数据源健康检查已启动")
    
    def stop_health_check(self) -> None:
        """停止健康检查"""
        self._running = False
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)
        self._executor.shutdown(wait=True)
        logging.info("数据源健康检查已停止")
    
    def _health_check_loop(self) -> None:
        """健康检查循环"""
        while self._running:
            try:
                self._perform_health_check()
                time.sleep(self._health_check_interval)
            except Exception as e:
                logging.error(f"健康检查失败: {e}")
                time.sleep(10)
    
    def _perform_health_check(self) -> None:
        """执行健康检查"""
        futures = {}
        
        for name, config in self.data_sources.items():
            if not config.enabled:
                continue
                
            future = self._executor.submit(self._check_source_health, name, config)
            futures[future] = name
        
        for future in as_completed(futures, timeout=20):
            source_name = futures[future]
            try:
                result = future.result(timeout=5)
                with self._lock:
                    self.source_health[source_name] = result.status
            except TimeoutError:
                logging.warning(f"数据源 {source_name} 健康检查超时")
                with self._lock:
                    self.source_health[source_name] = HealthStatus.UNHEALTHY
            except Exception as e:
                logging.error(f"数据源 {source_name} 健康检查异常: {e}")
                with self._lock:
                    self.source_health[source_name] = HealthStatus.UNHEALTHY
    
    def _check_source_health(self, name: str, config: DataSourceConfig) -> HealthCheckResult:
        """检查单个数据源的健康状态"""
        start_time = time.time()
        
        try:
            # 简单的连通性检查
            if config.source_type == DataSourceType.PRIMARY:
                url = config.connection_params.get("url", "")
                if url:
                    response = requests.get(f"{url}/health", timeout=config.timeout)
                    if response.status_code == 200:
                        return HealthCheckResult(
                            source_name=name,
                            status=HealthStatus.HEALTHY,
                            response_time=time.time() - start_time
                        )
                    else:
                        return HealthCheckResult(
                            source_name=name,
                            status=HealthStatus.UNHEALTHY,
                            response_time=time.time() - start_time,
                            error_message=f"HTTP {response.status_code}"
                        )
            
            # 其他类型的健康检查...
            return HealthCheckResult(
                source_name=name,
                status=HealthStatus.HEALTHY,
                response_time=time.time() - start_time
            )
            
        except Exception as e:
            return HealthCheckResult(
                source_name=name,
                status=HealthStatus.UNHEALTHY,
                response_time=time.time() - start_time,
                error_message=str(e)
            )
    
    def get_status(self) -> Dict[str, Any]:
        """获取管理器状态"""
        with self._lock:
            sources_status = {}
            for name, config in self.data_sources.items():
                stats = self.source_stats.get(name, {})
                sources_status[name] = {
                    "type": config.source_type.value,
                    "enabled": config.enabled,
                    "priority": config.priority,
                    "health": self.source_health.get(name, HealthStatus.UNKNOWN).value,
                    "stats": {
                        "total_requests": stats.get("total_requests", 0),
                        "success_rate": (
                            stats.get("successful_requests", 0) / max(stats.get("total_requests", 1), 1) * 100
                        ),
                        "avg_response_time": (
                            stats.get("total_response_time", 0) / max(stats.get("successful_requests", 1), 1)
                        ),
                        "consecutive_failures": stats.get("consecutive_failures", 0),
                        "circuit_breaker_open": stats.get("circuit_breaker_open", False)
                    }
                }
            
            return {
                "active_source": self.active_source,
                "total_sources": len(self.data_sources),
                "healthy_sources": len([
                    name for name, health in self.source_health.items()
                    if health == HealthStatus.HEALTHY
                ]),
                "cache_size": len(self._cache),
                "health_check_running": self._running,
                "sources": sources_status
            }
    
    def reset_circuit_breaker(self, source_name: str) -> bool:
        """重置指定数据源的熔断器"""
        with self._lock:
            if source_name in self.source_stats:
                stats = self.source_stats[source_name]
                stats["circuit_breaker_open"] = False
                stats["circuit_breaker_opened_at"] = None
                stats["consecutive_failures"] = 0
                self.source_health[source_name] = HealthStatus.UNKNOWN
                logging.info(f"数据源 {source_name} 熔断器已重置")
                return True
            return False
    
    def clear_cache(self) -> None:
        """清空缓存"""
        self._cache.clear()
        logging.info("数据源缓存已清空")
    
    def __del__(self):
        """析构函数"""
        self.stop_health_check()