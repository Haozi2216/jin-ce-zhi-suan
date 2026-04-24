"""
多级数据源备份机制
确保进化系统的数据安全和可靠性，支持多级备份和故障转移
"""
import os
import json
import sqlite3
import threading
import time
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import shutil
import pickle
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


class DataSourceType(Enum):
    """数据源类型"""
    PRIMARY = "primary"           # 主数据源
    SECONDARY = "secondary"       # 备用数据源
    CACHE = "cache"              # 缓存数据源
    BACKUP = "backup"            # 备份数据源
    REMOTE = "remote"            # 远程数据源


class BackupStatus(Enum):
    """备份状态"""
    PENDING = "pending"          # 等待备份
    IN_PROGRESS = "in_progress"  # 备份中
    COMPLETED = "completed"      # 备份完成
    FAILED = "failed"           # 备份失败
    CORRUPTED = "corrupted"      # 数据损坏


@dataclass
class DataSourceConfig:
    """数据源配置"""
    source_type: DataSourceType
    name: str
    connection_params: Dict[str, Any]
    priority: int  # 优先级，数字越小优先级越高
    enabled: bool = True
    read_only: bool = False
    timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 1.0


@dataclass
class BackupTask:
    """备份任务"""
    task_id: str
    source_type: DataSourceType
    data_key: str
    data: Any
    timestamp: datetime
    status: BackupStatus = BackupStatus.PENDING
    retry_count: int = 0
    error_message: str = ""
    checksum: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BackupTask':
        """从字典创建"""
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['status'] = BackupStatus(data['status'])
        return cls(**data)


@dataclass
class BackupPolicy:
    """备份策略"""
    auto_backup: bool = True
    backup_interval: int = 300  # 秒
    max_backup_age: int = 7 * 24 * 3600  # 秒
    compression_enabled: bool = True
    encryption_enabled: bool = False
    verification_enabled: bool = True
    parallel_backup: bool = True
    max_parallel_tasks: int = 3


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self):
        self.data_sources: Dict[str, DataSourceConfig] = {}
        self.active_source: Optional[str] = None
        self.source_health: Dict[str, bool] = {}
        self._lock = threading.Lock()
        self._health_check_interval = 60  # 秒
        self._health_check_thread = None
        self._running = False
        
    def add_data_source(self, config: DataSourceConfig) -> None:
        """添加数据源"""
        with self._lock:
            self.data_sources[config.name] = config
            self.source_health[config.name] = True
            
            # 如果这是第一个启用的数据源，设为活跃
            if (self.active_source is None and config.enabled and 
                config.source_type == DataSourceType.PRIMARY):
                self.active_source = config.name
    
    def remove_data_source(self, name: str) -> None:
        """移除数据源"""
        with self._lock:
            if name in self.data_sources:
                del self.data_sources[name]
            if name in self.source_health:
                del self.source_health[name]
            if self.active_source == name:
                self.active_source = self._find_best_source()
    
    def get_active_source(self) -> Optional[DataSourceConfig]:
        """获取当前活跃数据源"""
        with self._lock:
            if self.active_source and self.active_source in self.data_sources:
                return self.data_sources[self.active_source]
            return None
    
    def failover_to_next_source(self) -> bool:
        """故障转移到下一个数据源"""
        with self._lock:
            current_source = self.active_source
            if current_source:
                self.source_health[current_source] = False
            
            new_source = self._find_best_source()
            if new_source and new_source != current_source:
                self.active_source = new_source
                logging.info(f"数据源故障转移: {current_source} -> {new_source}")
                return True
            return False
    
    def _find_best_source(self) -> Optional[str]:
        """找到最佳数据源"""
        # 按优先级和健康状态排序
        available_sources = [
            (name, config) for name, config in self.data_sources.items()
            if config.enabled and self.source_health.get(name, True)
        ]
        
        if not available_sources:
            return None
        
        # 按优先级排序，优先级数字越小越好
        available_sources.sort(key=lambda x: x[1].priority)
        
        # 优先选择主数据源
        for name, config in available_sources:
            if config.source_type == DataSourceType.PRIMARY:
                return name
        
        # 其次选择备用数据源
        for name, config in available_sources:
            if config.source_type == DataSourceType.SECONDARY:
                return name
        
        # 最后选择其他类型的数据源
        return available_sources[0][0] if available_sources else None
    
    def start_health_check(self) -> None:
        """启动健康检查"""
        if self._health_check_thread is None or not self._health_check_thread.is_alive():
            self._running = True
            self._health_check_thread = threading.Thread(target=self._health_check_loop)
            self._health_check_thread.daemon = True
            self._health_check_thread.start()
    
    def stop_health_check(self) -> None:
        """停止健康检查"""
        self._running = False
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)
    
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
        with self._lock:
            for name, config in self.data_sources.items():
                if not config.enabled:
                    continue
                
                try:
                    # 简单的健康检查（实际实现中应该根据具体数据源类型进行检查）
                    if config.source_type in [DataSourceType.PRIMARY, DataSourceType.SECONDARY]:
                        # 检查数据库连接
                        self._check_database_health(config)
                    elif config.source_type == DataSourceType.CACHE:
                        # 检查缓存可用性
                        self._check_cache_health(config)
                    elif config.source_type == DataSourceType.BACKUP:
                        # 检查备份文件可访问性
                        self._check_backup_health(config)
                    
                    self.source_health[name] = True
                    
                except Exception as e:
                    logging.warning(f"数据源 {name} 健康检查失败: {e}")
                    self.source_health[name] = False
                    
                    # 如果是活跃数据源，尝试故障转移
                    if name == self.active_source:
                        self.failover_to_next_source()
    
    def _check_database_health(self, config: DataSourceConfig) -> None:
        """检查数据库健康状态"""
        # 这里应该实现具体的数据库健康检查逻辑
        # 例如：执行简单查询，检查连接等
        pass
    
    def _check_cache_health(self, config: DataSourceConfig) -> None:
        """检查缓存健康状态"""
        # 这里应该实现具体的缓存健康检查逻辑
        pass
    
    def _check_backup_health(self, config: DataSourceConfig) -> None:
        """检查备份健康状态"""
        # 这里应该实现具体的备份健康检查逻辑
        backup_path = Path(config.connection_params.get("path", ""))
        if backup_path.exists():
            # 检查备份文件是否可读
            with open(backup_path, 'rb') as f:
                f.read(1024)  # 尝试读取前1024字节
        else:
            raise FileNotFoundError(f"备份文件不存在: {backup_path}")


class MultiLevelBackupSystem:
    """多级备份系统"""

    def __init__(self, backup_dir: str = "evolution_backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)

        # 创建子目录
        (self.backup_dir / "primary").mkdir(exist_ok=True)
        (self.backup_dir / "secondary").mkdir(exist_ok=True)
        (self.backup_dir / "cache").mkdir(exist_ok=True)
        (self.backup_dir / "remote").mkdir(exist_ok=True)

        # 数据源管理器
        self.data_source_manager = DataSourceManager()
        self._initialize_default_sources()

        # 备份策略
        self.backup_policy = BackupPolicy()
        
        # 备份任务队列
        self.backup_queue: List[BackupTask] = []
        self.completed_tasks: List[BackupTask] = []
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=self.backup_policy.max_parallel_tasks)
        
        # 线程锁
        self._lock = threading.Lock()
        self._running = False
        
        # 备份索引数据库
        self.backup_db = self.backup_dir / "backup_index.db"
        self._init_backup_db()
        
        # 日志
        self.logger = logging.getLogger(__name__)
    
    def _init_backup_db(self) -> None:
        """初始化备份索引数据库"""
        conn = sqlite3.connect(str(self.backup_db))
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_tasks (
                task_id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                data_key TEXT NOT NULL,
                file_path TEXT,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL,
                checksum TEXT,
                file_size INTEGER,
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_sources (
                name TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                priority INTEGER NOT NULL,
                enabled BOOLEAN DEFAULT 1,
                health_status BOOLEAN DEFAULT 1,
                last_check TEXT,
                config_json TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def _initialize_default_sources(self) -> None:
        """初始化默认本地备份源"""
        defaults = [
            DataSourceConfig(
                source_type=DataSourceType.PRIMARY,
                name="primary",
                connection_params={"path": str((self.backup_dir / "primary").resolve())},
                priority=1,
            ),
            DataSourceConfig(
                source_type=DataSourceType.SECONDARY,
                name="secondary",
                connection_params={"path": str((self.backup_dir / "secondary").resolve())},
                priority=2,
            ),
            DataSourceConfig(
                source_type=DataSourceType.CACHE,
                name="cache",
                connection_params={"path": str((self.backup_dir / "cache").resolve())},
                priority=3,
            ),
            DataSourceConfig(
                source_type=DataSourceType.REMOTE,
                name="remote",
                connection_params={"path": str((self.backup_dir / "remote").resolve())},
                priority=4,
            ),
        ]
        for config in defaults:
            if config.name not in self.data_source_manager.data_sources:
                self.data_source_manager.add_data_source(config)

    def add_backup_source(self, config: DataSourceConfig) -> None:
        """添加备份源"""
        self.data_source_manager.add_data_source(config)
        
        # 保存到数据库
        conn = sqlite3.connect(str(self.backup_db))
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO backup_sources 
            (name, source_type, priority, enabled, config_json)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            config.name,
            config.source_type.value,
            config.priority,
            config.enabled,
            json.dumps(asdict(config))
        ))
        
        conn.commit()
        conn.close()
    
    def backup_data(self, data_key: str, data: Any, source_type: DataSourceType = DataSourceType.PRIMARY) -> str:
        """备份数据"""
        # 生成任务ID
        task_id = hashlib.md5(f"{data_key}_{datetime.now().isoformat()}".encode()).hexdigest()[:16]
        
        # 创建备份任务
        task = BackupTask(
            task_id=task_id,
            source_type=source_type,
            data_key=data_key,
            data=data,
            timestamp=datetime.now()
        )
        
        # 计算校验和
        task.checksum = self._calculate_checksum(data)
        
        with self._lock:
            self.backup_queue.append(task)
        
        # 如果启用自动备份，立即执行
        if self.backup_policy.auto_backup:
            self._process_backup_task(task)
        
        return task_id
    
    def _process_backup_task(self, task: BackupTask) -> None:
        """处理备份任务"""
        task.status = BackupStatus.IN_PROGRESS
        
        try:
            # 序列化数据
            serialized_data = self._serialize_data(task.data)
            
            # 压缩数据（如果启用）
            if self.backup_policy.compression_enabled:
                serialized_data = gzip.compress(serialized_data)
            
            # 保存到多个备份源
            backup_files = []
            active_source = self.data_source_manager.get_active_source()
            
            if active_source:
                # 保存到活跃数据源
                file_path = self._save_to_source(active_source, task, serialized_data)
                backup_files.append(file_path)
                
                # 异步保存到其他数据源
                if self.backup_policy.parallel_backup:
                    self._backup_to_other_sources(task, serialized_data, active_source.name)
                else:
                    self._backup_to_other_sources_sync(task, serialized_data, active_source.name)
            
            # 验证备份（如果启用）
            if self.backup_policy.verification_enabled and backup_files:
                self._verify_backup(task, backup_files[0])
            
            task.status = BackupStatus.COMPLETED
            self.logger.info(f"备份任务完成: {task.task_id}")
            
        except Exception as e:
            task.status = BackupStatus.FAILED
            task.error_message = str(e)
            self.logger.error(f"备份任务失败: {task.task_id}, 错误: {e}")
            
            # 重试逻辑
            if task.retry_count < 3:
                task.retry_count += 1
                time.sleep(2 ** task.retry_count)  # 指数退避
                self._process_backup_task(task)
        
        finally:
            # 更新数据库
            self._update_backup_task_in_db(task)
            
            # 移动任务到完成列表
            with self._lock:
                if task in self.backup_queue:
                    self.backup_queue.remove(task)
                self.completed_tasks.append(task)
                
                # 限制完成任务列表大小
                if len(self.completed_tasks) > 1000:
                    self.completed_tasks = self.completed_tasks[-500:]
    
    def _save_to_source(self, source_config: DataSourceConfig, task: BackupTask, data: bytes) -> Path:
        """保存到指定数据源"""
        if source_config.source_type in [DataSourceType.PRIMARY, DataSourceType.SECONDARY]:
            return self._save_to_file(source_config, task, data)
        elif source_config.source_type == DataSourceType.CACHE:
            return self._save_to_cache(source_config, task, data)
        elif source_config.source_type == DataSourceType.REMOTE:
            return self._save_to_remote(source_config, task, data)
        else:
            raise ValueError(f"不支持的数据源类型: {source_config.source_type}")
    
    def _save_to_file(self, source_config: DataSourceConfig, task: BackupTask, data: bytes) -> Path:
        """保存到文件"""
        source_dir = self.backup_dir / source_config.source_type.value
        source_dir.mkdir(exist_ok=True)
        
        # 生成文件名
        timestamp_str = task.timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"{task.data_key}_{timestamp_str}_{task.task_id}.bak"
        if self.backup_policy.compression_enabled:
            filename += ".gz"
        
        file_path = source_dir / filename
        
        # 写入文件
        with open(file_path, 'wb') as f:
            f.write(data)
        
        return file_path
    
    def _save_to_cache(self, source_config: DataSourceConfig, task: BackupTask, data: bytes) -> Path:
        """保存到缓存"""
        # 这里应该实现具体的缓存保存逻辑
        # 暂时使用文件作为缓存
        return self._save_to_file(source_config, task, data)
    
    def _save_to_remote(self, source_config: DataSourceConfig, task: BackupTask, data: bytes) -> Path:
        """保存到远程存储"""
        # 这里应该实现具体的远程保存逻辑
        # 暂时使用文件作为远程存储
        return self._save_to_file(source_config, task, data)
    
    def _backup_to_other_sources(self, task: BackupTask, data: bytes, exclude_source: str) -> None:
        """异步备份到其他数据源"""
        futures = []
        
        for name, config in self.data_source_manager.data_sources.items():
            if (name != exclude_source and config.enabled and 
                config.source_type != DataSourceType.BACKUP):
                
                future = self.executor.submit(self._save_to_source, config, task, data)
                futures.append(future)
        
        # 等待所有备份完成
        for future in as_completed(futures):
            try:
                future.result(timeout=30)
            except Exception as e:
                self.logger.warning(f"备份数据源失败: {e}")
    
    def _backup_to_other_sources_sync(self, task: BackupTask, data: bytes, exclude_source: str) -> None:
        """同步备份到其他数据源"""
        for name, config in self.data_source_manager.data_sources.items():
            if (name != exclude_source and config.enabled and 
                config.source_type != DataSourceType.BACKUP):
                try:
                    self._save_to_source(config, task, data)
                except Exception as e:
                    self.logger.warning(f"备份数据源失败: {e}")
    
    def _serialize_data(self, data: Any) -> bytes:
        """序列化数据"""
        try:
            # 尝试使用JSON序列化
            if isinstance(data, (dict, list, str, int, float, bool)):
                json_str = json.dumps(data, ensure_ascii=False)
                return json_str.encode('utf-8')
            else:
                # 使用pickle序列化
                return pickle.dumps(data)
        except Exception:
            # 最后尝试pickle
            return pickle.dumps(data)
    
    def _calculate_checksum(self, data: Any) -> str:
        """计算数据校验和"""
        try:
            serialized = self._serialize_data(data)
            return hashlib.sha256(serialized).hexdigest()
        except Exception:
            return ""
    
    def _verify_backup(self, task: BackupTask, file_path: Path) -> bool:
        """验证备份文件"""
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            # 解压缩（如果需要）
            if self.backup_policy.compression_enabled and file_path.suffix == '.gz':
                file_data = gzip.decompress(file_data)
            
            # 计算校验和
            calculated_checksum = hashlib.sha256(file_data).hexdigest()
            
            # 比较校验和
            if calculated_checksum != task.checksum:
                raise ValueError("备份文件校验和不匹配")
            
            return True
            
        except Exception as e:
            self.logger.error(f"备份验证失败: {e}")
            return False
    
    def _update_backup_task_in_db(self, task: BackupTask) -> None:
        """更新数据库中的备份任务"""
        conn = sqlite3.connect(str(self.backup_db))
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO backup_tasks 
            (task_id, source_type, data_key, timestamp, status, checksum, retry_count, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            task.task_id,
            task.source_type.value,
            task.data_key,
            task.timestamp.isoformat(),
            task.status.value,
            task.checksum,
            task.retry_count,
            task.error_message
        ))
        
        conn.commit()
        conn.close()
    
    def restore_data(self, data_key: str, source_type: Optional[DataSourceType] = None) -> Optional[Any]:
        """恢复数据"""
        # 首先尝试从缓存恢复
        if source_type is None or source_type == DataSourceType.CACHE:
            data = self._restore_from_source(data_key, DataSourceType.CACHE)
            if data is not None:
                return data
        
        # 然后尝试从主数据源恢复
        if source_type is None or source_type == DataSourceType.PRIMARY:
            data = self._restore_from_source(data_key, DataSourceType.PRIMARY)
            if data is not None:
                return data
        
        # 接着尝试从备用数据源恢复
        if source_type is None or source_type == DataSourceType.SECONDARY:
            data = self._restore_from_source(data_key, DataSourceType.SECONDARY)
            if data is not None:
                return data
        
        # 最后尝试从备份数据源恢复
        if source_type is None or source_type == DataSourceType.BACKUP:
            data = self._restore_from_source(data_key, DataSourceType.BACKUP)
            if data is not None:
                return data
        
        return None
    
    def _restore_from_source(self, data_key: str, source_type: DataSourceType) -> Optional[Any]:
        """从指定数据源恢复数据"""
        try:
            # 查找最新的备份文件
            backup_files = self._find_backup_files(data_key, source_type)
            
            if not backup_files:
                return None
            
            # 选择最新的备份文件
            latest_file = max(backup_files, key=lambda f: f.stat().st_mtime)
            
            # 读取文件
            with open(latest_file, 'rb') as f:
                file_data = f.read()
            
            # 解压缩（如果需要）
            if latest_file.suffix == '.gz':
                file_data = gzip.decompress(file_data)
            
            # 反序列化数据
            return self._deserialize_data(file_data)
            
        except Exception as e:
            self.logger.error(f"从数据源 {source_type.value} 恢复数据失败: {e}")
            return None
    
    def _find_backup_files(self, data_key: str, source_type: DataSourceType) -> List[Path]:
        """查找备份文件"""
        source_dir = self.backup_dir / source_type.value
        
        if not source_dir.exists():
            return []
        
        backup_files = []
        pattern = f"{data_key}_*.bak*"
        
        for file_path in source_dir.glob(pattern):
            backup_files.append(file_path)
        
        return backup_files
    
    def _deserialize_data(self, data: bytes) -> Any:
        """反序列化数据"""
        try:
            # 尝试JSON反序列化
            json_str = data.decode('utf-8')
            return json.loads(json_str)
        except Exception:
            try:
                # 尝试pickle反序列化
                return pickle.loads(data)
            except Exception as e:
                raise ValueError(f"无法反序列化数据: {e}")
    
    def start_auto_backup(self) -> None:
        """启动自动备份"""
        self._running = True
        
        # 启动数据源健康检查
        self.data_source_manager.start_health_check()
        
        # 启动备份处理线程
        backup_thread = threading.Thread(target=self._auto_backup_loop)
        backup_thread.daemon = True
        backup_thread.start()
    
    def stop_auto_backup(self) -> None:
        """停止自动备份"""
        self._running = False
        self.data_source_manager.stop_health_check()
        self.executor.shutdown(wait=True)
    
    def _auto_backup_loop(self) -> None:
        """自动备份循环"""
        while self._running:
            try:
                # 处理备份队列
                with self._lock:
                    pending_tasks = self.backup_queue.copy()
                
                for task in pending_tasks:
                    if task.status == BackupStatus.PENDING:
                        self._process_backup_task(task)
                
                # 清理过期备份
                self._cleanup_expired_backups()
                
                time.sleep(self.backup_policy.backup_interval)
                
            except Exception as e:
                self.logger.error(f"自动备份循环错误: {e}")
                time.sleep(10)
    
    def _cleanup_expired_backups(self) -> None:
        """清理过期备份"""
        try:
            cutoff_time = time.time() - self.backup_policy.max_backup_age
            
            for source_type in DataSourceType:
                if source_type == DataSourceType.BACKUP:
                    continue  # 不清理备份
                
                source_dir = self.backup_dir / source_type.value
                
                if not source_dir.exists():
                    continue
                
                for file_path in source_dir.glob("*.bak*"):
                    if file_path.stat().st_mtime < cutoff_time:
                        file_path.unlink()
                        self.logger.info(f"删除过期备份文件: {file_path}")
        
        except Exception as e:
            self.logger.error(f"清理过期备份失败: {e}")
    
    def get_backup_status(self) -> Dict[str, Any]:
        """获取备份状态"""
        with self._lock:
            pending_count = len([t for t in self.backup_queue if t.status == BackupStatus.PENDING])
            in_progress_count = len([t for t in self.backup_queue if t.status == BackupStatus.IN_PROGRESS])
            completed_count = len(self.completed_tasks)
            failed_count = len([t for t in self.completed_tasks if t.status == BackupStatus.FAILED])
            
            # 数据源状态
            sources_status = {}
            for name, config in self.data_source_manager.data_sources.items():
                sources_status[name] = {
                    "type": config.source_type.value,
                    "enabled": config.enabled,
                    "healthy": self.data_source_manager.source_health.get(name, True),
                    "priority": config.priority
                }
            
            return {
                "pending_tasks": pending_count,
                "in_progress_tasks": in_progress_count,
                "completed_tasks": completed_count,
                "failed_tasks": failed_count,
                "active_source": self.data_source_manager.active_source,
                "sources_status": sources_status,
                "auto_backup_enabled": self.backup_policy.auto_backup,
                "backup_interval": self.backup_policy.backup_interval
            }
    
    def list_backups(self, data_key: Optional[str] = None, source_type: Optional[DataSourceType] = None) -> List[Dict[str, Any]]:
        """列出备份文件"""
        backups = []
        
        # 从数据库查询备份记录
        conn = sqlite3.connect(str(self.backup_db))
        cursor = conn.cursor()
        
        query = "SELECT * FROM backup_tasks WHERE 1=1"
        params = []
        
        if data_key:
            query += " AND data_key = ?"
            params.append(data_key)
        
        if source_type:
            query += " AND source_type = ?"
            params.append(source_type.value)
        
        query += " ORDER BY timestamp DESC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # 获取列名
        columns = [description[0] for description in cursor.description]
        
        for row in rows:
            backup_info = dict(zip(columns, row))
            backups.append(backup_info)
        
        conn.close()
        return backups


# 全局备份系统实例
_backup_system = None


def get_backup_system(backup_dir: Optional[str] = None) -> MultiLevelBackupSystem:
    """获取全局备份系统实例"""
    global _backup_system
    if _backup_system is None:
        _backup_system = MultiLevelBackupSystem(backup_dir or "evolution_backups")
    return _backup_system