"""
告警集成系统
支持邮件、短信、飞书、钉钉等多种告警通知机制
"""
import smtplib
import requests
import json
import time
import threading
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from abc import ABC, abstractmethod
import hashlib
import hmac


class AlertLevel(Enum):
    """告警等级"""
    INFO = "info"           # 信息
    WARNING = "warning"     # 警告
    ERROR = "error"         # 错误
    CRITICAL = "critical"   # 严重
    EMERGENCY = "emergency" # 紧急


class AlertChannel(Enum):
    """告警渠道"""
    EMAIL = "email"         # 邮件
    SMS = "sms"            # 短信
    FEISHU = "feishu"      # 飞书
    DINGTALK = "dingtalk"  # 钉钉
    WEBHOOK = "webhook"    # Webhook
    SLACK = "slack"        # Slack


@dataclass
class AlertMessage:
    """告警消息"""
    alert_id: str
    title: str
    content: str
    level: AlertLevel
    strategy_id: str
    timestamp: datetime
    source: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "alert_id": self.alert_id,
            "title": self.title,
            "content": self.content,
            "level": self.level.value,
            "strategy_id": self.strategy_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "tags": self.tags,
            "metadata": self.metadata
        }


@dataclass
class AlertChannelConfig:
    """告警渠道配置"""
    channel: AlertChannel
    enabled: bool = True
    level_threshold: AlertLevel = AlertLevel.WARNING  # 最低告警等级
    rate_limit: int = 5  # 每分钟最大发送数
    retry_count: int = 3
    timeout: int = 30
    config: Dict[str, Any] = field(default_factory=dict)


class AlertNotifier(ABC):
    """告警通知器基类"""
    
    @abstractmethod
    async def send(self, message: AlertMessage, config: AlertChannelConfig) -> bool:
        """发送告警消息"""
        pass
    
    @abstractmethod
    def test_connection(self, config: AlertChannelConfig) -> bool:
        """测试连接"""
        pass


class EmailNotifier(AlertNotifier):
    """邮件通知器"""
    
    async def send(self, message: AlertMessage, config: AlertChannelConfig) -> bool:
        """发送邮件告警"""
        try:
            smtp_config = config.config
            smtp_server = smtp_config.get("smtp_server", "smtp.gmail.com")
            smtp_port = smtp_config.get("smtp_port", 587)
            username = smtp_config.get("username", "")
            password = smtp_config.get("password", "")
            recipients = smtp_config.get("recipients", [])
            
            if not username or not password or not recipients:
                logging.error("邮件配置不完整")
                return False
            
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = Header(f"[{message.level.value.upper()}] {message.title}", 'utf-8')
            
            # 邮件正文
            body = self._generate_email_body(message)
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            # 发送邮件
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
            
            logging.info(f"邮件告警发送成功: {message.alert_id}")
            return True
            
        except Exception as e:
            logging.error(f"邮件告警发送失败: {e}")
            return False
    
    def test_connection(self, config: AlertChannelConfig) -> bool:
        """测试邮件连接"""
        try:
            smtp_config = config.config
            smtp_server = smtp_config.get("smtp_server", "smtp.gmail.com")
            smtp_port = smtp_config.get("smtp_port", 587)
            username = smtp_config.get("username", "")
            password = smtp_config.get("password", "")
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
            
            return True
            
        except Exception as e:
            logging.error(f"邮件连接测试失败: {e}")
            return False
    
    def _generate_email_body(self, message: AlertMessage) -> str:
        """生成邮件正文"""
        level_colors = {
            AlertLevel.INFO: "#17a2b8",
            AlertLevel.WARNING: "#ffc107", 
            AlertLevel.ERROR: "#dc3545",
            AlertLevel.CRITICAL: "#fd7e14",
            AlertLevel.EMERGENCY: "#6f42c1"
        }
        
        color = level_colors.get(message.level, "#6c757d")
        
        return f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: {color}; color: white; padding: 15px; border-radius: 5px; }}
                .content {{ margin: 20px 0; }}
                .footer {{ color: #666; font-size: 12px; margin-top: 30px; }}
                .metadata {{ background: #f8f9fa; padding: 10px; border-radius: 5px; margin: 10px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚨 {message.level.value.upper()} 告警</h2>
                <p>{message.title}</p>
            </div>
            
            <div class="content">
                <h3>告警详情</h3>
                <p>{message.content}</p>
                
                <div class="metadata">
                    <strong>策略ID:</strong> {message.strategy_id}<br>
                    <strong>时间:</strong> {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}<br>
                    <strong>来源:</strong> {message.source}<br>
                    <strong>标签:</strong> {', '.join(message.tags) if message.tags else '无'}
                </div>
            </div>
            
            <div class="footer">
                <p>此邮件由金策智算系统自动发送，请勿回复。</p>
                <p>如需停止接收此类邮件，请联系系统管理员。</p>
            </div>
        </body>
        </html>
        """


class FeishuNotifier(AlertNotifier):
    """飞书通知器"""
    
    async def send(self, message: AlertMessage, config: AlertChannelConfig) -> bool:
        """发送飞书告警"""
        try:
            webhook_url = config.config.get("webhook_url", "")
            if not webhook_url:
                logging.error("飞书Webhook URL未配置")
                return False
            
            # 构建飞书消息
            payload = self._build_feishu_message(message)
            
            # 发送请求
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=config.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 0:
                    logging.info(f"飞书告警发送成功: {message.alert_id}")
                    return True
                else:
                    logging.error(f"飞书告警发送失败: {result.get('msg')}")
                    return False
            else:
                logging.error(f"飞书告警发送失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"飞书告警发送异常: {e}")
            return False
    
    def test_connection(self, config: AlertChannelConfig) -> bool:
        """测试飞书连接"""
        try:
            webhook_url = config.config.get("webhook_url", "")
            if not webhook_url:
                return False
            
            test_payload = {
                "msg_type": "text",
                "content": {
                    "text": "🧪 金策智算告警系统连接测试"
                }
            }
            
            response = requests.post(webhook_url, json=test_payload, timeout=10)
            return response.status_code == 200
            
        except Exception as e:
            logging.error(f"飞书连接测试失败: {e}")
            return False
    
    def _build_feishu_message(self, message: AlertMessage) -> Dict[str, Any]:
        """构建飞书消息"""
        level_emojis = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🔥",
            AlertLevel.EMERGENCY: "🚨"
        }
        
        emoji = level_emojis.get(message.level, "📢")
        
        # 构建富文本消息
        content = [
            {
                "tag": "text",
                "text": f"{emoji} **{message.title}**\n\n"
            },
            {
                "tag": "text",
                "text": f"{message.content}\n\n"
            },
            {
                "tag": "text",
                "text": f"📊 **策略ID**: {message.strategy_id}\n"
            },
            {
                "tag": "text", 
                "text": f"⏰ **时间**: {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
            },
            {
                "tag": "text",
                "text": f"🏷️ **等级**: {message.level.value.upper()}\n"
            }
        ]
        
        if message.source:
            content.append({
                "tag": "text",
                "text": f"📍 **来源**: {message.source}\n"
            })
        
        if message.tags:
            content.append({
                "tag": "text",
                "text": f"🏷️ **标签**: {', '.join(message.tags)}\n"
            })
        
        return {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f"{emoji} 金策智算告警通知",
                        "content": [
                            content
                        ]
                    }
                }
            }
        }


class DingTalkNotifier(AlertNotifier):
    """钉钉通知器"""
    
    async def send(self, message: AlertMessage, config: AlertChannelConfig) -> bool:
        """发送钉钉告警"""
        try:
            webhook_url = config.config.get("webhook_url", "")
            secret = config.config.get("secret", "")
            
            if not webhook_url:
                logging.error("钉钉Webhook URL未配置")
                return False
            
            # 添加签名
            if secret:
                webhook_url = self._add_signature(webhook_url, secret)
            
            # 构建钉钉消息
            payload = self._build_dingtalk_message(message)
            
            # 发送请求
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=config.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    logging.info(f"钉钉告警发送成功: {message.alert_id}")
                    return True
                else:
                    logging.error(f"钉钉告警发送失败: {result.get('errmsg')}")
                    return False
            else:
                logging.error(f"钉钉告警发送失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"钉钉告警发送异常: {e}")
            return False
    
    def test_connection(self, config: AlertChannelConfig) -> bool:
        """测试钉钉连接"""
        try:
            webhook_url = config.config.get("webhook_url", "")
            if not webhook_url:
                return False
            
            test_payload = {
                "msgtype": "text",
                "text": {
                    "content": "🧪 金策智算告警系统连接测试"
                }
            }
            
            response = requests.post(webhook_url, json=test_payload, timeout=10)
            return response.status_code == 200
            
        except Exception as e:
            logging.error(f"钉钉连接测试失败: {e}")
            return False
    
    def _add_signature(self, webhook_url: str, secret: str) -> str:
        """添加钉钉签名"""
        import urllib.parse
        
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = f'{timestamp}\\n{secret}'
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        
        return f"{webhook_url}&timestamp={timestamp}&sign={sign}"
    
    def _build_dingtalk_message(self, message: AlertMessage) -> Dict[str, Any]:
        """构建钉钉消息"""
        level_colors = {
            AlertLevel.INFO: "#0084ff",
            AlertLevel.WARNING: "#ff8c00",
            AlertLevel.ERROR: "#ff0000",
            AlertLevel.CRITICAL: "#ff4500",
            AlertLevel.EMERGENCY: "#8b0000"
        }
        
        color = level_colors.get(message.level, "#666666")
        
        # 构建Markdown消息
        markdown_text = f"""
### {message.level.value.upper()} 告警

**标题**: {message.title}

**详情**: {message.content}

---
**策略ID**: {message.strategy_id}  
**时间**: {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}  
**等级**: {message.level.value.upper()}  
**来源**: {message.source or '未知'}
        """.strip()
        
        return {
            "msgtype": "markdown",
            "markdown": {
                "title": f"金策智算告警 - {message.title}",
                "text": markdown_text
            }
        }


class WebhookNotifier(AlertNotifier):
    """Webhook通知器"""
    
    async def send(self, message: AlertMessage, config: AlertChannelConfig) -> bool:
        """发送Webhook告警"""
        try:
            webhook_url = config.config.get("url", "")
            headers = config.config.get("headers", {})
            method = config.config.get("method", "POST").upper()
            
            if not webhook_url:
                logging.error("Webhook URL未配置")
                return False
            
            # 构建请求载荷
            payload = {
                "alert": message.to_dict(),
                "timestamp": datetime.now().isoformat()
            }
            
            # 添加自定义字段
            custom_fields = config.config.get("custom_fields", {})
            payload.update(custom_fields)
            
            # 发送请求
            if method == "GET":
                response = requests.get(
                    webhook_url,
                    params=payload,
                    headers=headers,
                    timeout=config.timeout
                )
            else:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers=headers,
                    timeout=config.timeout
                )
            
            if response.status_code in [200, 201, 204]:
                logging.info(f"Webhook告警发送成功: {message.alert_id}")
                return True
            else:
                logging.error(f"Webhook告警发送失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Webhook告警发送异常: {e}")
            return False
    
    def test_connection(self, config: AlertChannelConfig) -> bool:
        """测试Webhook连接"""
        try:
            webhook_url = config.config.get("url", "")
            if not webhook_url:
                return False
            
            test_payload = {
                "test": True,
                "message": "金策智算告警系统连接测试",
                "timestamp": datetime.now().isoformat()
            }
            
            method = config.config.get("method", "POST").upper()
            headers = config.config.get("headers", {})
            
            if method == "GET":
                response = requests.get(webhook_url, params=test_payload, headers=headers, timeout=10)
            else:
                response = requests.post(webhook_url, json=test_payload, headers=headers, timeout=10)
            
            return response.status_code in [200, 201, 204]
            
        except Exception as e:
            logging.error(f"Webhook连接测试失败: {e}")
            return False


class AlertRateLimiter:
    """告警频率限制器"""
    
    def __init__(self):
        self.limits: Dict[str, List[datetime]] = {}
        self._lock = threading.Lock()
    
    def is_allowed(self, channel: str, rate_limit: int) -> bool:
        """检查是否允许发送"""
        with self._lock:
            now = datetime.now()
            
            if channel not in self.limits:
                self.limits[channel] = []
            
            # 清理1分钟前的记录
            cutoff = now - timedelta(minutes=1)
            self.limits[channel] = [
                timestamp for timestamp in self.limits[channel]
                if timestamp > cutoff
            ]
            
            # 检查是否超过限制
            if len(self.limits[channel]) >= rate_limit:
                return False
            
            # 记录本次发送
            self.limits[channel].append(now)
            return True


class AlertSystem:
    """告警系统"""
    
    def __init__(self):
        self.notifiers: Dict[AlertChannel, AlertNotifier] = {
            AlertChannel.EMAIL: EmailNotifier(),
            AlertChannel.FEISHU: FeishuNotifier(),
            AlertChannel.DINGTALK: DingTalkNotifier(),
            AlertChannel.WEBHOOK: WebhookNotifier()
        }
        
        self.channel_configs: Dict[AlertChannel, AlertChannelConfig] = {}
        self.rate_limiter = AlertRateLimiter()
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="alert")
        
        # 告警历史
        self.alert_history: List[AlertMessage] = []
        self.max_history = 1000
        
        # 统计信息
        self.stats = {
            "total_sent": 0,
            "total_failed": 0,
            "by_channel": {},
            "by_level": {}
        }
        
        # 加载配置
        self._load_config()
    
    def _load_config(self):
        """加载配置"""
        from src.utils.config_loader import ConfigLoader
        
        try:
            cfg = ConfigLoader.reload()
            alert_config = cfg.get("evolution.alerts", {})
            
            # 邮件配置
            if "email" in alert_config:
                self.channel_configs[AlertChannel.EMAIL] = AlertChannelConfig(
                    channel=AlertChannel.EMAIL,
                    enabled=alert_config["email"].get("enabled", False),
                    level_threshold=AlertLevel(alert_config["email"].get("level_threshold", "warning")),
                    rate_limit=alert_config["email"].get("rate_limit", 5),
                    config=alert_config["email"]
                )
            
            # 飞书配置
            if "feishu" in alert_config:
                self.channel_configs[AlertChannel.FEISHU] = AlertChannelConfig(
                    channel=AlertChannel.FEISHU,
                    enabled=alert_config["feishu"].get("enabled", False),
                    level_threshold=AlertLevel(alert_config["feishu"].get("level_threshold", "warning")),
                    rate_limit=alert_config["feishu"].get("rate_limit", 10),
                    config=alert_config["feishu"]
                )
            
            # 钉钉配置
            if "dingtalk" in alert_config:
                self.channel_configs[AlertChannel.DINGTALK] = AlertChannelConfig(
                    channel=AlertChannel.DINGTALK,
                    enabled=alert_config["dingtalk"].get("enabled", False),
                    level_threshold=AlertLevel(alert_config["dingtalk"].get("level_threshold", "warning")),
                    rate_limit=alert_config["dingtalk"].get("rate_limit", 10),
                    config=alert_config["dingtalk"]
                )
            
            # Webhook配置
            if "webhook" in alert_config:
                self.channel_configs[AlertChannel.WEBHOOK] = AlertChannelConfig(
                    channel=AlertChannel.WEBHOOK,
                    enabled=alert_config["webhook"].get("enabled", False),
                    level_threshold=AlertLevel(alert_config["webhook"].get("level_threshold", "warning")),
                    rate_limit=alert_config["webhook"].get("rate_limit", 20),
                    config=alert_config["webhook"]
                )
            
        except Exception as e:
            logging.error(f"加载告警配置失败: {e}")
    
    async def send_alert(self, message: AlertMessage, 
                        channels: Optional[List[AlertChannel]] = None) -> Dict[str, bool]:
        """发送告警"""
        if channels is None:
            channels = list(self.channel_configs.keys())
        
        # 记录告警历史
        self._add_to_history(message)
        
        # 更新统计
        self._update_stats(message.level, "sent")
        
        results = {}
        
        # 并发发送到各个渠道
        tasks = []
        for channel in channels:
            config = self.channel_configs.get(channel)
            if not config or not config.enabled:
                continue
            
            # 检查告警等级阈值
            if not self._check_level_threshold(message.level, config.level_threshold):
                continue
            
            # 检查频率限制
            if not self.rate_limiter.is_allowed(channel.value, config.rate_limit):
                logging.warning(f"告警频率超限: {channel.value}")
                results[channel.value] = False
                continue
            
            notifier = self.notifiers.get(channel)
            if notifier:
                task = asyncio.create_task(
                    self._send_with_retry(notifier, message, config)
                )
                tasks.append((channel, task))
        
        # 等待所有任务完成
        for channel, task in tasks:
            try:
                success = await task
                results[channel.value] = success
                
                if not success:
                    self._update_stats(message.level, "failed")
                    
            except Exception as e:
                logging.error(f"告警发送异常 {channel.value}: {e}")
                results[channel.value] = False
                self._update_stats(message.level, "failed")
        
        return results
    
    async def _send_with_retry(self, notifier: AlertNotifier, 
                              message: AlertMessage, 
                              config: AlertChannelConfig) -> bool:
        """带重试的发送"""
        for attempt in range(config.retry_count):
            try:
                success = await notifier.send(message, config)
                if success:
                    return True
                
                # 如果失败，等待后重试
                if attempt < config.retry_count - 1:
                    await asyncio.sleep(2 ** attempt)  # 指数退避
                    
            except Exception as e:
                logging.error(f"告警发送尝试 {attempt + 1} 失败: {e}")
                if attempt < config.retry_count - 1:
                    await asyncio.sleep(2 ** attempt)
        
        return False
    
    def _check_level_threshold(self, message_level: AlertLevel, 
                              threshold: AlertLevel) -> bool:
        """检查告警等级阈值"""
        level_order = {
            AlertLevel.INFO: 0,
            AlertLevel.WARNING: 1,
            AlertLevel.ERROR: 2,
            AlertLevel.CRITICAL: 3,
            AlertLevel.EMERGENCY: 4
        }
        
        return level_order[message_level] >= level_order[threshold]
    
    def _add_to_history(self, message: AlertMessage):
        """添加到历史记录"""
        self.alert_history.append(message)
        
        # 限制历史记录数量
        if len(self.alert_history) > self.max_history:
            self.alert_history = self.alert_history[-self.max_history:]
    
    def _update_stats(self, level: AlertLevel, status: str):
        """更新统计信息"""
        if status == "sent":
            self.stats["total_sent"] += 1
        else:
            self.stats["total_failed"] += 1
        
        # 按等级统计
        level_name = level.value
        if level_name not in self.stats["by_level"]:
            self.stats["by_level"][level_name] = {"sent": 0, "failed": 0}
        
        self.stats["by_level"][level_name][status] += 1
    
    async def test_channel(self, channel: AlertChannel) -> bool:
        """测试渠道连接"""
        config = self.channel_configs.get(channel)
        if not config:
            return False
        
        notifier = self.notifiers.get(channel)
        if not notifier:
            return False
        
        return await asyncio.get_event_loop().run_in_executor(
            self.executor, notifier.test_connection, config
        )
    
    def get_alert_history(self, hours: int = 24, 
                         level: Optional[AlertLevel] = None,
                         strategy_id: Optional[str] = None) -> List[AlertMessage]:
        """获取告警历史"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        filtered_history = []
        for alert in self.alert_history:
            if alert.timestamp < cutoff_time:
                continue
            
            if level and alert.level != level:
                continue
            
            if strategy_id and alert.strategy_id != strategy_id:
                continue
            
            filtered_history.append(alert)
        
        # 按时间倒序排列
        filtered_history.sort(key=lambda x: x.timestamp, reverse=True)
        
        return filtered_history
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_sent": self.stats["total_sent"],
            "total_failed": self.stats["total_failed"],
            "success_rate": (
                self.stats["total_sent"] / 
                max(1, self.stats["total_sent"] + self.stats["total_failed"]) * 100
            ),
            "by_level": self.stats["by_level"],
            "by_channel": self.stats["by_channel"],
            "active_channels": [
                channel.value for channel, config in self.channel_configs.items()
                if config.enabled
            ],
            "history_count": len(self.alert_history)
        }
    
    def update_channel_config(self, channel: AlertChannel, config: AlertChannelConfig):
        """更新渠道配置"""
        self.channel_configs[channel] = config
        logging.info(f"更新告警渠道配置: {channel.value}")
    
    def enable_channel(self, channel: AlertChannel):
        """启用渠道"""
        if channel in self.channel_configs:
            self.channel_configs[channel].enabled = True
            logging.info(f"启用告警渠道: {channel.value}")
    
    def disable_channel(self, channel: AlertChannel):
        """禁用渠道"""
        if channel in self.channel_configs:
            self.channel_configs[channel].enabled = False
            logging.info(f"禁用告警渠道: {channel.value}")


# 全局告警系统实例
alert_system = AlertSystem()