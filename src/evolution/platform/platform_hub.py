from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from src.evolution.alerts.alert_system import AlertChannel, AlertLevel, AlertMessage, alert_system
from src.evolution.backup.error_recovery import error_recovery_manager
from src.evolution.backup.multi_level_backup import DataSourceType, get_backup_system
from src.evolution.compliance.compliance_engine import compliance_engine
from src.evolution.deployment.deployment_pipeline import DeploymentConfig, DeploymentStatus, deployment_pipeline
from src.evolution.risk.optimized_risk_monitor import optimized_risk_monitoring_system
from src.evolution.risk.risk_monitor import risk_monitoring_system
from src.evolution.risk.risk_rating_system import risk_rating_system
from src.evolution.platform.schemas import (
    PlatformAlertView,
    PlatformDeploymentView,
    PlatformModuleStatus,
    PlatformOverview,
)


class EvolutionPlatformHub:
    def __init__(self, evolution_runtime=None):
        self.evolution_runtime = evolution_runtime
        self.backup_system = get_backup_system()
        self._services_started = False
        self._event_sink = None

    def set_event_sink(self, sink) -> None:
        self._event_sink = sink

    def start_services(self, auto_backup: bool = False) -> None:
        if self._services_started:
            return
        deployment_pipeline.add_deployment_callback(self._handle_deployment_update)
        self.backup_system.data_source_manager.start_health_check()
        if auto_backup:
            self.backup_system.start_auto_backup()
        self._services_started = True

    async def stop_services(self) -> None:
        if not self._services_started:
            return
        try:
            self.backup_system.stop_auto_backup()
        except Exception:
            try:
                self.backup_system.data_source_manager.stop_health_check()
            except Exception:
                pass
        try:
            await optimized_risk_monitoring_system.shutdown()
        except Exception:
            pass
        self._services_started = False

    def get_overview(self) -> Dict[str, Any]:
        risk_status = self.get_risk_overview()
        deployment_rows = self.get_deployments(limit=10)
        alert_stats = self.get_alert_stats()
        backup_status = self.get_backup_status()
        recovery_stats = self.get_recovery_stats()
        perf_stats = self.get_risk_performance()
        state = self.evolution_runtime.status() if self.evolution_runtime is not None else {}
        active_deployments = len([
            row for row in deployment_rows
            if str(row.get("status", "")) in {DeploymentStatus.PENDING.value, DeploymentStatus.RUNNING.value}
        ])
        avg_ms = None
        try:
            avg_text = str(((perf_stats.get("global_stats") or {}).get("avg_calculation_time") or "")).strip().lower().replace("s", "")
            if avg_text:
                avg_ms = float(avg_text) * 1000.0
        except Exception:
            avg_ms = None
        overview = PlatformOverview(
            evolution_running=bool(state.get("running", False)),
            evolution_iteration=int(state.get("iteration", 0) or 0),
            monitoring_strategies=int(risk_status.get("monitoring_strategies", 0) or 0),
            active_risk_alerts=int(risk_status.get("active_alerts", 0) or 0),
            active_deployments=active_deployments,
            alert_history_count=int(alert_stats.get("history_count", 0) or 0),
            backup_pending_tasks=int(backup_status.get("pending_tasks", 0) or 0),
            backup_failed_tasks=int(backup_status.get("failed_tasks", 0) or 0),
            active_backup_source=str(backup_status.get("active_source", "") or ""),
            recovery_error_count=int(recovery_stats.get("error_count", 0) or 0),
            recovery_circuit_breakers=len(recovery_stats.get("circuit_breakers", {}) or {}),
            avg_risk_calc_ms=avg_ms,
            extra={
                "backup_auto_enabled": bool(backup_status.get("auto_backup_enabled", False)),
                "alert_active_channels": list(alert_stats.get("active_channels", []) or []),
            },
        )
        modules = [
            PlatformModuleStatus(
                name="risk",
                healthy=True,
                summary=f"监控策略 {risk_status.get('monitoring_strategies', 0)} 个，活跃告警 {risk_status.get('active_alerts', 0)} 条",
                details=risk_status,
            ).to_dict(),
            PlatformModuleStatus(
                name="deployments",
                healthy=True,
                summary=f"最近部署 {len(deployment_rows)} 条，活跃 {active_deployments} 条",
                details={"rows": deployment_rows[:5]},
            ).to_dict(),
            PlatformModuleStatus(
                name="alerts",
                healthy=True,
                summary=f"累计告警 {alert_stats.get('history_count', 0)} 条，渠道 {', '.join(alert_stats.get('active_channels', [])) or '--'}",
                details=alert_stats,
            ).to_dict(),
            PlatformModuleStatus(
                name="backup",
                healthy=True,
                summary=f"待处理 {backup_status.get('pending_tasks', 0)}，失败 {backup_status.get('failed_tasks', 0)}",
                details=backup_status,
            ).to_dict(),
            PlatformModuleStatus(
                name="recovery",
                healthy=True,
                summary=f"错误记录 {recovery_stats.get('error_count', 0)}，熔断器 {len(recovery_stats.get('circuit_breakers', {}) or {})}",
                details=recovery_stats,
            ).to_dict(),
        ]
        return {
            "overview": overview.to_dict(),
            "modules": modules,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }

    def get_risk_overview(self) -> Dict[str, Any]:
        system_status = risk_monitoring_system.get_system_status()
        cross_summary = risk_monitoring_system.get_cross_strategy_risk_summary()
        return {
            "monitoring_strategies": int(system_status.get("monitoring_strategies", 0) or 0),
            "total_alerts": int(system_status.get("total_alerts", 0) or 0),
            "active_alerts": int(system_status.get("active_alerts", 0) or 0),
            "system_running": bool(system_status.get("system_running", False)),
            "cross_summary": cross_summary,
            "strategy_statuses": system_status.get("strategy_statuses", {}),
        }

    def get_strategy_risk(self, strategy_id: str) -> Dict[str, Any]:
        sid = str(strategy_id or "").strip()
        if not sid:
            return {"status": "error", "msg": "strategy_id required"}
        monitor = risk_monitoring_system.monitors.get(sid)
        if not monitor:
            return {"status": "not_monitored", "strategy_id": sid}
        payload = monitor.get_current_risk_status()
        payload["status"] = payload.get("status", "success")
        return payload

    def get_strategy_risk_history(self, strategy_id: str, hours: int = 24) -> Dict[str, Any]:
        sid = str(strategy_id or "").strip()
        if not sid:
            return {"status": "error", "msg": "strategy_id required"}
        monitor = risk_monitoring_system.monitors.get(sid)
        if not monitor:
            return {"status": "not_monitored", "strategy_id": sid, "timeline": [], "alerts": []}
        payload = monitor.get_risk_history(hours=max(1, int(hours or 24)))
        payload["status"] = "success"
        return payload

    def assess_strategy_risk(self, strategy_id: str, backtest_results: Optional[Dict[str, Any]] = None, strategy_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        report = risk_rating_system.assess_strategy_risk(
            str(strategy_id or "").strip(),
            backtest_results or {},
            strategy_data or {},
        )
        return {
            "strategy_id": report.strategy_id,
            "assessment_time": report.assessment_time.isoformat(),
            "overall_score": report.overall_score,
            "overall_level": report.overall_level.value,
            "recommendations": list(report.recommendations or []),
            "risk_factors": list(report.risk_factors or []),
            "mitigation_strategies": list(report.mitigation_strategies or []),
            "category_scores": [
                {
                    "category": item.category.value,
                    "score": item.score,
                    "level": item.level.value,
                    "weight": item.weight,
                    "description": item.description,
                    "details": dict(item.details or {}),
                }
                for item in list(report.category_scores or [])
            ],
        }

    def check_compliance(self, strategy_id: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        report = compliance_engine.check_strategy(str(strategy_id or "").strip(), data or {})
        return {
            "strategy_id": report.strategy_id,
            "check_time": report.check_time.isoformat(),
            "overall_level": report.overall_level.value,
            "passed_rules": report.passed_rules,
            "total_rules": report.total_rules,
            "summary": compliance_engine.get_compliance_summary(report),
            "recommendations": list(report.recommendations or []),
            "required_actions": list(report.required_actions or []),
            "results": [
                {
                    "rule_id": item.rule_id,
                    "rule_name": item.rule_name,
                    "category": item.category.value,
                    "level": item.level.value,
                    "passed": bool(item.passed),
                    "message": item.message,
                    "details": dict(item.details or {}),
                    "timestamp": item.timestamp.isoformat(),
                }
                for item in list(report.results or [])
            ],
        }

    def get_deployments(self, strategy_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        rows = deployment_pipeline.get_deployment_history(strategy_id=str(strategy_id or "").strip() or None, limit=max(1, min(int(limit or 20), 100)))
        return [self._serialize_deployment(item).to_dict() for item in rows]

    def get_deployment_detail(self, deployment_id: str) -> Dict[str, Any]:
        row = deployment_pipeline.get_deployment_status(str(deployment_id or "").strip())
        if row is None:
            return {"status": "error", "msg": "deployment not found"}
        payload = self._serialize_deployment(row).to_dict()
        payload["logs"] = list(row.logs or [])
        payload["artifacts"] = dict(row.artifacts or {})
        return payload

    def get_alerts(self, hours: int = 24, level: Optional[str] = None, strategy_id: Optional[str] = None) -> List[Dict[str, Any]]:
        level_enum = None
        if level:
            try:
                from src.evolution.alerts.alert_system import AlertLevel
                level_enum = AlertLevel(str(level).lower())
            except Exception:
                level_enum = None
        rows = alert_system.get_alert_history(hours=max(1, int(hours or 24)), level=level_enum, strategy_id=str(strategy_id or "").strip() or None)
        return [
            PlatformAlertView(
                title=str(item.title or ""),
                level=str(item.level.value if getattr(item, "level", None) else ""),
                strategy_id=str(item.strategy_id or ""),
                source=str(item.source or ""),
                timestamp=item.timestamp.isoformat() if getattr(item, "timestamp", None) else "",
                content=str(item.content or ""),
                metadata=dict(item.metadata or {}),
            ).to_dict()
            for item in rows
        ]

    def get_alert_stats(self) -> Dict[str, Any]:
        return alert_system.get_statistics()

    def get_backup_status(self) -> Dict[str, Any]:
        return self.backup_system.get_backup_status()

    def trigger_backup(self, data_key: str, data: Optional[Any] = None, source_type: Optional[str] = None) -> Dict[str, Any]:
        key = str(data_key or "").strip()
        if not key:
            return {"status": "error", "msg": "data_key required"}
        source_enum = self._parse_data_source_type(source_type) or DataSourceType.PRIMARY
        payload = data if data is not None else {
            "data_key": key,
            "triggered_at": datetime.now().isoformat(),
            "source": "platform_hub",
        }
        task_id = self.backup_system.backup_data(key, payload, source_type=source_enum)
        result = {
            "status": "success",
            "task_id": task_id,
            "data_key": key,
            "source_type": source_enum.value,
            "backup_status": self.get_backup_status(),
        }
        self._emit_platform_event("platform_backup_status", result)
        return result

    def restore_backup(self, data_key: str, source_type: Optional[str] = None) -> Dict[str, Any]:
        key = str(data_key or "").strip()
        if not key:
            return {"status": "error", "msg": "data_key required"}
        source_enum = self._parse_data_source_type(source_type)
        preferred_sources = [source_enum] if source_enum is not None else []
        preferred_sources.extend([
            DataSourceType.PRIMARY,
            DataSourceType.SECONDARY,
            DataSourceType.CACHE,
            DataSourceType.REMOTE,
            DataSourceType.BACKUP,
        ])
        seen = set()
        resolved_data = None
        resolved_source = None
        for candidate in preferred_sources:
            if candidate is None or candidate in seen:
                continue
            seen.add(candidate)
            try:
                resolved_data = self.backup_system.restore_data(key, source_type=candidate)
            except Exception:
                resolved_data = None
            if resolved_data is not None:
                resolved_source = candidate
                break
        if resolved_data is None:
            return {
                "status": "error",
                "msg": "backup not found",
                "data_key": key,
                "source_type": source_enum.value if source_enum else None,
            }
        result = {
            "status": "success",
            "data_key": key,
            "source_type": resolved_source.value if resolved_source else None,
            "data": resolved_data,
        }
        self._emit_platform_event("platform_backup_status", result)
        return result

    def list_backups(self, data_key: Optional[str] = None, source_type: Optional[str] = None) -> List[Dict[str, Any]]:
        source_enum = None
        if source_type:
            try:
                source_enum = DataSourceType(str(source_type).lower())
            except Exception:
                source_enum = None
        return self.backup_system.list_backups(data_key=str(data_key or "").strip() or None, source_type=source_enum)

    def get_recovery_stats(self) -> Dict[str, Any]:
        recent_errors = [
            {
                "error_type": item.error_type.value,
                "error_message": item.error_message,
                "timestamp": item.timestamp.isoformat(),
                "retry_count": item.retry_count,
                "source": item.source,
                "operation": item.operation,
            }
            for item in list(error_recovery_manager.error_history or [])[-20:]
        ]
        return {
            "error_count": len(list(error_recovery_manager.error_history or [])),
            "recovery_stats": dict(error_recovery_manager.recovery_stats or {}),
            "circuit_breakers": {
                name: {
                    "state": cb.state,
                    "failure_count": cb.failure_count,
                    "last_failure_time": cb.last_failure_time.isoformat() if cb.last_failure_time else None,
                    "failure_threshold": cb.failure_threshold,
                }
                for name, cb in dict(error_recovery_manager.circuit_breakers or {}).items()
            },
            "recent_errors": recent_errors,
        }

    def reset_circuit_breaker(self, name: str) -> Dict[str, Any]:
        breaker_name = str(name or "").strip()
        if not breaker_name:
            return {"status": "error", "msg": "name required"}
        manager = getattr(self.backup_system, "data_source_manager", None)
        reset_source = False
        if manager is not None:
            reset_fn = getattr(manager, "reset_circuit_breaker", None)
            if callable(reset_fn):
                try:
                    reset_source = bool(reset_fn(breaker_name))
                except Exception:
                    reset_source = False
            elif hasattr(manager, "source_health") and breaker_name in dict(getattr(manager, "source_health", {}) or {}):
                manager.source_health[breaker_name] = True
                active_source = getattr(manager, "active_source", None)
                if not active_source:
                    manager.active_source = breaker_name
                reset_source = True
        breaker = dict(error_recovery_manager.circuit_breakers or {}).get(breaker_name)
        reset_recovery = False
        if breaker is not None:
            breaker.reset()
            reset_recovery = True
        if not reset_source and not reset_recovery:
            return {"status": "error", "msg": "circuit breaker not found", "name": breaker_name}
        result = {
            "status": "success",
            "name": breaker_name,
            "reset_source_breaker": bool(reset_source),
            "reset_recovery_breaker": bool(reset_recovery),
        }
        self._emit_platform_event("platform_backup_status", result)
        return result

    def _emit_platform_event(self, event_type: str, data: Dict[str, Any]) -> None:
        if not callable(self._event_sink):
            return
        try:
            self._event_sink({
                "type": str(event_type or "platform_event"),
                "data": dict(data or {}),
                "server_time": datetime.now().isoformat(timespec="seconds"),
            })
        except Exception:
            pass

    def _handle_deployment_update(self, result) -> None:
        try:
            payload = self.get_deployment_detail(str(getattr(result, "deployment_id", "") or ""))
        except Exception:
            payload = {
                "deployment_id": str(getattr(result, "deployment_id", "") or ""),
                "status": str(getattr(getattr(result, "status", None), "value", "") or ""),
                "strategy_id": str(getattr(result, "strategy_id", "") or ""),
                "version": str(getattr(result, "version", "") or ""),
            }
        self._emit_platform_event("platform_deployment_update", payload)

    def get_risk_performance(self) -> Dict[str, Any]:
        return optimized_risk_monitoring_system.get_system_performance_stats()

    def start_deployment(
        self,
        strategy_id: str,
        version: Optional[str] = None,
        environment: str = "production",
        strategy_data: Optional[Dict[str, Any]] = None,
        auto_deploy: bool = True,
        require_approval: bool = False,
        rollback_enabled: bool = True,
    ) -> Dict[str, Any]:
        sid = str(strategy_id or "").strip()
        if not sid:
            return {"status": "error", "msg": "strategy_id required"}
        resolved_version = str(version or "").strip() or datetime.now().strftime("%Y%m%d%H%M%S")
        config = DeploymentConfig(
            strategy_id=sid,
            version=resolved_version,
            environment=str(environment or "production").strip() or "production",
            auto_deploy=bool(auto_deploy),
            require_approval=bool(require_approval),
            rollback_enabled=bool(rollback_enabled),
        )
        payload = dict(strategy_data or {})
        payload.setdefault("strategy_id", sid)
        deployment_id = deployment_pipeline.start_deployment(config, payload)
        detail = self.get_deployment_detail(deployment_id)
        result = {"status": "success", "deployment_id": deployment_id, "data": detail}
        self._emit_platform_event("platform_deployment_update", result)
        return result

    def cancel_deployment(self, deployment_id: str) -> Dict[str, Any]:
        did = str(deployment_id or "").strip()
        if not did:
            return {"status": "error", "msg": "deployment_id required"}
        cancelled = deployment_pipeline.cancel_deployment(did)
        if not cancelled:
            return {"status": "error", "msg": "deployment not cancellable", "deployment_id": did}
        result = {"status": "success", "deployment_id": did, "data": self.get_deployment_detail(did)}
        self._emit_platform_event("platform_deployment_update", result)
        return result

    async def send_test_alert(
        self,
        title: str,
        content: str,
        level: str = "warning",
        strategy_id: Optional[str] = None,
        channels: Optional[List[str]] = None,
        source: str = "platform_manual",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        title_text = str(title or "").strip()
        content_text = str(content or "").strip()
        if not title_text:
            return {"status": "error", "msg": "title required"}
        if not content_text:
            return {"status": "error", "msg": "content required"}
        level_enum = self._parse_alert_level(level) or AlertLevel.WARNING
        channel_enums = self._parse_alert_channels(channels)
        message = AlertMessage(
            alert_id=uuid4().hex,
            title=title_text,
            content=content_text,
            level=level_enum,
            strategy_id=str(strategy_id or "").strip(),
            timestamp=datetime.now(),
            source=str(source or "platform_manual").strip() or "platform_manual",
            metadata=dict(metadata or {}),
        )
        results = await alert_system.send_alert(message, channels=channel_enums or None)
        result = {
            "status": "success",
            "results": results,
            "message": message.to_dict(),
        }
        self._emit_platform_event("platform_alert_sent", result)
        return result

    async def set_alert_channel_state(self, channel: str, enabled: bool) -> Dict[str, Any]:
        channel_enum = self._parse_alert_channel(channel)
        if channel_enum is None:
            return {"status": "error", "msg": "invalid channel", "channel": str(channel or "")}
        if enabled:
            alert_system.enable_channel(channel_enum)
        else:
            alert_system.disable_channel(channel_enum)
        test_ok = await alert_system.test_channel(channel_enum)
        result = {
            "status": "success",
            "channel": channel_enum.value,
            "enabled": bool(enabled),
            "test_ok": bool(test_ok),
            "stats": self.get_alert_stats(),
        }
        self._emit_platform_event("platform_alert_sent", result)
        return result

    def _parse_data_source_type(self, value: Optional[str]) -> Optional[DataSourceType]:
        text = str(value or "").strip().lower()
        if not text:
            return None
        try:
            return DataSourceType(text)
        except Exception:
            return None

    def _parse_alert_level(self, value: Optional[str]) -> Optional[AlertLevel]:
        text = str(value or "").strip().lower()
        if not text:
            return None
        try:
            return AlertLevel(text)
        except Exception:
            return None

    def _parse_alert_channel(self, value: Optional[str]) -> Optional[AlertChannel]:
        text = str(value or "").strip().lower()
        if not text:
            return None
        try:
            return AlertChannel(text)
        except Exception:
            return None

    def _parse_alert_channels(self, values: Optional[List[str]]) -> List[AlertChannel]:
        resolved: List[AlertChannel] = []
        for item in list(values or []):
            channel = self._parse_alert_channel(item)
            if channel is not None:
                resolved.append(channel)
        return resolved

    def _serialize_deployment(self, row) -> PlatformDeploymentView:
        return PlatformDeploymentView(
            deployment_id=str(row.deployment_id or ""),
            strategy_id=str(row.strategy_id or ""),
            version=str(row.version or ""),
            stage=str(row.stage.value if getattr(row, "stage", None) else ""),
            status=str(row.status.value if getattr(row, "status", None) else ""),
            start_time=row.start_time.isoformat() if getattr(row, "start_time", None) else "",
            end_time=row.end_time.isoformat() if getattr(row, "end_time", None) else "",
            duration=float(row.duration or 0.0),
            steps_completed=int(row.steps_completed or 0),
            total_steps=int(row.total_steps or 0),
            error_message=str(row.error_message or ""),
        )
