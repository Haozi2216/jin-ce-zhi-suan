from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PlatformOverview:
    evolution_running: bool
    evolution_iteration: int
    monitoring_strategies: int
    active_risk_alerts: int
    active_deployments: int
    alert_history_count: int
    backup_pending_tasks: int
    backup_failed_tasks: int
    active_backup_source: str
    recovery_error_count: int
    recovery_circuit_breakers: int
    avg_risk_calc_ms: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evolution_running": bool(self.evolution_running),
            "evolution_iteration": int(self.evolution_iteration),
            "monitoring_strategies": int(self.monitoring_strategies),
            "active_risk_alerts": int(self.active_risk_alerts),
            "active_deployments": int(self.active_deployments),
            "alert_history_count": int(self.alert_history_count),
            "backup_pending_tasks": int(self.backup_pending_tasks),
            "backup_failed_tasks": int(self.backup_failed_tasks),
            "active_backup_source": str(self.active_backup_source or ""),
            "recovery_error_count": int(self.recovery_error_count),
            "recovery_circuit_breakers": int(self.recovery_circuit_breakers),
            "avg_risk_calc_ms": self.avg_risk_calc_ms,
            "extra": dict(self.extra or {}),
        }


@dataclass
class PlatformModuleStatus:
    name: str
    healthy: bool
    summary: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": str(self.name or ""),
            "healthy": bool(self.healthy),
            "summary": str(self.summary or ""),
            "details": dict(self.details or {}),
        }


@dataclass
class PlatformAlertView:
    title: str
    level: str
    strategy_id: str
    source: str
    timestamp: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": str(self.title or ""),
            "level": str(self.level or ""),
            "strategy_id": str(self.strategy_id or ""),
            "source": str(self.source or ""),
            "timestamp": str(self.timestamp or ""),
            "content": str(self.content or ""),
            "metadata": dict(self.metadata or {}),
        }


@dataclass
class PlatformDeploymentView:
    deployment_id: str
    strategy_id: str
    version: str
    stage: str
    status: str
    start_time: str
    end_time: str
    duration: float
    steps_completed: int
    total_steps: int
    error_message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "deployment_id": str(self.deployment_id or ""),
            "strategy_id": str(self.strategy_id or ""),
            "version": str(self.version or ""),
            "stage": str(self.stage or ""),
            "status": str(self.status or ""),
            "start_time": str(self.start_time or ""),
            "end_time": str(self.end_time or ""),
            "duration": float(self.duration or 0.0),
            "steps_completed": int(self.steps_completed or 0),
            "total_steps": int(self.total_steps or 0),
            "error_message": str(self.error_message or ""),
        }
