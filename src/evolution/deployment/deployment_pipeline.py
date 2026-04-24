"""
策略自动部署管道
实现策略从开发到生产环境的自动化部署流程
"""
import os
import ast
import json
import shutil
import hashlib
import subprocess
import threading
import time
import logging
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import zipfile


class DeploymentStage(Enum):
    """部署阶段"""
    VALIDATION = "validation"       # 验证阶段
    TESTING = "testing"            # 测试阶段
    STAGING = "staging"           # 预发布阶段
    PRODUCTION = "production"     # 生产阶段
    ROLLBACK = "rollback"         # 回滚阶段


class DeploymentStatus(Enum):
    """部署状态"""
    PENDING = "pending"           # 等待中
    RUNNING = "running"           # 运行中
    SUCCESS = "success"           # 成功
    FAILED = "failed"            # 失败
    CANCELLED = "cancelled"       # 已取消
    ROLLED_BACK = "rolled_back"   # 已回滚


class ValidationType(Enum):
    """验证类型"""
    CODE_QUALITY = "code_quality"       # 代码质量
    COMPLIANCE = "compliance"           # 合规检查
    RISK_ASSESSMENT = "risk_assessment" # 风险评估
    PERFORMANCE = "performance"         # 性能测试
    BACKTEST = "backtest"              # 回测验证


@dataclass
class DeploymentConfig:
    """部署配置"""
    strategy_id: str
    version: str
    environment: str
    auto_deploy: bool = True
    require_approval: bool = True
    rollback_enabled: bool = True
    health_check_enabled: bool = True
    monitoring_enabled: bool = True
    timeout_seconds: int = 1800  # 30分钟超时
    retry_count: int = 3
    notifications: List[str] = field(default_factory=list)
    custom_validations: List[str] = field(default_factory=list)


@dataclass
class DeploymentStep:
    """部署步骤"""
    step_id: str
    name: str
    stage: DeploymentStage
    order: int
    command: str
    timeout: int = 300
    required: bool = True
    rollback_command: Optional[str] = None
    conditions: List[str] = field(default_factory=list)


@dataclass
class DeploymentResult:
    """部署结果"""
    deployment_id: str
    strategy_id: str
    version: str
    stage: DeploymentStage
    status: DeploymentStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: float = 0.0
    steps_completed: int = 0
    total_steps: int = 0
    error_message: str = ""
    logs: List[str] = field(default_factory=list)
    artifacts: Dict[str, str] = field(default_factory=dict)
    rollback_info: Optional[Dict[str, Any]] = None


class DeploymentValidator:
    """部署验证器"""
    
    def __init__(self):
        self.validators = {
            ValidationType.CODE_QUALITY: self._validate_code_quality,
            ValidationType.COMPLIANCE: self._validate_compliance,
            ValidationType.RISK_ASSESSMENT: self._validate_risk,
            ValidationType.PERFORMANCE: self._validate_performance,
            ValidationType.BACKTEST: self._validate_backtest
        }
    
    def validate(self, validation_type: ValidationType, 
                strategy_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """执行验证"""
        if validation_type not in self.validators:
            return False, [f"不支持的验证类型: {validation_type.value}"]
        
        try:
            return self.validators[validation_type](strategy_data)
        except Exception as e:
            return False, [f"验证执行失败: {str(e)}"]
    
    def _validate_code_quality(self, strategy_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证代码质量"""
        issues = []
        strategy_code = strategy_data.get('code', '')
        
        # 检查语法
        try:
            ast.parse(strategy_code)
        except SyntaxError as e:
            issues.append(f"语法错误: {str(e)}")
        
        # 检查代码复杂度
        lines = strategy_code.split('\n')
        if len(lines) > 500:
            issues.append("代码行数过多，建议简化")
        
        # 检查导入
        imports = []
        for line in lines:
            if line.strip().startswith('import ') or line.strip().startswith('from '):
                imports.append(line.strip())
        
        # 检查危险导入
        dangerous_imports = ['os.system', 'subprocess.call', 'eval', 'exec']
        for imp in imports:
            if any(danger in imp for danger in dangerous_imports):
                issues.append(f"危险导入: {imp}")
        
        return len(issues) == 0, issues
    
    def _validate_compliance(self, strategy_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证合规性"""
        from ..compliance.compliance_engine import compliance_engine
        
        # 使用合规引擎检查
        report = compliance_engine.check_strategy(
            strategy_data.get('strategy_id', ''),
            strategy_data
        )
        
        issues = []
        if report.overall_level.value in ['violation', 'critical']:
            issues.extend(report.required_actions)
        
        return len(issues) == 0, issues
    
    def _validate_risk(self, strategy_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证风险评估"""
        from ..risk.risk_rating_system import risk_rating_system
        
        # 使用风险评级系统评估
        assessment = risk_rating_system.assess_strategy_risk(
            strategy_data.get('strategy_id', ''),
            strategy_data.get('backtest_results', {}),
            strategy_data
        )
        
        issues = []
        if assessment.overall_level.value in ['high', 'extreme']:
            issues.extend(assessment.recommendations)
        
        return len(issues) == 0, issues
    
    def _validate_performance(self, strategy_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证性能"""
        issues = []
        backtest_results = strategy_data.get('backtest_results', {})
        
        # 检查基本性能指标
        sharpe_ratio = backtest_results.get('sharpe_ratio', 0)
        max_drawdown = backtest_results.get('max_drawdown', 1)
        win_rate = backtest_results.get('win_rate', 0)
        
        if sharpe_ratio < 0.5:
            issues.append("夏普比率过低")
        
        if max_drawdown > 0.3:
            issues.append("最大回撤过大")
        
        if win_rate < 0.4:
            issues.append("胜率过低")
        
        return len(issues) == 0, issues
    
    def _validate_backtest(self, strategy_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证回测"""
        issues = []
        backtest_results = strategy_data.get('backtest_results', {})
        
        # 检查回测数据完整性
        required_fields = ['total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']
        for field in required_fields:
            if field not in backtest_results:
                issues.append(f"缺少回测指标: {field}")
        
        # 检查回测时间范围
        start_date = backtest_results.get('start_date')
        end_date = backtest_results.get('end_date')
        
        if start_date and end_date:
            try:
                start = datetime.fromisoformat(start_date)
                end = datetime.fromisoformat(end_date)
                duration = (end - start).days
                
                if duration < 30:
                    issues.append("回测时间过短")
                elif duration > 365 * 5:
                    issues.append("回测时间过长，可能包含过时数据")
                    
            except ValueError:
                issues.append("回测日期格式错误")
        
        return len(issues) == 0, issues


class DeploymentPipeline:
    """部署管道"""
    
    def __init__(self, workspace_dir: str = "deployment_workspace"):
        self.workspace_dir = Path(workspace_dir)
        self.workspace_dir.mkdir(exist_ok=True)
        
        self.validator = DeploymentValidator()
        self.deployments: Dict[str, DeploymentResult] = {}
        self.deployment_callbacks: List[Callable[[DeploymentResult], None]] = []
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="deployment")
        
        # 初始化部署步骤
        self.deployment_steps = self._initialize_deployment_steps()
    
    def _initialize_deployment_steps(self) -> List[DeploymentStep]:
        """初始化部署步骤"""
        return [
            DeploymentStep(
                step_id="validate_code",
                name="代码验证",
                stage=DeploymentStage.VALIDATION,
                order=1,
                command="validate_code",
                timeout=300
            ),
            DeploymentStep(
                step_id="compliance_check",
                name="合规检查",
                stage=DeploymentStage.VALIDATION,
                order=2,
                command="compliance_check",
                timeout=180
            ),
            DeploymentStep(
                step_id="risk_assessment",
                name="风险评估",
                stage=DeploymentStage.VALIDATION,
                order=3,
                command="risk_assessment",
                timeout=240
            ),
            DeploymentStep(
                step_id="unit_test",
                name="单元测试",
                stage=DeploymentStage.TESTING,
                order=4,
                command="run_unit_tests",
                timeout=600
            ),
            DeploymentStep(
                step_id="integration_test",
                name="集成测试",
                stage=DeploymentStage.TESTING,
                order=5,
                command="run_integration_tests",
                timeout=900
            ),
            DeploymentStep(
                step_id="staging_deploy",
                name="预发布部署",
                stage=DeploymentStage.STAGING,
                order=6,
                command="deploy_to_staging",
                timeout=300,
                rollback_command="rollback_staging"
            ),
            DeploymentStep(
                step_id="staging_test",
                name="预发布测试",
                stage=DeploymentStage.STAGING,
                order=7,
                command="test_staging",
                timeout=600
            ),
            DeploymentStep(
                step_id="production_deploy",
                name="生产部署",
                stage=DeploymentStage.PRODUCTION,
                order=8,
                command="deploy_to_production",
                timeout=600,
                rollback_command="rollback_production"
            ),
            DeploymentStep(
                step_id="health_check",
                name="健康检查",
                stage=DeploymentStage.PRODUCTION,
                order=9,
                command="health_check",
                timeout=300
            )
        ]
    
    def add_deployment_callback(self, callback: Callable[[DeploymentResult], None]) -> None:
        """添加部署回调"""
        self.deployment_callbacks.append(callback)
    
    def start_deployment(self, config: DeploymentConfig, 
                        strategy_data: Dict[str, Any]) -> str:
        """启动部署"""
        deployment_id = f"{config.strategy_id}_{config.version}_{int(time.time())}"
        
        # 创建部署结果对象
        result = DeploymentResult(
            deployment_id=deployment_id,
            strategy_id=config.strategy_id,
            version=config.version,
            stage=DeploymentStage.VALIDATION,
            status=DeploymentStatus.PENDING,
            start_time=datetime.now(),
            total_steps=len(self.deployment_steps)
        )
        
        with self._lock:
            self.deployments[deployment_id] = result
        
        # 异步执行部署
        future = self._executor.submit(
            self._execute_deployment, 
            deployment_id, 
            config, 
            strategy_data
        )
        
        logging.info(f"启动部署: {deployment_id}")
        return deployment_id
    
    def _execute_deployment(self, deployment_id: str, 
                           config: DeploymentConfig,
                           strategy_data: Dict[str, Any]) -> DeploymentResult:
        """执行部署"""
        result = self.deployments[deployment_id]
        
        try:
            result.status = DeploymentStatus.RUNNING
            
            # 创建策略工作目录
            strategy_workspace = self.workspace_dir / config.strategy_id / config.version
            strategy_workspace.mkdir(parents=True, exist_ok=True)
            
            # 保存策略数据
            self._save_strategy_data(strategy_workspace, strategy_data)
            
            # 按顺序执行部署步骤
            for step in self.deployment_steps:
                if result.status == DeploymentStatus.CANCELLED:
                    break
                
                step_result = self._execute_step(step, strategy_workspace, strategy_data)
                
                if not step_result['success']:
                    result.status = DeploymentStatus.FAILED
                    result.error_message = step_result['error']
                    
                    # 执行回滚
                    if config.rollback_enabled and step.rollback_command:
                        self._execute_rollback(deployment_id, step)
                    
                    break
                
                result.steps_completed += 1
                result.logs.append(f"步骤完成: {step.name}")
            
            # 如果所有步骤都成功
            if result.status == DeploymentStatus.RUNNING:
                result.status = DeploymentStatus.SUCCESS
                result.logs.append("部署成功完成")
            
        except Exception as e:
            result.status = DeploymentStatus.FAILED
            result.error_message = str(e)
            result.logs.append(f"部署异常: {str(e)}")
            logging.error(f"部署异常 {deployment_id}: {e}")
        
        finally:
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()
            
            # 触发回调
            for callback in self.deployment_callbacks:
                try:
                    callback(result)
                except Exception as e:
                    logging.error(f"部署回调执行失败: {e}")
        
        return result
    
    def _execute_step(self, step: DeploymentStep, 
                     workspace: Path, 
                     strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行部署步骤"""
        try:
            logging.info(f"执行部署步骤: {step.name}")
            
            if step.command == "validate_code":
                return self._validate_code_step(strategy_data)
            elif step.command == "compliance_check":
                return self._compliance_check_step(strategy_data)
            elif step.command == "risk_assessment":
                return self._risk_assessment_step(strategy_data)
            elif step.command == "run_unit_tests":
                return self._run_unit_tests_step(workspace)
            elif step.command == "run_integration_tests":
                return self._run_integration_tests_step(workspace, strategy_data)
            elif step.command == "deploy_to_staging":
                return self._deploy_to_staging_step(workspace, strategy_data)
            elif step.command == "test_staging":
                return self._test_staging_step(strategy_data)
            elif step.command == "deploy_to_production":
                return self._deploy_to_production_step(workspace, strategy_data)
            elif step.command == "health_check":
                return self._health_check_step(strategy_data)
            else:
                return {"success": False, "error": f"未知命令: {step.command}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _validate_code_step(self, strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """代码验证步骤"""
        success, issues = self.validator.validate(
            ValidationType.CODE_QUALITY, 
            strategy_data
        )
        
        if not success:
            return {"success": False, "error": "; ".join(issues)}
        
        return {"success": True}
    
    def _compliance_check_step(self, strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """合规检查步骤"""
        success, issues = self.validator.validate(
            ValidationType.COMPLIANCE, 
            strategy_data
        )
        
        if not success:
            return {"success": False, "error": "; ".join(issues)}
        
        return {"success": True}
    
    def _risk_assessment_step(self, strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """风险评估步骤"""
        success, issues = self.validator.validate(
            ValidationType.RISK_ASSESSMENT, 
            strategy_data
        )
        
        if not success:
            return {"success": False, "error": "; ".join(issues)}
        
        return {"success": True}
    
    def _run_unit_tests_step(self, workspace: Path) -> Dict[str, Any]:
        """运行单元测试步骤"""
        try:
            # 运行单元测试
            test_file = workspace / "test_strategy.py"
            if test_file.exists():
                result = subprocess.run(
                    ["python", "-m", "pytest", str(test_file), "-v"],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                
                if result.returncode != 0:
                    return {"success": False, "error": result.stderr}
            
            return {"success": True}
            
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "单元测试超时"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _run_integration_tests_step(self, workspace: Path, 
                                   strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """运行集成测试步骤"""
        try:
            # 模拟集成测试
            # 实际实现中应该运行具体的集成测试
            
            # 检查策略是否可以正常加载
            strategy_code = strategy_data.get('code', '')
            if not strategy_code:
                return {"success": False, "error": "策略代码为空"}
            
            # 尝试编译策略代码
            try:
                compile(strategy_code, '<string>', 'exec')
            except SyntaxError as e:
                return {"success": False, "error": f"策略代码编译失败: {str(e)}"}
            
            return {"success": True}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _deploy_to_staging_step(self, workspace: Path, 
                               strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """部署到预发布环境步骤"""
        try:
            # 创建部署包
            deployment_package = self._create_deployment_package(workspace, strategy_data)
            
            # 模拟部署到预发布环境
            staging_dir = workspace / "staging"
            staging_dir.mkdir(exist_ok=True)
            
            # 复制策略文件到预发布环境
            shutil.copy2(deployment_package, staging_dir / "strategy_package.zip")
            
            # 解压部署包
            with zipfile.ZipFile(deployment_package, 'r') as zip_ref:
                zip_ref.extractall(staging_dir)
            
            return {"success": True}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _test_staging_step(self, strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """预发布环境测试步骤"""
        try:
            # 模拟预发布环境测试
            # 实际实现中应该在预发布环境中运行测试
            
            # 检查策略基本功能
            strategy_id = strategy_data.get('strategy_id', '')
            if not strategy_id:
                return {"success": False, "error": "策略ID为空"}
            
            return {"success": True}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _deploy_to_production_step(self, workspace: Path, 
                                  strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """部署到生产环境步骤"""
        try:
            # 创建生产部署包
            deployment_package = self._create_deployment_package(workspace, strategy_data)
            
            # 模拟部署到生产环境
            production_dir = workspace / "production"
            production_dir.mkdir(exist_ok=True)
            
            # 复制策略文件到生产环境
            shutil.copy2(deployment_package, production_dir / "strategy_package.zip")
            
            # 解压部署包
            with zipfile.ZipFile(deployment_package, 'r') as zip_ref:
                zip_ref.extractall(production_dir)
            
            return {"success": True}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _health_check_step(self, strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """健康检查步骤"""
        try:
            # 模拟健康检查
            # 实际实现中应该检查策略在生产环境中的运行状态
            
            strategy_id = strategy_data.get('strategy_id', '')
            
            # 检查策略是否可以正常初始化
            if not strategy_id:
                return {"success": False, "error": "策略ID为空"}
            
            return {"success": True}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _execute_rollback(self, deployment_id: str, failed_step: DeploymentStep) -> None:
        """执行回滚"""
        result = self.deployments[deployment_id]
        
        try:
            if failed_step.rollback_command:
                logging.info(f"执行回滚: {failed_step.rollback_command}")
                
                # 模拟回滚操作
                result.stage = DeploymentStage.ROLLBACK
                result.logs.append(f"执行回滚: {failed_step.name}")
                
                # 实际实现中应该执行具体的回滚命令
                
                result.status = DeploymentStatus.ROLLED_BACK
                result.rollback_info = {
                    "failed_step": failed_step.step_id,
                    "rollback_command": failed_step.rollback_command,
                    "rollback_time": datetime.now().isoformat()
                }
                
        except Exception as e:
            result.logs.append(f"回滚失败: {str(e)}")
            logging.error(f"回滚失败 {deployment_id}: {e}")
    
    def _save_strategy_data(self, workspace: Path, strategy_data: Dict[str, Any]) -> None:
        """保存策略数据"""
        # 保存策略代码
        strategy_code = strategy_data.get('code', '')
        if strategy_code:
            with open(workspace / "strategy.py", 'w', encoding='utf-8') as f:
                f.write(strategy_code)
        
        # 保存策略配置
        config = {k: v for k, v in strategy_data.items() if k != 'code'}
        with open(workspace / "strategy_config.json", 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False, default=str)
    
    def _create_deployment_package(self, workspace: Path, 
                                  strategy_data: Dict[str, Any]) -> Path:
        """创建部署包"""
        package_path = workspace / "strategy_package.zip"
        
        with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 添加策略文件
            strategy_file = workspace / "strategy.py"
            if strategy_file.exists():
                zip_file.write(strategy_file, "strategy.py")
            
            # 添加配置文件
            config_file = workspace / "strategy_config.json"
            if config_file.exists():
                zip_file.write(config_file, "strategy_config.json")
            
            # 添加其他必要文件
            for file_path in workspace.glob("*.py"):
                if file_path.name != "strategy.py":
                    zip_file.write(file_path, file_path.name)
        
        return package_path
    
    def get_deployment_status(self, deployment_id: str) -> Optional[DeploymentResult]:
        """获取部署状态"""
        return self.deployments.get(deployment_id)
    
    def cancel_deployment(self, deployment_id: str) -> bool:
        """取消部署"""
        if deployment_id in self.deployments:
            result = self.deployments[deployment_id]
            if result.status in [DeploymentStatus.PENDING, DeploymentStatus.RUNNING]:
                result.status = DeploymentStatus.CANCELLED
                result.end_time = datetime.now()
                result.logs.append("部署已取消")
                return True
        return False
    
    def get_deployment_history(self, strategy_id: Optional[str] = None, 
                              limit: int = 50) -> List[DeploymentResult]:
        """获取部署历史"""
        deployments = list(self.deployments.values())
        
        if strategy_id:
            deployments = [d for d in deployments if d.strategy_id == strategy_id]
        
        # 按时间倒序排列
        deployments.sort(key=lambda x: x.start_time, reverse=True)
        
        return deployments[:limit]
    
    def cleanup_old_deployments(self, days: int = 30) -> int:
        """清理旧部署记录"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        to_remove = []
        for deployment_id, result in self.deployments.items():
            if result.start_time < cutoff_date:
                to_remove.append(deployment_id)
        
        for deployment_id in to_remove:
            del self.deployments[deployment_id]
        
        # 清理工作空间
        for workspace in self.workspace_dir.iterdir():
            if workspace.is_dir():
                try:
                    # 检查目录修改时间
                    mtime = datetime.fromtimestamp(workspace.stat().st_mtime)
                    if mtime < cutoff_date:
                        shutil.rmtree(workspace)
                except Exception as e:
                    logging.warning(f"清理工作空间失败 {workspace}: {e}")
        
        return len(to_remove)


# 全局部署管道实例
deployment_pipeline = DeploymentPipeline()