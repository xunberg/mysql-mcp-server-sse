import re
import os
from enum import IntEnum, Enum
import logging
from typing import Set, List

logger = logging.getLogger(__name__)

class SQLRiskLevel(IntEnum):
    """SQL操作风险等级"""
    LOW = 1      # 查询操作（SELECT）
    MEDIUM = 2   # 基本数据修改（INSERT，有WHERE的UPDATE/DELETE）
    HIGH = 3     # 结构变更（CREATE/ALTER）和无WHERE的数据修改
    CRITICAL = 4 # 危险操作（DROP/TRUNCATE等）

class EnvironmentType(Enum):
    """环境类型"""
    DEVELOPMENT = 'development'
    PRODUCTION = 'production'

class SQLOperationType:
    """SQL操作类型分析器"""
    
    def __init__(self):
        # 环境类型处理
        env_type_str = os.getenv('ENV_TYPE', 'development').lower()
        try:
            self.env_type = EnvironmentType(env_type_str)
        except ValueError:
            logger.warning(f"无效的环境类型: {env_type_str}，使用默认值: development")
            self.env_type = EnvironmentType.DEVELOPMENT
        
        # 基础操作集合
        self.ddl_operations = {
            'CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME'
        }
        self.dml_operations = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'
        }
        
        # 添加元数据操作集合
        self.metadata_operations = {
            'SHOW', 'DESC', 'DESCRIBE', 'EXPLAIN', 'HELP', 
            'ANALYZE', 'CHECK', 'CHECKSUM', 'OPTIMIZE'
        }
        
        # 风险等级配置
        self.allowed_risk_levels = self._parse_risk_levels()
        self.blocked_patterns = self._parse_blocked_patterns('BLOCKED_PATTERNS')
        
        # 生产环境特殊处理：如果没有明确配置风险等级，则只允许LOW风险操作
        if self.env_type == EnvironmentType.PRODUCTION and not os.getenv('ALLOWED_RISK_LEVELS'):
            self.allowed_risk_levels = {SQLRiskLevel.LOW}
        
        logger.info(f"SQL分析器初始化 - 环境: {self.env_type.value}")
        logger.info(f"允许的风险等级: {[level.name for level in self.allowed_risk_levels]}")

    def _parse_risk_levels(self) -> Set[SQLRiskLevel]:
        """解析允许的风险等级"""
        allowed_levels_str = os.getenv('ALLOWED_RISK_LEVELS', 'LOW,MEDIUM')
        allowed_levels = set()
        
        logger.info(f"从环境变量读取到的风险等级设置: '{allowed_levels_str}'")
        
        for level_str in allowed_levels_str.upper().split(','):
            level_str = level_str.strip()
            try:
                allowed_levels.add(SQLRiskLevel[level_str])
            except KeyError:
                logger.warning(f"未知的风险等级配置: {level_str}")
                
        return allowed_levels

    def _parse_blocked_patterns(self, env_var: str) -> List[str]:
        """解析禁止的操作模式"""
        patterns = os.getenv(env_var, '').split(',')
        return [p.strip() for p in patterns if p.strip()]

    def analyze_risk(self, sql_query: str) -> dict:
        """
        分析SQL查询的风险级别和影响范围
        
        Args:
            sql_query: SQL查询语句
            
        Returns:
            dict: 包含风险分析结果的字典
        """
        sql_query = sql_query.strip()
        
        # 处理空SQL
        if not sql_query:
            return {
                'operation': '',
                'operation_type': 'UNKNOWN',
                'is_dangerous': True,
                'affected_tables': [],
                'estimated_impact': {
                    'operation': '',
                    'estimated_rows': 0,
                    'needs_where': False,
                    'has_where': False
                },
                'risk_level': SQLRiskLevel.HIGH,
                'is_allowed': False
            }
            
        operation = sql_query.split()[0].upper()
        
        # 基本风险分析
        risk_analysis = {
            'operation': operation,
            'operation_type': 'DDL' if operation in self.ddl_operations else 'DML',
            'is_dangerous': self._check_dangerous_patterns(sql_query),
            'affected_tables': self._get_affected_tables(sql_query),
            'estimated_impact': self._estimate_impact(sql_query)
        }
        
        # 计算风险等级
        risk_level = self._calculate_risk_level(sql_query, operation, risk_analysis['is_dangerous'])
        risk_analysis['risk_level'] = risk_level
        risk_analysis['is_allowed'] = risk_level in self.allowed_risk_levels
        
        return risk_analysis

    def _calculate_risk_level(self, sql_query: str, operation: str, is_dangerous: bool) -> SQLRiskLevel:
        """
        计算操作风险等级
        
        规则：
        1. 危险操作（匹配危险模式）=> CRITICAL
        2. DDL操作：
           - CREATE/ALTER => HIGH
           - DROP/TRUNCATE => CRITICAL
        3. DML操作：
           - SELECT => LOW
           - INSERT => MEDIUM
           - UPDATE/DELETE（有WHERE）=> MEDIUM
           - UPDATE（无WHERE）=> HIGH
           - DELETE（无WHERE）=> CRITICAL
        4. 元数据操作：
           - SHOW/DESC/DESCRIBE等 => LOW
        """
        # 危险操作
        if is_dangerous:
            return SQLRiskLevel.CRITICAL
            
        # 元数据操作
        if operation in self.metadata_operations:
            return SQLRiskLevel.LOW  # 元数据查询视为低风险操作
            
        # 生产环境中非SELECT操作
        if self.env_type == EnvironmentType.PRODUCTION and operation != 'SELECT':
            return SQLRiskLevel.CRITICAL
            
        # DDL操作
        if operation in self.ddl_operations:
            if operation in {'DROP', 'TRUNCATE'}:
                return SQLRiskLevel.CRITICAL
            return SQLRiskLevel.HIGH
            
        # DML操作
        if operation == 'SELECT':
            return SQLRiskLevel.LOW
        elif operation == 'INSERT':
            return SQLRiskLevel.MEDIUM
        elif operation == 'UPDATE':
            return SQLRiskLevel.HIGH if 'WHERE' not in sql_query.upper() else SQLRiskLevel.MEDIUM
        elif operation == 'DELETE':
            # 无WHERE条件的DELETE操作视为CRITICAL风险
            return SQLRiskLevel.CRITICAL if 'WHERE' not in sql_query.upper() else SQLRiskLevel.MEDIUM
            
        return SQLRiskLevel.HIGH

    def _check_dangerous_patterns(self, sql_query: str) -> bool:
        """检查是否匹配危险操作模式"""
        sql_upper = sql_query.upper()
        
        # 生产环境额外的安全检查
        if self.env_type == EnvironmentType.PRODUCTION:
            # 生产环境中禁止所有非SELECT操作
            if sql_upper.split()[0] != 'SELECT':
                return True
        
        for pattern in self.blocked_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return True
                
        return False

    def _get_affected_tables(self, sql_query: str) -> list:
        """获取受影响的表名列表"""
        words = sql_query.upper().split()
        tables = []
        
        for i, word in enumerate(words):
            if word in {'FROM', 'JOIN', 'UPDATE', 'INTO', 'TABLE'}:
                if i + 1 < len(words):
                    table = words[i + 1].strip('`;')
                    if table not in {'SELECT', 'WHERE', 'SET'}:
                        tables.append(table)
        
        return list(set(tables))

    def _estimate_impact(self, sql_query: str) -> dict:
        """
        估算查询影响范围
        
        Returns:
            dict: 包含预估影响的字典
        """
        operation = sql_query.split()[0].upper()
        
        impact = {
            'operation': operation,
            'estimated_rows': 0,
            'needs_where': operation in {'UPDATE', 'DELETE'},
            'has_where': 'WHERE' in sql_query.upper()
        }
        
        # 根据环境类型调整估算
        if self.env_type == EnvironmentType.PRODUCTION:
            if operation == 'SELECT':
                impact['estimated_rows'] = 100
            else:
                impact['estimated_rows'] = float('inf')  # 生产环境中非SELECT操作视为影响无限行
        else:
            if operation == 'SELECT':
                impact['estimated_rows'] = 100
            elif operation in {'UPDATE', 'DELETE'}:
                impact['estimated_rows'] = 1000 if impact['has_where'] else float('inf')
        
        return impact 