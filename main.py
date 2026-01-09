#!/usr/bin/env python3
"""
qBittorrent Load Balancer
监控torrent文件并智能分配到多个qBittorrent实例
支持从 Hetzner 监控接收 IP 变更通知并自动更新配置
"""

import json
import os
import time
import threading
import logging
import csv
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, field
import hashlib
from pathlib import Path
import qbittorrentapi
from flask import Flask, request, jsonify
from webhook_server import WebhookServer


# 配置常量
DEFAULT_CONFIG_FILE = "config.json"

# 时间间隔常量（秒）
DEFAULT_SLEEP_TIME = 1
TASK_PROCESSOR_SLEEP = 1
ERROR_RETRY_SLEEP = 5
RECONNECT_INTERVAL = 180
CONNECTION_TIMEOUT = 10

# 网络和存储常量
BYTES_TO_KB = 1024
BYTES_TO_GB = 1024 ** 3
BYTES_TO_TB = 1024 ** 4
MAX_RECONNECT_ATTEMPTS = 1

# 种子汇报相关常量
ANNOUNCE_WINDOW_TOLERANCE = 5

# 支持的排序键（所有均为小值优先）
SUPPORTED_SORT_KEYS = {
    'upload_speed': '上传速度',
    'download_speed': '下载速度',
    'active_downloads': '活跃下载数'
}
DEFAULT_PRIMARY_SORT_KEY = 'upload_speed'

# 创建一个简单的logger，避免在初始化之前输出日志
logger = logging.getLogger(__name__)

def setup_logging(log_dir=None):
    """设置日志配置，同时输出到控制台和文件"""
    # 初始化logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(logging.DEBUG)
    app_logger.handlers.clear()
    
    # 设置基础格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 添加控制台处理器
    _add_console_handler(app_logger, formatter)
    
    # 添加文件处理器（如果指定了日志目录）
    if log_dir:
        _add_file_handlers(app_logger, formatter, log_dir)
    
    return app_logger


def _add_console_handler(logger, formatter):
    """添加控制台日志处理器"""
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def _add_file_handlers(logger, formatter, log_dir):
    """添加文件日志处理器"""
    try:
        from logging.handlers import TimedRotatingFileHandler
        
        # 创建日志目录
        os.makedirs(log_dir, exist_ok=True)
        
        # 主日志文件
        main_log_path = os.path.join(log_dir, 'qbittorrent_loadbalancer.log')
        file_handler = _create_rotating_handler(main_log_path, logging.DEBUG, formatter)
        logger.addHandler(file_handler)
        
        # 错误日志文件
        error_log_path = os.path.join(log_dir, 'qbittorrent_error.log')
        error_handler = _create_rotating_handler(error_log_path, logging.ERROR, formatter)
        logger.addHandler(error_handler)
        
        logger.info(f"日志文件将保存到：{log_dir}")
        
    except Exception as e:
        print(f"警告: 无法设置文件日志: {e}")


def _create_rotating_handler(filename, level, formatter):
    """创建按日期轮转的日志处理器"""
    from logging.handlers import TimedRotatingFileHandler
    
    handler = TimedRotatingFileHandler(
        filename=filename,
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


@dataclass
class InstanceInfo:
    """qBittorrent实例信息"""
    name: str
    url: str
    username: str
    password: str
    client: Optional[qbittorrentapi.Client] = None
    is_connected: bool = False
    upload_speed: float = 0.0  # KB/s
    download_speed: float = 0.0  # KB/s
    active_downloads: int = 0
    free_space: int = 0  # bytes
    new_tasks_count: int = 0  # 新分配的任务数
    total_added_tasks_count: int = 0  # 已添加的总任务计数
    success_metrics_count: int = 0  # 成功获取统计信息的次数
    traffic_out: int = 0  # 出站流量 (bytes)
    traffic_limit: int = 0  # 流量限制 (bytes)
    traffic_check_url: str = ""  # 流量检查URL
    reserved_space: int = 0  # 需要保留的空闲空间 (bytes)
    last_update: datetime = field(default_factory=datetime.now)
    is_reconnecting: bool = False  # 是否正在重连中


@dataclass
class PendingTorrent:
    """待处理的torrent"""
    download_url: str
    release_name: str
    category: Optional[str] = None


class ConfigManager:
    """🆕 配置文件管理器 - 负责读取和更新 config.json"""
    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE):
        self.config_file = config_file
        self.config_lock = threading.Lock()
    
    def load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"配置文件未找到：{self.config_file}")
            raise
        except json.JSONDecodeError:
            logger.error(f"配置文件格式错误：{self.config_file}")
            raise
    
    def save_config(self, config: dict) -> bool:
        """保存配置文件"""
        try:
            with self.config_lock:
                # 先备份原配置
                backup_file = f"{self.config_file}.backup"
                if os.path.exists(self.config_file):
                    import shutil
                    shutil.copy2(self.config_file, backup_file)
                
                # 写入新配置
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                
                logger.info(f"✓ 配置文件已更新：{self.config_file}")
                return True
                
        except Exception as e:
            logger.error(f"✗ 保存配置文件失败：{e}")
            return False
    
    def extract_ip_from_url(self, url: str) -> Optional[str]:
        """
        从 URL 中提取 IP 地址或主机名
        支持多种格式：
        - http://46.224.213.76:9090 → 46.224.213.76
        - http://111:9090 → 111
        - http://localhost:9090 → localhost
        - http://qb-server:9090 → qb-server
        """
        import re
        
        # 方法1: 尝试匹配完整 IPv4 地址
        ipv4_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        match = re.search(ipv4_pattern, url)
        if match:
            return match.group(1)
        
        # 方法2: 提取 host:port 格式中的 host 部分
        # 匹配 http(s)://host:port 或 http(s)://host
        host_pattern = r'https?://([^:/]+)'
        match = re.search(host_pattern, url)
        if match:
            return match.group(1)
        
        logger.warning(f"无法从 URL 中提取 IP/主机名: {url}")
        return None
    
    def update_instance_ip(self, old_ip: str, new_ip: str) -> Dict[str, any]:
        """
        🆕 优化版IP更新逻辑：
        1. 如果只提供new_ip（初始创建场景），找第一个占位符实例更新
        2. 如果提供old_ip和new_ip，执行替换
        3. 如果new_ip已存在，跳过
        """
        try:
            config = self.load_config()
            instances = config.get('qbittorrent_instances', [])
            
            # 1. 检查 new_ip 是否已存在
            for instance in instances:
                current_host = self.extract_ip_from_url(instance.get('url', ''))
                if current_host == new_ip:
                    logger.info(f"ℹ IP {new_ip} 已存在于实例 {instance.get('name')} 中")
                    return {'success': True, 'updated_count': 0, 'message': f'IP {new_ip} 已存在'}
            
            # 2. 确定要更新的目标实例
            target_instance = None
            
            # 如果提供了old_ip，优先匹配old_ip
            if old_ip:
                for instance in instances:
                    current_host = self.extract_ip_from_url(instance.get('url', ''))
                    if current_host == old_ip:
                        target_instance = instance
                        logger.info(f"🎯 匹配到旧IP ({old_ip}) 的实例: {instance.get('name')}")
                        break
            
            # 如果没有匹配到old_ip，或者没有提供old_ip（初始创建场景）
            if not target_instance:
                # 收集所有当前有效的IP
                current_ips = set()
                for inst in instances:
                    ip = self.extract_ip_from_url(inst.get('url', ''))
                    if ip and self._is_valid_ip(ip):
                        current_ips.add(ip)
                
                # 找第一个使用占位符或无效IP的实例
                for instance in instances:
                    current_host = self.extract_ip_from_url(instance.get('url', ''))
                    if not current_host or not self._is_valid_ip(current_host):
                        target_instance = instance
                        logger.info(f"📝 找到占位符实例进行更新: {instance.get('name')}")
                        break
            
            # 3. 执行更新
            if target_instance:
                old_url = target_instance['url']
                current_host = self.extract_ip_from_url(old_url)
                
                if current_host:
                    new_url = old_url.replace(current_host, new_ip)
                else:
                    # 如果无法提取host，重构URL
                    import re
                    port_match = re.search(r':(\d+)', old_url)
                    port = port_match.group(1) if port_match else '9090'
                    new_url = f"http://{new_ip}:{port}"
                
                target_instance['url'] = new_url
                
                if self.save_config(config):
                    logger.info(f"✅ 已更新实例 {target_instance.get('name')}: {old_url} → {new_url}")
                    return {
                        'success': True,
                        'updated_count': 1,
                        'updated_instances': [{
                            'name': target_instance.get('name'),
                            'old_url': old_url,
                            'new_url': new_url
                        }],
                        'message': f'成功更新到 {new_ip}'
                    }
            
            logger.warning("⚠ 未找到合适的实例进行更新")
            return {'success': True, 'updated_count': 0, 'message': '未找到合适的更新目标'}
                
        except Exception as e:
            logger.error(f"✗ 更新实例IP失败：{e}")
            return {'success': False, 'error': str(e)}

    def _is_valid_ip(self, ip: str) -> bool:
        """检查是否为有效的IPv4地址"""
        import re
        pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        if not re.match(pattern, ip):
            return False
        # 验证每个数字在0-255范围内
        parts = ip.split('.')
        return all(0 <= int(part) <= 255 for part in parts)
    
    def check_ip_exists(self, ip: str) -> bool:
        """检查配置中是否存在指定IP"""
        try:
            config = self.load_config()
            instances = config.get('qbittorrent_instances', [])
            
            for instance in instances:
                url = instance.get('url', '')
                # 使用增强的提取方法
                current_host = self.extract_ip_from_url(url)
                if current_host == ip or ip in url:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"检查IP存在性失败：{e}")
            return False


class ConfigWatcher:
    """配置文件监控器 - 检测 config.json 变化并触发热重载"""
    
    def __init__(self, config_file: str, check_interval: int = 5):
        """
        初始化监控器
        
        Args:
            config_file: 配置文件路径
            check_interval: 检查间隔（秒）
        """
        self.config_file = Path(config_file)
        self.check_interval = check_interval
        self.last_hash = self._get_file_hash()
        self.callbacks = []
        self.running = False
        self.watch_thread = None
        
    def _get_file_hash(self) -> str:
        """计算配置文件的哈希值"""
        try:
            if not self.config_file.exists():
                return ""
            
            with open(self.config_file, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.error(f"计算文件哈希失败: {e}")
            return ""
    
    def register_callback(self, callback):
        """注册配置变化时的回调函数"""
        self.callbacks.append(callback)
        logger.debug(f"已注册配置变化回调: {callback.__name__}")
    
    def _notify_change(self):
        """通知所有回调函数配置已变化"""
        logger.info(f"🔥 检测到配置文件变化，触发 {len(self.callbacks)} 个回调")
        for callback in self.callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"执行回调失败: {callback.__name__}, 错误: {e}")
    
    def _watch_loop(self):
        """监控循环"""
        logger.info(f"📂 配置文件监控已启动，检查间隔: {self.check_interval}秒")
        
        while self.running:
            try:
                current_hash = self._get_file_hash()
                
                if current_hash and current_hash != self.last_hash:
                    logger.info(f"🔔 配置文件已更新: {self.config_file}")
                    self.last_hash = current_hash
                    time.sleep(0.5)  # 等待文件写入完成
                    self._notify_change()
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"配置监控异常: {e}")
                time.sleep(self.check_interval)
    
    def start(self):
        """启动监控"""
        if self.running:
            logger.warning("配置监控已在运行中")
            return
        
        self.running = True
        self.watch_thread = threading.Thread(
            target=self._watch_loop,
            daemon=True,
            name="config-watcher"
        )
        self.watch_thread.start()
        logger.info("✓ 配置文件监控线程已启动")
    
    def stop(self):
        """停止监控"""
        self.running = False
        if self.watch_thread:
            self.watch_thread.join(timeout=2)
        logger.info("配置文件监控已停止")


class QBittorrentLoadBalancer:
    """qBittorrent负载均衡器"""
    
    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE):
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.load_config()
        self.instances: List[InstanceInfo] = []
        self.pending_torrents: List[PendingTorrent] = []
        self.pending_torrents_lock = threading.Lock()
        self.instances_lock = threading.Lock()
        self.announce_retry_counts = {}
        
        # 重新配置日志
        self._setup_logging()
        
        # 初始化webhook服务器
        self.webhook_server: Optional[WebhookServer] = None
        
        # 初始化 Flask API 服务器
        self.api_server: Optional[Flask] = None
        self.api_port = self.config.get('api_port', 5007)
        
        # 🆕 初始化配置文件监控器
        self.config_watcher = ConfigWatcher(
            config_file=config_file,
            check_interval=self.config.get('config_watch_interval', 5)
        )
        # 注册配置变化时的回调
        self.config_watcher.register_callback(self._on_config_changed)
        
        self._setup_environment()
        
    def _setup_logging(self) -> None:
        """根据配置设置日志"""
        global logger
        
        # 从配置中获取日志目录，默认为 /app/logs（Docker环境）或 ./logs（本地环境）
        log_dir = self.config.get('log_dir')
        if log_dir is None:
            # 自动检测环境
            if os.path.exists('/app'):  # Docker环境
                log_dir = '/app/logs'
            else:  # 本地环境
                log_dir = './logs'
        
        logger = setup_logging(log_dir)
        
    def _setup_environment(self) -> None:
        """设置运行环境"""
        # 验证配置
        self._validate_config()
        # 设置配置默认值和验证
        self._set_config_defaults()
        
        # 🆕 优先启动 API 服务器和 Webhook 服务器（立即可用）
        self._start_api_server()
        self._start_webhook_server()
        
        # 异步初始化 qBittorrent 实例（不阻塞启动）
        self._init_instances_async()
    def _on_config_changed(self):
        """配置文件变化时的处理函数（热重载核心逻辑）"""
        logger.info("="*70)
        logger.info("🔥 开始热重载配置...")
        logger.info("="*70)
        
        try:
            # 1. 重新加载配置文件
            new_config = self.config_manager.load_config()
            logger.info("✓ 配置文件已重新读取")
            
            # 2. 比对实例配置的变化
            old_instances = {inst['name']: inst for inst in self.config.get('qbittorrent_instances', [])}
            new_instances = {inst['name']: inst for inst in new_config.get('qbittorrent_instances', [])}
            
            # 3. 更新内存中的实例配置
            instances_changed = False
            with self.instances_lock:
                for instance in self.instances:
                    if instance.name in new_instances:
                        new_conf = new_instances[instance.name]
                        old_conf = old_instances.get(instance.name, {})
                        
                        # 🔥 关键修改：比对新配置和内存中的实例URL（而不是旧配置）
                        if new_conf['url'] != instance.url:
                            old_url = instance.url
                            instance.url = new_conf['url']
                            instance.username = new_conf['username']
                            instance.password = new_conf['password']
                            instance.is_connected = False
                            instances_changed = True
                            
                            logger.info(f"🔄 实例 {instance.name} URL 已变更:")
                            logger.info(f"   旧: {old_url}")
                            logger.info(f"   新: {instance.url}")
                            
                            # 立即触发重连
                            self._async_reconnect_single_instance(instance)
                        
                        # 检查认证信息是否变化
                        elif (new_conf.get('username') != instance.username or
                            new_conf.get('password') != instance.password):
                            instance.username = new_conf['username']
                            instance.password = new_conf['password']
                            instance.is_connected = False
                            instances_changed = True
                            logger.info(f"🔑 实例 {instance.name} 认证信息已变更，触发重连")
                            self._async_reconnect_single_instance(instance)
                        else:
                            logger.debug(f"✓ 实例 {instance.name} 配置无变化")
            
            # 4. 更新全局配置
            self.config = new_config
            logger.info("✓ 全局配置已更新")
            
            if not instances_changed:
                logger.info("ℹ️  本次配置变更未涉及实例URL或认证信息")
            
            logger.info("="*70)
            logger.info("🎉 配置热重载完成")
            logger.info("="*70)
            
        except Exception as e:
            logger.error(f"❌ 配置热重载失败: {e}")
            import traceback
            logger.error(traceback.format_exc())


    def _async_reconnect_single_instance(self, instance: InstanceInfo):
        """异步重连单个实例（不阻塞主线程）"""
        def reconnect():
            time.sleep(1)  # 等待1秒确保配置稳定
            logger.info(f"🔌 开始重连实例: {instance.name}")
            self._connect_instance(instance)
        
        threading.Thread(
            target=reconnect,
            daemon=True,
            name=f"reconnect-{instance.name}"
        ).start()


    def _validate_config(self) -> None:
        """验证配置文件的有效性"""
        # 验证primary_sort_key配置
        primary_sort_key = self.config.get('primary_sort_key', DEFAULT_PRIMARY_SORT_KEY)
        if primary_sort_key not in SUPPORTED_SORT_KEYS:
            logger.warning(f"不支持的排序键：{primary_sort_key}，使用默认值：{DEFAULT_PRIMARY_SORT_KEY}")
            self.config['primary_sort_key'] = DEFAULT_PRIMARY_SORT_KEY
        else:
            logger.info(f"使用排序策略：主要因素={SUPPORTED_SORT_KEYS[primary_sort_key]}，次要因素=累计添加任务数，第三因素=空闲空间")
            
        # 验证快速汇报分类黑名单配置
        blacklist = self.config.get('fast_announce_category_blacklist')
        if blacklist is not None:
            if not isinstance(blacklist, list):
                logger.warning(f"fast_announce_category_blacklist 配置格式错误，必须是数组，当前类型：{type(blacklist)}，已重置为空数组")
                self.config['fast_announce_category_blacklist'] = []
            else:
                # 验证数组中的每个元素都是字符串
                valid_blacklist = []
                for item in blacklist:
                    if isinstance(item, str):
                        valid_blacklist.append(item)
                    else:
                        logger.warning(f"黑名单中包含非字符串项目：{item} (类型：{type(item)})，已忽略")
                
                self.config['fast_announce_category_blacklist'] = valid_blacklist
                if valid_blacklist:
                    logger.info(f"快速汇报分类黑名单已配置，包含 {len(valid_blacklist)} 个分类：{valid_blacklist}")
                else:
                    logger.info("快速汇报分类黑名单为空，所有分类都将执行快速汇报")
        else:
            # 如果没有配置黑名单，设置为空数组
            self.config['fast_announce_category_blacklist'] = []
            logger.info("未配置快速汇报分类黑名单，所有分类都将执行快速汇报")
            
    def _load_config(self, config_file: str) -> dict:
        """加载配置文件（已被 ConfigManager 替代）"""
        return self.config_manager.load_config()
    
    def _set_config_defaults(self) -> None:
        """设置配置默认值和验证"""
        # 设置快速汇报间隔默认值，并限制在2-10秒范围内
        fast_interval = self.config.get('fast_announce_interval', 3)
        if not isinstance(fast_interval, (int, float)) or fast_interval < 2 or fast_interval > 10:
            logger.warning(f"fast_announce_interval 值无效 ({fast_interval})，必须在2-10秒范围内，使用默认值4秒")
            fast_interval = 3
        self.config['fast_announce_interval'] = fast_interval
        
        logger.info(f"状态更新间隔配置：快速检查={fast_interval}秒，正常检查={fast_interval * 2}秒")

    def _init_instances(self) -> None:
        """初始化qBittorrent实例连接（同步版本 - 会阻塞）"""
        for instance_config in self.config['qbittorrent_instances']:
            instance = self._create_instance_from_config(instance_config)
            self._connect_instance(instance)
            self.instances.append(instance)
    
    def _init_instances_async(self) -> None:
        """🆕 异步初始化qBittorrent实例连接（不阻塞启动）"""
        logger.info("🔄 开始异步初始化 qBittorrent 实例...")
        
        # 先创建所有实例对象（不连接）
        for instance_config in self.config['qbittorrent_instances']:
            instance = self._create_instance_from_config(instance_config)
            instance.is_connected = False
            self.instances.append(instance)
            logger.info(f"📝 已加载实例配置: {instance.name} ({instance.url})")
        
        # 在后台线程中连接实例
        def connect_instances():
            logger.info("🔌 开始连接 qBittorrent 实例...")
            for instance in self.instances:
                self._connect_instance(instance)
            logger.info("✅ qBittorrent 实例初始化完成")
        
        connect_thread = threading.Thread(target=connect_instances, daemon=True, name="init-instances")
        connect_thread.start()
            
    def _create_instance_from_config(self, config: Dict[str, str]) -> InstanceInfo:
        """根据配置创建实例信息对象"""
        # 安全地转换流量限制值（从MB转换为字节）
        try:
            traffic_limit_mb = config.get('traffic_limit', 0.0)
            traffic_limit_bytes = int(float(traffic_limit_mb) * 1024 * 1024)  # MB转字节
        except (ValueError, TypeError) as e:
            logger.warning(f"实例 {config.get('name', 'Unknown')} 流量限制值转换失败：{e}，设置为0")
            traffic_limit_bytes = 0
        
        # 安全地转换保留空间值（从MB转换为字节）
        try:
            reserved_space_mb = config.get('reserved_space', 21 * 1024)  # 默认21GB
            reserved_space_bytes = int(float(reserved_space_mb) * 1024 * 1024)  # MB转字节
        except (ValueError, TypeError) as e:
            logger.warning(f"实例 {config.get('name', 'Unknown')} 保留空间值转换失败：{e}，设置为默认值21GB")
            reserved_space_bytes = 21 * BYTES_TO_GB
            
        return InstanceInfo(
            name=config['name'],
            url=config['url'],
            username=config['username'],
            password=config['password'],
            traffic_check_url=config.get('traffic_check_url', ''),
            traffic_limit=traffic_limit_bytes,
            reserved_space=reserved_space_bytes
        )
        
    def _connect_instance(self, instance: InstanceInfo) -> None:
        """连接到qBittorrent实例"""
        try:
            connection_timeout = self.config.get('connection_timeout', CONNECTION_TIMEOUT)
            client = qbittorrentapi.Client(
                host=instance.url,
                username=instance.username,
                password=instance.password,
                REQUESTS_ARGS={'timeout': connection_timeout}
            )
            client.auth_log_in()
            instance.client = client
            instance.is_connected = True
            logger.info(f"成功连接到实例：{instance.name}")
        except Exception as e:
            logger.error(f"连接实例失败：{instance.name}，错误：{e}")
            instance.is_connected = False
            # 记录连接失败的时间，用于后续重连判断
            instance.last_update = datetime.now()
            
    def _attempt_reconnect(self, instance: InstanceInfo) -> bool:
        """尝试重新连接到实例"""
        logger.info(f"尝试重新连接到实例：{instance.name}")
        
        max_attempts = self.config.get('max_reconnect_attempts', MAX_RECONNECT_ATTEMPTS)
        connection_timeout = self.config.get('connection_timeout', CONNECTION_TIMEOUT)
        
        for attempt in range(max_attempts):
            try:
                client = qbittorrentapi.Client(
                    host=instance.url,
                    username=instance.username,
                    password=instance.password,
                    REQUESTS_ARGS={'timeout': connection_timeout}
                )
                
                # 设置连接超时并尝试登录
                client.auth_log_in()
                
                # 更新实例状态需要在锁内进行
                with self.instances_lock:
                    instance.client = client
                    instance.is_connected = True
                    instance.is_reconnecting = False
                    
                logger.info(f"重新连接成功：{instance.name}（尝试 {attempt + 1}/{max_attempts}）")
                return True
                
            except Exception as e:
                logger.warning(f"重连尝试 {attempt + 1}/{max_attempts} 失败：{instance.name}，错误：{e}")
                if attempt < max_attempts - 1:
                    time.sleep(2)  # 每次重连尝试间等待2秒
                    
        logger.error(f"重连彻底失败：{instance.name}")
        
        # 更新失败时间需要在锁内进行
        with self.instances_lock:
            instance.last_update = datetime.now()
            instance.is_reconnecting = False
            
        return False
        
    def _async_reconnect_instance(self, instance: InstanceInfo) -> None:
        """异步重连单个实例（在独立线程中执行）"""
        try:
            self._attempt_reconnect(instance)
        except Exception as e:
            logger.error(f"异步重连过程中发生异常：{instance.name}，错误：{e}")
            with self.instances_lock:
                instance.last_update = datetime.now()
                instance.is_reconnecting = False
        
    def _check_and_schedule_reconnects(self) -> None:
        """检查断开的实例并调度重连（非阻塞）"""
        current_time = datetime.now()
        reconnect_interval = self.config.get('reconnect_interval', RECONNECT_INTERVAL)
        
        instances_to_reconnect = []
        
        with self.instances_lock:
            for instance in self.instances:
                # 只处理未连接且未在重连中的实例
                if not instance.is_connected and not instance.is_reconnecting:
                    # 检查是否到了重连时间
                    time_since_last_attempt = (current_time - instance.last_update).total_seconds()
                    if time_since_last_attempt >= reconnect_interval:
                        instances_to_reconnect.append(instance)
                        # 标记为正在重连，防止重复调度
                        instance.is_reconnecting = True
                        instance.last_update = current_time
                        
        # 在锁外启动重连线程，避免阻塞
        for instance in instances_to_reconnect:
            logger.info(f"开始重连任务：{instance.name}")
            threading.Thread(
                target=self._async_reconnect_instance,
                args=(instance,),
                daemon=True,
                name=f"reconnect-{instance.name}"
            ).start()
        
    def _start_webhook_server(self) -> None:
        """启动webhook服务器"""
        try:
            self.webhook_server = WebhookServer(self, self.config)
            self.webhook_server.start()
            logger.info("Webhook服务器已启动")
        except Exception as e:
            logger.error(f"启动webhook服务器失败: {e}")
            raise
    
    def _start_api_server(self) -> None:
        """🆕 启动Flask API服务器"""
        try:
            logger.info(f"🚀 正在初始化API服务器...")
            
            self.api_server = Flask('qb_loadbalancer_api')
            self.api_server.logger.disabled = True
            
            # 禁用 Werkzeug 日志
            import logging as werkzeug_logging
            werkzeug_log = werkzeug_logging.getLogger('werkzeug')
            werkzeug_log.setLevel(werkzeug_logging.ERROR)
            
            @self.api_server.route('/api/update-ip', methods=['POST'])
            def update_ip():
                """接收Hetzner监控的IP变更通知"""
                try:
                    data = request.get_json()
                    if not data:
                        return jsonify({'success': False, 'error': 'No JSON data'}), 400
                    
                    new_ip = data.get('new_ip')
                    old_ip = data.get('old_ip')  # 可选
                    
                    if not new_ip:
                        return jsonify({
                            'success': False,
                            'error': 'Missing new_ip'
                        }), 400
                    
                    if old_ip:
                        logger.info(f"📡 收到IP变更通知: {old_ip} → {new_ip}")
                    else:
                        logger.info(f"📡 收到新IP通知: {new_ip}")
                    
                    # 先检查新IP是否已存在
                    if self.config_manager.check_ip_exists(new_ip):
                        logger.info(f"✓ IP {new_ip} 已存在于配置中，无需更新")
                        return jsonify({
                            'success': True,
                            'updated_count': 0,
                            'message': f'IP {new_ip} 已存在'
                        })
                    
                    # 更新配置文件
                    result = self.config_manager.update_instance_ip(old_ip, new_ip)
                    
                    if result['success'] and result['updated_count'] > 0:
                        # 🔥 关键修改：先更新内存中的实例，再重新加载配置
                        updated_instances = []
                        with self.instances_lock:
                            for instance in self.instances:
                                current_host = self.config_manager.extract_ip_from_url(instance.url)
                                
                                # 方式1: 如果提供了old_ip，精确匹配替换
                                if old_ip and current_host == old_ip:
                                    old_url = instance.url
                                    instance.url = instance.url.replace(old_ip, new_ip)
                                    instance.is_connected = False
                                    updated_instances.append(instance)
                                    logger.info(f"🔄 已更新实例 {instance.name}: {old_url} → {instance.url}")
                                
                                # 方式2: 如果没有提供old_ip，更新第一个无效IP的实例
                                elif not old_ip and (not current_host or not self.config_manager._is_valid_ip(current_host)):
                                    old_url = instance.url
                                    instance.url = instance.url.replace(current_host if current_host else '111', new_ip)
                                    instance.is_connected = False
                                    updated_instances.append(instance)
                                    logger.info(f"🔄 已更新实例 {instance.name}: {old_url} → {instance.url}")
                                    break  # 只更新第一个
                        
                        # 然后重新加载配置（保持一致性）
                        self.config = self.config_manager.load_config()
                        logger.info(f"🔄 配置已重新加载")
                        
                        # 异步触发重连
                        def reconnect_instances():
                            time.sleep(1)  # 等待1秒确保配置稳定
                            for instance in updated_instances:
                                logger.info(f"🔌 触发实例重连: {instance.name}")
                                self._connect_instance(instance)
                        
                        reconnect_thread = threading.Thread(
                            target=reconnect_instances, 
                            daemon=True, 
                            name="api-reconnect"
                        )
                        reconnect_thread.start()
                        
                        logger.info(f"✓ IP更新完成，共更新 {result['updated_count']} 个实例")
                    
                    return jsonify(result)
                    
                except Exception as e:
                    logger.error(f"✗ 处理IP更新请求失败：{e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return jsonify({
                        'success': False,
                        'error': str(e)
                    }), 500
            
            @self.api_server.route('/health', methods=['GET'])
            def health_check():
                """健康检查接口"""
                return jsonify({
                    'status': 'ok',
                    'timestamp': datetime.now().isoformat(),
                    'instances_connected': len([i for i in self.instances if i.is_connected])
                })
            
            # 在独立线程中启动API服务器
            def run_api():
                try:
                    logger.info(f"📡 API服务器线程启动中...")
                    self.api_server.run(
                        host='0.0.0.0',
                        port=self.api_port,
                        debug=False,
                        use_reloader=False,
                        threaded=True
                    )
                except Exception as e:
                    logger.error(f"❌ API服务器运行失败: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            api_thread = threading.Thread(target=run_api, daemon=True, name="api-server")
            api_thread.start()
            
            # 等待API服务器启动并验证
            max_wait = 10
            for i in range(max_wait):
                time.sleep(1)
                try:
                    response = requests.get(f'http://localhost:{self.api_port}/health', timeout=2)
                    if response.status_code == 200:
                        logger.info(f"✅ API服务器启动成功并通过健康检查!")
                        logger.info(f"🌐 监听地址: http://0.0.0.0:{self.api_port}")
                        logger.info(f"📍 可用端点:")
                        logger.info(f"   • POST /api/update-ip - 接收IP变更通知")
                        logger.info(f"   • GET /health - 健康检查")
                        return
                except:
                    if i < max_wait - 1:
                        logger.debug(f"等待API服务器就绪... ({i+1}/{max_wait})")
                    pass
            
            logger.warning(f"⚠️ API服务器可能未完全启动，但进程已运行")
            logger.info(f"🌐 监听地址: http://0.0.0.0:{self.api_port}")
            
        except Exception as e:
            logger.error(f"❌ 启动API服务器失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
    def add_pending_torrent(self, download_url: str, release_name: str, category: Optional[str] = None) -> None:
        """添加待处理的torrent"""
        if not download_url:
            logger.error("必须提供download_url")
            return
            
        if not release_name:
            logger.error("必须提供release_name")
            return
        
        try:
            with self.pending_torrents_lock:
                # 检查是否已存在（使用URL作为唯一标识）
                exists = any(t.download_url == download_url for t in self.pending_torrents)
                
                if not exists:
                    torrent = PendingTorrent(
                        download_url=download_url,
                        release_name=release_name,
                        category=category
                    )
                    self.pending_torrents.append(torrent)
                    logger.info(f"添加待处理种子：{release_name}")
                else:
                    logger.debug(f"种子已在待处理列表中：{release_name}")
                    
        except Exception as e:
            logger.error(f"添加种子失败：{release_name}，错误：{e}")
            

                
    def _update_instance_status(self) -> None:
        """更新所有实例的状态信息"""
        with self.instances_lock:
            for instance in self.instances:
                if instance.is_connected:
                    self._update_single_instance(instance)
                    
    def _update_single_instance(self, instance: InstanceInfo) -> None:
        """更新单个实例的状态信息"""
        def _try_update_instance():
            """尝试更新实例状态的内部函数"""
            maindata = instance.client.sync_maindata()
            self._update_instance_metrics(instance, maindata)
            self._process_instance_announces(instance, maindata)
        
        # 第一次尝试
        try:
            _try_update_instance()
            return
        except Exception as e:
            logger.warning(f"更新实例状态失败：{instance.name}，错误：{e}，等待5秒后重试")
            time.sleep(5)
        
        # 第二次尝试
        try:
            _try_update_instance()
            logger.info(f"实例 {instance.name} 重试成功")
        except Exception as e2:
            logger.error(f"重试后仍然失败：{instance.name}，错误：{e2}，标记为断开连接")
            instance.is_connected = False
            instance.last_update = datetime.now()
                    
    def _update_instance_metrics(self, instance: InstanceInfo, maindata: dict) -> None:
        """使用sync/maindata的结果更新单个实例的状态信息"""
        server_state = maindata.get('server_state', {})
        
        # 从server_state获取全局统计信息和硬盘空间
        instance.upload_speed = server_state.get('up_info_speed', 0) / BYTES_TO_KB
        instance.download_speed = server_state.get('dl_info_speed', 0) / BYTES_TO_KB
        instance.free_space = server_state.get('free_space_on_disk', 0)
        
        # 从torrents信息计算活跃下载数
        all_torrents = maindata.get('torrents', {}).values()
        instance.active_downloads = len([t for t in all_torrents if t.state == 'downloading'])
        
        instance.last_update = datetime.now()
        instance.success_metrics_count += 1  # 成功获取统计信息，计数器加1
        
        # 每30次成功更新时检查一次流量信息
        if instance.success_metrics_count % 30 == 0:
            self._check_instance_traffic(instance)
        
        logger.debug(f"实例 {instance.name}：" 
                   f"上传={instance.upload_speed:.1f}KB/s，"
                   f"下载={instance.download_speed:.1f}KB/s，"
                   f"活跃下载={instance.active_downloads}，"
                   f"空间={instance.free_space/BYTES_TO_GB:.1f}/{instance.reserved_space/BYTES_TO_GB:.1f}GB，"
                   f"更新={instance.success_metrics_count}，"
                   f"历史任务={instance.total_added_tasks_count}")


    def _check_instance_traffic(self, instance: InstanceInfo) -> None:
        """检查实例的流量信息"""
        if not instance.traffic_check_url:
            return
            
        try:
            response = requests.get(instance.traffic_check_url, timeout=5)
            response.raise_for_status()
            traffic_data = response.json()
            
            # 获取出站流量，从MB转换为字节
            try:
                traffic_out_mb = traffic_data.get('out', 0.0)
                instance.traffic_out = int(float(traffic_out_mb) * 1024 * 1024)  # MB转字节
                
                # 检查是否流量被限流
                traffic_throttled = traffic_data.get('trafficThrottled', False)
                if traffic_throttled:
                    instance.traffic_out = 9999 * BYTES_TO_TB  # 设置为极大值，确保在流量检查时被过滤
                    logger.warning(f"实例 {instance.name} 流量被限流，设置流量为极大值以避免被选择")
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"实例 {instance.name} 流量数据转换失败：{e}，设置为0")
                instance.traffic_out = 0
            
            logger.debug(f"更新实例 {instance.name} 流量信息：出站流量={instance.traffic_out/BYTES_TO_GB:.2f}GB，限制={instance.traffic_limit/BYTES_TO_GB:.2f}GB")
            
        except Exception as e:
            logger.warning(f"获取实例 {instance.name} 流量信息失败：{e}")
            instance.traffic_out = 0
    
    def _is_traffic_within_limit(self, instance: InstanceInfo) -> bool:
        """检查实例的流量是否在限制范围内"""
        # 如果出站流量为0（未检查或检查失败），认为流量未超出
        if instance.traffic_out == 0:
            return True
        
        # 如果没有设置流量限制，认为流量未超出
        if instance.traffic_limit == 0:
            return True
            
        # 比较出站流量和流量限制
        within_limit = instance.traffic_out < instance.traffic_limit
        
        if not within_limit:
            logger.warning(f"实例 {instance.name} 流量超限：出站流量={instance.traffic_out/BYTES_TO_GB:.2f}GB，限制={instance.traffic_limit/BYTES_TO_GB:.2f}GB")
        
        return within_limit
                   
    def _process_instance_announces(self, instance: InstanceInfo, maindata: dict) -> None:
        """处理实例的种子汇报检查"""
        # 如果debug_add_stopped为True，直接返回，不做任何处理
        if self.config.get('debug_add_stopped', False):
            return
            
        # 如果快速汇报开关未启用，直接返回，不做任何处理
        if not self.config.get('fast_announce_enabled', False):
            return

        max_retries = self.config.get('max_announce_retries', 12)
        error_keywords = ["unregistered", "not registered", "not found", "not exist"]
        current_time = datetime.now()

        all_torrents_items = maindata.get('torrents', {}).items()

        for torrent_hash, torrent in all_torrents_items:
            age_seconds = (current_time - datetime.fromtimestamp(torrent.added_on)).total_seconds()
            is_completed = torrent.progress == 1.0

            # 条件1：如果种子已完成或添加超过2分钟，则确保其已从监控列表中移除，并跳过
            if (is_completed and age_seconds > 60) or age_seconds > 140 or age_seconds < 2:
                if torrent_hash in self.announce_retry_counts:
                    del self.announce_retry_counts[torrent_hash]
                    if is_completed:
                        reason = "已完成"
                    elif age_seconds > 120:
                        reason = "超过2分钟"
                    else:
                        reason = "添加时间小于2秒"
                    logger.debug(f"停止汇报监控: {torrent.name} (原因: {reason})")
                continue
                
            # 检查种子分类是否在快速汇报黑名单中
            blacklist = self.config.get('fast_announce_category_blacklist', [])
            if blacklist and hasattr(torrent, 'category') and torrent.category in blacklist:
                # 如果种子分类在黑名单中，从监控列表中移除并跳过
                if torrent_hash in self.announce_retry_counts:
                    del self.announce_retry_counts[torrent_hash]
                    logger.debug(f"跳过快速汇报: {torrent.name} (分类 '{torrent.category}' 在黑名单中)")
                continue
                
            # 条件2：如果种子未完成且未超过2分钟，则进行汇报检查
            # 初始化或递增重试计数器
            if torrent_hash not in self.announce_retry_counts:
                self.announce_retry_counts[torrent_hash] = 0
            
            # 每次进入函数时递增计数器
            self.announce_retry_counts[torrent_hash] += 1
            current_retries = self.announce_retry_counts[torrent_hash]
            
            logger.debug(f"汇报检查: {torrent.name} (第{current_retries}次检查，最大{max_retries}次)")

            # 检查是否达到1分钟或者2分钟且种子仍未完成，如果是则强制汇报
            fast_interval = self.config.get('fast_announce_interval', 3)
            first_force_announce = int(60 / fast_interval)
            second_force_announce = int(120 / fast_interval)
            if (current_retries == first_force_announce or current_retries == second_force_announce) and not is_completed:
                logger.info(f"达到特定次数({current_retries})且种子未完成，强制汇报: {torrent.name}")
                self._announce_torrent(instance, torrent, torrent_hash, f"强制汇报(第{current_retries}次检查)")
                continue

            # 如果还没到最大重试次数，继续正常的汇报条件检查
            if current_retries < max_retries:
                # 检查汇报条件
                needs_announce = False
                reason = []

                try:
                    # 1. 检查Tracker状态
                    trackers = instance.client.torrents_trackers(torrent_hash=torrent_hash)
                    
                    # Filter out non-HTTP trackers and special trackers like DHT, PEX, LSD
                    filtered_trackers = []
                    for t in trackers:
                        if t.url.lower() in ('dht', 'pex', 'lsd'):
                            continue
                        if not t.url.startswith(('http://', 'https://')):
                            continue
                        filtered_trackers.append(t)

                    if not filtered_trackers:
                        logger.info(f"[{instance.name}] Announce check for '{torrent.name}': No valid HTTP trackers found, skipping.")
                        continue

                    all_trackers_failed = all(t.status in [1, 3, 4] for t in filtered_trackers)
                    has_error_keyword = any(keyword in t.msg.lower() for t in filtered_trackers for keyword in error_keywords)

                    if all_trackers_failed:
                        needs_announce = True
                        reason.append("所有tracker状态异常")
                    if has_error_keyword:
                        needs_announce = True
                        reason.append("发现tracker错误信息")

                    # 2. 检查Peer数量
                    if torrent.progress < 0.8 and torrent.num_leechs < 2:
                        needs_announce = True
                        reason.append(f"Peer数量不足({torrent.num_leechs})")

                    # 执行汇报
                    if needs_announce:
                        self._announce_torrent(instance, torrent, torrent_hash, ", ".join(reason))

                except Exception as e:
                    logger.warning(f"处理 {torrent.name} 的汇报时出错: {e}")

    def _announce_torrent(self, instance: InstanceInfo, torrent: any, torrent_hash: str, reason: str) -> None:
        """对单个种子执行announce"""
        try:
            instance.client.torrents_reannounce(torrent_hashes=torrent_hash)
            current_retries = self.announce_retry_counts.get(torrent_hash, 0)
            logger.info(f"触发汇报: {torrent.name} (原因: {reason}) | "
                        f"尝试次数: {current_retries}")
        except Exception as e:
            logger.warning(f"汇报失败: {torrent.name}，错误: {e}")


    def _get_primary_sort_value(self, instance: InstanceInfo) -> float:
        """获取主要排序因素的值"""
        primary_sort_key = self.config.get('primary_sort_key', DEFAULT_PRIMARY_SORT_KEY)
        
        if primary_sort_key == 'upload_speed':
            return instance.upload_speed
        elif primary_sort_key == 'download_speed':
            return instance.download_speed
        elif primary_sort_key == 'active_downloads':
            return float(instance.active_downloads)
        else:
            # 默认使用上传速度
            return instance.upload_speed
        
    def _select_best_instance(self) -> Optional[InstanceInfo]:
        """选择最佳的实例来分配新任务"""
        with self.instances_lock:
            available_instances = [
                instance for instance in self.instances 
                if instance.is_connected and 
                instance.new_tasks_count < self.config['max_new_tasks_per_instance'] and
                instance.free_space > instance.reserved_space and
                self._is_traffic_within_limit(instance)
            ]
            
            if not available_instances:
                return None
                
            # 按可配置算法排序：主要因素（小值优先），次要因素是任务计数（小值优先），第三因素是硬盘空间（大值优先）
            available_instances.sort(key=lambda x: (
                self._get_primary_sort_value(x),  # 主要因素：小值优先
                x.total_added_tasks_count,        # 次要因素：已添加任务计数小的优先
                -x.free_space                     # 第三因素：硬盘空间大的优先（使用负号）
            ))
            
            selected = available_instances[0]
            primary_sort_key = self.config.get('primary_sort_key', DEFAULT_PRIMARY_SORT_KEY)
            primary_value = self._get_primary_sort_value(selected)
            
            logger.debug(f"选择实例 {selected.name}：" 
                        f"{SUPPORTED_SORT_KEYS[primary_sort_key]}={primary_value:.1f}，"
                        f"已添加任务数={selected.total_added_tasks_count}，"
                        f"空闲空间={selected.free_space/BYTES_TO_GB:.1f}GB，"
                        f"保留空间={selected.reserved_space/BYTES_TO_GB:.1f}GB，"
                        f"流量={selected.traffic_out/BYTES_TO_GB:.2f}/{selected.traffic_limit/BYTES_TO_GB:.2f}GB")
            
            return selected
            
    def _add_torrent_to_instance(self, instance: InstanceInfo, torrent: PendingTorrent) -> bool:
        """将torrent添加到指定实例"""
        try:
            add_params = {'urls': torrent.download_url}
            
            # 设置分类
            if torrent.category:
                add_params['category'] = torrent.category
                logger.info(f"为种子设置分类：{torrent.release_name} -> {torrent.category}")
                
            # 根据配置决定是否将种子添加为暂停状态（用于调试）
            if self.config.get('debug_add_stopped', False):
                add_params['is_stopped'] = True
                logger.info(f"调试模式：种子将以暂停状态添加 - {torrent.release_name}")

            result = instance.client.torrents_add(**add_params)
            
            if result and result.startswith('Ok'):
                instance.new_tasks_count += 1
                instance.total_added_tasks_count += 1  # 增加累计任务计数
                log_msg = f"成功添加种子到实例 {instance.name}：{torrent.release_name}"
                if torrent.category:
                    log_msg += f"（分类：{torrent.category}）"
                logger.info(log_msg)
                return True
            else:
                logger.error(f"添加种子失败 - 实例：{instance.name}，种子：{torrent.release_name}，结果：{result}")
                return False
                
        except Exception as e:
            logger.error(f"添加种子到实例失败 - 实例：{instance.name}，种子：{torrent.release_name}，错误：{e}")
            return False
            
    def _process_torrents(self) -> None:
        """处理待分配的torrent URL"""
        with self.pending_torrents_lock:
            if not self.pending_torrents:
                return
                
            # 处理所有待处理的torrent URL
            for torrent in self.pending_torrents[:]:  # 使用切片避免修改列表时的问题
                instance = self._select_best_instance()
                if instance:
                    if self._add_torrent_to_instance(instance, torrent):
                        self.pending_torrents.remove(torrent)
                else:
                    logger.warning("没有可用的实例来分配新任务，清空待处理队列")
                    self.pending_torrents.clear()
                    break

    def _reset_task_counters(self) -> None:
        """重置任务计数器（每轮处理完成后）"""
        with self.instances_lock:
            for instance in self.instances:
                instance.new_tasks_count = 0
                
    def _log_status_summary(self) -> None:
        """记录状态摘要信息"""
        with self.instances_lock:
            total_instances = len(self.instances)
            connected_count = sum(1 for i in self.instances if i.is_connected)
            disconnected_instances = [i.name for i in self.instances if not i.is_connected]
            
            status_msg = f"实例状态: {connected_count}/{total_instances} 连接正常"
            if disconnected_instances:
                status_msg += f", 断开连接: {', '.join(disconnected_instances)}"
            
            logger.debug(status_msg)
                
    def status_update_thread(self) -> None:
        """状态更新线程"""
        # 等待初始连接完成（最多10秒）
        logger.info("⏳ 等待实例初始化...")
        for i in range(10):
            time.sleep(1)
            with self.instances_lock:
                connected = sum(1 for inst in self.instances if inst.is_connected)
                if connected > 0:
                    logger.info(f"✓ 已连接 {connected}/{len(self.instances)} 个实例")
                    break
        
        logger.info("🔄 状态监控线程开始运行")
        
        while True:
            try:
                self._update_instance_status()
                self._log_status_summary()
                self._check_and_schedule_reconnects()
                              
                # 根据是否有待重试的汇报任务来调整检查频率
                fast_interval = self.config['fast_announce_interval']
                if self.announce_retry_counts:
                    time.sleep(fast_interval)  # 有待重试任务时的快速检查频率
                else:
                    time.sleep(fast_interval * 2)  # 正常情况下的检查频率
                
            except Exception as e:
                logger.error(f"状态更新线程错误：{e}")
                time.sleep(ERROR_RETRY_SLEEP)
                
    def task_processor_thread(self) -> None:
        """任务处理线程"""
        logger.info("📦 任务处理线程开始运行")
        
        while True:
            try:
                # 记录当前待处理的种子数量（更及时的信息）
                with self.pending_torrents_lock:
                    pending_count = len(self.pending_torrents)
                
                if pending_count > 0:
                    logger.debug(f"处理 {pending_count} 个待分配的种子")
                
                self._process_torrents()
                self._reset_task_counters()
                time.sleep(TASK_PROCESSOR_SLEEP)
                
            except Exception as e:
                logger.error(f"任务处理线程错误：{e}")
                time.sleep(ERROR_RETRY_SLEEP)
                
    def run(self) -> None:
        """运行负载均衡器"""
        logger.info("="*70)
        logger.info("  qBittorrent 负载均衡器启动")
        logger.info("="*70)
        
        # 显示配置摘要
        logger.info(f"📋 配置摘要:")
        logger.info(f"   • 实例数量: {len(self.instances)}")
        logger.info(f"   • Webhook端口: {self.config.get('webhook_port', 5000)}")
        logger.info(f"   • API端口: {self.api_port}")
        logger.info(f"   • 日志目录: {self.config.get('log_dir', './logs')}")

        self.config_watcher.start()
        
        # 启动状态更新线程
        status_thread = threading.Thread(target=self.status_update_thread, daemon=True)
        status_thread.start()
        logger.info("✓ 状态更新线程已启动")

        # 启动状态更新线程
        status_thread = threading.Thread(target=self.status_update_thread, daemon=True)
        status_thread.start()
        logger.info("✓ 状态更新线程已启动")
        
        # 启动任务处理线程
        task_thread = threading.Thread(target=self.task_processor_thread, daemon=True)
        task_thread.start()
        logger.info("✓ 任务处理线程已启动")
        
        logger.info("="*70)
        logger.info("🚀 所有服务已启动，系统运行中...")
        logger.info("💡 提示: 使用 Ctrl+C 停止程序")
        logger.info("="*70)
        
        try:
            # 主线程保持运行
            while True:
                time.sleep(DEFAULT_SLEEP_TIME)
        except KeyboardInterrupt:
            logger.info("\n" + "="*70)
            logger.info("🛑 收到停止信号，正在关闭...")
            logger.info("="*70)
            
            # 🆕 停止配置监控
            if self.config_watcher:
                self.config_watcher.stop()
                logger.info("✓ 配置监控已停止")
            
            if self.webhook_server:
                self.webhook_server.stop()
                logger.info("✓ Webhook服务器已停止")
            
            logger.info("✓ 程序已安全退出")


def main() -> int:
    """主函数"""
    try:
        balancer = QBittorrentLoadBalancer()
        balancer.run()
        return 0
    except Exception as e:
        logger.error(f"程序启动失败：{e}")
        return 1


if __name__ == "__main__":
    exit(main())