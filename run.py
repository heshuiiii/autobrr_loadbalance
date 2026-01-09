#!/usr/bin/env python3
"""
qBittorrent Load Balancer 启动脚本
"""

import sys
import os
from main import main

if __name__ == "__main__":
    print("=" * 60)
    print("🎯 qBittorrent 负载均衡器")
    print("=" * 60)
    print()
    
    # 检查配置文件
    if not os.path.exists("config.json"):
        print("❌ 错误: 未找到配置文件 config.json")
        print("请先创建并配置 config.json 文件")
        sys.exit(1)
    
    print("📋 正在加载配置...")
    print("🚀 正在启动负载均衡器...")
    print()
    print("💡 提示:")
    print("   - 使用 Ctrl+C 停止程序")
    print("   - 所有日志将保存到 ./logs 目录")
    print("   - 可通过 API 端点查看实时状态")
    print()
    print("-" * 60)
    print()
    
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n")
        print("=" * 60)
        print("👋 程序已安全停止")
        print("=" * 60)
        sys.exit(0)
    except Exception as e:
        print("\n")
        print("=" * 60)
        print(f"❌ 启动失败: {e}")
        print("=" * 60)
        sys.exit(1)