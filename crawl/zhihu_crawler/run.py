#!/usr/bin/env python3
"""
知乎爬虫启动脚本
简化 Scrapy 命令的调用

使用方法:
    python run.py question --ids "19551997,585901765"
    python run.py question --file config/questions.yaml
    python run.py topic --ids "19554298"
    python run.py user --tokens "excited-vczh"
    python run.py user --file config/users.yaml --max-per-user 50
    
    # 自动登录模式（无需手动提供 Cookie）
    python run.py question --ids "19551997" --auto-login
"""

import argparse
import subprocess
import sys
from pathlib import Path


def get_cookie_from_auth() -> str:
    """使用 Playwright 自动获取 Cookie"""
    try:
        from zhihu.utils.auth import auto_get_cookie_string
        print("自动获取知乎 Cookie...")
        print("请完成登录（扫码或密码）")
        print("-" * 40)
        return auto_get_cookie_string(headless=False, save_path="cookies.json")
    except ImportError:
        print("错误: 未安装 playwright，无法使用自动登录")
        print("请运行: uv add playwright && playwright install chromium")
        sys.exit(1)


def run_spider(spider_name: str, args: list[str], cookie: str | None = None):
    """运行 Scrapy 爬虫"""
    cmd = ["scrapy", "crawl", spider_name] + args
    
    # 添加 Cookie 设置
    if cookie:
        cmd.extend(["-s", "COOKIES_ENABLED=True"])
        cmd.extend(["-s", f"ZHIHU_COOKIES={cookie}"])
    
    print(f"Running: {' '.join(cmd[:10])}..." if len(cmd) > 10 else f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=False)


def add_common_args(parser):
    """添加通用参数"""
    parser.add_argument(
        "--auto-login",
        action="store_true",
        help="自动登录模式（使用 Playwright 获取 Cookie）",
    )
    parser.add_argument(
        "--cookies",
        help="手动提供 Cookie 字符串（优先级高于 --auto-login）",
    )


def main():
    parser = argparse.ArgumentParser(
        description="知乎爬虫启动脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 按问题 ID 爬取（需要已配置 Cookie）
    python run.py question --ids "19551997,585901765"
    
    # 自动登录模式
    python run.py question --ids "19551997" --auto-login
    
    # 手动提供 Cookie
    python run.py question --ids "19551997" --cookies "_zap=xxx; d_c0=xxx"
    
    # 从配置文件批量爬取
    python run.py question --file config/questions.yaml
    
    # 限制数量
    python run.py question --ids "19551997" --max 100
    
    # 按话题爬取
    python run.py topic --ids "19554298"
    
    # 按用户爬取
    python run.py user --tokens "excited-vczh"
    
    # 只爬回答（不爬文章）
    python run.py user --tokens "excited-vczh" --no-articles
        """,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="爬虫类型")
    
    # question 子命令
    q_parser = subparsers.add_parser("question", help="问题回答爬虫")
    q_parser.add_argument("--ids", help="逗号分隔的问题 ID")
    q_parser.add_argument("--file", help="YAML 配置文件路径")
    q_parser.add_argument("--max", type=int, dest="max_answers", help="每个问题最多爬取的回答数")
    q_parser.add_argument(
        "--no-login",
        action="store_true",
        help="使用免登录 HTML 爬虫（限制：只能获取前 20-50 个回答）",
    )
    add_common_args(q_parser)
    
    # search 子命令（免登录）
    s_parser = subparsers.add_parser("search", help="搜索爬虫（免登录）")
    s_parser.add_argument("--query", required=True, help="搜索关键词")
    s_parser.add_argument("--max", type=int, dest="max_results", default=20, help="最大结果数（默认 20）")
    s_parser.add_argument(
        "--no-login",
        action="store_true",
        help="免登录（此模式固定免登录）",
    )
    
    # topic 子命令
    t_parser = subparsers.add_parser("topic", help="话题爬虫")
    t_parser.add_argument("--ids", help="逗号分隔的话题 ID")
    t_parser.add_argument("--file", help="YAML 配置文件路径")
    t_parser.add_argument("--max", type=int, dest="max_items", help="每个话题最多爬取的内容数")
    add_common_args(t_parser)
    
    # user 子命令
    u_parser = subparsers.add_parser("user", help="用户内容爬虫")
    u_parser.add_argument("--tokens", help="逗号分隔的用户 url_token")
    u_parser.add_argument("--file", help="YAML 配置文件路径")
    u_parser.add_argument("--max-per-user", type=int, help="每个用户最多爬取的内容数")
    u_parser.add_argument("--no-answers", action="store_true", help="不爬取回答")
    u_parser.add_argument("--no-articles", action="store_true", help="不爬取文章")
    add_common_args(u_parser)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # 处理 Cookie
    cookie = None
    if args.cookies:
        cookie = args.cookies
        print("使用命令行提供的 Cookie")
    elif args.auto_login:
        cookie = get_cookie_from_auth()
        print("使用自动登录获取的 Cookie")
    else:
        # 尝试从 cookies.json 加载
        try:
            from zhihu.utils.auth import load_cookies, cookies_to_string
            saved_cookies = load_cookies("cookies.json")
            if saved_cookies:
                cookie = cookies_to_string(saved_cookies)
                print("使用已保存的 Cookie (cookies.json)")
        except:
            pass
    
    spider_args = []
    
    if args.command == "question":
        # 判断是否使用免登录模式
        if args.no_login:
            print("使用免登录模式（HTML 爬虫）")
            print("注意：每个问题只能获取前 20-50 个回答")
            if args.ids:
                spider_args.extend(["-a", f"question_ids={args.ids}"])
            elif args.file:
                spider_args.extend(["-a", f"questions_file={args.file}"])
            else:
                print("Error: 请提供 --ids 或 --file 参数")
                sys.exit(1)
            
            if args.max_answers:
                spider_args.extend(["-a", f"max_answers={args.max_answers}"])
            
            run_spider("zhihu_question_html", spider_args, None)  # 免登录不需要 cookie
        else:
            # 普通模式需要 Cookie
            if not cookie:
                print("警告: 未提供 Cookie，知乎 API 可能返回 403")
                print("建议: 使用 --auto-login 或 --cookies 提供 Cookie")
                print("      或使用 --no-login 启用免登录模式（有限制）")
            
            if args.ids:
                spider_args.extend(["-a", f"question_ids={args.ids}"])
            elif args.file:
                spider_args.extend(["-a", f"questions_file={args.file}"])
            else:
                print("Error: 请提供 --ids 或 --file 参数")
                sys.exit(1)
            
            if args.max_answers:
                spider_args.extend(["-a", f"max_answers={args.max_answers}"])
            
            run_spider("zhihu_question", spider_args, cookie)
    
    elif args.command == "search":
        # 搜索模式固定免登录
        print("使用免登录搜索模式")
        spider_args.extend(["-a", f"query={args.query}"])
        spider_args.extend(["-a", f"max_results={args.max_results}"])
        run_spider("zhihu_search", spider_args, None)
    
    elif args.command == "topic":
        if not cookie:
            print("警告: 未提供 Cookie，知乎 API 可能返回 403")
            print("建议: 使用 --auto-login 或 --cookies 提供 Cookie")
        
        if args.ids:
            spider_args.extend(["-a", f"topic_ids={args.ids}"])
        elif args.file:
            spider_args.extend(["-a", f"topics_file={args.file}"])
        else:
            print("Error: 请提供 --ids 或 --file 参数")
            sys.exit(1)
        
        if args.max_items:
            spider_args.extend(["-a", f"max_items={args.max_items}"])
        
        run_spider("zhihu_topic", spider_args, cookie)
    
    elif args.command == "user":
        if not cookie:
            print("警告: 未提供 Cookie，知乎 API 可能返回 403")
            print("建议: 使用 --auto-login 或 --cookies 提供 Cookie")
        
        if args.tokens:
            spider_args.extend(["-a", f"user_tokens={args.tokens}"])
        elif args.file:
            spider_args.extend(["-a", f"users_file={args.file}"])
        else:
            print("Error: 请提供 --tokens 或 --file 参数")
            sys.exit(1)
        
        if args.max_per_user:
            spider_args.extend(["-a", f"max_per_user={args.max_per_user}"])
        
        if args.no_answers:
            spider_args.extend(["-a", "crawl_answers=0"])
        
        if args.no_articles:
            spider_args.extend(["-a", "crawl_articles=0"])
        
        run_spider("zhihu_user", spider_args, cookie)


if __name__ == "__main__":
    main()
