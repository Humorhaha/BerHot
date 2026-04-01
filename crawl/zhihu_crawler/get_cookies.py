#!/usr/bin/env python3
"""
知乎 Cookie 自动获取工具
无需手动复制，自动打开浏览器获取登录 Cookie

使用方法:
    python get_cookies.py                    # 交互模式（显示浏览器窗口）
    python get_cookies.py --headless         # 无头模式（需要已登录的浏览器状态）
    python get_cookies.py --save cookies.json
    
获取到的 Cookie 可以直接用于爬虫:
    scrapy crawl zhihu_question -s ZHIHU_COOKIES="$(cat cookies.txt)"
"""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="自动获取知乎登录 Cookie",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 交互模式（推荐首次使用）
    python get_cookies.py
    
    # 指定保存路径
    python get_cookies.py --save my_cookies.json
    
    # 查看 Cookie 字符串
    python get_cookies.py --print-only
        """,
    )
    
    parser.add_argument(
        "--headless",
        action="store_true",
        help="无头模式（不显示浏览器窗口）",
    )
    parser.add_argument(
        "--save",
        default="cookies.json",
        help="Cookie 保存路径（默认: cookies.json）",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="登录超时时间（秒，默认 120）",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="仅打印 Cookie 字符串，不保存文件",
    )
    parser.add_argument(
        "--export-txt",
        action="store_true",
        help="同时导出为 .txt 文件（便于命令行使用）",
    )
    
    args = parser.parse_args()
    
    try:
        from zhihu.utils.auth import auto_get_cookie_string, save_cookies, load_cookies
    except ImportError as e:
        print(f"导入错误: {e}")
        print("请确保已安装 playwright: uv add playwright")
        print("并安装浏览器: playwright install chromium")
        sys.exit(1)
    
    save_path = None if args.print_only else args.save
    
    try:
        print("启动浏览器获取 Cookie...")
        print("请完成知乎登录（扫码或密码）")
        print("-" * 40)
        
        cookie_str = auto_get_cookie_string(
            headless=args.headless,
            save_path=save_path
        )
        
        print("-" * 40)
        
        if args.print_only:
            print("\nCookie 字符串:")
            print(cookie_str)
        else:
            print(f"\nCookie 已保存到: {args.save}")
            
            # 同时导出 txt 文件
            if args.export_txt:
                txt_path = Path(args.save).with_suffix(".txt")
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(cookie_str)
                print(f"Cookie 字符串已保存到: {txt_path}")
            
            print("\n使用方式:")
            print(f'  scrapy crawl zhihu_question -s ZHIHU_COOKIES="{cookie_str[:50]}..."')
            print("\n或在 settings.py 中设置:")
            print(f'  ZHIHU_COOKIES = "{cookie_str[:50]}..."')
    
    except KeyboardInterrupt:
        print("\n\n用户取消")
        sys.exit(1)
    except Exception as e:
        print(f"\n错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
