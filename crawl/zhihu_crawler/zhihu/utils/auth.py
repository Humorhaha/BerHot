"""
知乎自动登录工具
使用 Playwright 自动打开浏览器获取 Cookie
"""

import json
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


def get_zhihu_cookies(headless: bool = False, timeout: int = 120) -> dict[str, str]:
    """
    使用 Playwright 自动获取知乎登录 Cookie
    
    Args:
        headless: 是否无头模式（False=显示浏览器窗口，方便扫码/登录）
        timeout: 等待登录的超时时间（秒）
        
    Returns:
        Cookie 字典
        
    使用示例:
        cookies = get_zhihu_cookies(headless=False)  # 显示浏览器，手动扫码
        print(cookies)
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        
        page = context.new_page()
        
        # 访问知乎首页
        print("打开知乎登录页面...")
        page.goto("https://www.zhihu.com/signin")
        
        # 等待登录完成（通过检查特定元素）
        print("请完成登录（扫码或密码）...")
        print(f"等待登录，超时时间: {timeout}秒")
        
        start_time = time.time()
        logged_in = False
        
        while time.time() - start_time < timeout:
            # 检查是否已登录（通过检查页面元素）
            try:
                # 检查是否存在用户头像或首页 Feed
                if page.locator(".GlobalSideBar-navLink").count() > 0 or \
                   page.locator("[data-za-detail-view-path-module='TopStory']").count() > 0 or \
                   page.locator(".AppHeader-profile").count() > 0:
                    print("检测到已登录！")
                    logged_in = True
                    break
            except:
                pass
            
            time.sleep(1)
        
        if not logged_in:
            print("登录超时，请重试")
            browser.close()
            return {}
        
        # 获取 Cookie
        cookies = context.cookies()
        browser.close()
        
        # 转换为字典格式
        cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
        
        # 过滤关键 Cookie
        essential_cookies = {}
        for key in ["_zap", "d_c0", "z_c0", "q_c1", "capsion_ticket"]:
            if key in cookie_dict:
                essential_cookies[key] = cookie_dict[key]
        
        print(f"成功获取 {len(essential_cookies)} 个关键 Cookie")
        return essential_cookies


def save_cookies(cookies: dict[str, str], path: Path | str = "cookies.json") -> None:
    """保存 Cookie 到文件"""
    path = Path(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)
    print(f"Cookie 已保存到: {path}")


def load_cookies(path: Path | str = "cookies.json") -> dict[str, str]:
    """从文件加载 Cookie"""
    path = Path(path)
    if not path.exists():
        return {}
    
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def cookies_to_string(cookies: dict[str, str]) -> str:
    """将 Cookie 字典转换为字符串格式"""
    return "; ".join([f"{k}={v}" for k, v in cookies.items()])


def auto_get_cookie_string(headless: bool = False, save_path: Path | str | None = "cookies.json") -> str:
    """
    自动获取 Cookie 字符串（一键获取）
    
    Args:
        headless: 是否无头模式
        save_path: 保存路径，None 表示不保存
        
    Returns:
        Cookie 字符串
    """
    # 先尝试加载已有 Cookie
    if save_path:
        existing = load_cookies(save_path)
        if existing:
            print(f"使用已保存的 Cookie: {save_path}")
            return cookies_to_string(existing)
    
    # 获取新 Cookie
    cookies = get_zhihu_cookies(headless=headless)
    
    if not cookies:
        raise RuntimeError("获取 Cookie 失败")
    
    # 保存
    if save_path:
        save_cookies(cookies, save_path)
    
    return cookies_to_string(cookies)


if __name__ == "__main__":
    # 测试
    print("知乎 Cookie 获取工具")
    print("=" * 40)
    
    try:
        cookie_str = auto_get_cookie_string(headless=False)
        print("\n获取到的 Cookie 字符串:")
        print(cookie_str[:200] + "..." if len(cookie_str) > 200 else cookie_str)
    except Exception as e:
        print(f"错误: {e}")
