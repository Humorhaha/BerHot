"""
agent_tree/llm.py — LLM 调用封装层
=====================================

职责：
  - 统一的 JSON 结构化输出（带自动重试和 fallback）
  - API 调用计数，供最终报告统计成本
  - 支持 async（批量 Agent 并行运行的基础）
"""

from __future__ import annotations

import json
import asyncio
from typing import Any, Type, TypeVar
from openai import AsyncOpenAI, AuthenticationError, PermissionDeniedError
from pydantic import BaseModel
from loguru import logger

T = TypeVar("T", bound=BaseModel)


class FatalLLMAuthError(RuntimeError):
    """认证/鉴权失败时抛出的不可重试错误。"""

# 全局调用计数器（线程不安全，但 asyncio 单线程足够）
_api_call_count: int = 0


def get_api_call_count() -> int:
    return _api_call_count


def reset_api_call_count() -> None:
    global _api_call_count
    _api_call_count = 0


async def llm_json(
    client: AsyncOpenAI,
    system: str,
    user: str,
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
    max_retries: int = 3,
    fail_fast_on_auth: bool = False,
) -> dict[str, Any]:
    """
    调用 LLM 并强制返回 JSON dict。
    失败时重试，最终仍失败则返回空 dict 并记录错误。
    """
    global _api_call_count

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

    auth_failed = False
    for attempt in range(max_retries):
        try:
            _api_call_count += 1
            resp = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                response_format={"type": "json_object"},
                max_tokens=2048,
            )
            content = resp.choices[0].message.content or "{}"
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse failed (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
        except (AuthenticationError, PermissionDeniedError) as e:
            # 401/403 通常不是瞬时错误，重复重试只会放大失败噪声与成本。
            logger.error(f"LLM auth failed: {type(e).__name__}: {e}")
            if fail_fast_on_auth:
                raise FatalLLMAuthError(str(e)) from e
            auth_failed = True
            break
        except Exception as e:
            logger.error(f"LLM call failed (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2.0 * (attempt + 1))

    if auth_failed:
        logger.error("LLM call aborted due to auth failure, returning empty result")
        return {}

    logger.error("All LLM retries exhausted, returning empty result")
    return {}


async def llm_parse(
    client: AsyncOpenAI,
    system: str,
    user: str,
    schema: Type[T],
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
) -> T | None:
    """
    调用 LLM 并尝试解析为 Pydantic 模型。
    解析失败返回 None。
    """
    raw = await llm_json(client, system, user, model=model, temperature=temperature)
    if not raw:
        return None
    try:
        return schema.model_validate(raw)
    except Exception as e:
        logger.warning(f"Pydantic validation failed for {schema.__name__}: {e}")
        return None
