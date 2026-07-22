"""LLM agent policy — plays the gym via the OpenMesh-bench LLM API (OpenRouter).

An OpenAI-shape client pointed at OpenRouter. The agent reads packet["text"]
(world state as-of the cursor + available tools + the task), optionally emits a
tool call as JSON, and finally emits a decision. Env-agnostic: it just maps a
packet -> an action string/dict, so `collect_trajectories --agent llm` yields
real LLM trajectories.
"""
from __future__ import annotations

import json
import os
import re

# short name -> OpenRouter id (from openmesh-bench registry)
MODELS = {
    "gpt-5.5": "openai/gpt-5.5", "gpt-5.4": "openai/gpt-5.4",
    "claude-opus-4.7": "anthropic/claude-opus-4.7",
    "gemini-3.1-pro": "google/gemini-3.1-pro-preview",
    "kimi-k2.6": "moonshotai/kimi-k2.6", "deepseek-v4-pro": "deepseek/deepseek-v4-pro",
    "claude-sonnet-4.6": "anthropic/claude-sonnet-4.6",
    "gemini-3-flash": "google/gemini-3-flash-preview",
    "qwen3.6-plus": "qwen/qwen3.6-plus", "minimax-m2.5": "minimax/minimax-m2.5",
    "deepseek-v4-flash": "deepseek/deepseek-v4-flash",
}

DECISIONS = {
    "data_approval": (["approve", "reject"], "approve"),
    "trading": (["long", "short", "flat"], "flat"),
    "forecast": (["up", "down"], "up"),
}

SYSTEM = (
    "You are a financial agent operating in a point-in-time environment. You only "
    "see information knowable as of the given date (no future data). You may pull "
    "more of the world by replying with EXACTLY one JSON object like "
    '{"tool":"NAME","args":{...}} using an available tool (respecting your budget). '
    "When you have enough information, output ONLY the final decision word for the "
    "task (nothing else). Do not explain."
)


class LLMAgent:
    def __init__(self, model: str = "gemini-3-flash", max_tool_calls: int = 2,
                 temperature: float = 0.7):
        from openai import OpenAI
        self.model_id = MODELS.get(model, model)
        self.max_tool_calls = max_tool_calls
        self.temperature = temperature
        self.client = OpenAI(
            api_key=os.environ["OPENROUTER_API_KEY"],
            base_url=os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
            default_headers={"HTTP-Referer": os.environ.get("OPENMESH_APP_URL", "https://openmesh.ai"),
                             "X-Title": "dot-world-state-lake"},
        )
        self._step = None
        self._tool_calls = 0

    def _decision_from_text(self, text: str, task: str):
        words, default = DECISIONS.get(task, (["approve", "reject"], "approve"))
        low = text.lower()
        for w in words:
            if re.search(rf"\b{w}\b", low):
                return w
        return default

    def __call__(self, packet: dict) -> str | dict:
        task = packet["task"]
        # reset per-step tool counter
        if packet.get("t") != self._step:
            self._step, self._tool_calls = packet.get("t"), 0

        budget = packet.get("tool_budget_left", 0)
        force_decide = budget <= 0 or self._tool_calls >= self.max_tool_calls

        prompt = packet["text"]
        if force_decide:
            words = DECISIONS.get(task, (["approve", "reject"], ""))[0]
            prompt += f"\n\n(You must DECIDE now — reply with exactly one of: {', '.join(words)}.)"

        try:
            resp = self.client.chat.completions.create(
                model=self.model_id, temperature=self.temperature, max_tokens=200,
                messages=[{"role": "system", "content": SYSTEM},
                          {"role": "user", "content": prompt}])
            out = (resp.choices[0].message.content or "").strip()
        except Exception:
            return DECISIONS.get(task, (["approve"], "approve"))[1]

        if not force_decide:
            m = re.search(r'\{[^{}]*"tool"[^{}]*\}', out)
            if m:
                try:
                    call = json.loads(m.group(0))
                    if call.get("tool"):
                        self._tool_calls += 1
                        return call
                except Exception:
                    pass
        return self._decision_from_text(out, task)
