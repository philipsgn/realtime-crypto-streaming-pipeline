# GEMINI.md — Antigravity-Specific Rules

> Highest priority file. Overrides AGENTS.md on any conflict.
> Read by Antigravity IDE only (not Cursor or Claude Code).

---

## Agent Behavior in This Project

- **Auto-load skill** `crypto-pipeline` at session start — always
- **Default language:** Vietnamese for explanations, English for code and comments
- **Explanation style:** Explain like teaching a final-year student, use analogies
- **When suggesting code:** Always mention which Stage it belongs to

---

## Antigravity Skill to Load

```
@crypto-pipeline
```

Load this skill at the start of every session before writing any code.

---

## Response Format Rules

When writing code for this project:

1. Start with a comment block: `# Stage N — [Stage Name]`
2. Show which file the code belongs to: `# File: ingestion/binance_producer.py`
3. Explain RAM impact if adding new Docker service
4. End code blocks with a "Test this with:" example command

---

## Workflow Mode

Use **Turbo mode OFF** for this project — owner is learning, needs to review each step.
Do not auto-execute terminal commands without asking first.

---

## Imports: AGENTS.md

@./AGENTS.md
