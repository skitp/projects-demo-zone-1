This project is for building Azure AI Foundry-based coding agents that review, refactor, and optimize code using Microsoft Agent Framework.

### Agent Roles

**Orchestrator Agent**  
Entry point. Receives code or a repo reference, plans the work, delegates to specialists, merges results, and produces a final structured report (issues, severity, suggested changes, optional diffs). Controls sequencing and human-in-the-loop pauses.

**Static Analysis Agent**  
Handles automated checks: linters, formatters, complexity metrics, unused code, and basic type issues. Preferred tools: code interpreter or external runners (ruff, pylint, eslint, mypy, etc.).

**Review Agent**  
Deeper semantic analysis: logic bugs, security issues, performance problems, and maintainability concerns. Can invoke scanners or profiling tools when available.

**Refactor & Optimize Agent**  
Generates concrete improved code, diffs, and explanations. Can iterate with the Review Agent on the proposed changes.

**Validation Agent** (optional but useful)  
Runs tests, type checks, or simple execution to confirm suggested changes preserve behavior. Feeds pass/fail results back to the Orchestrator.

**Suggested flow patterns**
- Sequential pipeline: Analysis → Review → Refactor → Validation
- Parallel checks: Analysis + Review run together, then results feed Refactor
- Human-in-the-loop: After suggestions are ready, pause for approval before applying changes

### Starter Project Structure

```
ai_agents/
├── README.md
├── azure.yaml                  # azd config for Hosted Agent deployment
├── requirements.txt
├── .env.example
├── src/
│   ├── main.py                 # Hosted agent entry (ResponsesHostServer)
│   ├── agents/
│   │   ├── orchestrator.py
│   │   ├── analysis.py
│   │   ├── review.py
│   │   ├── refactor.py
│   │   └── validation.py
│   ├── tools/
│   │   ├── code_tools.py       # linter/runner wrappers
│   │   └── git_tools.py        # optional
│   ├── workflows/
│   │   └── review_workflow.py  # Agent Framework workflow
│   └── prompts/
│       └── system_prompts.py
├── samples/
│   └── sample_code/            # evaluation examples
└── tests/
```

**Highest-value files to create first**
- `src/workflows/review_workflow.py` – the multi-agent flow
- `src/main.py` – hosts the workflow as a Foundry Hosted Agent
- Prompt definitions with clear output formats (structured JSON or consistent markdown)
- Minimal tool wrappers for analysis

I can generate the starter skeleton, the workflow definition, agent prompts, or the deployment config next.

Starter skeleton generated for the code review / refactor / optimize agents.

**Created structure** (in the project artifacts):

- `README.md` — overview and quick-start
- `requirements.txt` — Agent Framework + Foundry hosting deps
- `.env.example` — Foundry endpoint + model
- `azure.yaml` — azd deployment config for a Hosted Agent (Responses protocol)
- `src/prompts.py` — system prompts for Analysis, Review, and Refactor agents
- `src/workflows/review_workflow.py` — sequential WorkflowBuilder (Analysis → Review → Refactor)
- `src/main.py` — hosts the workflow as an agent via `ResponsesHostServer`
- `samples/sample_code.py` — tiny example to test against

**How the workflow works**
1. Analysis Agent runs first (style, basic issues, complexity).
2. Review Agent receives that output and does deeper semantic / security / performance review.
3. Refactor Agent produces improved code + explanation + diff.
4. The whole workflow is exposed as a single agent and hosted with `ResponsesHostServer` so it deploys cleanly as a Foundry Hosted Agent.

**Next steps you can take**
- Fill `.env` with your Foundry project endpoint and model deployment name.
- Run locally: `python -m src.main` (after installing requirements and `az login`).
- Deploy: `azd auth login && azd deploy`.

I can next:
- Add tool wrappers (code interpreter or simple local linter runners),
- Expand to parallel analysis + review,
- Add a Validation agent,
- Or tighten the prompts and output schemas.

Tool wrappers added.

**New / updated files**
- `src/tools/code_tools.py` — local tools:
  - `run_ruff` — runs ruff on stdin code
  - `run_pylint` — runs pylint on stdin code
  - `measure_complexity` — simple line/branch heuristic
- `src/workflows/review_workflow.py` — updated so:
  - **Analysis Agent** gets all three local tools + Foundry Code Interpreter
  - **Review Agent** and **Refactor Agent** get the Code Interpreter
- `requirements.txt` — added `ruff` and `pylint`
- `README.md` — documents the tools

**How it works**
- Local tools use the `@tool` decorator and run via `subprocess` (they gracefully report if the binary is missing).
- Code Interpreter is attached via `FoundryChatClient.get_code_interpreter_tool()` so agents can execute Python in Foundry’s sandboxed environment.
- Agents are instructed to use the tools when helpful.

**To try it**
1. `pip install -r requirements.txt`
2. Ensure `ruff` and `pylint` are on the PATH (or they’ll return a friendly message).
3. Run the agent and send it a Python snippet.

I can next:
- Add more tools (radon for real complexity, black/formatter, security scanners),
- Wire tool results more tightly into the structured JSON output,
- Or add a Validation agent that runs the refactored code.

Which would you like?
