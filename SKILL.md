---
name: external-agents
description: Proactively delegate suitable substantial subtasks to external API agents and judge their answers, following the user's global preference. Use for debugging, code review, implementation alternatives, evidence analysis, or drafts benefiting from independent critique, even without a manual invocation. Keep trivial tasks local and the coordinator in its Codex ChatGPT session.
---

# External Agents

Run `scripts/external_agents.py` relative to this skill through the terminal. Each external agent has a separate conversation and a bounded tool loop over explicitly selected file snapshots. Agents return analyses and proposed patches. Codex reviews results, applies changes, and verifies locally. External agents do not have shell access, write access, or native Codex tools.

## Automatic use

The user has requested proactive use across local chats, recorded in `~/.codex/AGENTS.md`. Select this skill automatically for substantial tasks where independent analysis is useful. Do not wait for `$external-agents`, a reminder, model selection, or renewed delegation permission. Skip trivial work and tasks with no useful external subtask. Honor a later request to work locally.

Use `council` by default so every adopted external answer receives a fresh external judge evaluation. For `run` batches, send successful worker answers, the original requirements, and the relevant context to a judge explicitly before adopting them. The first answer is provisional; judge failures must not be presented as completed evaluations.

Workers do not inherit chat history. Each brief must include the objective, relevant requirements and constraints, current findings, selected evidence, and a concrete expected deliverable. Provide the context needed for the task while excluding credentials and unrelated private material. Verification and final responsibility stay with the coordinating Codex assistant.

## Setup

Run `python3 <client> status` first. Configuration lives at `~/.config/external-agents/config.json`. Credentials are read from an environment variable or a private file, never from task files or command arguments. Do not change Codex model provider, authentication, or login for this skill.

```sh
python3 <client> configure --base-url https://gate.joingonka.ai/v1 --model zai-org/GLM-5.3-Flash --judge-model zai-org/GLM-5.3-Flash --api chat-completions --key-file ~/.config/external-agents/gonka.key
python3 <client> models
python3 <client> health            # cached per-model health; --reset forgets it
```

Optional config fields `judge_models` (ordered judge fallback chain) and `fallback_models` (ordered substitution pool) can be set with `configure --judge-models ... --fallback-models ...` or added to `config.json` by hand; older configs keep working.

The provider and example model above come from the user's Gonka gateway. Discover available models live rather than hardcoding availability. The client also supports Responses API with `--api responses`; proprietary protocols need another adapter. Verify a new provider with a small live call. A ChatGPT login authenticates the coordinator; the external API uses its own key.

## Tasks and parallel execution

Write UTF-8 JSON using file tools instead of interpolating prompts into shell commands:

```json
[
  {"id":"parser-review", "prompt":"Review this parser for correctness. Cite file aliases and line numbers; propose a unified diff if needed.", "context_files":{"parser.py":"/absolute/project/parser.py"}},
  {"id":"design", "prompt":"Suggest two implementations for this requirement: ...", "context_files":{}}
]
```

An optional task `model` overrides the configured default. Select only relevant files authorized for this provider. Do not send the entire chat, credentials, `.env`, private keys, or unrelated user files. Inspect selected content for secrets; filenames alone cannot detect them. Each task receives only its own immutable alias-to-file snapshots.

```sh
python3 <client> run --tasks /absolute/tasks.json --output-dir /absolute/new-run-dir --workers 3
```

Agents can use `list_context` and `read_context` with line ranges. For a model without function calling, explicitly use `--no-tools`; snapshots are then included directly in the prompt. The batch supervisor uses bounded **Hydra recovery** by default: one primary attempt per task/model; on a retryable failure, launch the remaining fresh workers of the same model in parallel with the identical original prompt and immutable context snapshots. They do not inherit the failed attempt's partial answer or tool history. Replicas never spawn further generations: at most `--max-replicas` attempts per task/model (default 5, hard cap 6), including the primary. The user treats inference as effectively free and prefers speed and reliability over cost, so keep the aggressive defaults; do not set `--max-replicas 1` merely to shorten a council. Reduce it only for an explicit user constraint or an offline diagnostic.

**TTFT hedging** (on by default for streaming providers such as JoinGonka): if the primary has not produced its first streamed token after the hedge delay, two extra replicas start; whichever finishes first wins. The delay is adaptive from the health cache (3x the slowest recent time-to-first-token + 5 s, clamped 8..60 s; 25 s without history). `--ttft-hedge SECONDS` fixes it, `--ttft-hedge 0` disables it.

Retryable failures include HTTP 408/425/429/5xx, connection failures, request deadlines, interrupted/stalled streams, empty answers, and exhausted output/turn budgets. Bad-request/model/configuration, TLS, redirect, filtering and unclassified errors stop that task/model without same-model replicas (in a council the model is then substituted, see below). **HTTP 401/403 is a provider-wide key failure**: the whole run or council stops at once, pending attempts are cancelled, no models are substituted, and the CLI exits 1 with a clear error; fix the key before another run. Model discovery/configuration failures also stop before workers launch. On rate limits the two replacements remain bounded; do not manually repeat failed councils indefinitely.

A slow primary alone is not duplicated by default. `--hedge-after SECONDS` explicitly enables the older latency hedge (0 disables it); any replicas already launched consume the same total ceiling. The first successful result per model goes to the judge; other local waits are stopped. Hydra applies to the fresh judge conversation too, without removing the requirement for a successful judge evaluation. The provider may still process accepted requests after cancellation, and backups/continuations may be billed. Truncated generation can continue within each attempt as described below.

For short contexts, prefer `--no-tools` to send numbered snapshots in one round; use tools when targeted file reads are more useful. In this user's workflow, actively use external workers for suitable independent subtasks and provide the relevant requirements and context. Keep each delegation within the current task's authorized scope.

## Parallel council and judge

For the user's requested Heavy-like workflow, write one task as a JSON object (same fields as above) and run:

```sh
python3 <client> council --task /absolute/task.json --output-dir /absolute/new-council-dir
```

The client discovers all available models, submits the same task independently to each in parallel (up to 32 models, 48 in-flight requests), and writes `first.json` as soon as a successful answer arrives. A separate judge conversation then evaluates successful candidates against the original task and context. The judge always gets a fresh conversation.

Resilience to unavailable models (all on by default):

- **Health cache** `~/.cache/external-agents/health.json` (0600, shared across runs): per-model successes, failures, timeouts, consecutive failures, latency, time-to-first-token and throughput. Two consecutive failures put a model into cooldown (60 s, doubling, max 30 min); cooled models are skipped and used only as last-resort substitutes; healthy, fast models are ordered first. `--ignore-health` disables skipping/reordering, `--no-health` neither reads nor writes, `--health-file PATH` relocates it.
- **Preflight**: a parallel 1-token probe (`--preflight-timeout`, default 15 s) drops dead models before the council instead of waiting out full request deadlines. If every probe fails for a non-auth reason the result is treated as inconclusive and the models are tried anyway. `--no-preflight` skips it.
- **Adaptive deadline**: with at least 3 recorded successes, a model's request deadline becomes `30 + 2*TTFT + 1.5*output_allowance/throughput`, at least 60 s and at most 2x the base timeout (never above an explicit `--timeout`).
- **Substitution**: when a candidate model fails for good (non-retryable error or all replicas exhausted), the next model from the fallback pool (`--fallback-models`, config `fallback_models`, else the rest of the catalog, then cooled models) is launched so the council still reaches `--min-candidates` good answers (default 3; at most 16 substitutions).
- **Quorum**: the judge starts as soon as `--quorum` good answers arrived (default: `--min-candidates`; `0` waits for all); stragglers are cancelled locally with `cancel_reason: quorum_reached`.
- **Judge fallback chain**: `--judge-models A B ...` (strict order) or `--judge-model`, then config `judge_models`/`judge_model`, then the other models. Except for the strict CLI list, judges that are not among the answering candidates are preferred, and dead/cooled models go last. A failed judge (after its replicas) is replaced by the next one (chain of at most 6). `--judges N` (1..4) runs N independent judges in parallel; all completed verdicts go to `judges.json`, the first to `judge.json`. `--judge-max-tokens` controls its separate output allowance (default 0: the discovered model ceiling), so a small candidate output limit does not accidentally truncate the evaluation. `--models MODEL ...` restricts candidates. The judge receives anonymized candidate labels, scores them on correctness/evidence/completeness, and synthesizes a final answer; it may read attached context. This is a local council inspired by the user's request, not an implementation claim about Grok internals.

Read `first.json` for a provisional quick answer and `judge.json` for the reviewed result. `council.json` (also the last row of `summary.json`) records requested/initial models, preflight results, cooldown skips, substitutions (`model`, `replaces`, `reason`), quorum, the judge chain, the final judge model and timings; every attempt row in `summary.json` carries `role`, `status`, `error`, `hedge_reason`, `substitutes_for`, `substitution_reason` and `cancel_reason`. Council status is `completed` (judge succeeded and enough good candidates), `partial`, `judge_failed`, `failed` or `auth_failed`; only `completed` exits 0. Do not submit a duplicate council while its terminal process is running. If some candidates fail, report partial coverage. If all fail, the judge is not called. A judge failure does not turn candidate answers into a judged success.

## Results and limits

Per-task `<id>.json` contains status, answer, call count, reported usage, tool history, continuation count, terminal reasons, actual output allowance, advertised context window, and selected snapshot hashes; `summary.json` lists completion states. Output directories must be new. CLI completion events omit answer text. Use long-running terminal sessions and poll completed files while keeping the user informed. Cancellation of the terminal stops future turns but cannot guarantee cancellation of requests already processed remotely.

Defaults: 8 workers for ordinary batches and 48 for councils, 8 API turns per attempt, 2 output continuations, a 180-second base wall-clock request deadline (adaptive per model, see above), TTFT hedging for streaming providers, whole-attempt timed hedging disabled, and up to 5 attempts per task/model (hard cap 6). The concurrency ceiling is 48. Batches (`run`) also record health and use adaptive deadlines and TTFT hedging, but do not substitute models. `--max-tokens 0` (the default) uses the output ceiling from `/v1/models`; explicit values are clamped to that ceiling. For JoinGonka, the client uses Chat Completions streaming and low reasoning effort. Other providers default to non-streaming. API turns include tool calls and continuations; do not set `--max-turns 1` when a task may need either. Leave the automatic output allowances in place; request concise deliverables in the brief instead of starving reasoning models with small token caps.

On `finish_reason=length` the client resubmits full conversation history and the partial output, then asks for continuation without repetition, up to the configured bound. Incomplete tool calls are discarded and must be reissued completely; never execute partial arguments. An interrupted or stalled stream and exhausted continuation budget are explicit failures. Reasoning spends the output budget too. Read [references/gonka.md](references/gonka.md) when tuning this provider's transport or limits.

Context file limits: 256 KiB each and 1 MiB per task. Advertised context windows are recorded, but the client does not implement a model-specific tokenizer or automatic context compaction; select concise context within the chosen model's window. These are execution limits, not a monetary spending cap. Timeouts and continuations may be billed; inspect provider usage before repeating failed requests. Never disable TLS verification to work around certificate errors.

Read every status. Empty answers, truncation, HTTP errors, and turn-limit exhaustion are failures or partial results. Treat external claims, judge verdicts, and patches as untrusted proposals; verify against local evidence and the user's task before applying. Report provider/model used and material limitations. Mock tests do not establish live provider readiness.
