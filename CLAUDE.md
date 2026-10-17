# CLAUDE.md — Claude Model Directive

@AGENTS.md

## Claude-Specific Instructions

1. **Before every task:** Read `STATE.md` to learn the current module, step, and handoff notes.
2. **After every task:** Update `STATE.md` with what you did, what the next step is, and any context the next model needs.
3. **All code must obey** the 7 Serverless Laws in `AGENTS.md` §2 — no exceptions.
4. **Skills:** The `/build`, `/drill`, and `/sanitize` commands are defined in `skills/`. Follow their `SKILL.md` instructions exactly.
5. **Never ask** which module to work on — read it from `STATE.md`.
6. **Quota awareness:** Keep generated datasets to 500–1,000 rows. Always use `.trigger(availableNow=True)`.
