# Shared operating instructions: Krishna, Juhi, Codex, Claude Code

This is the single authoritative agent instruction file for MP3. Claude entrypoints
and the repository-root AGENTS.md refer here; do not maintain competing rules.

1. At startup read `IMPLEMENTATION_PLAN.md` and `KEY_DECISIONS.md` in this directory
   before editing. Read `EXPLANATION.md` for actual behavior and `README.md` for commands.
2. Before **each new substantial task**, ask the human for the main coding-agent
   model AND review/documentation-subagent model, including reasoning preferences,
   unless explicitly supplied with that task. Do not silently reuse today's choice
   as a permanent default. Phase 0 requested GPT-6.1 Sol/high main and GPT-6 Luna/high
   reviewer. The main model must be selected by the human when the tool cannot do it.
3. One primary implementation agent and one independent review/documentation agent
   are enough. Do not recursively delegate. Disclose unsupported model selection or
   execution; never claim a model switch, delegation, or review that did not happen.
4. The main agent writes production code and its tests. After a coherent unit, the
   reviewer independently inspects changes, checks requirements/decision boundaries,
   runs permitted local tests, maintains Markdown, and prepares evidence-backed report
   material. **The reviewer must not independently rewrite production code or tests.**
   It reports focused fixes to the main agent. No invented report results.
5. Both tools use the same approved architecture. A recommendation is not approval.
   Never change a decision from PENDING to APPROVED without explicit human approval;
   record who approved, when, the final choice, and rationale. Correctness conditions
   mandated by the assignment are requirements, not optional design proposals.
6. Phase 0 ends at the gate in the implementation plan. Do not implement storage,
   ABD, eventual consistency, read repair, mode changes, or recovery before review.
   Pending operations must visibly fail with NOT_IMPLEMENTED, never fake success.
7. Respect ownership in the plan. Coordinate with the human/other developer before
   editing another developer's owned module. Shared-interface changes require joint
   coordination and an updated contract before either side implements against them.
8. Use feature branches and small integrations in the actual Git checkout; never
   overwrite the other person's work. Preserve all existing/uncommitted files;
   never reset or clean the working tree. After each future coding batch is complete,
   independently reviewed, and its applicable tests pass, inspect the staged tree,
   commit the verified batch, and push its branch to the private review repository
   `https://github.com/krishna31102004/distr-test-3`. Verify the remote branch and
   commit contents. Do not push to the professor-provided course repository. Keep
   the batch bounded and stop before starting the next coding batch. This standing
   project instruction authorizes the requested review-repository commit/push only;
   other remotes or external destinations remain out of scope.
9. Update task statuses only with evidence. Allowed states: COMPLETE, IN_PROGRESS,
   BLOCKED_BY_DECISION, NOT_STARTED, VERIFIED. VERIFIED requires applicable tests and
   a recorded command/result. Distinguish a tested guard from an implemented feature.
10. Every code change must update `EXPLANATION.md`, including function inputs,
    outputs, behavior, callers, dependencies, status, and decision IDs. Update the
    plan, README, and tests when their claims/interfaces change.
    After each implementation batch has passed independent review and applicable
    tests, also update the cumulative plain-English `BUILD_PROGRESS_EXPLAINED.md`.
    Keep planned work separate from completed/verified work; never mark a batch
    complete before its code review and verification evidence are recorded. The final
    handoff for each verified coding batch must report its branch, full commit SHA,
    exact test results, and successful remote verification, then stop before the next
    batch.
11. Never claim tests passed without running them. Use local sockets, fake clocks,
    and temporary directories. Never fabricate experiments, measurements, plots,
    standard deviations, successful deployments, or readiness evidence.
12. Preserve MP1 and MP2 byte-for-byte as references. Reuse selected own prior code
    with source attribution and tests; do not copy whole projects. Do not run their
    remote suites/scripts as local verification. Their README claims are not proof.
13. Do not copy Cassandra implementation code or other students' solutions. AI assistance is permitted for this course and is not a policy-approval blocker.
    Follow the other university/course academic-integrity requirements; keep truthful
    provenance and human design understanding. Do not misrepresent authorship.
14. No destructive operations, remote VM commands, deployment, data resets, or remote
    fault tests without explicit human approval. Commits and pushes are permitted only
    through the verified review-repository workflow in rule 8; do not push to other
    remotes. Approval for local tests does not authorize live VM workloads.
15. Keep dependencies minimal, code readable, and scope specific to this assignment.
    No speculative platform, storage engine installation, or performance claims.

`mp3/SPEC.md` exists and is the authoritative October 21 specification; it overrides
the task summary. Its absence at the initial Phase 0 bootstrap was historical, not
a current blocker. Krishna's approval is sufficient for architectural contracts;
coordination with Juhi preserves ownership but is not a second approval requirement.
