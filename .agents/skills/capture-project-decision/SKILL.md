---
name: capture-project-decision
description: Capture an accepted mizuchi design or project outcome in the smallest durable artifact. Use after a Discussion, Issue, or PR resolves a choice that may require an ADR, maintained documentation, example, or reusable Skill.
---

<!-- Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e .agents/skills/capture-project-decision/SKILL.md (adapted for mizuchi). Compare against the source to detect drift. -->

# Capture Project Decision

Promote the durable result, not the conversation that produced it.

## Procedure

1. Read the complete source Discussion, Issue, PR, and relevant current docs or
   code. Confirm the choice is accepted rather than merely proposed. Complete
   when the decision, owner, alternatives, and supporting evidence are named.
2. Select exactly the artifacts justified by future use:
   - ADR: a durable boundary, trust model, public contract, persistence
     strategy, or difficult-to-reverse technical choice;
   - documentation: repeated operator, integrator, or contributor knowledge;
   - example: a supported path is best taught and tested as executable code;
   - Skill: a repeated project-specific AI procedure or high-risk checklist;
   - none: the outcome is local to the closed work item.
   Complete when every artifact has a future audience and owner.
3. Preserve one source of truth. Link to code/protocols rather than duplicating
   exhaustive details. Put rationale in the ADR and usage in docs. Complete when
   the same rule is not maintained in multiple prose locations.
4. For an ADR, follow the template in `docs/decisions/README.md` (context,
   decision, consequences, rejected alternatives), allocate the next number,
   include the source links, and update the index in `docs/decisions/README.md`. Supersede by
   adding a new ADR and cross-linking both; retain historical text. Complete
   when status and relationships are explicit.
5. Check links, examples, Skill metadata, and affected indexes. Complete when a
   future contributor can discover the result without the original prompt.

## Output

State the selected artifact(s), why each is durable, files changed or proposed,
source links, owner, and validation. If no promotion is warranted, say so and
leave the knowledge in the closed Issue/PR.

A decision owned by another repository (for example a protocol owned by an
upstream project) is recorded there and linked from here, not duplicated.

Do not turn unresolved debate into policy, copy investigation logs into docs,
or create an ADR for routine implementation detail.
