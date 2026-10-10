<!-- Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e .agents/skills/technical-documentation/references/tooling.md (verbatim). Compare against the source to detect drift. -->

# Documentation Tooling Guide

Source: https://www.mintlify.com/blog/top-7-api-documentation-tools-of-2025

Use this file when deciding build/review expectations for doc platforms.

## Tool-selection checkpoints

- Existing stack lock-in: do not force migration for minor gains.
- API workflow depth: generated references, OpenAPI support, testability.
- Collaboration model: docs-as-code, review workflow, versioning.
- Runtime quality: search, navigation, and copy-ready code snippets.
- AI readiness: structured content, stable URLs, machine-friendly layout yet human readable.
- Human readiness: reading complexity, reading UX, navigation depth, minimize jargon.

## Apply in brownfield mode

- Prioritize compatibility with the current platform.
- Use available components and style conventions before introducing new patterns.
- Propose migration only when current constraints block critical outcomes.

## Apply in evergreen mode

- Favor platforms and templates that make routine updates low-friction.
- Standardize section templates to reduce drift.
- Capture ownership, update cadence, and stale-content detection rules.

## Review implications

- Check whether content uses platform primitives correctly (tabs, callouts, endpoint blocks).
- Flag docs that are technically correct but hard to scan in the chosen platform.
- Recommend platform-specific improvements only when they reduce cognitive load.
