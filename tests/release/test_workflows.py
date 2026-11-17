"""Release invariants no workflow linter expresses, asserted on the parsed YAML.

scripts/release/ enforces these at run time; these tests keep the workflows
from bypassing the scripts. Assert on structure (triggers, permissions, job
graph, step order, which script a step runs), never on the wording of a step.
"""

import unittest

import yaml

from tests.release import ROOT

WORKFLOWS = ROOT / ".github" / "workflows"
VERIFIED_SHA = "${{ needs.verify-tag.outputs.sha }}"
VERIFIED_TAG = "${{ needs.verify-tag.outputs.tag }}"
TAGGED_ONLY = "needs.verify-tag.outputs.tag != ''"


def load(name: str) -> dict:
    doc = yaml.safe_load((WORKFLOWS / name).read_text())
    doc["on"] = doc.pop(True, doc.get("on"))  # YAML 1.1 reads a bare `on` key as True.
    return doc


def runs(step: dict) -> str:
    return step.get("run", "")


def uses(step: dict) -> str:
    return step.get("uses", "")


def is_mutation(step: dict) -> bool:
    return (
        uses(step).startswith("actions/attest")
        or "scripts/release/image.sh" in runs(step)
        or "github-release.sh draft" in runs(step)
        or "github-release.sh publish" in runs(step)
    )


def is_guard(step: dict) -> bool:
    return "github-release.sh guard" in runs(step)


def index(steps: list, predicate) -> int:
    return next(i for i, step in enumerate(steps) if predicate(step))


def needs(job: dict) -> set:
    value = job.get("needs", [])
    return {value} if isinstance(value, str) else set(value)


def strings(node):
    if isinstance(node, dict):
        for value in node.values():
            yield from strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from strings(value)
    elif isinstance(node, str):
        yield node


class Dispatcher(unittest.TestCase):
    """A v* tag push only starts release.yml from main; it can do nothing else."""

    def setUp(self):
        self.wf = load("release-dispatch.yml")

    def test_triggers_only_on_v_tags(self):
        self.assertEqual(self.wf["on"], {"push": {"tags": ["v*"]}})

    def test_can_only_dispatch_workflows(self):
        self.assertEqual(self.wf["permissions"], {"actions": "write", "contents": "read"})
        [job] = self.wf["jobs"].values()
        self.assertNotIn("permissions", job)

    def test_runs_one_step_that_dispatches_release_on_main(self):
        [job] = self.wf["jobs"].values()
        [step] = job["steps"]
        self.assertNotIn("uses", step)
        self.assertEqual(step["run"].strip(), 'gh workflow run release.yml --ref main -f tag="$TAG"')


class Release(unittest.TestCase):
    def setUp(self):
        self.wf = load("release.yml")
        self.jobs = self.wf["jobs"]

    def test_runs_only_on_dispatch_with_a_tag_input(self):
        self.assertEqual(set(self.wf["on"]), {"workflow_dispatch"})
        self.assertIn("tag", self.wf["on"]["workflow_dispatch"]["inputs"])

    def test_one_run_per_tag(self):
        self.assertIn("inputs.tag", self.wf["concurrency"]["group"])
        self.assertFalse(self.wf["concurrency"]["cancel-in-progress"])

    def test_a_tagged_run_refuses_a_ref_other_than_main_before_anything_else(self):
        first = self.jobs["verify-tag"]["steps"][0]
        self.assertEqual(first.get("if"), "inputs.tag != ''")
        self.assertEqual(first["env"]["WORKFLOW_REF"], "${{ github.ref }}")
        self.assertIn("refs/heads/main", runs(first))

    def test_verifies_the_checked_out_tag_with_mains_verifier(self):
        steps = self.jobs["verify-tag"]["steps"]
        main = index(steps, lambda s: s.get("with", {}).get("path") == "main-tree")
        self.assertNotIn("ref", steps[main]["with"], "main's verifier must come from the workflow ref")
        source = index(steps, lambda s: s.get("with", {}).get("path") == "source")
        self.assertEqual(steps[source]["with"]["ref"], "${{ inputs.tag && format('refs/tags/{0}', inputs.tag) || '' }}")
        verify = index(steps, lambda s: "verify-tag.sh" in runs(s))
        step = steps[verify]
        self.assertIn('"${MAIN_TREE}/scripts/release/verify-tag.sh" release "$TAG"', step["run"])
        self.assertTrue(step["env"]["MAIN_TREE"].endswith("/main-tree"))
        self.assertEqual(step["working-directory"], "source")
        for allowed in ("security/release-tag-key.asc", "security/release-tag-fingerprints"):
            self.assertIn(f"${{MAIN_TREE}}/{allowed}", step["run"])
        guard = index(steps, lambda s: "main-tree/scripts/release/github-release.sh guard" in runs(s))
        resolve = index(steps, lambda s: s.get("id") == "resolve")
        self.assertLess(main, verify)
        self.assertLess(source, verify)
        self.assertLess(verify, resolve)
        self.assertLess(guard, resolve)

    def test_every_later_job_builds_the_verified_commit(self):
        for name, job in self.jobs.items():
            if name == "verify-tag":
                continue
            with self.subTest(job=name):
                self.assertIn("verify-tag", needs(job))
                steps = job["steps"]
                checkouts = [i for i, s in enumerate(steps) if uses(s).startswith("actions/checkout@")]
                self.assertTrue(checkouts)
                for i in checkouts:
                    self.assertEqual(steps[i]["with"].get("ref"), VERIFIED_SHA)
                    self.assertEqual(steps[i + 1]["env"]["SHA"], VERIFIED_SHA)
                    self.assertIn('test "$(git rev-parse HEAD)" = "$SHA"', runs(steps[i + 1]))

    def test_never_uses_the_event_commit_or_ref_name(self):
        for value in strings(self.jobs):
            for banned in ("github.sha", "github.ref_name", "GITHUB_SHA", "GITHUB_REF", "github.event_name"):
                self.assertNotIn(banned, value)

    def test_release_changes_go_through_the_release_script(self):
        for value in strings(self.jobs):
            self.assertNotIn("gh release ", value)

    def test_every_mutating_job_refuses_a_published_release_first(self):
        mutating = set()
        for name, job in self.jobs.items():
            steps = job["steps"]
            first = next((i for i, s in enumerate(steps) if is_mutation(s)), None)
            if first is None:
                continue
            mutating.add(name)
            with self.subTest(job=name):
                guards = [s for s in steps[:first] if is_guard(s)]
                self.assertTrue(guards, f"{name} mutates before the guard")
                self.assertTrue(all("if" not in s for s in guards), f"{name} guard must always run")
                self.assertEqual(job.get("if"), TAGGED_ONLY)
        self.assertEqual(mutating, {"image", "attest", "publish"})

    def test_drafts_and_publishes_the_verified_tag_and_commit(self):
        steps = self.jobs["publish"]["steps"]
        notes = index(steps, lambda s: "scripts/release/notes.py" in runs(s))
        draft = index(steps, lambda s: "github-release.sh draft" in runs(s))
        publish = index(steps, lambda s: "github-release.sh publish" in runs(s))
        self.assertLess(notes, draft)
        self.assertLess(draft, publish)
        self.assertEqual(publish, len(steps) - 1)
        for i in (draft, publish):
            self.assertEqual(steps[i]["env"]["TAG"], VERIFIED_TAG)
            self.assertEqual(steps[i]["env"]["SHA"], VERIFIED_SHA)
            self.assertIn('"$TAG" "$SHA"', steps[i]["run"])

    def test_version_tag_is_applied_only_after_attestation_and_the_draft(self):
        push = [
            (name, s) for name, job in self.jobs.items() for s in job["steps"]
            if "scripts/release/image.sh push" in runs(s)
        ]
        self.assertEqual([name for name, _ in push], ["image"])
        promote = [
            (name, i) for name, job in self.jobs.items() for i, s in enumerate(job["steps"])
            if "scripts/release/image.sh promote" in runs(s)
        ]
        self.assertEqual([name for name, _ in promote], ["publish"])
        self.assertLessEqual({"image", "attest"}, needs(self.jobs["publish"]))
        steps = self.jobs["publish"]["steps"]
        [(_, tag)] = promote
        draft = index(steps, lambda s: "github-release.sh draft" in runs(s))
        publish = index(steps, lambda s: "github-release.sh publish" in runs(s))
        self.assertLess(draft, tag)
        self.assertLess(tag, publish)
        self.assertEqual(steps[tag]["env"]["TAG"], VERIFIED_TAG)
        self.assertEqual(steps[tag]["env"]["DIGEST"], "${{ needs.image.outputs.digest }}")
        self.assertIn('promote "$TAG" "$DIGEST"', runs(steps[tag]))

    def test_publish_runs_last_after_every_asset_job(self):
        self.assertLessEqual({"source", "wheels", "image", "attest"}, needs(self.jobs["publish"]))
        self.assertFalse(any("publish" in needs(job) for job in self.jobs.values()))

    def test_attested_manifest_records_the_verified_commit(self):
        steps = self.jobs["attest"]["steps"]
        write = index(steps, lambda s: "scripts/release/manifest.py" in runs(s))
        sign = index(
            steps, lambda s: uses(s).startswith("actions/attest") and s["with"].get("subject-path") == "dist/mizuchi-release.json"
        )
        self.assertLess(write, sign)
        env = steps[write]["env"]
        self.assertEqual((env["SHA"], env["TAG"]), (VERIFIED_SHA, VERIFIED_TAG))
        self.assertIn('--git-sha "$SHA"', steps[write]["run"])
        self.assertIn('--tag "$TAG"', steps[write]["run"])

    def test_wheels_cover_linux_macos_and_windows(self):
        self.assertEqual(
            set(self.jobs["wheels"]["strategy"]["matrix"]["os"]), {"ubuntu-latest", "macos-latest", "windows-latest"}
        )

    def test_write_permissions_are_job_scoped(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        writers = {
            name: {k for k, v in job.get("permissions", {}).items() if v == "write"}
            for name, job in self.jobs.items()
        }
        self.assertEqual(
            {name: perms for name, perms in writers.items() if perms},
            {"image": {"packages"}, "attest": {"id-token", "attestations"}, "publish": {"contents", "packages"}},
        )


if __name__ == "__main__":
    unittest.main()
