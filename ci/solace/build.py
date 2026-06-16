#!/usr/bin/env python3
# Build and push the mzbuild images needed for Solace integration tests to
# ghcr.io/jessemenning/materialize-solace, so subsequent jobs (and other
# environments) can pull them without recompiling.
#
# Invoke via bin/pyactivate so the Materialize virtualenv is active:
#
#   bin/pyactivate ci/solace/build.py
#
# Why not deps.ensure()?
# ensure() pushes to both Docker Hub AND ghcr.io/materializeinc/... (hardcoded
# in mzbuild). We own neither. Instead we use acquire() which builds locally
# without pushing, then push only our target images manually.

import os
import re
from pathlib import Path

from materialize import mzbuild, spawn

IMAGE_REGISTRY = "ghcr.io/jessemenning/materialize-solace"
IMAGE_NAMES = {"materialized", "testdrive"}


def _alias_tag(branch: str) -> str:
    """Return the human-readable alias tag for this branch.

    On main: "latest"  (so faa-continuum always tracks the blessed build).
    On any other branch: a sanitized branch name (e.g. fix-solace-probe-frontier).
    This prevents feature-branch builds from overwriting :latest and inadvertently
    updating faa-continuum or any other stack that tracks :latest.
    """
    if branch == "main":
        return "latest"
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "-", branch).strip("-")
    return sanitized or "dev"


def main() -> None:
    # Allow local build — CI=true is set by GHA, which would otherwise require
    # images to already exist in the registry before building.
    os.environ["CI_ALLOW_LOCAL_BUILD"] = "true"

    repo = mzbuild.Repository(
        Path("."),
        profile=mzbuild.Profile.DEV,
        image_registry=IMAGE_REGISTRY,
    )
    deps = repo.resolve_dependencies(
        image for image in repo if image.name in IMAGE_NAMES
    )

    # acquire() pulls base/dependency images from the registry (correct — they
    # are stable upstream layers). For target images it also tries to pull, but
    # we override that below. This is needed so base images are available for
    # the docker build step that follows.
    deps.acquire()

    # Force-rebuild target images from the freshly-checked-out source,
    # overwriting whatever acquire() may have pulled for them.
    #
    # We cannot rely on acquire() alone because it calls try_pull() first:
    # if the content-addressed tag already exists in the registry (from a
    # prior run with the same mzbuild fingerprint hash), it pulls the stale
    # image rather than rebuilding — the "stale artifact trap" documented in
    # doc/developer/solace/CLAUDE.md.
    target_deps = [dep for dep in deps if dep.name in IMAGE_NAMES]
    prep = deps._prepare_batch(target_deps)
    for dep in target_deps:
        print(f"==> Force-rebuilding {dep.spec()} from source")
        dep.build(prep)

    # Push target images: content-addressed tag + human-readable alias.
    # On main the alias is :latest so faa-continuum always tracks the blessed build.
    # On feature branches the alias is the sanitized branch name — this avoids
    # overwriting :latest and inadvertently updating stacks that track it.
    branch = os.environ.get("GITHUB_REF_NAME", "")
    alias = _alias_tag(branch)
    print(f"==> Branch: '{branch}'  →  alias tag: '{alias}'")

    for dep in target_deps:
        tag = dep.spec()
        print(f"==> Pushing {tag}")
        spawn.runv(["docker", "push", tag])
        alias_tag = f"{IMAGE_REGISTRY}/{dep.name}:{alias}"
        print(f"==> Tagging and pushing {alias_tag}")
        spawn.runv(["docker", "tag", tag, alias_tag])
        spawn.runv(["docker", "push", alias_tag])

    print()
    print("=== Images pushed ===")
    for dep in target_deps:
        print(f"  {IMAGE_REGISTRY}/{dep.name}:{alias}")
    if alias != "latest":
        print()
        print("NOTE: :latest was NOT updated — this is a feature-branch build.")
        print("      Pull the branch tag above for local testing.")


if __name__ == "__main__":
    main()
