#!/usr/bin/env python3
# Dump the exact files (mode + sha1 + path) that go into the mzbuild fingerprint
# for 'materialized' and 'testdrive'. Run on both local and CI and diff the outputs
# to identify why the content-addressed hashes diverge between environments.
#
# Usage:  bin/pyactivate ci/solace/fingerprint_debug.py > fingerprint.txt

import hashlib
import os
import stat
from pathlib import Path

from materialize import git as mgit
from materialize import mzbuild

TARGET_IMAGES = {"materialized", "testdrive"}

repo = mzbuild.Repository(
    Path("."),
    profile=mzbuild.Profile.DEV,
    image_registry="ghcr.io/jessemenning/materialize-solace",
)

for target_name in sorted(TARGET_IMAGES):
    imgs = [i for i in repo if i.name == target_name]
    if not imgs:
        print(f"# {target_name}: NOT FOUND", flush=True)
        continue

    deps = repo.resolve_dependencies(imgs)
    for d in deps:
        if d.name != target_name:
            continue

        print(f"\n# === {target_name} ===", flush=True)
        print(f"# spec:     {d.spec()}", flush=True)
        print(f"# profile:  {d.image.rd.profile}", flush=True)
        print(f"# arch:     {d.image.rd.arch}", flush=True)
        print(f"# coverage: {d.image.rd.coverage}", flush=True)
        print(f"# sanitizer:{d.image.rd.sanitizer}", flush=True)

        inputs = d.inputs()
        if d.image._context_files_cache is not None:
            resolved = sorted(inputs)
        else:
            resolved = sorted(set(mgit.expand_globs(d.image.rd.root, *inputs)))

        print(f"# file_count: {len(resolved)}", flush=True)
        print("# mode sha1 path", flush=True)

        for rel_path in resolved:
            abs_path = d.image.rd.root / rel_path
            file_hash = hashlib.sha1()
            try:
                raw_mode = os.lstat(abs_path).st_mode
                if stat.S_ISLNK(raw_mode):
                    file_mode = 0o120000
                elif raw_mode & stat.S_IXUSR:
                    file_mode = 0o100755
                else:
                    file_mode = 0o100644
                with open(abs_path, "rb") as f:
                    file_hash.update(f.read())
                print(f"{file_mode:o} {file_hash.hexdigest()} {rel_path}", flush=True)
            except Exception as exc:
                print(f"ERROR {rel_path}: {exc}", flush=True)
