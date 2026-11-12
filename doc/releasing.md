# Making a release

A release is the source at a tag, `v<version>`, on the release's own
branch. From it a C program gets the library, `libiclaustron` (shared
and static), its header and a pkg-config file, through `cargo xtask
install`; a Rust program depends on the crates by git at the tag (user
guide, section 1). Nothing is published to crates.io, and no binaries
are built for a release.

Supported: data nodes of RonDB 26.10.0 or later; Linux on x86_64 and
aarch64, macOS on aarch64.

## Branches

- `trunk` is where development goes on.
- Each release has a branch named by its version, `1.0.0`, made from
  `trunk` when the release is cut, and is tagged on it: `v1.0.0`.
- A released branch takes no further changes (the author, 2026-11-12),
  fixes included: they go to `trunk`, and reach users in the next
  release, which gets a branch of its own, `1.1.0`, made from `trunk`
  once its work is ready to release.

## Version numbers

The version is kept in one place, `version` under `[workspace.package]`
in `Cargo.toml`; every crate takes it from there, and
`ic_port::consts::IC_VERSION` and `IC_VERSION_STR` are made from it
when the crate is compiled.

The C header carries it too, as `IC_API_VERSION_MAJOR`, `_MINOR` and
`_PATCH`. cbindgen copies only literals into the header, so these are
written out in `crates/ic_capi/src/lib.rs`, and a check in
`ic_api_version` stops the crate compiling when they differ from
`Cargo.toml`.

What a number means, for the C API and for the Rust crates alike:

- **Major**: a program built against the one before may break: a type
  changed size, a function's arguments changed, a function went, a
  value was renumbered, a field moved. The major version is in the name
  a program records, `libiclaustron.so.1` on Linux and
  `libiclaustron.1.dylib` on macOS, so programs built against different
  major versions each keep their own library.
- **Minor**: something added (functions, values appended at the end, a
  field at the end of a structure whose size is passed in); a program
  built against the one before runs unchanged.
- **Patch**: fixes only.

## Before the tag: the checks

Run on the commit to be tagged, on the release branch.

1. Static checks, the header, and our own names:

   ```sh
   cargo xtask check                 # fmt, clippy, unit tests, style
   cargo xtask header                # must print no diff
   cargo xtask names $RONDB          # no RonDB name in code or docs
   ```

2. The fuzz tests, at their default number of cases:

   ```sh
   cargo xtask fuzz
   ```

3. The whole integration run, every group, against a release-built
   RonDB 26.10 cluster set up as in [`dev-cluster.md`](dev-cluster.md);
   when a release supports an older data node version than the newest,
   once against each:

   ```sh
   export IC_TEST_CONNECTSTRING=localhost:1186
   cargo xtask test-integration
   ```

4. The C library as a user gets it: installed under a scratch prefix,
   the C examples built against it through pkg-config, and one run
   against the cluster (the examples' tables are made by
   `examples/setup.sql`; [`examples/README.md`](../examples/README.md)):

   ```sh
   PREFIX=$(mktemp -d)
   cargo xtask install $PREFIX
   PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig make -C examples/c clean all
   examples/c/describe localhost:1186
   ```

   The name the programs recorded carries the major version:

   ```sh
   otool -L examples/c/describe                     # macOS: libiclaustron.1.dylib
   readelf -d examples/c/describe | grep NEEDED     # Linux: libiclaustron.so.1
   ```

5. The Rust examples, built and one run (`examples/README.md`, "Rust").

## Cutting the release

1. For a new major or minor version, make its branch from `trunk`:

   ```sh
   git switch -c 1.1.0 trunk
   ```

   A patch release is made on the existing branch.

2. Set the version: `version` in `Cargo.toml`, then the three
   `IC_API_VERSION_*` in `crates/ic_capi/src/lib.rs` to match. Build
   once so that `Cargo.lock` takes the new version, and regenerate the
   header: `cargo xtask header` writes `target/ic_apid.h` and diffs it;
   copy it over `include/iclaustron/ic_apid.h` once the diff shows only
   the version.

3. Write the release's section at the top of
   [`CHANGELOG.md`](../CHANGELOG.md), dated the day of the tag: what it
   needs, what is new, and every change that breaks a program built
   against the release before, with what to do about it. Bring the
   README's status line and the user guide's dependency lines (section
   1, the tag) up to date.

4. Commit, and cherry-pick the commit to `trunk`.

5. Run the checks above on the release branch.

6. Tag the release on its branch, annotated, with its CHANGELOG section
   as the message (first line `iClaustron <version>`), and push the
   branches and the tag:

   ```sh
   git tag -a v1.1.0 -F notes.txt
   git push origin 1.1.0 trunk v1.1.0
   ```

7. Make the GitHub release from the tag, with the same notes:

   ```sh
   gh release create v1.1.0 --title "iClaustron 1.1.0" --notes-file notes.txt
   ```
