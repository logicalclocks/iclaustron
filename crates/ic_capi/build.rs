// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! The shared library's name as a program linked with it records it:
//! on Linux the soname, `libiclaustron.so.` and the major version from
//! Cargo.toml (`libiclaustron.so.1`); on macOS `@rpath/libiclaustron.dylib`,
//! the name of the file in `target/`, which `cargo xtask install` turns
//! into the installed path, `libiclaustron.1.dylib`. The macOS library
//! is linked with room for that longer name, under any prefix.

fn main() {
  let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
  let major = env!("CARGO_PKG_VERSION_MAJOR");
  if os == "linux" {
    println!(
      "cargo:rustc-cdylib-link-arg=-Wl,-soname,libiclaustron.so.{}",
      major
    );
  } else if os == "macos" {
    println!(concat!(
      "cargo:rustc-cdylib-link-arg=-Wl,-install_name,",
      "@rpath/libiclaustron.dylib"
    ));
    // Without it install_name_tool refuses a name longer than this one.
    println!("cargo:rustc-cdylib-link-arg=-Wl,-headerpad_max_install_names");
  }
  println!("cargo:rerun-if-changed=build.rs");
}
