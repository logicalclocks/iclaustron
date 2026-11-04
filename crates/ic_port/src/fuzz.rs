// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! A fuzzer of our own for the decoders of what arrives from the
//! network (chapter 08, "Fuzzing"). It needs no crate beyond std, runs
//! on the stable toolchain, and is run by the developer, with
//! `cargo xtask fuzz`.
//!
//! Each decoder has a test named `fuzz_…`, which a plain `cargo test`
//! leaves out, that hands [`run`] a few seeds made by our own encoders
//! and a target: a function that feeds one input to the decoder. [`run`]
//! makes cases from the seeds, each one of them changed a little, and
//! calls the target on each. A decoder passes when every case returns,
//! whatever it returns, with no panic and within
//! [`IC_FUZZ_CASE_LIMIT_MS`]; a case still running after
//! [`IC_FUZZ_HANG_MS`] stops the run. A failure names the run's seed
//! and the case, which run again alone with:
//!
//! - `IC_FUZZ_SEED`: the run's seed; one from the clock if not set, and
//!   printed either way.
//! - `IC_FUZZ_CASE`: run only this case of the run.
//! - `IC_FUZZ_ITERS`: cases for each target, [`IC_FUZZ_DEFAULT_ITERS`]
//!   if not set.
//!
//! The mutations are those that find what a decoder trusts: bits
//! flipped, bytes changed, a word set to a value at an edge (0, 1, all
//! ones, the largest signed), the input cut short or made longer, a
//! piece repeated, and the tail of another seed spliced on.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::time;

/// Cases for each target unless `IC_FUZZ_ITERS` says.
pub const IC_FUZZ_DEFAULT_ITERS: u64 = 100_000;
/// The longest one case may take before the run calls it a failure.
pub const IC_FUZZ_CASE_LIMIT_MS: u64 = 1_000;
/// How long a case may run before the run is stopped as hung.
pub const IC_FUZZ_HANG_MS: u64 = 10_000;
/// The longest case made: a decoder of network input meets no more at
/// once than a few signals' worth.
const IC_FUZZ_MAX_INPUT: usize = 64 * 1024;
/// Values a length, a count or an offset is tried with.
const IC_FUZZ_EDGE_WORDS: [u32; 8] =
  [0, 1, 2, 0x7F, 0xFF, 0xFFFF, 0x7FFF_FFFF, 0xFFFF_FFFF];
/// No case is running.
const IC_FUZZ_NO_CASE: u64 = u64::MAX;

/// The source of a run's cases: splitmix64, so that a case is the same
/// on every machine given the same seed.
#[derive(Debug)]
pub struct Mutator {
  state: u64,
}

impl Mutator {
  /// A mutator starting from `seed`.
  pub fn new(seed: u64) -> Mutator {
    Mutator { state: seed }
  }

  /// The next number.
  pub fn next_u64(&mut self) -> u64 {
    self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  }

  /// A number below `n`; 0 when `n` is 0.
  pub fn below(&mut self, n: usize) -> usize {
    if n == 0 {
      return 0;
    }
    (self.next_u64() % n as u64) as usize
  }

  /// Make a case into `out`: one of `seeds` changed by one to four
  /// mutations, or, now and then, bytes of no seed at all.
  pub fn case_into(&mut self, seeds: &[Vec<u8>], out: &mut Vec<u8>) {
    out.clear();
    if seeds.is_empty() || self.below(16) == 0 {
      let len = self.below(256);
      while out.len() < len {
        out.push(self.next_u64() as u8);
      }
      return;
    }
    out.extend_from_slice(&seeds[self.below(seeds.len())]);
    let count = 1 + self.below(4);
    let mut i: usize = 0;
    while i < count {
      self.mutate(seeds, out);
      i += 1;
    }
    out.truncate(IC_FUZZ_MAX_INPUT);
  }

  /// Change `out` in one way.
  fn mutate(&mut self, seeds: &[Vec<u8>], out: &mut Vec<u8>) {
    let len = out.len();
    match self.below(7) {
      0 => {
        // A bit flipped.
        if len > 0 {
          let at = self.below(len);
          out[at] ^= 1 << self.below(8);
        }
      }
      1 => {
        // A byte changed.
        if len > 0 {
          let at = self.below(len);
          out[at] = self.next_u64() as u8;
        }
      }
      2 => {
        // A word, where words lie, set to an edge, or just past one.
        if len >= 4 {
          let at = self.below(len / 4) * 4;
          let mut value = IC_FUZZ_EDGE_WORDS[self.below(8)];
          if self.below(2) == 0 {
            value = value.wrapping_add(self.below(8) as u32);
          }
          out[at..at + 4].copy_from_slice(&value.to_ne_bytes());
        }
      }
      3 => {
        // Cut short.
        let keep = self.below(len + 1);
        out.truncate(keep);
      }
      4 => {
        // Bytes put in.
        let at = self.below(len + 1);
        let count = 1 + self.below(16);
        let mut i: usize = 0;
        while i < count {
          out.insert(at, self.next_u64() as u8);
          i += 1;
        }
      }
      5 => {
        // A piece repeated.
        if len > 0 {
          let from = self.below(len);
          let span = 1 + self.below(std::cmp::min(len - from, 256));
          let piece: Vec<u8> = out[from..from + span].to_vec();
          let at = self.below(len + 1);
          let tail: Vec<u8> = out.split_off(at);
          out.extend_from_slice(&piece);
          out.extend_from_slice(&tail);
        }
      }
      _ => {
        // Another seed's tail spliced on.
        let other = &seeds[self.below(seeds.len())];
        let cut = self.below(len + 1);
        let from = self.below(other.len() + 1);
        out.truncate(cut);
        out.extend_from_slice(&other[from..]);
      }
    }
  }
}

/// The bytes as 32-bit words in this machine's order, as the words of
/// a signal are read from a socket; bytes past the last whole word are
/// left out.
pub fn words_of(bytes: &[u8], out: &mut Vec<u32>) {
  out.clear();
  let mut at: usize = 0;
  while at + 4 <= bytes.len() {
    let word = [bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]];
    out.push(u32::from_ne_bytes(word));
    at += 4;
  }
}

/// Words as the bytes a socket carries them in: a seed for a decoder of
/// words.
pub fn bytes_of(words: &[u32]) -> Vec<u8> {
  let mut out: Vec<u8> = Vec::with_capacity(words.len() * 4);
  for word in words {
    out.extend_from_slice(&word.to_ne_bytes());
  }
  out
}

/// What the run and its watcher share: the case running, and since
/// when.
#[derive(Debug)]
struct Watch {
  case: AtomicU64,
  started_at: AtomicU64,
  done: AtomicBool,
}

/// Stop the process if a case runs past [`IC_FUZZ_HANG_MS`]: a decoder
/// caught in a loop never returns to be timed.
fn watch_for_hangs(name: String, seed: u64, watch: Arc<Watch>) {
  while !watch.done.load(Ordering::Acquire) {
    std::thread::sleep(std::time::Duration::from_millis(100));
    let case = watch.case.load(Ordering::Acquire);
    if case == IC_FUZZ_NO_CASE {
      continue;
    }
    let started = watch.started_at.load(Ordering::Acquire);
    let ms = time::millis_elapsed(started, time::gethrtime());
    if ms > IC_FUZZ_HANG_MS && watch.case.load(Ordering::Acquire) == case {
      eprintln!(
        "fuzz {}: case {} has run for {} ms; IC_FUZZ_SEED={} \
         IC_FUZZ_CASE={} runs it again",
        name, case, ms, seed, case
      );
      std::process::exit(1);
    }
  }
}

/// A number from the environment.
fn env_u64(name: &str) -> Option<u64> {
  let text = std::env::var(name).ok()?;
  text.trim().parse().ok()
}

/// The seed of case `case` of a run seeded with `seed`.
fn case_seed(seed: u64, case: u64) -> u64 {
  let mut m = Mutator::new(seed ^ case.wrapping_mul(0xD1B5_4A32_D192_ED03));
  m.next_u64()
}

/// Run `target` over the cases of a run made from `seeds`, as the
/// module note says, the seeds themselves first. Panics, failing the
/// test it runs in, on the first case that panics or takes too long,
/// after saying which it was.
pub fn run(name: &str, seeds: &[Vec<u8>], target: fn(&[u8])) {
  let iters = env_u64("IC_FUZZ_ITERS").unwrap_or(IC_FUZZ_DEFAULT_ITERS);
  let seed = match env_u64("IC_FUZZ_SEED") {
    Some(seed) => seed,
    None => time::gethrtime(),
  };
  let (first, end) = match env_u64("IC_FUZZ_CASE") {
    Some(case) => (case, case.saturating_add(1)),
    None => (0, iters),
  };
  run_cases(name, seeds, target, seed, first, end);
}

/// Run cases `first` to `end`, not included, of the run seeded with
/// `seed`, as [`run`] does.
fn run_cases(
  name: &str,
  seeds: &[Vec<u8>],
  target: fn(&[u8]),
  seed: u64,
  first: u64,
  end: u64,
) {
  eprintln!(
    "fuzz {}: {} cases, IC_FUZZ_SEED={}",
    name,
    end - first,
    seed
  );
  let watch = Arc::new(Watch {
    case: AtomicU64::new(IC_FUZZ_NO_CASE),
    started_at: AtomicU64::new(0),
    done: AtomicBool::new(false),
  });
  let watcher_watch = Arc::clone(&watch);
  let watcher_name = name.to_string();
  // std starts a thread only with a closure.
  let watcher = std::thread::Builder::new()
    .name("fuzz-watch".to_string())
    .spawn(move || watch_for_hangs(watcher_name, seed, watcher_watch));
  let mut input: Vec<u8> = Vec::new();
  let mut case = first;
  while case < end {
    // The first cases are the seeds as they are, to show that each
    // decodes; the rest are made from them.
    input.clear();
    if (case as usize) < seeds.len() {
      input.extend_from_slice(&seeds[case as usize]);
    } else {
      let mut m = Mutator::new(case_seed(seed, case));
      m.case_into(seeds, &mut input);
    }
    let started = time::gethrtime();
    watch.started_at.store(started, Ordering::Release);
    watch.case.store(case, Ordering::Release);
    // std catches a panic only around a closure.
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| target(&input)));
    watch.case.store(IC_FUZZ_NO_CASE, Ordering::Release);
    let ms = time::millis_elapsed(started, time::gethrtime());
    let failed = if outcome.is_err() {
      Some("panicked".to_string())
    } else if ms > IC_FUZZ_CASE_LIMIT_MS {
      Some(format!("took {} ms", ms))
    } else {
      None
    };
    if let Some(what) = failed {
      watch.done.store(true, Ordering::Release);
      panic!(
        "fuzz {}: case {} {}, {} bytes; IC_FUZZ_SEED={} IC_FUZZ_CASE={} \
         runs it again",
        name,
        case,
        what,
        input.len(),
        seed,
        case
      );
    }
    case += 1;
  }
  watch.done.store(true, Ordering::Release);
  if let Ok(handle) = watcher {
    let _ = handle.join();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_case_is_the_same_for_the_same_seed_and_number() {
    let seeds = vec![vec![1u8, 2, 3, 4, 5, 6, 7, 8], vec![9u8; 40]];
    let mut a: Vec<u8> = Vec::new();
    let mut b: Vec<u8> = Vec::new();
    let mut case: u64 = 0;
    while case < 200 {
      Mutator::new(case_seed(42, case)).case_into(&seeds, &mut a);
      Mutator::new(case_seed(42, case)).case_into(&seeds, &mut b);
      assert_eq!(a, b, "case {}", case);
      assert!(a.len() <= IC_FUZZ_MAX_INPUT);
      case += 1;
    }
  }

  #[test]
  fn cases_differ_from_their_seeds() {
    let seeds = vec![vec![0u8; 64]];
    let mut out: Vec<u8> = Vec::new();
    let mut changed: u32 = 0;
    let mut case: u64 = 0;
    while case < 100 {
      Mutator::new(case_seed(7, case)).case_into(&seeds, &mut out);
      if out != seeds[0] {
        changed += 1;
      }
      case += 1;
    }
    assert!(changed > 90, "{} of 100 changed", changed);
  }

  #[test]
  fn words_and_bytes_go_both_ways() {
    let words = [1u32, 0xDEAD_BEEF, u32::MAX];
    let bytes = bytes_of(&words);
    assert_eq!(bytes.len(), 12);
    let mut back: Vec<u32> = Vec::new();
    words_of(&bytes, &mut back);
    assert_eq!(back, words);
    // A part word at the end is left out.
    words_of(&bytes[..7], &mut back);
    assert_eq!(back, vec![1u32]);
  }

  fn never_fails(_input: &[u8]) {}

  fn fails_on_long_input(input: &[u8]) {
    assert!(input.len() < 4, "a long input");
  }

  #[test]
  fn a_target_that_never_fails_passes() {
    run_cases("never_fails", &[vec![1u8, 2, 3]], never_fails, 1, 0, 50);
  }

  #[test]
  fn a_target_that_panics_fails_the_run() {
    let outcome = std::panic::catch_unwind(|| {
      run_cases("fails", &[vec![0u8; 8]], fails_on_long_input, 1, 0, 50)
    });
    assert!(outcome.is_err(), "the run failed");
  }
}
