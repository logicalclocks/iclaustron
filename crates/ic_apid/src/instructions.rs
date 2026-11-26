// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! Programs written an instruction at a time, with registers, labels
//! and jumps either way, for what a condition and its assignments
//! cannot say: a loop (doc/rust/04-api-design.md, "Instructions"). New:
//! the C had nothing of the kind.
//!
//! **Beside the [`ProgramBuilder`](crate::program::ProgramBuilder), not
//! under it.** That one is told what must hold and what to assign, and
//! lays the instructions out itself. An [`InstructionBuilder`] is told
//! the instructions, in the order they run: what goes into which of the
//! eight registers, where the labels stand and which instruction jumps
//! to which. Both make a [`Program`], which an operation runs the same
//! way whichever made it.
//!
//! **Registers** hold integers of 64 bits, or NULL, and hold nothing a
//! program can count on before it has put something there. Integer
//! columns are read into them, and columns of 32 and 64 bits written
//! from them, on an update. A comparison of a NULL register fails the
//! program (878): a program tests one that may be NULL first. Sums and
//! the bitwise instructions give NULL where a register they take is
//! NULL. A value read from an unsigned column is unsigned, which a
//! shift right goes by: it fills with zeros, where a signed value keeps
//! its sign.
//!
//! **Labels and jumps.** A label is made, used by any number of jumps,
//! and placed once, before the instruction it names, before or after
//! the jumps to it. It is good in the builder that made it, and
//! another's is refused. A jump to a label placed before it goes
//! backwards, which makes a loop. The data node ends a program that
//! runs too long on a row (886): it counts about 16 000 instructions,
//! some for more than one. So a loop that does not end fails its
//! operation, or ends its scan, and nothing else.
//!
//! **Endings.** `accept` lets the row pass. `refuse` refuses it as a
//! missed condition is refused: a scan skips the row, and a key
//! operation fails with 6000 where it aborts on error and with 626
//! where it ignores errors. `refuse_with` refuses it with a code of the
//! application's own, 6001 to 6999, whatever the abort option; a scan
//! would end on one, so it does not take a program that has any. The
//! last instruction is an ending or a jump, so that no row runs off
//! the end.
//!
//! What is wrong is kept as the builder's error until the finish, as
//! the other builder keeps it.

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ic_port::err;
use ic_port::IcError;
use ic_signals::interp;
use ic_signals::interp::BitOp;
use ic_signals::interp::Label as Place;
use ic_signals::interp::ProgramWriter;
use ic_signals::table_props;

use crate::dict_cache::TableDef;
use crate::program;
use crate::program::Cmp;
use crate::program::Program;
use crate::program::ProgramParts;

/// The builders made so far, for the next one's mark.
static IC_BUILDERS_MADE: AtomicU32 = AtomicU32::new(0);

/// The marks a builder may have: 16 bits, without 0, which no label
/// has, and without the last, which with the last label would be the
/// C API's `IC_PROGRAM_NONE`.
const IC_BUILDER_MARKS: u32 = 0xFFFE;
/// The labels a builder hands out at most; a program has fewer words
/// than twice this.
const IC_MAX_LABELS: usize = 0x1_0000;

/// A place in a program that jumps go to, handed out by its builder
/// before it is placed, and good in that builder only: its number
/// carries its builder's mark, so that one from another builder is
/// refused where it is used and not taken for a label of this one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Label(u32);

impl Label {
  /// The label a number names, as the C API passes one.
  pub fn from_u32(number: u32) -> Label {
    Label(number)
  }

  /// The label's number: its builder's mark above its number in the
  /// builder.
  pub fn as_u32(self) -> u32 {
    self.0
  }
}

/// The registers a program has, numbered from 0.
pub const IC_PROGRAM_REGISTERS: u32 = interp::IC_INTERP_REGISTERS;

/// A program over a table, written an instruction at a time.
#[derive(Debug)]
pub struct InstructionBuilder {
  table: Arc<TableDef>,
  w: ProgramWriter,
  /// Where the `REFUSE` words of [`refuse`](Self::refuse) are.
  refusals: Vec<u32>,
  /// The mark its labels carry, 1 to [`IC_BUILDER_MARKS`].
  mark: u32,
  /// The labels a jump goes to, a flag each.
  used: Vec<bool>,
  inputs: u32,
  writes: bool,
  own_codes: bool,
  uses_disk: bool,
  /// The last instruction ends the program or jumps away.
  ended: bool,
  error: Option<IcError>,
}

impl InstructionBuilder {
  /// A builder of a program for `table`'s rows.
  pub fn new(table: &Arc<TableDef>) -> InstructionBuilder {
    let mut w = ProgramWriter::new();
    w.allow_backward();
    InstructionBuilder {
      table: Arc::clone(table),
      w,
      refusals: Vec::new(),
      mark: 1
        + IC_BUILDERS_MADE.fetch_add(1, Ordering::Relaxed) % IC_BUILDER_MARKS,
      used: Vec::new(),
      inputs: 0,
      writes: false,
      own_codes: false,
      uses_disk: false,
      ended: false,
      error: None,
    }
  }

  /// The table the program is for.
  pub fn table(&self) -> &Arc<TableDef> {
    &self.table
  }

  /// A new label, to be placed once and jumped to from anywhere in
  /// this builder's program. One of another builder is refused where it
  /// is used (`IC_ERROR_BAD_PROGRAM`).
  pub fn label(&mut self) -> Label {
    let number = self.used.len();
    if number >= IC_MAX_LABELS {
      self.fail(err::IC_ERROR_CONDITION_TOO_COMPLEX);
      return Label(0);
    }
    self.used.push(false);
    self.w.new_label();
    Label((self.mark << 16) | number as u32)
  }

  /// Place `label` before the next instruction.
  pub fn place(&mut self, label: Label) {
    let place = match self.place_for(label) {
      Some(place) if self.w.place_of(place).is_none() => place,
      _ => {
        self.fail(err::IC_ERROR_BAD_PROGRAM);
        return;
      }
    };
    self.w.place(place);
    // An instruction is to follow: a jump here must find one.
    self.ended = false;
  }

  /// R`reg` = the row's value of `column`, an integer column; NULL if
  /// the column is.
  pub fn read_column(&mut self, reg: u32, column: u32) {
    if !self.check_reg(reg) || !self.check_column(column, false) {
      return;
    }
    self.w.read_column(reg, column);
    self.ended = false;
  }

  /// `column` = R`reg`, cut to the column's size; NULL if the register
  /// is. An integer column of 32 or 64 bits, and only on an update.
  pub fn write_column(&mut self, column: u32, reg: u32) {
    if !self.check_reg(reg) || !self.check_column(column, true) {
      return;
    }
    self.w.write_column(column, reg);
    self.writes = true;
    self.ended = false;
  }

  /// R`reg` = NULL.
  pub fn set_null(&mut self, reg: u32) {
    if !self.check_reg(reg) {
      return;
    }
    self.w.set_null(reg);
    self.ended = false;
  }

  /// R`reg` = `value`.
  pub fn set_integer(&mut self, reg: u32, value: i64) {
    if !self.check_reg(reg) {
      return;
    }
    if (0..=0xFFFF).contains(&value) {
      self.w.set_16(reg, value as u16);
    } else if (0..=u32::MAX as i64).contains(&value) {
      self.w.set_32(reg, value as u32);
    } else {
      self.w.set_64(reg, value as u64);
    }
    self.ended = false;
  }

  /// R`reg` = input `input`, 0 to 15, which each operation sets.
  pub fn read_input(&mut self, reg: u32, input: u32) {
    if !self.check_reg(reg) {
      return;
    }
    if input >= program::IC_PROGRAM_INPUTS {
      self.fail(err::IC_ERROR_BAD_PROGRAM);
      return;
    }
    self.w.read_input(reg, input);
    self.inputs |= 1 << input;
    self.ended = false;
  }

  /// R`dest` = R`a` + R`b`; NULL if either is.
  pub fn add(&mut self, dest: u32, a: u32, b: u32) {
    if !self.check_reg(dest) || !self.check_reg(a) || !self.check_reg(b) {
      return;
    }
    self.w.add(dest, a, b);
    self.ended = false;
  }

  /// R`dest` = R`a` − R`b`; NULL if either is.
  pub fn subtract(&mut self, dest: u32, a: u32, b: u32) {
    if !self.check_reg(dest) || !self.check_reg(a) || !self.check_reg(b) {
      return;
    }
    self.w.subtract(dest, a, b);
    self.ended = false;
  }

  /// R`dest` = R`src` + `value`, in one instruction; NULL if R`src`
  /// is.
  pub fn add_integer(&mut self, dest: u32, src: u32, value: u16) {
    if !self.check_reg(dest) || !self.check_reg(src) {
      return;
    }
    self.w.add_imm(dest, src, value);
    self.ended = false;
  }

  /// R`dest` = R`src` − `value`, in one instruction; NULL if R`src`
  /// is.
  pub fn subtract_integer(&mut self, dest: u32, src: u32, value: u16) {
    if !self.check_reg(dest) || !self.check_reg(src) {
      return;
    }
    self.w.subtract_imm(dest, src, value);
    self.ended = false;
  }

  /// R`dest` = R`a` AND R`b`, bit by bit; NULL if either is.
  pub fn bit_and(&mut self, dest: u32, a: u32, b: u32) {
    self.bits(BitOp::And, dest, a, b);
  }

  /// R`dest` = R`src` AND `value`, bit by bit, in one instruction;
  /// NULL if R`src` is.
  pub fn bit_and_integer(&mut self, dest: u32, src: u32, value: u16) {
    self.bits_integer(BitOp::And, dest, src, value);
  }

  /// R`dest` = R`a` OR R`b`, bit by bit; NULL if either is.
  pub fn bit_or(&mut self, dest: u32, a: u32, b: u32) {
    self.bits(BitOp::Or, dest, a, b);
  }

  /// R`dest` = R`src` OR `value`, bit by bit, in one instruction;
  /// NULL if R`src` is.
  pub fn bit_or_integer(&mut self, dest: u32, src: u32, value: u16) {
    self.bits_integer(BitOp::Or, dest, src, value);
  }

  /// R`dest` = R`a` XOR R`b`, bit by bit; NULL if either is.
  pub fn bit_xor(&mut self, dest: u32, a: u32, b: u32) {
    self.bits(BitOp::Xor, dest, a, b);
  }

  /// R`dest` = R`src` XOR `value`, bit by bit, in one instruction;
  /// NULL if R`src` is.
  pub fn bit_xor_integer(&mut self, dest: u32, src: u32, value: u16) {
    self.bits_integer(BitOp::Xor, dest, src, value);
  }

  /// R`dest` = R`src` with every bit inverted; NULL if R`src` is.
  pub fn bit_not(&mut self, dest: u32, src: u32) {
    if !self.check_reg(dest) || !self.check_reg(src) {
      return;
    }
    self.w.bit_not(dest, src);
    self.ended = false;
  }

  /// R`dest` = R`a` shifted left by R`count` bits; NULL if either is.
  /// A count that is not 0 to 63 fails the program (891).
  pub fn shift_left(&mut self, dest: u32, a: u32, count: u32) {
    self.bits(BitOp::ShiftLeft, dest, a, count);
  }

  /// R`dest` = R`src` shifted left by `count` bits, 0 to 63, in one
  /// instruction; NULL if R`src` is.
  pub fn shift_left_integer(&mut self, dest: u32, src: u32, count: u32) {
    self.shift_integer(BitOp::ShiftLeft, dest, src, count);
  }

  /// R`dest` = R`a` shifted right by R`count` bits; NULL if either is.
  /// A signed value keeps its sign, and one read from an unsigned
  /// column is filled with zeros. A count that is not 0 to 63 fails the
  /// program (891).
  pub fn shift_right(&mut self, dest: u32, a: u32, count: u32) {
    self.bits(BitOp::ShiftRight, dest, a, count);
  }

  /// R`dest` = R`src` shifted right by `count` bits, 0 to 63, in one
  /// instruction; NULL if R`src` is.
  pub fn shift_right_integer(&mut self, dest: u32, src: u32, count: u32) {
    self.shift_integer(BitOp::ShiftRight, dest, src, count);
  }

  fn bits(&mut self, op: BitOp, dest: u32, a: u32, b: u32) {
    if !self.check_reg(dest) || !self.check_reg(a) || !self.check_reg(b) {
      return;
    }
    self.w.bits(op, dest, a, b);
    self.ended = false;
  }

  fn bits_integer(&mut self, op: BitOp, dest: u32, src: u32, value: u16) {
    if !self.check_reg(dest) || !self.check_reg(src) {
      return;
    }
    self.w.bits_imm(op, dest, src, value);
    self.ended = false;
  }

  fn shift_integer(&mut self, op: BitOp, dest: u32, src: u32, count: u32) {
    if count > interp::IC_MAX_SHIFT {
      self.fail(err::IC_ERROR_BAD_PROGRAM);
      return;
    }
    self.bits_integer(op, dest, src, count as u16);
  }

  /// Jump to `to`.
  pub fn jump(&mut self, to: Label) {
    let to = match self.check_jump(to) {
      Some(to) => to,
      None => return,
    };
    self.w.jump(to);
    self.ended = true;
  }

  /// Jump to `to` if R`a` compares with R`b` as `cmp` says, else go
  /// on. Both must hold values: a NULL fails the program (878).
  pub fn jump_if(&mut self, cmp: Cmp, a: u32, b: u32, to: Label) {
    if !self.check_reg(a) || !self.check_reg(b) {
      return;
    }
    let to = match self.check_jump(to) {
      Some(to) => to,
      None => return,
    };
    self.w.jump_if(cmp.reg_cmp(), a, b, to);
    self.ended = false;
  }

  /// Jump to `to` if R`reg` is NULL, else go on.
  pub fn jump_if_null(&mut self, reg: u32, to: Label) {
    if !self.check_reg(reg) {
      return;
    }
    let to = match self.check_jump(to) {
      Some(to) => to,
      None => return,
    };
    self.w.jump_if_null(reg, to);
    self.ended = false;
  }

  /// Jump to `to` if R`reg` holds a value, else go on.
  pub fn jump_if_value(&mut self, reg: u32, to: Label) {
    if !self.check_reg(reg) {
      return;
    }
    let to = match self.check_jump(to) {
      Some(to) => to,
      None => return,
    };
    self.w.jump_if_value(reg, to);
    self.ended = false;
  }

  /// End: the row passes, and an update's columns are written.
  pub fn accept(&mut self) {
    self.w.accept();
    self.ended = true;
  }

  /// End: the row is refused as a missed condition is. A scan passes
  /// it over; a key operation fails with 6000 where it aborts on error
  /// and with 626 where it ignores errors.
  pub fn refuse(&mut self) {
    self.refusals.push(self.w.len() as u32);
    self.w.refuse(interp::IC_REFUSE_NO_ROW);
    self.ended = true;
  }

  /// End: the row is refused with `code`, a code of the application's
  /// own, 6001 to 6999, whatever the operation's abort option. Key
  /// operations only: a scan does not take a program that has one.
  pub fn refuse_with(&mut self, code: u32) {
    if code < u32::from(interp::IC_REFUSE_OWN_FIRST)
      || code > u32::from(interp::IC_REFUSE_OWN_LAST)
    {
      self.fail(err::IC_ERROR_BAD_PROGRAM);
      return;
    }
    self.w.refuse(code as u16);
    self.own_codes = true;
    self.ended = true;
  }

  /// The program, once every label a jump goes to is placed and the
  /// last instruction is an ending or a jump. The first error met in
  /// writing it comes back here.
  pub fn finish(&mut self) -> Result<Arc<Program>, IcError> {
    if let Some(e) = self.error {
      return Err(e);
    }
    if !self.ended {
      return Err(IcError::new(err::IC_ERROR_BAD_PROGRAM));
    }
    let mut label: u32 = 0;
    while (label as usize) < self.used.len() {
      let placed = self.w.place_of(Place::from_u32(label)).is_some();
      if self.used[label as usize] && !placed {
        return Err(IcError::new(err::IC_ERROR_BAD_PROGRAM));
      }
      label += 1;
    }
    // The length first: a program too long may also have a jump too
    // far, and is to be called too long.
    if self.w.len() > program::IC_MAX_PROGRAM_WORDS {
      return Err(IcError::new(err::IC_ERROR_CONDITION_TOO_COMPLEX));
    }
    self.w.finish()?;
    Ok(Arc::new(Program::from_parts(ProgramParts {
      table: Arc::clone(&self.table),
      code: self.w.words().to_vec(),
      refusals: self.refusals.clone(),
      inputs: self.inputs,
      writes: self.writes,
      own_codes: self.own_codes,
      uses_disk: self.uses_disk,
    })))
  }

  /// The writer's label behind `label`, if it is this builder's: its
  /// mark is ours and its number one handed out.
  fn place_for(&self, label: Label) -> Option<Place> {
    let number = label.0 & 0xFFFF;
    if label.0 >> 16 != self.mark || number as usize >= self.used.len() {
      return None;
    }
    Some(Place::from_u32(number))
  }

  /// A register there is.
  fn check_reg(&mut self, reg: u32) -> bool {
    if reg >= IC_PROGRAM_REGISTERS {
      self.fail(err::IC_ERROR_BAD_PROGRAM);
      return false;
    }
    true
  }

  /// An integer column of the table, of 32 or 64 bits where it is
  /// written.
  fn check_column(&mut self, column: u32, written: bool) -> bool {
    let (fits, on_disk) = match self.table.field(column) {
      Some(attr) => (
        if written {
          program::is_wide_integer(attr)
        } else {
          program::is_integer(attr)
        },
        attr.storage_kind == table_props::IC_STORAGE_DISK,
      ),
      None => {
        self.fail(err::IC_ERROR_NO_SUCH_FIELD);
        return false;
      }
    };
    if !fits {
      self.fail(err::IC_ERROR_NOT_SUPPORTED);
      return false;
    }
    self.uses_disk |= on_disk;
    true
  }

  /// The writer's label for a jump to `to`: a label of this builder,
  /// and not one placed at the jump itself.
  fn check_jump(&mut self, to: Label) -> Option<Place> {
    let place = match self.place_for(to) {
      Some(place) if self.w.place_of(place) != Some(self.w.len() as u32) => {
        place
      }
      _ => {
        self.fail(err::IC_ERROR_BAD_PROGRAM);
        return None;
      }
    };
    self.used[(to.0 & 0xFFFF) as usize] = true;
    Some(place)
  }

  /// Keep the first error.
  fn fail(&mut self, code: i32) {
    if self.error.is_none() {
      self.error = Some(IcError::new(code));
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::program::check_program;
  use crate::program::run_model_code;
  use crate::program::test_table;
  use crate::program::test_table_with_disk;

  // The tests' table (program.rs): id INT, val INT NULL, name VARCHAR,
  // code CHAR, big BIGINT UNSIGNED NULL, n INT, w INT NULL.
  const VAL: u32 = 1;
  const NAME: u32 = 2;
  const BIG: u32 = 4;
  const N: u32 = 5;
  const W: u32 = 6;

  /// w = val × n by adding val n times, refusing a row whose val is
  /// NULL and, with 6001, one whose n is negative.
  fn times() -> InstructionBuilder {
    let mut b = InstructionBuilder::new(&test_table());
    let again = b.label();
    let done = b.label();
    let no_value = b.label();
    let negative = b.label();
    b.read_column(0, VAL);
    b.jump_if_null(0, no_value);
    b.read_column(1, N);
    b.set_integer(2, 0);
    b.set_integer(3, 0);
    b.jump_if(Cmp::Lt, 1, 3, negative);
    b.place(again);
    b.jump_if(Cmp::Ge, 3, 1, done);
    b.add(2, 2, 0);
    b.add_integer(3, 3, 1);
    b.jump(again);
    b.place(done);
    b.write_column(W, 2);
    b.accept();
    b.place(no_value);
    b.refuse();
    b.place(negative);
    b.refuse_with(6001);
    b
  }

  #[test]
  fn a_loop_jumps_backwards_to_a_label_placed_before() {
    let p = times().finish().expect("finish");
    assert_eq!(
      p.words(),
      [
        0x0001_0001,          // r0 = val
        0x000B_000A,          // if r0 is NULL, 11 on, to the refusal
        0x0005_0041,          // r1 = n
        0x0000_0084,          // r2 = 0
        0x0000_00C4,          // r3 = 0
        0x0008_064E,          // if r1 < r3, 8 on, to the code
        0x0004_02D1,          // if r3 >= r1, 4 on, to the write
        0x0002_0087,          // r2 = r2 + r0
        0x0001_30C7 | 0x8000, // r3 = r3 + 1, instruction 71
        0x8003_0009,          // 3 back, to the comparison
        0x0006_0082,          // w = r2
        0x0000_0012,          // accept
        0x0272_0013,          // refuse, 626 until the operation says
        0x1771_0013,          // refuse with 6001
      ]
    );
    assert!(p.writes());
    assert!(p.own_codes());
    assert!(!p.uses_disk());
    assert_eq!(p.inputs(), 0);
    // The plain refusal takes the operation's code, the other keeps
    // its own.
    let mut out: Vec<u32> = Vec::new();
    p.code_into(interp::IC_REFUSE_CONDITION, &mut out);
    assert_eq!(out[12], 0x1770_0013);
    assert_eq!(out[13], 0x1771_0013);
  }

  #[test]
  fn the_loop_runs_on_the_model() {
    let p = times().finish().expect("finish");
    let inputs = [0i64; 16];
    let row = |val: Option<i64>, n: i64| {
      [Some(1), val, None, None, None, Some(n), None]
    };
    let (refused, writes) =
      run_model_code(p.words(), &row(Some(7), 6), &inputs).expect("ran");
    assert_eq!(refused, None);
    assert_eq!(writes, [(W, Some(42))]);
    let (refused, writes) =
      run_model_code(p.words(), &row(Some(7), 0), &inputs).expect("ran");
    assert_eq!(refused, None);
    assert_eq!(writes, [(W, Some(0))]);
    let (refused, _) =
      run_model_code(p.words(), &row(None, 6), &inputs).expect("ran");
    assert_eq!(refused, Some(626));
    let (refused, _) =
      run_model_code(p.words(), &row(Some(7), -1), &inputs).expect("ran");
    assert_eq!(refused, Some(6001));
    // Six instructions before the loop, four each turn and three
    // after: 3997 turns are within the 16 000 the model runs, and one
    // more is not. The data node counts some instructions for more: it
    // was seen to run 3996 turns and not 3997 (chapter 05 §9.4).
    assert!(run_model_code(p.words(), &row(Some(1), 3997), &inputs).is_ok());
    let e = run_model_code(p.words(), &row(Some(1), 3998), &inputs)
      .expect_err("long");
    assert_eq!(e, "886");
  }

  /// w = the bits set in val, counted in a loop: the lowest bit added,
  /// val shifted right by one, until nothing is left. A NULL val is
  /// refused, and so is a negative one, whose sign would never shift
  /// out.
  fn count_bits() -> InstructionBuilder {
    let mut b = InstructionBuilder::new(&test_table());
    let again = b.label();
    let done = b.label();
    let no = b.label();
    b.read_column(0, VAL);
    b.jump_if_null(0, no);
    b.set_integer(1, 0);
    b.set_integer(2, 0);
    b.jump_if(Cmp::Lt, 0, 1, no);
    b.place(again);
    b.jump_if(Cmp::Eq, 0, 1, done);
    b.bit_and_integer(3, 0, 1);
    b.add(2, 2, 3);
    b.shift_right_integer(0, 0, 1);
    b.jump(again);
    b.place(done);
    b.write_column(W, 2);
    b.accept();
    b.place(no);
    b.refuse();
    b
  }

  #[test]
  fn bits_are_counted_by_a_loop_of_and_and_shift() {
    let p = count_bits().finish().expect("finish");
    assert_eq!(p.words()[6], 0x0001_8000 | 32 | (3 << 12));
    assert_eq!(p.words()[8], 0x0001_8000 | 29);
    let inputs = [0i64; 16];
    for (val, bits) in [
      (0i64, 0i64),
      (1, 1),
      (0b1011_0110, 5),
      (i32::MAX as i64, 31),
    ] {
      let row = [Some(1), Some(val), None, None, None, Some(0), None];
      let (refused, writes) =
        run_model_code(p.words(), &row, &inputs).expect("ran");
      assert_eq!(refused, None, "{}", val);
      assert_eq!(writes, [(W, Some(bits))], "{}", val);
    }
    let row = [Some(1), Some(-4), None, None, None, Some(0), None];
    let (refused, _) = run_model_code(p.words(), &row, &inputs).expect("ran");
    assert_eq!(refused, Some(626));
  }

  #[test]
  fn each_bitwise_instruction_does_what_it_says_on_the_model() {
    // w = the instruction's result of val and n, or of val and 12.
    type Write = fn(&mut InstructionBuilder);
    let run = |write: Write, val: i64, n: i64| {
      let mut b = InstructionBuilder::new(&test_table());
      b.read_column(0, VAL);
      b.read_column(1, N);
      write(&mut b);
      b.write_column(W, 2);
      b.accept();
      let p = b.finish().expect("finish");
      let row = [Some(1), Some(val), None, None, None, Some(n), None];
      run_model_code(p.words(), &row, &[0i64; 16]).map(|(_, w)| w[0].1)
    };
    let val: i64 = 0b1100;
    let n: i64 = 0b1010;
    assert_eq!(run(|b| b.bit_and(2, 0, 1), val, n), Ok(Some(0b1000)));
    assert_eq!(run(|b| b.bit_or(2, 0, 1), val, n), Ok(Some(0b1110)));
    assert_eq!(run(|b| b.bit_xor(2, 0, 1), val, n), Ok(Some(0b0110)));
    assert_eq!(run(|b| b.bit_not(2, 0), val, n), Ok(Some(!0b1100)));
    assert_eq!(run(|b| b.shift_left(2, 0, 1), 3, 4), Ok(Some(48)));
    assert_eq!(run(|b| b.shift_right(2, 0, 1), 48, 4), Ok(Some(3)));
    // A signed value keeps its sign.
    assert_eq!(run(|b| b.shift_right(2, 0, 1), -48, 4), Ok(Some(-3)));
    assert_eq!(run(|b| b.bit_and_integer(2, 0, 12), 10, 0), Ok(Some(8)));
    assert_eq!(run(|b| b.bit_or_integer(2, 0, 12), 10, 0), Ok(Some(14)));
    assert_eq!(run(|b| b.bit_xor_integer(2, 0, 12), 10, 0), Ok(Some(6)));
    assert_eq!(
      run(|b| b.shift_left_integer(2, 0, 12), 1, 0),
      Ok(Some(4096))
    );
    assert_eq!(
      run(|b| b.shift_right_integer(2, 0, 12), 8192, 0),
      Ok(Some(2))
    );
    // A shift by 64 bits, or by a negative count, is the data node's
    // error; by an integer, the finish's.
    assert_eq!(
      run(|b| b.shift_left(2, 0, 1), 1, 64),
      Err("891".to_string())
    );
    assert_eq!(
      run(|b| b.shift_right(2, 0, 1), 1, -1),
      Err("891".to_string())
    );
    assert_eq!(
      refused(|b| {
        b.shift_left_integer(0, 0, 64);
        b.accept();
      }),
      err::IC_ERROR_BAD_PROGRAM
    );
    assert_eq!(
      refused(|b| {
        b.bit_xor(0, 8, 0);
        b.accept();
      }),
      err::IC_ERROR_BAD_PROGRAM
    );
  }

  #[test]
  fn inputs_and_the_disk_are_kept_for_the_operation() {
    let mut b = InstructionBuilder::new(&test_table_with_disk(VAL));
    let low = b.label();
    b.read_column(0, VAL);
    b.read_input(1, 3);
    b.read_input(2, 0);
    b.subtract(0, 0, 2);
    b.subtract_integer(0, 0, 5);
    b.jump_if(Cmp::Lt, 0, 1, low);
    b.accept();
    b.place(low);
    b.refuse();
    let p = b.finish().expect("finish");
    assert_eq!(p.inputs(), 0b1001);
    assert!(p.uses_disk());
    assert!(!p.writes());
    assert!(!p.own_codes());
    let t = test_table_with_disk(VAL);
    // A scan takes it, loop or not, and so does a read.
    assert!(check_program(&p, &t, false, true).is_ok());
    assert!(check_program(&p, &t, false, false).is_ok());
    let mut inputs = [0i64; 16];
    inputs[0] = 10;
    inputs[3] = 100;
    let row = |val: i64| [Some(1), Some(val), None, None, None, Some(0), None];
    let (refused, _) =
      run_model_code(p.words(), &row(115), &inputs).expect("ran");
    assert_eq!(refused, None);
    let (refused, _) =
      run_model_code(p.words(), &row(114), &inputs).expect("ran");
    assert_eq!(refused, Some(626));
  }

  #[test]
  fn a_program_of_its_own_codes_or_its_writes_is_kept_from_where_it_cannot_run()
  {
    let t = test_table();
    let p = times().finish().expect("finish");
    // It writes: only an update. It has a code of its own: no scan.
    assert!(check_program(&p, &t, true, false).is_ok());
    let e = check_program(&p, &t, false, false).expect_err("a read");
    assert_eq!(e.code, err::IC_ERROR_NOT_SUPPORTED);
    let mut b = InstructionBuilder::new(&t);
    b.refuse_with(6999);
    let p = b.finish().expect("finish");
    let e = check_program(&p, &t, false, true).expect_err("a scan");
    assert_eq!(e.code, err::IC_ERROR_NOT_SUPPORTED);
    assert!(check_program(&p, &t, false, false).is_ok());
  }

  /// The error the finish gives a program `write` wrote.
  fn refused(write: fn(&mut InstructionBuilder)) -> i32 {
    let mut b = InstructionBuilder::new(&test_table());
    write(&mut b);
    b.finish().expect_err("refused").code
  }

  #[test]
  fn what_is_wrong_is_refused_at_the_finish() {
    let bad = err::IC_ERROR_BAD_PROGRAM;
    // Nothing; no ending; an ending and then more; a label placed
    // last.
    assert_eq!(refused(|_| {}), bad);
    assert_eq!(refused(|b| b.set_integer(0, 1)), bad);
    assert_eq!(
      refused(|b| {
        b.accept();
        b.set_integer(0, 1);
      }),
      bad
    );
    assert_eq!(
      refused(|b| {
        let end = b.label();
        b.jump(end);
        b.accept();
        b.place(end);
      }),
      bad
    );
    // A ninth register, a seventeenth input.
    assert_eq!(
      refused(|b| {
        b.set_integer(8, 1);
        b.accept();
      }),
      bad
    );
    assert_eq!(
      refused(|b| {
        b.read_input(0, 16);
        b.accept();
      }),
      bad
    );
    // A label never placed, placed twice, not the builder's, and a
    // jump to itself.
    assert_eq!(
      refused(|b| {
        let never = b.label();
        b.jump_if_null(0, never);
        b.accept();
      }),
      bad
    );
    assert_eq!(
      refused(|b| {
        let twice = b.label();
        b.place(twice);
        b.accept();
        b.place(twice);
        b.accept();
      }),
      bad
    );
    assert_eq!(
      refused(|b| {
        b.jump(Label::from_u32(3));
        b.accept();
      }),
      bad
    );
    // Another builder's label, of the same number as one of ours, is
    // not taken for ours.
    let mut other = InstructionBuilder::new(&test_table());
    let theirs = other.label();
    let mut b = InstructionBuilder::new(&test_table());
    let ours = b.label();
    assert_eq!(theirs.as_u32() & 0xFFFF, ours.as_u32() & 0xFFFF);
    assert_ne!(theirs, ours);
    b.jump_if_null(0, theirs);
    b.accept();
    b.place(ours);
    b.refuse_with(6002);
    assert_eq!(b.finish().expect_err("another's label").code, bad);
    let mut b = InstructionBuilder::new(&test_table());
    let _ours = b.label();
    b.place(theirs);
    b.accept();
    assert_eq!(b.finish().expect_err("another's label placed").code, bad);
    assert_eq!(
      refused(|b| {
        let here = b.label();
        b.place(here);
        b.jump(here);
      }),
      bad
    );
    // Codes that are not the application's.
    for code in [0u32, 626, 6000, 7000] {
      let mut b = InstructionBuilder::new(&test_table());
      b.refuse_with(code);
      let e = b.finish().expect_err("code");
      assert_eq!(e.code, bad, "code {}", code);
    }
    // A column the table lacks; one that is no integer; one of 64 bits
    // unsigned read, which a register takes; a column written that is
    // not of 32 or 64 bits there is none of here, so the VARCHAR.
    assert_eq!(
      refused(|b| {
        b.read_column(0, 99);
        b.accept();
      }),
      err::IC_ERROR_NO_SUCH_FIELD
    );
    assert_eq!(
      refused(|b| {
        b.read_column(0, NAME);
        b.accept();
      }),
      err::IC_ERROR_NOT_SUPPORTED
    );
    assert_eq!(
      refused(|b| {
        b.set_null(0);
        b.write_column(NAME, 0);
        b.accept();
      }),
      err::IC_ERROR_NOT_SUPPORTED
    );
    let mut b = InstructionBuilder::new(&test_table());
    b.read_column(0, BIG);
    b.write_column(BIG, 0);
    b.accept();
    assert!(b.finish().is_ok());
    // A label not used need not be placed.
    let mut b = InstructionBuilder::new(&test_table());
    let _spare = b.label();
    b.accept();
    assert!(b.finish().is_ok());
  }

  #[test]
  fn a_program_too_long_is_called_too_long() {
    // 32 000 words are taken, one more is not; nor is a jump back over
    // more than a jump reaches, which is first of all too long.
    let build = |words: usize| {
      let mut b = InstructionBuilder::new(&test_table());
      let top = b.label();
      b.place(top);
      let mut i: usize = 2;
      while i < words {
        b.set_integer(0, 1);
        i += 1;
      }
      b.jump(top);
      b.accept();
      b.finish()
    };
    let p = build(32_000).expect("the longest");
    assert_eq!(p.words().len(), 32_000);
    assert_eq!(p.words()[31_998], 0x8000_0009 | (31_998 << 16));
    let e = build(32_001).expect_err("one more");
    assert_eq!(e.code, err::IC_ERROR_CONDITION_TOO_COMPLEX);
    let e = build(40_000).expect_err("a jump too far");
    assert_eq!(e.code, err::IC_ERROR_CONDITION_TOO_COMPLEX);
  }
}
