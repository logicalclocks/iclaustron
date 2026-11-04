// Copyright (c) 2007-2015 Hopsworks AB.
// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! The object map (`IC_DYNAMIC_PTR_ARRAY`,
//! `legacy-c/util/ic_dyn_array.c`): objects in, ids out, and the object
//! back from the id.
//!
//! An id is an index plus a generation, in 64 bits: the low 24 bits
//! index the slot, the high 40 count how many times the slot has been
//! used. An id is never followed once its object is gone (the author,
//! 2026-10-23): a stale id names a generation that no longer matches,
//! and a slot whose generation would wrap is retired rather than used
//! again, so no id ever names a second object. At a million uses a
//! second of one slot, a retirement comes after twelve days.
//!
//! What travels in a signal is a 32-bit word made from the id
//! ([`PtrId::wire_word`]): the index and eight bits of the generation,
//! never 0, and never a raw pointer, which the data node could make us
//! follow. A reply's word finds its object again with
//! [`PtrArray::id_of_wire`]. Eight bits cannot tell every late reply
//! from a new one, so a reply is also checked against what it is about,
//! its transaction's id, as the protocol's replies are.

use ic_port::err;
use ic_port::IcError;

/// Highest slot index, so also the largest number of live objects.
pub const IC_PTR_ARRAY_MAX_ENTRIES: u32 = 0x00FF_FFFF;

const IC_INDEX_MASK: u64 = 0x00FF_FFFF;
const IC_GENERATION_SHIFT: u32 = 24;
/// The last generation a slot is used in; the next free retires it.
const IC_MAX_GENERATION: u64 = (1 << 40) - 1;
/// A slot's generation once it is retired: no id names it.
const IC_RETIRED: u64 = 0;
const IC_WIRE_INDEX_MASK: u32 = 0x00FF_FFFF;
const IC_WIRE_GENERATION_SHIFT: u32 = 24;
/// The generations a wire word tells apart: 1 to 255, never 0.
const IC_WIRE_GENERATIONS: u64 = 255;

/// The id of an object in a [`PtrArray`].
///
/// A valid id is never 0, because generations start at 1, so 0 can be
/// used as "no object" the way `IC_NULL_PTR` is in the NDB protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PtrId(u64);

impl PtrId {
  /// The id that names no object.
  pub const NONE: PtrId = PtrId(0);

  /// The id as a 64-bit word, for a language binding.
  pub fn as_u64(&self) -> u64 {
    self.0
  }

  /// An id from its 64-bit word, as a language binding hands it back.
  /// Whether it still names a live object is answered by
  /// [`PtrArray::get`].
  pub fn from_u64(word: u64) -> PtrId {
    PtrId(word)
  }

  /// The 32-bit word that goes into a signal: the index, and the
  /// generation's low part in the top eight bits.
  pub fn wire_word(&self) -> u32 {
    self.index()
      | (wire_generation(self.generation()) << IC_WIRE_GENERATION_SHIFT)
  }

  /// The slot this id names.
  pub fn index(&self) -> u32 {
    (self.0 & IC_INDEX_MASK) as u32
  }

  /// The generation this id was issued in.
  pub fn generation(&self) -> u64 {
    self.0 >> IC_GENERATION_SHIFT
  }

  /// True for an id that cannot name any object.
  pub fn is_none(&self) -> bool {
    self.0 == 0
  }
}

/// The part of a generation a wire word carries: 1 to 255, so that a
/// valid word is never 0.
fn wire_generation(generation: u64) -> u32 {
  if generation == 0 {
    return 0;
  }
  ((generation - 1) % IC_WIRE_GENERATIONS + 1) as u32
}

fn id_of(index: u32, generation: u64) -> PtrId {
  PtrId(index as u64 | (generation << IC_GENERATION_SHIFT))
}

struct Slot<T> {
  /// The generation of the slot's next or current object; `IC_RETIRED`
  /// once it is used no more.
  generation: u64,
  value: Option<T>,
}

/// A table of objects addressed by an id.
///
/// Rust notes for C readers: `PtrArray<T>` holds objects of one type `T`,
/// the way `Vec<T>` does. The C version stored `void*` and the caller
/// remembered what it had put in; here the compiler remembers.
pub struct PtrArray<T> {
  slots: Vec<Slot<T>>,
  free_list: Vec<u32>,
  num_entries: u32,
}

impl<T> PtrArray<T> {
  /// An empty table.
  pub fn new() -> PtrArray<T> {
    PtrArray {
      slots: Vec::new(),
      free_list: Vec::new(),
      num_entries: 0,
    }
  }

  /// An empty table with room for `capacity` objects reserved.
  pub fn with_capacity(capacity: usize) -> PtrArray<T> {
    PtrArray {
      slots: Vec::with_capacity(capacity),
      free_list: Vec::new(),
      num_entries: 0,
    }
  }

  /// Store an object and return its id (`ic_insert_ptr`).
  pub fn insert(&mut self, value: T) -> Result<PtrId, IcError> {
    if let Some(index) = self.free_list.pop() {
      let slot = &mut self.slots[index as usize];
      slot.value = Some(value);
      self.num_entries += 1;
      return Ok(id_of(index, slot.generation));
    }
    let index = self.slots.len() as u32;
    if index > IC_PTR_ARRAY_MAX_ENTRIES {
      return Err(IcError::new(err::IC_ERROR_PTR_ARRAY_INDEX_OUT_OF_BOUND));
    }
    // Generations start at 1 so that a valid id is never the word 0.
    self.slots.push(Slot {
      generation: 1,
      value: Some(value),
    });
    self.num_entries += 1;
    Ok(id_of(index, 1))
  }

  /// The object an id names, or `None` if the id is stale or was never
  /// issued (`ic_get_ptr`).
  pub fn get(&self, id: PtrId) -> Option<&T> {
    let slot = self.slots.get(id.index() as usize)?;
    if slot.generation == IC_RETIRED || slot.generation != id.generation() {
      return None;
    }
    slot.value.as_ref()
  }

  /// The object an id names, for modification.
  pub fn get_mut(&mut self, id: PtrId) -> Option<&mut T> {
    let slot = self.slots.get_mut(id.index() as usize)?;
    if slot.generation == IC_RETIRED || slot.generation != id.generation() {
      return None;
    }
    slot.value.as_mut()
  }

  /// The id of the live object a word received in a signal names: the
  /// word must carry the slot's index and the low part of its current
  /// generation, as [`PtrId::wire_word`] put them there.
  pub fn id_of_wire(&self, word: u32) -> Option<PtrId> {
    let index = word & IC_WIRE_INDEX_MASK;
    let slot = self.slots.get(index as usize)?;
    if slot.generation == IC_RETIRED || slot.value.is_none() {
      return None;
    }
    let generation = word >> IC_WIRE_GENERATION_SHIFT;
    if wire_generation(slot.generation) != generation {
      return None;
    }
    Some(id_of(index, slot.generation))
  }

  /// Take the object out and free the slot for reuse
  /// (`ic_remove_ptr`). The old id is refused from then on.
  pub fn remove(&mut self, id: PtrId) -> Option<T> {
    let index = id.index() as usize;
    let slot = self.slots.get_mut(index)?;
    if slot.generation == IC_RETIRED || slot.generation != id.generation() {
      return None;
    }
    let value = slot.value.take()?;
    self.num_entries -= 1;
    // A slot whose generation would wrap is retired, never used again,
    // so that no id it gave out comes to name another object.
    if slot.generation >= IC_MAX_GENERATION {
      slot.generation = IC_RETIRED;
      return Some(value);
    }
    slot.generation += 1;
    self.free_list.push(index as u32);
    Some(value)
  }

  /// Number of objects currently stored.
  pub fn len(&self) -> u32 {
    self.num_entries
  }

  /// True if no object is stored.
  pub fn is_empty(&self) -> bool {
    self.num_entries == 0
  }

  /// Highest slot ever used (`ic_get_max_index`).
  pub fn max_index(&self) -> u32 {
    self.slots.len() as u32
  }

  /// Drop every object. The slots go too, so ids given out before may
  /// be given out again: for a table whose ids nobody keeps.
  pub fn clear(&mut self) {
    self.slots.clear();
    self.free_list.clear();
    self.num_entries = 0;
  }
}

impl<T> Default for PtrArray<T> {
  fn default() -> PtrArray<T> {
    PtrArray::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn insert_get_remove() {
    let mut a: PtrArray<String> = PtrArray::new();
    assert!(a.is_empty());
    let one = a.insert("one".to_string()).expect("insert");
    let two = a.insert("two".to_string()).expect("insert");
    assert_eq!(a.len(), 2);
    assert_eq!(a.get(one).map(|s| s.as_str()), Some("one"));
    assert_eq!(a.get(two).map(|s| s.as_str()), Some("two"));
    if let Some(s) = a.get_mut(one) {
      s.push_str(" changed");
    }
    assert_eq!(a.get(one).map(|s| s.as_str()), Some("one changed"));
    let taken = a.remove(one).expect("remove");
    assert_eq!(taken, "one changed");
    assert_eq!(a.get(one), None);
    assert_eq!(a.len(), 1);
  }

  #[test]
  fn a_stale_id_is_rejected() {
    let mut a: PtrArray<u32> = PtrArray::new();
    let old = a.insert(1).expect("insert");
    a.remove(old).expect("remove");
    // The slot is reused; the new id differs from the old one.
    let new = a.insert(2).expect("insert");
    assert_eq!(old.index(), new.index());
    assert_ne!(old.as_u64(), new.as_u64());
    assert_eq!(a.get(old), None);
    assert_eq!(a.get(new), Some(&2));
    assert_eq!(a.remove(old), None);
  }

  #[test]
  fn a_stale_id_is_refused_however_often_its_slot_is_reused() {
    let mut a: PtrArray<u32> = PtrArray::new();
    let first = a.insert(0).expect("insert");
    a.remove(first).expect("remove");
    // Last in, first out: the same slot every time, more times than a
    // wire word's eight bits of generation tell apart.
    let mut i: u32 = 1;
    while i < 1000 {
      let id = a.insert(i).expect("insert");
      assert_eq!(id.index(), first.index());
      assert_eq!(a.get(first), None, "use {}", i);
      a.remove(id).expect("remove");
      i += 1;
    }
  }

  #[test]
  fn a_slot_is_retired_before_its_generation_wraps() {
    let mut a: PtrArray<u32> = PtrArray::new();
    let id = a.insert(1).expect("insert");
    a.remove(id).expect("remove");
    a.slots[0].generation = IC_MAX_GENERATION;
    let last = a.insert(2).expect("insert");
    assert_eq!(last.generation(), IC_MAX_GENERATION);
    a.remove(last).expect("remove");
    // The slot is used no more: a new object takes a new one, and no
    // id names the old.
    let next = a.insert(3).expect("insert");
    assert_eq!(next.index(), 1);
    assert_eq!(a.get(last), None);
    assert_eq!(a.get(PtrId::from_u64(0)), None);
    assert_eq!(a.id_of_wire(last.wire_word()), None);
  }

  #[test]
  fn ids_survive_the_wire_and_a_binding() {
    let mut a: PtrArray<u32> = PtrArray::new();
    let id = a.insert(42).expect("insert");
    let word = id.wire_word();
    assert_ne!(word, 0);
    assert_eq!(a.id_of_wire(word), Some(id));
    assert_eq!(a.get(PtrId::from_u64(id.as_u64())), Some(&42));
    assert!(PtrId::from_u64(0).is_none());
    assert!(PtrId::NONE.is_none());
    assert_eq!(a.get(PtrId::NONE), None);
    assert_eq!(a.get(PtrId::from_u64(u64::MAX)), None);
    assert_eq!(a.id_of_wire(0), None);
    assert_eq!(a.id_of_wire(0xFFFF_FFFF), None);
    // A word of a freed object finds nothing.
    a.remove(id).expect("remove");
    assert_eq!(a.id_of_wire(word), None);
  }

  #[test]
  fn the_wire_word_is_as_the_c_had_it_for_a_slot_s_first_uses() {
    // Index in the low 24 bits, the generation in the top eight, for
    // generations 1 to 255; then the eight bits go round, skipping 0.
    assert_eq!(id_of(5, 1).wire_word(), (1 << 24) | 5);
    assert_eq!(id_of(5, 255).wire_word(), (255 << 24) | 5);
    assert_eq!(id_of(5, 256).wire_word(), (1 << 24) | 5);
    assert_eq!(id_of(5, 510).wire_word(), (255 << 24) | 5);
  }

  // The C unit test, test type 4: insert many, remove some, check that
  // what is left still reads back and that slots are reused.
  #[test]
  fn many_inserts_and_removes() {
    let mut a: PtrArray<u32> = PtrArray::new();
    let mut ids: Vec<PtrId> = Vec::new();
    let mut i: u32 = 0;
    while i < 1000 {
      ids.push(a.insert(i).expect("insert"));
      i += 1;
    }
    assert_eq!(a.len(), 1000);
    assert_eq!(a.max_index(), 1000);
    i = 0;
    while i < 1000 {
      assert_eq!(a.get(ids[i as usize]), Some(&i));
      i += 1;
    }
    // Remove every second object.
    i = 0;
    while i < 1000 {
      assert_eq!(a.remove(ids[i as usize]), Some(i));
      i += 2;
    }
    assert_eq!(a.len(), 500);
    i = 1;
    while i < 1000 {
      assert_eq!(a.get(ids[i as usize]), Some(&i));
      i += 2;
    }
    // The freed slots are reused rather than the table growing.
    i = 0;
    while i < 500 {
      a.insert(9999).expect("insert");
      i += 1;
    }
    assert_eq!(a.max_index(), 1000);
    assert_eq!(a.len(), 1000);
    a.clear();
    assert!(a.is_empty());
  }
}
