@0xf2757f551645f46c;

# TODO: It's clear from initial benchmarking that this structure isn't what
# we'll want. In addition to the duplication of key, etc data, there's more
# runtime overhead to following the capnp pointers around. Revisit this.
struct Batch {
  lower @0 :UInt64;
  upper @1 :UInt64;
  since @2 :UInt64;
  tuples @3 :List(Tuple);
}

struct Tuple {
  key @0 :Data;
  val @1 :Data;
  ts @2 :UInt64;
  diff @3 :Int64;
}

# Yup, okay. This seems much better than above. Interesting to note that at this
# point, we don't really see any particular advantage from using Cap'n Proto
# over something like protobuf.
struct BatchColumnar {
  lower @0 :UInt64;
  upper @1 :UInt64;
  since @2 :UInt64;

  keyData @3 :Data;
  keyDataOffsets @4 :List(UInt32);

  valData @5 :Data;
  valDataOffsets @6 :List(UInt32);

  timestamps @7 :List(UInt64);
  diffs @8 :List(Int64);

  # TODO: I suspect there's something better to do here to optimize the various
  # Cursor methods this ends up implementing.
  keyIdxByIdx @9 :List(UInt32);
  valIdxByIdx @10 :List(UInt32);
}
