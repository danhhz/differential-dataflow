@0xf2757f551645f46c;

# TODO: Should we reduce the redundant storage of keys somehow?
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
