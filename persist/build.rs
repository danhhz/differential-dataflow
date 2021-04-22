fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src")
        .file("src/trace.capnp")
        .default_parent_module(vec!["capnpgen".into()])
        .run()
        .expect("capnp schema compilation");
}
