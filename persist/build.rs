fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src")
        .file("src/trace.capnp")
        .default_parent_module(vec!["trace".into(), "capnpgen".into()])
        .run()
        .expect("capnp schema compilation");
}
