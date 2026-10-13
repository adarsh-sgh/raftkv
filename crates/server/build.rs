fn main() {
    if std::env::var_os("PROTOC").is_none() {
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
        // SAFETY: build scripts are single-threaded.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }
    tonic_prost_build::configure()
        .compile_protos(&["proto/raftkv.proto"], &["proto"])
        .expect("compile protos");
}
