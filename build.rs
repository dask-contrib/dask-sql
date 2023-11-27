fn main() {
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    println!("cargo:rerun-if-env-changed=TARGET_ARCH");

    match target_arch.as_str() {
        "aarch64" => {
            println!("cargo:warning=Building for aarch64, which needs dynamic TLS enabled for mimalloc");
            println!("cargo:rustc-cfg=NEEDS_DYNAMIC_TLS");
        }
        _ => {}
    }
}