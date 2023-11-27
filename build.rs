fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ARCH");

    if target_os.as_str() == "linux" && target_arch.as_str() == "aarch64" {
        println!(
            "cargo:warning=Building for aarch64, which needs dynamic TLS enabled for mimalloc"
        );
        println!("cargo:rustc-cfg=NEEDS_DYNAMIC_TLS");
    }
}
