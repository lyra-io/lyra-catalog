// Keep the startup artwork consistent with lyra-stream/cli/src/banner.rs.
pub const BANNER: &str = r#"
                       █████████
                   █████████  ████
                ████████  ███  ███
             ████████  ███ ███  ███ █
          █████████ ███ ██  ██  ██  ██
         ███████     ██ ███ ██  ██  ██
        ███████      ██ ██  ██ ███ ███
       █████ ██      ██ ██ ██ ███ ███ ██
      ███ ████         ██ ██  ██ ███ ███
                      ██ ██ ███ ███ ███
                     █████ ██  ██  ██
                     ███████ ███ ███ ██
                   ███████████ ███ ███
                     ███████████ ████
                           ██ ████
"#;

pub fn print_banner() {
    println!("{BANNER}");
    println!("         Lyra :: Catalog :: v{}", env!("CARGO_PKG_VERSION"));
    println!();
}
