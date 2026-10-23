const BANNER: &str = r#"
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

pub fn print_banner(role: &str) {
    println!("{}", BANNER);
    println!(
        "         Lyra :: {} :: v{}",
        role,
        env!("CARGO_PKG_VERSION")
    );
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prints_a_banner_for_the_given_role() {
        print_banner("catalog");
    }
}
