#![allow(clippy::too_many_arguments, clippy::type_complexity, clippy::needless_range_loop, clippy::enum_variant_names)]
mod cc;
mod channel;
mod cli;
mod crypto;
mod ctl;
mod engine;
#[cfg(feature = "gui")]
mod gui;
mod log;
mod node;
mod stream;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("daemon") => {
            let n = match node::Node::start() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("deckdrop: не удалось запустить службу: {e}");
                    std::process::exit(1);
                }
            };
            if let Err(e) = ctl::serve(n) {
                eprintln!("deckdrop: {e}");
                std::process::exit(1);
            }
        }
        None => {
            #[cfg(feature = "gui")]
            if let Err(e) = gui::run() {
                eprintln!("deckdrop: {e}");
                std::process::exit(1);
            }
            #[cfg(not(feature = "gui"))]
            eprintln!("эта сборка без окна, см. deckdrop --help");
        }
        Some(_) => {
            if let Err(e) = cli::run(&args) {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    }
}
