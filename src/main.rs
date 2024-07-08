mod content;
mod revisions;
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{self, Write};


#[derive(Parser)]
#[command(version, about, author)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}


#[derive(Subcommand)]
enum Commands {
    /// Process a Wikipedia article dump.
    Articles {
        /// The input file containing the XML dump. May be compressed with bzip2.
        input_file: String,
        /// The output file containing article metadata
        articles_output_file: String,
        /// The output file containing the graph
        graph_output_file: String,
        /// The maximum number of pages to process
        #[clap(short, long)]
        max_pages: Option<usize>,
    },
    /// Process a Wikipedia article revisions dump.
    Revisions {
        /// The input file containing the XML dump. May be compressed with bzip2.
        input_file: String,
        /// The output file containing the editors of each article
        editors_output_file: String,
        /// The maximum number of pages to process
        #[clap(short, long)]
        max_pages: Option<usize>,
    },
}


fn main() -> io::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Articles { input_file, articles_output_file, graph_output_file, max_pages } => {
            let adjacency_list_map;
            let title_index_map;
            if input_file.ends_with(".bz2") {
                (adjacency_list_map, title_index_map) = content::process_articles::<true>(input_file, max_pages);
            } else {
                (adjacency_list_map, title_index_map) = content::process_articles::<false>(input_file, max_pages);
            }

            let csv_out = File::create(articles_output_file)?;
            let mut csv_writer = io::BufWriter::new(csv_out);
            for (src, destinations) in adjacency_list_map {
                for d in destinations {
                    writeln!(csv_writer, "{} {}", src, d)?;
                }
            }

            let names_out = File::create(graph_output_file)?;
            let mut names_writer = io::BufWriter::new(names_out);
            for (key, value) in title_index_map {
                writeln!(names_writer, "{}\t\"{}\"", value, key)?;
            }

        },
        Commands::Revisions { input_file, editors_output_file, max_pages } => {
            let title_editors_map;
            if input_file.ends_with(".gz") {
                title_editors_map = revisions::process_revisions::<true>(input_file, max_pages);
            } else {
                panic!("Only gz compressed files are supported for revisions, uncompressed files are too large.");
            }

            let csv_out = File::create(editors_output_file)?;
            let mut csv_writer = io::BufWriter::new(csv_out);
            for (title, editors) in title_editors_map {
                for e in editors {
                    writeln!(csv_writer, "{} {}", title, e)?;
                }
            }
        },
    }
    Ok(())
}
