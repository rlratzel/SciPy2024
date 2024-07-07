mod content;
use std::env;
use std::fs::File;
use std::io::{self, Write};


fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let xml_file_name = &args[1];
    let csv_out_file_name = &args[2];
    let node_names_out_file_name = &args[3];
    let max_pages = if args.len() > 4 {
        Some(args[4].parse::<usize>().unwrap())
    } else {
        None
    };

    let adjacency_list_map;
    let title_index_map;
    if xml_file_name.ends_with(".bz2") {
        (adjacency_list_map, title_index_map) = content::process_xml_file::<true>(xml_file_name, max_pages);
    } else {
        (adjacency_list_map, title_index_map) = content::process_xml_file::<false>(xml_file_name, max_pages);
    }

    let csv_out = File::create(csv_out_file_name)?;
    let mut csv_writer = io::BufWriter::new(csv_out);
    for (src, destinations) in adjacency_list_map {
        for d in destinations {
            writeln!(csv_writer, "{} {}", src, d)?;
        }
    }

    let names_out = File::create(node_names_out_file_name)?;
    let mut names_writer = io::BufWriter::new(names_out);
    for (key, value) in title_index_map {
        writeln!(names_writer, "{}\t\"{}\"", value, key)?;
    }

    Ok(())
}

