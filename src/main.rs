use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write, BufReader};
use std::time::Instant;
use std::str;
use bzip2::read::MultiBzDecoder;

fn convert_seconds_to_human_readable(s: u64) -> String {
    let hours = s / 3600;
    let minutes = (s / 60) % 60;
    let seconds = s % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

enum ReaderType {
    Compressed(quick_xml::Reader<BufReader<bzip2::read::MultiBzDecoder<File>>>),
    Uncompressed(quick_xml::Reader<BufReader<File>>),
}

trait XmlReader {
    fn config_mut(&mut self) -> &mut quick_xml::reader::Config;
    fn read_event_into<'b>(&mut self, buf: &'b mut Vec<u8>) -> quick_xml::Result<Event<'b>>;
    fn buffer_position(&self) -> u64;
}

impl XmlReader for ReaderType {
    fn config_mut(&mut self) -> &mut quick_xml::reader::Config {
        match self {
            ReaderType::Compressed(r) => r.config_mut(),
            ReaderType::Uncompressed(r) => r.config_mut(),
        }
    }

    fn read_event_into<'b>(&mut self, buf: &'b mut Vec<u8>) -> quick_xml::Result<Event<'b>> {
        match self {
            ReaderType::Compressed(r) => r.read_event_into(buf),
            ReaderType::Uncompressed(r) => r.read_event_into(buf),
        }
    }

    fn buffer_position(&self) -> u64 {
        match self {
            ReaderType::Compressed(r) => r.buffer_position(),
            ReaderType::Uncompressed(r) => r.buffer_position(),
        }
    }
}

fn process_xml_file<const READ_COMPRESSED: bool>(
    xml_file_name: &str,
    max_pages: Option<usize>,
) -> (
    HashMap<usize, Vec<usize>>,
    HashMap<String, usize>,
) {
    let mut title_index_map: HashMap<String, usize> = HashMap::new();
    let mut adjacency_list_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut redirect_map: HashMap<String, String> = HashMap::new();
    let mut title: Option<String> = None;
    let mut processing_page = false;
    let mut processing_title = false;
    let mut processing_text = false;

    println!("Reading: {}", xml_file_name);
    let st = Instant::now();
    let mut intermediate_time = st;
    let num_pages_to_issue_update = 10000;
    let mut page_index = 0;
    let mut real_page_index = 0;  // Pages that are not redirects

    let file = File::open(xml_file_name).unwrap();
    let mut reader: ReaderType = if READ_COMPRESSED {
        let bz2_reader = MultiBzDecoder::new(file);
        ReaderType::Compressed(Reader::from_reader(BufReader::new(bz2_reader)))
    } else {
        ReaderType::Uncompressed(Reader::from_reader(BufReader::new(file)))
    };

    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let patt = Regex::new(r#"\[\[([\w:;,. \-\+/#$%\^&*?<>"'()]+)(?:\|[\w:;,. \-\+/#$%\^&*?<>"'()]+)?\]\]"#).unwrap();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Empty(ref e)) => {
                if e.starts_with(b"redirect") {
                    let text = String::from_utf8_lossy(e.name().0);
                    redirect_map.insert(title.clone().unwrap(), text.to_string());
                }
            }
            Ok(Event::Start(ref e)) => {
                if e.ends_with(b"page") {
                    processing_page = true;
                } else if e.ends_with(b"title") {
                    processing_title = true;
                }  else if e.starts_with(b"text") {
                    processing_text = true;
                }
            }
            Ok(Event::End(ref e)) => {
                if e.ends_with(b"page") {
                    processing_page = false;
                    page_index += 1;
                    if page_index % num_pages_to_issue_update == 0 {
                        let now = Instant::now();
                        println!(
                            "processed {} pages in {} seconds ({} total pages in {})...",
                            num_pages_to_issue_update,
                            now.duration_since(intermediate_time).as_secs(),
                            page_index,
                            convert_seconds_to_human_readable(now.duration_since(st).as_secs())
                        );
                        intermediate_time = now;
                    }
                } else if e.ends_with(b"title") {
                    processing_title = false;
                }  else if e.starts_with(b"text") {
                    processing_text = false;
                }

            }
            Ok(Event::Text(e)) => {
                if processing_page {
                    if processing_title {
                        let text = String::from(e.unescape().unwrap());
                        title = Some(text);
                    } else if processing_text {
                        let adj_list: Vec<String> = patt
                            .captures_iter(e.unescape().unwrap().as_ref())
                            .map(|cap| cap[1].to_string())
                            .collect();
                        adjacency_list_map.insert(title.clone().unwrap(), adj_list);
                        title_index_map.insert(title.clone().unwrap(), real_page_index);
                        real_page_index += 1;
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => {}
        }

        if let Some(max_pages) = max_pages {
            if page_index >= max_pages {
                break;
            }
        }

        buf.clear();
    }

    println!("post-processing redirects...");
    let pt = Instant::now();

    for adj_list in adjacency_list_map.values_mut() {
        for k in adj_list.iter_mut() {
            while let Some(new_k) = redirect_map.get(k) {
                *k = new_k.clone();
            }
        }
    }

    let mut index_adj_map: HashMap<usize, Vec<usize>> = HashMap::new();
    if max_pages.is_some() {
        for (title, adj_list) in adjacency_list_map.iter() {
            index_adj_map.insert(
                title_index_map[title],
                adj_list
                    .iter()
                    .filter_map(|k| title_index_map.get(k).cloned())
                    .collect(),
            );
        }
    } else {
        for (title, adj_list) in adjacency_list_map.iter() {
            index_adj_map.insert(
                title_index_map[title],
                adj_list.iter().map(|k| title_index_map[k]).collect(),
            );
        }
    }

    println!(
        "Done postprocessing in {} seconds.",
        pt.elapsed().as_secs()
    );
    println!("Done processing XML in {} seconds.", st.elapsed().as_secs());
    (index_adj_map, title_index_map)
}

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
        (adjacency_list_map, title_index_map) = process_xml_file::<true>(xml_file_name, max_pages);
    } else {
        (adjacency_list_map, title_index_map) = process_xml_file::<false>(xml_file_name, max_pages);
    }

    let csv_out = File::create(csv_out_file_name)?;
    let mut csv_writer = BufWriter::new(csv_out);
    for (src, destinations) in adjacency_list_map {
        for d in destinations {
            writeln!(csv_writer, "{} {}", src, d)?;
        }
    }

    let names_out = File::create(node_names_out_file_name)?;
    let mut names_writer = BufWriter::new(names_out);
    for (key, value) in title_index_map {
        writeln!(names_writer, "{}\t\"{}\"", value, key)?;
    }

    Ok(())
}

