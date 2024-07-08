use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::time::Instant;
use std::str;
use flate2::read::MultiGzDecoder;

fn convert_seconds_to_human_readable(s: u64) -> String {
    let hours = s / 3600;
    let minutes = (s / 60) % 60;
    let seconds = s % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

enum ReaderType {
    Compressed(quick_xml::Reader<BufReader<MultiGzDecoder<File>>>),
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

pub fn process_revisions<const READ_COMPRESSED: bool>(
    xml_file_name: &str,
    max_pages: &Option<usize>,
) -> HashMap<String, Vec<String>>
 {
    let mut title_editors_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut redirect_map: HashMap<String, String> = HashMap::new();
    let mut title: Option<String> = None;
    let mut processing_page = false;
    let mut processing_title = false;
    let mut processing_username = false;

    println!("Reading: {}", xml_file_name);
    let st = Instant::now();
    let mut intermediate_time = st;
    let num_pages_to_issue_update = 1000;
    let mut page_index = 0;

    let file = File::open(xml_file_name).unwrap();
    let mut reader: ReaderType = if READ_COMPRESSED {
        let gz_reader = MultiGzDecoder::new(file);
        ReaderType::Compressed(Reader::from_reader(BufReader::new(gz_reader)))
    } else {
        ReaderType::Uncompressed(Reader::from_reader(BufReader::new(file)))
    };

    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

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
                } else if e.starts_with(b"username") {
                    processing_username = true;
                }
            }
            Ok(Event::End(ref e)) => {
                if e.ends_with(b"page") {
                    // Uniquify the editors for this page up front to save space.
                    // Check if title is None
                    if processing_page {
                        let set: HashSet<String> = title_editors_map.get(&title.clone().unwrap()).unwrap().into_iter().cloned().collect();
                        title_editors_map.insert(title.clone().unwrap(), set.into_iter().collect());
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
                    }
                } else if e.ends_with(b"title") {
                    processing_title = false;
                }  else if e.starts_with(b"username") {
                    processing_username = false;
                }

            }
            Ok(Event::Text(e)) => {
                if processing_page {
                    if processing_title {
                        let text = String::from(e.unescape().unwrap());
                        if text.starts_with("User:") || text.starts_with("Talk:") {
                            // Skip this page. Set the title to None, then also set processing_page
                            // to false as a signal not to count the page when the page end tag hits.
                            title = None;
                            processing_page = false;
                        } else {
                            title = Some(text);
                        }
                    } else if processing_username {
                        let text = String::from(e.unescape().unwrap());
                        title_editors_map.entry(title.clone().unwrap()).or_insert_with(Vec::new).push(text);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => {}
        }

        if let Some(max_pages) = max_pages {
            if page_index >= *max_pages {
                break;
            }
        }

        buf.clear();
    }

    println!("post-processing redirects...");
    let pt = Instant::now();
    
    // For simplicity, do a two-pass approach for now. This could be sped up by using a single
    // pass, as well as by doing all the intermediate redirects at the same time as the final.
    let mut final_redirect_map: HashMap<String, String> = HashMap::new();
    for (redirect, mut target) in redirect_map.iter() {
        while let Some(new_target) = redirect_map.get(target) {
            target = new_target;
        };
        final_redirect_map.insert(redirect.clone(), target.clone());
    }
    for (redirect, target) in final_redirect_map.iter() {
        let values = title_editors_map.remove(redirect).unwrap_or_default();
        title_editors_map.entry(target.to_string()).or_insert_with(Vec::new).extend(values);
    }
    
    println!(
        "Done postprocessing in {} seconds.",
        pt.elapsed().as_secs()
    );
    println!("Done processing XML in {} seconds.", st.elapsed().as_secs());
    title_editors_map
}
