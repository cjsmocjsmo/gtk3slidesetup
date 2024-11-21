use clap::{Command, Arg};
use walkdir::WalkDir;
use image::GenericImageView;
use rusqlite::{params, Connection, Result};
use std::fs::File;
use std::path::Path;
use std::hash::{Hasher, Hash};
use std::collections::hash_map::DefaultHasher;
use rayon::prelude::*;
use std::time::Instant;

fn main() -> Result<()> {
    let start = Instant::now();
    println!("start time  {:?}", start);

    let matches = Command::new("Image CLI")
        .version("1.0")
        .author("Author Name <author@example.com>")
        .about("Processes images and stores metadata in a SQLite database")
        .arg(Arg::new("image_path")
            .short('i')
            .long("image_path")
            .value_name("IMAGE_PATH")
            .required(true))
        .arg(Arg::new("db_path")
            .short('d')
            .long("db_path")
            .value_name("DB_PATH")
            .required(true))
        .get_matches();

    let image_path = matches.get_one::<String>("image_path").unwrap();
    let db_path = matches.get_one::<String>("db_path").unwrap();

    let conn = Connection::open(db_path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS imageData (
            id INTEGER PRIMARY KEY,
            idx INTEGER NOT NULL,
            img_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            img_hash TEXT NOT NULL
        )",
        [],
    )?;

    let entries: Vec<_> = WalkDir::new(image_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("jpg"))
        .collect();

    entries.par_iter().enumerate().for_each(|(idx, entry)| {
        let img_path = entry.path().to_str().unwrap().to_string();
        match image::open(&img_path) {
            Ok(img) => {
                let size = img.dimensions().0 * img.dimensions().1;
                let img_hash = calculate_hash(&img_path);

                println!("size {}", size);

                let conn = Connection::open(db_path).unwrap();
                conn.execute(
                    "INSERT INTO imageData (idx, img_path, size, img_hash) VALUES (?1, ?2, ?3, ?4)",
                    params![idx as i32 + 1, img_path, size, img_hash],
                ).unwrap();
            },
            Err(e) => {
                eprintln!("Failed to open image {}: {:?}", img_path, e);
            }
        }
    });

    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);
    
    Ok(())
}

fn calculate_hash<P: AsRef<Path>>(path: P) -> String {
    let mut hasher = DefaultHasher::new();
    let mut file = File::open(path).unwrap();
    let mut buffer = Vec::new();
    std::io::copy(&mut file, &mut buffer).unwrap();
    buffer.hash(&mut hasher);
    hasher.finish().to_string()
}