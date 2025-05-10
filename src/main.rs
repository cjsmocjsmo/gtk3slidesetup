
// use image::GenericImageView;
// use clap::{Command, Arg};
// use walkdir::WalkDir;
// use rusqlite::{params, Connection, Result};
// use rayon::prelude::*;
// use std::time::Instant;
// use base64::{engine::general_purpose, Engine as _};

// fn main() -> Result<()> {
//     let start = Instant::now();
//     println!("start time  {:?}", start);

//     let matches = Command::new("Image CLI")
//         .version("1.0")
//         .author("Author Name <author@example.com>")
//         .about("Processes images and stores metadata in a SQLite database")
//         .arg(Arg::new("image_path")
//             .short('i')
//             .long("image_path")
//             .value_name("IMAGE_PATH")
//             .required(true))
//         .arg(Arg::new("db_path")
//             .short('d')
//             .long("db_path")
//             .value_name("DB_PATH")
//             .required(true))
//         .get_matches();

//     let image_path = matches.get_one::<String>("image_path").unwrap();
//     let db_path = matches.get_one::<String>("db_path").unwrap();

//     let conn = Connection::open(db_path)?;

//     conn.execute(
//         "CREATE TABLE IF NOT EXISTS imageData (
//             id INTEGER PRIMARY KEY,
//             idx INTEGER NOT NULL,
//             img_path TEXT NOT NULL,
//             size INTEGER NOT NULL,
//             orientation TEXT NOT NULL,
//             b64 TEXT NOT NULL
//         )",
//         [],
//     )?;

//     let entries: Vec<_> = WalkDir::new(image_path)
//         .into_iter()
//         .filter_map(|e| e.ok())
//         .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("jpg"))
//         .collect();

//     entries.par_iter().enumerate().for_each(|(idx, entry)| {
//         let img_path = entry.path().to_str().unwrap().to_string();
//         match image::open(&img_path) {
//             Ok(_) => {
//                 let metadata = std::fs::metadata(&img_path).unwrap();
//                 let size = metadata.len();
//                 let orientation = get_orientation(&img_path);
//                 let b64 = general_purpose::STANDARD.encode(&std::fs::read(&img_path).unwrap());

//                 let conn = Connection::open(db_path).unwrap();
//                 conn.execute(
//                     "INSERT INTO imageData (idx, img_path, size, orientation, b64) VALUES (?1, ?2, ?3, ?4, ?5)",
//                     params![idx as i32 + 1, img_path, size, orientation, b64],
//                 ).unwrap();
//             },
//             Err(e) => {
//                 eprintln!("Failed to open image {}: {:?}", img_path, e);
//                 let bad_pics_dir = "/media/piir/PiTB/BadPics/";
//                 std::fs::create_dir_all(bad_pics_dir).unwrap();
//                 let dest_path = std::path::Path::new(bad_pics_dir).join(entry.file_name());
//                 println!("Moving bad image {}\n to\n {:?}", img_path, dest_path);
//                 if let Err(e) = std::fs::rename(&img_path, &dest_path) {
//                     eprintln!("Failed to move bad image {}: {:?}", img_path, e);
//                 }
//             }
//         }
//     });

//     let duration = start.elapsed();
//     println!("Time elapsed: {:?}", duration);
    
//     Ok(())
// }

// fn get_orientation(img_path: &str) -> String {
//     let img = image::open(img_path).unwrap();
//     let (width, height) = img.dimensions();
//     println!("width {} height {}", width, height);

//     if width > height {
//         return "landscape".to_string()
//     } else if width < height {
//         return "portrait".to_string()
//     } else {
//         return "square".to_string()
//     }
// }

use image::GenericImageView;
use clap::{Command, Arg};
use walkdir::WalkDir;
use rusqlite::{params, Connection, Result};
use rayon::prelude::*;
use std::sync::Mutex;
use std::time::Instant;
use base64::{engine::general_purpose, Engine as _};

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

    let mut conn = Connection::open(db_path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS imageData (
            id INTEGER PRIMARY KEY,
            idx INTEGER NOT NULL,
            img_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            orientation TEXT NOT NULL,
            b64 TEXT NOT NULL
        )",
        [],
    )?;

    // Use a thread-safe vector to collect results
    let results = Mutex::new(Vec::new());

    WalkDir::new(image_path)
    .into_iter()
    .filter_map(|e| e.ok())
    .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("jpg"))
    .enumerate() // Apply enumerate before par_bridge
    .par_bridge() // Parallelize the iterator
    .for_each(|(idx, entry)| {
        let img_path = entry.path().to_str().unwrap().to_string();
        match image::open(&img_path) {
            Ok(img) => {
                let size = std::fs::metadata(&img_path).unwrap().len();
                let orientation = get_orientation(&img);
                let b64 = general_purpose::STANDARD.encode(&std::fs::read(&img_path).unwrap());

                let mut results = results.lock().unwrap();
                results.push((idx as i32 + 1, img_path, size, orientation, b64));
            }
            Err(e) => {
                eprintln!("Failed to open image {}: {:?}", img_path, e);
                let bad_pics_dir = "/home/whitepi/Pictures/BadPics/";
                std::fs::create_dir_all(bad_pics_dir).unwrap();
                let dest_path = std::path::Path::new(bad_pics_dir).join(entry.file_name());
                println!("Moving bad image {}\n to\n {:?}", img_path, dest_path);
                if let Err(e) = std::fs::rename(&img_path, &dest_path) {
                    eprintln!("Failed to move bad image {}: {:?}", img_path, e);
                }
            }
        }
    });

    // Batch insert into the database
    let results = results.into_inner().unwrap();
    let tx = conn.transaction()?;
    for (idx, img_path, size, orientation, b64) in results {
        tx.execute(
            "INSERT INTO imageData (idx, img_path, size, orientation, b64) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![idx, img_path, size, orientation, b64],
        )?;
    }
    tx.commit()?;

    let duration = start.elapsed();

    Ok(println!("Time elapsed: {:?}", duration))

    
}

fn get_orientation(img: &image::DynamicImage) -> String {
    let (width, height) = img.dimensions();
    // println!("width {} height {}", width, height);

    if width > height {
        "landscape".to_string()
    } else if width < height {
        "portrait".to_string()
    } else {
        "square".to_string()
    }
}