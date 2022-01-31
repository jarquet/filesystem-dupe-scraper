use clap::Parser;
use env_logger;
use log::{debug, error, info};
use md5::{Digest, Md5};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use walkdir::{DirEntry, WalkDir};

/// Receive a command from command-line, expecting a path from which to start walking the fs tree.
#[derive(Parser)]
struct Cli {
    /// The command. "walk"
    command: String,
    /// The path to the folder to begin walking the fs tree
    #[clap(parse(from_os_str))]
    path: Option<std::path::PathBuf>,
}

#[derive(Debug)]
struct FileRecord<'a> {
    filename: &'a String,
    filepath: &'a String,
    hash: &'a String,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Hello, world!");

    let args = Cli::parse();
    let conn = Connection::open("filesystem_dupes.db").unwrap();

    if args.command == "walk" {
        info!("Walk command received");
        create_tables(&conn);
        walk_filesystem_hashing(args.path.expect("'walk' command expects a path arg")).await;
        let count = match conn.query_row("Select count(*) from file_record;", [], |row| row.get(0))
        {
            Ok(count) => count,
            Err(sql_error) => {
                error!("sql error msg: {}", sql_error);
                usize::MAX
            }
        };
        info!("Records count: {}", count,);
        return;
    }
    if args.command == "setup" {
        create_tables(&conn);
    }
}

fn create_tables(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_record (
                  id            INTEGER PRIMARY KEY,
                  filename      TEXT NOT NULL,
                  filepath      TEXT NOT NULL,
                  hash          TEXT NOT NULL
                  )",
        [],
    )
    .unwrap();
}

fn insert_file_record(record: FileRecord) {
    let conn = Connection::open("filesystem_dupes.db").unwrap();
    match conn.execute(
        "INSERT INTO file_record (filename, filepath, hash) VALUES (?1, ?2, ?3)",
        params![*record.filename, *record.filepath, *record.hash],
    ) {
        Ok(_) => {
            debug!("{} inserted into file_record table", record.filename);
        }
        Err(err) => {
            let err_msg = format!("Error inserting file_record: {err}");
            error!("{}", err_msg);
        }
    }
}

async fn walk_filesystem_hashing(root: std::path::PathBuf) {
    info!("Walking {}", root.display());
    let files = WalkDir::new(root).same_file_system(true);

    let mut handles = vec![];
    for file_result in files {
        let file = match file_result {
            Ok(file) => {
                let path_string = file.path().to_string_lossy();
                if path_string.contains(".git")
                    | path_string.contains("/target/")
                    | path_string.contains(".idea/")
                {
                    debug!("Skipping (.git, /target/) file.");
                    continue;
                }
                file
            }
            Err(e) => {
                debug!("Bad file?: {}", e);
                continue;
            }
        };
        handles.push(tokio::spawn(digest_and_insert_path(file)));
    }
    futures::future::join_all(handles).await;
}

async fn digest_and_insert_path(file: DirEntry) {
    let path = file.into_path();
    if path.is_dir() {
        debug!("Directory found: {}", path.to_str().unwrap());
        return;
    }
    let digest = calculate_digest(&path, 500 * 1024 * 1024);
    let record = FileRecord {
        filename: &path.file_name().unwrap().to_string_lossy().to_string(),
        filepath: &path.to_string_lossy().to_string(),
        hash: &String::from(digest.await.unwrap()),
    };
    insert_file_record(record)
}

async fn calculate_digest(file: &PathBuf, chunk_size: usize) -> Option<String> {
    let mut f = match tokio::fs::File::open(file).await {
        Ok(f) => f,
        Err(open_error) => {
            error!(
                "Error opening file {}: {}",
                file.as_os_str().to_string_lossy(),
                open_error
            );
            return None;
        }
    };
    let mut md5 = Md5::new();
    let mut input_buffer = vec![0u8; chunk_size];

    let md5_result = loop {
        let amount_read = f.read(&mut input_buffer).await.unwrap();
        if amount_read > 0 {
            md5.update(&input_buffer[0..amount_read]);
        } else {
            break md5.finalize();
        }
    };
    Some(format!("{:x}", md5_result))
}
