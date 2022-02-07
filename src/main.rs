use clap::Parser;
use env_logger;
use log::{debug, error, info, warn};
use md5::{Digest, Md5};
use rusqlite::{params, Connection};
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
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
struct FileRecord {
    filename: String,
    filepath: String,
    hash: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    env_logger::init();
    info!("Hello, world!");

    let args = Cli::parse();
    let conn = Connection::open("filesystem_dupes.db").unwrap();
    let conn_lock = Arc::new(RwLock::new(conn));

    if args.command == "walk" {
        info!("Walk command received");
        create_tables(&conn_lock);

        walk_filesystem_hashing(
            args.path.expect("'walk' command expects a path arg"),
            &conn_lock,
        )
        .await;
        let count = match conn_lock.read().unwrap().query_row(
            "Select count(*) from file_record;",
            [],
            |row| row.get(0),
        ) {
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
        create_tables(&conn_lock);
    }
}

fn create_tables(conn_lock: &Arc<RwLock<Connection>>) {
    conn_lock
        .write()
        .unwrap()
        .execute(
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

async fn insert_file_record(
    conn_lock: &Arc<RwLock<Connection>>,
    record: FileRecord,
) -> Result<(), &str> {
    return match conn_lock.write().unwrap().execute(
        "INSERT INTO file_record (filename, filepath, hash) VALUES (?1, ?2, ?3)",
        params![record.filename, record.filepath, record.hash],
    ) {
        Ok(_) => {
            debug!("{} inserted into file_record table", record.filename);
            Ok(())
        }
        Err(err) => {
            let err_msg = format!("Error inserting file_record: {err}");
            warn!("{}", err_msg);
            Err("Failed to insert into db")
        }
    };
}

async fn walk_filesystem_hashing(root: std::path::PathBuf, conn_lock: &Arc<RwLock<Connection>>) {
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
        handles.push(digest_and_insert_path(file, &conn_lock));
    }
    info!("Joining {} async handles", handles.len());
    futures::future::join_all(handles).await;
}

async fn digest_and_insert_path(file: DirEntry, conn_lock: &Arc<RwLock<Connection>>) {
    debug!("digest: {:?}", file);
    let path = file.into_path();
    if path.is_dir() {
        debug!("Directory found: {}", path.to_str().unwrap());
        return;
    }
    let digest = calculate_digest(&path).await.unwrap();
    let record = FileRecord {
        filename: path.file_name().unwrap().to_string_lossy().to_string(),
        filepath: path.to_string_lossy().to_string(),
        hash: digest,
    };
    insert_file_record(&conn_lock, record).await.unwrap()
}

async fn calculate_digest(file: &PathBuf) -> Option<String> {
    debug!("Calculate digest: {:?}", file);
    let mut f = match std::fs::File::open(file) {
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
    let chunk_size = 0x4000;
    let md5_result = loop {
        let mut chunk = Vec::with_capacity(chunk_size);
        match f
            .by_ref()
            .take(chunk_size as u64)
            .read_to_end(&mut chunk)
            .ok()
        {
            Some(n) => {
                if n == 0 {
                    break md5.finalize();
                }
                md5.update(chunk);
                if n < chunk_size {
                    break md5.finalize();
                }
            }
            None => break md5.finalize(),
        }
    };
    Some(format!("{:x}", md5_result))
}
