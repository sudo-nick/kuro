use std::{
    collections::HashMap,
    fs,
    io::{Read, Seek, Write},
    os::unix::fs::FileExt,
    path,
    time::{SystemTime, UNIX_EPOCH},
};

const TOMBSTONE: &[u8] = b"__TOMBSTONE__";

#[derive(Debug)]
struct KeyDir {
    file_id: u64,
    value_size: u64,
    value_pos: u64,
    timestamp: u64,
}

#[derive(Debug)]
struct HintFileEntry {
    timestamp: u64,
    key_size: u64,
    value_size: u64,
    value_pos: u64,
    key: Vec<u8>,
}

impl HintFileEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.key_size.to_le_bytes());
        bytes.extend_from_slice(&self.value_size.to_le_bytes());
        bytes.extend_from_slice(&self.value_pos.to_le_bytes());
        bytes.extend_from_slice(&self.key);
        bytes
    }
}

#[derive(Debug)]
pub struct Bitcask {
    key_dir: HashMap<Vec<u8>, KeyDir>,
    active_file: fs::File,
    active_file_id: u64,
    writer_pos: u64,
    data_path: String,
}

#[derive(Debug)]
struct DataFileEntry {
    crc: u64,
    timestamp: u64,
    key_size: u64,
    value_size: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl DataFileEntry {
    pub fn new(key: &Vec<u8>, value: &Vec<u8>) -> Self {
        let crc = 0;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        DataFileEntry {
            crc, // TODO: Calculate CRC
            timestamp,
            key_size,
            value_size,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.crc.to_le_bytes());
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.key_size.to_le_bytes());
        bytes.extend_from_slice(&self.value_size.to_le_bytes());
        bytes.extend_from_slice(&self.key);
        bytes.extend_from_slice(&self.value);
        bytes
    }
}

fn build_keydir(dirpath: &str) -> HashMap<Vec<u8>, KeyDir> {
    let mut map = HashMap::new();
    let dir = path::Path::new(dirpath);
    if !dir.exists() {
        println!("Directory does not exist: {}", dirpath);
        return map;
    }
    for entry in dir
        .to_path_buf()
        .read_dir()
        .expect("Unable to read directory")
    {
        let mut file_pos = 0;
        let entry = entry.expect("Unable to read entry");
        if let Some(extension) = entry.path().extension() {
            if extension != "dat" {
                continue;
            }
        }
        let hint_filepath = entry.path().with_extension("hint");
        if hint_filepath.exists() {
            let mut hint_file = fs::File::open(&hint_filepath).expect("Unable to open data file");
            let file_id_str = hint_filepath.file_stem().expect("Unable to get file stem");
            let file_id = file_id_str
                .to_str()
                .expect("Invalid file ID")
                .parse::<u64>()
                .expect("Invalid file ID");
            let file_len = hint_file.metadata().expect("Unable to get metadata").len();

            let mut buf = [0u8; 8];

            while file_pos < file_len {
                // timestamp
                let _ = hint_file.read_exact(&mut buf);
                let timestamp = u64::from_le_bytes(buf);
                file_pos += 8;

                // key size
                let _ = hint_file.read_exact(&mut buf);
                let key_size = u64::from_le_bytes(buf);
                file_pos += 8;

                // value size
                let _ = hint_file.read_exact(&mut buf);
                let value_size = u64::from_le_bytes(buf);
                file_pos += 8;

                // value pos
                let _ = hint_file.read_exact(&mut buf);
                let value_pos = u64::from_le_bytes(buf);
                file_pos += 8;

                // key
                let mut key = vec![0u8; key_size as usize];

                let _ = hint_file.read_exact(&mut key);
                file_pos += key_size;

                let map_entry = KeyDir {
                    file_id,
                    value_size,
                    value_pos,
                    timestamp,
                };

                map.insert(key, map_entry);
            }
        } else {
            let dat_filepath = entry.path();
            let mut dat_file = fs::File::open(&dat_filepath).expect("Unable to open data file");
            let file_id = dat_filepath.file_stem().expect("Unable to get file stem");

            let mut buf = [0u8; 8];

            let file_len = dat_file.metadata().expect("Unable to get metadata").len();

            while file_pos < file_len {
                file_pos += 8;
                let _ = dat_file.seek_relative(8);

                let _ = dat_file.read_exact(&mut buf);
                let timestamp = u64::from_le_bytes(buf);
                file_pos += 8;

                let _ = dat_file.read_exact(&mut buf);
                let key_size = u64::from_le_bytes(buf);
                file_pos += 8;

                let _ = dat_file.read_exact(&mut buf);
                let value_size = u64::from_le_bytes(buf);
                file_pos += 8;

                let mut key = vec![0u8; key_size as usize];
                let _ = dat_file.read_exact(&mut key).expect("Unable to read key");
                file_pos += key_size;

                let map_entry = KeyDir {
                    file_id: file_id.to_str().unwrap().parse().expect("Invalid file ID"),
                    value_size,
                    value_pos: file_pos,
                    timestamp,
                };

                let _ = dat_file.seek_relative(value_size as i64);
                file_pos += value_size;

                map.insert(key, map_entry);
            }
        }
    }
    map
}

impl Bitcask {
    pub fn open(path: &str) -> Self {
        let now = SystemTime::now();
        let mut file_id = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let dirpath = path::Path::new(path);
        if !dirpath.exists() {
            match fs::create_dir(path) {
                Ok(_) => println!("Created directory: {}", path),
                Err(e) => panic!("Failed to create directory: {}", e),
            }
        }
        let mut filepath = dirpath.join(format!("{}.dat", file_id));
        if filepath.exists() {
            file_id += 1;
        }
        filepath = dirpath.join(format!("{}.dat", file_id));
        let active_file = fs::File::create(filepath).expect("Unable to create data file");
        let key_dir = build_keydir(path);
        Bitcask {
            key_dir,
            active_file,
            active_file_id: file_id,
            writer_pos: 0,
            data_path: path.to_string(),
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let kd_value = self.key_dir.get(key);
        match kd_value {
            Some(kd) => {
                let dirpath = path::Path::new(&self.data_path);
                let filepath = dirpath.join(format!("{}.dat", kd.file_id));
                let data_file = fs::File::open(filepath).expect("Unable to open data file");
                print!(
                    "Reading from file: {} at position: {} of size {}",
                    kd.file_id, kd.value_pos, kd.value_size
                );
                let mut buf = vec![0u8; kd.value_size as usize];
                data_file
                    .read_exact_at(&mut buf, kd.value_pos)
                    .expect("Unable to read data file");
                return Some(buf);
            }
            None => None,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry = DataFileEntry::new(&key, &value);
        //  FORMAT: CRC + TMSTMP + KEY_SIZE + VALUE_SIZE + KEY
        let value_pos = self.writer_pos + 8 + 8 + 8 + 8 + key.len() as u64;
        let kd_value = KeyDir {
            file_id: self.active_file_id,
            value_size: value.len() as u64,
            value_pos,
            timestamp: entry.timestamp,
        };
        let _ = self
            .active_file
            .write_at(&entry.to_bytes(), self.writer_pos);
        // FORMAT: CRC + TMSTMP + KEY_SIZE + VALUE_SIZE + KEY + VALUE
        self.writer_pos += 8 + 8 + 8 + 8 + key.len() as u64 + value.len() as u64;
        self.key_dir.insert(key, kd_value);
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.put(key, TOMBSTONE.to_vec());
    }

    pub fn list_keys(&self) -> Option<Vec<&Vec<u8>>> {
        Some(self.key_dir.keys().collect::<Vec<&Vec<u8>>>())
    }

    pub fn merge(&mut self, dirpath: &str) {
        let keydir = build_keydir(dirpath);
        let mut file_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut merge_filepath = path::Path::new(&self.data_path).join(format!("{}.dat", file_id));
        if merge_filepath.exists() {
            file_id += 1;
        }
        merge_filepath = path::Path::new(&self.data_path).join(format!("{}.dat", file_id));
        let mut merge_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(merge_filepath)
            .expect("Unable to open data file for merging");
        let hint_filepath = path::Path::new(&self.data_path).join(format!("{}.hint", file_id));
        let mut hint_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(hint_filepath)
            .expect("Unable to open hint file for merging");
        let mut write_pos = 0;
        for (key, _) in keydir {
            if let Some(value) = self.get(&key) {
                if value.eq(&TOMBSTONE.to_vec()) {
                    continue;
                }
                let entry = DataFileEntry::new(&key, &value);
                let _ = merge_file.write(&entry.to_bytes());
                write_pos += 8 + 8 + 8 + 8 + key.len() as u64;

                let hint_entry = HintFileEntry {
                    timestamp: entry.timestamp,
                    key_size: entry.key_size,
                    value_size: entry.value_size,
                    value_pos: write_pos,
                    key: entry.key.clone(),
                };
                let _ = hint_file.write(&hint_entry.to_bytes());

                write_pos += entry.value_size;
            }
        }
        let dir = path::Path::new(dirpath)
            .read_dir()
            .expect("Unable to read directory");
        for file in dir {
            let filepath = file.expect("Unable to read file").path();
            let id_str = filepath
                .file_stem()
                .expect("Unable to get file stem")
                .to_str()
                .expect("Invalid file stem");
            let id = u64::from_str_radix(id_str, 10).expect("Invalid file ID");
            if id == file_id || id == self.active_file_id {
                continue;
            }
            let _ = fs::remove_file(filepath);
        }
        merge_file.sync_all().expect("Failed to sync merge file");
        hint_file.sync_all().expect("Failed to sync hint file");
    }

    pub fn fold() {
        panic!("Sync operation not implemented yet");
    }

    pub fn sync(&mut self) {
        self.active_file
            .sync_all()
            .expect("Failed to sync active file");
    }

    pub fn close(self) {
        drop(self.active_file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_put() {
        let mut bitcask = Bitcask::open("/tmp/test1");
        bitcask.put(b"key1".to_vec(), b"value1".to_vec());
        let result = bitcask.get(&b"key1".to_vec());
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_list_keys() {
        let bitcask = Bitcask::open("/tmp/test1");
        let keys = bitcask.list_keys();
        assert_eq!(keys, Some(vec![&b"key1".to_vec()]));
    }

    #[test]
    fn test_build_keydir() {
        // let mut bitcask = Bitcask::open("/tmp/test3");
        // bitcask.put(b"key1".to_vec(), b"value1".to_vec());
        // bitcask.put(b"key2".to_vec(), b"value2".to_vec());

        let key_dir = build_keydir("/tmp/test1");
        assert_eq!(key_dir.len(), 1);
        assert!(key_dir.contains_key(&b"key1".to_vec()));
    }

    #[test]
    fn test_keydir() {
        let bitcask = Bitcask::open("/tmp/test1");
        let result = bitcask.get(&b"key1".to_vec());
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_merge() {
        let mut bitcask = Bitcask::open("/tmp/test4");
        bitcask.put(b"key1".to_vec(), b"value1".to_vec());
        bitcask.put(b"key2".to_vec(), b"value2".to_vec());

        let mut bitcask2 = Bitcask::open("/tmp/test4");
        bitcask2.merge("/tmp/test4");

        let bitcask3 = Bitcask::open("/tmp/test4");
        let val1 = bitcask3.get(&b"key1".to_vec());
        let val2 = bitcask3.get(&b"key2".to_vec());

        assert_eq!(val1, Some(b"value1".to_vec()));
        assert_eq!(val2, Some(b"value2".to_vec()));
    }
}
