use std::env;
use std::fs;
use std::io::{Write, Error, BufReader, BufRead, SeekFrom, Seek, Read};
use memmap2::{MmapOptions, Mmap};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::cmp;
use std::str::FromStr;

fn get_col_indices_to_query(column_names_file_path: &str, memory_map: bool) -> Vec<usize> {
    let mut index_range: Vec<usize> = Vec::new();
    let file = File::open(column_names_file_path).unwrap();
    if memory_map {
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        let n = mmap.split(|byte|byte == &b'\n');
        for line in n {
            if line.len() == 0 {
                continue;
            }
            let int_string = std::str::from_utf8(line.split(|byte| byte== &b'\t').nth(0).unwrap());
            let num = int_string.unwrap().trim().parse::<usize>().unwrap();
            index_range.push(num);
        }
    }
    else {
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line_string = line.unwrap();
            let string_index = line_string.trim_end().split("\t").nth(0).unwrap();
            index_range.push(FromStr::from_str(string_index).unwrap())
        }
    }
    return index_range;
}

fn parse_data_coords(line_indices: Vec<usize>, coords_file: Mmap, coords_file_max_length: usize, full_str_length: usize) -> Vec<Vec<usize>> {
    let mut results: Vec<Vec<usize>> = Vec::new();
    let coords_file_length = coords_file.len() as usize;
    let out_dict: Vec<usize> = Vec::new();
    let mut data_start_pos:usize;
    let mut data_end_pos:usize;
    for index in line_indices {
        let start_pos = index * (coords_file_max_length + 1);
        let next_start_pos = start_pos + coords_file_max_length + 1;
        let further_next_start_pos = cmp::min(next_start_pos + coords_file_max_length, coords_file_length);
        if out_dict.iter().any(|&i| i==start_pos) {
            data_start_pos = out_dict[start_pos as usize];
        }
        else {
            let test = &coords_file[start_pos as usize..next_start_pos as usize];
            let int_string = std::str::from_utf8(test).unwrap().trim_end();
            data_start_pos = int_string.parse::<usize>().unwrap();
        }
        if next_start_pos == further_next_start_pos {
            data_end_pos = full_str_length;
        }
        else {
            if out_dict.iter().any(|&i| i==next_start_pos) {
                data_end_pos = out_dict[next_start_pos as usize];
            }
            else {
                let test = &coords_file[next_start_pos as usize..further_next_start_pos as usize];
                let int_string = std::str::from_utf8(test).unwrap().trim_end();
                data_end_pos = int_string.parse::<usize>().unwrap();
            }
        }
        let temp_results:Vec<usize> = vec![index, data_start_pos, data_end_pos];
        results.push(temp_results);
    }
    return results;
}

fn parse_data_coords_seek(line_indices: Vec<usize>, coords_file_path: &str, coords_file_max_length: usize, full_str_length: usize) -> Vec<Vec<usize>> {
    let mut results: Vec<Vec<usize>> = Vec::new();
    let coords_file_length = fs::metadata(coords_file_path).unwrap().len();
    let out_dict: Vec<usize> = Vec::new();
    let mut data_start_pos:usize;
    let mut data_end_pos;
    //let mut data_end_pos = None;
    let mut file = &File::open(coords_file_path).unwrap();
    for index in line_indices {
        let start_pos = index * (coords_file_max_length + 1);
        let next_start_pos = start_pos + coords_file_max_length + 1;
        let further_next_start_pos = cmp::min(next_start_pos + coords_file_max_length, coords_file_length as usize);

        if out_dict.iter().any(|&i| i==start_pos) {
            data_start_pos = out_dict[start_pos];
        }
        else {
            if let Err(e) = file.seek(SeekFrom::Start(start_pos as u64)) {
                println!("Seek error: {}", e.to_string());
            }

            let mut buffer = String::new();

            if let Err(e) = file.take((next_start_pos - start_pos) as u64).read_to_string(&mut buffer) {
                println!("Take error: {}", e.to_string());
            }

            data_start_pos = FromStr::from_str(&*buffer.trim_end()).unwrap();
        }
        if next_start_pos == further_next_start_pos {
            data_end_pos = Some(full_str_length);
        }
        else {
            if out_dict.iter().any(|&i| i==next_start_pos) {
                data_end_pos = Some(out_dict[next_start_pos as usize]);
            }
            else {
                if let Err(e) = file.seek(SeekFrom::Start(next_start_pos as u64)) {
                    println!("Seekerror: {}", e.to_string());   
                }

                let mut buffer = String::new();

                if let Err(e) = file.take((further_next_start_pos - next_start_pos) as u64).read_to_string(&mut buffer) {
                    println!("Take error: {}", e.to_string());
                }

                data_end_pos = Some(FromStr::from_str(&*buffer.trim_end()).unwrap());
            }
        }
        if data_end_pos.is_some() {
            let temp_results: Vec<usize> = vec![index, data_start_pos, data_end_pos.unwrap()];
            results.push(temp_results);
        }
    }
    return results;
}

fn parse_data_values (start_offset: usize, segment_length: usize, data_coords: &Vec<Vec<usize>>, str_like_object: &Mmap) -> String {
    let mut current_line: Vec<&str> = Vec::new();
    let end_offset = 0;
    let start_pos = start_offset * segment_length;
    for coords in data_coords {
        let result = &str_like_object[(start_pos + coords[1]) as usize..(start_pos + coords[2] + end_offset) as usize];
        let result_string = std::str::from_utf8(result).unwrap().trim_end();
        current_line.push(result_string);
    }
    return current_line.join("\t");
}

fn parse_data_values_seek (start_offset: usize, segment_length:usize, data_coords: &Vec<Vec<usize>>, data_file_path: &str) -> String {
    let mut current_line: Vec<String> = Vec::new();
    let end_offset = 0;
    let start_pos = start_offset * segment_length;
    let mut file = &File::open(data_file_path).unwrap();
    for coords in data_coords {
        if let Err(e) = file.seek(SeekFrom::Start((start_pos + coords[1]) as u64)) {
            println!("Seek error: {}", e.to_string());
        }

        let mut buffer = String::new();

        if let Err(e) = file.take(((start_pos + coords[2] + end_offset) - (start_pos + coords[1])) as u64).read_to_string(&mut buffer) {
            println!("Take error: {}", e.to_string());
        }

        current_line.push(buffer.trim_end().parse().unwrap());
    }
    return current_line.join("\t");
}

fn main() -> Result<(), Error> {
    // Get command line variables
    let args: Vec<String> = env::args().collect();
    let file_path_ll = &args[1];
    let data_file_path = &args[2];
    let file_path_cc = &args[3];
    let out_file_path = &args[4];
    let file_path_mccl = &args[5];
    let column_names_file_path = &args[6];
    let num_rows: usize = FromStr::from_str(&args[7]).unwrap();
    let use_mmap = &args[8];
    let chunk_size: usize = 1000;
    //let mut memory_map = true;
    let memory_map;

    if use_mmap.eq("MMAP") {
        memory_map = true;
    } else if use_mmap.eq("NO_MMAP") {
        memory_map = false;
    } else {
        println!("Invalid argument for m_map, expected 'MMAP' or 'NO_MMAP' got '{}'", use_mmap);
        std::process::exit(1);
    }

    //Determine the column indices to be queried and create the row indices
    let col_indices = get_col_indices_to_query(column_names_file_path, memory_map);
    let line_length = fs::read_to_string(file_path_ll).expect("Unable to read file").parse().unwrap();
    let max_column_coord_length = fs::read_to_string(file_path_mccl).expect("Unable to read file").parse().unwrap();
    let row_indices: Vec<usize> = (0..num_rows + 1).collect();

    // Create the mmaps to be used in parse_data_coords and parse_data_values, open the outfile and
    // get the column coordinates...
    let path: PathBuf = PathBuf::from(out_file_path);
    let mut out_file =  OpenOptions::new().read(true).write(true).create(true).open(&path)?;
    let mmap_cc;
    let mut mmap_data = None;
    let col_coords;
    if memory_map {
        let file_cc = File::open(file_path_cc).unwrap();
        mmap_cc = unsafe { MmapOptions::new().map(&file_cc).unwrap() };
        let file_data = File::open(data_file_path).unwrap();
        mmap_data = Some(unsafe { MmapOptions::new().map(&file_data).unwrap() });
        col_coords = parse_data_coords(col_indices, mmap_cc, max_column_coord_length, line_length);
    } else {
        col_coords = parse_data_coords_seek(col_indices, file_path_cc, max_column_coord_length, line_length);
    }

    //Using the knowledge of the column coordinates and the correct location in the rows, isolate it and format it, and write it to the outfile chunk by chunk
    let mut out_lines = Vec::new();
    if memory_map {
        for row_index in row_indices {
            out_lines.push(parse_data_values(row_index, line_length, &col_coords, &mmap_data.as_ref().unwrap()));
            if out_lines.len() % chunk_size as usize == 0 {
                let out_string = out_lines.join("\n");
                out_file.write(out_string.as_bytes())?;
                out_file.write("\n".as_bytes())?;
                out_lines = Vec::new();
            }
        }
    }
    else {
        for row_index in row_indices {
            out_lines.push(parse_data_values_seek(row_index, line_length, &col_coords, data_file_path));
            if out_lines.len() % chunk_size as usize == 0 {
                let out_string = out_lines.join("\n");
                out_file.write(out_string.as_bytes())?;
                out_file.write("\n".as_bytes())?;
                out_lines = Vec::new();
            }
        }
    }
    if out_lines.len() > 0 {
        let out_string = out_lines.join("\n");
        out_file.write(out_string.as_bytes())?;
        out_file.write("\n".as_bytes())?;
    }
    Ok(())
}
