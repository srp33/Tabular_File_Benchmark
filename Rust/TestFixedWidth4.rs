use std::env;
use std::fs;
use std::io::{Write, Error};
use memmap2::{MmapOptions, Mmap};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::cmp;
use std::str::FromStr;
use std::collections::HashMap;
use std::ops::Deref;

fn parse_data_values (start_offset: &usize, segment_length: &usize, data_coords: &Vec<usize>, str_like_object: &[u8]) -> String {
    let end_offset = 0;
    let start_pos = start_offset * segment_length;
    let result = &str_like_object[(start_pos + data_coords[1]) as usize..(start_pos + data_coords[2] + end_offset) as usize];
    return std::str::from_utf8(result).unwrap().trim_end().to_owned();
}

fn open_read_file(file_path: &String, file_extension: &str) -> Mmap {
    let the_file = File::open(file_path.clone() + file_extension).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&the_file).unwrap() };
    return mmap;
}

fn get_column_type(query_col_index: usize, &max_column_type_length: &usize, file_handles: &HashMap<String, Mmap>) -> String {
    return parse_data_values(&query_col_index,&max_column_type_length, &vec![query_col_index, 0, 1], file_handles.get("ct").unwrap().deref());
}

fn parse_data_coords(line_indices: &Vec<usize>, coords_file: &Mmap, coords_file_max_length: usize, full_str_length: usize) -> Vec<Vec<usize>> {
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
        let temp_results:Vec<usize> = vec![*index, data_start_pos, data_end_pos];
        results.push(temp_results);
    }
    return results;
}

fn parse_all_data_values (start_offset: usize, segment_length: usize, data_coords: &Vec<Vec<usize>>, str_like_object: &[u8]) -> String {
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

fn is_match (value: String, value_type: &str) -> usize {
    if value_type.eq("n") {
        if value.parse::<f32>().unwrap() >= 0.1 {
            return 1;
        }
    }
    else if value.starts_with("A") || value.ends_with("Z") {
        return 1;
    }
    return 0;
}

fn get_col_indices_to_query(column_names_file_path: &str) -> Vec<usize> {
    let mut index_range: Vec<usize> = Vec::new();
    let file = File::open(column_names_file_path).unwrap();
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
    return index_range;
}

fn parse_row(row_coord_0: &Vec<usize>, file_handles: &HashMap<String, Mmap>) -> Vec<u8> {
    let result =  &file_handles["data"][row_coord_0[1] as usize..row_coord_0[2] as usize];
    let max_decompressed_size: usize = (row_coord_0[2] as usize - row_coord_0[1] as usize + 1) * 100;
    let decompressed_vector: Vec<u8> = zstd::block::decompress(&result, max_decompressed_size).unwrap();
    return decompressed_vector;
}

fn read_int_from_file(file_path: &String, file_extension: &str) -> usize {
    return fs::read_to_string(file_path.clone() + file_extension).expect("Unable to read file").parse().unwrap();
}


fn main () -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let col_names_file_path = &args[2];
    let out_file_path = &args[3];
    // let num_rows: usize = FromStr::from_str(&args[4]).unwrap();
    let query_col_indices = args[5].split(",").map(|x| FromStr::from_str(x).unwrap()).collect::<Vec<usize>>();
    //let compression_method = &args[6];
    //let compression_level = &args[7];
    //let memory_map: bool = true;

    //if !compression_method.eq("zstd") {
    //    println!("{}", "No matching compression method");
    //    std::process::exit(1);
    //}

    let mut file_handles = HashMap::new();
    file_handles.insert(String::from("cc"), open_read_file(file_path, ".cc"));
    file_handles.insert(String::from("data"), open_read_file(file_path, ""));
    file_handles.insert(String::from("ct"), open_read_file(file_path, ".ct"));
    file_handles.insert(String::from("rowstart"), open_read_file(file_path, ".rowstart"));

    let line_length: usize = read_int_from_file(file_path, ".ll");
    let max_column_coord_length: usize = read_int_from_file(file_path, ".mccl");
    let max_column_type_length: usize = read_int_from_file(file_path, ".mctl");
    let max_row_start_length: usize = read_int_from_file(file_path, ".mrsl");
    let out_col_indices: Vec<usize> = get_col_indices_to_query(col_names_file_path);
    //let out_col_coords = parse_data_coords(&out_col_indices, file_handles.get("cc").unwrap(), max_column_coord_length, line_length);

    let path: PathBuf = PathBuf::from(out_file_path);
    let mut out_file =  OpenOptions::new().read(true).write(true).create(true).open(&path)?;
    //let num_cols: usize = file_handles.get("cc").unwrap().len() / (max_column_coord_length + 1);
    let out_col_coords = parse_data_coords(&out_col_indices, file_handles.get("cc").unwrap(), max_column_coord_length, line_length);
    let mut query_col_types: Vec<String> = Vec::new();
    for query_col_index in &query_col_indices {
        query_col_types.push(get_column_type(*query_col_index, &max_column_type_length, &file_handles));
    }
    let all_query_col_coords = parse_data_coords(&query_col_indices, file_handles.get("cc").unwrap(), max_column_coord_length, line_length);
    let num_rows: usize = file_handles.get("rowstart").unwrap().len() / (max_row_start_length + 1);
    let all_row_coord = parse_data_coords(&(0..(num_rows)).collect(), file_handles.get("rowstart").unwrap(), max_row_start_length, file_handles.get("data").unwrap().len());
    let chunk_size: usize = 1000;
    let mut out_lines = Vec::new();
    out_lines.push(parse_all_data_values(0, 0, &out_col_coords, parse_row(&all_row_coord[0], &file_handles).deref()));
    let mut i = 0;
    for row_coord in all_row_coord {
        if i == 0 {
            i = i +1;
            continue;
        }
        let line_vec = parse_row(&row_coord, &file_handles);
        let line = line_vec.deref();
        let value1 = parse_data_values(&0usize, &0usize, &all_query_col_coords[0], line);
        let value_type1 = &query_col_types[0];
        let value2 = parse_data_values(&0usize, &0usize, &all_query_col_coords[1], line);
        let value_type2 = &query_col_types[1];
        let num_matches = (is_match(value1, value_type1)) + is_match(value2, value_type2);
        if num_matches == query_col_indices.len() {
            out_lines.push(parse_all_data_values(0, 0, &out_col_coords, line));
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
