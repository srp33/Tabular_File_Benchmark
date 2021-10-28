use std::env;
use std::fs;
use std::io::{Write, Error};
use memmap2::{MmapOptions, Mmap};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::cmp;
use std::str::FromStr;
use std::collections::HashMap;

fn open_read_file(file_path: &String, file_extension: &str) -> Mmap {
    let the_file = File::open(file_path.clone() + file_extension).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&the_file).unwrap() };
    return mmap;
}

fn read_int_from_file(file_path: &String, file_extension: &str) -> usize {
    return fs::read_to_string(file_path.clone() + file_extension).expect("Unable to read file").parse().unwrap();
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

fn parse_data_values (start_offset: &usize, segment_length: &usize, data_coords: &Vec<usize>, str_like_object: &Mmap) -> String {
    let end_offset = 0;
    let start_pos = start_offset * segment_length;
    let result = &str_like_object[(start_pos + data_coords[1]) as usize..(start_pos + data_coords[2] + end_offset) as usize];
    return std::str::from_utf8(result).unwrap().trim_end().to_owned();
}

fn filter_rows (row_indices: &Vec<usize>, query_col_index: usize, query_col_coords: &Vec<usize>, file_handles: &HashMap<String, Mmap>, max_column_type_length: &usize, line_length: &usize) -> Vec<usize> {
    let col_type = parse_data_values(&query_col_index, max_column_type_length, &vec![query_col_index, 0, 1], file_handles.get("ct").unwrap());
    let mut return_rows:Vec<usize> = Vec::new();

    if col_type.eq("n") {
        for row_index in row_indices {
            let float_value:f64 = parse_data_values(row_index, line_length, query_col_coords, file_handles.get("data").unwrap()).parse().unwrap();
            if float_value >= 0.1 {
                return_rows.push(*row_index);
            }
        }
    }
    else {
        for row_index in row_indices {
            let value = parse_data_values(row_index, line_length, query_col_coords, file_handles.get("data").unwrap());
            if value.starts_with("A") || value.ends_with("Z") {
                return_rows.push(*row_index);
            }
        }
    }
    return return_rows;
}

fn parse_all_data_values (start_offset: usize, segment_length: usize, data_coords: &Vec<Vec<usize>>, str_like_object: &Mmap) -> String {
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

fn main () -> Result<(), Error>  {
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let col_names_file_path = &args[2];
    let out_file_path = &args[3];
    let num_rows: usize = FromStr::from_str(&args[4]).unwrap();
    let query_col_indices = args[5].split(",").map(|x| FromStr::from_str(x).unwrap()).collect::<Vec<usize>>();

    let mut file_handles = HashMap::new();
    file_handles.insert(String::from("cc"), open_read_file(file_path, ".cc"));
    file_handles.insert(String::from("data"), open_read_file(file_path, ""));
    file_handles.insert(String::from("ct"), open_read_file(file_path, ".ct"));

    let line_length: usize = read_int_from_file(file_path, ".ll");
    let max_column_coord_length: usize = read_int_from_file(file_path, ".mccl");
    let max_column_type_length: usize = read_int_from_file(file_path, ".mctl");
    let out_col_indices: Vec<usize> = get_col_indices_to_query(col_names_file_path);
    let out_col_coords = parse_data_coords(&out_col_indices, file_handles.get("cc").unwrap(), max_column_coord_length, line_length);

    let path: PathBuf = PathBuf::from(out_file_path);
    let mut out_file =  OpenOptions::new().read(true).write(true).create(true).open(&path)?;
    let all_query_col_coords = parse_data_coords(&query_col_indices, file_handles.get("cc").unwrap(), max_column_coord_length, line_length);

    let mut keep_row_indices:Vec<usize> = (0..(num_rows + 1)).collect();

    let mut index: usize = 0;
    for query_col_index in query_col_indices {
        keep_row_indices = filter_rows(&keep_row_indices, query_col_index, all_query_col_coords.get(index).unwrap(), &file_handles, &max_column_type_length, &line_length);
        index = index + 1;
    }

    let mut out_lines = Vec::new();
    let chunk_size:usize = 1000;

    keep_row_indices.insert(0,0);
    for row_index in keep_row_indices {
        out_lines.push(parse_all_data_values(row_index, line_length, &out_col_coords, file_handles.get("data").unwrap()));

        if out_lines.len() % chunk_size as usize == 0 {
            let out_string = out_lines.join("\n");
            out_file.write(out_string.as_bytes())?;
            out_file.write("\n".as_bytes())?;
            out_lines = Vec::new();
        }
    }
    if out_lines.len() > 0 {
        let out_string = out_lines.join("\n");
        out_file.write(out_string.as_bytes())?;
        out_file.write("\n".as_bytes())?;
    }

    Ok(())
}
