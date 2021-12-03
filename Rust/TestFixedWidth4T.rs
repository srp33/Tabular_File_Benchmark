use std::env;
use std::fs;
use std::io::{Write};
use memmap2::{MmapOptions, Mmap};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::cmp;
use std::str::FromStr;
use std::collections::HashMap;

fn read_int_from_file(file_path: &String, file_extension: &str) -> usize {
    return fs::read_to_string(file_path.clone() + file_extension).expect("Unable to read file").parse().unwrap();
}

fn open_read_file(file_path: &String, file_extension: &str) -> Mmap {
    let the_file = File::open(file_path.clone() + file_extension).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&the_file).unwrap() };
    return mmap;
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

fn parse_data_values<'a>(start_offset: usize, segment_length: usize, data_coords: &'a Vec<Vec<usize>>, str_like_object: &'a [u8], end_offset: usize) -> impl Iterator<Item=&'a[u8]> + 'a {
    let start_pos = start_offset * segment_length;
    let mut data_coords_iter = data_coords.iter();
    let iter = std::iter::from_fn(move || {
        let coords = data_coords_iter.next()?;
        let result = &str_like_object[(start_pos + coords[1]) as usize..(start_pos + coords[2] + end_offset) as usize];
        Some(result)
    });
    return iter;
}

fn filter_rows (line: &str, col_coords: &Vec<Vec<usize>>, query_col_index: usize, max_column_type_length: usize, file_handles: &HashMap<String, Mmap>) -> Vec<Vec<usize>> {
    let data_coords = &vec![vec![query_col_index, 0, 1]];
    let col_type = parse_data_values(query_col_index, max_column_type_length, data_coords, file_handles.get("ct").unwrap(), 0).next().unwrap();
    let col_type_string = convert_bytes_to_string(col_type);
    let mut return_rows: Vec<Vec<usize>> = Vec::new();
    if col_type_string.eq("n") {
        for coords in col_coords {
            let float_value:f64 = line[*coords.get(1).unwrap()..*coords.get(2).unwrap()].trim_end().parse().unwrap();
            if float_value >= 0.1 {
                return_rows.push(coords.to_vec());
            }
        }
    }
    else {
        for coords in col_coords {
            let value = line[*coords.get(1).unwrap()..*coords.get(2).unwrap()].trim_end();
            if value.starts_with("A") || value.ends_with("Z") {
                return_rows.push(coords.to_vec());
            }
        }
    }
    return return_rows;
}

fn convert_bytes_to_string(col_type: &[u8]) -> String {
    let col_type_string = std::str::from_utf8(col_type).unwrap().to_owned();
    col_type_string
}

fn decompress_to_string(input: &[u8]) -> String {
    let decompressed_bytes = zstd::stream::decode_all(input).unwrap();
    let decompressed_line_str = convert_bytes_to_string(&decompressed_bytes);
    decompressed_line_str
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

fn main () -> Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let transposed_file_path = &args[2];
    let col_names_file_path = &args[3];
    let out_file_path = &args[4];
    let query_col_indices = args[5].split(",").map(|x| FromStr::from_str(x).unwrap()).collect::<Vec<usize>>();

    let line_length = read_int_from_file(file_path, ".ll");
    let t_line_length = read_int_from_file(transposed_file_path, ".ll");
    let max_column_coord_length = read_int_from_file(file_path, ".mccl");
    let t_max_column_coord_length = read_int_from_file(transposed_file_path, ".mccl");
    let max_row_start_length = read_int_from_file(file_path, ".mrsl");
    let t_max_row_start_length = read_int_from_file(transposed_file_path, ".mrsl");
    let max_column_type_length = read_int_from_file(file_path, ".mctl");

    let mut file_handles = HashMap::new();
    file_handles.insert(String::from("cc"), open_read_file(file_path, ".cc"));
    file_handles.insert(String::from("t_cc"), open_read_file(transposed_file_path, ".cc"));
    file_handles.insert(String::from("data"), open_read_file(file_path, ""));
    file_handles.insert(String::from("t_data"), open_read_file(transposed_file_path, ""));
    file_handles.insert(String::from("ct"), open_read_file(file_path, ".ct"));
    file_handles.insert(String::from("rowstart"), open_read_file(file_path, ".rowstart"));
    file_handles.insert(String::from("t_rowstart"), open_read_file(transposed_file_path, ".rowstart"));

    let num_samples: usize = file_handles.get("t_cc").unwrap().len() / (t_max_column_coord_length + 1);

    let t_filter_variable_coords = parse_data_coords(&query_col_indices, file_handles.get("t_rowstart").unwrap(), t_max_row_start_length, file_handles.get("t_data").unwrap().len());

    let mut filter_lines = parse_data_values(0, t_line_length, &t_filter_variable_coords, &file_handles["t_data"], 0);

    let mut t_sample_coords = parse_data_coords(&(0..(num_samples)).collect(), &file_handles.get("t_cc").unwrap(), t_max_column_coord_length, t_line_length);

    for query_col_index in query_col_indices {
        let filter_line_option = filter_lines.next();
        if filter_line_option.is_none() {
            break;
        }
        let filter_line = filter_line_option.unwrap();
        let filter_line_string = decompress_to_string(filter_line);
        t_sample_coords = filter_rows(&filter_line_string, &t_sample_coords, query_col_index, max_column_type_length, &file_handles);
    }

    let variable_indices = get_col_indices_to_query(col_names_file_path);
    let variable_coords = parse_data_coords(&variable_indices,file_handles.get("cc").unwrap(), max_column_coord_length,line_length);
    let mut new_t_sample_coords:Vec<usize> = Vec::new();
    new_t_sample_coords.push(0);

    for sample_coord in t_sample_coords {
        new_t_sample_coords.push(sample_coord.get(0).unwrap() + 1);
    }

    let sample_coords = parse_data_coords(&new_t_sample_coords, file_handles.get("rowstart").unwrap(), max_row_start_length, file_handles.get("data").unwrap().len());

    let path: PathBuf = PathBuf::from(out_file_path);
    let mut out_file =  OpenOptions::new().read(true).write(true).create(true).open(&path)?;
    let chunk_size: usize = 1000;
    let mut out_lines = Vec::new();
    for sample_coord in sample_coords {
        let data_coords = &vec![sample_coord];
        let out_line_option = parse_data_values(0, line_length, data_coords, file_handles.get("data").unwrap(), 0).next();
        if out_line_option.is_none() {
            break;
        }
        let out_line = out_line_option.unwrap();
        let decompressed_out_line_str = decompress_to_string(out_line);
        let out_items = parse_data_values(0, max_column_coord_length, &variable_coords, decompressed_out_line_str.as_ref(), 0);
        out_lines.push(out_items.map(|bytes| convert_bytes_to_string(bytes).trim_end().to_string())
            .collect::<Vec<String>>()
            .join("\t"));
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
