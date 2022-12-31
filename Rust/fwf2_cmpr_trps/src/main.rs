use std::env;
use std::fs;
use std::io::{Write, Error};
use memmap2::{MmapOptions, Mmap};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::path::PathBuf;
use std::cmp;
use std::str::FromStr;
use std::collections::HashMap;
use std::collections::HashSet;

fn open_read_file(file_path: &String, file_extension: &str) -> Mmap {
    let the_file = File::open(file_path.clone() + file_extension).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&the_file).unwrap() };
    return mmap;
}

fn read_int_from_file(file_path: &String, file_extension: &str) -> usize {
    return fs::read_to_string(file_path.clone() + file_extension).expect("Unable to read file").parse().unwrap();
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

fn filter_discrete_simple(row_indices: &Vec<usize>, data_values: &Vec<String>) -> Vec<usize> {
    let mut return_values:Vec<usize> = Vec::new();

    for (i, &row_index) in row_indices.iter().enumerate() {
        let value = &data_values[i];

        if value == "AM" || value == "NZ" {
            return_values.push(row_index);
        }
    }

    return return_values;
}

fn filter_discrete_startsendswith(row_indices: &Vec<usize>, data_values: &Vec<String>) -> Vec<usize> {
    let mut return_values:Vec<usize> = Vec::new();

    for (i, &row_index) in row_indices.iter().enumerate() {
        let value = &data_values[i];

        if value.starts_with("A") || value.ends_with("Z") {
            return_values.push(row_index);
        }
    }

    return return_values;
}

fn filter_numeric(row_indices: &Vec<usize>, data_values: &Vec<String>) -> Vec<usize> {
    let mut return_values:Vec<usize> = Vec::new();

    for (i, &row_index) in row_indices.iter().enumerate() {
        let value:f64 = data_values[i].parse().unwrap();

        if value >= 0.1 {
            return_values.push(row_index);
        }
    }

    return return_values;
}

fn get_compressed_line(start_offset: &usize, segment_length: &usize, data_coords: &Vec<usize>, str_like_object: &[u8]) -> String {
    let end_offset = 0;
    let start_pos = start_offset * segment_length;
    let result = &str_like_object[(start_pos + data_coords[1]) as usize..(start_pos + data_coords[2] + end_offset) as usize];

    return decompress_to_string(result);
}

fn parse_data_values<'a>(start_offset: &'a usize, segment_length: &'a usize, data_coords: &'a Vec<Vec<usize>>, str_like_object: &'a [u8]) -> Vec<String> {
    let mut values: Vec<String> = Vec::new();
    let start_pos = start_offset * segment_length;

    for coords in data_coords {
        let result = &str_like_object[(start_pos + coords[1]) as usize..(start_pos + coords[2]) as usize];
        let result_string = std::str::from_utf8(result).unwrap().trim_end().to_owned();
        values.push(result_string);
    }

    return values;

}

fn decompress_to_string(input: &[u8]) -> String {
    let decompressed_bytes = zstd::stream::decode_all(input).unwrap();
    let decompressed_line_str = convert_bytes_to_string(&decompressed_bytes);
    decompressed_line_str
}

fn convert_bytes_to_string(col_type: &[u8]) -> String {
    let col_type_string = std::str::from_utf8(col_type).unwrap().to_owned();
    col_type_string
}

fn main () -> Result<(), Error>  {
    let args: Vec<String> = env::args().collect();
    let compression_method = &args[1];
    let compression_level = &args[2];
    let query_type = &args[3];
    let in_file_path = &args[4];
    let out_file_path = &args[5];
    let discrete_query_col_name = &args[6];
    let numeric_query_col_name = &args[7];
    let col_names_to_keep = &args[8];

    let path = Path::new(in_file_path);
    let in_dir_name = path.parent().expect("That path does not exist").to_string_lossy();
    let in_file_name = path.file_name().expect("That path does not exist").to_string_lossy();

    let portrait_file_path = &format!("{in_dir_name}/compressed/{in_file_name}.{compression_method}_{compression_level}");
    let landscape_file_path = &format!("{in_dir_name}/transposed_and_compressed/{in_file_name}.{compression_method}_{compression_level}");

    let mut portrait_file_handles = HashMap::new();
    portrait_file_handles.insert(String::from("cc"), open_read_file(portrait_file_path, ".cc"));
    portrait_file_handles.insert(String::from("cn"), open_read_file(portrait_file_path, ".cn"));
    portrait_file_handles.insert(String::from("data"), open_read_file(portrait_file_path, ""));
    portrait_file_handles.insert(String::from("rowstart"), open_read_file(portrait_file_path, ".rowstart"));

    let mut landscape_file_handles = HashMap::new();
    landscape_file_handles.insert(String::from("cc"), open_read_file(landscape_file_path, ".cc"));
    landscape_file_handles.insert(String::from("data"), open_read_file(landscape_file_path, ""));
    landscape_file_handles.insert(String::from("rowstart"), open_read_file(landscape_file_path, ".rowstart"));

    let portrait_line_length: usize = read_int_from_file(portrait_file_path, ".ll");
    let portrait_max_column_coord_length: usize = read_int_from_file(portrait_file_path, ".mccl");
    let portrait_max_column_name_length: usize = read_int_from_file(portrait_file_path, ".mcnl");
    let portrait_max_row_start_length: usize = read_int_from_file(portrait_file_path, ".mrsl");
    let portrait_num_rows: usize = portrait_file_handles.get("rowstart").unwrap().len() / (portrait_max_row_start_length + 1) - 1;

    let landscape_line_length: usize = read_int_from_file(landscape_file_path, ".ll");
    let landscape_max_column_coord_length: usize = read_int_from_file(landscape_file_path, ".mccl");
    let landscape_max_row_start_length: usize = read_int_from_file(landscape_file_path, ".mrsl");

    let path: PathBuf = PathBuf::from(out_file_path);
    let mut out_file =  OpenOptions::new().read(true).write(true).create(true).open(&path)?;

    let mut discrete_query_col_index = 0;
    let mut numeric_query_col_index = 0;
    let out_col_coords: Vec<Vec<usize>>;

    if col_names_to_keep == "all_columns" {
        let num_cols = portrait_file_handles.get("cn").unwrap().len() / (portrait_max_column_name_length + 1);
        let mut out_col_indices: Vec<usize> = Vec::new();
        for i in 0..num_cols {
            out_col_indices.push(i);
        }

        let mut column_names: Vec<String> = Vec::new();

        for i in out_col_indices.iter() {
            let start_i = i * (portrait_max_column_name_length + 1) as usize;
            let end_i = start_i + portrait_max_column_name_length + 1 as usize;
            let column_name = std::str::from_utf8(&portrait_file_handles.get("cn").unwrap()[start_i..end_i]).unwrap().trim_end().to_string();
            column_names.push(column_name.clone());

            if &column_name == discrete_query_col_name {
                discrete_query_col_index = *i;
            }
            else {
                if &column_name == numeric_query_col_name {
                    numeric_query_col_index = *i;
                }
            }
        }

        out_col_coords = parse_data_coords(&out_col_indices, portrait_file_handles.get("cc").unwrap(), portrait_max_column_coord_length, portrait_line_length);

        out_file.write((column_names.join("\t") + "\n").as_bytes()).unwrap();
    }
    else {
        let col_names_to_keep2: Vec<String> = col_names_to_keep.split(",").map(|x| FromStr::from_str(x).unwrap()).collect::<Vec<String>>();

        let mut column_names_to_find: HashSet<String> = HashSet::new(); 
        column_names_to_find.insert(discrete_query_col_name.clone());
        column_names_to_find.insert(numeric_query_col_name.clone());
        for column_name in col_names_to_keep2.iter() {
            column_names_to_find.insert(column_name.to_string());
        }

        let mut column_name_indices = HashMap::<String, usize>::new();

        let cn_length: usize = portrait_file_handles.get("cn").unwrap().len();
        for i in (0..cn_length).step_by(portrait_max_column_name_length + 1) {
            let column_name = std::str::from_utf8(&portrait_file_handles.get("cn").unwrap()[i..(i + portrait_max_column_name_length)]).unwrap().trim_end().to_owned();

            if column_names_to_find.contains(&column_name) {
                column_name_indices.insert(column_name, (i / (portrait_max_column_name_length + 1)) as usize);
            }
        }

        discrete_query_col_index = *column_name_indices.get(discrete_query_col_name).unwrap();
        numeric_query_col_index = *column_name_indices.get(numeric_query_col_name).unwrap();

        let mut out_col_indices: Vec<usize> = Vec::new();
        for name in col_names_to_keep2.iter() {
            out_col_indices.push(*column_name_indices.get(name).unwrap());
        }

        out_col_coords = parse_data_coords(&out_col_indices, portrait_file_handles.get("cc").unwrap(), portrait_max_column_coord_length, portrait_line_length);

        out_file.write((col_names_to_keep2.join("\t") + "\n").as_bytes()).unwrap();
    }

    let landscape_num_cols = landscape_file_handles.get("cc").unwrap().len() / (landscape_max_column_coord_length + 1);
    let landscape_col_coords = parse_data_coords(&(0..(landscape_num_cols)).collect(), landscape_file_handles.get("cc").unwrap(), landscape_max_column_coord_length, landscape_line_length);

    let mut discrete_query_col_indices: Vec<usize> = Vec::new();
    discrete_query_col_indices.push(discrete_query_col_index);
    let discrete_query_col_coords = &parse_data_coords(&discrete_query_col_indices, landscape_file_handles.get("rowstart").unwrap(), landscape_max_row_start_length, landscape_file_handles.get("data").unwrap().len());
    let discrete_col_string = get_compressed_line(&0usize, &landscape_line_length, &discrete_query_col_coords[0], landscape_file_handles.get("data").unwrap());
    let discrete_col_values = parse_data_values(&0usize, &0usize, &landscape_col_coords, discrete_col_string.as_ref());

    let mut keep_row_indices:Vec<usize> = (0..portrait_num_rows).collect();

    if query_type == "simple" {
        keep_row_indices = filter_discrete_simple(&keep_row_indices, &discrete_col_values);
    }
    else {
        if query_type == "startsendswith" {
            keep_row_indices = filter_discrete_startsendswith(&keep_row_indices, &discrete_col_values);
        }
    }
    //print!("{:#?}\n", keep_row_indices);

    let mut landscape_col_coords2: Vec<Vec<usize>> = Vec::new();
    for row_index in keep_row_indices.iter() {
        landscape_col_coords2.push(landscape_col_coords[*row_index].clone());
    }

    let mut numeric_query_col_indices: Vec<usize> = Vec::new();
    numeric_query_col_indices.push(numeric_query_col_index);
    let numeric_query_col_coords = &parse_data_coords(&numeric_query_col_indices, landscape_file_handles.get("rowstart").unwrap(), landscape_max_row_start_length, landscape_file_handles.get("data").unwrap().len());
    let numeric_col_string = get_compressed_line(&0usize, &landscape_line_length, &numeric_query_col_coords[0], landscape_file_handles.get("data").unwrap());
    let numeric_col_values = parse_data_values(&0usize, &0usize, &landscape_col_coords2, numeric_col_string.as_ref());

    keep_row_indices = filter_numeric(&keep_row_indices, &numeric_col_values);

    let portrait_row_coords = parse_data_coords(&keep_row_indices, portrait_file_handles.get("rowstart").unwrap(), portrait_max_row_start_length, portrait_file_handles.get("data").unwrap().len());

    for row_coords in portrait_row_coords {
        let line_string = get_compressed_line(&0usize, &portrait_line_length, &row_coords, portrait_file_handles.get("data").unwrap());
        let out_line = parse_data_values(&0usize, &0usize, &out_col_coords, line_string.as_ref()).join("\t") + "\n";
        out_file.write(out_line.as_bytes()).unwrap();
    }

    Ok(())
}
