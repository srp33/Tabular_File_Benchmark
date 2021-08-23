use std::env;
use std::fs;
use std::io::{Write, Error};
use memmap2::{MmapOptions, Mmap};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::cmp;
use std::str::FromStr;

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
    let chunk_size: usize = 1000;

    //Determine the column indices to be queried and create the row indices
    let col_indices = get_col_indices_to_query(column_names_file_path);
    let line_length = fs::read_to_string(file_path_ll).expect("Unable to read file").parse().unwrap();
    let max_column_coord_length = fs::read_to_string(file_path_mccl).expect("Unable to read file").parse().unwrap();
    let row_indices: Vec<usize> = (0..num_rows+1).collect();

    //Create the mmaps to be used in parse_data_coords and parse_data_values, open the outfile
    let file_cc = File::open(file_path_cc).unwrap();
    let mmap_cc = unsafe { MmapOptions::new().map(&file_cc).unwrap() };
    let path: PathBuf = PathBuf::from(out_file_path);
    let mut out_file =  OpenOptions::new().read(true).write(true).create(true).open(&path)?;
    let file_data = File::open(data_file_path).unwrap();
    let mmap_data = unsafe { MmapOptions::new().map(&file_data).unwrap() };

    //Get the column coordinates
    let col_coords = parse_data_coords(col_indices, mmap_cc, max_column_coord_length, line_length);

    //Using the knowledge of the column coordinates and the correct location in the rows, isolate it and format it, and write it to the outfile chunk by chunk
    let mut out_lines = Vec::new();
    for row_index in row_indices {
            out_lines.push(parse_data_values(row_index, line_length, &col_coords, &mmap_data));
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
