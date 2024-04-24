use std::error::Error;

use ndarray::{s, Array2, Axis};
use num_traits::Float;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use regex::Regex;

pub struct SpreadSheet2D {
    preamble:Vec<String>,
    col_delimeter:String,
    data:Array2<String>,
    column_headers:Vec<String>
}

impl ToString for SpreadSheet2D {
    fn to_string(&self) -> String {

        let mut s = if self.preamble.is_empty() {
            self.preamble.join("\n")
        }else {
            let mut s = self.preamble.join("\n");
            s.push('\n');
            s
        };

        s.push_str(&self.column_headers.join(&self.col_delimeter));
        s.push('\n');

        self.data.axis_iter(Axis(0)).for_each(|row|{
            let row_str = row.to_vec().join(&self.col_delimeter);
            s.push_str(&row_str);
            s.push('\n');
        });
        s
    }
}

impl SpreadSheet2D {
    pub fn from_string(s:String,col_delimeter:&str,line_offset:usize) -> Self {
        
        let mut rows = s.lines();
    
        let mut preamble = Vec::<String>::new();
        for _ in 0..line_offset {
            if let Some(line) = rows.next() {
                preamble.push(line.to_string());
            }
        }
    
        if !preamble.is_empty() {
            println!("{}",preamble.join("\n"));
        }

        let column_headers:Vec<_> = rows.next().expect("unexpected end of rows!").split(col_delimeter).map(|x|x.to_string()).collect();
        
        let n_columns = column_headers.len();
    
        // read rows into a flat vector
        let mut table_elements = vec![];
    
        let mut col_counter = 0;
        rows.for_each(|row|{
            row.split(col_delimeter).for_each(|entry|{
                col_counter += 1;
                table_elements.push(entry.to_string());
            });
            if col_counter != n_columns {
                panic!("issue with reading row! Missing {} element(s)",n_columns - col_counter);
            }else {
                col_counter = 0;
            }
        });
    
        let n_rows = table_elements.len() / n_columns;
    
        let data = Array2::from_shape_vec((n_rows,n_columns),table_elements)
        .expect("incorrect dimensions for array construction");

        Self {
            col_delimeter: col_delimeter.to_owned(),
            data,
            column_headers,
            preamble,
        }

    }

    pub fn columns_numeric(&self) -> Vec<Vec<f64>> {
        let cols = self.data.columns();
        cols.into_iter().map(|col| {
            let num_col:Vec<_> = col.iter().map(|elem| {
                elem.parse::<f64>().unwrap_or(f64::NAN)
            }).collect();
            num_col
        }).collect()
    }

    fn do_operation(col1:&[f64],col2:&[f64],operation:&str) -> Result<Vec<f64>,Box<dyn Error>> {
        match operation {
            "*" => Ok(col1.par_iter().zip(col2.par_iter()).map(|(&a, &b)| a * b).collect()),
            "/" => Ok(col1.par_iter().zip(col2.par_iter()).map(|(&a, &b)| a / b).collect()),
            "-" => Ok(col1.par_iter().zip(col2.par_iter()).map(|(&a, &b)| a - b).collect()),
            "+" => Ok(col1.par_iter().zip(col2.par_iter()).map(|(&a, &b)| a + b).collect()),
            _=> Err(format!("unknown operation {}",operation))?
        }
    }

    pub fn exract_column(&self,column_label_pattern:&str) -> Result<Vec<f32>,Box<dyn Error>> {
        let idx = column_index(&self.column_headers, column_label_pattern)?;
        Ok(self.extract_column_from_idx(idx))
    }

    fn extract_column_from_idx(&self,column_idx:usize) -> Vec<f32> {
        self.data.slice(s![.., column_idx]).map(|x| x.parse::<f32>().unwrap_or(f32::NAN)).to_vec()
    }

    pub fn column_op(&mut self,col1:&str,operation:&str,col2:&str,new_col_name:&str) -> Result<(),Box<dyn Error>> {

        let idx1 = column_index(&self.column_headers, col1)?;
        let idx2 = column_index(&self.column_headers, col2)?;
    
        // Extract and parse columns as f64
        let col1 = self.data.slice(s![.., idx1]).map(|x| x.parse::<f64>().unwrap_or(f64::NAN));
        let col2 = self.data.slice(s![.., idx2]).map(|x| x.parse::<f64>().unwrap_or(f64::NAN));
    
        // Perform division 
        let new_col = Self::do_operation(col1.as_slice().unwrap(),col2.as_slice().unwrap(),operation)?;

        // Convert result to strings
        let new_col_str: Vec<_> = new_col.iter().map(|&x| x.to_string()).collect();
    
        let n_rows = self.data.shape()[0];

        let to_append = Array2::from_shape_vec((n_rows, 1), new_col_str).unwrap();
    
        //println!("to append shape: {:?}",to_append.shape());
        //println!("data shape: {:?}",self.data.shape());
    
       // Stack the new column with the original data
        self.data.append(Axis(1), to_append.view()).unwrap();

        self.column_headers.push(new_col_name.to_string());

        Ok(())
    }

    pub fn column_headers(&self) -> &[String] {
        &self.column_headers
    }

}

fn column_index(column_header: &[String], pattern: &str) -> Result<usize,Box<dyn Error>> {
    let re = Regex::new(pattern)?;
    let matches:Vec<_> = column_header.iter().enumerate().filter_map(|(idx,header)|{
        if re.is_match(header) {
            Some((idx,header))
        }else {
            None
        }
    }).collect();
    if matches.len() > 1 {
        println!("too many matches found for pattern: '{}'",pattern);
        println!("matches found:");
        for m in matches {
            println!("col: {} : {}",m.0 + 1,m.1);
        }
        Err("consider narrowing your search pattern")?
    }else if matches.is_empty() {
        Err(format!("no matches found for '{}'",pattern))?
    }else {
        Ok(matches[0].0)
    }
}