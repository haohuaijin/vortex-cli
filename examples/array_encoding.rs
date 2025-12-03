// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Inspect Encodings in Vortex Files
//!
//! This example demonstrates how to read a Vortex file and inspect the encoding
//! of all columns, including nested encodings. It shows which compression methods
//! are used for each column (zstd, dictionary, RLE, ALP, etc.)
//!
//! Usage:
//!   cargo run --example inspect_encodings -- <path-to-vortex-file>
//!
//! Or to create and inspect a sample file:
//!   cargo run --example inspect_encodings

use std::path::Path;

use vortex::VortexSessionDefault;
use vortex_array::Array;
use vortex_array::ArrayRef;
use vortex_array::ArrayVisitor;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::StructArray;
use vortex_array::stream::ArrayStreamExt;
use vortex_file::OpenOptionsSessionExt;
use vortex_session::VortexSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "default/log.vortex";

    println!("\n=== Inspecting Vortex File: {} ===\n", file_path);

    inspect_vortex_file(file_path).await?;

    println!("\n=== Inspection Complete ===");
    Ok(())
}

/// Inspects a Vortex file and displays encoding information for all columns
async fn inspect_vortex_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let session = VortexSession::default();

    // Open the file
    let reader = session.open_options().open(Path::new(path)).await?;

    println!("File Information:");
    println!("  Rows: {}", reader.row_count());
    println!("  DType: {}", reader.dtype());
    println!();

    // Read the array
    let array = reader.scan()?.into_array_stream()?.read_all().await?;

    println!("Root Array Encoding:");
    println!("  {}", array.encoding_id());
    println!();

    // Try to extract column names from DType if it's a struct type
    if let Some(struct_fields) = reader.dtype().as_struct_fields_opt() {
        println!("Columns ({} total):\n", struct_fields.names().len());

        // Try to get encodings from struct in the tree
        let column_encodings = extract_column_encodings_from_tree(&array);

        if !column_encodings.is_empty() {
            // Successfully found column encodings
            for (idx, (name, child)) in struct_fields
                .names()
                .iter()
                .zip(column_encodings.iter())
                .enumerate()
            {
                let prefix = if idx == column_encodings.len() - 1 {
                    "└─"
                } else {
                    "├─"
                };

                // Get encoding summary
                let enc_id = child.encoding_id();
                let enc_desc = get_encoding_description(enc_id.as_ref(), child);

                println!(
                    "{} {} -> {} ({} bytes){}",
                    prefix,
                    name,
                    enc_id,
                    child.nbytes(),
                    enc_desc
                );
            }
        } else {
            // Couldn't extract column encodings, just show names
            for (idx, name) in struct_fields.names().iter().enumerate() {
                let prefix = if idx == struct_fields.names().len() - 1 {
                    "└─"
                } else {
                    "├─"
                };
                println!("{} {}", prefix, name);
            }
        }

        // Add a summary of zstd usage
        println!("\n--- Compression Summary ---");
        let zstd_columns = find_columns_with_encoding(&array, "vortex.zstd", struct_fields.names());
        if !zstd_columns.is_empty() {
            println!("Zstd compressed columns: {}", zstd_columns.join(", "));
        } else {
            println!("No zstd compressed columns found");
        }

        // Show detailed encoding tree for debugging
        println!("\n--- Detailed Encoding Tree ---");
        analyze_encoding_tree_with_names(&array, 0, struct_fields.names());
    } else {
        // Not a struct type, show the root array tree
        println!("Array Encodings:\n");
        analyze_encoding_tree(&array, 0);
    }

    Ok(())
}

/// Recursively analyzes and displays the encoding tree with column names
fn analyze_encoding_tree_with_names(
    array: &ArrayRef,
    depth: usize,
    column_names: &vortex_dtype::FieldNames,
) {
    let indent = "  ".repeat(depth);
    let encoding_id = array.encoding_id();

    // Build compact encoding description
    let encoding_desc = get_encoding_description(encoding_id.as_ref(), array);

    // Display encoding information
    println!(
        "{}└─ {} ({} bytes){}",
        indent,
        encoding_id,
        array.nbytes(),
        encoding_desc
    );

    // Check if this is a struct by encoding ID
    if encoding_id.as_ref() == "vortex.struct" {
        // Get children (which are the struct fields)
        let children = get_array_children(array);

        if children.len() == column_names.len() {
            // Display each field with its column name
            for (idx, (name, child)) in column_names.iter().zip(children.iter()).enumerate() {
                let prefix = if idx == column_names.len() - 1 {
                    "└─"
                } else {
                    "├─"
                };
                println!("{}  {} Column [{}]:", indent, prefix, name);
                analyze_encoding_tree(child, depth + 2);
            }
            return;
        }
    }

    // Not a struct or column count doesn't match, recurse into children
    let children = get_array_children(array);

    if !children.is_empty() {
        for child in children {
            // Recursively call with names in case we find struct deeper in the tree
            analyze_encoding_tree_with_names(&child, depth + 1, column_names);
        }
    }
}

/// Recursively analyzes and displays the encoding tree of an array
fn analyze_encoding_tree(array: &ArrayRef, depth: usize) {
    let indent = "  ".repeat(depth);
    let encoding_id = array.encoding_id();

    // Build compact encoding description
    let encoding_desc = get_encoding_description(encoding_id.as_ref(), array);

    // Display encoding information in one line
    println!(
        "{}└─ {} ({} bytes){}",
        indent,
        encoding_id,
        array.nbytes(),
        encoding_desc
    );

    // Check for child arrays (nested encodings)
    let children = get_array_children(array);

    if !children.is_empty() {
        for child in children {
            analyze_encoding_tree(&child, depth + 1);
        }
    }
}

/// Get compact description for specific encodings
fn get_encoding_description(encoding_id: &str, array: &ArrayRef) -> String {
    match encoding_id {
        "vortex.zstd" => " [Zstd]".to_string(),
        "vortex.dict" => {
            if let Some(dict_array) = array.as_any().downcast_ref::<DictArray>() {
                format!(
                    " [Dict: {} values, {} codes]",
                    dict_array.values().len(),
                    dict_array.codes().len()
                )
            } else {
                " [Dict]".to_string()
            }
        }
        "vortex.runend" => {
            if let Some(rle_array) = array.as_any().downcast_ref::<vortex_runend::RunEndArray>() {
                format!(" [RLE: {} runs]", rle_array.ends().len())
            } else {
                " [RLE]".to_string()
            }
        }
        "vortex.sparse" => " [Sparse]".to_string(),
        "vortex.alp" => " [ALP float compression]".to_string(),
        "vortex.alprd" => " [ALP-RD]".to_string(),
        "vortex.pco" => " [PCO quantile compression]".to_string(),
        "vortex.for" => " [Frame-of-Reference]".to_string(),
        "fastlanes.bitpacked" => " [Bit-packed]".to_string(),
        "vortex.delta" => " [Delta]".to_string(),
        "vortex.fsst" => " [FSST string compression]".to_string(),
        "vortex.sequence" => " [Sequence]".to_string(),
        "vortex.constant" => " [Constant]".to_string(),
        _ => String::new(),
    }
}

/// Helper function to get children arrays
fn get_array_children(array: &ArrayRef) -> Vec<ArrayRef> {
    // Use the ArrayVisitor trait to get children
    array.children()
}

/// Extract column arrays from the tree structure
fn extract_column_encodings_from_tree(array: &ArrayRef) -> Vec<ArrayRef> {
    // Check if this is a struct
    if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
        return struct_array.fields().iter().cloned().collect();
    }

    // Recursively search children
    for child in array.children() {
        let result = extract_column_encodings_from_tree(&child);
        if !result.is_empty() {
            return result;
        }
    }

    Vec::new()
}

/// Find columns that use a specific encoding (recursively search)
fn find_columns_with_encoding(
    array: &ArrayRef,
    target_encoding: &str,
    column_names: &vortex_dtype::FieldNames,
) -> Vec<String> {
    let mut result = Vec::new();

    // Check if the entire array tree contains the target encoding
    // This handles cases where data is chunked
    if contains_encoding(array, target_encoding) {
        // If we find the encoding anywhere in the tree, report all columns
        // (since we can't easily map chunks back to individual columns)
        for name in column_names.iter() {
            result.push(name.to_string());
        }
    } else {
        // Try the old logic for non-chunked cases
        let column_encodings = extract_column_encodings_from_tree(array);

        if !column_encodings.is_empty() {
            for (name, child) in column_names.iter().zip(column_encodings.iter()) {
                if contains_encoding(child, target_encoding) {
                    result.push(name.to_string());
                }
            }
        }
    }

    result
}

/// Check if an array or its children contain a specific encoding
fn contains_encoding(array: &ArrayRef, target_encoding: &str) -> bool {
    // Check current array
    if array.encoding_id().as_ref() == target_encoding {
        return true;
    }

    // Recursively check children
    for child in array.children() {
        if contains_encoding(&child, target_encoding) {
            return true;
        }
    }

    false
}
