use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use vortex::VortexSessionDefault;
use vortex_array::{Array, ArrayRef, ArrayVisitor};
use vortex_array::arrays::{DictArray, StructArray};
use vortex_array::stream::ArrayStreamExt;
use vortex_file::{OpenOptionsSessionExt, VortexFile, register_default_encodings};
use vortex_flatbuffers::footer as fb_footer;
use vortex_layout::display::DisplayLayoutTree;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "vortex-cli")]
#[command(about = "A CLI tool for inspecting Vortex format files", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Display metadata information from a Vortex file
    Metadata {
        /// Path to the Vortex file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: OutputFormat,
    },

    /// Display schema (Arrow schema) from a Vortex file
    Schema {
        /// Path to the Vortex file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: OutputFormat,

        /// Show detailed field information
        #[arg(short, long)]
        verbose: bool,
    },

    /// Display layout information from a Vortex file
    Layout {
        /// Path to the Vortex file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: OutputFormat,

        /// Show detailed layout tree
        #[arg(short, long)]
        verbose: bool,
    },

    /// Display all information (metadata, schema, and layout)
    Inspect {
        /// Path to the Vortex file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: OutputFormat,

        /// Show verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Inspect array encodings and compression methods used in a Vortex file
    Encoding {
        /// Path to the Vortex file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: OutputFormat,

        /// Show detailed encoding tree
        #[arg(short, long)]
        verbose: bool,
    },
}

#[derive(Clone, Debug)]
enum OutputFormat {
    Json,
    Text,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "text" => Ok(OutputFormat::Text),
            _ => Err(format!("Invalid format: {}. Use 'json' or 'text'", s)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Metadata { file, format } => {
            show_metadata(&file, format).await?;
        }
        Commands::Schema {
            file,
            format,
            verbose,
        } => {
            show_schema(&file, format, verbose).await?;
        }
        Commands::Layout {
            file,
            format,
            verbose,
        } => {
            show_layout(&file, format, verbose).await?;
        }
        Commands::Inspect {
            file,
            format,
            verbose,
        } => {
            show_inspect(&file, format, verbose).await?;
        }
        Commands::Encoding {
            file,
            format,
            verbose,
        } => {
            show_encoding(&file, format, verbose).await?;
        }
    }

    Ok(())
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

/// Read footer flatbuffer from file and extract encoding specs
async fn read_footer_encodings(path: &Path) -> Result<(Vec<String>, Vec<String>)> {
    use vortex_flatbuffers::footer as fb;

    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    if file_size < 8 {
        anyhow::bail!("File too small to be a valid Vortex file");
    }

    // Read EOF (last 8 bytes)
    // Format: [version: 2 bytes][postscript_len: 2 bytes][magic "VTXF": 4 bytes]
    file.seek(std::io::SeekFrom::End(-8)).await?;
    let mut eof = vec![0u8; 8];
    file.read_exact(&mut eof).await?;

    // Verify magic bytes (last 4 bytes)
    if &eof[4..8] != b"VTXF" {
        anyhow::bail!("Invalid magic bytes, not a Vortex file");
    }

    // Extract postscript size (bytes 2-4)
    let postscript_size = u16::from_le_bytes([eof[2], eof[3]]) as u64;

    if postscript_size == 0 {
        anyhow::bail!("Invalid postscript size: 0");
    }

    if postscript_size + 8 > file_size {
        anyhow::bail!("Postscript size {} exceeds file size {}", postscript_size, file_size);
    }

    // Read postscript (before EOF)
    let postscript_offset = file_size - 8 - postscript_size;
    file.seek(std::io::SeekFrom::Start(postscript_offset)).await?;
    let mut postscript_bytes = vec![0u8; postscript_size as usize];
    file.read_exact(&mut postscript_bytes).await?;

    if postscript_bytes.is_empty() {
        anyhow::bail!("Failed to read postscript bytes");
    }

    // Parse postscript flatbuffer (use root_unchecked to skip alignment check)
    let fb_postscript = unsafe {
        flatbuffers::root_unchecked::<fb::Postscript>(&postscript_bytes)
    };

    // Get footer segment info from postscript
    let footer_segment = fb_postscript.footer()
        .ok_or_else(|| anyhow::anyhow!("Postscript missing footer segment"))?;
    let footer_offset = footer_segment.offset();
    let footer_length = footer_segment.length();

    if footer_length == 0 {
        anyhow::bail!("Invalid footer length: 0");
    }

    if footer_offset + footer_length as u64 > file_size {
        anyhow::bail!("Footer extends beyond file size");
    }

    // Read footer bytes
    file.seek(std::io::SeekFrom::Start(footer_offset)).await?;
    let mut footer_bytes = vec![0u8; footer_length as usize];
    file.read_exact(&mut footer_bytes).await?;

    if footer_bytes.is_empty() {
        anyhow::bail!("Failed to read footer bytes");
    }

    // Parse footer flatbuffer (use root_unchecked to skip alignment check)
    let fb_footer = unsafe {
        flatbuffers::root_unchecked::<fb_footer::Footer>(&footer_bytes)
    };

    // Extract array encodings
    let mut array_encodings = Vec::new();
    if let Some(array_specs) = fb_footer.array_specs() {
        for spec in array_specs.iter() {
            let encoding_id = spec.id();
            if !encoding_id.is_empty() && !array_encodings.contains(&encoding_id.to_string()) {
                array_encodings.push(encoding_id.to_string());
            }
        }
    }

    // Extract layout encodings
    let mut layout_encodings = Vec::new();
    if let Some(layout_specs) = fb_footer.layout_specs() {
        for spec in layout_specs.iter() {
            let encoding_id = spec.id();
            if !encoding_id.is_empty() && !layout_encodings.contains(&encoding_id.to_string()) {
                layout_encodings.push(encoding_id.to_string());
            }
        }
    }

    Ok((array_encodings, layout_encodings))
}

async fn open_vortex_file(path: &Path) -> Result<VortexFile> {
    // Create a new Vortex session
    let session = Arc::new(VortexSession::default());

    // Register default encodings
    register_default_encodings(&session);

    // Open the Vortex file
    let vortex_file = session
        .open_options()
        .open(path.to_path_buf())
        .await
        .context(format!("Failed to open Vortex file: {}", path.display()))?;

    Ok(vortex_file)
}

async fn show_metadata(path: &Path, format: OutputFormat) -> Result<()> {
    let vortex_file = open_vortex_file(path).await?;
    let dtype = vortex_file.dtype();

    match format {
        OutputFormat::Json => {
            let metadata = serde_json::json!({
                "file": path.display().to_string(),
                "row_count": vortex_file.row_count(),
                "dtype": format!("{:?}", dtype),
            });
            println!("{}", serde_json::to_string_pretty(&metadata)?);
        }
        OutputFormat::Text => {
            println!("=== Vortex File Metadata ===");
            println!("File: {}", path.display());
            println!("Row Count: {}", vortex_file.row_count());

            // Show a simplified dtype or field count for struct types
            if let Some(fields) = dtype.as_struct_fields_opt() {
                println!("Type: Struct with {} fields", fields.names().len());
                println!("Nullable: {}", dtype.is_nullable());
            } else {
                println!("DType: {:?}", dtype);
            }
        }
    }

    Ok(())
}

async fn show_schema(path: &Path, format: OutputFormat, verbose: bool) -> Result<()> {
    let vortex_file = open_vortex_file(path).await?;
    let dtype = vortex_file.dtype();

    match format {
        OutputFormat::Json => {
            let arrow_schema = dtype.to_arrow_schema()?;
            let schema_json = serde_json::json!({
                "file": path.display().to_string(),
                "vortex_dtype": format!("{:?}", dtype),
                "arrow_schema": format!("{:?}", arrow_schema),
            });
            println!("{}", serde_json::to_string_pretty(&schema_json)?);
        }
        OutputFormat::Text => {
            println!("=== Vortex Schema ===");
            println!("File: {}", path.display());

            let arrow_schema = dtype.to_arrow_schema()?;

            if verbose {
                println!("\nVortex DType:");
                println!("{:#?}", dtype);
                println!();
            }

            // Print schema as a table
            println!("\nSchema Fields:");
            println!(
                "{:<5} {:<60} {:<20} {:<10}",
                "Index", "Field Name", "Data Type", "Nullable"
            );
            println!("{}", "-".repeat(100));

            for (idx, field) in arrow_schema.fields().iter().enumerate() {
                let data_type_str = format!("{:?}", field.data_type());
                let nullable_str = if field.is_nullable() { "true" } else { "false" };

                println!(
                    "{:<5} {:<60} {:<20} {:<10}",
                    idx,
                    truncate_string(field.name(), 60),
                    truncate_string(&data_type_str, 20),
                    nullable_str
                );

                if verbose && !field.metadata().is_empty() {
                    println!("      Metadata: {:?}", field.metadata());
                }
            }

            println!("\nTotal fields: {}", arrow_schema.fields().len());
        }
    }

    Ok(())
}

async fn show_layout(path: &Path, format: OutputFormat, verbose: bool) -> Result<()> {
    let vortex_file = open_vortex_file(path).await?;
    let layout = vortex_file.footer().layout();

    match format {
        OutputFormat::Json => {
            let layout_json = serde_json::json!({
                "file": path.display().to_string(),
                "layout": format!("{:?}", layout),
            });
            println!("{}", serde_json::to_string_pretty(&layout_json)?);
        }
        OutputFormat::Text => {
            println!("=== Vortex Layout ===");
            println!("File: {}", path.display());

            // Show summary info
            println!("\nLayout Type: {}", layout.encoding());
            println!("Row Count: {}", layout.row_count());
            println!("Children: {}", layout.nchildren());

            // Show column encoding summary
            if layout.encoding().to_string() == "vortex.struct" {
                println!("\nColumn Encodings:");
                println!("{:<5} {:<60} {:<20}", "Index", "Column Name", "Encoding Type");
                println!("{}", "-".repeat(90));

                for idx in 0..layout.nchildren() {
                    if let Ok(child) = layout.child(idx) {
                        let child_type = layout.child_type(idx);
                        let col_name = child_type.name();
                        let encoding = child.encoding().to_string();

                        // Get the actual data encoding (skip stats wrapper if present)
                        let data_encoding = if encoding == "vortex.stats" && child.nchildren() > 0 {
                            if let Ok(data_child) = child.child(0) {
                                data_child.encoding().to_string()
                            } else {
                                encoding.clone()
                            }
                        } else {
                            encoding.clone()
                        };

                        println!(
                            "{:<5} {:<60} {:<20}",
                            idx,
                            truncate_string(&col_name.to_string(), 60),
                            data_encoding
                        );
                    }
                }
                println!();
            }

            // Extract and display all encodings from footer
            match read_footer_encodings(path).await {
                Ok((array_encodings, layout_encodings)) => {
                    println!("\nArray Encodings (compression methods):");
                    if array_encodings.is_empty() {
                        println!("  (none)");
                    } else {
                        for (idx, encoding) in array_encodings.iter().enumerate() {
                            println!("  {}. {}", idx + 1, encoding);
                        }
                    }

                    println!("\nLayout Encodings:");
                    if layout_encodings.is_empty() {
                        println!("  (none)");
                    } else {
                        for (idx, encoding) in layout_encodings.iter().enumerate() {
                            println!("  {}. {}", idx + 1, encoding);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to extract encodings from footer: {}", e);
                }
            }

            println!("\nLayout Tree:");
            let display_tree = DisplayLayoutTree::new(layout.clone(), verbose);
            println!("{}", display_tree);
        }
    }

    Ok(())
}

async fn show_inspect(path: &Path, format: OutputFormat, verbose: bool) -> Result<()> {
    let vortex_file = open_vortex_file(path).await?;
    let dtype = vortex_file.dtype();
    let layout = vortex_file.footer().layout();
    let row_count = vortex_file.row_count();
    let file_stats = vortex_file.file_stats();

    match format {
        OutputFormat::Json => {
            let arrow_schema = dtype.to_arrow_schema()?;

            let mut inspect_json = serde_json::json!({
                "file": path.display().to_string(),
                "metadata": {
                    "row_count": row_count,
                    "dtype": format!("{:?}", dtype),
                },
                "schema": {
                    "vortex_dtype": format!("{:?}", dtype),
                    "arrow_schema": format!("{:?}", arrow_schema),
                },
                "layout": format!("{:?}", layout),
            });

            if let Some(stats) = file_stats {
                inspect_json["statistics"] = serde_json::json!({
                    "available": true,
                    "stats": format!("{:?}", stats),
                });
            }

            println!("{}", serde_json::to_string_pretty(&inspect_json)?);
        }
        OutputFormat::Text => {
            println!("=== Vortex File Inspection ===");
            println!("File: {}", path.display());

            println!("\n--- Metadata ---");
            println!("Row Count: {}", row_count);

            // Show a simplified dtype or field count for struct types
            if let Some(fields) = dtype.as_struct_fields_opt() {
                println!("Type: Struct with {} fields", fields.names().len());
                println!("Nullable: {}", dtype.is_nullable());
            } else {
                println!("DType: {:?}", dtype);
            }

            println!("\n--- Schema ---");
            let arrow_schema = dtype.to_arrow_schema()?;

            if verbose {
                println!("Vortex DType:");
                println!("{:#?}", dtype);
                println!();
            }

            // Print schema as a table
            println!("Schema Fields:");
            println!(
                "{:<5} {:<60} {:<20} {:<10}",
                "Index", "Field Name", "Data Type", "Nullable"
            );
            println!("{}", "-".repeat(100));

            for (idx, field) in arrow_schema.fields().iter().enumerate() {
                let data_type_str = format!("{:?}", field.data_type());
                let nullable_str = if field.is_nullable() { "true" } else { "false" };

                println!(
                    "{:<5} {:<60} {:<20} {:<10}",
                    idx,
                    truncate_string(field.name(), 60),
                    truncate_string(&data_type_str, 20),
                    nullable_str
                );

                if verbose && !field.metadata().is_empty() {
                    println!("      Metadata: {:?}", field.metadata());
                }
            }

            println!("\nTotal fields: {}", arrow_schema.fields().len());

            println!("\n--- Layout ---");
            println!("Layout Type: {}", layout.encoding());
            println!("Children: {}", layout.nchildren());

            // Show column encoding summary
            if layout.encoding().to_string() == "vortex.struct" {
                println!("\nColumn Encodings:");
                println!("{:<5} {:<60} {:<20}", "Index", "Column Name", "Encoding Type");
                println!("{}", "-".repeat(90));

                for idx in 0..layout.nchildren() {
                    if let Ok(child) = layout.child(idx) {
                        let child_type = layout.child_type(idx);
                        let col_name = child_type.name();
                        let encoding = child.encoding().to_string();

                        // Get the actual data encoding (skip stats wrapper if present)
                        let data_encoding = if encoding == "vortex.stats" && child.nchildren() > 0 {
                            if let Ok(data_child) = child.child(0) {
                                data_child.encoding().to_string()
                            } else {
                                encoding.clone()
                            }
                        } else {
                            encoding.clone()
                        };

                        println!(
                            "{:<5} {:<60} {:<20}",
                            idx,
                            truncate_string(&col_name.to_string(), 60),
                            data_encoding
                        );
                    }
                }
            }

            // Extract and display all encodings from footer
            match read_footer_encodings(path).await {
                Ok((array_encodings, layout_encodings)) => {
                    println!("\nArray Encodings (compression methods):");
                    if array_encodings.is_empty() {
                        println!("  (none)");
                    } else {
                        for (idx, encoding) in array_encodings.iter().enumerate() {
                            println!("  {}. {}", idx + 1, encoding);
                        }
                    }

                    println!("\nLayout Encodings:");
                    if layout_encodings.is_empty() {
                        println!("  (none)");
                    } else {
                        for (idx, encoding) in layout_encodings.iter().enumerate() {
                            println!("  {}. {}", idx + 1, encoding);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to extract encodings from footer: {}", e);
                }
            }

            println!("\nLayout Tree:");
            let display_tree = DisplayLayoutTree::new(layout.clone(), verbose);
            println!("{}", display_tree);

            // Show statistics if available
            if let Some(stats) = file_stats {
                println!("\n--- Statistics ---");
                if verbose {
                    println!("{:#?}", stats);
                } else {
                    println!("{:?}", stats);
                }
            } else {
                println!("\n--- Statistics ---");
                println!("No statistics available");
            }
        }
    }

    Ok(())
}

/// Helper function to get children arrays
fn get_array_children(array: &ArrayRef) -> Vec<ArrayRef> {
    array.children()
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

/// Find columns that use a specific encoding (recursively search)
fn find_columns_with_encoding(
    array: &ArrayRef,
    target_encoding: &str,
    column_names: &vortex_dtype::FieldNames,
) -> Vec<String> {
    let mut result = Vec::new();

    // Check if the entire array tree contains the target encoding
    if contains_encoding(array, target_encoding) {
        // If we find the encoding anywhere in the tree, report all columns
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

/// Recursively analyzes and displays the encoding tree with column names
fn analyze_encoding_tree_with_names(
    array: &ArrayRef,
    depth: usize,
    column_names: &vortex_dtype::FieldNames,
    show_first_struct_details: bool,
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
            if show_first_struct_details {
                // Display each field with its column name (first struct only)
                for (idx, (name, child)) in column_names.iter().zip(children.iter()).enumerate() {
                    let prefix = if idx == column_names.len() - 1 {
                        "└─"
                    } else {
                        "├─"
                    };
                    println!("{}  {} Column [{}]:", indent, prefix, name);
                    analyze_encoding_tree(child, depth + 2);
                }
            } else {
                // For subsequent structs, just show a summary
                println!("{}  [Same structure: {} columns]", indent, children.len());
            }
            return;
        }
    }

    // Not a struct or column count doesn't match, recurse into children
    let children = get_array_children(array);

    if !children.is_empty() {
        // Track if we've already shown details for a struct
        let mut shown_first_struct = !show_first_struct_details;

        for child in children {
            // Only show details for the first struct we encounter
            let show_details = !shown_first_struct;
            if child.encoding_id().as_ref() == "vortex.struct" && show_details {
                shown_first_struct = true;
            }

            // Recursively call with names in case we find struct deeper in the tree
            analyze_encoding_tree_with_names(&child, depth + 1, column_names, show_details);
        }
    }
}

async fn show_encoding(path: &Path, format: OutputFormat, verbose: bool) -> Result<()> {
    let session = VortexSession::default();

    // Open the file
    let reader = session.open_options().open(path.to_path_buf()).await?;

    // Read the array
    let array = reader.scan()?.into_array_stream()?.read_all().await?;

    match format {
        OutputFormat::Json => {
            let mut encoding_info = serde_json::json!({
                "file": path.display().to_string(),
                "row_count": reader.row_count(),
                "root_encoding": array.encoding_id().as_ref(),
            });

            // Try to extract column information
            if let Some(struct_fields) = reader.dtype().as_struct_fields_opt() {
                let column_encodings = extract_column_encodings_from_tree(&array);

                if !column_encodings.is_empty() {
                    let mut columns = Vec::new();
                    for (name, child) in struct_fields.names().iter().zip(column_encodings.iter()) {
                        columns.push(serde_json::json!({
                            "name": name,
                            "encoding": child.encoding_id().as_ref(),
                            "bytes": child.nbytes(),
                        }));
                    }
                    encoding_info["columns"] = serde_json::Value::Array(columns);
                }

                // Add compression summary
                let zstd_columns = find_columns_with_encoding(&array, "vortex.zstd", struct_fields.names());
                encoding_info["zstd_compressed_columns"] = serde_json::Value::Array(
                    zstd_columns.into_iter().map(serde_json::Value::String).collect()
                );
            }

            println!("{}", serde_json::to_string_pretty(&encoding_info)?);
        }
        OutputFormat::Text => {
            println!("=== Vortex File Encoding Inspection ===");
            println!("File: {}", path.display());
            println!();

            println!("File Information:");
            println!("  Rows: {}", reader.row_count());
            println!("  DType: {}", reader.dtype());
            println!();

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

                // Show detailed encoding tree
                if verbose {
                    println!("\n--- Detailed Encoding Tree ---");
                    analyze_encoding_tree_with_names(&array, 0, struct_fields.names(), true);
                }
            } else {
                // Not a struct type, show the root array tree
                println!("Array Encodings:\n");
                analyze_encoding_tree(&array, 0);
            }
        }
    }

    Ok(())
}
