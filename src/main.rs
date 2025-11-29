use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use vortex::VortexSessionDefault;
use vortex_file::{OpenOptionsSessionExt, VortexFile, register_default_encodings};
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
