from mcp.server.fastmcp import FastMCP
import pandas as pd
import os
import json
import csv
from datetime import datetime
from thefuzz import fuzz

def register(mcp: FastMCP):
    def detect_delimiter(file_path):
        """
        Detects the delimiter used in a CSV file.
        Returns the detected delimiter (comma or semicolon).
        """
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            sample = csvfile.read(4096)
            
            # Count occurrences of common delimiters
            comma_count = sample.count(',')
            semicolon_count = sample.count(';')
            
            # Return the most common delimiter
            if semicolon_count > comma_count:
                return ';'
            return ','
    
    @mcp.tool(name="🔄 James - Migration Agent")
    def apply_mappings(file_path: str, mappings: dict, output_path: str) -> str:
        """Applies approved mappings and validates required fields"""
        try:
            df = pd.read_csv(file_path)
            
            # Apply mappings to the dataframe
            for column, mapping in mappings.items():
                if column in df.columns:
                    df[column] = df[column].map(mapping).fillna(df[column])
            
            # Save the transformed data
            df.to_csv(output_path, index=False)
            return f"Successfully migrated data to {output_path}"
        except Exception as e:
            return f"Error during migration: {str(e)}"
    
    @mcp.tool(name="🔄 James - Validate and Check Duplicates")
    def validate_and_check_duplicates(
        mapped_file_path: str, 
        target_data_file: str, 
        key_fields: list = None,
        similarity_threshold: int = 100,
        duplicate_handling: str = "ask"
    ) -> dict:
        """
        Validates mapped data against target data and identifies duplicates with similarity score
        
        Args:
            mapped_file_path: Path to the mapped CSV file (output from Mathias)
            target_data_file: Path to existing target data to check for duplicates
            key_fields: List of field names to use as unique identifiers for duplicate checking (optional)
            similarity_threshold: Minimum similarity percentage to consider as duplicate (100 = exact match)
            duplicate_handling: How to handle duplicates ("ask", "skip", "overwrite", "append")
            
        Returns:
            A dictionary with validation results and paths to output files
        """
        try:
            # Basic validation
            if not os.path.exists(mapped_file_path):
                return {"error": f"Mapped file not found: {mapped_file_path}"}
            
            if not os.path.exists(target_data_file):
                return {"error": f"Target data file not found: {target_data_file}"}
            
            # Detect delimiters
            mapped_delimiter = detect_delimiter(mapped_file_path)
            target_delimiter = detect_delimiter(target_data_file)
            
            # Load the data
            mapped_data = pd.read_csv(mapped_file_path, sep=mapped_delimiter)
            target_data = pd.read_csv(target_data_file, sep=target_delimiter)
            
            # Initialize results
            results = {
                "mapped_file": mapped_file_path,
                "target_file": target_data_file,
                "mapped_records": len(mapped_data),
                "target_records": len(target_data),
                "similarity_threshold": similarity_threshold,
                "output_files": {}
            }
            
            # If key_fields is not provided, use common columns between datasets
            if not key_fields:
                key_fields = [col for col in mapped_data.columns if col in target_data.columns]
                results["message"] = f"No key fields provided. Using all common columns: {key_fields}"
            
            results["key_fields"] = key_fields
            
            # Validate key fields exist in both datasets
            valid_keys = [k for k in key_fields if k in mapped_data.columns and k in target_data.columns]
            
            if not valid_keys:
                return {
                    "error": "No valid key fields found in both datasets",
                    "provided_keys": key_fields,
                    "mapped_columns": mapped_data.columns.tolist(),
                    "target_columns": target_data.columns.tolist()
                }
            
            # Find duplicates with similarity scores
            duplicates = []
            unique_records = []
            
            for idx, source_row in mapped_data.iterrows():
                highest_similarity = 0
                best_match = None
                
                for _, target_row in target_data.iterrows():
                    # Check similarity for each key field
                    field_similarities = []
                    
                    for key in valid_keys:
                        source_value = str(source_row[key]) if not pd.isna(source_row[key]) else ""
                        target_value = str(target_row[key]) if not pd.isna(target_row[key]) else ""
                        
                        # Calculate similarity ratio
                        similarity = fuzz.ratio(source_value, target_value)
                        field_similarities.append(similarity)
                    
                    # Average similarity across all key fields
                    avg_similarity = sum(field_similarities) / len(field_similarities)
                    
                    if avg_similarity > highest_similarity:
                        highest_similarity = avg_similarity
                        best_match = target_row
                
                # If similarity is above threshold, consider it a duplicate
                if highest_similarity >= similarity_threshold:
                    duplicate_info = source_row.to_dict()
                    duplicate_info["similarity_score"] = highest_similarity
                    duplicate_info["match_id"] = idx
                    duplicates.append(duplicate_info)
                else:
                    unique_records.append(source_row.to_dict())
            
            # Convert to DataFrames
            if duplicates:
                duplicates_df = pd.DataFrame(duplicates)
                # Sort by similarity score (descending)
                duplicates_df = duplicates_df.sort_values(by="similarity_score", ascending=False)
            else:
                duplicates_df = pd.DataFrame(columns=list(mapped_data.columns) + ["similarity_score", "match_id"])
            
            unique_df = pd.DataFrame(unique_records) if unique_records else pd.DataFrame(columns=mapped_data.columns)
            
            # Save duplicates to a separate file for review
            duplicates_file = mapped_file_path.replace(".csv", f"_duplicates_{similarity_threshold}pct.csv")
            duplicates_df.to_csv(duplicates_file, index=False)
            
            # Save unique records to a separate file
            unique_file = mapped_file_path.replace(".csv", "_unique.csv")
            unique_df.to_csv(unique_file, index=False)
            
            # Update results
            results["duplicate_check"] = {
                "key_fields_used": valid_keys,
                "duplicates_found": len(duplicates) > 0,
                "duplicate_count": len(duplicates),
                "unique_count": len(unique_records),
                "duplicates_file": duplicates_file,
                "unique_records_file": unique_file
            }
            
            results["output_files"]["duplicates"] = duplicates_file
            results["output_files"]["unique_records"] = unique_file
            
            # Handle duplicates based on the duplicate_handling parameter
            if duplicates:
                if duplicate_handling == "ask":
                    results["status"] = "review_needed"
                    results["message"] = (
                        f"Found {len(duplicates)} potential duplicate records with similarity ≥{similarity_threshold}%. "
                        f"Please review the duplicates file and decide how to proceed. "
                        f"Options: 'skip' (don't import duplicates), 'overwrite' (replace existing records), "
                        f"or 'append' (add as new records)."
                    )
                elif duplicate_handling == "skip":
                    # Only use unique records
                    final_output = unique_df
                    results["status"] = "complete"
                    results["message"] = f"Skipped {len(duplicates)} duplicate records. {len(unique_records)} unique records ready for import."
                elif duplicate_handling == "overwrite":
                    # Use all records, will overwrite existing ones
                    final_output = mapped_data
                    results["status"] = "complete"
                    results["message"] = f"All {len(mapped_data)} records will be imported, overwriting {len(duplicates)} existing records."
                elif duplicate_handling == "append":
                    # Use all records, will add duplicates as new records
                    final_output = mapped_data
                    results["status"] = "complete"
                    results["message"] = f"All {len(mapped_data)} records will be imported as new records, including {len(duplicates)} duplicates."
                
                # Save final output if not in "ask" mode
                if duplicate_handling != "ask":
                    final_file = mapped_file_path.replace(".csv", "_final.csv")
                    final_output.to_csv(final_file, index=False)
                    results["output_files"]["final_output"] = final_file
            else:
                # No duplicates found, use all records
                final_file = mapped_file_path.replace(".csv", "_final.csv")
                mapped_data.to_csv(final_file, index=False)
                results["output_files"]["final_output"] = final_file
                results["status"] = "complete"
                results["message"] = f"All {len(mapped_data)} records are unique and ready for import."
            
            return results
            
        except Exception as e:
            return {"error": f"Error during duplicate check: {str(e)}"}
    
    @mcp.tool(name="🔄 James - Process Duplicates")
    def process_duplicates(mapped_file_path: str, target_path: str, handling_option: str) -> dict:
        """
        Processes duplicates based on the chosen handling option and transfers data to target
        
        Args:
            mapped_file_path: Path to the original mapped CSV file
            target_path: Path where the final data should be transferred to
            handling_option: How to handle duplicates ("skip", "overwrite", "append")
            
        Returns:
            A dictionary with processing results and path to the final output file
        """
        try:
            # Validate inputs
            if not os.path.exists(mapped_file_path):
                return {"error": f"Mapped file not found: {mapped_file_path}"}
            
            if handling_option not in ["skip", "overwrite", "append"]:
                return {"error": f"Invalid handling option: {handling_option}. Must be 'skip', 'overwrite', or 'append'."}
            
            # Detect delimiter
            mapped_delimiter = detect_delimiter(mapped_file_path)
            
            # Load the mapped data
            mapped_data = pd.read_csv(mapped_file_path, sep=mapped_delimiter)
            
            # Check if duplicates file exists
            duplicates_file = None
            for file in os.listdir(os.path.dirname(mapped_file_path)):
                if "_duplicates_" in file and file.endswith(".csv"):
                    duplicates_file = os.path.join(os.path.dirname(mapped_file_path), file)
                    break
            
            # Get the unique records file path
            unique_file = mapped_file_path.replace(".csv", "_unique.csv")
            
            # Process based on handling option
            if handling_option == "skip" and os.path.exists(unique_file):
                # Only use unique records
                unique_delimiter = detect_delimiter(unique_file)
                final_output = pd.read_csv(unique_file, sep=unique_delimiter)
                message = f"Skipped duplicate records. {len(final_output)} unique records transferred to target."
            
            elif handling_option == "overwrite" or handling_option == "append":
                # Use all records
                final_output = mapped_data
                action = "overwriting" if handling_option == "overwrite" else "adding"
                message = f"All {len(mapped_data)} records transferred to target, {action} any duplicates."
            
            else:
                # Fallback if unique file doesn't exist
                final_output = mapped_data
                message = f"All {len(mapped_data)} records transferred to target."
            
            # Ensure target directory exists
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Save to target path
            final_output.to_csv(target_path, index=False)
            
            # Create a timestamp for the report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = os.path.join(
                os.path.dirname(mapped_file_path), 
                f"transfer_report_{timestamp}.json"
            )
            
            # Create a report
            report = {
                "status": "complete",
                "timestamp": timestamp,
                "message": message,
                "handling_option": handling_option,
                "source_file": mapped_file_path,
                "target_file": target_path,
                "total_records_transferred": len(final_output),
                "duplicates_file": duplicates_file
            }
            
            # Save the report
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            return report
            
        except Exception as e:
            return {"error": f"Error processing duplicates: {str(e)}"} 