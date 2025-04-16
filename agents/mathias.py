from mcp.server.fastmcp import FastMCP
import pandas as pd
import json
import os
from thefuzz import fuzz
import re
import csv

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
    
    @mcp.tool(name="🔄 Mathias - Mapping Agent")
    def map_data(file_path: str, field_mapping_file: str = None, value_mapping_file: str = None, auto_suggest: bool = True) -> dict:
        """
        Maps data from source format to target format using provided or suggested mappings
        
        Args:
            file_path: Path to the source CSV file
            field_mapping_file: Optional path to a JSON file for field mappings
            value_mapping_file: Optional path to a CSV file for value mappings
            auto_suggest: Whether to automatically suggest mappings for unmapped fields
            
        Returns:
            A dictionary with mapping results and the path to the mapped file
        """
        try:
            # Basic validation
            if not os.path.exists(file_path):
                return {"error": f"File not found: {file_path}"}
            
            # Detect delimiter
            delimiter = detect_delimiter(file_path)
            
            # Load the source data (just headers for initial analysis)
            source_columns = pd.read_csv(file_path, nrows=0, sep=delimiter).columns.tolist()
            
            # Initialize results
            results = {
                "source_file": file_path,
                "source_columns": source_columns,
                "delimiter": delimiter,
                "mapping_summary": {},
                "field_mappings": {},
                "value_mappings": {},
                "output_file": None
            }
            
            # Load field mapping from file if provided
            field_mapping = {}
            if field_mapping_file and os.path.exists(field_mapping_file):
                # Check if it's a CSV or JSON file
                if field_mapping_file.lower().endswith('.csv'):
                    # Handle CSV mapping file
                    try:
                        # Detect delimiter for mapping file
                        mapping_delimiter = detect_delimiter(field_mapping_file)
                        mapping_df = pd.read_csv(field_mapping_file, sep=mapping_delimiter)
                        # Convert CSV to mapping dictionary
                        for _, row in mapping_df.iterrows():
                            if 'source_field' in mapping_df.columns and 'target_field' in mapping_df.columns:
                                source = row['source_field']
                                target = row['target_field']
                                field_mapping[source] = {"target_field": target}
                    except Exception as e:
                        return {"error": f"Error reading CSV field mapping file: {str(e)}"}
                else:
                    # Handle JSON mapping file
                    try:
                        with open(field_mapping_file, 'r') as f:
                            field_mapping = json.load(f)
                    except json.JSONDecodeError:
                        return {"error": f"Invalid JSON in field mapping file: {field_mapping_file}"}
                
                results["field_mapping_file"] = field_mapping_file
            
            # If no field mapping file or empty mapping, create a simple template
            if not field_mapping:
                field_mapping = {col: {"target_field": col} for col in source_columns}
                
                # Save the mapping template
                template_file = file_path.replace(".csv", "_field_mapping_template.json")
                with open(template_file, 'w') as f:
                    json.dump(field_mapping, f, indent=2)
                
                results["field_mapping_template"] = template_file
                results["status"] = "template_created"
                results["message"] = "Created field mapping template. Please edit and provide as field_mapping_file."
                return results
            
            # Load value mapping from file if provided
            value_mapping = {}
            if value_mapping_file and os.path.exists(value_mapping_file):
                try:
                    # Detect delimiter for value mapping file
                    value_mapping_delimiter = detect_delimiter(value_mapping_file)
                    # Read the value mapping file
                    value_df = pd.read_csv(value_mapping_file, sep=value_mapping_delimiter)
                    
                    # Check if the file has the expected columns
                    if len(value_df.columns) >= 3:
                        # Process each row in the value mapping file
                        for i, row in value_df.iterrows():
                            field = str(row.iloc[0])
                            old_value = row.iloc[1]
                            new_value = row.iloc[2]
                            
                            if field not in value_mapping:
                                value_mapping[field] = {}
                            
                            value_mapping[field][old_value] = new_value
                    else:
                        print(f"Warning: Value mapping file does not have enough columns. Expected at least 3, got {len(value_df.columns)}")
                except Exception as e:
                    print(f"Error reading value mapping file: {str(e)}")
                    # Continue without value mapping
                
                results["value_mapping_file"] = value_mapping_file
            
            # Apply the mapping - use chunks for large files
            chunk_size = 10000  # Process in chunks to avoid memory issues
            output_file = file_path.replace(".csv", "_mapped.csv")
            
            # Track mapping stats
            mapped_fields = 0
            unmapped_fields = 0
            total_rows = 0
            
            # Process the first chunk to set up the output file
            first_chunk = pd.read_csv(file_path, nrows=chunk_size, sep=delimiter)
            total_rows += len(first_chunk)
            
            # Create output dataframe for the first chunk
            output_df = pd.DataFrame()
            
            # Apply each field mapping to the first chunk
            for source_field, mapping_info in field_mapping.items():
                target_field = mapping_info.get("target_field", "")
                
                if target_field and source_field in first_chunk.columns:
                    # Copy the data to the output dataframe
                    output_df[target_field] = first_chunk[source_field].copy()
                    
                    # Apply value mapping if available for this field
                    if source_field in value_mapping:
                        # Replace values according to the mapping
                        output_df[target_field] = output_df[target_field].replace(value_mapping[source_field])
                    
                    mapped_fields += 1
                else:
                    unmapped_fields += 1
            
            # Write the first chunk to the output file
            output_df.to_csv(output_file, index=False, mode='w')
            
            # Process the rest of the file in chunks if it's large
            if os.path.getsize(file_path) > 10 * 1024 * 1024:  # If file is larger than 10MB
                for chunk in pd.read_csv(file_path, skiprows=range(1, chunk_size + 1), chunksize=chunk_size, sep=delimiter):
                    total_rows += len(chunk)
                    
                    # Create output dataframe for this chunk
                    chunk_output = pd.DataFrame()
                    
                    # Apply each field mapping to the chunk
                    for source_field, mapping_info in field_mapping.items():
                        target_field = mapping_info.get("target_field", "")
                        
                        if target_field and source_field in chunk.columns:
                            # Copy the data to the output dataframe
                            chunk_output[target_field] = chunk[source_field].copy()
                            
                            # Apply value mapping if available for this field
                            if source_field in value_mapping:
                                # Replace values according to the mapping
                                chunk_output[target_field] = chunk_output[target_field].replace(value_mapping[source_field])
                    
                    # Append this chunk to the output file
                    chunk_output.to_csv(output_file, index=False, mode='a', header=False)
            
            # Update results
            results["output_file"] = output_file
            results["mapping_summary"] = {
                "mapped_fields": mapped_fields,
                "unmapped_fields": unmapped_fields,
                "total_fields": len(source_columns),
                "total_rows_processed": total_rows,
                "value_mappings_applied": len(value_mapping)
            }
            results["field_mappings"] = field_mapping
            results["value_mappings"] = value_mapping
            results["status"] = "complete"
            
            return results
            
        except Exception as e:
            return {"error": f"Error during data mapping: {str(e)}"}
    
    @mcp.tool(name="📋 Mathias - Create Value Mapping")
    def create_value_mapping(file_path: str, field_name: str, sample_size: int = 100) -> dict:
        """
        Creates a value mapping template for a specific field
        
        Args:
            file_path: Path to the source CSV file
            field_name: Name of the field to create value mapping for
            sample_size: Number of unique values to sample
            
        Returns:
            A dictionary with the value mapping template and path to the saved template file
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                return {"error": f"File not found: {file_path}"}
            
            # Detect delimiter
            delimiter = detect_delimiter(file_path)
            
            # Load the data
            df = pd.read_csv(file_path, sep=delimiter)
            
            # Check if field exists
            if field_name not in df.columns:
                return {"error": f"Field '{field_name}' not found in the file"}
            
            # Get unique values for the field
            unique_values = df[field_name].dropna().unique()
            
            # Limit to sample size
            if len(unique_values) > sample_size:
                unique_values = unique_values[:sample_size]
            
            # Create value mapping template
            value_mapping = []
            for value in unique_values:
                value_mapping.append({
                    "field": field_name,
                    "old_value": value,
                    "new_value": value  # Default to same value
                })
            
            # Convert to DataFrame
            mapping_df = pd.DataFrame(value_mapping)
            
            # Save to CSV
            template_file = f"{file_path.replace('.csv', '')}_{field_name}_value_mapping.csv"
            mapping_df.to_csv(template_file, index=False)
            
            return {
                "status": "success",
                "message": f"Created value mapping template for field '{field_name}'",
                "template_file": template_file,
                "unique_values_count": len(unique_values),
                "sample": value_mapping[:10]  # Show first 10 values
            }
            
        except Exception as e:
            return {"error": f"Error creating value mapping: {str(e)}"}

    @mcp.tool(name="📋 Mathias - Update Field Mapping")
    def update_field_mapping(file_path: str, mapping_updates: dict, existing_mapping_file: str = None) -> dict:
        """
        Updates an existing field mapping with new mappings
        
        Args:
            file_path: Path to the source CSV file
            mapping_updates: Dictionary of new mappings to add/update
            existing_mapping_file: Path to existing mapping file (optional)
            
        Returns:
            Updated mapping and path to saved mapping file
        """
        try:
            # Detect delimiter
            delimiter = detect_delimiter(file_path)
            
            # Load source columns
            source_columns = pd.read_csv(file_path, nrows=0, sep=delimiter).columns.tolist()
            
            # Load existing mapping if provided
            existing_mapping = {}
            if existing_mapping_file and os.path.exists(existing_mapping_file):
                with open(existing_mapping_file, 'r') as f:
                    existing_mapping = json.load(f)
            else:
                # Create empty mapping
                for col in source_columns:
                    existing_mapping[col] = {
                        "target_field": ""
                    }
            
            # Update with new mappings
            for source_field, target_field in mapping_updates.items():
                if source_field in existing_mapping:
                    existing_mapping[source_field]["target_field"] = target_field
            
            # Save updated mapping
            mapping_file = existing_mapping_file or file_path.replace(".csv", "_field_mapping.json")
            with open(mapping_file, 'w') as f:
                json.dump(existing_mapping, f, indent=2)
            
            return {
                "status": "success",
                "message": f"Field mapping updated with {len(mapping_updates)} new mappings",
                "mapping_file": mapping_file,
                "mapping": existing_mapping
            }
            
        except Exception as e:
            return {"error": f"Error updating field mapping: {str(e)}"}

    @mcp.tool(name="📋 Mathias - Update Value Mapping")
    def update_value_mapping(file_path: str, field_name: str, value_updates: dict, existing_mapping_file: str = None) -> dict:
        """
        Updates an existing value mapping with new value mappings
        
        Args:
            file_path: Path to the source CSV file
            field_name: Name of the field to update mappings for
            value_updates: Dictionary of old_value -> new_value mappings
            existing_mapping_file: Path to existing value mapping file (optional)
            
        Returns:
            Updated mapping and path to saved mapping file
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                return {"error": f"File not found: {file_path}"}
            
            # Load existing value mapping if provided
            existing_mappings = []
            if existing_mapping_file and os.path.exists(existing_mapping_file):
                try:
                    # Detect delimiter for mapping file
                    mapping_delimiter = detect_delimiter(existing_mapping_file)
                    mapping_df = pd.read_csv(existing_mapping_file, sep=mapping_delimiter)
                    for _, row in mapping_df.iterrows():
                        existing_mappings.append({
                            "field": row.iloc[0],
                            "old_value": row.iloc[1],
                            "new_value": row.iloc[2]
                        })
                except Exception as e:
                    return {"error": f"Error reading existing value mapping file: {str(e)}"}
            
            # Update existing mappings or add new ones
            updated = False
            for old_value, new_value in value_updates.items():
                found = False
                for mapping in existing_mappings:
                    if mapping["field"] == field_name and mapping["old_value"] == old_value:
                        mapping["new_value"] = new_value
                        found = True
                        updated = True
                        break
                
                if not found:
                    existing_mappings.append({
                        "field": field_name,
                        "old_value": old_value,
                        "new_value": new_value
                    })
                    updated = True
            
            # Save updated mapping
            mapping_file = existing_mapping_file or f"{file_path.replace('.csv', '')}_{field_name}_value_mapping.csv"
            mapping_df = pd.DataFrame(existing_mappings)
            mapping_df.to_csv(mapping_file, index=False)
            
            return {
                "status": "success",
                "message": f"Value mapping updated with {len(value_updates)} new mappings",
                "mapping_file": mapping_file,
                "updated": updated,
                "mapping_count": len(existing_mappings)
            }
            
        except Exception as e:
            return {"error": f"Error updating value mapping: {str(e)}"} 