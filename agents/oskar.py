from mcp.server.fastmcp import FastMCP
import pandas as pd
import numpy as np
from thefuzz import fuzz, process
import json

def register(mcp: FastMCP):
    @mcp.tool(name="🧹 Oskar - Cleaning Agent")
    def clean_data(file_path: str, cleaning_tasks: list = None, auto_apply: bool = False, 
                  review_threshold: int = 85, emma_results: dict = None) -> dict:
        """
        Cleans data by removing duplicates, standardizing formats, and handling missing values
        
        Args:
            file_path: Path to the CSV file
            cleaning_tasks: List of cleaning tasks to perform
            auto_apply: Whether to automatically apply all cleaning operations
            review_threshold: Similarity threshold below which human review is required
            emma_results: Optional results from Emma's profiling
            
        Returns:
            A dictionary with cleaning results and recommendations
        """
        try:
            # Load the data
            df = pd.read_csv(file_path)
            original_rows = len(df)
            
            # Initialize results
            results = {
                "file_info": {
                    "path": file_path,
                    "original_rows": original_rows,
                    "original_columns": len(df.columns)
                },
                "cleaning_summary": {
                    "operations_performed": [],
                    "rows_affected": 0,
                    "auto_merged_groups": 0,
                    "review_required_groups": 0,
                    "human_escalation_groups": 0
                },
                "cleaning_details": {},
                "recommendations": [],
                "output_file": None
            }
            
            # If Emma results are provided, extract cleaning tasks
            if emma_results and not cleaning_tasks:
                if "oskar_instructions" in emma_results and "cleaning_tasks" in emma_results["oskar_instructions"]:
                    cleaning_tasks = emma_results["oskar_instructions"]["cleaning_tasks"]
            
            # If no cleaning tasks specified, analyze the data to determine what needs cleaning
            if not cleaning_tasks:
                cleaning_tasks = analyze_data_for_cleaning(df)
            
            # Process each cleaning task
            for task in cleaning_tasks:
                task_type = task.get("task_type", "")
                
                # Handle exact duplicates
                if task_type == "remove_exact_duplicates":
                    columns = task.get("columns", df.columns.tolist())
                    duplicate_results = remove_exact_duplicates(df, columns)
                    df = duplicate_results["cleaned_df"]
                    
                    results["cleaning_details"]["exact_duplicates"] = {
                        "columns_used": columns,
                        "duplicates_removed": duplicate_results["duplicates_removed"],
                        "groups_found": duplicate_results["groups_found"]
                    }
                    
                    results["cleaning_summary"]["operations_performed"].append("Removed exact duplicates")
                    results["cleaning_summary"]["rows_affected"] += duplicate_results["duplicates_removed"]
                    results["cleaning_summary"]["auto_merged_groups"] += duplicate_results["groups_found"]
                
                # Handle fuzzy duplicates
                elif task_type == "review_fuzzy_duplicates":
                    columns = task.get("columns", [col for col in df.columns if pd.api.types.is_string_dtype(df[col])])
                    similarity_threshold = task.get("similarity_threshold", 90)
                    
                    fuzzy_results = handle_fuzzy_duplicates(
                        df, columns, similarity_threshold, 
                        auto_apply=auto_apply, review_threshold=review_threshold
                    )
                    
                    df = fuzzy_results["cleaned_df"]
                    
                    results["cleaning_details"]["fuzzy_duplicates"] = {
                        "columns_used": columns,
                        "similarity_threshold": similarity_threshold,
                        "auto_merged_groups": fuzzy_results["auto_merged_groups"],
                        "review_required_groups": fuzzy_results["review_required_groups"],
                        "human_escalation_groups": fuzzy_results["human_escalation_groups"],
                        "groups_for_review": fuzzy_results["groups_for_review"]
                    }
                    
                    results["cleaning_summary"]["operations_performed"].append("Processed fuzzy duplicates")
                    results["cleaning_summary"]["auto_merged_groups"] += fuzzy_results["auto_merged_groups"]
                    results["cleaning_summary"]["review_required_groups"] += fuzzy_results["review_required_groups"]
                    results["cleaning_summary"]["human_escalation_groups"] += fuzzy_results["human_escalation_groups"]
                
                # Handle missing values
                elif task_type == "handle_missing_values":
                    column = task.get("column")
                    strategy = task.get("strategy", "auto")
                    
                    if column:
                        missing_results = handle_missing_values(df, column, strategy)
                        df = missing_results["cleaned_df"]
                        
                        if "missing_values" not in results["cleaning_details"]:
                            results["cleaning_details"]["missing_values"] = {}
                        
                        results["cleaning_details"]["missing_values"][column] = {
                            "strategy_used": missing_results["strategy_used"],
                            "values_filled": missing_results["values_filled"]
                        }
                        
                        results["cleaning_summary"]["operations_performed"].append(f"Handled missing values in {column}")
                        results["cleaning_summary"]["rows_affected"] += missing_results["values_filled"]
                
                # Handle inconsistent capitalization
                elif task_type == "standardize_capitalization":
                    column = task.get("column")
                    
                    if column:
                        cap_results = standardize_capitalization(df, column)
                        df = cap_results["cleaned_df"]
                        
                        if "capitalization" not in results["cleaning_details"]:
                            results["cleaning_details"]["capitalization"] = {}
                        
                        results["cleaning_details"]["capitalization"][column] = {
                            "values_standardized": cap_results["values_standardized"]
                        }
                        
                        results["cleaning_summary"]["operations_performed"].append(f"Standardized capitalization in {column}")
                        results["cleaning_summary"]["rows_affected"] += cap_results["values_standardized"]
            
            # Save the cleaned data
            output_file = file_path.replace(".csv", "_cleaned.csv")
            df.to_csv(output_file, index=False)
            results["output_file"] = output_file
            
            # Generate recommendations
            results["recommendations"] = generate_cleaning_recommendations(results)
            
            # Add final stats
            results["file_info"]["cleaned_rows"] = len(df)
            results["file_info"]["rows_removed"] = original_rows - len(df)
            results["file_info"]["cleaning_percentage"] = f"{(results['file_info']['rows_removed'] / original_rows * 100):.2f}%"
            
            return results
            
        except Exception as e:
            return {"error": f"Error during data cleaning: {str(e)}"}

# Helper function to analyze data and determine cleaning tasks
def analyze_data_for_cleaning(df):
    """
    Analyzes a dataframe to determine what cleaning tasks are needed
    
    Args:
        df: Pandas DataFrame to analyze
        
    Returns:
        List of cleaning tasks
    """
    cleaning_tasks = []
    
    # Check for exact duplicates across all columns
    if df.duplicated().any():
        cleaning_tasks.append({
            "task_type": "remove_exact_duplicates",
            "columns": df.columns.tolist(),
            "duplicate_count": int(df.duplicated().sum())
        })
    
    # Check for missing values
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            cleaning_tasks.append({
                "task_type": "handle_missing_values",
                "column": col,
                "missing_count": int(null_count),
                "data_type": str(df[col].dtype)
            })
    
    # Check for inconsistent capitalization in string columns
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            if df[col].nunique() > 0:
                lowercase_nunique = df[col].str.lower().nunique()
                if lowercase_nunique < df[col].nunique():
                    cleaning_tasks.append({
                        "task_type": "standardize_capitalization",
                        "column": col,
                        "inconsistent_count": int(df[col].nunique() - lowercase_nunique)
                    })
    
    # Check for potential fuzzy duplicates in string columns
    string_columns = [col for col in df.columns if pd.api.types.is_string_dtype(df[col])]
    if string_columns:
        cleaning_tasks.append({
            "task_type": "review_fuzzy_duplicates",
            "columns": string_columns,
            "similarity_threshold": 90
        })
    
    return cleaning_tasks

# Helper function to remove exact duplicates
def remove_exact_duplicates(df, columns):
    """
    Removes exact duplicates based on specified columns
    
    Args:
        df: Pandas DataFrame
        columns: Columns to use for duplicate detection
        
    Returns:
        Dictionary with cleaned dataframe and stats
    """
    # Create a copy of the dataframe
    cleaned_df = df.copy()
    
    # Count duplicates before removal
    duplicated_mask = cleaned_df.duplicated(subset=columns, keep=False)
    duplicate_count = duplicated_mask.sum()
    
    if duplicate_count == 0:
        return {
            "cleaned_df": cleaned_df,
            "duplicates_removed": 0,
            "groups_found": 0
        }
    
    # Count duplicate groups
    duplicate_groups = cleaned_df[duplicated_mask].groupby(columns).size()
    group_count = len(duplicate_groups)
    
    # Remove duplicates (keep first occurrence)
    cleaned_df = cleaned_df.drop_duplicates(subset=columns, keep='first')
    
    # Calculate how many rows were removed
    rows_removed = len(df) - len(cleaned_df)
    
    return {
        "cleaned_df": cleaned_df,
        "duplicates_removed": int(rows_removed),
        "groups_found": int(group_count)
    }

# Helper function to handle fuzzy duplicates
def handle_fuzzy_duplicates(df, columns, similarity_threshold, auto_apply=False, review_threshold=85):
    """
    Identifies and handles fuzzy duplicates
    
    Args:
        df: Pandas DataFrame
        columns: Columns to use for fuzzy matching
        similarity_threshold: Threshold for similarity matching (0-100)
        auto_apply: Whether to automatically merge duplicates
        review_threshold: Threshold below which human review is required
        
    Returns:
        Dictionary with cleaned dataframe and stats
    """
    # Create a copy of the dataframe
    cleaned_df = df.copy()
    
    # Create a combined string for each row using the selected columns
    cleaned_df['_combined_for_fuzzy'] = cleaned_df[columns].apply(
        lambda row: ' '.join(str(val) for val in row if pd.notna(val)), axis=1
    )
    
    # Find similar rows
    similar_groups = []
    processed_indices = set()
    
    for i in range(len(cleaned_df)):
        if i in processed_indices:
            continue
            
        row_text = cleaned_df['_combined_for_fuzzy'].iloc[i]
        
        # Skip empty strings
        if pd.isna(row_text) or not row_text or row_text.isspace():
            continue
        
        # Find similar rows
        group = [i]
        
        for j in range(i+1, len(cleaned_df)):
            if j in processed_indices:
                continue
                
            other_text = cleaned_df['_combined_for_fuzzy'].iloc[j]
            
            # Skip empty strings
            if pd.isna(other_text) or not other_text or other_text.isspace():
                continue
                
            # Calculate similarity
            similarity = fuzz.ratio(row_text, other_text)
            
            # If similarity is above threshold, add to group
            if similarity >= similarity_threshold:
                group.append(j)
                processed_indices.add(j)
        
        # If we found similar rows, add the group
        if len(group) > 1:
            # Calculate group similarity
            group_similarity = calculate_group_similarity(cleaned_df, group, '_combined_for_fuzzy')
            
            similar_groups.append({
                "indices": group,
                "similarity": group_similarity,
                "records": cleaned_df.iloc[group][columns].to_dict(orient='records')
            })
    
    # Drop the temporary column
    cleaned_df = cleaned_df.drop('_combined_for_fuzzy', axis=1)
    
    # Process the groups
    auto_merged_groups = 0
    review_required_groups = 0
    human_escalation_groups = 0
    groups_for_review = []
    
    # Track indices to remove
    indices_to_remove = set()
    
    for group in similar_groups:
        group_indices = group["indices"]
        group_similarity = group["similarity"]
        
        if group_similarity >= review_threshold and auto_apply:
            # Auto-merge: Keep the first record, remove others
            keep_idx = group_indices[0]
            remove_indices = group_indices[1:]
            
            # Add to indices to remove
            indices_to_remove.update(remove_indices)
            
            auto_merged_groups += 1
        elif group_similarity >= review_threshold:
            # Suggest merge but don't apply
            review_required_groups += 1
            groups_for_review.append(group)
        else:
            # Low confidence, require human review
            human_escalation_groups += 1
            groups_for_review.append(group)
    
    # Remove the duplicate records
    if indices_to_remove:
        cleaned_df = cleaned_df.drop(index=list(indices_to_remove))
    
    return {
        "cleaned_df": cleaned_df,
        "auto_merged_groups": auto_merged_groups,
        "review_required_groups": review_required_groups,
        "human_escalation_groups": human_escalation_groups,
        "groups_for_review": groups_for_review
    }

# Helper function to calculate similarity within a group
def calculate_group_similarity(df, indices, text_column):
    """
    Calculates the average similarity within a group of records
    
    Args:
        df: Pandas DataFrame
        indices: List of indices in the group
        text_column: Column containing the text to compare
        
    Returns:
        Average similarity score (0-100)
    """
    if len(indices) <= 1:
        return 100
    
    # Calculate pairwise similarities
    similarities = []
    
    for i in range(len(indices)):
        for j in range(i+1, len(indices)):
            idx1 = indices[i]
            idx2 = indices[j]
            
            text1 = df[text_column].iloc[idx1]
            text2 = df[text_column].iloc[idx2]
            
            if pd.isna(text1) or pd.isna(text2):
                continue
                
            similarity = fuzz.ratio(text1, text2)
            similarities.append(similarity)
    
    # Return average similarity
    if similarities:
        return sum(similarities) / len(similarities)
    else:
        return 0

# Helper function to handle missing values
def handle_missing_values(df, column, strategy='auto'):
    """
    Handles missing values in a column
    
    Args:
        df: Pandas DataFrame
        column: Column to process
        strategy: Strategy to use ('auto', 'remove', 'mean', 'median', 'mode', 'zero')
        
    Returns:
        Dictionary with cleaned dataframe and stats
    """
    # Count missing values
    missing_count = df[column].isnull().sum()
    
    if missing_count == 0:
        return {
            "cleaned_df": df,
            "strategy_used": "none",
            "values_filled": 0
        }
    
    # Create a copy of the dataframe
    cleaned_df = df.copy()
    
    # Determine the best strategy if 'auto' is specified
    if strategy == 'auto':
        if pd.api.types.is_numeric_dtype(df[column]):
            # For numeric columns, use median
            strategy = 'median'
        elif pd.api.types.is_datetime64_dtype(df[column]):
            # For date columns, use median
            strategy = 'median'
        else:
            # For other columns, use mode
            strategy = 'mode'
    
    # Apply the strategy
    if strategy == 'remove':
        # Remove rows with missing values
        cleaned_df = cleaned_df.dropna(subset=[column])
        values_filled = missing_count
    elif strategy == 'mean' and pd.api.types.is_numeric_dtype(df[column]):
        # Fill with mean
        mean_value = df[column].mean()
        cleaned_df[column] = cleaned_df[column].fillna(mean_value)
        values_filled = missing_count
    elif strategy == 'median' and pd.api.types.is_numeric_dtype(df[column]):
        # Fill with median
        median_value = df[column].median()
        cleaned_df[column] = cleaned_df[column].fillna(median_value)
        values_filled = missing_count
    elif strategy == 'mode':
        # Fill with mode
        mode_value = df[column].mode()[0] if not df[column].mode().empty else None
        if mode_value is not None:
            cleaned_df[column] = cleaned_df[column].fillna(mode_value)
            values_filled = missing_count
        else:
            values_filled = 0
    elif strategy == 'zero' and pd.api.types.is_numeric_dtype(df[column]):
        # Fill with zero
        cleaned_df[column] = cleaned_df[column].fillna(0)
        values_filled = missing_count
    else:
        # Default: do nothing
        values_filled = 0
    
    return {
        "cleaned_df": cleaned_df,
        "strategy_used": strategy,
        "values_filled": int(values_filled)
    }

# Helper function to standardize capitalization
def standardize_capitalization(df, column):
    """
    Standardizes capitalization in a text column
    
    Args:
        df: Pandas DataFrame
        column: Column to process
        
    Returns:
        Dictionary with cleaned dataframe and stats
    """
    if not pd.api.types.is_string_dtype(df[column]):
        return {
            "cleaned_df": df,
            "values_standardized": 0
        }
    
    # Create a copy of the dataframe
    cleaned_df = df.copy()
    
    # Count unique values before standardization
    unique_before = df[column].nunique()
    
    # Determine the most common capitalization for each value
    value_counts = df[column].str.lower().value_counts()
    
    # Create a mapping of lowercase to most common capitalization
    capitalization_map = {}
    
    for lowercase_val, count in value_counts.items():
        if pd.isna(lowercase_val):
            continue
            
        # Find all occurrences of this value (case-insensitive)
        matches = df[df[column].str.lower() == lowercase_val][column]
        
        # Get the most common capitalization
        if not matches.empty:
            most_common = matches.value_counts().index[0]
            capitalization_map[lowercase_val] = most_common
    
    # Apply the mapping
    for i, val in enumerate(cleaned_df[column]):
        if pd.isna(val):
            continue
            
        lowercase_val = val.lower()
        if lowercase_val in capitalization_map and val != capitalization_map[lowercase_val]:
            cleaned_df.at[i, column] = capitalization_map[lowercase_val]
    
    # Count unique values after standardization
    unique_after = cleaned_df[column].nunique()
    values_standardized = unique_before - unique_after
    
    return {
        "cleaned_df": cleaned_df,
        "values_standardized": int(values_standardized)
    }

# Helper function to generate cleaning recommendations
def generate_cleaning_recommendations(results):
    """
    Generates recommendations based on cleaning results
    
    Args:
        results: Cleaning results dictionary
        
    Returns:
        List of recommendations
    """
    recommendations = []
    
    # Check if any operations were performed
    if not results["cleaning_summary"]["operations_performed"]:
        recommendations.append("No cleaning operations were performed. The data appears to be clean.")
        return recommendations
    
    # Add recommendations based on operations performed
    if "Removed exact duplicates" in results["cleaning_summary"]["operations_performed"]:
        exact_dupes = results["cleaning_details"].get("exact_duplicates", {})
        if exact_dupes.get("duplicates_removed", 0) > 0:
            recommendations.append(
                f"Successfully removed {exact_dupes.get('duplicates_removed')} exact duplicates from {exact_dupes.get('groups_found')} groups."
            )
    
    if "Processed fuzzy duplicates" in results["cleaning_summary"]["operations_performed"]:
        fuzzy_dupes = results["cleaning_details"].get("fuzzy_duplicates", {})
        auto_merged = fuzzy_dupes.get("auto_merged_groups", 0)
        review_required = fuzzy_dupes.get("review_required_groups", 0)
        human_escalation = fuzzy_dupes.get("human_escalation_groups", 0)
        
        if auto_merged > 0:
            recommendations.append(f"Auto-merged {auto_merged} groups of similar records.")
        
        if review_required > 0:
            recommendations.append(f"Found {review_required} groups of similar records that require review.")
        
        if human_escalation > 0:
            recommendations.append(f"Escalated {human_escalation} groups of records to human review due to low confidence.")
    
    # Add recommendations for missing values
    if "missing_values" in results["cleaning_details"]:
        missing_vals = results["cleaning_details"]["missing_values"]
        for col, details in missing_vals.items():
            if details["values_filled"] > 0:
                recommendations.append(
                    f"Filled {details['values_filled']} missing values in column '{col}' using {details['strategy_used']} strategy."
                )
    
    # Add recommendations for capitalization
    if "capitalization" in results["cleaning_details"]:
        cap_details = results["cleaning_details"]["capitalization"]
        for col, details in cap_details.items():
            if details["values_standardized"] > 0:
                recommendations.append(
                    f"Standardized capitalization for {details['values_standardized']} values in column '{col}'."
                )
    
    # Add overall recommendation
    if results["file_info"].get("rows_removed", 0) > 0:
        percentage = results["file_info"].get("cleaning_percentage", "0%")
        recommendations.append(
            f"Overall, removed {results['file_info']['rows_removed']} rows ({percentage}) from the dataset."
        )
    
    return recommendations 