import pandas as pd
import numpy as np
from mcp.server.fastmcp import FastMCP
from thefuzz import fuzz
import json
import os
from collections import defaultdict
import openai

def register(mcp: FastMCP):
    # Set up OpenAI API key from environment variable
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    if openai_api_key:
        openai.api_key = openai_api_key

    @mcp.tool(name="🔍 Emma - Profiling Agent")
    def profile_csv(file_path: str, focus_columns: list = None, analyze_duplicates: bool = True, 
                   similarity_threshold: int = 90, ai_analysis: bool = False) -> dict:
        """
        Detects field types, calculates data quality stats, and identifies potential duplicates
        
        Args:
            file_path: Path to the CSV file
            focus_columns: Optional list of columns to focus on (if None, all columns are analyzed)
            analyze_duplicates: Whether to analyze for potential duplicates
            similarity_threshold: Threshold for similarity matching (0-100)
            ai_analysis: Whether to use AI to provide deeper insights about the data
            
        Returns:
            A dictionary with comprehensive profiling results including potential duplicates and next steps for Oskar
        """
        try:
            # Load the data
            df = pd.read_csv(file_path)
            
            # Select columns to analyze
            if focus_columns:
                columns_to_analyze = [col for col in focus_columns if col in df.columns]
                if not columns_to_analyze:
                    return {"error": f"None of the specified columns {focus_columns} found in the data"}
            else:
                columns_to_analyze = df.columns.tolist()
            
            # Basic profiling
            profile_results = {
                "file_info": {
                    "path": file_path,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "memory_usage": f"{df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB"
                },
                "column_stats": {},
                "data_quality_summary": {},
                "duplicate_analysis": {},
                "next_steps": [],
                "oskar_instructions": {}  # Instructions for Oskar
            }
            
            # Analyze each column
            for col in columns_to_analyze:
                col_data = df[col]
                
                # Calculate basic stats
                stats = {
                    "type": str(col_data.dtype),
                    "count": int(col_data.count()),
                    "nulls": int(col_data.isnull().sum()),
                    "null_percentage": f"{(col_data.isnull().sum() / len(df) * 100):.2f}%",
                    "unique_values": int(col_data.nunique()),
                    "unique_percentage": f"{(col_data.nunique() / len(df) * 100):.2f}%",
                }
                
                # Add type-specific stats
                if pd.api.types.is_numeric_dtype(col_data):
                    stats.update({
                        "min": float(col_data.min()) if not pd.isna(col_data.min()) else None,
                        "max": float(col_data.max()) if not pd.isna(col_data.max()) else None,
                        "mean": float(col_data.mean()) if not pd.isna(col_data.mean()) else None,
                        "median": float(col_data.median()) if not pd.isna(col_data.median()) else None,
                        "std_dev": float(col_data.std()) if not pd.isna(col_data.std()) else None,
                    })
                    
                    # Detect outliers using IQR method
                    q1 = col_data.quantile(0.25)
                    q3 = col_data.quantile(0.75)
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    outliers = col_data[(col_data < lower_bound) | (col_data > upper_bound)]
                    
                    if len(outliers) > 0:
                        stats["outliers"] = {
                            "count": int(len(outliers)),
                            "percentage": f"{(len(outliers) / len(df) * 100):.2f}%",
                            "lower_bound": float(lower_bound),
                            "upper_bound": float(upper_bound)
                        }
                    
                elif pd.api.types.is_string_dtype(col_data):
                    # Get value length statistics for string columns
                    lengths = col_data.str.len().dropna()
                    stats.update({
                        "min_length": int(lengths.min()) if not lengths.empty else None,
                        "max_length": int(lengths.max()) if not lengths.empty else None,
                        "avg_length": float(lengths.mean()) if not lengths.empty else None,
                    })
                    
                    # Check for inconsistent capitalization
                    if col_data.nunique() > 0:
                        lowercase_nunique = col_data.str.lower().nunique()
                        if lowercase_nunique < col_data.nunique():
                            stats["inconsistent_capitalization"] = True
                            stats["potential_duplicates_due_to_case"] = int(col_data.nunique() - lowercase_nunique)
                
                # Add most common values (top 5)
                if col_data.nunique() < len(df):  # Only if there are duplicates
                    value_counts = col_data.value_counts().head(5).to_dict()
                    stats["top_values"] = {str(k): int(v) for k, v in value_counts.items()}
                
                profile_results["column_stats"][col] = stats
            
            # Identify potential key columns
            profile_results["key_columns"] = identify_key_columns(df)
            
            # Generate data quality summary
            profile_results["data_quality_summary"] = generate_data_quality_summary(df, profile_results["column_stats"])
            
            # Analyze for duplicates if requested
            if analyze_duplicates:
                # Identify the best columns for duplicate detection
                duplicate_strategies = suggest_duplicate_strategies(df, profile_results["key_columns"])
                profile_results["duplicate_strategies"] = duplicate_strategies
                
                # Perform duplicate analysis using the best strategy
                if duplicate_strategies:
                    best_strategy = duplicate_strategies[0]  # Use the first (highest priority) strategy
                    
                    # Handle large datasets by processing in chunks if needed
                    total_rows = len(df)
                    chunk_size = 10000  # Process 10,000 rows at a time
                    
                    if total_rows > chunk_size:
                        # Process in chunks and aggregate results
                        profile_results["duplicate_analysis"]["processing_note"] = f"Dataset has {total_rows} rows. Processing in chunks of {chunk_size} rows."
                        
                        # Initialize aggregate results
                        exact_duplicates = {
                            "exact_duplicates": {},
                            "summary": {
                                "total_rows": total_rows,
                                "exact_duplicate_groups": 0,
                                "exact_duplicate_records": 0
                            }
                        }
                        
                        # Process in chunks
                        for i in range(0, total_rows, chunk_size):
                            end_idx = min(i + chunk_size, total_rows)
                            chunk_df = df.iloc[i:end_idx]
                            
                            # Check for exact duplicates in this chunk
                            chunk_results = detect_exact_duplicates(chunk_df, best_strategy["columns"])
                            
                            # Aggregate results
                            exact_duplicates["summary"]["exact_duplicate_groups"] += chunk_results["summary"]["exact_duplicate_groups"]
                            exact_duplicates["summary"]["exact_duplicate_records"] += chunk_results["summary"]["exact_duplicate_records"]
                            
                            # Merge duplicate groups
                            for group_id, group_data in chunk_results["exact_duplicates"].items():
                                new_group_id = f"group_{len(exact_duplicates['exact_duplicates']) + 1}"
                                exact_duplicates["exact_duplicates"][new_group_id] = group_data
                    else:
                        # Process the entire dataset at once
                        exact_duplicates = detect_exact_duplicates(df, best_strategy["columns"])
                    
                    profile_results["duplicate_analysis"]["exact_duplicates"] = exact_duplicates
                    
                    # Check for fuzzy duplicates if appropriate
                    string_columns = [col for col in best_strategy["columns"] if pd.api.types.is_string_dtype(df[col])]
                    if string_columns and best_strategy["threshold"] < 100:
                        # For fuzzy matching, use a sample if the dataset is very large
                        sample_size = min(5000, total_rows)
                        if total_rows > sample_size:
                            sample_df = df.sample(sample_size, random_state=42)
                            profile_results["duplicate_analysis"]["fuzzy_note"] = f"Fuzzy matching performed on a sample of {sample_size} rows due to dataset size."
                            fuzzy_duplicates = detect_fuzzy_duplicates(sample_df, string_columns, similarity_threshold)
                        else:
                            fuzzy_duplicates = detect_fuzzy_duplicates(df, string_columns, similarity_threshold)
                        
                        profile_results["duplicate_analysis"]["fuzzy_duplicates"] = fuzzy_duplicates
                
                # Generate recommendations for duplicate handling
                duplicate_recommendations = generate_duplicate_recommendations(profile_results["duplicate_analysis"])
                profile_results["duplicate_analysis"]["recommendations"] = duplicate_recommendations
                
                # Create instructions for Oskar
                oskar_instructions = {
                    "file_path": file_path,
                    "cleaning_tasks": []
                }
                
                # Add duplicate handling instructions
                if "exact_duplicates" in profile_results["duplicate_analysis"]:
                    exact_dupes = profile_results["duplicate_analysis"]["exact_duplicates"]
                    if exact_dupes["summary"]["exact_duplicate_groups"] > 0:
                        oskar_instructions["cleaning_tasks"].append({
                            "task_type": "remove_exact_duplicates",
                            "columns": best_strategy["columns"],
                            "duplicate_count": exact_dupes["summary"]["exact_duplicate_records"],
                            "group_count": exact_dupes["summary"]["exact_duplicate_groups"]
                        })
                
                # Add fuzzy duplicate handling instructions
                if "fuzzy_duplicates" in profile_results["duplicate_analysis"]:
                    fuzzy_dupes = profile_results["duplicate_analysis"]["fuzzy_duplicates"]
                    if "fuzzy_duplicates" in fuzzy_dupes and len(fuzzy_dupes["fuzzy_duplicates"]) > 0:
                        oskar_instructions["cleaning_tasks"].append({
                            "task_type": "review_fuzzy_duplicates",
                            "columns": string_columns,
                            "similarity_threshold": similarity_threshold,
                            "group_count": len(fuzzy_dupes["fuzzy_duplicates"])
                        })
                
                # Add data quality cleaning instructions
                for col, stats in profile_results["column_stats"].items():
                    # Handle missing values
                    if stats["nulls"] > 0:
                        oskar_instructions["cleaning_tasks"].append({
                            "task_type": "handle_missing_values",
                            "column": col,
                            "missing_count": stats["nulls"],
                            "data_type": stats["type"]
                        })
                    
                    # Handle inconsistent capitalization
                    if "inconsistent_capitalization" in stats and stats["inconsistent_capitalization"]:
                        oskar_instructions["cleaning_tasks"].append({
                            "task_type": "standardize_capitalization",
                            "column": col,
                            "inconsistent_count": stats["potential_duplicates_due_to_case"]
                        })
                
                profile_results["oskar_instructions"] = oskar_instructions
            
            # Generate overall summary
            profile_results["summary"] = generate_profile_summary(df, profile_results)
            
            # Generate recommendations
            profile_results["recommendations"] = generate_quality_recommendations(
                df, 
                profile_results["column_stats"],
                detect_outliers(df, columns_to_analyze)
            )
            
            # Generate next steps for Steward
            profile_results["next_steps"] = generate_next_steps(profile_results)
            
            # Add AI-driven analysis if requested and API key is available
            if ai_analysis and openai_api_key:
                profile_results["ai_analysis"] = perform_ai_analysis(df, profile_results)
            
            return profile_results
            
        except Exception as e:
            return {"error": f"Error during profiling: {str(e)}"}

# Helper function to detect exact duplicates
def detect_exact_duplicates(df, columns):
    """Detects exact duplicates in the dataset"""
    results = {
        "exact_duplicates": {},
        "summary": {
            "total_rows": len(df),
            "exact_duplicate_groups": 0,
            "exact_duplicate_records": 0
        },
        "recommendations": []
    }
    
    # Check for exact duplicates
    if len(columns) > 0:
        exact_dupes = df.duplicated(subset=columns, keep=False)
        if exact_dupes.any():
            exact_dupe_df = df[exact_dupes].copy()
            
            # Group by the columns to find duplicate sets
            # Use a more efficient approach for large dataframes
            if len(exact_dupe_df) > 10000:
                # For very large dataframes, just report summary statistics
                duplicate_count = exact_dupes.sum()
                group_count = len(exact_dupe_df.groupby(columns))
                
                results["exact_duplicates"] = {
                    "summary_only": True,
                    "message": f"Found {duplicate_count} duplicate records in {group_count} groups. Data too large to display all records."
                }
                results["summary"]["exact_duplicate_groups"] = group_count
                results["summary"]["exact_duplicate_records"] = duplicate_count
            else:
                # For smaller dataframes, show the actual duplicates
                exact_dupe_groups = exact_dupe_df.groupby(columns).apply(lambda x: x.index.tolist()).to_dict()
                
                # Convert to serializable format
                exact_duplicate_groups = {}
                for i, (key, indices) in enumerate(exact_dupe_groups.items()):
                    if isinstance(key, tuple):
                        key_str = "_".join(str(k) for k in key)
                    else:
                        key_str = str(key)
                    
                    # Limit the number of records shown per group
                    if len(indices) > 10:
                        sample_indices = indices[:10]
                        group_data = df.iloc[sample_indices].to_dict(orient='records')
                        exact_duplicate_groups[f"group_{i+1}"] = {
                            "count": len(indices),
                            "records_shown": 10,
                            "records": group_data,
                            "note": f"Showing 10 of {len(indices)} records in this group"
                        }
                    else:
                        group_data = df.iloc[indices].to_dict(orient='records')
                        exact_duplicate_groups[f"group_{i+1}"] = {
                            "count": len(indices),
                            "records": group_data
                        }
                
                results["exact_duplicates"] = exact_duplicate_groups
                results["summary"]["exact_duplicate_groups"] = len(exact_duplicate_groups)
                results["summary"]["exact_duplicate_records"] = exact_dupes.sum()
            
            # Add recommendation
            results["recommendations"].append(
                f"Found {results['summary']['exact_duplicate_records']} exact duplicate records in {results['summary']['exact_duplicate_groups']} groups. Consider using Oskar to deduplicate these records."
            )
    
    if not results["recommendations"]:
        results["recommendations"].append(
            "No exact duplicate issues detected in the analyzed columns."
        )
    
    return results

# Helper function for fuzzy duplicate detection
def detect_fuzzy_duplicates(df, string_columns, similarity_threshold):
    """Detects fuzzy duplicates using a more efficient algorithm"""
    results = {
        "fuzzy_duplicates": {},
        "summary": {
            "total_rows": len(df),
            "fuzzy_duplicate_groups": 0,
            "fuzzy_duplicate_records": 0
        }
    }
    
    # Create a combined string for fuzzy matching
    df['_combined_for_fuzzy'] = df[string_columns].apply(
        lambda row: " ".join(str(val) for val in row if pd.notna(val)), axis=1
    )
    
    # Find fuzzy duplicates more efficiently
    fuzzy_groups = []
    processed_indices = set()
    
    # Limit the number of rows to compare to prevent timeouts
    max_rows_to_compare = min(500, len(df))
    sample_indices = np.random.choice(df.index, size=max_rows_to_compare, replace=False) if len(df) > max_rows_to_compare else df.index
    
    # Use a more efficient approach - compare only a subset of rows
    for i in sample_indices:
        if i in processed_indices:
            continue
            
        group = [i]
        processed_indices.add(i)
        
        # Only compare against a limited number of other rows
        comparison_limit = min(100, len(df))
        potential_matches = df.index.difference([i])
        if len(potential_matches) > comparison_limit:
            potential_matches = np.random.choice(potential_matches, size=comparison_limit, replace=False)
        
        for j in potential_matches:
            if j in processed_indices:
                continue
                
            # Calculate similarity
            similarity = fuzz.token_sort_ratio(
                df.loc[i, '_combined_for_fuzzy'], 
                df.loc[j, '_combined_for_fuzzy']
            )
            
            if similarity >= similarity_threshold:
                group.append(j)
                processed_indices.add(j)
        
        if len(group) > 1:
            fuzzy_groups.append({
                "count": len(group),
                "similarity_threshold": similarity_threshold,
                "records": df.iloc[group][string_columns + ['_combined_for_fuzzy']].to_dict(orient='records')
            })
    
    # Remove the temporary column
    df.drop('_combined_for_fuzzy', axis=1, inplace=True)
    
    # Format the results
    for i, group in enumerate(fuzzy_groups):
        results["fuzzy_duplicates"][f"fuzzy_group_{i+1}"] = group
    
    results["summary"]["fuzzy_duplicate_groups"] = len(fuzzy_groups)
    results["summary"]["fuzzy_duplicate_records"] = sum(group["count"] for group in fuzzy_groups)
    
    return results

# Helper function to identify key columns
def identify_key_columns(df):
    """Identifies potential key columns in the dataset"""
    key_columns = {
        "identifier": [],
        "name": [],
        "categorical": [],
        "numerical": [],
        "date": []
    }
    
    # Identify ID columns
    id_patterns = ['id', 'key', 'code', 'number', 'nr', 'no']
    for col in df.columns:
        col_lower = col.lower()
        # Check if column name contains ID patterns
        if any(pattern in col_lower for pattern in id_patterns):
            key_columns["identifier"].append(col)
        
        # Check if column name suggests it's a name
        elif any(name_part in col_lower for name_part in ['name', 'first', 'last', 'customer', 'client', 'person']):
            key_columns["name"].append(col)
        
        # Categorize by data type
        elif pd.api.types.is_numeric_dtype(df[col]):
            key_columns["numerical"].append(col)
        elif pd.api.types.is_string_dtype(df[col]):
            # Check if it's likely categorical (few unique values relative to dataset size)
            if df[col].nunique() < min(50, len(df) * 0.1):
                key_columns["categorical"].append(col)
        elif pd.api.types.is_datetime64_dtype(df[col]):
            key_columns["date"].append(col)
    
    return key_columns

# Helper function to generate data quality summary
def generate_data_quality_summary(df, column_stats):
    """Generates a summary of data quality dimensions"""
    quality_summary = {
        "completeness": {
            "score": 0,
            "issues": []
        },
        "uniqueness": {
            "score": 0,
            "issues": []
        },
        "consistency": {
            "score": 0,
            "issues": []
        },
        "accuracy": {
            "score": 0,
            "issues": []
        },
        "validity": {
            "score": 0,
            "issues": []
        }
    }
    
    # Assess completeness (missing values)
    total_cells = len(df) * len(df.columns)
    missing_cells = df.isnull().sum().sum()
    completeness_score = 100 - (missing_cells / total_cells * 100)
    quality_summary["completeness"]["score"] = round(completeness_score, 2)
    
    for col, stats in column_stats.items():
        if stats["nulls"] > 0:
            quality_summary["completeness"]["issues"].append({
                "column": col,
                "missing_values": stats["nulls"],
                "percentage": stats["null_percentage"]
            })
    
    # Assess uniqueness (duplicate values)
    uniqueness_issues = []
    for col, stats in column_stats.items():
        if stats["unique_values"] < len(df) * 0.9 and stats["unique_values"] > 1:
            # Column has duplicates but is not a single value
            uniqueness_issues.append({
                "column": col,
                "unique_values": stats["unique_values"],
                "percentage": stats["unique_percentage"]
            })
    
    quality_summary["uniqueness"]["issues"] = uniqueness_issues
    quality_summary["uniqueness"]["score"] = 100 - min(100, len(uniqueness_issues) * 10)
    
    # Assess consistency (inconsistent capitalization)
    consistency_issues = []
    for col, stats in column_stats.items():
        if "inconsistent_capitalization" in stats and stats["inconsistent_capitalization"]:
            consistency_issues.append({
                "column": col,
                "issue": "Inconsistent capitalization",
                "potential_duplicates": stats["potential_duplicates_due_to_case"]
            })
    
    quality_summary["consistency"]["issues"] = consistency_issues
    quality_summary["consistency"]["score"] = 100 - min(100, len(consistency_issues) * 15)
    
    return quality_summary

# Helper function to detect outliers
def detect_outliers(df, columns):
    """Detects outliers in the dataset"""
    outliers = {}
    
    # Process numerical columns
    numerical_cols = [col for col in columns if pd.api.types.is_numeric_dtype(df[col])]
    for col in numerical_cols:
        # Calculate IQR
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        
        # Define bounds
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # Find outliers
        outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
        outlier_count = outlier_mask.sum()
        
        if outlier_count > 0:
            outliers[col] = {
                "method": "IQR",
                "count": int(outlier_count),
                "percentage": f"{(outlier_count / len(df) * 100):.2f}%",
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound)
            }
    
    # Process categorical columns
    categorical_cols = [col for col in columns if pd.api.types.is_string_dtype(df[col])]
    for col in categorical_cols:
        # Calculate frequency
        value_counts = df[col].value_counts(normalize=True)
        
        # Find rare categories (less than 1%)
        rare_values = value_counts[value_counts < 0.01]
        
        if len(rare_values) > 0:
            outliers[col] = {
                "method": "Rare categories",
                "count": int(len(rare_values)),
                "percentage": f"{(sum(rare_values) * 100):.2f}%",
                "examples": rare_values.index.tolist()[:5]  # First 5 rare values
            }
    
    return outliers

# Helper function to suggest duplicate detection strategies
def suggest_duplicate_strategies(df, key_columns):
    """Suggests strategies for duplicate detection"""
    strategies = []
    
    # Strategy 1: Use identifier columns if available
    if "identifier" in key_columns and key_columns["identifier"]:
        strategies.append({
            "name": "Exact ID match",
            "columns": key_columns["identifier"],
            "description": "Find records with identical ID values",
            "threshold": 100
        })
    
    # Strategy 2: Use name columns if available
    if "name" in key_columns and key_columns["name"]:
        strategies.append({
            "name": "Fuzzy name match",
            "columns": key_columns["name"],
            "description": "Find records with similar names using fuzzy matching",
            "threshold": 85
        })
    
    # Strategy 3: Combine name and categorical columns
    name_cols = key_columns.get("name", [])
    cat_cols = key_columns.get("categorical", [])
    
    if name_cols and cat_cols:
        combined_cols = name_cols + cat_cols[:2]  # Limit to first 2 categorical columns
        strategies.append({
            "name": "Name + Category match",
            "columns": combined_cols,
            "description": "Find records with similar names and categories",
            "threshold": 90
        })
    
    # Strategy 4: Use all string columns for a comprehensive check
    string_cols = [col for col in df.columns if pd.api.types.is_string_dtype(df[col])]
    if len(string_cols) > 0:
        # Limit to a reasonable number of columns
        selected_string_cols = string_cols[:min(5, len(string_cols))]
        strategies.append({
            "name": "All text fields",
            "columns": selected_string_cols,
            "description": "Comprehensive check across all text fields",
            "threshold": 80
        })
    
    return strategies

# Helper function to generate profile summary
def generate_profile_summary(df, profile_results):
    """Generates a human-readable summary of the profile results"""
    column_stats = profile_results["column_stats"]
    
    # Count columns by type
    column_types = {}
    for col, stats in column_stats.items():
        col_type = stats["type"]
        column_types[col_type] = column_types.get(col_type, 0) + 1
    
    # Count columns with issues
    columns_with_nulls = sum(1 for stats in column_stats.values() if stats["nulls"] > 0)
    columns_with_outliers = sum(1 for stats in column_stats.values() if "outliers" in stats)
    columns_with_inconsistent_case = sum(1 for stats in column_stats.values() if "inconsistent_capitalization" in stats)
    
    # Generate summary text
    summary = [
        f"Dataset has {len(df)} rows and {len(df.columns)} columns.",
        f"Column types: {', '.join([f'{type}: {count}' for type, count in column_types.items()])}",
        f"Data quality: {columns_with_nulls} columns have missing values, {columns_with_outliers} have outliers, {columns_with_inconsistent_case} have inconsistent capitalization."
    ]
    
    # Add key column information
    if "key_columns" in profile_results:
        key_col_summary = []
        for key_type, cols in profile_results["key_columns"].items():
            if cols:
                key_col_summary.append(f"{key_type}: {', '.join(cols[:3])}" + ("..." if len(cols) > 3 else ""))
        
        if key_col_summary:
            summary.append(f"Key columns: {'; '.join(key_col_summary)}")
    
    # Add duplicate strategy information
    if "duplicate_strategies" in profile_results:
        strategy_count = len(profile_results["duplicate_strategies"])
        if strategy_count > 0:
            summary.append(f"Suggested {strategy_count} strategies for duplicate detection.")
    
    return "\n".join(summary)

# Helper function to generate quality recommendations
def generate_quality_recommendations(df, column_stats, outliers):
    """Generates recommendations for improving data quality"""
    recommendations = []
    
    # Check for missing values
    columns_with_nulls = [col for col, stats in column_stats.items() if stats["nulls"] > 0]
    if columns_with_nulls:
        recommendations.append(f"Fill missing values in {', '.join(columns_with_nulls[:3])}" + 
                              ("..." if len(columns_with_nulls) > 3 else ""))
    
    # Check for inconsistent capitalization
    inconsistent_case_cols = [col for col, stats in column_stats.items() 
                             if "inconsistent_capitalization" in stats and stats["inconsistent_capitalization"]]
    if inconsistent_case_cols:
        recommendations.append(f"Standardize capitalization in {', '.join(inconsistent_case_cols[:3])}" +
                              ("..." if len(inconsistent_case_cols) > 3 else ""))
    
    # Check for outliers
    if outliers:
        outlier_cols = list(outliers.keys())
        recommendations.append(f"Review outliers in {', '.join(outlier_cols[:3])}" +
                              ("..." if len(outlier_cols) > 3 else ""))
    
    # Check for potential duplicate detection
    string_cols = [col for col, stats in column_stats.items() if "type" in stats and "object" in stats["type"]]
    if string_cols:
        recommendations.append("Consider using fuzzy matching for duplicate detection on text fields")
    
    return recommendations

# New function for AI-driven analysis
def perform_ai_analysis(df, profile_results):
    """
    Uses OpenAI to provide deeper insights about the data
    
    Args:
        df: The pandas DataFrame
        profile_results: The existing profile results
        
    Returns:
        A dictionary with AI-generated insights
    """
    try:
        # Create a sample of the data for analysis
        sample_size = min(100, len(df))
        sample_df = df.head(sample_size)
        
        # Create a description of the data
        data_description = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": list(df.columns),
            "sample_data": sample_df.head(5).to_dict(orient='records'),
            "column_types": {col: str(df[col].dtype) for col in df.columns},
            "missing_values": {col: int(df[col].isnull().sum()) for col in df.columns},
            "unique_values": {col: int(df[col].nunique()) for col in df.columns},
            "profile_summary": profile_results.get("summary", "")
        }
        
        # Create a prompt for the AI
        prompt = f"""
        You are a data analysis expert. I need you to analyze this dataset and provide insights.
        
        Here's information about the dataset:
        - Total rows: {data_description['total_rows']}
        - Total columns: {data_description['total_columns']}
        - Columns: {data_description['columns']}
        - Column types: {json.dumps(data_description['column_types'])}
        - Missing values per column: {json.dumps(data_description['missing_values'])}
        - Unique values per column: {json.dumps(data_description['unique_values'])}
        
        Here's a sample of the data:
        {json.dumps(data_description['sample_data'], indent=2)}
        
        Profile summary:
        {data_description['profile_summary']}
        
        Based on this information, please provide:
        
        1. Data type identification: What type of data is this (e.g., customer data, product data, transaction data)?
        
        2. Data quality assessment: What are the main data quality issues and how might they impact analysis?
        
        3. Potential insights: What interesting patterns or relationships might exist in this data?
        
        4. Recommendations: What specific steps would you recommend to clean and prepare this data?
        
        5. Analysis suggestions: What types of analyses would be most valuable with this dataset?
        
        Please be specific and provide examples where possible.
        """
        
        # Call the OpenAI API
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a data analysis expert providing insights about datasets."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1500,
            temperature=0.2
        )
        
        # Extract the response
        ai_insights = response.choices[0].message.content
        
        # Return the insights
        return {
            "insights": ai_insights,
            "data_type": infer_data_type(df, ai_insights),
            "analysis_suggestions": extract_analysis_suggestions(ai_insights)
        }
        
    except Exception as e:
        return {"error": f"Error performing AI analysis: {str(e)}"}

def infer_data_type(df, ai_insights):
    """
    Infers the type of data based on column names and AI insights
    
    Args:
        df: The pandas DataFrame
        ai_insights: The AI-generated insights
        
    Returns:
        A string describing the data type
    """
    # Look for common column patterns
    columns = [col.lower() for col in df.columns]
    
    # Customer data indicators
    customer_indicators = ['customer', 'client', 'name', 'email', 'phone', 'address', 'city', 'state', 'zip']
    
    # Product data indicators
    product_indicators = ['product', 'item', 'sku', 'price', 'cost', 'category', 'inventory']
    
    # Transaction data indicators
    transaction_indicators = ['transaction', 'order', 'purchase', 'sale', 'date', 'amount', 'quantity']
    
    # Count matches
    customer_count = sum(1 for indicator in customer_indicators if any(indicator in col for col in columns))
    product_count = sum(1 for indicator in product_indicators if any(indicator in col for col in columns))
    transaction_count = sum(1 for indicator in transaction_indicators if any(indicator in col for col in columns))
    
    # Determine type based on highest count
    counts = {
        "Customer data": customer_count,
        "Product data": product_count,
        "Transaction data": transaction_count
    }
    
    # Get the type with the highest count
    inferred_type = max(counts, key=counts.get)
    
    # If the AI mentioned a specific type, use that instead
    if "customer data" in ai_insights.lower():
        inferred_type = "Customer data"
    elif "product data" in ai_insights.lower():
        inferred_type = "Product data"
    elif "transaction data" in ai_insights.lower():
        inferred_type = "Transaction data"
    
    return inferred_type

def extract_analysis_suggestions(ai_insights):
    """
    Extracts analysis suggestions from the AI insights
    
    Args:
        ai_insights: The AI-generated insights
        
    Returns:
        A list of analysis suggestions
    """
    # Common analysis types to look for
    analysis_types = [
        "clustering",
        "segmentation",
        "regression",
        "classification",
        "time series",
        "correlation",
        "trend analysis",
        "anomaly detection",
        "predictive modeling",
        "cohort analysis"
    ]
    
    # Extract suggestions that mention these analysis types
    suggestions = []
    
    for analysis_type in analysis_types:
        if analysis_type in ai_insights.lower():
            # Find the sentence containing this analysis type
            sentences = ai_insights.split('.')
            for sentence in sentences:
                if analysis_type in sentence.lower():
                    suggestions.append(sentence.strip())
                    break
    
    # If no specific analysis types were found, return a generic suggestion
    if not suggestions:
        suggestions = ["Consider exploratory data analysis to identify patterns and relationships."]
    
    return suggestions

def generate_duplicate_recommendations(duplicate_analysis):
    """Generates recommendations for handling duplicates"""
    recommendations = []
    
    # Check for exact duplicates
    if "exact_duplicates" in duplicate_analysis:
        exact_dupes = duplicate_analysis["exact_duplicates"]
        if "summary" in exact_dupes and exact_dupes["summary"]["exact_duplicate_groups"] > 0:
            count = exact_dupes["summary"]["exact_duplicate_records"]
            groups = exact_dupes["summary"]["exact_duplicate_groups"]
            recommendations.append(
                f"Remove {count} exact duplicate records found in {groups} groups."
            )
    
    # Check for fuzzy duplicates
    if "fuzzy_duplicates" in duplicate_analysis:
        fuzzy_dupes = duplicate_analysis["fuzzy_duplicates"]
        if "fuzzy_duplicates" in fuzzy_dupes:
            groups = len(fuzzy_dupes["fuzzy_duplicates"])
            if groups > 0:
                recommendations.append(
                    f"Review {groups} groups of similar records that may be duplicates."
                )
                recommendations.append(
                    "Consider using Oskar (Cleaning Agent) to merge or deduplicate these records."
                )
    
    return recommendations

# Helper function to generate next steps for Steward
def generate_next_steps(profile_results):
    """Generates next steps for the Steward agent"""
    next_steps = []
    
    # Check for duplicates
    if "duplicate_analysis" in profile_results:
        duplicate_analysis = profile_results["duplicate_analysis"]
        
        # Check for exact duplicates
        if "exact_duplicates" in duplicate_analysis:
            exact_dupes = duplicate_analysis["exact_duplicates"]
            if "summary" in exact_dupes and exact_dupes["summary"]["exact_duplicate_groups"] > 0:
                next_steps.append(f"Route to Oskar to remove {exact_dupes['summary']['exact_duplicate_records']} exact duplicates")
        
        # Check for fuzzy duplicates
        if "fuzzy_duplicates" in duplicate_analysis:
            fuzzy_dupes = duplicate_analysis["fuzzy_duplicates"]
            if "fuzzy_duplicates" in fuzzy_dupes and len(fuzzy_dupes["fuzzy_duplicates"]) > 0:
                next_steps.append(f"Route to Oskar to review and merge {len(fuzzy_dupes['fuzzy_duplicates'])} groups of similar records")
    
    # Check for data quality issues
    data_quality_issues = []
    for col, stats in profile_results["column_stats"].items():
        if stats["nulls"] > 0:
            data_quality_issues.append(f"missing values in {col}")
        if "inconsistent_capitalization" in stats and stats["inconsistent_capitalization"]:
            data_quality_issues.append(f"inconsistent capitalization in {col}")
    
    if data_quality_issues:
        next_steps.append(f"Route to Oskar to clean data quality issues: {', '.join(data_quality_issues[:3])}" + 
                         ("..." if len(data_quality_issues) > 3 else ""))
    
    # If no issues found
    if not next_steps:
        next_steps.append("Data quality is good - proceed to next step in workflow")
    
    return next_steps 