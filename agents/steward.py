from mcp.server.fastmcp import FastMCP
import pandas as pd
import os
import json

def register(mcp: FastMCP):
    @mcp.tool(name="🧠 Steward - Data Steward")
    def steward(request: str, data_source: str = None, emma_results: dict = None) -> dict:
        """
        Routes tasks and manages approval logic.
        
        Args:
            request: The user's request or intent
            data_source: Optional path to data source (e.g., CSV file path)
            emma_results: Optional results from Emma's profiling (for workflow continuation)
        
        Returns:
            A dictionary with routing information and initial analysis
        """
        intent = request.lower()
        response = {
            "routing": "",
            "questions": [],
            "initial_analysis": {},
            "next_steps": [],
            "workflow": []
        }
        
        # If Emma results are provided, this is a workflow continuation
        if emma_results:
            # Extract key information from Emma's results
            data_quality_issues = []
            duplicate_issues = []
            
            # Check for duplicates
            if "duplicate_analysis" in emma_results:
                duplicate_analysis = emma_results["duplicate_analysis"]
                
                # Check for exact duplicates
                if "exact_duplicates" in duplicate_analysis:
                    exact_dupes = duplicate_analysis["exact_duplicates"]
                    if "summary" in exact_dupes and exact_dupes["summary"]["exact_duplicate_groups"] > 0:
                        count = exact_dupes["summary"]["exact_duplicate_records"]
                        groups = exact_dupes["summary"]["exact_duplicate_groups"]
                        duplicate_issues.append(f"{count} exact duplicates in {groups} groups")
                
                # Check for fuzzy duplicates
                if "fuzzy_duplicates" in duplicate_analysis:
                    fuzzy_dupes = duplicate_analysis["fuzzy_duplicates"]
                    if "fuzzy_duplicates" in fuzzy_dupes and len(fuzzy_dupes["fuzzy_duplicates"]) > 0:
                        groups = len(fuzzy_dupes["fuzzy_duplicates"])
                        duplicate_issues.append(f"{groups} groups of similar records")
            
            # Check for data quality issues
            if "column_stats" in emma_results:
                for col, stats in emma_results["column_stats"].items():
                    if stats["nulls"] > 0:
                        data_quality_issues.append(f"missing values in {col}")
                    if "inconsistent_capitalization" in stats and stats["inconsistent_capitalization"]:
                        data_quality_issues.append(f"inconsistent capitalization in {col}")
            
            # Route to Oskar if there are issues to clean
            if duplicate_issues or data_quality_issues:
                response["routing"] = "Routed to Oskar (Cleaning Agent)"
                
                # Prepare instructions for Oskar
                response["oskar_instructions"] = emma_results.get("oskar_instructions", {})
                
                # Add questions for the user
                response["questions"] = [
                    "Would you like to automatically clean all issues or review them first?",
                    "Are there specific columns you want to prioritize for cleaning?",
                    "How should missing values be handled (remove, impute with mean/median/mode)?"
                ]
                
                # Add next steps
                response["next_steps"] = []
                if duplicate_issues:
                    response["next_steps"].append(f"Deduplicate records: {', '.join(duplicate_issues)}")
                if data_quality_issues:
                    response["next_steps"].append(f"Clean data quality issues: {', '.join(data_quality_issues[:3])}" + 
                                                ("..." if len(data_quality_issues) > 3 else ""))
                
                # Add workflow information
                response["workflow"] = [
                    "Emma (Profiling Agent) - ✓ Completed",
                    "Oskar (Cleaning Agent) - ⟳ In Progress",
                    "Mathias (Mapping Agent) - Next",
                    "James (Migration Agent) - Pending",
                    "Gina (Visualization Agent) - Pending"
                ]
                
                return response
            else:
                # If no issues to clean, route to the next step in the workflow
                response["routing"] = "Routed to Mathias (Mapping Agent)"
                response["questions"] = [
                    "What are your target field mappings?",
                    "Are there any field transformations needed?",
                    "Do you have a target schema to map to?"
                ]
                response["next_steps"] = ["Define field mappings", "Create transformation rules", "Validate mappings"]
                response["workflow"] = [
                    "Emma (Profiling Agent) - ✓ Completed",
                    "Oskar (Cleaning Agent) - ✓ Skipped (No issues)",
                    "Mathias (Mapping Agent) - ⟳ In Progress",
                    "James (Migration Agent) - Pending",
                    "Gina (Visualization Agent) - Pending"
                ]
                return response
        
        # Connect to data source if provided
        data_overview = {}
        if data_source and os.path.exists(data_source):
            try:
                if data_source.endswith('.csv'):
                    df = pd.read_csv(data_source)
                    
                    # Create a basic overview of the data
                    data_overview = {
                        "rows": len(df),
                        "columns": list(df.columns),
                        "sample": df.head(3).to_dict(orient='records'),
                        "missing_values": {col: int(df[col].isnull().sum()) for col in df.columns}
                    }
                    response["initial_analysis"] = data_overview
                    
                    # If we have a data source and the request mentions duplicates,
                    # we should automatically route to the deduplication workflow
                    if any(keyword in intent for keyword in ["duplicate", "duplicates", "duplication", "same", "similar"]):
                        response["routing"] = "Routed to Emma (Profiling Agent) first, then Oskar (Cleaning Agent)"
                        response["workflow"] = [
                            "Emma (Profiling Agent) - Data profiling and duplicate detection",
                            "Oskar (Cleaning Agent) - Deduplication and cleaning"
                        ]
                        
                        response["questions"] = [
                            "Which columns should be used to identify duplicates?",
                            "What similarity threshold would you like to use (default: 90%)?",
                            "Would you like to automatically merge duplicates or review them first?"
                        ]
                        
                        response["next_steps"] = [
                            "Profile data to identify potential duplicates",
                            "Generate similarity report for duplicate records",
                            "Prepare merge suggestions for review"
                        ]
                        
                        return response
                    
                    # For any new data source without specific intent, first route to Mike for project coordination
                    response["routing"] = "Routed to Mike (Lead Agent)"
                    response["workflow"] = [
                        "Mike (Lead Agent) - Project initialization",
                        "Emma (Profiling Agent) - Data profiling",
                        "Oskar (Cleaning Agent) - Data cleaning (if needed)",
                        "Mathias (Mapping Agent) - Field mapping (if needed)",
                        "James (Migration Agent) - Data migration (if needed)",
                        "Gina (Visualization Agent) - Results visualization"
                    ]
                    
                    response["questions"] = [
                        "What is the overall goal for this data set?",
                        "Are there any specific data quality concerns?",
                        "What is the timeline for this project?"
                    ]
                    
                    response["next_steps"] = [
                        "Initialize project tracking",
                        "Perform initial data profiling with Emma",
                        "Determine required data transformations"
                    ]
                    
                    return response
                    
            except Exception as e:
                response["error"] = f"Error analyzing data source: {str(e)}"
        
        # If no data source or specific intent provided, determine routing based on intent
        if "profile" in intent or "analyze" in intent or "examine" in intent:
            response["routing"] = "Routed to Emma (Profiling Agent)"
            response["questions"] = [
                "Which specific columns would you like to profile?",
                "Are you interested in data quality metrics or just data types?",
                "Would you like to identify potential duplicate fields?"
            ]
            response["next_steps"] = [
                "Run full profile", 
                "Check for missing values", 
                "Analyze data distributions",
                "Identify inconsistent values",
                "Detect potential duplicates"
            ]
            
        elif any(keyword in intent for keyword in ["deduplicate", "clean", "duplicate", "duplicates", "duplication", "same", "similar"]):
            # For cleaning requests, we should first profile the data
            response["routing"] = "Routed to Emma (Profiling Agent) first, then Oskar (Cleaning Agent)"
            response["questions"] = [
                "Which columns should be analyzed for potential duplicates?",
                "What similarity threshold would you like to use (default: 90%)?",
                "Would you like to automatically merge duplicates or review them first?"
            ]
            response["next_steps"] = [
                "Profile data to identify inconsistencies",
                "Identify potential duplicates", 
                "Generate similarity report", 
                "Prepare merge suggestions"
            ]
            
        elif "map" in intent:
            response["routing"] = "Routed to Mathias (Mapping Agent)"
            response["questions"] = [
                "What are your target field mappings?",
                "Are there any field transformations needed?",
                "Do you have a target schema to map to?"
            ]
            response["next_steps"] = ["Define field mappings", "Create transformation rules", "Validate mappings"]
            
        elif "migrate" in intent or "export" in intent or "load" in intent:
            response["routing"] = "Routed to James (Migration Agent)"
            response["questions"] = [
                "What is the target system or format?",
                "Do you need incremental or full migration?",
                "Are there any special handling requirements for the migration?"
            ]
            response["next_steps"] = ["Configure target connection", "Set up migration rules", "Schedule migration job"]
            
        elif "visualize" in intent or "chart" in intent or "graph" in intent:
            response["routing"] = "Routed to Gina (Visualization Agent)"
            response["questions"] = [
                "What type of visualization would you prefer (bar, line, pie, etc.)?",
                "Which metrics or KPIs are most important to visualize?",
                "Do you need interactive dashboards or static reports?"
            ]
            response["next_steps"] = ["Generate summary charts", "Prepare data quality dashboard", "Create detailed reports"]
            
        elif "orchestrate" in intent or "manage" in intent or "coordinate" in intent or "start" in intent or "begin" in intent:
            response["routing"] = "Routed to Mike (Lead Agent)"
            response["questions"] = [
                "What is the overall project goal?",
                "Which agents need to be coordinated?",
                "What is the current status of the project?"
            ]
            response["next_steps"] = ["Create project plan", "Monitor agent activities", "Generate status reports"]
            
        else:
            response["routing"] = "Unclear request — I need more information."
            response["questions"] = [
                "Could you clarify if you need to profile, clean, map, migrate, or visualize data?",
                "What is the main goal of your data project?",
                "What data source would you like to work with?"
            ]
            response["next_steps"] = ["Clarify request", "Connect to data source", "Determine appropriate agent"]
        
        return response 