import matplotlib.pyplot as plt
import pandas as pd
import io
import base64
from mcp.server.fastmcp import FastMCP
import os
import json
import glob
from datetime import datetime
import markdown
import re

def register(mcp: FastMCP):
    @mcp.tool(name="📊 Gina - Visualization Agent")
    def visualize_data(file_path: str, chart_type: str = "bar") -> str:
        """Prepares charts & metrics for data visualization"""
        try:
            df = pd.read_csv(file_path)
            
            # Create a simple visualization based on chart_type
            plt.figure(figsize=(10, 6))
            
            if chart_type == "bar":
                # Example: Count of non-null values per column
                df.count().plot(kind='bar')
                plt.title('Count of Values by Column')
                plt.ylabel('Count')
                plt.xlabel('Columns')
            
            # Save the plot to a bytes buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            
            # Convert to base64 for embedding in HTML/markdown
            img_str = base64.b64encode(buf.read()).decode('utf-8')
            
            return f"![Data Visualization](data:image/png;base64,{img_str})"
        except Exception as e:
            return f"Error generating visualization: {str(e)}"

    @mcp.tool(name="📊 Gina - Generate Migration Report")
    def generate_migration_report(
        project_directory: str,
        report_title: str = "Datenmigrationsreport",
        include_details: bool = True
    ) -> dict:
        """
        Erstellt einen umfassenden Bericht über den Datenmigrationsprozess im Markdown-Format
        
        Args:
            project_directory: Verzeichnis, in dem die Migrationsdateien gespeichert sind
            report_title: Titel des Berichts
            include_details: Ob detaillierte Informationen einbezogen werden sollen
            
        Returns:
            Ein Dictionary mit dem Bericht und Pfad zur gespeicherten Markdown-Datei
        """
        # Definiere report_file_path außerhalb des Try-Blocks
        report_file_path = None
        
        try:
            if not os.path.exists(project_directory):
                return {"error": f"Verzeichnis nicht gefunden: {project_directory}"}
            
            # Initialisiere den Markdown-Report
            report = [
                f"# {report_title}",
                f"Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}",
                "",
                "## Zusammenfassung",
                ""
            ]
            
            # Sammle Informationen aus verschiedenen Dateien
            mapping_files = glob.glob(os.path.join(project_directory, "*_field_mapping*.json"))
            value_mapping_files = glob.glob(os.path.join(project_directory, "*_value_mapping*.csv"))
            mapped_files = glob.glob(os.path.join(project_directory, "*_mapped.csv"))
            duplicate_files = glob.glob(os.path.join(project_directory, "*_duplicates_*.csv"))
            unique_files = glob.glob(os.path.join(project_directory, "*_unique.csv"))
            final_files = glob.glob(os.path.join(project_directory, "*_final*.csv"))
            report_files = glob.glob(os.path.join(project_directory, "*_report_*.json"))
            target_files = glob.glob(os.path.join(project_directory, "*_target*.csv"))
            
            # Statistiken sammeln
            stats = {
                "mapping_files": len(mapping_files),
                "value_mapping_files": len(value_mapping_files),
                "mapped_files": len(mapped_files),
                "duplicate_files": len(duplicate_files),
                "unique_files": len(unique_files),
                "final_files": len(final_files),
                "report_files": len(report_files),
                "target_files": len(target_files)
            }
            
            # Zusammenfassung hinzufügen
            report.append("### Migrationsprozess-Übersicht")
            report.append("")
            report.append("| Prozessschritt | Anzahl der Dateien |")
            report.append("|---------------|-------------------|")
            report.append(f"| Feldmappings | {stats['mapping_files']} |")
            report.append(f"| Wertmappings | {stats['value_mapping_files']} |")
            report.append(f"| Gemappte Dateien | {stats['mapped_files']} |")
            report.append(f"| Duplikat-Dateien | {stats['duplicate_files']} |")
            report.append(f"| Eindeutige Datensätze | {stats['unique_files']} |")
            report.append(f"| Finale Dateien | {stats['final_files']} |")
            report.append(f"| Zieldateien | {stats['target_files']} |")
            report.append(f"| Prozessberichte | {stats['report_files']} |")
            report.append("")
            
            # Teil 1: Quelldaten und Duplikate
            if include_details:
                # Quelldaten
                if mapped_files:
                    source_data = {}
                    for mapped_file in mapped_files:
                        try:
                            delimiter = detect_delimiter(mapped_file)
                            df = pd.read_csv(mapped_file, sep=delimiter)
                            base_name = os.path.basename(mapped_file).replace("_mapped.csv", "")
                            source_data[base_name] = {
                                "records": len(df),
                                "columns": len(df.columns),
                                "file_size": os.path.getsize(mapped_file) / 1024  # KB
                            }
                        except Exception as e:
                            print(f"Fehler beim Lesen von {mapped_file}: {str(e)}")
                    
                    if source_data:
                        report.append("### Quelldaten")
                        report.append("")
                        report.append("| Datei | Datensätze | Spalten | Dateigröße (KB) |")
                        report.append("|-------|------------|---------|-----------------|")
                        for name, data in source_data.items():
                            report.append(f"| {name} | {data['records']} | {data['columns']} | {data['file_size']:.2f} |")
                        report.append("")
                
                # Duplikate
                if duplicate_files:
                    duplicate_data = {}
                    for dup_file in duplicate_files:
                        try:
                            delimiter = detect_delimiter(dup_file)
                            df = pd.read_csv(dup_file, sep=delimiter)
                            
                            threshold_match = re.search(r'_duplicates_(\d+)pct', dup_file)
                            threshold = threshold_match.group(1) if threshold_match else "unbekannt"
                            
                            base_name = os.path.basename(dup_file).split("_duplicates_")[0]
                            duplicate_data[base_name] = {
                                "threshold": threshold,
                                "duplicate_count": len(df),
                                "file_path": dup_file
                            }
                        except Exception as e:
                            print(f"Fehler beim Lesen von {dup_file}: {str(e)}")
                    
                    if duplicate_data:
                        report.append("### Duplikate")
                        report.append("")
                        report.append("| Datei | Schwellenwert | Anzahl Duplikate |")
                        report.append("|-------|--------------|------------------|")
                        for name, data in duplicate_data.items():
                            report.append(f"| {name} | {data['threshold']}% | {data['duplicate_count']} |")
                        report.append("")
            
            # Teil 2: Eindeutige Datensätze, Finale Daten und Prozessberichte
            if include_details:
                # Eindeutige Datensätze
                if unique_files:
                    unique_data = {}
                    for unique_file in unique_files:
                        try:
                            delimiter = detect_delimiter(unique_file)
                            df = pd.read_csv(unique_file, sep=delimiter)
                            base_name = os.path.basename(unique_file).replace("_unique.csv", "")
                            unique_data[base_name] = {
                                "unique_count": len(df),
                                "file_path": unique_file
                            }
                            
                            # Berechne den Prozentsatz der eindeutigen Datensätze
                            if base_name in source_data and source_data[base_name]["records"] > 0:
                                unique_percentage = (len(df) / source_data[base_name]["records"]) * 100
                                unique_data[base_name]["unique_percentage"] = unique_percentage
                        except Exception as e:
                            print(f"Fehler beim Lesen von {unique_file}: {str(e)}")
                    
                    if unique_data:
                        report.append("### Eindeutige Datensätze")
                        report.append("")
                        report.append("| Datei | Anzahl eindeutiger Datensätze | % der Quelldaten |")
                        report.append("|-------|------------------------------|------------------|")
                        for name, data in unique_data.items():
                            percentage = f"{data.get('unique_percentage', 0):.2f}%" if 'unique_percentage' in data else "N/A"
                            report.append(f"| {name} | {data['unique_count']} | {percentage} |")
                        report.append("")
                
                # Zieldaten
                if target_files:
                    target_data = {}
                    for target_file in target_files:
                        try:
                            delimiter = detect_delimiter(target_file)
                            df = pd.read_csv(target_file, sep=delimiter)
                            base_name = os.path.basename(target_file)
                            target_data[base_name] = {
                                "records": len(df),
                                "columns": len(df.columns),
                                "file_size": os.path.getsize(target_file) / 1024  # KB
                            }
                        except Exception as e:
                            print(f"Fehler beim Lesen von {target_file}: {str(e)}")
                    
                    if target_data:
                        report.append("### Zieldaten")
                        report.append("")
                        report.append("| Datei | Datensätze | Spalten | Dateigröße (KB) |")
                        report.append("|-------|------------|---------|-----------------|")
                        for name, data in target_data.items():
                            report.append(f"| {name} | {data['records']} | {data['columns']} | {data['file_size']:.2f} |")
                        report.append("")
                
                # Finale Daten
                if final_files:
                    final_data = {}
                    for final_file in final_files:
                        try:
                            delimiter = detect_delimiter(final_file)
                            df = pd.read_csv(final_file, sep=delimiter)
                            
                            # Extrahiere Handling-Option aus dem Dateinamen
                            handling_match = re.search(r'_final_(\w+)_', final_file)
                            handling = handling_match.group(1) if handling_match else "standard"
                            
                            base_name = os.path.basename(final_file).split("_final")[0]
                            final_data[base_name] = {
                                "handling": handling,
                                "record_count": len(df),
                                "file_size": os.path.getsize(final_file) / 1024,  # KB
                                "file_path": final_file
                            }
                            
                            # Berechne den Prozentsatz im Vergleich zur Quelldatei
                            if base_name in source_data and source_data[base_name]["records"] > 0:
                                percentage = (len(df) / source_data[base_name]["records"]) * 100
                                final_data[base_name]["percentage"] = percentage
                        except Exception as e:
                            print(f"Fehler beim Lesen von {final_file}: {str(e)}")
                    
                    if final_data:
                        report.append("### Finale Daten")
                        report.append("")
                        report.append("| Datei | Behandlung | Datensätze | % der Quelldaten | Dateigröße (KB) |")
                        report.append("|-------|------------|------------|------------------|-----------------|")
                        for name, data in final_data.items():
                            percentage = f"{data.get('percentage', 0):.2f}%" if 'percentage' in data else "N/A"
                            report.append(f"| {name} | {data['handling']} | {data['record_count']} | {percentage} | {data['file_size']:.2f} |")
                        report.append("")
                
                # Prozessberichte
                if report_files:
                    process_data = []
                    for report_file in report_files:
                        try:
                            with open(report_file, 'r') as f:
                                report_data = json.load(f)
                                process_data.append({
                                    "timestamp": report_data.get("timestamp", "unbekannt"),
                                    "status": report_data.get("status", "unbekannt"),
                                    "message": report_data.get("message", "Keine Nachricht"),
                                    "handling_option": report_data.get("handling_option", "unbekannt"),
                                    "total_records": report_data.get("total_records_transferred", 0),
                                    "source_file": report_data.get("source_file", ""),
                                    "target_file": report_data.get("target_file", "")
                                })
                        except Exception as e:
                            print(f"Fehler beim Lesen von {report_file}: {str(e)}")
                    
                    if process_data:
                        report.append("### Prozessberichte")
                        report.append("")
                        for data in process_data:
                            report.append(f"**Bericht vom {data['timestamp']}**")
                            report.append("")
                            report.append(f"- Status: {data['status']}")
                            if data['source_file']:
                                report.append(f"- Quelldatei: {os.path.basename(data['source_file'])}")
                            if data['target_file']:
                                report.append(f"- Zieldatei: {os.path.basename(data['target_file'])}")
                            report.append(f"- Behandlungsoption: {data['handling_option']}")
                            report.append(f"- Übertragene Datensätze: {data['total_records']}")
                            report.append(f"- Nachricht: {data['message']}")
                            report.append("")
            
            # Teil 3: Migrationsstatistik und HTML-Bericht
            # Füge eine Zusammenfassung der Migrationsstatistik hinzu
            if include_details and 'source_data' in locals() and source_data:
                total_source_records = sum(data["records"] for data in source_data.values())
                total_final_records = sum(data["record_count"] for data in final_data.values()) if 'final_data' in locals() and final_data else 0
                total_duplicates = sum(data["duplicate_count"] for data in duplicate_data.values()) if 'duplicate_data' in locals() and duplicate_data else 0
                
                migration_stats = [
                    "### Migrationsstatistik",
                    "",
                    "| Metrik | Wert |",
                    "|--------|------|",
                    f"| Gesamtzahl Quelldatensätze | {total_source_records} |",
                    f"| Gesamtzahl Duplikate | {total_duplicates} |",
                    f"| Gesamtzahl migrierte Datensätze | {total_final_records} |"
                ]
                
                if total_source_records > 0:
                    duplicate_percentage = (total_duplicates / total_source_records) * 100
                    migration_percentage = (total_final_records / total_source_records) * 100
                    migration_stats.append(f"| Duplikatrate | {duplicate_percentage:.2f}% |")
                    migration_stats.append(f"| Migrationsrate | {migration_percentage:.2f}% |")
                
                migration_stats.append("")
                
                # Füge die Migrationsstatistik nach der Zusammenfassung ein
                insert_position = report.index("## Zusammenfassung") + 2
                for i, line in enumerate(migration_stats):
                    report.insert(insert_position + i, line)

            # Erstelle auch eine HTML-Version des Berichts
            try:
                html_content = markdown.markdown("\n".join(report), extensions=['tables'])
                html_file_path = os.path.join(project_directory, f"migration_report_{timestamp}.html")
                
                with open(html_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="utf-8">
                        <title>{report_title}</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; line-height: 1.6; max-width: 1200px; margin: 0 auto; padding: 20px; }}
                            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            tr:nth-child(even) {{ background-color: #f9f9f9; }}
                            h1, h2, h3, h4 {{ color: #333; }}
                        </style>
                    </head>
                    <body>
                        {html_content}
                    </body>
                    </html>
                    """)
                
                return_data = {
                    "status": "success",
                    "message": f"Bericht erfolgreich erstellt",
                    "report_file": report_file_path,
                    "html_file": html_file_path,
                    "statistics": stats,
                    "report_content": "\n".join(report)
                }
            except Exception as e:
                print(f"Fehler bei der HTML-Erstellung: {str(e)}")
                return_data = {
                    "status": "success",
                    "message": f"Bericht erfolgreich erstellt (ohne HTML)",
                    "report_file": report_file_path,
                    "statistics": stats,
                    "report_content": "\n".join(report)
                }

            # Speichere den Bericht als Markdown-Datei
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file_path = os.path.join(project_directory, f"migration_report_{timestamp}.md")
            
            with open(report_file_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(report))

            return return_data
            
        except Exception as e:
            if report_file_path:
                return {"error": f"Fehler bei der Berichterstellung: {str(e)}", "report_file": report_file_path}
            else:
                return {"error": f"Fehler bei der Berichterstellung: {str(e)}"}
    
    def detect_delimiter(file_path):
        """
        Erkennt das in einer CSV-Datei verwendete Trennzeichen.
        Gibt das erkannte Trennzeichen zurück (Komma oder Semikolon).
        """
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            sample = csvfile.read(4096)
            
            # Zähle das Vorkommen gängiger Trennzeichen
            comma_count = sample.count(',')
            semicolon_count = sample.count(';')
            
            # Gib das häufigste Trennzeichen zurück
            if semicolon_count > comma_count:
                return ';'
            return ',' 