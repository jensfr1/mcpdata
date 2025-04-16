from mcp.server.fastmcp import FastMCP
import os
import glob
import pandas as pd
import json
from datetime import datetime

def register(mcp: FastMCP):
    @mcp.tool(name="🚀 Mike - Migration Orchestrator")
    def orchestrate_migration(
        action: str = "start",
        source_file: str = None,
        target_structure: str = None,
        mapping_file: str = None,
        similarity_threshold: int = 90,
        duplicate_handling: str = "ask"
    ) -> dict:
        """
        Orchestriert den gesamten Migrationsprozess und führt den Benutzer durch alle Schritte
        
        Args:
            action: Aktion, die ausgeführt werden soll ("start", "map", "validate", "transfer", "report")
            source_file: Pfad zur Quelldatei
            target_structure: Pfad zur Zielstruktur-Datei
            mapping_file: Pfad zur Mapping-Datei
            similarity_threshold: Schwellenwert für die Ähnlichkeitserkennung bei Duplikaten (0-100)
            duplicate_handling: Wie mit Duplikaten umgegangen werden soll ("ask", "skip", "overwrite", "append")
            
        Returns:
            Ein Dictionary mit dem Status des Migrationsprozesses und den nächsten Schritten
        """
        try:
            # Initialisiere die Antwort
            response = {
                "status": "in_progress",
                "action": action,
                "next_steps": [],
                "message": ""
            }
            
            # Aktion: Start des Migrationsprozesses
            if action == "start":
                response["message"] = (
                    "# 🚀 Willkommen beim Migrationsprozess!\n\n"
                    "Ich bin Mike, Ihr Migrations-Orchestrator. Ich werde Sie durch den gesamten Prozess führen.\n\n"
                    "## Wie kann ich Ihnen helfen?\n\n"
                    "1. **Neue Migration starten**: Geben Sie den Pfad zur Quelldatei an\n"
                    "2. **Vorhandene Migration fortsetzen**: Geben Sie das Verzeichnis an, in dem sich die Migrationsdateien befinden\n\n"
                    "Bitte geben Sie den Pfad zur Quelldatei oder zum Migrationsverzeichnis an."
                )
                response["next_steps"] = ["Pfad zur Quelldatei angeben", "Pfad zum Migrationsverzeichnis angeben"]
                return response
            
            # Überprüfe, ob die Quelldatei existiert
            if source_file and not os.path.exists(source_file):
                return {
                    "status": "error",
                    "message": f"Die angegebene Quelldatei '{source_file}' wurde nicht gefunden. Bitte überprüfen Sie den Pfad und versuchen Sie es erneut.",
                    "next_steps": ["Korrekten Pfad zur Quelldatei angeben"]
                }
            
            # Aktion: Quelldatei analysieren und Mapping vorbereiten
            if action == "analyze":
                if not source_file:
                    return {
                        "status": "error",
                        "message": "Keine Quelldatei angegeben. Bitte geben Sie den Pfad zur Quelldatei an.",
                        "next_steps": ["Pfad zur Quelldatei angeben"]
                    }
                
                # Analysiere die Quelldatei
                file_info = analyze_source_file(source_file)
                
                # Suche nach vorhandenen Mapping-Dateien im selben Verzeichnis
                source_dir = os.path.dirname(source_file)
                existing_mappings = glob.glob(os.path.join(source_dir, "*_field_mapping*.json"))
                
                if existing_mappings:
                    mapping_options = "\n".join([f"- {os.path.basename(m)}" for m in existing_mappings])
                    response["message"] = (
                        f"# 📊 Quelldatei analysiert\n\n"
                        f"Ich habe die Quelldatei '{os.path.basename(source_file)}' analysiert:\n\n"
                        f"- **Anzahl Datensätze**: {file_info['records']}\n"
                        f"- **Anzahl Spalten**: {file_info['columns']}\n"
                        f"- **Spalten**: {', '.join(file_info['column_names'])}\n\n"
                        f"Ich habe folgende vorhandene Mapping-Dateien gefunden:\n\n{mapping_options}\n\n"
                        f"Möchten Sie eine vorhandene Mapping-Datei verwenden oder ein neues Mapping erstellen?"
                    )
                    response["existing_mappings"] = existing_mappings
                    response["next_steps"] = ["Vorhandenes Mapping verwenden", "Neues Mapping erstellen"]
                else:
                    response["message"] = (
                        f"# 📊 Quelldatei analysiert\n\n"
                        f"Ich habe die Quelldatei '{os.path.basename(source_file)}' analysiert:\n\n"
                        f"- **Anzahl Datensätze**: {file_info['records']}\n"
                        f"- **Anzahl Spalten**: {file_info['columns']}\n"
                        f"- **Spalten**: {', '.join(file_info['column_names'])}\n\n"
                        f"Ich habe keine vorhandenen Mapping-Dateien gefunden. Wir müssen ein neues Mapping erstellen.\n\n"
                        f"Bitte geben Sie den Pfad zur Zielstruktur-Datei an oder beschreiben Sie die Zielstruktur."
                    )
                    response["next_steps"] = ["Pfad zur Zielstruktur angeben", "Zielstruktur beschreiben"]
                
                response["file_info"] = file_info
                return response
            
            # Aktion: Mapping erstellen oder aktualisieren
            if action == "map":
                if not source_file:
                    return {
                        "status": "error",
                        "message": "Keine Quelldatei angegeben. Bitte geben Sie den Pfad zur Quelldatei an.",
                        "next_steps": ["Pfad zur Quelldatei angeben"]
                    }
                
                # Hier würden wir Mathias aufrufen, um das Mapping zu erstellen
                response["message"] = (
                    "# 🔄 Mapping-Prozess\n\n"
                    f"Ich werde jetzt Mathias bitten, ein Mapping für die Datei '{os.path.basename(source_file)}' zu erstellen.\n\n"
                    f"Bitte verwenden Sie das Tool '🔄 Mathias - Create Field Mapping' mit folgenden Parametern:\n\n"
                    f"- **source_file**: {source_file}\n"
                    f"- **target_structure**: {target_structure if target_structure else 'Bitte angeben'}\n\n"
                    f"Sobald das Mapping erstellt wurde, können wir mit der Validierung fortfahren."
                )
                response["next_steps"] = ["Mathias aufrufen", "Mapping manuell anpassen"]
                return response
            
            # Aktion: Daten validieren und Duplikate prüfen
            if action == "validate":
                if not source_file or not mapping_file:
                    missing = []
                    if not source_file:
                        missing.append("Quelldatei")
                    if not mapping_file:
                        missing.append("Mapping-Datei")
                    
                    return {
                        "status": "error",
                        "message": f"Fehlende Informationen: {', '.join(missing)}. Bitte geben Sie alle erforderlichen Informationen an.",
                        "next_steps": ["Fehlende Informationen angeben"]
                    }
                
                # Hier würden wir James aufrufen, um die Daten zu validieren
                response["message"] = (
                    "# 🔍 Validierung und Duplikatprüfung\n\n"
                    f"Ich werde jetzt James bitten, die gemappten Daten zu validieren und auf Duplikate zu prüfen.\n\n"
                    f"Bitte verwenden Sie das Tool '🔍 James - Validate and Check Duplicates' mit folgenden Parametern:\n\n"
                    f"- **mapped_file_path**: Die gemappte Datei von Mathias\n"
                    f"- **target_data_file**: Die Zieldatei\n"
                    f"- **similarity_threshold**: {similarity_threshold}\n"
                    f"- **duplicate_handling**: {duplicate_handling}\n\n"
                    f"Sobald die Validierung abgeschlossen ist, können wir mit der Übertragung fortfahren."
                )
                response["next_steps"] = ["James aufrufen", "Similarity Threshold anpassen", "Duplicate Handling ändern"]
                return response
            
            # Aktion: Daten übertragen
            if action == "transfer":
                # Hier würden wir James aufrufen, um die Daten zu übertragen
                response["message"] = (
                    "# 📤 Datenübertragung\n\n"
                    "Ich werde jetzt James bitten, die validierten Daten zu übertragen.\n\n"
                    "Bitte verwenden Sie das Tool '🔄 James - Process Duplicates' mit folgenden Parametern:\n\n"
                    "- **mapped_file_path**: Die gemappte Datei\n"
                    "- **target_path**: Der Zielpfad\n"
                    f"- **handling_option**: {duplicate_handling}\n\n"
                    "Sobald die Übertragung abgeschlossen ist, können wir einen Bericht erstellen."
                )
                response["next_steps"] = ["James aufrufen", "Duplicate Handling ändern"]
                return response
            
            # Aktion: Bericht erstellen
            if action == "report":
                # Hier würden wir Gina aufrufen, um einen Bericht zu erstellen
                source_dir = os.path.dirname(source_file) if source_file else "."
                
                response["message"] = (
                    "# 📊 Berichterstellung\n\n"
                    "Ich werde jetzt Gina bitten, einen umfassenden Bericht über den Migrationsprozess zu erstellen.\n\n"
                    "Bitte verwenden Sie das Tool '📊 Gina - Generate Migration Report' mit folgenden Parametern:\n\n"
                    f"- **project_directory**: {source_dir}\n"
                    "- **report_title**: Migration Report\n"
                    "- **include_details**: True\n\n"
                    "Nach der Berichterstellung ist der Migrationsprozess abgeschlossen."
                )
                response["next_steps"] = ["Gina aufrufen", "Prozess abschließen"]
                return response
            
            # Aktion: Prozess abschließen
            if action == "complete":
                response["status"] = "complete"
                response["message"] = (
                    "# 🎉 Migrationsprozess abgeschlossen\n\n"
                    "Der Migrationsprozess wurde erfolgreich abgeschlossen. Hier ist eine Zusammenfassung:\n\n"
                    f"- **Quelldatei**: {os.path.basename(source_file) if source_file else 'Nicht angegeben'}\n"
                    f"- **Mapping-Datei**: {os.path.basename(mapping_file) if mapping_file else 'Nicht angegeben'}\n"
                    f"- **Similarity Threshold**: {similarity_threshold}%\n"
                    f"- **Duplicate Handling**: {duplicate_handling}\n\n"
                    "Vielen Dank für die Nutzung des Migrationsprozesses. Wenn Sie eine weitere Migration durchführen möchten, starten Sie einfach einen neuen Prozess."
                )
                response["next_steps"] = ["Neue Migration starten"]
                return response
            
            # Unbekannte Aktion
            return {
                "status": "error",
                "message": f"Unbekannte Aktion: {action}. Gültige Aktionen sind: start, analyze, map, validate, transfer, report, complete.",
                "next_steps": ["Gültige Aktion angeben"]
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Fehler bei der Orchestrierung: {str(e)}",
                "next_steps": ["Prozess neu starten"]
            }
    
    def analyze_source_file(file_path):
        """Analysiert eine Quelldatei und gibt Informationen zurück"""
        try:
            # Erkennen des Trennzeichens
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                sample = csvfile.read(4096)
                
                # Zähle das Vorkommen gängiger Trennzeichen
                comma_count = sample.count(',')
                semicolon_count = sample.count(';')
                
                # Bestimme das wahrscheinlichste Trennzeichen
                delimiter = ';' if semicolon_count > comma_count else ','
            
            # Lese die Datei mit dem erkannten Trennzeichen
            df = pd.read_csv(file_path, sep=delimiter)
            
            return {
                "records": len(df),
                "columns": len(df.columns),
                "column_names": df.columns.tolist(),
                "delimiter": delimiter,
                "file_size": os.path.getsize(file_path) / 1024  # KB
            }
        except Exception as e:
            return {
                "error": f"Fehler bei der Analyse der Quelldatei: {str(e)}"
            }
    
    @mcp.tool(name="🔄 Mike - Continue Migration")
    def continue_migration(
        source_file: str,
        current_step: str,
        mapping_file: str = None,
        target_file: str = None,
        similarity_threshold: int = 90,
        duplicate_handling: str = "ask"
    ) -> dict:
        """
        Setzt den Migrationsprozess an einem bestimmten Schritt fort
        
        Args:
            source_file: Pfad zur Quelldatei
            current_step: Aktueller Schritt im Prozess ("analyze", "map", "validate", "transfer", "report")
            mapping_file: Pfad zur Mapping-Datei (optional)
            target_file: Pfad zur Zieldatei (optional)
            similarity_threshold: Schwellenwert für die Ähnlichkeitserkennung bei Duplikaten (0-100)
            duplicate_handling: Wie mit Duplikaten umgegangen werden soll ("ask", "skip", "overwrite", "append")
            
        Returns:
            Ein Dictionary mit dem Status des Migrationsprozesses und den nächsten Schritten
        """
        return orchestrate_migration(
            action=current_step,
            source_file=source_file,
            target_structure=target_file,
            mapping_file=mapping_file,
            similarity_threshold=similarity_threshold,
            duplicate_handling=duplicate_handling
        )
    
    @mcp.tool(name="📋 Mike - Migration Status")
    def get_migration_status(project_directory: str) -> dict:
        """
        Gibt den aktuellen Status eines Migrationsprojekts zurück
        
        Args:
            project_directory: Verzeichnis des Migrationsprojekts
            
        Returns:
            Ein Dictionary mit dem Status des Migrationsprojekts
        """
        try:
            if not os.path.exists(project_directory):
                return {
                    "status": "error",
                    "message": f"Das angegebene Verzeichnis '{project_directory}' wurde nicht gefunden."
                }
            
            # Suche nach relevanten Dateien
            source_files = glob.glob(os.path.join(project_directory, "*.csv"))
            mapping_files = glob.glob(os.path.join(project_directory, "*_field_mapping*.json"))
            mapped_files = glob.glob(os.path.join(project_directory, "*_mapped.csv"))
            duplicate_files = glob.glob(os.path.join(project_directory, "*_duplicates_*.csv"))
            unique_files = glob.glob(os.path.join(project_directory, "*_unique.csv"))
            final_files = glob.glob(os.path.join(project_directory, "*_final*.csv"))
            report_files = glob.glob(os.path.join(project_directory, "*_report_*.json"))
            
            # Bestimme den aktuellen Status
            current_step = "start"
            if source_files:
                current_step = "analyze"
            if mapping_files:
                current_step = "map"
            if mapped_files:
                current_step = "validate"
            if duplicate_files or unique_files:
                current_step = "transfer"
            if final_files:
                current_step = "report"
            if report_files:
                current_step = "complete"
            
            # Erstelle eine Zusammenfassung
            summary = {
                "project_directory": project_directory,
                "current_step": current_step,
                "files": {
                    "source_files": [os.path.basename(f) for f in source_files],
                    "mapping_files": [os.path.basename(f) for f in mapping_files],
                    "mapped_files": [os.path.basename(f) for f in mapped_files],
                    "duplicate_files": [os.path.basename(f) for f in duplicate_files],
                    "unique_files": [os.path.basename(f) for f in unique_files],
                    "final_files": [os.path.basename(f) for f in final_files],
                    "report_files": [os.path.basename(f) for f in report_files]
                }
            }
            
            # Erstelle eine benutzerfreundliche Nachricht
            message = (
                f"# 📋 Status des Migrationsprojekts\n\n"
                f"Verzeichnis: {project_directory}\n\n"
                f"## Aktueller Schritt: {current_step.upper()}\n\n"
                f"### Gefundene Dateien:\n\n"
            )
            
            for file_type, files in summary["files"].items():
                if files:
                    message += f"**{file_type}**:\n"
                    for file in files:
                        message += f"- {file}\n"
                    message += "\n"
            
            message += (
                f"## Nächste Schritte\n\n"
                f"Um den Prozess fortzusetzen, verwenden Sie das Tool '🔄 Mike - Continue Migration' mit folgenden Parametern:\n\n"
                f"- **source_file**: {source_files[0] if source_files else 'Bitte angeben'}\n"
                f"- **current_step**: {current_step}\n"
                f"- **mapping_file**: {mapping_files[0] if mapping_files else 'Optional'}\n"
                f"- **target_file**: {'Bitte angeben' if current_step == 'analyze' else 'Optional'}\n"
            )
            
            return {
                "status": "success",
                "message": message,
                "summary": summary
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Fehler bei der Statusabfrage: {str(e)}"
            }
    
    @mcp.tool(name="💬 Mike - Migration Workflow")
    def migration_workflow(
        user_input: str,
        current_state: str = "start",
        context: dict = None
    ) -> dict:
        """
        Führt einen vollständigen Migrations-Workflow durch und koordiniert alle Agenten

        Args:
            user_input: Die Eingabe des Benutzers
            current_state: Der aktuelle Zustand des Workflows
            context: Kontext-Informationen aus vorherigen Interaktionen

        Returns:
            Ein Dictionary mit der Antwort und dem nächsten Zustand
        """
        try:
            # Initialisiere den Kontext, wenn er nicht existiert
            if context is None:
                context = {}

            # Initialisiere die Antwort
            response = {
                "message": "",
                "next_state": current_state,
                "context": context,
                "actions": [],
                "workflow_status": {
                    "steward": "pending",
                    "emma": "pending",
                    "oskar": "pending",
                    "mathias": "pending",
                    "james": "pending",
                    "gina": "pending"
                }
            }

            # Aktualisiere den Workflow-Status aus dem Kontext, falls vorhanden
            if "workflow_status" in context:
                response["workflow_status"] = context["workflow_status"]

            # Zustand: Start des Migrationsprozesses
            if current_state == "start":
                response["message"] = (
                    "# 🚀 Willkommen beim Migrationsprozess!\n\n"
                    "Ich bin Mike, Ihr Migrations-Orchestrator. Ich werde Sie durch den gesamten Prozess führen und "
                    "die verschiedenen Agenten koordinieren.\n\n"
                    "## Workflow-Übersicht:\n"
                    "1. **Steward** - Initialisierung und Routing\n"
                    "2. **Emma** - Datenprofilierung\n"
                    "3. **Oskar** - Datenbereinigung\n"
                    "4. **Mathias** - Feldmapping\n"
                    "5. **James** - Datenmigration\n"
                    "6. **Gina** - Berichterstellung\n\n"
                    "Um zu beginnen, benötige ich den Pfad zu Ihrer Quelldatei (CSV)."
                )
                response["next_state"] = "get_source_file"
                response["actions"] = ["Pfad zur Quelldatei angeben", "Projekt-Verzeichnis angeben"]
                return response

            # Zustand: Quelldatei erfassen
            elif current_state == "get_source_file":
                # Prüfe, ob der Benutzer einen Pfad angegeben hat
                if os.path.exists(user_input):
                    # Prüfe, ob es sich um eine Datei oder ein Verzeichnis handelt
                    if os.path.isfile(user_input):
                        # Es ist eine Datei, wahrscheinlich die Quelldatei
                        context["source_file"] = user_input

                        # Aktualisiere den Workflow-Status
                        response["workflow_status"]["steward"] = "in_progress"

                        response["message"] = (
                            f"# 📁 Quelldatei gefunden\n\n"
                            f"Ich habe die Quelldatei '{os.path.basename(user_input)}' gefunden.\n\n"
                            f"Als nächstes werde ich Steward bitten, die Datei zu analysieren und den Workflow zu initialisieren.\n\n"
                            f"Bitte verwenden Sie das Tool '🧠 Steward - Data Steward' mit folgenden Parametern:\n\n"
                            f"- **request**: \"Analyze and initialize migration workflow\"\n"
                            f"- **data_source**: \"{user_input}\"\n\n"
                            f"Sobald Steward die Analyse abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                        )
                        response["next_state"] = "steward_analysis"
                        response["actions"] = ["Steward aufrufen", "Andere Datei wählen"]
                    else:
                        # Es ist ein Verzeichnis, prüfe auf vorhandene Migrationsdateien
                        context["project_directory"] = user_input

                        # Suche nach CSV-Dateien im Verzeichnis
                        csv_files = glob.glob(os.path.join(user_input, "*.csv"))

                        if csv_files:
                            csv_list = "\n".join([f"- {os.path.basename(f)}" for f in csv_files])
                            response["message"] = (
                                f"# 📁 Projekt-Verzeichnis gefunden\n\n"
                                f"Ich habe folgende CSV-Dateien im Verzeichnis '{user_input}' gefunden:\n\n"
                                f"{csv_list}\n\n"
                                f"Bitte wählen Sie eine Datei aus, die als Quelldatei verwendet werden soll."
                            )
                            response["next_state"] = "select_source_file"
                            response["context"]["csv_files"] = csv_files
                            response["actions"] = [os.path.basename(f) for f in csv_files[:5]]
                            if len(csv_files) > 5:
                                response["actions"].append("Weitere Dateien anzeigen")
                        else:
                            response["message"] = (
                                f"# ⚠️ Keine CSV-Dateien gefunden\n\n"
                                f"Im Verzeichnis '{user_input}' wurden keine CSV-Dateien gefunden.\n\n"
                                f"Bitte geben Sie den Pfad zu einer CSV-Datei an oder wählen Sie ein anderes Verzeichnis."
                            )
                            response["next_state"] = "get_source_file"
                            response["actions"] = ["Pfad zur Quelldatei angeben", "Anderes Verzeichnis wählen"]
                else:
                    # Kein gültiger Pfad, frage erneut
                    response["message"] = (
                        "# ⚠️ Pfad nicht gefunden\n\n"
                        f"Der angegebene Pfad '{user_input}' existiert nicht.\n\n"
                        f"Bitte geben Sie einen gültigen Pfad zu einer CSV-Datei oder einem Verzeichnis an."
                    )
                    response["next_state"] = "get_source_file"
                    response["actions"] = ["Pfad zur Quelldatei angeben", "Projekt-Verzeichnis angeben"]

                return response

            # Zustand: Quelldatei aus Verzeichnis auswählen
            elif current_state == "select_source_file":
                # Prüfe, ob die ausgewählte Datei in der Liste der CSV-Dateien ist
                csv_files = context.get("csv_files", [])
                selected_file = None

                for file in csv_files:
                    if os.path.basename(file).lower() == user_input.lower() or user_input.lower() in file.lower():
                        selected_file = file
                        break

                if selected_file:
                    context["source_file"] = selected_file

                    # Aktualisiere den Workflow-Status
                    response["workflow_status"]["steward"] = "in_progress"

                    response["message"] = (
                        f"# 📁 Quelldatei ausgewählt\n\n"
                        f"Sie haben die Datei '{os.path.basename(selected_file)}' ausgewählt.\n\n"
                        f"Als nächstes werde ich Steward bitten, die Datei zu analysieren und den Workflow zu initialisieren.\n\n"
                        f"Bitte verwenden Sie das Tool '🧠 Steward - Data Steward' mit folgenden Parametern:\n\n"
                        f"- **request**: \"Analyze and initialize migration workflow\"\n"
                        f"- **data_source**: \"{selected_file}\"\n\n"
                        f"Sobald Steward die Analyse abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "steward_analysis"
                    response["actions"] = ["Steward aufrufen", "Andere Datei wählen"]
                else:
                    # Datei nicht gefunden, frage erneut
                    csv_list = "\n".join([f"- {os.path.basename(f)}" for f in csv_files])
                    response["message"] = (
                        f"# ⚠️ Datei nicht gefunden\n\n"
                        f"Die angegebene Datei '{user_input}' wurde nicht gefunden.\n\n"
                        f"Bitte wählen Sie eine der folgenden Dateien:\n\n{csv_list}"
                    )
                    response["next_state"] = "select_source_file"
                    response["actions"] = [os.path.basename(f) for f in csv_files[:5]]

                return response

            # Zustand: Steward-Analyse verarbeiten
            elif current_state == "steward_analysis":
                # Hier erwarten wir, dass der Benutzer die Ergebnisse von Steward mitteilt
                # Wir könnten versuchen, die JSON-Antwort zu parsen, aber das ist komplex
                # Stattdessen fragen wir nach dem nächsten Schritt basierend auf Stewards Empfehlung

                # Aktualisiere den Workflow-Status
                response["workflow_status"]["steward"] = "completed"
                response["workflow_status"]["emma"] = "in_progress"

                # Versuche, die Ergebnisse von Steward zu parsen (optional)
                try:
                    steward_results = json.loads(user_input)
                    context["steward_results"] = steward_results
                    # Hier könnten wir die Ergebnisse von Steward verwenden, um die Nachricht anzupassen
                except json.JSONDecodeError:
                    print("Warnung: Konnte Steward-Ergebnisse nicht als JSON parsen.")

                response["message"] = (
                    "# 📊 Datenprofilierung mit Emma\n\n"
                    "Basierend auf Stewards Analyse sollten wir als nächstes eine detaillierte Datenprofilierung durchführen.\n\n"
                    f"Bitte verwenden Sie das Tool '🔍 Emma - Profiling Agent' mit folgenden Parametern:\n\n"
                    f"- **file_path**: \"{context['source_file']}\"\n"
                    f"- **analyze_duplicates**: True\n"
                    f"- **similarity_threshold**: 90\n"
                    f"- **ai_analysis**: True (optional)\n\n"
                    f"Emma wird die Daten analysieren und potenzielle Probleme identifizieren. "
                    f"Sobald Emma die Analyse abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                )
                response["next_state"] = "emma_profiling"
                response["actions"] = ["Emma aufrufen", "Profilierungsparameter anpassen"]

                return response

            # Zustand: Emma-Profilierung verarbeiten
            elif current_state == "emma_profiling":
                # Hier erwarten wir, dass der Benutzer die Ergebnisse von Emma mitteilt

                # Aktualisiere den Workflow-Status
                response["workflow_status"]["emma"] = "completed"

                # Versuche, die Ergebnisse von Emma zu parsen (optional)
                try:
                    emma_results = json.loads(user_input)
                    context["emma_results"] = emma_results

                    # Prüfe, ob Oskar benötigt wird (basierend auf Emmas Ergebnissen)
                    needs_oskar = False
                    if "oskar_instructions" in emma_results and emma_results["oskar_instructions"].get("cleaning_tasks"):
                        needs_oskar = True

                    if needs_oskar:
                        response["workflow_status"]["oskar"] = "in_progress"
                        response["message"] = (
                            "# 🧹 Datenbereinigung mit Oskar\n\n"
                            "Emma hat die Datenprofilierung abgeschlossen und einige Bereiche identifiziert, die bereinigt werden müssen.\n\n"
                            f"Bitte verwenden Sie das Tool '🧹 Oskar - Cleaning Agent' mit folgenden Parametern:\n\n"
                            f"- **file_path**: \"{context['source_file']}\"\n"
                            f"- **emma_results**: (Fügen Sie hier die JSON-Ergebnisse von Emma ein)\n"
                            f"- **auto_apply**: False (oder True, wenn Sie die Bereinigung automatisch durchführen möchten)\n\n"
                            f"Oskar wird die Daten bereinigen. "
                            f"Sobald Oskar die Bereinigung abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                        )
                        response["next_state"] = "oskar_cleaning"
                        response["actions"] = ["Oskar aufrufen", "Bereinigungsparameter anpassen"]
                    else:
                        # Oskar wird übersprungen
                        response["workflow_status"]["oskar"] = "skipped"
                        response["workflow_status"]["mathias"] = "in_progress"
                        response["message"] = (
                            "# 🔄 Feldmapping mit Mathias\n\n"
                            "Emma hat die Datenprofilierung abgeschlossen und keine Bereinigung ist erforderlich.\n\n"
                            "Der nächste Schritt ist das Feldmapping mit Mathias.\n\n"
                            "Benötigen wir eine spezifische Zielstruktur? Bitte geben Sie den Pfad zur Zielstruktur-Datei an oder beschreiben Sie die Zielstruktur."
                        )
                        response["next_state"] = "prepare_mapping"
                        response["actions"] = ["Zielstruktur-Datei angeben", "Zielstruktur beschreiben"]

                except json.JSONDecodeError:
                    print("Warnung: Konnte Emma-Ergebnisse nicht als JSON parsen. Gehe davon aus, dass Oskar benötigt wird.")
                    # Fallback, wenn das Parsen fehlschlägt
                    response["workflow_status"]["oskar"] = "in_progress"
                    response["message"] = (
                        "# 🧹 Datenbereinigung mit Oskar\n\n"
                        "Emma hat die Datenprofilierung abgeschlossen. Als nächstes sollten wir die Daten mit Oskar bereinigen.\n\n"
                        f"Bitte verwenden Sie das Tool '🧹 Oskar - Cleaning Agent' mit folgenden Parametern:\n\n"
                        f"- **file_path**: \"{context['source_file']}\"\n"
                        f"- **auto_apply**: False\n\n"
                        f"Sobald Oskar die Bereinigung abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "oskar_cleaning"
                    response["actions"] = ["Oskar aufrufen", "Bereinigungsparameter anpassen"]

                return response

            # Zustand: Oskar-Bereinigung verarbeiten
            elif current_state == "oskar_cleaning":
                # Hier erwarten wir, dass der Benutzer die Ergebnisse von Oskar mitteilt

                # Aktualisiere den Workflow-Status
                response["workflow_status"]["oskar"] = "completed"
                response["workflow_status"]["mathias"] = "in_progress"

                # Versuche, die Ergebnisse von Oskar zu parsen (optional)
                try:
                    oskar_results = json.loads(user_input)
                    context["oskar_results"] = oskar_results
                    # Speichere den Pfad zur bereinigten Datei
                    if "output_file" in oskar_results:
                        context["cleaned_file"] = oskar_results["output_file"]
                except json.JSONDecodeError:
                    print("Warnung: Konnte Oskar-Ergebnisse nicht als JSON parsen.")
                    # Versuche, den Pfad zur bereinigten Datei zu erraten
                    cleaned_file_guess = context['source_file'].replace(".csv", "_cleaned.csv")
                    if os.path.exists(cleaned_file_guess):
                        context["cleaned_file"] = cleaned_file_guess

                response["message"] = (
                    "# 🔄 Feldmapping mit Mathias\n\n"
                    "Oskar hat die Datenbereinigung abgeschlossen.\n\n"
                    "Der nächste Schritt ist das Feldmapping mit Mathias.\n\n"
                    "Benötigen wir eine spezifische Zielstruktur? Bitte geben Sie den Pfad zur Zielstruktur-Datei an oder beschreiben Sie die Zielstruktur."
                )
                response["next_state"] = "prepare_mapping"
                response["actions"] = ["Zielstruktur-Datei angeben", "Zielstruktur beschreiben"]

                return response

            # Zustand: Mapping vorbereiten
            elif current_state == "prepare_mapping":
                # Prüfe, ob der Benutzer einen Pfad angegeben hat
                if os.path.exists(user_input):
                    # Es ist eine Datei, wahrscheinlich die Zielstruktur
                    context["target_structure"] = user_input

                    response["message"] = (
                        "# 🔄 Feldmapping mit Mathias\n\n"
                        f"Ich habe die Zielstruktur-Datei '{os.path.basename(user_input)}' gefunden.\n\n"
                        f"Bitte verwenden Sie das Tool '🔄 Mathias - Create Field Mapping' mit folgenden Parametern:\n\n"
                        f"- **source_file**: \"{context.get('cleaned_file', context['source_file'])}\"\n"
                        f"- **target_structure**: \"{user_input}\"\n\n"
                        f"Mathias wird ein Mapping zwischen der Quelldatei und der Zielstruktur erstellen. "
                        f"Sobald Mathias das Mapping erstellt hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "mathias_mapping"
                    response["actions"] = ["Mathias aufrufen", "Mapping-Parameter anpassen"]
                else:
                    # Benutzer möchte die Zielstruktur beschreiben
                    context["target_description"] = user_input

                    response["message"] = (
                        "# 🔄 Feldmapping mit Mathias\n\n"
                        f"Danke für die Beschreibung der Zielstruktur.\n\n"
                        f"Bitte verwenden Sie das Tool '🔄 Mathias - Create Field Mapping' mit folgenden Parametern:\n\n"
                        f"- **source_file**: \"{context.get('cleaned_file', context['source_file'])}\"\n"
                        f"- **target_description**: \"{user_input}\"\n\n"
                        f"Mathias wird basierend auf Ihrer Beschreibung ein Mapping erstellen. "
                        f"Sobald Mathias das Mapping erstellt hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "mathias_mapping"
                    response["actions"] = ["Mathias aufrufen", "Mapping-Parameter anpassen"]

                return response

            # Zustand: Mathias-Mapping verarbeiten
            elif current_state == "mathias_mapping":
                # Hier erwarten wir, dass der Benutzer die Ergebnisse von Mathias mitteilt

                # Aktualisiere den Workflow-Status
                response["workflow_status"]["mathias"] = "completed"
                response["workflow_status"]["james"] = "in_progress"

                # Versuche, die Ergebnisse von Mathias zu parsen (optional)
                try:
                    mathias_results = json.loads(user_input)
                    context["mathias_results"] = mathias_results
                    if "mapping_file" in mathias_results:
                         context["mapping_file"] = mathias_results["mapping_file"]
                except json.JSONDecodeError:
                    print("Warnung: Konnte Mathias-Ergebnisse nicht als JSON parsen.")

                # Frage nach der Mapping-Datei, falls sie nicht im Kontext ist
                if "mapping_file" not in context:
                    response["message"] = (
                        "# 📤 Datenmigration mit James\n\n"
                        "Mathias hat das Feldmapping abgeschlossen. Als nächstes müssen wir die Datenmigration durchführen.\n\n"
                        "Bitte geben Sie den Pfad zur Mapping-Datei an, die Mathias erstellt hat."
                    )
                    response["next_state"] = "get_mapping_file"
                    response["actions"] = ["Pfad zur Mapping-Datei angeben"]
                else:
                    # Mapping-Datei ist bekannt, fahre fort mit James
                    response["message"] = (
                        "# 📤 Datenmigration mit James\n\n"
                        f"Ich habe die Mapping-Datei '{os.path.basename(context['mapping_file'])}' gefunden.\n\n"
                        f"Bitte verwenden Sie das Tool '📤 James - Apply Field Mapping' mit folgenden Parametern:\n\n"
                        f"- **source_file**: \"{context.get('cleaned_file', context['source_file'])}\"\n"
                        f"- **mapping_file**: \"{context['mapping_file']}\"\n"
                        f"- **handling_option**: \"create_new\" (oder eine andere Option nach Bedarf)\n\n"
                        f"James wird die Daten gemäß dem Mapping migrieren. "
                        f"Sobald James die Migration abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "james_migration"
                    response["actions"] = ["James aufrufen", "Migrations-Parameter anpassen"]

                return response

            # Zustand: Mapping-Datei erfassen
            elif current_state == "get_mapping_file":
                if os.path.exists(user_input):
                    # Mapping-Datei gefunden
                    context["mapping_file"] = user_input

                    response["message"] = (
                        "# 📤 Datenmigration mit James\n\n"
                        f"Ich habe die Mapping-Datei '{os.path.basename(user_input)}' gefunden.\n\n"
                        f"Bitte verwenden Sie das Tool '📤 James - Apply Field Mapping' mit folgenden Parametern:\n\n"
                        f"- **source_file**: \"{context.get('cleaned_file', context['source_file'])}\"\n"
                        f"- **mapping_file**: \"{user_input}\"\n"
                        f"- **handling_option**: \"create_new\" (oder eine andere Option nach Bedarf)\n\n"
                        f"James wird die Daten gemäß dem Mapping migrieren. "
                        f"Sobald James die Migration abgeschlossen hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "james_migration"
                    response["actions"] = ["James aufrufen", "Migrations-Parameter anpassen"]
                else:
                    # Datei nicht gefunden
                    response["message"] = (
                        "# ⚠️ Datei nicht gefunden\n\n"
                        f"Die angegebene Datei '{user_input}' wurde nicht gefunden.\n\n"
                        f"Bitte geben Sie einen gültigen Pfad zur Mapping-Datei an."
                    )
                    response["next_state"] = "get_mapping_file"
                    response["actions"] = ["Pfad zur Mapping-Datei angeben"]

                return response

            # Zustand: James-Migration verarbeiten
            elif current_state == "james_migration":
                # Hier erwarten wir, dass der Benutzer die Ergebnisse von James mitteilt

                # Aktualisiere den Workflow-Status
                response["workflow_status"]["james"] = "completed"
                response["workflow_status"]["gina"] = "in_progress"

                # Versuche, die Ergebnisse von James zu parsen (optional)
                try:
                    james_results = json.loads(user_input)
                    context["james_results"] = james_results
                    # Speichere den Pfad zur migrierten Datei
                    if "output_file" in james_results:
                        context["migrated_file"] = james_results["output_file"]
                        # Bestimme das Projektverzeichnis
                        context["migration_directory"] = os.path.dirname(james_results["output_file"])
                except json.JSONDecodeError:
                    print("Warnung: Konnte James-Ergebnisse nicht als JSON parsen.")

                # Frage nach dem Migrationsverzeichnis, falls es nicht im Kontext ist
                if "migration_directory" not in context:
                    response["message"] = (
                        "# 📊 Berichterstellung mit Gina\n\n"
                        "James hat die Datenmigration abgeschlossen. Als letzten Schritt erstellen wir einen Bericht.\n\n"
                        "Bitte geben Sie das Verzeichnis an, in dem die Migrationsdateien gespeichert sind."
                    )
                    response["next_state"] = "get_migration_directory"
                    response["actions"] = ["Pfad zum Migrations-Verzeichnis angeben"]
                else:
                    # Migrationsverzeichnis ist bekannt, fahre fort mit Gina
                    response["message"] = (
                        "# 📊 Berichterstellung mit Gina\n\n"
                        f"Ich habe das Migrations-Verzeichnis '{context['migration_directory']}' gefunden.\n\n"
                        f"Bitte verwenden Sie das Tool '📊 Gina - Generate Migration Report' mit folgenden Parametern:\n\n"
                        f"- **project_directory**: \"{context['migration_directory']}\"\n"
                        f"- **report_title**: \"Migrationsbericht für {os.path.basename(context['source_file'])}\"\n"
                        f"- **include_details**: True\n\n"
                        f"Gina wird einen umfassenden Bericht über den Migrationsprozess erstellen. "
                        f"Sobald Gina den Bericht erstellt hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "gina_report"
                    response["actions"] = ["Gina aufrufen", "Berichts-Parameter anpassen"]

                return response

            # Zustand: Migrations-Verzeichnis erfassen
            elif current_state == "get_migration_directory":
                if os.path.exists(user_input) and os.path.isdir(user_input):
                    # Verzeichnis gefunden
                    context["migration_directory"] = user_input

                    response["message"] = (
                        "# 📊 Berichterstellung mit Gina\n\n"
                        f"Ich habe das Migrations-Verzeichnis '{user_input}' gefunden.\n\n"
                        f"Bitte verwenden Sie das Tool '📊 Gina - Generate Migration Report' mit folgenden Parametern:\n\n"
                        f"- **project_directory**: \"{user_input}\"\n"
                        f"- **report_title**: \"Migrationsbericht für {os.path.basename(context['source_file'])}\"\n"
                        f"- **include_details**: True\n\n"
                        f"Gina wird einen umfassenden Bericht über den Migrationsprozess erstellen. "
                        f"Sobald Gina den Bericht erstellt hat, teilen Sie mir die Ergebnisse mit."
                    )
                    response["next_state"] = "gina_report"
                    response["actions"] = ["Gina aufrufen", "Berichts-Parameter anpassen"]
                else:
                    # Verzeichnis nicht gefunden
                    response["message"] = (
                        "# ⚠️ Verzeichnis nicht gefunden\n\n"
                        f"Das angegebene Verzeichnis '{user_input}' wurde nicht gefunden.\n\n"
                        f"Bitte geben Sie einen gültigen Pfad zum Migrations-Verzeichnis an."
                    )
                    response["next_state"] = "get_migration_directory"
                    response["actions"] = ["Pfad zum Migrations-Verzeichnis angeben"]

                return response

            # Zustand: Gina-Bericht verarbeiten
            elif current_state == "gina_report":
                # Hier erwarten wir, dass der Benutzer die Ergebnisse von Gina mitteilt

                # Aktualisiere den Workflow-Status
                response["workflow_status"]["gina"] = "completed"

                # Versuche, die Ergebnisse von Gina zu parsen (optional)
                try:
                    gina_results = json.loads(user_input)
                    context["gina_results"] = gina_results
                    if "report_file" in gina_results:
                        context["report_file"] = gina_results["report_file"]
                except json.JSONDecodeError:
                    print("Warnung: Konnte Gina-Ergebnisse nicht als JSON parsen.")

                # Workflow abgeschlossen
                report_path_info = f"Der Bericht wurde unter '{context.get('report_file', 'unbekannt')}' gespeichert." if "report_file" in context else "Der Migrationsbericht wurde erstellt."

                response["message"] = (
                    "# 🎉 Migrationsprozess abgeschlossen\n\n"
                    "Herzlichen Glückwunsch! Der gesamte Migrationsprozess wurde erfolgreich abgeschlossen.\n\n"
                    "## Zusammenfassung des Workflows:\n"
                    f"1. **Steward** - Initialisierung und Routing ✅\n"
                    f"2. **Emma** - Datenprofilierung ✅\n"
                    f"3. **Oskar** - Datenbereinigung {('✅' if response['workflow_status']['oskar'] == 'completed' else '⏭️')}\n"
                    f"4. **Mathias** - Feldmapping ✅\n"
                    f"5. **James** - Datenmigration ✅\n"
                    f"6. **Gina** - Berichterstellung ✅\n\n"
                    f"{report_path_info}\n\n"
                    f"Möchten Sie einen neuen Migrationsprozess starten oder haben Sie Fragen zum abgeschlossenen Prozess?"
                )
                response["next_state"] = "completed"
                response["actions"] = ["Neuen Prozess starten", "Fragen zum Prozess stellen"]

                return response

            # Zustand: Workflow abgeschlossen
            elif current_state == "completed":
                if "neu" in user_input.lower() or "start" in user_input.lower():
                    # Benutzer möchte einen neuen Prozess starten
                    response["message"] = (
                        "# 🚀 Neuer Migrationsprozess\n\n"
                        "Ich starte einen neuen Migrationsprozess.\n\n"
                        "Um zu beginnen, benötige ich den Pfad zu Ihrer Quelldatei (CSV)."
                    )
                    response["next_state"] = "get_source_file"
                    response["context"] = {}  # Kontext zurücksetzen
                    response["workflow_status"] = { # Workflow-Status zurücksetzen
                        "steward": "pending", "emma": "pending", "oskar": "pending",
                        "mathias": "pending", "james": "pending", "gina": "pending"
                    }
                else:
                    # Benutzer hat eine Frage zum Prozess
                    response["message"] = (
                        "# ❓ Fragen zum Migrationsprozess\n\n"
                        "Ich beantworte gerne Ihre Fragen zum abgeschlossenen Migrationsprozess.\n\n"
                        "Sie können jederzeit einen neuen Prozess starten, indem Sie 'Neuen Prozess starten' eingeben."
                    )
                    response["actions"] = ["Neuen Prozess starten", "Wie kann ich den Bericht anpassen?", "Wo finde ich die bereinigten Daten?"]

                return response

            # Fallback für unbekannte Zustände
            else:
                response["message"] = (
                    f"# ⚠️ Unbekannter Zustand\n\n"
                    f"Entschuldigung, ich verstehe den aktuellen Zustand '{current_state}' nicht.\n\n"
                    f"Bitte starten Sie den Prozess neu, indem Sie 'Neuen Prozess starten' eingeben."
                )
                response["next_state"] = "start"
                response["actions"] = ["Neuen Prozess starten"]

                return response

        except Exception as e:
            # Log the full error for debugging
            import traceback
            print(f"Fehler im Chat-Workflow (Zustand: {current_state}): {str(e)}")
            traceback.print_exc()

            return {
                "message": f"# ❌ Fehler im Workflow\n\nEs ist ein Fehler aufgetreten: {str(e)}\n\n"
                           f"Aktueller Zustand: {current_state}\n"
                           f"Bitte überprüfen Sie die Eingaben und versuchen Sie es erneut oder starten Sie den Prozess neu.",
                "next_state": current_state, # Bleibe im aktuellen Zustand, um Korrektur zu ermöglichen
                "context": context,
                "actions": ["Prozess neu starten", "Letzten Schritt wiederholen"]
            }

    # ... (other helper functions like analyze_source_file, if needed)

# Helper function to analyze source file (example)
def analyze_source_file(file_path):
    try:
        delimiter = detect_delimiter(file_path)
        df = pd.read_csv(file_path, sep=delimiter, nrows=10) # Read only first 10 rows for analysis
        return {
            "records": "unknown (full analysis needed)", # Placeholder
            "columns": len(df.columns),
            "column_names": df.columns.tolist()
        }
    except Exception as e:
        print(f"Fehler beim Analysieren der Quelldatei {file_path}: {str(e)}")
        return {
            "records": "error",
            "columns": "error",
            "column_names": []
        }

# Helper function to detect delimiter (example)
def detect_delimiter(file_path):
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            # Read a sample of the file to detect the delimiter
            sample = csvfile.read(4096) # Read 4KB sample
            sniffer = pd.io.parsers.readers.csv.CSVParser(pd.io.common.StringIO(sample), sep=None, engine='python')
            delimiter = sniffer.read(nrows=1).columns[0] # Hacky way to get delimiter from parser
            # Basic fallback if sniffer fails
            if delimiter not in [',', ';', '\t', '|']:
                 comma_count = sample.count(',')
                 semicolon_count = sample.count(';')
                 if semicolon_count > comma_count:
                     return ';'
                 else:
                     return ','
            return delimiter
    except Exception:
         # Default fallback
         return ',' 