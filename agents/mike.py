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
    
    @mcp.tool(name="💬 Mike - Chat Flow")
    def migration_chat_flow(
        user_input: str,
        current_state: str = "start",
        context: dict = None
    ) -> dict:
        """
        Führt einen interaktiven Chat-Flow für den Migrationsprozess durch
        
        Args:
            user_input: Die Eingabe des Benutzers
            current_state: Der aktuelle Zustand des Chat-Flows
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
                "actions": []
            }
            
            # Zustand: Start des Migrationsprozesses
            if current_state == "start":
                # Prüfe, ob der Benutzer einen Pfad angegeben hat
                if os.path.exists(user_input):
                    # Prüfe, ob es sich um eine Datei oder ein Verzeichnis handelt
                    if os.path.isfile(user_input):
                        # Es ist eine Datei, wahrscheinlich die Quelldatei
                        context["source_file"] = user_input
                        response["message"] = (
                            f"Danke! Ich habe die Quelldatei '{os.path.basename(user_input)}' gefunden.\n\n"
                            f"Möchten Sie diese Datei analysieren, um mit dem Migrationsprozess fortzufahren?"
                        )
                        response["next_state"] = "confirm_analysis"
                        response["actions"] = ["Ja, Datei analysieren", "Nein, andere Datei wählen"]
                    else:
                        # Es ist ein Verzeichnis, wahrscheinlich das Projektverzeichnis
                        context["project_directory"] = user_input
                        # Rufe die Funktion get_migration_status auf, um den Status zu ermitteln
                        status_result = get_migration_status(user_input)
                        response["message"] = status_result["message"]
                        response["next_state"] = "continue_migration"
                        response["actions"] = ["Migration fortsetzen", "Neue Migration starten"]
                        response["context"]["migration_status"] = status_result["summary"]
                else:
                    # Kein gültiger Pfad, frage erneut
                    response["message"] = (
                        "Der angegebene Pfad existiert nicht. Bitte geben Sie einen gültigen Pfad zu einer Quelldatei "
                        "oder einem Migrationsverzeichnis an."
                    )
                    response["actions"] = ["Pfad zur Quelldatei angeben", "Pfad zum Migrationsverzeichnis angeben"]
            
            # Zustand: Bestätigung der Analyse
            elif current_state == "confirm_analysis":
                if user_input.lower() in ["ja", "ja, datei analysieren", "analysieren", "yes"]:
                    # Analysiere die Quelldatei
                    analysis_result = orchestrate_migration(
                        action="analyze",
                        source_file=context["source_file"]
                    )
                    response["message"] = analysis_result["message"]
                    response["next_state"] = "choose_mapping"
                    response["context"]["file_info"] = analysis_result.get("file_info", {})
                    response["context"]["existing_mappings"] = analysis_result.get("existing_mappings", [])
                    
                    if "existing_mappings" in analysis_result and analysis_result["existing_mappings"]:
                        response["actions"] = ["Vorhandenes Mapping verwenden", "Neues Mapping erstellen"]
                    else:
                        response["actions"] = ["Neues Mapping erstellen", "Zielstruktur beschreiben"]
                else:
                    # Benutzer möchte eine andere Datei wählen
                    response["message"] = "Bitte geben Sie den Pfad zu einer anderen Quelldatei an."
                    response["next_state"] = "start"
                    response["actions"] = ["Pfad zur Quelldatei angeben"]
            
            # Zustand: Mapping auswählen
            elif current_state == "choose_mapping":
                if "vorhandenes mapping" in user_input.lower():
                    # Benutzer möchte ein vorhandenes Mapping verwenden
                    if "existing_mappings" in context and context["existing_mappings"]:
                        mapping_options = "\n".join([f"- {os.path.basename(m)}" for m in context["existing_mappings"]])
                        response["message"] = (
                            f"Bitte wählen Sie eines der folgenden Mappings:\n\n{mapping_options}\n\n"
                            f"Geben Sie den Namen des Mappings an, das Sie verwenden möchten."
                        )
                        response["next_state"] = "select_existing_mapping"
                        response["actions"] = [os.path.basename(m) for m in context["existing_mappings"]]
                    else:
                        response["message"] = (
                            "Es wurden keine vorhandenen Mappings gefunden. Wir müssen ein neues Mapping erstellen.\n\n"
                            "Bitte geben Sie den Pfad zur Zielstruktur-Datei an oder beschreiben Sie die Zielstruktur."
                        )
                        response["next_state"] = "create_mapping"
                        response["actions"] = ["Pfad zur Zielstruktur angeben", "Zielstruktur beschreiben"]
                else:
                    # Benutzer möchte ein neues Mapping erstellen
                    response["message"] = (
                        "Um ein neues Mapping zu erstellen, benötige ich Informationen über die Zielstruktur.\n\n"
                        "Bitte geben Sie den Pfad zur Zielstruktur-Datei an oder beschreiben Sie die Zielstruktur."
                    )
                    response["next_state"] = "create_mapping"
                    response["actions"] = ["Pfad zur Zielstruktur angeben", "Zielstruktur beschreiben"]
            
            # Zustand: Vorhandenes Mapping auswählen
            elif current_state == "select_existing_mapping":
                # Suche nach dem angegebenen Mapping in den vorhandenen Mappings
                selected_mapping = None
                for mapping in context.get("existing_mappings", []):
                    if os.path.basename(mapping).lower() == user_input.lower() or user_input.lower() in mapping.lower():
                        selected_mapping = mapping
                        break
                
                if selected_mapping:
                    context["mapping_file"] = selected_mapping
                    response["message"] = (
                        f"Danke! Ich verwende das Mapping '{os.path.basename(selected_mapping)}'.\n\n"
                        f"Jetzt müssen wir die Daten validieren und auf Duplikate prüfen.\n\n"
                        f"Bitte geben Sie den Pfad zur Zieldatei an, gegen die wir prüfen sollen."
                    )
                    response["next_state"] = "validate_data"
                    response["actions"] = ["Pfad zur Zieldatei angeben", "Similarity Threshold anpassen"]
                else:
                    response["message"] = (
                        f"Ich konnte das angegebene Mapping nicht finden. Bitte wählen Sie eines der folgenden Mappings:\n\n"
                        f"{', '.join([os.path.basename(m) for m in context.get('existing_mappings', [])])}"
                    )
                    response["actions"] = [os.path.basename(m) for m in context.get("existing_mappings", [])]
            
            # Zustand: Mapping erstellen
            elif current_state == "create_mapping":
                # Prüfe, ob der Benutzer einen Pfad angegeben hat
                if os.path.exists(user_input):
                    context["target_structure"] = user_input
                    response["message"] = (
                        f"Danke! Ich verwende die Zielstruktur-Datei '{os.path.basename(user_input)}'.\n\n"
                        f"Jetzt werde ich Mathias bitten, ein Mapping zu erstellen.\n\n"
                        f"Bitte verwenden Sie das Tool '🔄 Mathias - Create Field Mapping' mit folgenden Parametern:\n\n"
                        f"- **source_file**: {context['source_file']}\n"
                        f"- **target_structure**: {user_input}\n\n"
                        f"Sobald das Mapping erstellt wurde, können wir mit der Validierung fortfahren."
                    )
                    response["next_state"] = "wait_for_mapping"
                    response["actions"] = ["Mathias aufrufen", "Mapping manuell erstellen"]
                else:
                    # Benutzer hat eine Beschreibung der Zielstruktur angegeben
                    context["target_description"] = user_input
                    response["message"] = (
                        f"Danke für die Beschreibung der Zielstruktur. Basierend darauf werde ich Mathias bitten, "
                        f"ein Mapping zu erstellen.\n\n"
                        f"Bitte verwenden Sie das Tool '🔄 Mathias - Create Field Mapping' mit folgenden Parametern:\n\n"
                        f"- **source_file**: {context['source_file']}\n"
                        f"- **target_description**: {user_input}\n\n"
                        f"Sobald das Mapping erstellt wurde, können wir mit der Validierung fortfahren."
                    )
                    response["next_state"] = "wait_for_mapping"
                    response["actions"] = ["Mathias aufrufen", "Mapping manuell erstellen"]
            
            # Weitere Zustände können hier hinzugefügt werden...
            
            # Fallback für unbekannte Zustände
            else:
                response["message"] = (
                    f"Entschuldigung, ich verstehe den aktuellen Zustand '{current_state}' nicht. "
                    f"Bitte starten Sie den Prozess neu."
                )
                response["next_state"] = "start"
                response["actions"] = ["Prozess neu starten"]
            
            return response
            
        except Exception as e:
            return {
                "message": f"Fehler im Chat-Flow: {str(e)}",
                "next_state": "error",
                "context": context,
                "actions": ["Prozess neu starten"]
            } 