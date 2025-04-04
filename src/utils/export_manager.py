#!/usr/bin/env python3
"""
Data Export Manager for Gitea-Kimai Integration

This module provides utilities for exporting sync data in multiple formats
including CSV, JSON, Excel, and PDF reports.
"""

import os
import csv
import json
import logging
import sqlite3
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
import io
import zipfile

logger = logging.getLogger(__name__)

@dataclass
class ExportRequest:
    """Export request configuration."""
    export_id: str
    format_type: str
    data_type: str
    filters: Dict[str, Any]
    output_path: str
    created_at: datetime
    status: str = "pending"
    error_message: Optional[str] = None
    file_size: Optional[int] = None
    records_count: Optional[int] = None

class DataExporter:
    """Base class for data exporters."""

    def __init__(self, name: str):
        self.name = name

    def export(self, data: List[Dict[str, Any]], output_path: str, **kwargs) -> bool:
        """Export data to specified format."""
        raise NotImplementedError

    def validate_data(self, data: List[Dict[str, Any]]) -> bool:
        """Validate data before export."""
        return isinstance(data, list) and all(isinstance(item, dict) for item in data)

class CSVExporter(DataExporter):
    """CSV format exporter."""

    def __init__(self):
        super().__init__("CSV")

    def export(self, data: List[Dict[str, Any]], output_path: str, **kwargs) -> bool:
        """Export data to CSV format."""
        try:
            if not data:
                logger.warning("No data to export to CSV")
                return False

            # Get all unique keys from data
            fieldnames = set()
            for item in data:
                fieldnames.update(item.keys())
            fieldnames = sorted(fieldnames)

            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for item in data:
                    # Flatten nested objects
                    flattened_item = self._flatten_dict(item)
                    writer.writerow(flattened_item)

            logger.info(f"Exported {len(data)} records to CSV: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            return False

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)

class JSONExporter(DataExporter):
    """JSON format exporter."""

    def __init__(self):
        super().__init__("JSON")

    def export(self, data: List[Dict[str, Any]], output_path: str, **kwargs) -> bool:
        """Export data to JSON format."""
        try:
            indent = kwargs.get('indent', 2)
            sort_keys = kwargs.get('sort_keys', True)

            with open(output_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=indent, sort_keys=sort_keys, default=str)

            logger.info(f"Exported {len(data)} records to JSON: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export JSON: {e}")
            return False

class ExcelExporter(DataExporter):
    """Excel format exporter."""

    def __init__(self):
        super().__init__("Excel")

    def export(self, data: List[Dict[str, Any]], output_path: str, **kwargs) -> bool:
        """Export data to Excel format."""
        try:
            import pandas as pd

            if not data:
                logger.warning("No data to export to Excel")
                return False

            # Create DataFrame
            df = pd.json_normalize(data)

            # Export to Excel
            sheet_name = kwargs.get('sheet_name', 'Data')
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

            logger.info(f"Exported {len(data)} records to Excel: {output_path}")
            return True

        except ImportError:
            logger.error("pandas and openpyxl are required for Excel export")
            return False
        except Exception as e:
            logger.error(f"Failed to export Excel: {e}")
            return False

class HTMLExporter(DataExporter):
    """HTML format exporter."""

    def __init__(self):
        super().__init__("HTML")

    def export(self, data: List[Dict[str, Any]], output_path: str, **kwargs) -> bool:
        """Export data to HTML format."""
        try:
            title = kwargs.get('title', 'Data Export')
            include_css = kwargs.get('include_css', True)

            html_content = self._generate_html(data, title, include_css)

            with open(output_path, 'w', encoding='utf-8') as htmlfile:
                htmlfile.write(html_content)

            logger.info(f"Exported {len(data)} records to HTML: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export HTML: {e}")
            return False

    def _generate_html(self, data: List[Dict[str, Any]], title: str, include_css: bool) -> str:
        """Generate HTML content from data."""
        css = """
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; font-weight: bold; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .export-info { color: #666; font-size: 0.9em; margin-bottom: 20px; }
        </style>
        """ if include_css else ""

        if not data:
            return f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{title}</title>
                {css}
            </head>
            <body>
                <h1>{title}</h1>
                <p class="export-info">No data available for export.</p>
            </body>
            </html>
            """

        # Get all unique keys
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())
        all_keys = sorted(all_keys)

        # Generate table
        table_rows = ""
        for item in data:
            row = "<tr>"
            for key in all_keys:
                value = item.get(key, "")
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                row += f"<td>{self._escape_html(str(value))}</td>"
            row += "</tr>"
            table_rows += row

        header_row = "".join(f"<th>{self._escape_html(key)}</th>" for key in all_keys)

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            {css}
        </head>
        <body>
            <h1>{title}</h1>
            <p class="export-info">
                Exported on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                Total records: {len(data)}
            </p>
            <table>
                <thead>
                    <tr>{header_row}</tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </body>
        </html>
        """

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace('"', "&quot;")
                   .replace("'", "&#39;"))

class PDFExporter(DataExporter):
    """PDF format exporter."""

    def __init__(self):
        super().__init__("PDF")

    def export(self, data: List[Dict[str, Any]], output_path: str, **kwargs) -> bool:
        """Export data to PDF format."""
        try:
            from reportlab.lib.pagesizes import letter, A4
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib import colors

            if not data:
                logger.warning("No data to export to PDF")
                return False

            # Create PDF document
            doc = SimpleDocTemplate(output_path, pagesize=A4)
            elements = []
            styles = getSampleStyleSheet()

            # Title
            title = kwargs.get('title', 'Data Export Report')
            title_para = Paragraph(title, styles['Title'])
            elements.append(title_para)

            # Export info
            export_info = f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>Total records: {len(data)}"
            info_para = Paragraph(export_info, styles['Normal'])
            elements.append(info_para)

            # Prepare table data
            if data:
                all_keys = sorted(set().union(*(d.keys() for d in data)))
                table_data = [all_keys]  # Header row

                for item in data:
                    row = []
                    for key in all_keys:
                        value = item.get(key, "")
                        if isinstance(value, (dict, list)):
                            value = json.dumps(value)[:50] + "..." if len(json.dumps(value)) > 50 else json.dumps(value)
                        row.append(str(value)[:100])  # Limit cell content
                    table_data.append(row)

                # Create table
                table = Table(table_data)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('FONTSIZE', (0, 1), (-1, -1), 6),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                elements.append(table)

            # Build PDF
            doc.build(elements)

            logger.info(f"Exported {len(data)} records to PDF: {output_path}")
            return True

        except ImportError:
            logger.error("reportlab is required for PDF export")
            return False
        except Exception as e:
            logger.error(f"Failed to export PDF: {e}")
            return False

class ExportManager:
    """Main export management system."""

    def __init__(self, export_dir: str = "exports", database_path: str = "sync.db"):
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(exist_ok=True)
        self.database_path = database_path
        self.exporters: Dict[str, DataExporter] = {}
        self.export_history: List[ExportRequest] = []

        # Register default exporters
        self._register_exporters()

    def _register_exporters(self):
        """Register available exporters."""
        self.exporters = {
            'csv': CSVExporter(),
            'json': JSONExporter(),
            'excel': ExcelExporter(),
            'html': HTMLExporter(),
            'pdf': PDFExporter()
        }

    def export_sync_data(self, format_type: str, filters: Dict[str, Any] = None,
                        output_filename: str = None) -> Optional[str]:
        """Export sync data in specified format."""
        try:
            # Generate export ID and filename
            export_id = f"export_{int(datetime.now().timestamp())}"
            if not output_filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"sync_data_{timestamp}.{format_type}"

            output_path = self.export_dir / output_filename

            # Create export request
            export_request = ExportRequest(
                export_id=export_id,
                format_type=format_type,
                data_type="sync_data",
                filters=filters or {},
                output_path=str(output_path),
                created_at=datetime.now()
            )

            # Get data from database
            data = self._get_sync_data(filters)
            export_request.records_count = len(data)

            # Get exporter
            exporter = self.exporters.get(format_type.lower())
            if not exporter:
                raise ValueError(f"Unsupported export format: {format_type}")

            # Export data
            if exporter.export(data, str(output_path)):
                export_request.status = "completed"
                export_request.file_size = output_path.stat().st_size
                logger.info(f"Export completed: {export_id}")
            else:
                export_request.status = "failed"
                export_request.error_message = "Export failed"

            # Save export request to history
            self.export_history.append(export_request)

            return str(output_path) if export_request.status == "completed" else None

        except Exception as e:
            logger.error(f"Export failed: {e}")
            if 'export_request' in locals():
                export_request.status = "failed"
                export_request.error_message = str(e)
                self.export_history.append(export_request)
            return None

    def export_metrics(self, format_type: str, period_hours: int = 24,
                      output_filename: str = None) -> Optional[str]:
        """Export metrics data."""
        try:
            # Get metrics data
            cutoff_time = datetime.now() - timedelta(hours=period_hours)
            data = self._get_metrics_data(cutoff_time)

            if not output_filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"metrics_{timestamp}.{format_type}"

            output_path = self.export_dir / output_filename

            exporter = self.exporters.get(format_type.lower())
            if not exporter:
                raise ValueError(f"Unsupported export format: {format_type}")

            if exporter.export(data, str(output_path), title="Sync Metrics Report"):
                logger.info(f"Metrics export completed: {output_path}")
                return str(output_path)
            else:
                return None

        except Exception as e:
            logger.error(f"Metrics export failed: {e}")
            return None

    def export_audit_logs(self, format_type: str, period_hours: int = 24,
                         output_filename: str = None) -> Optional[str]:
        """Export audit logs."""
        try:
            # Get audit data
            cutoff_time = datetime.now() - timedelta(hours=period_hours)
            data = self._get_audit_data(cutoff_time)

            if not output_filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"audit_logs_{timestamp}.{format_type}"

            output_path = self.export_dir / output_filename

            exporter = self.exporters.get(format_type.lower())
            if not exporter:
                raise ValueError(f"Unsupported export format: {format_type}")

            if exporter.export(data, str(output_path), title="Audit Logs Report"):
                logger.info(f"Audit logs export completed: {output_path}")
                return str(output_path)
            else:
                return None

        except Exception as e:
            logger.error(f"Audit logs export failed: {e}")
            return None

    def create_archive(self, export_paths: List[str], archive_name: str = None) -> Optional[str]:
        """Create archive containing multiple export files."""
        try:
            if not archive_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_name = f"export_archive_{timestamp}.zip"

            archive_path = self.export_dir / archive_name

            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for export_path in export_paths:
                    if Path(export_path).exists():
                        zipf.write(export_path, Path(export_path).name)

            logger.info(f"Created export archive: {archive_path}")
            return str(archive_path)

        except Exception as e:
            logger.error(f"Failed to create archive: {e}")
            return None

    def _get_sync_data(self, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get sync data from database."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                conn.row_factory = sqlite3.Row

                query = "SELECT * FROM sync_items WHERE 1=1"
                params = []

                if filters:
                    if 'repository' in filters:
                        query += " AND repository = ?"
                        params.append(filters['repository'])

                    if 'status' in filters:
                        query += " AND sync_status = ?"
                        params.append(filters['status'])

                    if 'since' in filters:
                        query += " AND last_updated >= ?"
                        params.append(filters['since'])

                query += " ORDER BY last_updated DESC"

                cursor = conn.execute(query, params)
                rows = cursor.fetchall()

                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Failed to get sync data: {e}")
            return []

    def _get_metrics_data(self, since: datetime) -> List[Dict[str, Any]]:
        """Get metrics data from database."""
        try:
            with sqlite3.connect("metrics.db") as conn:
                conn.row_factory = sqlite3.Row

                cursor = conn.execute("""
                    SELECT * FROM sync_metrics
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                """, (since.isoformat(),))

                rows = cursor.fetchall()
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Failed to get metrics data: {e}")
            return []

    def _get_audit_data(self, since: datetime) -> List[Dict[str, Any]]:
        """Get audit data from database."""
        try:
            with sqlite3.connect("audit.db") as conn:
                conn.row_factory = sqlite3.Row

                cursor = conn.execute("""
                    SELECT * FROM audit_events
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                """, (since.isoformat(),))

                rows = cursor.fetchall()
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Failed to get audit data: {e}")
            return []

    def get_export_history(self) -> List[Dict[str, Any]]:
        """Get export history."""
        return [asdict(req) for req in self.export_history]

    def cleanup_old_exports(self, days: int = 30):
        """Clean up old export files."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            for file_path in self.export_dir.iterdir():
                if file_path.is_file():
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_time < cutoff_date:
                        file_path.unlink()
                        logger.info(f"Cleaned up old export: {file_path}")

        except Exception as e:
            logger.error(f"Failed to cleanup old exports: {e}")

    def get_available_formats(self) -> List[str]:
        """Get list of available export formats."""
        return list(self.exporters.keys())

# Global export manager instance
_global_export_manager = None

def get_export_manager() -> ExportManager:
    """Get global export manager instance."""
    global _global_export_manager

    if _global_export_manager is None:
        _global_export_manager = ExportManager()

    return _global_export_manager

# Convenience functions
def export_data(data_type: str, format_type: str, **kwargs) -> Optional[str]:
    """Export data using global export manager."""
    manager = get_export_manager()

    if data_type == "sync":
        return manager.export_sync_data(format_type, **kwargs)
    elif data_type == "metrics":
        return manager.export_metrics(format_type, **kwargs)
    elif data_type == "audit":
        return manager.export_audit_logs(format_type, **kwargs)
    else:
        logger.error(f"Unknown data type: {data_type}")
        return None

if __name__ == "__main__":
    # Example usage
    manager = ExportManager()

    # Export sync data to CSV
    csv_file = manager.export_sync_data("csv")
    if csv_file:
        print(f"Exported to: {csv_file}")

    # Export metrics to JSON
    json_file = manager.export_metrics("json", period_hours=48)
    if json_file:
        print(f"Metrics exported to: {json_file}")
```

Now let me commit this:
