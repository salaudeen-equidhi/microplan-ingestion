import sqlite3
import csv
import os
import base64
from IPython.display import display, HTML, clear_output
import ipywidgets as widgets


def export_data_to_csv(db_file, output_folder):
    """Export all tables from the SQLite DB to CSV files."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    os.makedirs(output_folder, exist_ok=True)
    exported = []

    for table in tables:
        csv_path = os.path.join(output_folder, f'{table}.csv')
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            cursor.execute(f'SELECT * FROM [{table}]')
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(cursor)
        exported.append(csv_path)

    conn.close()
    return exported


def build_export_ui(ctx):
    OUTPUT_DIR = ctx['OUTPUT_DIR']

    out_csv = widgets.Output()

    btn_export_csv = widgets.Button(
        description='Export CSVs', button_style='info',
        layout=widgets.Layout(width='150px', height='35px'))

    def on_export_csv(b):
        with out_csv:
            clear_output(wait=True)

            w_db_name = ctx['widgets'].get('w_db_name')
            db_name = w_db_name.value.strip() if w_db_name else 'microplan.db'
            if not db_name:
                db_name = 'microplan.db'
            db_path = os.path.join(OUTPUT_DIR, db_name)

            if not os.path.exists(db_path):
                display(HTML(
                    "<p style='color:red'><b>Database file not found.</b> "
                    "Run the transformation first.</p>"
                ))
                return

            try:
                db_stem = os.path.splitext(db_name)[0]
                output_folder = os.path.join(OUTPUT_DIR, f"csv_export_{db_stem}")
                csv_files = export_data_to_csv(db_path, output_folder)

                html = ("<div style='padding:12px; background:#e8f4fd; border-left:4px solid #2196F3; "
                        "margin:10px 0;'>"
                        f"<b style='color:#2196F3;'>Exported {len(csv_files)} CSV file(s)</b><br><br>")

                for fp in csv_files:
                    fname = os.path.basename(fp)
                    with open(fp, 'rb') as f:
                        b64 = base64.b64encode(f.read()).decode()
                    html += (
                        f'<a href="data:text/csv;base64,{b64}" download="{fname}" '
                        f'style="display:inline-block; padding:8px 15px; background:#4caf50; color:white; '
                        f'text-decoration:none; border-radius:4px; margin:4px 2px;">'
                        f'Download {fname}</a> '
                    )

                html += "</div>"
                display(HTML(html))

            except Exception as ex:
                import traceback
                display(HTML(
                    f"<div style='padding:10px; background:#ffc7ce; border-radius:5px;'>"
                    f"<b style='color:red'>CSV Export Error:</b><br>"
                    f"<pre>{traceback.format_exc()}</pre></div>"
                ))

    btn_export_csv.on_click(on_export_csv)

    return widgets.VBox([
        widgets.HTML("<h3>Export to CSV</h3>"),
        widgets.HTML("<p>Export all database tables to individual CSV files for inspection.</p>"),
        btn_export_csv,
        out_csv,
    ])
