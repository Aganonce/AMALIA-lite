import base64
import logging
import sys

logger = logging.getLogger(__name__.split('.')[-1])
import pandas as pd
import matplotlib.pyplot as plt
import io

TRUNCATE_CUTOFF = 40

_HEADER = """
<!DOCTYPE html>
<html lang="en">
    <head>
        <title>{title}</title>
        <link rel="stylesheet" href="https://fonts.googleapis.com/icon?family=Material+Icons" />
        <link rel="stylesheet" href="https://code.getmdl.io/1.3.0/material.red-pink.min.css" />
        <script defer='defer' src="https://code.getmdl.io/1.3.0/material.min.js"></script>
        <style>
            main {{
                margin-left: auto;
                margin-right: auto;
                margin-top: 40px;
                margin-bottom: 40px;
                max-width: 720px;
                padding: 20px;
            }}
            
            table {{
                margin: auto;
            }}
            
            img {{
                margin: auto;
            }}
        </style>
    </head>
    <body>
        <header class="mdl-layout__header">
            <div class="mdl-layout__header-row">
              <!-- Title -->
              <span class="mdl-layout-title">{title}</span>
              <!-- Add spacer, to align navigation to the right -->
              <div class="mdl-layout-spacer"></div>
            </div>
          </header>
          <main class="mdl-shadow--8dp">
"""

_FOOTER = """
        </main>
    </body>
</html>
"""


def _truncate_stings(s):
    if type(s) != str:
        return s

    if len(s) > TRUNCATE_CUTOFF:
        return s[:TRUNCATE_CUTOFF] + '...'

    return s

class ReportWriter:
    """
    Handle generation of HTML reports
    """

    def _write_html(self, html, **kwargs):
        html = html.format(**kwargs)
        for line in html.split('\n'):
            self.doc.append(line)

    def __init__(self, title):
        self.doc = []
        self._write_html(_HEADER, title=title)

    def section(self, header, level=1):
        if not 1 <= level <= 6:
            logger.warning("Section level must be between 1 and 6. Defaulting to 6.")
            level = 6
        self._write_html(f"<h{level}>{header}</h{level}>")

    def p(self, text):
        self._write_html(f"<p>{text}</p>")

    def table(self, df: pd.DataFrame):
        df = df.applymap(_truncate_stings)
        df.index = df.index.map(_truncate_stings)
        table_html = df.to_html(classes="mdl-data-table mdl-js-data-table mdl-shadow--2dp", border=0)
        self._write_html(table_html)

    def savefig(self):
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        src = "data:image/png;base64, " + base64.b64encode(buf.getvalue()).decode()
        self._write_html(f"<img src='{src}' />")

    def finish(self):
        self._write_html(_FOOTER)
        return '\n'.join(self.doc)
