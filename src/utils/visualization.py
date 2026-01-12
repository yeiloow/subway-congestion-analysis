import plotly.io as pio
import plotly.express as px
import plotly.graph_objects as go


def apply_theme():
    """
    Sets the default Plotly template/theme for the project.
    """
    # Use a built-in template as a base, e.g., 'plotly_white' or 'plotly_dark'
    # We can customize it further if needed.
    pio.templates.default = "plotly_white"

    # Custom font settings can be applied here if needed globally,
    # though Plotly usually handles standard fonts well.
    # For Korean, modern browsers usually pick up system fonts fine,
    # but we can specify if strictly necessary.
    pio.templates[
        pio.templates.default
    ].layout.font.family = "Malgun Gothic, AppleGothic, sans-serif"


def get_color_palette():
    """
    Returns a unified color palette for the project.
    """
    return px.colors.qualitative.Plotly


def save_plot(fig, output_path):
    """
    Helper to save a Plotly figure to HTML.
    Ensures the directory exists.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(output_path)
    # print(f"Saved visualization to {output_path}")


# Initialize theme on import

# Initialize theme on import
apply_theme()

import platform
import os


def get_font_path():
    """
    Returns the system file path for a Korean font (Malgun Gothic or AppleGothic).
    Useful for WordCloud or Matplotlib.
    """
    system_name = platform.system()
    if system_name == "Windows":
        font_path = "C:/Windows/Fonts/malgun.ttf"
    elif system_name == "Darwin":  # Mac
        font_path = "/System/Library/Fonts/Supplemental/AppleGothic.ttf"
        if not os.path.exists(font_path):
            font_path = "/System/Library/Fonts/AppleGothic.ttf"
    else:
        # Linux or other, fallback
        font_path = "malgun.ttf"  # Assumes it is in current dir or path

    return font_path if os.path.exists(font_path) else None
