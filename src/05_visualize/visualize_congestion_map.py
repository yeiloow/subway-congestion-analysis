import sqlite3
import pandas as pd
import folium
from folium.plugins import TimestampedGeoJson
import logging
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def visualization_congestion_map():
    logger.info("Starting congestion map visualization...")

    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    try:
        # Fetch station location and congestion data
        # We average congestion over all days/quarters available to get a "typical" day view
        query = """
            SELECT 
                s.station_name_kr,
                l.line_name,
                l.color_hex,
                sr.lat,
                sr.lon,
                sc.time_slot,
                AVG(sc.congestion_level) as avg_congestion
            FROM Station_Congestion sc
            JOIN Station_Routes sr ON sc.station_number = sr.station_number
            JOIN Stations s ON sr.station_id = s.station_id
            JOIN Lines l ON sr.line_id = l.line_id
            WHERE sr.lat IS NOT NULL AND sr.lon IS NOT NULL
            GROUP BY s.station_name_kr, l.line_name, sr.lat, sr.lon, sc.time_slot
            ORDER BY sc.time_slot
        """
        logger.info("Executing query...")
        df = pd.read_sql_query(query, conn)
        logger.info(f"Fetched {len(df)} records.")

        if df.empty:
            logger.warning("No data found for visualization.")
            return

        # Calculate a base time for the slider (Time slot 1 = 05:30)
        # We will create pseudo-timestamps for the slider
        # Let's say we map everything to "2024-01-01" + time
        def get_time_str(slot):
            minutes_from_midnight = (5 * 60) + 30 + (slot - 1) * 30  # Start 05:30
            hour = (minutes_from_midnight // 60) % 24
            minute = minutes_from_midnight % 60
            return f"2024-01-01T{hour:02d}:{minute:02d}:00"

        df["time_str"] = df["time_slot"].apply(get_time_str)

        # Create base map centered on Seoul
        seoul_coords = [37.5665, 126.9780]
        m = folium.Map(location=seoul_coords, zoom_start=11, tiles="CartoDB positron")

        # Prepare features for TimestampedGeoJson
        features = []
        for _, row in df.iterrows():
            # Determine color based on congestion
            congestion = row["avg_congestion"]
            if congestion < 30:
                color = "#00ff00"  # Green
            elif congestion < 50:
                color = "#ffff00"  # Yellow
            elif congestion < 70:
                color = "#ffa500"  # Orange
            else:
                color = "#ff0000"  # Red

            # Create Feature
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["lon"], row["lat"]],
                },
                "properties": {
                    "time": row["time_str"],
                    "style": {"color": color},
                    "icon": "circle",
                    "iconstyle": {
                        "fillColor": color,
                        "fillOpacity": 0.8,
                        "stroke": "false",
                        "radius": 5 + (congestion / 10),  # Size based on congestion
                    },
                    "popup": f"""
                        <b>{row["station_name_kr"]} ({row["line_name"]})</b><br>
                        Time: {row["time_str"].split("T")[1][:5]}<br>
                        Congestion: {congestion:.1f}
                    """,
                },
            }
            features.append(feature)

        # Add TimestampedGeoJson
        TimestampedGeoJson(
            {
                "type": "FeatureCollection",
                "features": features,
            },
            period="PT30M",
            add_last_point=False,
            auto_play=False,
            loop=False,
            max_speed=1,
            loop_button=True,
            date_options="HH:mm",
            time_slider_drag_update=True,
            duration="PT30M",  # Show points for 30 minutes
        ).add_to(m)

        # Add Legend
        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 150px; height: 130px; 
                    border:2px solid grey; z-index:9999; font-size:14px;
                    background-color:white; opacity:0.8;
                    padding: 10px;">
        <b>Congestion Levels</b><br>
        <i style="background:#00ff00; width:10px; height:10px; display:inline-block; border-radius:50%"></i> < 30 (Good)<br>
        <i style="background:#ffff00; width:10px; height:10px; display:inline-block; border-radius:50%"></i> 30 - 50 (Fair)<br>
        <i style="background:#ffa500; width:10px; height:10px; display:inline-block; border-radius:50%"></i> 50 - 70 (Busy)<br>
        <i style="background:#ff0000; width:10px; height:10px; display:inline-block; border-radius:50%"></i> > 70 (Crowded)<br>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))

        # Save map
        output_file = OUTPUT_DIR / "congestion_map.html"
        m.save(output_file)
        logger.info(f"Map saved to {output_file}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    visualization_congestion_map()
