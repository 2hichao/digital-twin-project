"""
Visualization module for the NIO Digital Twin Simulation.

This module provides functions to create and update visualizations for simulation data.
The visualizations include static charts (bar, pie, histogram, and line plots) using Matplotlib,
interactive charts using Plotly, real-time updates via Matplotlib animation, and a full web dashboard
using Dash. Additional visual reports (e.g., scatter plots) are also included.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.animation as animation
import pandas as pd
import numpy as np
import datetime
import logging
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import threading
import time

# Set up module-level logging.
logger = logging.getLogger("visualization")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class Visualization:
    """
    The Visualization class provides methods to generate plots and dashboards from simulation data.

    Attributes:
        data (pd.DataFrame): The data used for plotting.
        fig (plt.Figure): Matplotlib Figure object.
        axs (np.ndarray): Array of Matplotlib Axes objects.
    """
    def __init__(self, data: pd.DataFrame):
        """
        Initialize the Visualization object with data.

        Args:
            data (pd.DataFrame): Input data for plotting.
        """
        self.data = data.copy()
        self.fig = None
        self.axs = None
        logger.info("Visualization instance created with data shape: %s", self.data.shape)

    def prepare_data(self):
        """
        Prepare the data for plotting.

        Converts the 'timestamp' column to datetime if needed and creates additional columns.
        """
        if self.data.empty:
            logger.warning("Data is empty. No plots can be generated.")
            return
        if not np.issubdtype(self.data['timestamp'].dtype, np.datetime64):
            try:
                self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], unit='s')
                logger.debug("Converted 'timestamp' to datetime.")
            except Exception as e:
                logger.error("Error converting 'timestamp': %s", e)
        if 'date' not in self.data.columns:
            self.data['date'] = self.data['timestamp'].dt.date
            logger.debug("Added 'date' column for grouping.")

    def plot_production_count(self):
        """
        Plot the number of records per vehicle ID as a bar chart using Matplotlib.
        """
        if self.data.empty:
            logger.warning("No data for production count plot.")
            return None, None
        try:
            counts = self.data.groupby("vehicle_id").size()
            fig, ax = plt.subplots(figsize=(10, 6))
            counts.plot(kind="bar", ax=ax, color="skyblue")
            ax.set_title("Vehicle Production Count")
            ax.set_xlabel("Vehicle ID")
            ax.set_ylabel("Count")
            ax.grid(True, linestyle="--", alpha=0.5)
            plt.tight_layout()
            logger.info("Generated production count plot.")
            return fig, ax
        except Exception as e:
            logger.error("Error in production count plot: %s", e)

    def plot_event_distribution(self):
        """
        Plot the distribution of event types as a pie chart using Matplotlib.
        """
        if self.data.empty:
            logger.warning("No data for event distribution plot.")
            return None, None
        try:
            event_counts = self.data["event"].value_counts()
            fig, ax = plt.subplots(figsize=(8, 8))
            event_counts.plot(kind="pie", autopct="%1.1f%%", startangle=90, ax=ax)
            ax.set_ylabel("")
            ax.set_title("Event Distribution")
            plt.tight_layout()
            logger.info("Generated event distribution pie chart.")
            return fig, ax
        except Exception as e:
            logger.error("Error in event distribution plot: %s", e)

    def plot_quality_check_results(self):
        """
        Plot a histogram of quality check values if data exists using Matplotlib.
        """
        quality_data = self.data[self.data["event"] == "quality"]
        if quality_data.empty:
            logger.warning("No quality check data for histogram.")
            return None, None
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(quality_data["value"], bins=20, color="lightgreen", edgecolor="black")
            ax.set_title("Quality Check Score Distribution")
            ax.set_xlabel("Quality Score")
            ax.set_ylabel("Frequency")
            ax.grid(True, linestyle="--", alpha=0.5)
            plt.tight_layout()
            logger.info("Generated quality check histogram.")
            return fig, ax
        except Exception as e:
            logger.error("Error in quality check histogram: %s", e)

    def plot_event_timeline(self):
        """
        Plot a time series of event counts per minute using Matplotlib.
        """
        if self.data.empty:
            logger.warning("No data for event timeline plot.")
            return None, None
        try:
            timeline = self.data.set_index("timestamp").resample("1T").size()
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(timeline.index, timeline.values, marker="o", linestyle="-", color="coral")
            ax.set_title("Event Timeline (per Minute)")
            ax.set_xlabel("Time")
            ax.set_ylabel("Number of Events")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax.grid(True, linestyle="--", alpha=0.5)
            plt.tight_layout()
            logger.info("Generated event timeline plot.")
            return fig, ax
        except Exception as e:
            logger.error("Error in event timeline plot: %s", e)

    def interactive_production_count(self):
        """
        Generate an interactive bar chart for production count using Plotly.
        """
        if self.data.empty:
            logger.warning("No data for interactive production count.")
            return None
        try:
            counts = self.data.groupby("vehicle_id").size().reset_index(name="count")
            fig = px.bar(counts, x="vehicle_id", y="count", title="Interactive Vehicle Production Count",
                         labels={"vehicle_id": "Vehicle ID", "count": "Event Count"})
            fig.update_layout(template="plotly_white")
            logger.info("Generated interactive production count chart.")
            return fig
        except Exception as e:
            logger.error("Error generating interactive production count: %s", e)
            return None

    def interactive_event_distribution(self):
        """
        Generate an interactive pie chart for event distribution using Plotly.
        """
        if self.data.empty:
            logger.warning("No data for interactive event distribution.")
            return None
        try:
            event_counts = self.data["event"].value_counts().reset_index()
            event_counts.columns = ["event", "count"]
            fig = px.pie(event_counts, names="event", values="count", title="Interactive Event Distribution",
                         hole=0.3)
            fig.update_layout(template="plotly_white")
            logger.info("Generated interactive event distribution chart.")
            return fig
        except Exception as e:
            logger.error("Error generating interactive event distribution: %s", e)
            return None

    def interactive_quality_check_results(self):
        """
        Generate an interactive histogram for quality check results using Plotly.
        """
        quality_data = self.data[self.data["event"] == "quality"]
        if quality_data.empty:
            logger.warning("No quality check data for interactive histogram.")
            return None
        try:
            fig = px.histogram(quality_data, x="value", nbins=20, title="Interactive Quality Check Histogram",
                               labels={"value": "Quality Score"})
            fig.update_layout(template="plotly_white")
            logger.info("Generated interactive quality check histogram.")
            return fig
        except Exception as e:
            logger.error("Error generating interactive quality check histogram: %s", e)
            return None

    def interactive_event_timeline(self):
        """
        Generate an interactive time series chart for event timeline using Plotly.
        """
        if self.data.empty:
            logger.warning("No data for interactive event timeline.")
            return None
        try:
            timeline = self.data.set_index("timestamp").resample("1T").size().reset_index(name="event_count")
            fig = px.line(timeline, x="timestamp", y="event_count", title="Interactive Event Timeline (per Minute)",
                          labels={"timestamp": "Time", "event_count": "Number of Events"})
            fig.update_layout(template="plotly_white")
            logger.info("Generated interactive event timeline chart.")
            return fig
        except Exception as e:
            logger.error("Error generating interactive event timeline: %s", e)
            return None

    def real_time_update_chart(self, interval=1000):
        """
        Create a real-time updating chart using Matplotlib animation.

        This method sets up a live updating plot for event counts.
        Args:
            interval (int): Update interval in milliseconds.
        """
        if self.data.empty:
            logger.warning("No data for real-time update chart.")
            return

        self.prepare_data()
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_title("Real-Time Event Timeline")
        ax.set_xlabel("Time")
        ax.set_ylabel("Event Count")
        line, = ax.plot([], [], marker="o", linestyle="-", color="coral")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.grid(True, linestyle="--", alpha=0.5)

        def init():
            ax.set_xlim(datetime.datetime.now(), datetime.datetime.now() + datetime.timedelta(seconds=60))
            ax.set_ylim(0, 10)
            line.set_data([], [])
            return line,

        def update(frame):
            current_time = datetime.datetime.now()
            new_record = {
                "timestamp": pd.to_datetime(time.time(), unit='s'),
                "vehicle_id": np.random.randint(1, 100),
                "event": np.random.choice(["produced", "assembled", "quality", "tested"]),
                "value": np.random.rand() * 100
            }
            self.data = self.data.append(new_record, ignore_index=True)
            timeline = self.data.set_index("timestamp").resample("5S").size()
            times = timeline.index.to_pydatetime()
            counts = timeline.values
            ax.set_xlim(times[0], times[-1] + datetime.timedelta(seconds=5))
            if len(counts) > 0:
                ax.set_ylim(0, max(counts) + 5)
            line.set_data(times, counts)
            return line,

        ani = animation.FuncAnimation(fig, update, init_func=init, interval=interval, blit=True)
        logger.info("Real-time update chart created.")
        plt.show()

    def run_dash_dashboard(self):
        """
        Run a Dash dashboard to display multiple interactive charts.

        The dashboard includes production count, event distribution, quality check results,
        and event timeline charts that update every few seconds.
        """
        self.prepare_data()
        app = dash.Dash(__name__)
        app.layout = html.Div([
            html.H1("NIO Digital Twin Dashboard"),
            dcc.Graph(id="production-count"),
            dcc.Graph(id="event-distribution"),
            dcc.Graph(id="quality-check"),
            dcc.Graph(id="event-timeline"),
            dcc.Interval(
                id="interval-component",
                interval=5*1000,  # Update every 5 seconds.
                n_intervals=0
            )
        ])

        @app.callback(
            [Output("production-count", "figure"),
             Output("event-distribution", "figure"),
             Output("quality-check", "figure"),
             Output("event-timeline", "figure")],
            [Input("interval-component", "n_intervals")]
        )
        def update_dashboard(n):
            prod_fig = self.interactive_production_count()
            event_fig = self.interactive_event_distribution()
            quality_fig = self.interactive_quality_check_results()
            timeline_fig = self.interactive_event_timeline()
            return prod_fig, event_fig, quality_fig, timeline_fig

        # Run the Dash app in a separate thread.
        def run_dash():
            app.run_server(debug=False, port=8050)
        
        dash_thread = threading.Thread(target=run_dash, name="DashDashboardThread", daemon=True)
        dash_thread.start()
        logger.info("Dash dashboard started on http://127.0.0.1:8050")
        return app

    def show_dashboard(self):
        """
        Create and display a dashboard with multiple static visualizations using Matplotlib.
        """
        self.prepare_data()
        logger.info("Preparing static dashboard with multiple visualizations.")

        try:
            self.fig, self.axs = plt.subplots(2, 2, figsize=(14, 10))
            plt.subplots_adjust(hspace=0.4, wspace=0.3)

            # Production count chart.
            counts = self.data.groupby("vehicle_id").size()
            self.axs[0, 0].bar(counts.index.astype(str), counts.values, color="skyblue")
            self.axs[0, 0].set_title("Vehicle Production Count")
            self.axs[0, 0].set_xlabel("Vehicle ID")
            self.axs[0, 0].set_ylabel("Count")
            self.axs[0, 0].grid(True, linestyle="--", alpha=0.5)

            # Event distribution pie chart.
            event_counts = self.data["event"].value_counts()
            self.axs[0, 1].pie(event_counts.values, labels=event_counts.index, autopct="%1.1f%%", startangle=90)
            self.axs[0, 1].set_title("Event Distribution")

            # Event timeline line plot.
            timeline = self.data.set_index("timestamp").resample("1T").size()
            self.axs[1, 0].plot(timeline.index, timeline.values, marker="o", linestyle="-", color="coral")
            self.axs[1, 0].set_title("Event Timeline (per Minute)")
            self.axs[1, 0].set_xlabel("Time")
            self.axs[1, 0].set_ylabel("Number of Events")
            self.axs[1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            self.axs[1, 0].grid(True, linestyle="--", alpha=0.5)

            # Quality check histogram.
            quality_data = self.data[self.data["event"] == "quality"]
            if not quality_data.empty:
                self.axs[1, 1].hist(quality_data["value"], bins=20, color="lightgreen", edgecolor="black")
                self.axs[1, 1].set_title("Quality Check Score Distribution")
                self.axs[1, 1].set_xlabel("Quality Score")
                self.axs[1, 1].set_ylabel("Frequency")
                self.axs[1, 1].grid(True, linestyle="--", alpha=0.5)
            else:
                self.axs[1, 1].text(0.5, 0.5, "No quality check data", horizontalalignment="center",
                                    verticalalignment="center", fontsize=12, transform=self.axs[1, 1].transAxes)
                self.axs[1, 1].set_title("Quality Check")
                self.axs[1, 1].axis("off")

            plt.tight_layout()
            logger.info("Static dashboard prepared. Displaying dashboard.")
            plt.show()
        except Exception as e:
            logger.error("Error preparing static dashboard: %s", e)


if __name__ == "__main__":
    # Generate sample data for demonstration.
    sample_data = pd.DataFrame({
        "timestamp": pd.date_range(start="2025-01-01 08:00:00", periods=200, freq="T").astype(np.int64) // 10**9,
        "vehicle_id": np.random.randint(1, 20, size=200),
        "event": np.random.choice(["produced", "assembled", "quality", "tested", "inspected"], size=200),
        "value": np.random.rand(200) * 100
    })
    
    vis = Visualization(sample_data)
    vis.prepare_data()
    # Generate and display individual static plots.
    vis.plot_production_count()
    vis.plot_event_distribution()
    vis.plot_event_timeline()
    vis.plot_quality_check_results()
    
    # Display the static dashboard.
    vis.show_dashboard()
    
    # Uncomment to run the real-time update chart.
    # vis.real_time_update_chart()
    
    # Uncomment to launch the interactive Dash dashboard.
    # vis.run_dash_dashboard()
