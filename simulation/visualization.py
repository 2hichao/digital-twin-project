"""
Visualization module for the NIO Digital Twin Simulation.

This module provides functions to create and update visualizations for simulation data.
The visualizations include production counts, event distributions, quality check results,
and event timelines. It uses matplotlib for plotting and pandas for data handling.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import datetime
import logging

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
    The Visualization class provides methods to generate plots from simulation data.

    Attributes:
        data (pd.DataFrame): The data used for visualization. Expected to have columns such as:
            - 'timestamp': a float or datetime representing when the event was recorded.
            - 'vehicle_id': an integer identifying a vehicle.
            - 'event': a string indicating the event type.
            - 'value': a float representing a sensor or simulation measurement.
        fig (plt.Figure): Matplotlib Figure object used for dashboard display.
        axs (np.ndarray): Array of Axes objects for individual subplots.
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize the Visualization object with data.

        Args:
            data (pd.DataFrame): The input data for plotting.
        """
        self.data = data.copy()
        self.fig = None
        self.axs = None
        logger.info("Visualization instance created with data of shape %s", self.data.shape)

    def prepare_data(self):
        """
        Prepare the data for plotting.

        Converts the 'timestamp' column to datetime if it is not already in datetime format.
        Creates additional columns if needed for time series plotting.
        """
        if self.data.empty:
            logger.warning("Data is empty. No plots will be generated.")
            return

        if not np.issubdtype(self.data['timestamp'].dtype, np.datetime64):
            try:
                self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], unit='s')
                logger.debug("Converted 'timestamp' column to datetime.")
            except Exception as e:
                logger.error("Error converting 'timestamp' to datetime: %s", e)

        # Create a new column 'date' for grouping by day if necessary.
        if 'date' not in self.data.columns:
            self.data['date'] = self.data['timestamp'].dt.date
            logger.debug("Created 'date' column for daily grouping.")

    def plot_production_count(self):
        """
        Plot the number of records per vehicle ID as a bar chart.

        This plot shows the count of events for each vehicle.
        """
        if self.data.empty:
            logger.warning("No data available for production count plot.")
            return

        try:
            production_counts = self.data.groupby("vehicle_id").size()
            fig, ax = plt.subplots(figsize=(10, 6))
            production_counts.plot(kind="bar", ax=ax, color="skyblue")
            ax.set_title("Vehicle Production Count")
            ax.set_xlabel("Vehicle ID")
            ax.set_ylabel("Event Count")
            ax.grid(True, linestyle="--", alpha=0.5)
            plt.tight_layout()
            logger.info("Generated production count plot.")
            return fig, ax
        except Exception as e:
            logger.error("Error generating production count plot: %s", e)

    def plot_event_distribution(self):
        """
        Plot the distribution of event types as a pie chart.

        This plot shows how events are distributed across different types.
        """
        if self.data.empty:
            logger.warning("No data available for event distribution plot.")
            return

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
            logger.error("Error generating event distribution plot: %s", e)

    def plot_quality_check_results(self):
        """
        Plot a histogram of quality check values if such data exists.

        It is assumed that quality check events have 'value' entries representing scores.
        """
        # Filter for quality check events, assumed to be labeled 'quality'
        quality_data = self.data[self.data["event"] == "quality"]
        if quality_data.empty:
            logger.warning("No quality check data found for histogram.")
            return

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
            logger.error("Error generating quality check histogram: %s", e)

    def plot_event_timeline(self):
        """
        Plot a time series showing the number of events per time interval.

        This plot aggregates events in 1-minute bins and shows the frequency over time.
        """
        if self.data.empty:
            logger.warning("No data available for event timeline plot.")
            return

        try:
            # Resample data by minute
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
            logger.error("Error generating event timeline plot: %s", e)

    def show_dashboard(self):
        """
        Create and display a dashboard with multiple subplots.

        The dashboard includes:
            - Production count bar chart.
            - Event distribution pie chart.
            - Event timeline line plot.
            - (Optional) Quality check histogram if data is available.
        """
        self.prepare_data()
        logger.info("Preparing dashboard with multiple visualizations.")

        try:
            # Create a figure with a grid of subplots.
            self.fig, self.axs = plt.subplots(2, 2, figsize=(14, 10))
            plt.subplots_adjust(hspace=0.4, wspace=0.3)

            # Plot production count in the top-left subplot.
            production_counts = self.data.groupby("vehicle_id").size()
            self.axs[0, 0].bar(production_counts.index.astype(str), production_counts.values, color="skyblue")
            self.axs[0, 0].set_title("Vehicle Production Count")
            self.axs[0, 0].set_xlabel("Vehicle ID")
            self.axs[0, 0].set_ylabel("Event Count")
            self.axs[0, 0].grid(True, linestyle="--", alpha=0.5)

            # Plot event distribution in the top-right subplot.
            event_counts = self.data["event"].value_counts()
            self.axs[0, 1].pie(event_counts.values, labels=event_counts.index, autopct="%1.1f%%", startangle=90)
            self.axs[0, 1].set_title("Event Distribution")

            # Plot event timeline in the bottom-left subplot.
            timeline = self.data.set_index("timestamp").resample("1T").size()
            self.axs[1, 0].plot(timeline.index, timeline.values, marker="o", linestyle="-", color="coral")
            self.axs[1, 0].set_title("Event Timeline (per Minute)")
            self.axs[1, 0].set_xlabel("Time")
            self.axs[1, 0].set_ylabel("Number of Events")
            self.axs[1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            self.axs[1, 0].grid(True, linestyle="--", alpha=0.5)

            # Plot quality check histogram in the bottom-right subplot if available.
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
            logger.info("Dashboard prepared successfully. Displaying dashboard.")
            plt.show()
        except Exception as e:
            logger.error("Error displaying dashboard: %s", e)

    def update_plots(self, new_data: pd.DataFrame):
        """
        Update the internal data and refresh all plots.

        Args:
            new_data (pd.DataFrame): New data to merge with existing data.
        """
        try:
            # Merge the new data with the existing data.
            self.data = pd.concat([self.data, new_data]).drop_duplicates().reset_index(drop=True)
            logger.info("Data updated. New data shape: %s", self.data.shape)
            # Refresh all plots by re-preparing the dashboard.
            self.prepare_data()
            self.show_dashboard()
        except Exception as e:
            logger.error("Error updating plots: %s", e)

    def save_figure(self, filename: str):
        """
        Save the current figure to a file.

        Args:
            filename (str): The file path where the figure will be saved.
        """
        if self.fig is None:
            logger.warning("No figure available to save. Generate a plot first.")
            return
        try:
            self.fig.savefig(filename, dpi=300)
            logger.info("Figure saved to %s", filename)
        except Exception as e:
            logger.error("Error saving figure: %s", e)


# Additional helper functions for standalone plot generation.

def plot_single_production_count(data: pd.DataFrame, output_file: str = None):
    """
    Generate and display a single production count plot.

    Args:
        data (pd.DataFrame): Input data for plotting.
        output_file (str, optional): If provided, save the plot to this file.
    """
    vis = Visualization(data)
    fig_ax = vis.plot_production_count()
    if fig_ax is not None:
        fig, ax = fig_ax
        if output_file:
            try:
                fig.savefig(output_file, dpi=300)
                logger.info("Production count plot saved to %s", output_file)
            except Exception as e:
                logger.error("Error saving production count plot: %s", e)
        else:
            plt.show()

def plot_single_event_distribution(data: pd.DataFrame, output_file: str = None):
    """
    Generate and display a single event distribution pie chart.

    Args:
        data (pd.DataFrame): Input data for plotting.
        output_file (str, optional): If provided, save the plot to this file.
    """
    vis = Visualization(data)
    fig_ax = vis.plot_event_distribution()
    if fig_ax is not None:
        fig, ax = fig_ax
        if output_file:
            try:
                fig.savefig(output_file, dpi=300)
                logger.info("Event distribution plot saved to %s", output_file)
            except Exception as e:
                logger.error("Error saving event distribution plot: %s", e)
        else:
            plt.show()

# For standalone testing of visualization functions.
if __name__ == "__main__":
    # Generate some sample data for testing.
    sample_data = pd.DataFrame({
        "timestamp": pd.date_range(start="2025-01-01 08:00:00", periods=200, freq="T").astype(np.int64) // 10**9,
        "vehicle_id": np.random.randint(1, 20, size=200),
        "event": np.random.choice(["produced", "assembled", "quality", "tested", "inspected"], size=200),
        "value": np.random.rand(200) * 100
    })
    
    # Create a Visualization instance with sample data.
    vis = Visualization(sample_data)
    vis.prepare_data()
    
    # Generate individual plots.
    vis.plot_production_count()
    vis.plot_event_distribution()
    vis.plot_event_timeline()
    vis.plot_quality_check_results()
    
    # Display the dashboard with all plots.
    vis.show_dashboard()
