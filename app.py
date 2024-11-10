from shiny import App, ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
from data_utils import (
    fetch_daily_metrics, fetch_user_metrics,
    process_daily_metrics, process_user_metrics,
    get_metric_info, format_large_number
)
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import tempfile

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load initial data
logger.info("Fetching initial data...")
daily_df = process_daily_metrics(fetch_daily_metrics())

# Get metric information
metric_info = get_metric_info()
global_metrics = metric_info['global_metrics']
user_metrics = metric_info['user_metrics']

app_ui = ui.page_fluid(
    ui.h2("USDT Analytics Dashboard"),
    
    # Top Navigation Section
    ui.div(
        ui.div(
            ui.p("Analyzing USDT metrics on Ethereum", class_="lead"),
            class_="mt-3"
        ),
        
        # Main tabs
        ui.div(
            ui.input_radio_buttons(
                "main_tab",
                None,
                choices={
                    "global": "Global Analytics",
                    "user": "User Analytics"
                },
                selected="global",
                inline=True
            ),
            class_="mb-3"
        ),
        
        # User Input Section (shows only when user analytics is selected)
        ui.panel_conditional(
            "input.main_tab === 'user'",
            ui.div(
                ui.row(
                    ui.column(8,
                        ui.input_text(
                            "address",
                            "Enter Wallet Address",
                            value="0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67",
                            placeholder="0x..."
                        )
                    ),
                    ui.column(4,
                        ui.input_action_button(
                            "lookup",
                            "Analyze Wallet",
                            class_="btn-primary w-100"
                        )
                    )
                ),
                class_="mb-4"
            )
        ),
        class_="mb-4"
    ),
    
    # Global Analytics UI
    ui.panel_conditional(
        "input.main_tab === 'global'",
        ui.div(
            # Network Overview
            ui.card(
                ui.card_header(ui.h3("Network Overview")),
                ui.div(
                    {"class": "d-flex justify-content-around"},
                    ui.div(
                        ui.h4("24h Volume"),
                        ui.output_text("daily_volume"),
                        class_="text-center p-3"
                    ),
                    ui.div(
                        ui.h4("Active Users"),
                        ui.output_text("active_users"),
                        class_="text-center p-3"
                    )
                )
            ),
            # Raw Data Section
            ui.card(
                ui.card_header("Raw Data"),
                ui.div(
                    ui.download_button("download_global_data", "Download CSV"),
                    class_="mb-3"
                ),
                ui.output_data_frame("global_data_table")
            ),
            # Metrics Section
            ui.page_sidebar(
                ui.sidebar(
                    ui.h3("Settings"),
                    ui.input_selectize(
                        "global_metric",
                        "Global Metric",
                        choices={k: v['name'] for k, v in global_metrics.items()},
                        selected="volume"
                    ),
                    ui.input_numeric(
                        "global_ma_window", 
                        "Moving Average (Days)", 
                        value=7, 
                        min=1, 
                        max=30
                    ),
                    width=3
                ),
                ui.card(
                    ui.card_header("Global Metrics Over Time"),
                    output_widget("global_time_series")
                ),
                ui.row(
                    ui.column(6,
                        ui.card(
                            ui.card_header("Transfer Distribution"),
                            output_widget("global_transfer_distribution")
                        )
                    ),
                    ui.column(6,
                        ui.card(
                            ui.card_header("Activity Heatmap"),
                            output_widget("global_activity_heatmap")
                        )
                    )
                )
            )
        )
    ),
    
    # User Analytics UI
    ui.panel_conditional(
        "input.main_tab === 'user'",
        ui.page_sidebar(
            ui.sidebar(
                ui.h3("Metrics Settings"),
                ui.input_selectize(
                    "user_metric",
                    "Select Metric",
                    choices={k: v['name'] for k, v in user_metrics.items()},
                    selected="transferCount"
                ),
                ui.input_numeric(
                    "user_ma_window",
                    "Moving Average (Days)", 
                    value=7, 
                    min=1, 
                    max=30
                ),
                width=3
            ),
            ui.card(
                ui.card_header("Wallet Overview"),
                ui.div(
                    {"class": "d-flex justify-content-around"},
                    ui.div(
                        ui.h4("Total Transfers"),
                        ui.output_text("total_transfers"),
                        class_="text-center p-3"
                    ),
                    ui.div(
                        ui.h4("Total Volume"),
                        ui.output_text("total_volume"),
                        class_="text-center p-3"
                    ),
                    ui.div(
                        ui.h4("Current Balance"),
                        ui.output_text("current_balance"),
                        class_="text-center p-3"
                    )
                )
            ),
            ui.card(
                ui.card_header("Metrics Over Time"),
                output_widget("user_time_series")
            ),
            ui.row(
                ui.column(6,
                    ui.card(
                        ui.card_header("Transfer Distribution"),
                        output_widget("transfer_distribution")
                    )
                ),
                ui.column(6,
                    ui.card(
                        ui.card_header("Activity Heatmap"),
                        output_widget("activity_heatmap")
                    )
                )
            ),
            ui.card(
                ui.card_header("Raw Data"),
                ui.div(
                    ui.download_button("download_user_data", "Download CSV"),
                    class_="mb-3"
                ),
                ui.output_data_frame("user_data_table")
            )
        )
    )
)

def server(input, output, session):
    # Create reactive value for user data
    user_data = reactive.Value(pd.DataFrame())
    
    @reactive.Effect
    @reactive.event(input.lookup)
    def update_user_data():
        address = input.address()
        if address:
            data = process_user_metrics(fetch_user_metrics(address))
            user_data.set(data)
            if data.empty:
                ui.notification_show(
                    "No data found for this address",
                    type="warning"
                )
            else:
                ui.notification_show(
                    "Data loaded successfully",
                    type="message"
                )

    # Global metrics text outputs
    @output
    @render.text
    def daily_volume():
        if daily_df.empty:
            return "N/A"
        return format_large_number(daily_df.iloc[-1]['volume'])

    @output
    @render.text
    def active_users():
        if daily_df.empty:
            return "N/A"
        return format_large_number(daily_df.iloc[-1]['activeAccounts'])

    @output
    @render.text
    def new_users():
        if daily_df.empty:
            return "N/A"
        return format_large_number(daily_df.iloc[-1]['newAccounts'])

    # User metrics text outputs
    @output
    @render.text
    def total_transfers():
        df = user_data.get()
        if df.empty:
            return "N/A"
        return format_large_number(df['transferCount'].sum())

    @output
    @render.text
    def total_volume():
        df = user_data.get()
        if df.empty:
            return "N/A"
        return format_large_number(df['totalTransferred'].sum())

    @output
    @render.text
    def current_balance():
        df = user_data.get()
        if df.empty:
            return "N/A"
        return format_large_number(df.iloc[-1]['endDayBalance'])

    # Global metrics plots
    @output
    @render_plotly
    def global_time_series():
        if daily_df.empty:
            return create_empty_plot("Global Metrics Over Time")
            
        metric = input.global_metric()
        metric_name = global_metrics[metric]['name']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_df['timestamp'],
            y=daily_df[metric],
            mode='lines',
            name=metric_name
        ))
        
        if len(daily_df) >= input.global_ma_window():
            ma = daily_df[metric].rolling(window=input.global_ma_window(), min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=daily_df['timestamp'],
                y=ma,
                mode='lines',
                line=dict(dash='dash'),
                name=f"{input.global_ma_window()}-day MA"
            ))
        
        fig.update_layout(
            title=f"{metric_name} Over Time",
            height=400,
            hovermode='x unified'
        )
        return fig

    @output
    @render_plotly
    def global_transfer_distribution():
        if daily_df.empty:
            return create_empty_plot("Transfer Distribution")
            
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=daily_df['averageTransferAmount'],
            nbinsx=30,
            name="Transfers"
        ))
        
        fig.update_layout(
            title="Global Transfer Amount Distribution",
            xaxis_title="Average Transfer Amount",
            yaxis_title="Count of Days",
            height=400
        )
        return fig

    @output
    @render_plotly
    def global_activity_heatmap():
        if daily_df.empty:
            return create_empty_plot("Activity Heatmap")
            
        # Convert timestamp to hour and weekday
        df = daily_df.copy()
        df['hour'] = df['timestamp'].dt.hour
        df['weekday'] = df['timestamp'].dt.day_name()
        
        # Create pivot table of transfer counts
        pivot = df.pivot_table(
            values='transferCount',
            index='weekday',
            columns='hour',
            aggfunc='mean',  # Using mean for the global view
            fill_value=0
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=[str(h) for h in range(24)],
            y=pivot.index,
            colorscale='Viridis'
        ))
        
        fig.update_layout(
            title="Global Activity Heatmap (Average Transfers)",
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week",
            height=400
        )
        return fig

    # User metrics plots
    @output
    @render_plotly
    def user_time_series():
        df = user_data.get()
        if df.empty:
            return create_empty_plot("User Metrics Over Time")
            
        metric = input.user_metric()
        metric_name = user_metrics[metric]['name']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df[metric],
            mode='lines',
            name=metric_name
        ))
        
        if len(df) >= input.user_ma_window():
            ma = df[metric].rolling(window=input.user_ma_window(), min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=ma,
                mode='lines',
                line=dict(dash='dash'),
                name=f"{input.user_ma_window()}-day MA"
            ))
        
        fig.update_layout(
            title=f"{metric_name} Over Time",
            height=400,
            hovermode='x unified'
        )
        return fig

    @output
    @render_plotly
    def transfer_distribution():
        df = user_data.get()
        if df.empty:
            return create_empty_plot("Transfer Distribution")
            
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=df['averageTransferAmount'],
            nbinsx=30,
            name="Transfers"
        ))
        
        fig.update_layout(
            title="Transfer Amount Distribution",
            xaxis_title="Average Transfer Amount",
            yaxis_title="Count",
            height=400
        )
        return fig

    @output
    @render_plotly
    def activity_heatmap():
        df = user_data.get()
        if df.empty:
            return create_empty_plot("Activity Heatmap")
            
        df['hour'] = df['timestamp'].dt.hour
        df['weekday'] = df['timestamp'].dt.day_name()
        
        pivot = df.pivot_table(
            values='transferCount',
            index='weekday',
            columns='hour',
            aggfunc='sum',
            fill_value=0
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=[str(h) for h in range(24)],
            y=pivot.index,
            colorscale='Viridis'
        ))
        
        fig.update_layout(
            title="Activity Heatmap",
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week",
            height=400
        )
        return fig

    def create_empty_plot(title, message="No data available"):
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text=message,
            showarrow=False,
            font=dict(size=14)
        )
        fig.update_layout(
            title=title,
            showlegend=False,
            height=400
        )
        return fig

    @output
    @render.data_frame
    def global_data_table():
        return render.DataGrid(daily_df)

    @output
    @render.data_frame
    def user_data_table():
        return render.DataGrid(user_data.get())

    @session.download(filename="global_metrics.csv")
    async def download_global_data():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            daily_df.to_csv(tmp.name, index=False)
            return tmp.name

    @session.download(filename="user_metrics.csv")
    async def download_user_data():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            user_data.get().to_csv(tmp.name, index=False)
            return tmp.name

app = App(app_ui, server)