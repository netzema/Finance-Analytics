
from pathlib import Path
import polars as pl
import pandas as pd
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
import dash
from dash import html, dcc, Input, Output, ctx, dash_table
import dash_bootstrap_components as dbc
from utils import *
import constants as constants

today = pd.Timestamp.today()

# Precompute month options
df_init = load_account_data(constants.ACCOUNTS)
df_init = df_init.with_columns(
    pl.col("bookingDate").dt.strftime("%Y-%m").alias("year_month")
)
all_months = sorted(set(df_init["year_month"].to_list()))
# keep only the last 24
months_dt = pd.to_datetime([m + "-01" for m in all_months])
cutoff = today - pd.DateOffset(months=24)
month_options = [
    {"label": m, "value": m}
    for m, dt in zip(all_months, months_dt)
    if dt >= cutoff
]
default_month = month_options[-1]["value"]

# Define Monthly Budgets
budgets = constants.BUDGETS

# Start Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SLATE])
app.title = "Finance Dashboard"

app.layout = html.Div([dbc.Container([
    html.H2("💼 Personal Finance Dashboard", className="my-4 text-center"),
    
    dbc.Row([
        dbc.Col([
            html.H5("Select Account:"),
            dcc.Dropdown(
                id="account-select",
                options=[{"label": n, "value": n} for n in ["Daniel", "Hansi", "Silvia", "Visa"]],
                value=["Daniel"],
                multi=True,
                style={"width": "100%"}
            )
        ], width=4),

        dbc.Col([
            html.H5("Select Month:"),
            dcc.Dropdown(
                id="month-select",
                options=month_options,
                value=default_month,
                multi=False,
                style={"width": "100%"}
            )
        ], width=4),
        dbc.Col([
            html.H5("Privacy Mode:"),
            dcc.Checklist(
            id="blur-toggle",
            options=[{"label":"Blur numbers","value":"BLUR"}],
            value=[],
            inline=True
            )
        ], width=4)
    ], className="mb-4"),
    
    html.Div(id="dynamic-content"),
    dcc.Store("non-transfer"),
    dcc.Store("this-month"),
    dcc.Store(id="selected-months")
], fluid=True)], style={"backgroundColor": "#121212", "minHeight": "100vh", "paddingTop": "20px"})


@app.callback(
    Output("dynamic-content", "children"),
    Output("non-transfer", "data"),
    Output("this-month", "data"),
    Output("selected-months", "data"),
    Input("account-select", "value"),
    Input("month-select", "value"),
    Input("blur-toggle","value")
)
def update_dashboard(selected_accounts, selected_month, blur_values):
    if not selected_accounts:
        return html.Div("Please select at least one account."), [], [], []
    
    df = load_account_data(selected_accounts)

    if df.is_empty():
        return html.Div("No data found for selected accounts."), [], [], []
    
    # Preprocess data
    df = df.with_columns([
        # pl.col("bookingDate").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("amount").cast(pl.Float64),
        pl.col("category").fill_null("Uncategorized")
    ]).with_columns(
        pl.col("bookingDate").dt.strftime("%Y-%m").alias("year_month")
    ).filter(
        pl.col("year_month") > pl.col("year_month").min() # exclude January as only fees and no actual expenses happened
    )
    
    min_date = df["year_month"].min()
    max_date = df["year_month"].max()
    print(f"Date range for accounts ({", ".join(selected_accounts)}):\n{min_date} - {max_date}")

    # Filter for selected months
    if selected_month is None:
        selected_month = df["year_month"].max()

    selected_month = pd.to_datetime(selected_month)
    this_year = selected_month.year
    sel_month_str = selected_month.strftime("%Y-%m")
    df = df.filter(pl.col("year_month").str.strptime(pl.Date, "%Y-%m") <= selected_month)

    # Exclude transfers for expense/income
    non_transfer = df.filter(pl.col("category") != "Transfer")

    # net expense per category
    cat_monthly_net = (
        non_transfer
        .group_by(["year_month", "category"])
        .agg(pl.sum("amount").alias("net"))  # net = income (+) minus expense (-)
    )

    # Summary metrics
    # Monthly income/expense from netted categories
    monthly_stats = (
        cat_monthly_net
        .group_by("year_month")
        .agg([
            pl.col("net").filter(pl.col("category").str.to_lowercase().str.contains("income")).sum().alias("monthly_income"),
            (pl.col("net").filter(~pl.col("category").str.to_lowercase().str.contains("income"))).sum().alias("monthly_expense")
        ])
        .sort("year_month")
    )

    total_income = monthly_stats["monthly_income"].sum() or 0.0
    total_expenses = monthly_stats["monthly_expense"].sum() or 0.0

    total_savings = abs(df.filter(pl.col("category") == "Transfer").select(pl.sum("amount")).item() or 0.0)
    savings_rate = (total_savings / total_income * 100) if total_income else 0.0

    monthly_avg_income  = monthly_stats.select(pl.mean("monthly_income")).item() or 0.0
    monthly_avg_expense = monthly_stats.select(pl.mean("monthly_expense")).item() or 0.0

    # Cumulative expenses this month
    th_month_filter = (
        (non_transfer["bookingDate"].dt.month() == selected_month.month) &
        (non_transfer["bookingDate"].dt.year() == this_year)
    )

    # Use raw amounts (include positives so reimbursements reduce the curve)
    cum_df = (
        non_transfer
        .filter(th_month_filter)
        .sort("bookingDate")
        .with_columns(pl.col("amount").cum_sum().alias("cumulative"))
        .to_pandas()
    )

    # Monthly income vs expenses trend for plotting

    monthly_trend = monthly_stats.select([
        pl.col("year_month"),
        pl.col("monthly_income").alias("total_income"),
        pl.col("monthly_expense").abs().alias("total_expense")
    ]).to_pandas()

    # Top categories
    this_month = non_transfer.filter(
        (pl.col("bookingDate").dt.month() == selected_month.month) & 
        (pl.col("bookingDate").dt.year() == this_year)
    )

    # top categories (all time)
    top_all = (
        non_transfer
        .with_columns(pl.col("bookingDate").dt.year() == this_year)
        .group_by("category")
        .agg(pl.sum("amount").alias("net"))
        .with_columns((-pl.col("net")).alias("expense"))  # only categories net-negative
        .filter(pl.col("expense") > 0)
        .sort("expense", descending=False)
        .tail(10)
        .select([pl.col("category"), pl.col("expense").alias("total")])
        .to_pandas()
    )

    # top categories (this month)
    this_month_net = (
        this_month
        .group_by("category")
        .agg(pl.sum("amount").alias("net"))
        .with_columns((-pl.col("net")).alias("expense"))
        .filter(pl.col("expense") > 0)
    )

    top_this_month = (
        this_month_net
        .sort("expense", descending=False)
        .tail(10)
        .select([pl.col("category"), pl.col("expense").alias("total")])
        .to_pandas()
    )


    # Monthly average per category for top 10 this month
    # Compute average monthly expense per category over all months
    # Net by category per month
    cat_monthly = cat_monthly_net  # reuse from step (1)

    # Expense component per (category, month) = -min(net, 0)
    cat_monthly_exp = cat_monthly.with_columns(
        (-pl.col("net")).alias("monthly_cat_expense")
    )

    # Average across months per category
    avg_cat = (
        cat_monthly_exp
        .group_by("category")
        .agg(pl.mean("monthly_cat_expense").alias("avg_monthly_expense"))
        .to_pandas()
    )

    # Current month vs avg of prev 3 months
    # to pandas + month-start timestamps
    pivot = cat_monthly_exp.to_pandas()
    pivot["year_month"] = pd.to_datetime(pivot["year_month"] + "-01")

    curr = pd.Timestamp(selected_month.year, selected_month.month, 1)

    # pivot to months x categories, fill missing with 0
    piv = pivot.pivot_table(
        index="year_month",
        columns="category",
        values="monthly_cat_expense",
        aggfunc="sum",
        fill_value=0,
    )

    # reindex to a COMPLETE monthly index up to the PREVIOUS month
    if len(piv.index):
        start = piv.index.min().to_period("M").to_timestamp()
    else:
        start = curr - pd.offsets.MonthBegin(3)  # fallback if no data
    prev_month = curr - pd.offsets.MonthBegin(1)
    full_idx = pd.period_range(
        start=start.to_period("M"),
        end=prev_month.to_period("M"),
        freq="M"
    ).to_timestamp()

    piv = piv.reindex(full_idx, fill_value=0)

    # average of the last 3 months BEFORE current (use shorter window if not enough history)
    window = min(3, len(piv.index))
    prev3_avgs = piv.tail(window).mean().reset_index()
    prev3_avgs.columns = ["category", "avg_prev3"]

    # current month expense per category
    curr_exp = (
        pivot[pivot["year_month"] == curr]
        .groupby("category")["monthly_cat_expense"]
        .sum()
        .rename("curr_expense")
        .reset_index()
    )

    # trends using the proper 3-mo avg
    trend_df = pd.merge(curr_exp, prev3_avgs, on="category", how="inner")
    trend_df["abs_change"] = trend_df["curr_expense"] - trend_df["avg_prev3"]
    # safe % change (avoid div-by-zero)
    trend_df["pct_change"] = trend_df.apply(
        lambda r: (r["abs_change"] / r["avg_prev3"] * 100) if r["avg_prev3"] else 0.0, axis=1
    )

    top_inc = trend_df.sort_values("pct_change", ascending=False).head(3)
    top_dec = trend_df.sort_values("pct_change").head(3)

    # === Use 3-mo avg for the grouped compare chart as well ===
    topcats = top_this_month["category"].tolist()
    avg_top = (
        prev3_avgs[prev3_avgs["category"].isin(topcats)]
        .rename(columns={"avg_prev3": "avg_monthly_expense"})
    )
    last30_df = (
        top_this_month.set_index("category")["total"]
        .rename_axis("category")
        .reset_index()
    )
    grouped_compare = pd.merge(avg_top, last30_df, on="category", how="inner")

    # Net by category for the selected month
    month_net = (
        non_transfer
        .filter(
            (pl.col('bookingDate').dt.month() == selected_month.month) &
            (pl.col('bookingDate').dt.year()  == this_year)
        )
        .group_by("category")
        .agg(pl.sum("amount").alias("net"))
    )

    # Helper to get expense (>=0) from net
    # expense = -min(net, 0)
    month_expenses = month_net.with_columns(
        (-pl.col("net")).alias("expense")
    )

    spent = {}
    budget_categories = list(budgets.keys())
    known_cats = pl.Series(budget_categories)

    # Map explicit categories
    for cat in budget_categories:
        if cat == "Other":
            continue
        spent_amt = month_expenses.filter(pl.col("category") == cat).select(pl.sum("expense")).item() or 0.0
        spent[cat] = spent_amt

    # "Other" = sum of expense for cats not in budgets
    other_amt = (
        month_expenses
        .filter(~pl.col("category").is_in(known_cats))
        .select(pl.sum("expense"))
        .item() or 0.0
    )
    spent["Other"] = other_amt

    budget_df = pd.DataFrame({
        'Category': list(budgets.keys()),
        'Budget': list(budgets.values()),
        'Spent': [spent[c] for c in budgets.keys()]
    })
    budget_df['Remaining'] = budget_df['Budget'] - budget_df['Spent']

    dynamic_ui = html.Div([
        # Summary Cards with monthly averages
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardHeader("Total Income"), dbc.CardBody(html.H5(f"{total_income:,.2f} €"))])),
            dbc.Col(dbc.Card([dbc.CardHeader("Total Expenses"), dbc.CardBody(html.H5(f"{total_expenses:,.2f} €"))])),
            dbc.Col(dbc.Card([dbc.CardHeader("Monthly Avg Income"), dbc.CardBody(html.H5(f"{monthly_avg_income:,.2f} €"))])),
            dbc.Col(dbc.Card([dbc.CardHeader("Monthly Avg Expense"), dbc.CardBody(html.H5(f"{abs(monthly_avg_expense):,.2f} €"))])),
            dbc.Col(dbc.Card([dbc.CardHeader("Total Savings"), dbc.CardBody(html.H5(f"{total_savings:,.2f} €"))])),
            dbc.Col(dbc.Card([dbc.CardHeader("Savings Rate"), dbc.CardBody(html.H5(f"{savings_rate:.1f}%"))]))
        ], className="mb-4"),

        # Cumulative Expenses Chart
        dcc.Graph(
            id='cum-expense-graph',
            figure=go.Figure([
                go.Scatter(x=cum_df['bookingDate'], y=cum_df['cumulative'], mode='lines', name=f'Cumulative Exp ({sel_month_str})'),
                go.Scatter(x=cum_df['bookingDate'], y=[monthly_avg_expense]*len(cum_df), mode='lines', name='Monthly Avg Exp', line=dict(dash='dash'))
            ]).update_layout(title=f'Cumulative Expenses {sel_month_str}', template='plotly_dark')
        ),

        # Monthly Trend Chart
        dcc.Graph(
            id='monthly-trend-graph',
            figure=px.bar(monthly_trend, x='year_month', y=['total_income','total_expense'], barmode='group', title='Monthly Income vs Expenses', labels={'value':'€','year_month':'Month'}).update_layout(template='plotly_dark')
        ),

        # Top Categories Charts
        dbc.Row([
            dbc.Col(dcc.Graph(id='top-all-graph', figure=px.bar(top_all, x='category', y='total', title=f'Top 10 Categories {this_year}', labels={'total':'€'}).update_layout(template='plotly_dark'))),
            dbc.Col(dcc.Graph(id='top-30-graph', figure=px.bar(top_this_month, x='category', y='total', title=f'Top 10 Categories ({sel_month_str})', labels={'total':'€'}).update_layout(template='plotly_dark')))    
        ]),

        # Grouped bar: monthly avg vs last30 for top categories
        dcc.Graph(
            id='avg-vs-last30-graph',
            figure=go.Figure(data=[
                go.Bar(name='3 Months Mv. Avg', x=grouped_compare['category'], y=grouped_compare['avg_monthly_expense']),
                go.Bar(name=f'{sel_month_str} Total', x=grouped_compare['category'], y=grouped_compare['total'])
            ]).update_layout(barmode='group', title=f'Top Categories: 3 Months Moving Avg. vs {sel_month_str}', template='plotly_dark')
        ),

        # Trend charts: top 3 increasing/decreasing
        dbc.Row([
            dbc.Col(dcc.Graph(
                id='trend-inc-graph',
                figure=px.bar(
                    top_inc,
                    x='category',
                    y='pct_change',
                    hover_data={
                        'avg_prev3': ':.2f',
                        'curr_expense': ':.2f',
                        'pct_change': ':.1f'
                    },
                    title='Top 3 % Increase',
                    labels={
                        'pct_change': '% Change',
                        'avg_prev3': 'Avg Monthly Exp (€)',
                        'curr_expense': f"{sel_month_str} Exp (€)"
                    },
                    color_discrete_sequence=['red']  # bars in red
                ).update_layout(template='plotly_dark', yaxis_tickformat='.1f%%')
            )),
            dbc.Col(dcc.Graph(
                id='trend-dec-graph',
                figure=px.bar(
                    top_dec,
                    x='category',
                    y='pct_change',
                    hover_data={
                        'avg_prev3': ':.2f',
                        'curr_expense': ':.2f',
                        'pct_change': ':.1f'
                    },
                    title='Top 3 % Decrease',
                    labels={
                        'pct_change': '% Change',
                        'avg_prev3': 'Avg Monthly Exp (€)',
                        'curr_expense': f"{sel_month_str} Exp (€)"
                    },
                    color_discrete_sequence=['green']  # bars in green
                )
                .update_layout(template='plotly_dark', yaxis_tickformat='.1f%%')
            ))
        ]),

        dcc.Graph(
            id='budget-chart',
            figure=go.Figure(data=[
                go.Bar(name='Budget', x=budget_df['Category'], y=budget_df['Budget']),
                go.Bar(name='Spent', x=budget_df['Category'], y=budget_df['Spent']),
                go.Bar(
                    name='Remaining',
                    x=budget_df['Category'],
                    y=budget_df['Remaining'],
                    marker_color=[
                        'green' if rem >= 0 else 'red'
                        for rem in budget_df['Remaining']
                    ]
                )
            ]).update_layout(
                barmode='group',
                title=f"Monthly Budget vs Spent ({selected_month.strftime('%B %Y')})",
                template='plotly_dark',
                yaxis_title='€'
            )
        ),

        html.Hr(),
        # Dropdown for category table
        html.H5("Transactions Table"),
        dbc.Row([
            dbc.Col(dcc.Dropdown(id='category-search', options=non_transfer["category"].unique().sort().to_list(), style={'width':'100%'}, multi=True)),
            dbc.Col(html.Div(id='category-sum'))
        ], className="mb-3"),
        # Transaction Table
        dash_table.DataTable(
            id='transaction-table',
            columns=[{"name": c, "id": c} for c in ['bookingDate','remittance','amount','category']],
            data=[],
            sort_action='native',
            style_table={'overflowX': 'auto'},
            style_header={'backgroundColor': '#2c3e50', 'color': 'white'},
            style_cell={'backgroundColor': '#1e1e1e', 'color': 'white', 'textAlign': 'left'}
        )
    ])

    # Apply blur if toggle is on
    blur_on = "BLUR" in blur_values
    wrapper_style = {"filter":"blur(4px)"} if blur_on else {}

    return html.Div(dynamic_ui, style=wrapper_style), non_transfer.to_dicts(), this_month.to_dicts(), selected_month


# Callback to show transactions dynamically
@app.callback(
    Output('category-sum', 'children'),
    Output('transaction-table','data'),
    Input("non-transfer", "data"),
    Input("this-month", "data"),
    Input('category-search','value'),
    Input('month-select', 'value'),
    Input('cum-expense-graph','clickData'),
    Input('monthly-trend-graph','clickData'),
    Input('top-all-graph','clickData'),
    Input('top-30-graph','clickData'),
    Input('avg-vs-last30-graph','clickData'),
    Input('trend-inc-graph','clickData'),
    Input('trend-dec-graph','clickData'),
    Input('budget-chart','clickData')
)
def display_transactions(non_transfer, this_month, selected_cats, selected_month, cum_click, trend_click, all_click, last30_click, avg_click, inc_click, dec_click, budget_click):
    # Priority: search overrides clicks
    non_transfer = pl.DataFrame(non_transfer) if non_transfer else pl.DataFrame()
    if non_transfer is not None:
        non_transfer = non_transfer.with_columns([
            pl.col("bookingDate").str.strptime(pl.Date, "%Y-%m-%d"),
        ])
    this_month = pl.DataFrame(this_month) if this_month else pl.DataFrame()

    if selected_month is None:
        selected_month = non_transfer["year_month"].max()
        
    selected_month = pd.to_datetime(selected_month)

    if selected_cats:
        df_click = non_transfer.filter(
            (pl.col('category').is_in(selected_cats))
        )

        total = (
            non_transfer
            .filter(pl.col('category').is_in(selected_cats))
            .select(pl.sum('amount'))
            .item() or 0.0
        )
        first_month = non_transfer["bookingDate"].dt.month().min()
        last_month = selected_month.month
        month_diff = max(1, last_month - first_month + 1)
        avg_per_month = total / month_diff
        sum_text = f"Total for {', '.join(selected_cats)}: {total:.2f}€ | Avg. per Month: {avg_per_month:.2f}€"
    else:
        triggered = ctx.triggered_id
    
        df_click = None
        sum_text = ""
        if triggered == 'cum-expense-graph' and cum_click:
            date_val = pd.to_datetime(cum_click['points'][0]['x']).date()
            df_click = non_transfer.filter(pl.col('bookingDate') == date_val)
        elif triggered == 'monthly-trend-graph' and trend_click:
            mon = pd.to_datetime(trend_click['points'][0]['x']).strftime('%Y-%m')
            df_click = non_transfer.filter(pl.col('year_month') == mon)
        elif triggered == 'top-all-graph' and all_click:
            cat = all_click['points'][0]['x']
            df_click = non_transfer.filter(pl.col('category') == cat)
        elif triggered == 'top-30-graph' and last30_click:
            cat = last30_click['points'][0]['x']
            df_click = this_month.filter(pl.col('category') == cat)
        elif triggered == 'avg-vs-last30-graph' and avg_click:
            cat = avg_click['points'][0]['x']
            df_click = non_transfer.filter(pl.col('category') == cat)
        elif triggered == 'trend-inc-graph' and inc_click:
            cat = inc_click['points'][0]['x']
            df_click = non_transfer.filter(pl.col('category') == cat)
        elif triggered == 'trend-dec-graph' and dec_click:
            cat = dec_click['points'][0]['x']
            df_click = non_transfer.filter(pl.col('category') == cat)
        elif triggered == 'budget-chart' and budget_click:
            # Show spent transactions for selected budget category this month
            cat = budget_click['points'][0]['x']
            
            cat_cond = (pl.col("category") == cat) if cat != "Other" else (~pl.col("category").is_in(budgets))
            df_click = non_transfer.filter(
                (pl.col('bookingDate').dt.month() == selected_month.month) &
                (pl.col('bookingDate').dt.year() == this_year) &
                # (pl.col('amount') < 0) &
                cat_cond
            )

        if df_click is not None:
            total = df_click["amount"].sum()
            sum_text = f"Total: {total:.2f}€"

    if (df_click is None) or (df_click.is_empty()):
        return "", []

    pdf = df_click.select(['bookingDate','remittance','amount','category']).to_pandas()
    pdf['bookingDate'] = pdf['bookingDate'].astype(str)
    pdf['amount'] = pdf['amount'].round(2)
    return sum_text, pdf.to_dict('records')

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8051)
