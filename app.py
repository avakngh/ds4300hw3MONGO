from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from pymongo import MongoClient

# database connection
def get_col(name):
    client = MongoClient("mongodb://localhost:27017/")
    return client["covid_data"][name]

# define functions here since they're used a lot later 

# joins epidemiology and demographics files using location_key as the country code
LOOKUP_DEMOGRAPHICS = {
    "$lookup": {
        "from": "demographics",
        "let": {"cc": "$_id"},                                 
        "pipeline": [{"$match": {"$expr": {"$eq": ["$location_key", "$$cc"]}}}],  # match on location_key
        "as": "demographics"                                    # stores result as array 
    }
}

# stores country code as country_code for easy connection b/w data
ADD_COUNTRY_CODE = {
    "$addFields": {"country_code": {"$substr": ["$location_key", 0, 2]}}
}


## QUERIES

# highest covid cases vs elderly pop
def query_high_covid_elderly(limit=20):
    pipeline = [
        ADD_COUNTRY_CODE,
        # group rows by country code and sum cases by country
        {"$group": {"_id": "$country_code", "latest_cumulative_confirmed": {"$sum": "$cumulative_confirmed"}}},
        LOOKUP_DEMOGRAPHICS,
        # take all elderly age groups
        {"$addFields": {
            "total_elderly": {"$sum": [
                {"$arrayElemAt": ["$demographics.population_age_60_69", 0]},
                {"$arrayElemAt": ["$demographics.population_age_70_79", 0]},
                {"$arrayElemAt": ["$demographics.population_age_80_and_older", 0]}
            ]},
            "total_population": {"$arrayElemAt": ["$demographics.population", 0]}
        }},
        # divide by pop for per capita
        {"$addFields": {
            "confirmed_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$latest_cumulative_confirmed", "$total_population"]}, "else": None}},
            "elderly_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$total_elderly", "$total_population"]}, "else": None}}
        }},
        # drop na
        {"$match": {"confirmed_per_capita": {"$ne": None}, "elderly_per_capita": {"$ne": None}}},
        # sort by top countries and take top
        {"$sort": {"confirmed_per_capita": -1}},
        {"$limit": limit}
    ]
    return pd.DataFrame(list(get_col("epidemiology").aggregate(pipeline)))

# highest elderly pop vs covid cases (similar query but sorting by elderly instead of covid)
def query_high_elderly_covid(limit=20):
    pipeline = [
        ADD_COUNTRY_CODE,
        {"$group": {"_id": "$country_code", "latest_cumulative_confirmed": {"$sum": "$cumulative_confirmed"}}},
        LOOKUP_DEMOGRAPHICS,
        {"$addFields": {
            "total_elderly": {"$sum": [
                {"$arrayElemAt": ["$demographics.population_age_60_69", 0]},
                {"$arrayElemAt": ["$demographics.population_age_70_79", 0]},
                {"$arrayElemAt": ["$demographics.population_age_80_and_older", 0]}
            ]},
            "total_population": {"$arrayElemAt": ["$demographics.population", 0]}
        }},
        {"$addFields": {
            "confirmed_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$latest_cumulative_confirmed", "$total_population"]}, "else": None}},
            "elderly_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$total_elderly", "$total_population"]}, "else": None}}
        }},
        {"$match": {"confirmed_per_capita": {"$ne": None}, "elderly_per_capita": {"$ne": None}}},
        {"$sort": {"elderly_per_capita": -1}},
        {"$limit": limit}
    ]
    return pd.DataFrame(list(get_col("epidemiology").aggregate(pipeline)))

# proportion of urban population vs covid cases
def query_urban_covid(most_urban=True, limit=20):
    # most urban, otherwise least urban (for the two visualizations)
    sort_dir = -1 if most_urban else 1  
    pipeline = [
        ADD_COUNTRY_CODE,
        {"$group": {"_id": "$country_code", "latest_cumulative_confirmed": {"$sum": "$cumulative_confirmed"}}},
        LOOKUP_DEMOGRAPHICS,
        # take urban and total pop from demographics
        {"$addFields": {
            "population_urban": {"$arrayElemAt": ["$demographics.population_urban", 0]},
            "total_population": {"$arrayElemAt": ["$demographics.population", 0]}
        }},
        # calc proportion of urban per country and covid case numbers
        {"$addFields": {
            "urban_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$population_urban", "$total_population"]}, "else": None}},
            "confirmed_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$latest_cumulative_confirmed", "$total_population"]}, "else": None}}
        }},
        {"$match": {"urban_per_capita": {"$ne": None}, "confirmed_per_capita": {"$ne": None}}},
        {"$sort": {"urban_per_capita": sort_dir}},
        {"$limit": limit}
    ]
    return pd.DataFrame(list(get_col("epidemiology").aggregate(pipeline)))

# population density vs covid cases
# also very similar to above code
def query_density_covid(most_dense=True, limit=20):
    sort_dir = -1 if most_dense else 1 
    pipeline = [
        ADD_COUNTRY_CODE,
        {"$group": {"_id": "$country_code", "latest_cumulative_confirmed": {"$sum": "$cumulative_confirmed"}}},
        LOOKUP_DEMOGRAPHICS,
        {"$addFields": {
            "population_density": {"$arrayElemAt": ["$demographics.population_density", 0]},
            "total_population": {"$arrayElemAt": ["$demographics.population", 0]}
        }},
        {"$addFields": {
            "confirmed_per_capita": {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$latest_cumulative_confirmed", "$total_population"]}, "else": None}}
        }},
        {"$match": {"population_density": {"$ne": None}, "confirmed_per_capita": {"$ne": None}}},
        {"$sort": {"population_density": sort_dir}},
        {"$limit": limit}
    ]
    return pd.DataFrame(list(get_col("epidemiology").aggregate(pipeline)))

# function combining HDI val and death numbers bc they're similar queries
def query_hdi(high_hdi=True, use_deaths=False, limit=20):
    sort_dir = -1 if high_hdi else 1
    # choose either cases vs deaths depending on viz
    confirmed_field = "cumulative_deceased" if use_deaths else "cumulative_confirmed"
    result_field = "death_rate" if use_deaths else "confirmed_per_capita"
    pipeline = [
        ADD_COUNTRY_CODE,
        # sum number of cases/deaths per country
        {"$group": {"_id": "$country_code", "total_metric": {"$sum": f"${confirmed_field}"}}},
        LOOKUP_DEMOGRAPHICS,
        {"$addFields": {
            "hdi": {"$arrayElemAt": ["$demographics.human_development_index", 0]},
            "total_population": {"$arrayElemAt": ["$demographics.population", 0]},
        }},
        # divide by pop
        {"$addFields": {
            result_field: {"$cond": {"if": {"$gt": ["$total_population", 0]},
                "then": {"$divide": ["$total_metric", "$total_population"]}, "else": None}}
        }},
        {"$match": {"hdi": {"$ne": None}, result_field: {"$ne": None}}},
        {"$sort": {"hdi": sort_dir}},
        {"$limit": limit}
    ]
    return pd.DataFrame(list(get_col("epidemiology").aggregate(pipeline)))



# converts decimal to percentage for viz readability
def pct(val): return round(val * 100, 2)

# color pallete
# got from pallete chooser website
COLORS = {
    "cases": "#EF553B",    
    "elderly": "#636EFA",  
    "urban": "#00CC96",    
    "density": "#AB63FA",  
    "hdi_high": "#FFA15A", 
    "hdi_low":  "#19D3F3", 
    "deaths": "#FF6692"    
}

# dark layout and title function for making it look nice! just got from plotly dash website example
def dark_layout(title):
    return {
        "title": {"text": title, "font": {"size": 13}},
        "paper_bgcolor": "#1a1d27",   
        "plot_bgcolor": "#1a1d27",    
        "font": {"color": "#ccc"},
        "margin": {"l": 120, "r": 20, "t": 50, "b": 40},
        "xaxis": {"gridcolor": "#2a2d3a"},
        "yaxis": {"gridcolor": "#2a2d3a"},
    }

# toggle styles
def radio_style():
    return {"color": "#ccc", "marginBottom": "16px", "fontSize": "14px"}

def input_style():
    return {"marginRight": "6px", "marginLeft": "16px"}


# app layout

app = Dash(__name__)
app.title = "COVID-19 Over Different Demographics"

app.layout = html.Div(style={"fontFamily": "Inter, sans-serif", "backgroundColor": "#0f1117", "color": "#fff", "padding": "24px"}, children=[

    html.H1("COVID-19 Over Different Demographics",
            style={"textAlign": "center", "marginBottom": "8px", "fontSize": "28px"}),
    html.P("COVID-19's impact across populations worldwide based on elderly population, urban population, population density, and HDI index.",
           style={"textAlign": "center", "color": "#aaa", "marginBottom": "32px"}),
    html.P("These visualizations were created using Google's COVID-19 Open Data Repository. Links to the JSON files used can be downloaded below for further investigation.",
           style={"textAlign": "center", "color": "#aaa", "marginBottom": "32px"}),

    html.Div(style={"textAlign": "center", "marginBottom": "32px"}, children=[
        html.Span("Download data: ", style={"color": "#888", "fontSize": "13px"}),
        html.A(
            "Epidemiology JSON",
            href="database.epidemiology_normalized.json",
            download="database.epidemiology_normalized.json",  
            style={"color": "#636EFA", "fontSize": "13px", "marginRight": "16px"}
        ),
        html.A(
            "Demographics JSON",
            href="database.demographics_normalized.json",
            download="database.demographics_normalized.json",
            style={"color": "#636EFA", "fontSize": "13px"}
        ),
    ]),
    # elderly + covid
    html.H2("Elderly Population Analysis", style={"borderBottom": "1px solid #333", "paddingBottom": "8px"}),
    html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "32px"}, children=[
        dcc.Graph(id="chart-high-covid-elderly"),
        dcc.Graph(id="chart-high-elderly-covid"),
    ]),

    # urban + covid
    html.H2("Urban vs Non-Urban Population", style={"borderBottom": "1px solid #333", "paddingBottom": "8px"}),
    dcc.RadioItems(
        id="urban-toggle",
        options=[
            {"label": " Most Urban",  "value": "most"},
            {"label": " Least Urban", "value": "least"},
        ],
        value="most",       
        inline=True,
        style=radio_style(),
        inputStyle=input_style(),
    ),
    dcc.Graph(id="chart-urban"),
    html.P("In general, it appears that more urban populations tend to have more COVID cases. This is consistent with the natural spread of a respiratory disease like COVID: when an infected individual is exposed to more people, more people can get infected through particles in the air."),
    html.Div(style={"marginBottom": "32px"}),

    # density + covid
    html.H2("Population Density", style={"borderBottom": "1px solid #333", "paddingBottom": "8px"}),
    dcc.RadioItems(
        id="density-toggle",
        options=[
            {"label": " Most Dense",  "value": "most"},
            {"label": " Least Dense", "value": "least"},
        ],
        value="most",
        inline=True,
        style=radio_style(),
        inputStyle=input_style(),
    ),
    dcc.Graph(id="chart-density"),
    html.P("Interestingly, and possibly contradictory to the urban vs rural graph shown above, it appears that population density had little impact on COVID case numbers. Perhaps this has to do with number of urban centers, how the population is spread across the country, or overall population. More statistical analysis would be required to further investigate these findings."),
    html.Div(style={"marginBottom": "32px"}),

    # hdi + covid
    html.H2("Human Development Index — COVID Cases", style={"borderBottom": "1px solid #333", "paddingBottom": "8px"}),
    dcc.RadioItems(
        id="hdi-cases-toggle",
        options=[
            {"label": " Highest HDI", "value": "high"},
            {"label": " Lowest HDI",  "value": "low"},
        ],
        value="high",
        inline=True,
        style=radio_style(),
        inputStyle=input_style(),
    ),
    dcc.Graph(id="chart-hdi-cases"),
    html.P("These graphs show that, in general, higher HDI seems to correlate with more COVID cases. Human Development Index measures a country on three factors: life expectancy, knowledge/education, and decent standard of living. In general, a country with a higher HDI might have better hospitals and medical care resources, which may lead one to conclude that there should be fewer cases. However, countries with a higher HDI index may also have more urban centers where, as shown above, more COVID cases tend to accumulate. More statistical analysis must be done to further investigate."),
    html.Div(style={"marginBottom": "32px"}),

    # hdi + death
    html.H2("Human Development Index — Death Rates", style={"borderBottom": "1px solid #333", "paddingBottom": "8px"}),
    dcc.RadioItems(
        id="hdi-deaths-toggle",
        options=[
            {"label": " Highest HDI", "value": "high"},
            {"label": " Lowest HDI",  "value": "low"},
        ],
        value="high",
        inline=True,
        style=radio_style(),
        inputStyle=input_style(),
    ),
    dcc.Graph(id="chart-hdi-deaths"),
    html.P("Interestingly, from conclusions taken from the above graph, death rates appear to be inversely correlated with HDI index. Countries with higher HDI seem to have higher death rates, although not by a large factor. More statistical analysis must be done to understand the significance of these conclusions."),
    html.Div(style={"marginBottom": "32px"}),

    # allows the elderly charts to load properly without toggle
    dcc.Store(id="data-loaded"),
])


# callbacks query mongoDB and update charts
# much of the callback code below is the same, so i won't comment everything in detail 

@app.callback(Output("chart-high-covid-elderly", "figure"), Input("data-loaded", "data"))
def chart_high_covid_elderly(_):
    df = query_high_covid_elderly()
    # convert to percentages for readability
    df["elderly_pct"] = df["elderly_per_capita"].apply(pct)
    df["covid_pct"] = df["confirmed_per_capita"].apply(pct)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # bar graph
    fig.add_trace(go.Bar(y=df["_id"], x=df["covid_pct"], name="COVID Cases %", orientation="h",
                         marker_color=COLORS["cases"]), secondary_y=False)
    # scatter plot
    fig.add_trace(go.Scatter(y=df["_id"], x=df["elderly_pct"], name="Elderly %", mode="markers",
                             marker=dict(color=COLORS["elderly"], size=8, symbol="diamond")), secondary_y=True)
    fig.update_layout(**dark_layout("Top COVID Countries — Elderly Population %"))
    fig.update_xaxes(title_text="COVID Cases per Capita (%)")
    fig.update_yaxes(title_text="Elderly per Capita (%)", secondary_y=True)
    return fig

@app.callback(Output("chart-high-elderly-covid", "figure"), Input("data-loaded", "data"))
def chart_high_elderly_covid(_):
    df = query_high_elderly_covid()
    df["elderly_pct"] = df["elderly_per_capita"].apply(pct)
    df["covid_pct"] = df["confirmed_per_capita"].apply(pct)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(y=df["_id"], x=df["elderly_pct"], name="Elderly %", orientation="h",
                         marker_color=COLORS["elderly"]), secondary_y=False)
    fig.add_trace(go.Scatter(y=df["_id"], x=df["covid_pct"], name="COVID Cases %", mode="markers",
                             marker=dict(color=COLORS["cases"], size=8, symbol="diamond")), secondary_y=True)
    fig.update_layout(**dark_layout("Most Elderly Countries — COVID Cases per Capita"))
    fig.update_xaxes(title_text="Elderly per Capita (%)")
    fig.update_yaxes(title_text="COVID Cases per Capita (%)", secondary_y=True)
    return fig

# radio triggers which chart to display, urban vs rural population
@app.callback(Output("chart-urban", "figure"), Input("urban-toggle", "value"))
def chart_urban(toggle):
    # initiates radio button functionality
    most_urban = toggle == "most"
    df = query_urban_covid(most_urban=most_urban)
    df["urban_pct"] = df["urban_per_capita"].apply(pct)
    df["covid_pct"] = df["confirmed_per_capita"].apply(pct)
    label = "Most Urban" if most_urban else "Least Urban"
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(y=df["_id"], x=df["urban_pct"], name="Urban %", orientation="h",
                         marker_color=COLORS["urban"]), secondary_y=False)
    fig.add_trace(go.Scatter(y=df["_id"], x=df["covid_pct"], name="COVID %", mode="markers",
                             marker=dict(color=COLORS["cases"], size=8)), secondary_y=True)
    fig.update_layout(**dark_layout(f"{label} Countries — COVID Cases per Capita"))
    fig.update_xaxes(title_text="Urban Population %")
    fig.update_yaxes(title_text="COVID Cases per Capita (%)", secondary_y=True)
    return fig


@app.callback(Output("chart-density", "figure"), Input("density-toggle", "value"))
def chart_density(toggle):
    most_dense = toggle == "most"
    df = query_density_covid(most_dense=most_dense)
    df["covid_pct"] = df["confirmed_per_capita"].apply(pct)
    label = "Most Dense" if most_dense else "Least Dense"
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(y=df["_id"], x=df["population_density"], name="Density (per km²)", orientation="h",
                         marker_color=COLORS["density"]), secondary_y=False)
    fig.add_trace(go.Scatter(y=df["_id"], x=df["covid_pct"], name="COVID %", mode="markers",
                             marker=dict(color=COLORS["cases"], size=8)), secondary_y=True)
    fig.update_layout(**dark_layout(f"{label} Countries — COVID Cases per Capita"))
    fig.update_xaxes(title_text="Population Density (per km²)")
    fig.update_yaxes(title_text="COVID Cases per Capita (%)", secondary_y=True)
    return fig

# radio button not only changes high vs low HDI but also changes colors of bars based on value
@app.callback(Output("chart-hdi-cases", "figure"), Input("hdi-cases-toggle", "value"))
def chart_hdi_cases(toggle):
    high_hdi = toggle == "high"
    df = query_hdi(high_hdi=high_hdi, use_deaths=False)
    df["covid_pct"] = df["confirmed_per_capita"].apply(pct)
    label = "Highest HDI" if high_hdi else "Lowest HDI"
    # colors change with radio button
    color = COLORS["hdi_high"] if high_hdi else COLORS["hdi_low"] 
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(y=df["_id"], x=df["hdi"], name="HDI Score", orientation="h",
                         marker_color=color), secondary_y=False)
    fig.add_trace(go.Scatter(y=df["_id"], x=df["covid_pct"], name="COVID Cases %", mode="markers",
                             marker=dict(color=COLORS["cases"], size=8)), secondary_y=True)
    fig.update_layout(**dark_layout(f"{label} Countries — COVID Cases per Capita"))
    fig.update_xaxes(title_text="HDI Score")
    fig.update_yaxes(title_text="COVID Cases per Capita (%)", secondary_y=True)
    return fig

# same structure as above
@app.callback(Output("chart-hdi-deaths", "figure"), Input("hdi-deaths-toggle", "value"))
def chart_hdi_deaths(toggle):
    high_hdi = toggle == "high"
    df = query_hdi(high_hdi=high_hdi, use_deaths=True)
    df["death_pct"] = df["death_rate"].apply(pct)
    label = "Highest HDI" if high_hdi else "Lowest HDI"
    color = COLORS["hdi_high"] if high_hdi else COLORS["hdi_low"]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(y=df["_id"], x=df["hdi"], name="HDI Score", orientation="h",
                         marker_color=color), secondary_y=False)
    fig.add_trace(go.Scatter(y=df["_id"], x=df["death_pct"], name="Death Rate %", mode="markers",
                             marker=dict(color=COLORS["deaths"], size=8)), secondary_y=True)
    fig.update_layout(**dark_layout(f"{label} Countries — Death Rates"))
    fig.update_xaxes(title_text="HDI Score")
    fig.update_yaxes(title_text="Death Rate per Capita (%)", secondary_y=True)
    return fig

if __name__ == "__main__":
    app.run(debug=True)