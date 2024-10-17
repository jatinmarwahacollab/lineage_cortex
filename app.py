import streamlit as st
import json
from graphviz import Digraph
import warnings

# Ensure page config is the first Streamlit command
st.set_page_config(layout="wide")

# Suppress deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# CSS to change the multi-select color
st.markdown(
    """
    <style>
    div[data-baseweb="select"] {
        background-color: #f0f0f0;
    }
    div[data-baseweb="select"] > div {
        background-color: #f0f0f0;
    }
    ul[role="listbox"] {
        background-color: #f0f0f0;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Define the Node class to represent each entity in the lineage
class Node:
    def __init__(self, name, node_type, table_name='', description='', reasoning='', formula='', lineage_type=''):
        self.name = name
        self.type = node_type
        self.table_name = table_name
        self.description = description
        self.reasoning = reasoning
        self.formula = formula
        self.lineage_type = lineage_type
        self.children = []  # List of child nodes

    def add_child(self, child_node):
        self.children.append(child_node)

    def get_metadata(self):
        """Returns metadata for the node in a dictionary format."""
        return {
            'Name': self.name,
            'Type': self.type,
            'Table': self.table_name,
            'Description': self.description,
            'Reasoning': self.reasoning,
            'Formula': self.formula,
            'Lineage Type': self.lineage_type
        }

# Function to build the lineage tree from the JSON data
def build_lineage_tree(field_data):
    root_node = Node(
        name=field_data['name'],
        node_type='Field',
        formula=field_data.get('formula', ''),
        lineage_type='Reporting Side Lineage'
    )

    for upstream_column in field_data.get('upstreamColumns', []):
        column_name = upstream_column['name']
        table_name = ', '.join([table['name'] for table in upstream_column.get('upstreamTables', [])])

        column_node = Node(
            name=column_name,
            node_type='Column',
            table_name=table_name,
            lineage_type='Reporting Side Lineage'
        )
        root_node.add_child(column_node)

        db_lineage = upstream_column.get('database_lineage', None)
        if db_lineage:
            lineage_node = build_db_lineage(db_lineage, set())
            if lineage_node:
                column_node.add_child(lineage_node)

    for upstream_field in field_data.get('upstreamFields', []):
        upstream_field_node = build_lineage_tree(upstream_field)
        root_node.add_child(upstream_field_node)

    return root_node

# Function to build lineage nodes recursively from dblineage part
def build_db_lineage(db_lineage, visited):
    node_id = f"{db_lineage['model']}.{db_lineage['column']}"
    if node_id in visited:
        return None
    visited.add(node_id)

    node = Node(
        name=db_lineage['column'],
        node_type='DB Column',
        table_name=db_lineage.get('model', ''),
        description=db_lineage.get('column Description', ''),
        reasoning=db_lineage.get('reasoning', ''),
        lineage_type='Database Side Lineage'
    )

    for upstream_model in db_lineage.get('upstream_models', []):
        upstream_node = build_db_lineage(upstream_model, visited)
        if upstream_node:
            node.add_child(upstream_node)

    return node

# Function to create the lineage graph using Graphviz
def create_graph(node, theme):
    dot = Digraph(comment='Data Lineage')
    dot.attr('graph', bgcolor=theme.bgcolor, rankdir='LR')  # Set layout to left-to-right
    dot.attr('node', style=theme.style, shape=theme.shape, fillcolor=theme.fillcolor,
              color=theme.color, fontcolor=theme.tcolor, width='2.5', height='1.0')  # Increased size slightly
    dot.attr('edge', color=theme.pencolor, penwidth=theme.penwidth)

    def add_nodes_edges(current_node):
        label = f"{current_node.name}"
        if current_node.table_name:
            label += f"\n({current_node.table_name})"
        elif current_node.type != 'Field':
            label += f"\n({current_node.type})"

        label += f"\n\n{current_node.lineage_type}"

        metadata = current_node.get_metadata()
        hover_text = "\n".join(f"{key}: {value}" for key, value in metadata.items())

        node_id = f"{current_node.name}_{current_node.table_name}_{current_node.lineage_type}"
        dot.node(node_id, label=label, tooltip=hover_text)

        for child in current_node.children:
            child_id = f"{child.name}_{child.table_name}_{child.lineage_type}"
            dot.edge(node_id, child_id)
            add_nodes_edges(child)

    add_nodes_edges(node)
    return dot

# Theme class to define visual styles
class Theme:
    def __init__(self, color, fillcolor, bgcolor, tcolor, style, shape, pencolor, penwidth):
        self.color = color
        self.fillcolor = fillcolor
        self.bgcolor = bgcolor
        self.tcolor = tcolor
        self.style = style
        self.shape = shape
        self.pencolor = pencolor
        self.penwidth = penwidth

def getThemes():
    return {
        "Default": Theme("#6c6c6c", "#e0e0e0", "#ffffff", "#000000", "filled", "box", "#696969", "1"),
        "Blue": Theme("#1a5282", "#d3dcef", "#ffffff", "#000000", "filled", "ellipse", "#0078d7", "2"),
        "Dark": Theme("#ffffff", "#333333", "#000000", "#ffffff", "filled", "box", "#ffffff", "1"),
    }

st.title('Data Lineage Visualization')

st.sidebar.header('Configuration')
themes = getThemes()
theme_name = st.sidebar.selectbox('Select Theme', list(themes.keys()), index=0)
theme = themes[theme_name]

with st.spinner('Loading lineage data...'):
    try:
        with open('combined_lineage.json', 'r') as f:
            lineage_data = json.load(f)
    except Exception as e:
        st.error(f"Error loading JSON file: {e}")
        st.stop()

workbook_names = [wb['name'] for wb in lineage_data.get('workbooks', [])]
selected_workbook = st.sidebar.selectbox('Select a Workbook', workbook_names)
selected_workbook_data = next(wb for wb in lineage_data['workbooks'] if wb['name'] == selected_workbook)

dashboard_names = [db['name'] for db in selected_workbook_data['dashboards']]
selected_dashboard = st.sidebar.selectbox('Select a Dashboard', dashboard_names)
selected_dashboard_data = next(db for db in selected_workbook_data['dashboards'] if db['name'] == selected_dashboard)

datasource_names = [ds['name'] for ds in selected_dashboard_data['upstreamDatasources']]
selected_datasource = st.sidebar.selectbox('Select a Datasource', datasource_names)
selected_datasource_data = next(ds for ds in selected_dashboard_data['upstreamDatasources'] if ds['name'] == selected_datasource)

sheet_names = [sheet['name'] for sheet in selected_datasource_data['sheets']]
selected_sheet = st.sidebar.selectbox('Select a Sheet', sheet_names)
selected_sheet_data = next(sheet for sheet in selected_datasource_data['sheets'] if sheet['name'] == selected_sheet)

fields = selected_sheet_data.get('upstreamFields', [])
field_names = [field['name'] for field in fields]
selected_fields = st.sidebar.multiselect('Select Fields', field_names, default=field_names)

for field in fields:
    if field['name'] in selected_fields:
        with st.expander(f"{field['name']}", expanded=True):
            selected_node = build_lineage_tree(field)
            dot = create_graph(selected_node, theme)
            st.graphviz_chart(dot, use_container_width=True)
