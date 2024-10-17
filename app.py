import streamlit as st
import json
from graphviz import Digraph
import pandas as pd
import warnings

# Suppress deprecation warnings in Python (though it won't hide Streamlit warnings in UI)
warnings.filterwarnings("ignore", category=DeprecationWarning)

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

        for db_lineage in upstream_column.get('dblineage', []):
            lineage_node = build_db_lineage(db_lineage, set())
            if lineage_node:
                column_node.add_child(lineage_node)

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
def create_graph(node, theme, metadata_buttons):
    dot = Digraph(comment='Data Lineage')
    dot.attr('graph', bgcolor=theme.bgcolor, rankdir='LR')
    dot.attr('node', style=theme.style, shape=theme.shape, fillcolor=theme.fillcolor,
              color=theme.color, fontcolor=theme.tcolor, width='2.16', height='0.72')
    dot.attr('edge', color=theme.pencolor, penwidth=theme.penwidth)

    def add_nodes_edges(current_node):
        label = f"{current_node.name}"
        if current_node.table_name:
            label += f"\n({current_node.table_name})"
        elif current_node.type != 'Field':
            label += f"\n({current_node.type})"
        label += f"\n\n{current_node.lineage_type}"

        dot.node(current_node.name + current_node.table_name, label=label)
        metadata_buttons[current_node.name] = current_node.get_metadata()

        for child in current_node.children:
            dot.edge(current_node.name + current_node.table_name, child.name + child.table_name)
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

# Function to get predefined themes
def getThemes():
    return {
        "Default": Theme("#6c6c6c", "#e0e0e0", "#ffffff", "#000000", "filled", "box", "#696969", "1"),
        "Blue": Theme("#1a5282", "#d3dcef", "#ffffff", "#000000", "filled", "ellipse", "#0078d7", "2"),
        "Dark": Theme("#ffffff", "#333333", "#000000", "#ffffff", "filled", "box", "#ffffff", "1"),
    }

# Streamlit app starts here
st.set_page_config(layout="wide")
st.title('Data Lineage Visualization')

# Sidebar options
st.sidebar.header('Configuration')
themes = getThemes()
theme_name = st.sidebar.selectbox('Select Theme', list(themes.keys()), index=0)
theme = themes[theme_name]

# Load and parse the JSON data
with st.spinner('Loading lineage data...'):
    try:
        with open('combined_lineage.json', 'r') as f:
            lineage_data = json.load(f)
    except Exception as e:
        st.error(f"Error loading JSON file: {e}")
        st.stop()

# Extract available workbooks
workbook_names = [workbook['name'] for workbook in lineage_data.get('workbooks', [])]
selected_workbook = st.sidebar.selectbox('Select a Workbook', workbook_names)

# Find the selected workbook data
selected_workbook_data = next((workbook for workbook in lineage_data.get('workbooks', []) if workbook['name'] == selected_workbook), None)
if not selected_workbook_data:
    st.error("No data found for the selected workbook.")
    st.stop()

# Extract dashboards
dashboard_names = [dashboard['name'] for dashboard in selected_workbook_data.get('dashboards', [])]
selected_dashboard = st.sidebar.selectbox('Select a Dashboard', dashboard_names)

# Find the selected dashboard data
selected_dashboard_data = next((dashboard for dashboard in selected_workbook_data.get('dashboards', []) if dashboard['name'] == selected_dashboard), None)
if not selected_dashboard_data:
    st.error("No data found for the selected dashboard.")
    st.stop()

# Extract sheets
sheet_names = [sheet['name'] for sheet in selected_dashboard_data.get('sheets', [])]
selected_sheet = st.sidebar.selectbox('Select a Sheet', sheet_names)

# Find the selected sheet data
selected_sheet_data = next((sheet for sheet in selected_dashboard_data.get('sheets', []) if sheet['name'] == selected_sheet), None)
if not selected_sheet_data:
    st.error("No data found for the selected sheet.")
    st.stop()

# Extract fields from the selected sheet
fields = selected_sheet_data.get('sheetFieldInstances', [])
if not fields:
    st.write("No fields available for the selected sheet.")
else:
    for field in fields:
        st.write(f"### {field['name']}")
        
        # Expander for lineage graph
        metadata_buttons = {}
        with st.expander("Show Lineage Graph"):
            selected_node = build_lineage_tree(field)
            dot = create_graph(selected_node, theme, metadata_buttons)
            st.graphviz_chart(dot, use_container_width=True)

            # Create columns dynamically based on number of buttons (nodes)
            num_buttons = len(metadata_buttons)
            cols = st.columns(num_buttons)  # Create 'num_buttons' number of columns

            # Place each button in a respective column
            for i, (node_name, metadata) in enumerate(metadata_buttons.items()):
                with cols[i]:  # Place each button in its column
                    if st.button("Show Metadata", key=f"metadata_{node_name}_{field['name']}", use_container_width=False):
                        metadata_df = pd.DataFrame(list(metadata.items()), columns=['Field', 'Value'])
                        st.table(metadata_df)
