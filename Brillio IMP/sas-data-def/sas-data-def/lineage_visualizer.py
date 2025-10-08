"""
Dataset Lineage Visualization Module

This module provides various visualization options for dataset lineage graphs:
- Interactive network graphs using NetworkX + Matplotlib
- Hierarchical flow diagrams 
- Text-based tree visualization
- Export options for external tools
"""

import json
import matplotlib.pyplot as plt
import networkx as nx
from typing import Dict, List, Any, Optional, Tuple
import matplotlib.patches as mpatches
from dataclasses import asdict

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("Plotly not available. Install with: pip install plotly")

from llm_lineage_generator import DatasetLineage, LineageNode, LineageRelationship


class LineageVisualizer:
    """Visualize dataset lineage graphs in various formats"""
    
    def __init__(self, lineage: DatasetLineage):
        self.lineage = lineage
        self.graph = self._build_networkx_graph()
    
    def _build_networkx_graph(self) -> nx.DiGraph:
        """Build NetworkX directed graph from lineage"""
        G = nx.DiGraph()
        
        # Add nodes with attributes
        for node in self.lineage.nodes:
            G.add_node(
                node.dataset_name,
                dataset_type=node.dataset_type,
                created_by=node.created_by,
                business_logic=node.business_logic,
                variables_created=node.variables_created,
                transformations=node.transformations
            )
        
        # Add edges with attributes
        for rel in self.lineage.relationships:
            G.add_edge(
                rel.source_dataset,
                rel.target_dataset,
                transformation_type=rel.transformation_type,
                description=rel.transformation_description,
                variables_passed=rel.variables_passed
            )
        
        return G
    
    def plot_network_graph(self, 
                          figsize: Tuple[int, int] = (16, 12),
                          layout: str = 'hierarchical',
                          show_labels: bool = True,
                          show_edge_labels: bool = True,
                          save_path: Optional[str] = None) -> None:
        """
        Create a network graph visualization using matplotlib
        
        Args:
            figsize: Figure size (width, height)
            layout: Layout algorithm ('hierarchical', 'spring', 'circular', 'shell')
            show_labels: Whether to show node labels
            show_edge_labels: Whether to show edge transformation labels
            save_path: Path to save the figure
        """
        
        fig, ax = plt.subplots(figsize=figsize)
        
        # Choose layout
        if layout == 'hierarchical':
            pos = self._hierarchical_layout()
        elif layout == 'spring':
            pos = nx.spring_layout(self.graph, k=3, iterations=50, seed=42)
        elif layout == 'circular':
            pos = nx.circular_layout(self.graph)
        elif layout == 'shell':
            # Group nodes by type for shell layout
            source_nodes = [n for n in self.graph.nodes() 
                           if self.graph.nodes[n]['dataset_type'] == 'source']
            intermediate_nodes = [n for n in self.graph.nodes() 
                                if self.graph.nodes[n]['dataset_type'] == 'intermediate']
            final_nodes = [n for n in self.graph.nodes() 
                          if self.graph.nodes[n]['dataset_type'] == 'final']
            nlist = [source_nodes, intermediate_nodes, final_nodes]
            pos = nx.shell_layout(self.graph, nlist=nlist)
        else:
            pos = nx.spring_layout(self.graph)
        
        # Color nodes by type
        node_colors = []
        node_sizes = []
        for node in self.graph.nodes():
            node_type = self.graph.nodes[node]['dataset_type']
            if node_type == 'source':
                node_colors.append('#E8F4FD')  # Light blue
                node_sizes.append(3000)
            elif node_type == 'intermediate':
                node_colors.append('#FFF2CC')  # Light yellow
                node_sizes.append(4000)
            elif node_type == 'final':
                node_colors.append('#E1F5FE')  # Light green
                node_sizes.append(3500)
            else:
                node_colors.append('#F5F5F5')  # Light gray
                node_sizes.append(2000)
        
        # Draw nodes
        nx.draw_networkx_nodes(
            self.graph, pos, 
            node_color=node_colors,
            node_size=node_sizes,
            edgecolors='black',
            linewidths=2,
            alpha=0.9
        )
        
        # Draw edges with different styles based on transformation type
        edge_styles = {
            'data_step': '-',
            'sql_join': '--',
            'aggregation': '-.',
            'filtering': ':'
        }
        
        edge_colors = {
            'data_step': '#2E86AB',
            'sql_join': '#A23B72',
            'aggregation': '#F18F01',
            'filtering': '#C73E1D'
        }
        
        for edge in self.graph.edges():
            source, target = edge
            edge_data = self.graph.edges[edge]
            transform_type = edge_data.get('transformation_type', 'data_step')
            
            nx.draw_networkx_edges(
                self.graph, pos,
                edgelist=[edge],
                edge_color=edge_colors.get(transform_type, '#666666'),
                style=edge_styles.get(transform_type, '-'),
                width=2,
                alpha=0.7,
                arrowsize=20,
                arrowstyle='->'
            )
        
        # Add node labels
        if show_labels:
            labels = {}
            for node in self.graph.nodes():
                # Truncate long names for display
                name = node
                if len(name) > 15:
                    name = name[:12] + "..."
                labels[node] = name
            
            nx.draw_networkx_labels(
                self.graph, pos, labels,
                font_size=10, font_weight='bold'
            )
        
        # Add edge labels
        if show_edge_labels:
            edge_labels = {}
            for edge in self.graph.edges():
                edge_data = self.graph.edges[edge]
                transform_type = edge_data.get('transformation_type', '')
                edge_labels[edge] = transform_type.replace('_', ' ').title()
            
            nx.draw_networkx_edge_labels(
                self.graph, pos, edge_labels,
                font_size=8, alpha=0.8
            )
        
        # Create legend
        legend_elements = [
            mpatches.Patch(color='#E8F4FD', label='Source Datasets', ec='black'),
            mpatches.Patch(color='#FFF2CC', label='Intermediate Datasets', ec='black'),
            mpatches.Patch(color='#E1F5FE', label='Final Datasets', ec='black'),
            mpatches.Patch(color='#2E86AB', label='Data Step', ec='none'),
            mpatches.Patch(color='#A23B72', label='SQL Join', ec='none'),
            mpatches.Patch(color='#F18F01', label='Aggregation', ec='none'),
            mpatches.Patch(color='#C73E1D', label='Filtering', ec='none')
        ]
        
        ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1, 1))
        
        ax.set_title(f'Dataset Lineage Graph\n{self.lineage.flow_description}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.axis('off')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Graph saved to: {save_path}")
        
        plt.show()
    
    def _hierarchical_layout(self) -> Dict[str, Tuple[float, float]]:
        """Create hierarchical layout with sources at top, final at bottom"""
        pos = {}
        
        # Group nodes by type
        source_nodes = [n for n in self.graph.nodes() 
                       if self.graph.nodes[n]['dataset_type'] == 'source']
        intermediate_nodes = [n for n in self.graph.nodes() 
                            if self.graph.nodes[n]['dataset_type'] == 'intermediate']
        final_nodes = [n for n in self.graph.nodes() 
                      if self.graph.nodes[n]['dataset_type'] == 'final']
        
        # Position sources at top
        for i, node in enumerate(source_nodes):
            pos[node] = (i * 3, 2)
        
        # Position intermediates in middle (may have multiple levels)
        intermediate_levels = self._get_intermediate_levels(intermediate_nodes)
        for level, nodes in enumerate(intermediate_levels):
            y_pos = 1 - (level * 0.5)
            for i, node in enumerate(nodes):
                pos[node] = (i * 2 + 1, y_pos)
        
        # Position finals at bottom
        for i, node in enumerate(final_nodes):
            pos[node] = (i * 3 + 1, 0)
        
        return pos
    
    def _get_intermediate_levels(self, intermediate_nodes: List[str]) -> List[List[str]]:
        """Group intermediate nodes by their dependency levels"""
        levels = []
        remaining = set(intermediate_nodes)
        processed = set()
        
        # Add source nodes to processed (they are at level 0)
        source_nodes = [n for n in self.graph.nodes() 
                       if self.graph.nodes[n]['dataset_type'] == 'source']
        processed.update(source_nodes)
        
        while remaining:
            current_level = []
            for node in list(remaining):
                # Check if all predecessors are processed
                predecessors = set(self.graph.predecessors(node))
                if predecessors.issubset(processed):
                    current_level.append(node)
            
            if not current_level:
                # Handle cycles or disconnected components
                current_level = [remaining.pop()]
            
            levels.append(current_level)
            processed.update(current_level)
            remaining -= set(current_level)
        
        return levels
    
    def plot_interactive_graph(self, save_path: Optional[str] = None) -> None:
        """Create interactive graph using Plotly (if available)"""
        if not PLOTLY_AVAILABLE:
            print("Plotly not available. Using matplotlib fallback...")
            self.plot_network_graph()
            return
        
        pos = self._hierarchical_layout()
        
        # Extract node information
        node_x = []
        node_y = []
        node_text = []
        node_info = []
        node_colors = []
        
        for node in self.graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            node_data = self.graph.nodes[node]
            node_type = node_data['dataset_type']
            
            # Color mapping
            color_map = {
                'source': '#E8F4FD',
                'intermediate': '#FFF2CC',
                'final': '#E1F5FE'
            }
            node_colors.append(color_map.get(node_type, '#F5F5F5'))
            
            # Node text and hover info
            node_text.append(node)
            
            transformations = node_data.get('transformations', [])
            variables = node_data.get('variables_created', [])
            
            hover_text = f"""
            <b>{node}</b><br>
            Type: {node_type}<br>
            Created by: {node_data['created_by']}<br>
            Variables: {', '.join(variables[:3])}{'...' if len(variables) > 3 else ''}<br>
            Business Logic: {node_data['business_logic'][:100]}...
            """
            node_info.append(hover_text)
        
        # Extract edge information
        edge_x = []
        edge_y = []
        edge_info = []
        
        for edge in self.graph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
            
            edge_data = self.graph.edges[edge]
            edge_info.append(f"{edge[0]} → {edge[1]}: {edge_data.get('transformation_type', '')}")
        
        # Create figure
        fig = go.Figure()
        
        # Add edges
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=2, color='#888'),
            hoverinfo='none',
            showlegend=False
        ))
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            marker=dict(
                size=20,
                color=node_colors,
                line=dict(width=2, color='black')
            ),
            text=node_text,
            textposition="middle center",
            hovertemplate='%{customdata}<extra></extra>',
            customdata=node_info,
            showlegend=False
        ))
        
        fig.update_layout(
            title=f'Interactive Dataset Lineage Graph<br><sub>{self.lineage.flow_description}</sub>',
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            annotations=[ dict(
                text="Hover over nodes for details",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002, xanchor='left', yanchor='bottom',
                font=dict(color="#888", size=12)
            )],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=600,
            width=1000
        )
        
        if save_path:
            fig.write_html(save_path)
            print(f"Interactive graph saved to: {save_path}")
        
        fig.show()
    
    def print_text_tree(self) -> None:
        """Print ASCII tree representation of lineage"""
        print("=" * 80)
        print("📊 DATASET LINEAGE TREE")
        print("=" * 80)
        
        # Group nodes by type
        source_nodes = [n for n in self.lineage.nodes if n.dataset_type == 'source']
        
        def print_descendants(node_name: str, level: int = 0, visited: set = None):
            if visited is None:
                visited = set()
            
            if node_name in visited:
                return
            visited.add(node_name)
            
            # Print current node
            indent = "  " * level
            symbol = "📥" if level == 0 else "⚙️" if level < 3 else "📤"
            
            node = next((n for n in self.lineage.nodes if n.dataset_name == node_name), None)
            if node:
                print(f"{indent}{symbol} {node_name}")
                print(f"{indent}   📝 {node.business_logic[:60]}...")
                
                if node.variables_created:
                    vars_text = ", ".join(node.variables_created[:3])
                    if len(node.variables_created) > 3:
                        vars_text += "..."
                    print(f"{indent}   🧬 Variables: {vars_text}")
            else:
                print(f"{indent}{symbol} {node_name}")
            
            # Print descendants
            descendants = [rel.target_dataset for rel in self.lineage.relationships 
                          if rel.source_dataset == node_name]
            
            for i, descendant in enumerate(descendants):
                is_last = i == len(descendants) - 1
                connector = "└── " if is_last else "├── "
                print(f"{indent}{connector}", end="")
                
                # Find transformation info
                rel = next((r for r in self.lineage.relationships 
                           if r.source_dataset == node_name and r.target_dataset == descendant), None)
                if rel:
                    print(f"[{rel.transformation_type}] ", end="")
                
                print_descendants(descendant, level + 1, visited.copy())
        
        # Start from source nodes
        for source in source_nodes:
            print_descendants(source.dataset_name)
            print()
    
    def export_to_json(self, filepath: str) -> None:
        """Export lineage to JSON format for external tools"""
        export_data = {
            "metadata": {
                "total_nodes": len(self.lineage.nodes),
                "total_relationships": len(self.lineage.relationships),
                "flow_description": self.lineage.flow_description
            },
            "nodes": [asdict(node) for node in self.lineage.nodes],
            "relationships": [asdict(rel) for rel in self.lineage.relationships],
            "variable_lineage": self.lineage.variable_lineage,
            "graph_structure": {
                "adjacency_list": dict(self.graph.adjacency()),
                "node_attributes": dict(self.graph.nodes(data=True)),
                "edge_attributes": dict(self.graph.edges(data=True))
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"Lineage exported to: {filepath}")
    
    def export_to_mermaid(self, filepath: str) -> None:
        """Export to Mermaid diagram format for documentation"""
        lines = ["flowchart TD"]
        
        # Add nodes with styling
        for node in self.lineage.nodes:
            node_id = node.dataset_name.replace('.', '_').replace(' ', '_')
            
            if node.dataset_type == 'source':
                lines.append(f'    {node_id}["{node.dataset_name}"]')
                lines.append(f'    {node_id} --> {node_id}_style')
                lines.append(f'    {node_id}_style{{Source Dataset}}')
            elif node.dataset_type == 'final':
                lines.append(f'    {node_id}["{node.dataset_name}"]')
                lines.append(f'    {node_id} --> {node_id}_style')  
                lines.append(f'    {node_id}_style{{Final Dataset}}')
            else:
                lines.append(f'    {node_id}["{node.dataset_name}"]')
        
        # Add relationships
        for rel in self.lineage.relationships:
            source_id = rel.source_dataset.replace('.', '_').replace(' ', '_')
            target_id = rel.target_dataset.replace('.', '_').replace(' ', '_')
            
            transform_label = rel.transformation_type.replace('_', ' ').title()
            lines.append(f'    {source_id} -->|{transform_label}| {target_id}')
        
        # Add styling
        lines.extend([
            "",
            "    classDef sourceClass fill:#e8f4fd,stroke:#1e88e5,stroke-width:2px",
            "    classDef intermediateClass fill:#fff2cc,stroke:#fb8c00,stroke-width:2px", 
            "    classDef finalClass fill:#e1f5fe,stroke:#00acc1,stroke-width:2px"
        ])
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"Mermaid diagram exported to: {filepath}")


def visualize_lineage(lineage: DatasetLineage, 
                     style: str = 'network',
                     interactive: bool = False,
                     save_path: Optional[str] = None) -> None:
    """
    Convenience function to visualize dataset lineage
    
    Args:
        lineage: DatasetLineage object to visualize
        style: Visualization style ('network', 'tree', 'both')  
        interactive: Use interactive Plotly visualization
        save_path: Path to save visualization files
    """
    visualizer = LineageVisualizer(lineage)
    
    if style in ['network', 'both']:
        if interactive:
            html_path = save_path.replace('.png', '.html') if save_path else None
            visualizer.plot_interactive_graph(html_path)
        else:
            visualizer.plot_network_graph(save_path=save_path)
    
    if style in ['tree', 'both']:
        visualizer.print_text_tree()
    
    # Always export JSON for external tools
    if save_path:
        json_path = save_path.replace('.png', '.json').replace('.html', '.json')
        visualizer.export_to_json(json_path)
        
        mermaid_path = save_path.replace('.png', '.md').replace('.html', '.md')
        visualizer.export_to_mermaid(mermaid_path)


# Example usage function
def demo_visualization():
    """Demo function showing different visualization options"""
    # This would use actual lineage from your generator
    print("Demo: Different ways to visualize dataset lineage")
    print("1. Network Graph (matplotlib)")
    print("2. Interactive Graph (plotly)")
    print("3. Text Tree")
    print("4. Export to JSON/Mermaid")