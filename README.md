# mrt-viz
# MRT Internet AS Topology Visualization

This project collects a BGP routing table snapshot (MRT `bview` file) from a public route collector (RIPE RIS), parses it to extract AS path information, analyzes adjacent AS pairs to build a basic network graph, and visualizes this graph using D3.js. The resulting static website is deployed automatically to Netlify via GitHub Actions.

**Live Demo:** [Link to your Netlify site will go here]

## Features

*   Downloads MRT data from RIPE RIS.
*   Parses MRT files using `pybgpdump`.
*   Builds an AS adjacency graph (nodes are ASNs, links represent adjacent ASes in a path).
*   Visualizes the AS graph using D3.js force-directed layout.
*   Interactive graph (drag nodes, hover for AS number).
*   Automated deployment to Netlify on push to `main` branch.
