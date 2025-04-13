document.addEventListener('DOMContentLoaded', () => {
    const svg = d3.select("svg");
    const width = +svg.attr("width");
    const height = +svg.attr("height");
    const loadingIndicator = d3.select("#loading");

    // Tooltip div (optional, but nice)
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("position", "absolute")
        .style("visibility", "hidden")
        .style("background", "rgba(0, 0, 0, 0.7)")
        .style("color", "#fff")
        .style("padding", "5px 10px")
        .style("border-radius", "3px")
        .style("font-size", "12px");

    // Fetch the graph data
    d3.json("data/as_graph.json").then(function(graph) {
        loadingIndicator.style("display", "none"); // Hide loading indicator
        svg.style("display", "block"); // Show SVG

        console.log(`Loaded graph: ${graph.nodes.length} nodes, ${graph.links.length} links`);

        if (!graph.nodes.length) {
            console.error("No nodes found in the graph data.");
            loadingIndicator.text("Error: No data to display.").style("display", "block");
            return;
        }

        // --- D3 Force Simulation ---
        const simulation = d3.forceSimulation(graph.nodes)
            .force("link", d3.forceLink(graph.links).id(d => d.id).distance(30).strength(0.1)) // Adjust distance/strength
            .force("charge", d3.forceManyBody().strength(-20)) // Adjust charge strength
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collide", d3.forceCollide().radius(6)); // Prevent node overlap


        // --- Create SVG Elements ---
        const link = svg.append("g")
            .attr("class", "links")
            .selectAll("line")
            .data(graph.links)
            .enter().append("line");

        const node = svg.append("g")
            .attr("class", "nodes")
            .selectAll("circle")
            .data(graph.nodes)
            .enter().append("circle")
            .attr("r", 4) // Node radius
            .call(drag(simulation)) // Enable dragging
            .on("mouseover", (event, d) => {
                tooltip.style("visibility", "visible").text(`AS: ${d.id}`);
            })
            .on("mousemove", (event) => {
                tooltip.style("top", (event.pageY - 10) + "px").style("left", (event.pageX + 10) + "px");
            })
            .on("mouseout", () => {
                tooltip.style("visibility", "hidden");
            });

        // Optional: Add labels to nodes (can be slow for many nodes)
        // const label = svg.append("g")
        //     .attr("class", "labels")
        //     .selectAll("text")
        //     .data(graph.nodes)
        //     .enter().append("text")
        //     .attr("class", "label")
        //     .text(d => d.id)
        //     .attr("dx", 8)
        //     .attr("dy", ".35em");

        // --- Simulation Ticks ---
        simulation.on("tick", () => {
            link
                .attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            node
                .attr("cx", d => d.x)
                .attr("cy", d => d.y);

            // Update label positions if enabled
            // label
            //     .attr("x", d => d.x)
            //     .attr("y", d => d.y);
        });

        // --- Drag Functionality ---
        function drag(simulation) {
            function dragstarted(event, d) {
                if (!event.active) simulation.alphaTarget(0.3).restart();
                d.fx = d.x;
                d.fy = d.y;
            }

            function dragged(event, d) {
                d.fx = event.x;
                d.fy = event.y;
            }

            function dragended(event, d) {
                if (!event.active) simulation.alphaTarget(0);
                d.fx = null; // Let simulation take over again
                d.fy = null;
            }

            return d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended);
        }

    }).catch(function(error) {
        console.error("Error loading or processing graph data:", error);
        loadingIndicator.text(`Error loading data: ${error.message}`).style("color", "red");
        svg.style("display", "none"); // Hide SVG on error
    });
});
