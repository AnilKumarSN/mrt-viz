import requests
import gzip
import shutil
import pandas as pd
import json
import os
from pybgpdump import BGPDump # Make sure pybgpdump is installed
from datetime import datetime, timedelta

# --- Configuration ---
# Use a specific RIPE collector (e.g., rrc00)
# NOTE: Check RIPE/RouteViews for current file paths and availability!
# Using a fixed historical date for reproducibility in this example.
# Adjust the date or use dynamic dates as needed.
TARGET_DATE = datetime(2023, 10, 1) # Example fixed date
RRC_COLLECTOR = "rrc00"
BASE_URL = f"https://data.ris.ripe.net/{RRC_COLLECTOR}/"
DATE_PATH = TARGET_DATE.strftime('%Y.%m')
# Use bview (snapshot) file. Format: bview.YYYYMMDD.HHMM.gz
# Fetching the 00:00 snapshot for the example date
MRT_FILE_NAME_GZ = f"bview.{TARGET_DATE.strftime('%Y%m%d')}.0000.gz"
MRT_URL = f"{BASE_URL}{DATE_PATH}/{MRT_FILE_NAME_GZ}"

DOWNLOAD_PATH_GZ = f"{MRT_FILE_NAME_GZ}"
DECOMPRESSED_PATH = "latest_dump.mrt"
PARSED_JSON_PATH = "parsed_routes.json" # Temporary intermediate file
OUTPUT_GRAPH_DIR = "site/data"
OUTPUT_GRAPH_PATH = os.path.join(OUTPUT_GRAPH_DIR, "as_graph.json")

# --- Helper Functions ---
def download_mrt_file(url, dest_path):
    """Downloads the MRT file."""
    print(f"Attempting to download MRT file from: {url}")
    try:
        response = requests.get(url, stream=True, timeout=120) # Increased timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {os.path.basename(dest_path)}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return False

def decompress_mrt_file(gz_path, dest_path):
    """Decompresses a gzipped file."""
    print(f"Decompressing {gz_path} to {dest_path}...")
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(dest_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Decompression successful.")
        return True
    except Exception as e:
        print(f"Error during decompression: {e}")
        return False

def parse_mrt_to_json(mrt_path, json_path):
    """Parses MRT dump file focusing on prefix and AS path."""
    print(f"Parsing MRT file: {mrt_path}...")
    records = []
    try:
        # Use 'with open' for BGPDump to ensure file handle is closed
        with open(mrt_path, "rb") as mrt_file_handle:
            dump = BGPDump(file_handle=mrt_file_handle) # Pass file handle
            count = 0
            for entry in dump:
                # Process TABLE_DUMP_V2 RIB entries (IPv4 and IPv6)
                if entry.type in [16, 17] and entry.subtype in [1, 2]: # TABLE_DUMP_V2, PEER_INDEX_TABLE (subtype doesn't matter much here) / RIB_IPV4_UNICAST / RIB_IPV6_UNICAST
                     # Iterate through RIB entries associated with the peer entry
                    for rib_entry in entry.rib_entries:
                        # Check if as_path exists and is not None
                        if hasattr(rib_entry, 'as_path') and rib_entry.as_path:
                            record = {
                                "prefix": str(rib_entry.prefix) if hasattr(rib_entry, 'prefix') else None,
                                "as_path": rib_entry.as_path
                            }
                            # Only add if both prefix and as_path are valid
                            if record["prefix"] and record["as_path"]:
                                records.append(record)
                                count += 1
                                if count % 100000 == 0:
                                    print(f"  Parsed {count} routes...")
            print(f"Finished parsing. Total valid routes found: {count}")

        # Save parsed data (optional intermediate step)
        # with open(json_path, "w") as f:
        #     json.dump(records, f)
        # print(f"Saved parsed data structure to {json_path}") # Commented out saving intermediate JSON for efficiency
        return records # Return records directly
    except Exception as e:
        print(f"Error during parsing: {e}")
        return None # Indicate failure

def analyze_routes_to_graph(records):
    """Analyzes parsed routes to create an AS graph structure."""
    if not records:
        print("No records to analyze.")
        return None

    print("Analyzing AS paths to build graph...")
    nodes = set()
    links = set() # Use a set of tuples to store unique links

    processed_paths = 0
    for record in records:
        as_path = record.get("as_path")
        if not as_path: # Skip if no AS path
            continue

        # Clean the path: remove AS_SETs {} and convert to strings
        # Flatten prepended paths (e.g., 701 701 3356 -> 701 3356)
        cleaned_path = []
        last_asn = None
        for item in as_path:
            # Basic check if it's a valid ASN (integer or string of digits)
            asn_str = str(item)
            if asn_str.isdigit():
                if asn_str != last_asn: # Avoid self-loops from prepending
                    cleaned_path.append(asn_str)
                    last_asn = asn_str
            # Ignore non-numeric items (like AS_SET markers if pybgpdump includes them)

        if len(cleaned_path) < 2:
            continue # Need at least two ASes for a link

        for i in range(len(cleaned_path) - 1):
            source = cleaned_path[i]
            target = cleaned_path[i+1]

            # Add nodes
            nodes.add(source)
            nodes.add(target)

            # Add link (store as sorted tuple for undirected graph)
            links.add(tuple(sorted((source, target))))
        
        processed_paths += 1
        if processed_paths % 100000 == 0:
            print(f"  Processed {processed_paths} AS paths...")


    print(f"Analysis complete. Found {len(nodes)} unique ASes and {len(links)} unique links.")

    # Convert to D3 format
    graph_data = {
        "nodes": [{"id": node_id} for node_id in nodes],
        "links": [{"source": link_tuple[0], "target": link_tuple[1]} for link_tuple in links]
    }
    return graph_data

def save_graph_data(graph_data, output_path):
    """Saves the graph data to a JSON file."""
    if not graph_data:
        print("No graph data to save.")
        return False
    
    print(f"Saving graph data to {output_path}...")
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(graph_data, f) # No indent for smaller file size
        print("Graph data saved successfully.")
        return True
    except Exception as e:
        print(f"Error saving graph data: {e}")
        return False

def cleanup_files(*files):
    """Removes specified temporary files."""
    for file_path in files:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"Removed temporary file: {file_path}")
            except OSError as e:
                print(f"Error removing file {file_path}: {e}")

# --- Main Pipeline ---
if __name__ == "__main__":
    print("--- Starting MRT Data Pipeline ---")

    # 1. Collection
    if not download_mrt_file(MRT_URL, DOWNLOAD_PATH_GZ):
        print("Pipeline aborted due to download failure.")
        exit(1)

    # 2. Decompression (pybgpdump can handle .gz directly, but let's decompress for clarity/flexibility)
    # if not decompress_mrt_file(DOWNLOAD_PATH_GZ, DECOMPRESSED_PATH):
    #     print("Pipeline aborted due to decompression failure.")
    #     cleanup_files(DOWNLOAD_PATH_GZ)
    #     exit(1)
    # UPDATE: pybgpdump seems to want a file path, not handle .gz directly. Let's stick with decompression.
    # UPDATE 2: Let's TRY passing the .gz path directly to BGPDump; it might handle it internally.
    # If it fails, uncomment decompression step.
    # Let's try using the compressed file directly
    mrt_source_path = DOWNLOAD_PATH_GZ

    # 3. Parsing
    # records = parse_mrt_to_json(DECOMPRESSED_PATH, PARSED_JSON_PATH)
    records = parse_mrt_to_json(mrt_source_path, PARSED_JSON_PATH) # Try with .gz
    if records is None:
        print("Pipeline aborted due to parsing failure.")
        cleanup_files(DOWNLOAD_PATH_GZ, DECOMPRESSED_PATH) # Clean up both if decompressed
        exit(1)

    # 4. Analysis
    graph_data = analyze_routes_to_graph(records)
    if graph_data is None:
        print("Pipeline aborted due to analysis failure.")
        cleanup_files(DOWNLOAD_PATH_GZ, DECOMPRESSED_PATH)
        exit(1)
        
    # Limit graph size for initial testing if needed (e.g., too many nodes/links)
    MAX_NODES = 5000 # Example limit
    MAX_LINKS = 10000 # Example limit
    if len(graph_data["nodes"]) > MAX_NODES or len(graph_data["links"]) > MAX_LINKS:
        print(f"Warning: Graph size ({len(graph_data['nodes'])} nodes, {len(graph_data['links'])} links) exceeds limits.")
        # Optional: Implement sampling or filtering here if needed
        # For now, just warn and proceed with the full data from the single dump.
        pass


    # 5. Save Output for Visualization
    if not save_graph_data(graph_data, OUTPUT_GRAPH_PATH):
        print("Pipeline aborted due to saving failure.")
        cleanup_files(DOWNLOAD_PATH_GZ, DECOMPRESSED_PATH)
        exit(1)

    # 6. Cleanup
    cleanup_files(DOWNLOAD_PATH_GZ, DECOMPRESSED_PATH, PARSED_JSON_PATH) # Clean up downloaded/intermediate files

    print("--- MRT Data Pipeline Finished Successfully ---")
