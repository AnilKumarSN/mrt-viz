import requests
# import gzip # No longer needed for decompression by the script
# import shutil # No longer needed for decompression by the script
import pandas as pd
import json
import os
# from pybgpdump import BGPDump # REMOVE THIS
from bgpkit_parser import BgpkitParser # ADD THIS
from datetime import datetime, timedelta

# --- Configuration ---
# Use a specific RIPE collector (e.g., rrc00)
TARGET_DATE = datetime(2023, 10, 1) # Example fixed date
RRC_COLLECTOR = "rrc00"
BASE_URL = f"https://data.ris.ripe.net/{RRC_COLLECTOR}/"
DATE_PATH = TARGET_DATE.strftime('%Y.%m')
MRT_FILE_NAME_GZ = f"bview.{TARGET_DATE.strftime('%Y%m%d')}.0000.gz"
MRT_URL = f"{BASE_URL}{DATE_PATH}/{MRT_FILE_NAME_GZ}"

DOWNLOAD_PATH_GZ = f"{MRT_FILE_NAME_GZ}"
# DECOMPRESSED_PATH = "latest_dump.mrt" # No longer needed
# PARSED_JSON_PATH = "parsed_routes.json" # No longer needed as intermediate step
OUTPUT_GRAPH_DIR = "site/data"
OUTPUT_GRAPH_PATH = os.path.join(OUTPUT_GRAPH_DIR, "as_graph.json")

# --- Helper Functions ---
def download_mrt_file(url, dest_path):
    """Downloads the MRT file."""
    print(f"Attempting to download MRT file from: {url}")
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {os.path.basename(dest_path)}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return False

# def decompress_mrt_file(...): # REMOVE this function, bgpkit handles .gz

def parse_mrt_to_records(mrt_path): # Renamed function slightly
    """Parses MRT dump file using bgpkit-parser."""
    print(f"Parsing MRT file using bgpkit-parser: {mrt_path}...")
    records = []
    try:
        # BgpkitParser takes the file path directly
        parser = BgpkitParser(filename=mrt_path)
        count = 0
        skipped_no_path = 0
        # Iterate through BGP elements in the file
        for elem in parser:
            # We are interested in RIB entries from TABLE_DUMP_V2
            # elem_type for RIB entries is 'RIB'
            if elem.elem_type == "RIB":
                 # Check if as_path exists and is not empty or None
                if elem.as_path and str(elem.as_path).strip():
                    # Convert AS path to a list of strings (it might already be)
                    # Handle potential variations in how bgpkit returns the path
                    as_path_str = str(elem.as_path) # Get string representation
                    as_path_list = as_path_str.split() # Split by space

                    record = {
                        "prefix": str(elem.prefix),
                        # Store AS path as a list of strings
                        "as_path": [asn for asn in as_path_list if asn.isdigit()]
                    }
                    # Only add if path is not empty after cleaning
                    if record["as_path"]:
                        records.append(record)
                        count += 1
                        if count % 100000 == 0:
                            print(f"  Parsed {count} RIB entries...")
                    else:
                        skipped_no_path += 1
                else:
                    skipped_no_path += 1

        print(f"Finished parsing. Total valid RIB entries found: {count}")
        if skipped_no_path > 0:
             print(f"Skipped {skipped_no_path} entries due to missing/empty AS paths.")
        return records # Return records directly
    except Exception as e:
        # Print more detailed error if possible
        import traceback
        print(f"Error during BGPKit parsing: {e}")
        print(traceback.format_exc())
        return None # Indicate failure


def analyze_routes_to_graph(records):
    """Analyzes parsed routes to create an AS graph structure."""
    if not records:
        print("No records to analyze.")
        return None

    print("Analyzing AS paths to build graph...")
    nodes = set()
    links = set()

    processed_paths = 0
    for record in records:
        # The as_path should already be a list of strings from the parser function
        as_path_list = record.get("as_path")

        if not as_path_list or len(as_path_list) < 2:
            continue # Need at least two ASes for a link

        # Flatten prepended paths (remove consecutive duplicates)
        cleaned_path = []
        last_asn = None
        for asn_str in as_path_list:
            # ASN should already be a string digit from parser
            if asn_str != last_asn:
                cleaned_path.append(asn_str)
                last_asn = asn_str

        if len(cleaned_path) < 2:
            continue

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

    # 2. Parsing (using bgpkit-parser on the downloaded .gz file)
    # No decompression step needed.
    mrt_source_path = DOWNLOAD_PATH_GZ
    records = parse_mrt_to_records(mrt_source_path) # Use the new parser function

    if records is None:
        print("Pipeline aborted due to parsing failure.")
        cleanup_files(DOWNLOAD_PATH_GZ) # Only need to clean up the downloaded file
        exit(1)

    # 3. Analysis
    graph_data = analyze_routes_to_graph(records)
    if graph_data is None:
        print("Pipeline aborted due to analysis failure.")
        cleanup_files(DOWNLOAD_PATH_GZ)
        exit(1)

    # Optional: Limit graph size check (can remove if performance is okay)
    MAX_NODES = 10000 # Increased limits slightly
    MAX_LINKS = 20000
    num_nodes = len(graph_data["nodes"])
    num_links = len(graph_data["links"])
    print(f"Generated graph has {num_nodes} nodes and {num_links} links.")
    if num_nodes > MAX_NODES or num_links > MAX_LINKS:
        print(f"Warning: Graph size exceeds suggested limits ({MAX_NODES} nodes, {MAX_LINKS} links). Visualization might be slow.")
        # Consider implementing filtering/sampling in analyze_routes_to_graph if needed
        pass

    # 4. Save Output for Visualization
    if not save_graph_data(graph_data, OUTPUT_GRAPH_PATH):
        print("Pipeline aborted due to saving failure.")
        cleanup_files(DOWNLOAD_PATH_GZ)
        exit(1)

    # 5. Cleanup
    cleanup_files(DOWNLOAD_PATH_GZ) # Only need to clean up the downloaded file

    print("--- MRT Data Pipeline Finished Successfully ---")
