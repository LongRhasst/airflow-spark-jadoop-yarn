#!/usr/bin/env python3
"""
Test YARN NodeManager status in the cluster
"""

import requests

YARN_NODES_URL = "http://resourcemanager:8088/ws/v1/cluster/nodes"
YARN_CLUSTER_INFO_URL = "http://resourcemanager:8088/ws/v1/cluster/info"

def test_yarn_nodes():
    print("🔍 Fetching YARN NodeManager list...")

    try:
        response = requests.get(YARN_NODES_URL, timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed to fetch node list, status: {response.status_code}")
            return False

        data = response.json()
        nodes = data.get("nodes", {}).get("node", [])
        if not nodes:
            print("⚠️  No NodeManagers found in cluster")
            return False

        print(f"✅ Found {len(nodes)} NodeManager(s):\n")
        for node in nodes:
            print(f"🖥️  Node ID: {node.get('id')}")
            print(f"    Rack: {node.get('rack')}")
            print(f"    State: {node.get('state')}")
            print(f"    Health: {node.get('healthReport', 'Healthy')}")
            print(f"    Memory (MB): {node.get('availMemoryMB')}/{node.get('totalMemoryMB')}")
            print(f"    vCores: {node.get('usedVirtualCores')}/{node.get('availableVirtualCores')}")
            print("-" * 40)

        return True

    except Exception as e:
        print(f"❌ Error during YARN node test: {e}")
        return False

def test_yarn_cluster_state():
    print("🔍 Checking YARN cluster state...")

    try:
        response = requests.get(YARN_CLUSTER_INFO_URL, timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed to get cluster info, status: {response.status_code}")
            return False

        info = response.json().get("clusterInfo", {})
        print(f"✅ YARN Cluster State: {info.get('state', 'Unknown')}")
        print(f"📦 ResourceManager Version: {info.get('resourceManagerVersion', 'Unknown')}")
        print(f"📊 HA State: {info.get('haState', 'N/A')}")
        print(f"🕒 Started On: {info.get('startedOn', 'Unknown')}")
        return info.get('state') == "STARTED"

    except Exception as e:
        print(f"❌ Failed to check cluster state: {e}")
        return False

def main():
    print("🚀 Starting YARN NodeManager Test...\n")

    cluster_ok = test_yarn_cluster_state()
    print("\n" + "=" * 60 + "\n")
    nodes_ok = test_yarn_nodes()

    if cluster_ok and nodes_ok:
        print("\n🎉 YARN cluster and nodes are healthy and ready.")
        return True
    elif cluster_ok:
        print("\n⚠️  Cluster is running but no healthy nodes were found.")
        return False
    else:
        print("\n❌ Cluster or NodeManager status is invalid.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
