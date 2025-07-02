#!/usr/bin/env python3
"""
Simple connectivity test that doesn't require PySpark in the current container
"""
import requests
import socket
import time

def test_service_connectivity():
    """Test basic network connectivity to all services"""
    
    services = {
        "Spark Master": ("spark-master", 8080),
        "Spark Master (Spark Port)": ("spark-master", 7077),
        "YARN ResourceManager": ("resourcemanager", 8088),
        "Hadoop NameNode": ("hadoop-namenode", 9870),
        "Hadoop NameNode (HDFS)": ("hadoop-namenode", 9000),
    }
    
    print("🔍 Testing network connectivity to all services...\n")
    
    results = {}
    
    for service_name, (host, port) in services.items():
        print(f"🧪 Testing {service_name} at {host}:{port}...")
        
        try:
            # Test socket connectivity
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"✅ {service_name} is reachable")
                results[service_name] = True
            else:
                print(f"❌ {service_name} is not reachable")
                results[service_name] = False
                
        except Exception as e:
            print(f"❌ {service_name} connection failed: {e}")
            results[service_name] = False
    
    return results

def test_web_endpoints():
    """Test HTTP endpoints"""
    
    endpoints = {
        "Spark Master UI": "http://spark-master:8080",
        "YARN ResourceManager UI": "http://resourcemanager:8088",
        "Hadoop NameNode UI": "http://hadoop-namenode:9870",
    }
    
    print("\n🌐 Testing web endpoints...\n")
    
    results = {}
    
    for name, url in endpoints.items():
        print(f"🧪 Testing {name} at {url}...")
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"✅ {name} is accessible")
                results[name] = True
            else:
                print(f"⚠️  {name} returned status {response.status_code}")
                results[name] = False
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            results[name] = False
    
    return results

def main():
    """Main test function"""
    print("🚀 Starting simple connectivity tests...\n")
    
    # Test network connectivity
    network_results = test_service_connectivity()
    
    # Test web endpoints
    web_results = test_web_endpoints()
    
    # Combine results
    all_results = {**network_results, **web_results}
    
    # Summary
    print("\n" + "="*60)
    print("📊 CONNECTIVITY TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for result in all_results.values() if result)
    total = len(all_results)
    
    for test_name, result in all_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:30} : {status}")
    
    print("-" * 60)
    print(f"Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All connectivity tests passed!")
        return True
    elif passed >= total * 0.7:  # 70% pass rate
        print("⚠️  Most tests passed, cluster should be functional")
        return True
    else:
        print("❌ Too many tests failed, cluster may have issues")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
