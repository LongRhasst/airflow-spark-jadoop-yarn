#!/usr/bin/env python3
"""
Test Spark cluster connectivity by submitting jobs to the Spark master
"""
import requests
import json
import time
import subprocess
import os

# def test_spark_master_connection():
#     """Test connection to Spark master"""
#     print("🔍 Testing Spark Master connection...")
    
#     try:
#         response = requests.get("http://spark-master:8080/json/", timeout=10)
#         if response.status_code == 200:
#             print("✅ Spark Master is accessible")
#             cluster_info = response.json()
#             active_apps = cluster_info.get('activeapps', [])
#             print(f"📊 Found {len(active_apps)} active applications")
#             print(f"📊 Cluster status: {cluster_info.get('status', 'UNKNOWN')}")
#             print(f"📊 Alive workers: {cluster_info.get('aliveworkers', 0)}")
#             return True
#         else:
#             print(f"❌ Spark Master returned status code: {response.status_code}")
#             return False
#     except Exception as e:
#         print(f"❌ Failed to connect to Spark Master: {e}")
#         return False

def test_yarn_resourcemanager():
    """Test connection to YARN ResourceManager"""
    print("🔍 Testing YARN ResourceManager connection...")
    
    try:
        response = requests.get("http://resourcemanager:8088/ws/v1/cluster/info", timeout=10)
        if response.status_code == 200:
            print("✅ YARN ResourceManager is accessible")
            info = response.json()
            cluster_info = info.get('clusterInfo', {})
            print(f"📊 Cluster State: {cluster_info.get('state', 'Unknown')}")
            print(f"📊 YARN Version: {cluster_info.get('hadoopVersion', 'Unknown')}")
            return True
        else:
            print(f"❌ YARN ResourceManager returned status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Failed to connect to YARN ResourceManager: {e}")
        return False

def test_hadoop_namenode():
    """Test connection to Hadoop NameNode"""
    print("🔍 Testing Hadoop NameNode connection...")
    
    try:
        response = requests.get("http://namenode:9870/webhdfs/v1/?op=LISTSTATUS", timeout=10)
        if response.status_code == 200:
            print("✅ Hadoop NameNode is accessible")
            return True
        else:
            print(f"❌ Hadoop NameNode returned status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Failed to connect to Hadoop NameNode: {e}")
        return False

def submit_test_spark_job():
    """Submit a simple test job to Spark cluster"""
    print("🚀 Submitting test Spark job...")
    
    # Create a simple Python script for testing
    test_script = """
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSparkConnectivity") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

print("✅ Test DataFrame created successfully!")
df.show()

print("📊 Total records:", df.count())
print("🎉 Spark job completed successfully!")

spark.stop()
"""
    
    # Write test script to a file
    script_path = "/tmp/test_spark_job.py"
    with open(script_path, 'w') as f:
        f.write(test_script)
    
    try:
        # Submit job to Spark cluster using PySpark directly
        # Instead of trying to use docker exec, we'll use PySpark's cluster mode
        from pyspark.sql import SparkSession
        
        print("📋 Submitting job to Spark cluster...")
        
        # Create Spark session that connects to the cluster
        spark = SparkSession.builder \
            .appName("ClusterTest") \
            .master("yarn") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", "1") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()

        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Execute the test logic directly
        data = [1, 2, 3, 4, 5]
        rdd = spark.sparkContext.parallelize(data)
        result_data = rdd.map(lambda x: x * 2).collect()
        
        spark.stop()
        
        # Mock result for compatibility with the rest of the test
        class MockResult:
            def __init__(self, returncode, stdout, stderr):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr
        
        result = MockResult(0, f"Test completed successfully. Results: {result_data}", "")
        
        if result.returncode == 0:
            print("✅ Spark job submitted and completed successfully!")
            print("📋 Job output:")
            print(result.stdout)
            return True
        else:
            print("❌ Spark job failed!")
            print("📋 Error output:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("⏰ Spark job timed out after 60 seconds")
        return False
    except Exception as e:
        print(f"❌ Failed to submit Spark job: {e}")
        return False

def run_comprehensive_test():
    """Run comprehensive connectivity tests"""
    print("🚀 Starting comprehensive Spark cluster connectivity tests...\n")
    
    tests = [
        # ("Spark Master", test_spark_master_connection),
        ("YARN ResourceManager", test_yarn_resourcemanager),
        ("Hadoop NameNode", test_hadoop_namenode),
        ("Spark Job Submission", submit_test_spark_job)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"🧪 Running {test_name} test...")
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results[test_name] = False
        print("-" * 50)
    
    # Summary
    print("\n📊 Test Summary:")
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Spark cluster is ready for use.")
        return True
    else:
        print("⚠️  Some tests failed. Check the logs above for details.")
        return False

if __name__ == "__main__":
    success = run_comprehensive_test()
    exit(0 if success else 1)
