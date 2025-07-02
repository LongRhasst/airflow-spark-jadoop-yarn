#!/usr/bin/env python3
"""
Test Spark connection to YARN cluster
"""

import os
import sys
import time
import requests
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def check_yarn_health():
    """Check if YARN ResourceManager is accessible"""
    print("🔍 Checking YARN ResourceManager health...")
    
    try:
        response = requests.get("http://resourcemanager:8088/ws/v1/cluster/info", timeout=10)
        if response.status_code == 200:
            data = response.json()
            cluster_info = data.get("clusterInfo", {})
            print(f"✅ YARN ResourceManager is running")
            print(f"   State: {cluster_info.get('state', 'Unknown')}")
            print(f"   Version: {cluster_info.get('hadoopVersion', 'Unknown')}")
            return True
        else:
            print(f"❌ YARN ResourceManager responded with status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to YARN ResourceManager: {e}")
        return False

def check_yarn_nodes():
    """Check available YARN NodeManagers"""
    print("🔍 Checking YARN NodeManagers...")
    
    try:
        response = requests.get("http://resourcemanager:8088/ws/v1/cluster/nodes", timeout=10)
        if response.status_code == 200:
            data = response.json()
            nodes = data.get("nodes", {}).get("node", [])
            
            if not nodes:
                print("⚠️  No NodeManagers found in cluster")
                return False
                
            print(f"✅ Found {len(nodes)} NodeManager(s):")
            for node in nodes:
                node_id = node.get("id", "Unknown")
                state = node.get("state", "Unknown")
                used_memory = node.get("usedMemoryMB", 0)
                avail_memory = node.get("availMemoryMB", 0)
                used_vcores = node.get("usedVirtualCores", 0)
                avail_vcores = node.get("availableVirtualCores", 0)
                
                print(f"   Node: {node_id}")
                print(f"   State: {state}")
                print(f"   Memory: {used_memory}MB used, {avail_memory}MB available")
                print(f"   vCores: {used_vcores} used, {avail_vcores} available")
                print()
                
                if avail_vcores == 0:
                    print("⚠️  Warning: No vCores available on this node")
                    
            return True
        else:
            print(f"❌ Failed to get node information: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error checking YARN nodes: {e}")
        return False

def test_spark_yarn_minimal():
    """Test minimal Spark connection to YARN"""
    print("🧪 Testing minimal Spark connection to YARN...")
    
    try:
        # Set environment variables
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
        os.environ['HADOOP_CONF_DIR'] = '/opt/hadoop/etc/hadoop'
        os.environ['YARN_CONF_DIR'] = '/opt/hadoop/etc/hadoop'
        
        # Configure Spark with minimal resources
        conf = SparkConf()
        conf.setAppName("TestSparkYARN-Minimal")
        conf.setMaster("yarn")
        conf.set("spark.submit.deployMode", "client")
        
        # YARN configuration
        conf.set("spark.yarn.resourcemanager.address", "resourcemanager:8032")
        conf.set("spark.yarn.resourcemanager.scheduler.address", "resourcemanager:8030")
        conf.set("spark.yarn.resourcemanager.resource-tracker.address", "resourcemanager:8031")
        conf.set("spark.yarn.resourcemanager.webapp.address", "resourcemanager:8088")
        
        # Hadoop configuration
        conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        
        # Minimal resource allocation
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.executor.cores", "1")
        conf.set("spark.executor.instances", "1")
        conf.set("spark.driver.memory", "1g")
        
        # Additional configuration for containerized environment
        conf.set("spark.driver.host", "spark")
        conf.set("spark.driver.bindAddress", "0.0.0.0")
        conf.set("spark.ui.enabled", "false")
        
        # Increase timeouts
        conf.set("spark.sql.adaptive.enabled", "false")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        print("🔧 Creating SparkContext with minimal configuration...")
        print("   Master: yarn")
        print("   Deploy Mode: client")
        print("   Executor Memory: 1g")
        print("   Executor Cores: 1")
        print("   Executor Instances: 1")
        
        # Create SparkContext
        sc = SparkContext(conf=conf)
        
        print("✅ SparkContext created successfully!")
        print(f"   Application ID: {sc.applicationId}")
        print(f"   Spark Version: {sc.version}")
        print(f"   Master: {sc.master}")
        
        # Simple test operation
        print("🧪 Testing basic RDD operation...")
        rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = rdd.map(lambda x: x * 2).collect()
        print(f"✅ RDD operation successful: {result}")
        
        # Stop SparkContext
        sc.stop()
        print("✅ SparkContext stopped successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create SparkContext: {e}")
        print(f"   Error type: {type(e).__name__}")
        return False

def test_spark_session_yarn():
    """Test SparkSession with YARN"""
    print("🧪 Testing SparkSession with YARN...")
    
    try:
        # Set environment variables
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
        os.environ['HADOOP_CONF_DIR'] = '/opt/hadoop/etc/hadoop'
        os.environ['YARN_CONF_DIR'] = '/opt/hadoop/etc/hadoop'
        
        # Create SparkSession
        spark = SparkSession.builder \
            .appName("TestSparkSession-YARN") \
            .master("yarn") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.yarn.resourcemanager.address", "resourcemanager:8032") \
            .config("spark.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
            .config("spark.yarn.resourcemanager.resource-tracker.address", "resourcemanager:8031") \
            .config("spark.yarn.resourcemanager.webapp.address", "resourcemanager:8088") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.instances", "1") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.host", "spark") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        print("✅ SparkSession created successfully!")
        print(f"   Application ID: {spark.sparkContext.applicationId}")
        print(f"   Spark Version: {spark.version}")
        
        # Simple DataFrame test
        print("🧪 Testing basic DataFrame operation...")
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
        df.show()
        
        count = df.count()
        print(f"✅ DataFrame operation successful: {count} rows")
        
        # Stop SparkSession
        spark.stop()
        print("✅ SparkSession stopped successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create SparkSession: {e}")
        print(f"   Error type: {type(e).__name__}")
        return False

def test_spark_local_fallback():
    """Test Spark in local mode as fallback"""
    print("🔄 Testing Spark in local mode (fallback)...")
    
    try:
        conf = SparkConf()
        conf.setAppName("TestSparkLocal-Fallback")
        conf.setMaster("local[2]")
        
        sc = SparkContext(conf=conf)
        
        print("✅ Local SparkContext created successfully!")
        
        # Simple test
        rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = rdd.map(lambda x: x * 2).sum()
        print(f"✅ Local computation result: {result}")
        
        sc.stop()
        print("✅ Local SparkContext stopped successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create local SparkContext: {e}")
        return False

def main():
    """Main test function"""
    print("=" * 60)
    print("🚀 Spark YARN Connection Test Suite")
    print("=" * 60)
    
    # Step 1: Check YARN health
    if not check_yarn_health():
        print("❌ YARN ResourceManager is not accessible. Aborting tests.")
        return False
    
    print()
    
    # Step 2: Check YARN nodes
    if not check_yarn_nodes():
        print("❌ No NodeManagers available. Aborting tests.")
        return False
    
    print()
    
    # Step 3: Test minimal Spark connection
    print("-" * 60)
    if test_spark_yarn_minimal():
        print("✅ Minimal Spark YARN test passed!")
    else:
        print("❌ Minimal Spark YARN test failed!")
        print("\n🔄 Trying local mode fallback...")
        test_spark_local_fallback()
        return False
    
    print()
    
    # Step 4: Test SparkSession
    print("-" * 60)
    if test_spark_session_yarn():
        print("✅ SparkSession YARN test passed!")
    else:
        print("❌ SparkSession YARN test failed!")
    
    print()
    print("=" * 60)
    print("🎉 All tests completed!")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
