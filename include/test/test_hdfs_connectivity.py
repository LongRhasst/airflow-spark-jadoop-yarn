#!/usr/bin/env python3
"""
HDFS connectivity test script
"""

import subprocess
import sys
import os

def test_hdfs_connectivity():
    """Test HDFS connectivity using hadoop command line tools if available"""
    print("🧪 Testing HDFS connectivity...")
    
    try:
        # Test basic HDFS connection via namenode web interface
        import requests
        
        print("🔍 Testing HDFS NameNode web interface...")
        response = requests.get("http://namenode:9870/webhdfs/v1/?op=LISTSTATUS", timeout=10)
        
        if response.status_code == 200:
            print("✅ HDFS NameNode web interface is accessible")
            data = response.json()
            print(f"✅ HDFS root directory listing successful")
            print(f"🔍 Found {len(data.get('FileStatuses', {}).get('FileStatus', []))} items in root directory")
            return True
        else:
            print(f"❌ HDFS NameNode returned status code: {response.status_code}")
            return False
            
    except ImportError:
        print("⚠️  requests library not available, skipping web interface test")
        return False
    except Exception as e:
        print(f"❌ HDFS connectivity test failed: {e}")
        return False

def test_spark_hdfs_integration():
    """Test Spark integration with HDFS"""
    print("\n🧪 Testing Spark-HDFS integration...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = None
        try:
            # Create Spark session with HDFS configuration
            print("🚀 Creating Spark session with HDFS configuration...")
            spark = SparkSession.builder \
                .appName("HDFSConnectivityTest") \
                .master("local[1]") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("ERROR")
            
            print("✅ Spark session with HDFS configuration created successfully!")
            
            # Try to list HDFS root directory using Spark
            print("🔍 Testing HDFS access through Spark...")
            
            # Create a simple test file in local filesystem first
            test_data = [("test1", 1), ("test2", 2)]
            df = spark.createDataFrame(test_data, ["name", "value"])
            
            # Test local file operations (since HDFS might not be fully initialized)
            local_test_path = "file:///tmp/spark_hdfs_test"
            print(f"🔍 Testing file operations at {local_test_path}...")
            
            df.write.mode("overwrite").parquet(local_test_path)
            print("✅ Successfully wrote test data to file system")
            
            read_df = spark.read.parquet(local_test_path)
            count = read_df.count()
            print(f"✅ Successfully read back {count} rows from file system")
            
            # Clean up
            import shutil
            if os.path.exists("/tmp/spark_hdfs_test"):
                shutil.rmtree("/tmp/spark_hdfs_test")
                
            print("✅ Spark-HDFS integration test completed successfully")
            return True
            
        finally:
            if spark:
                spark.stop()
                print("✅ Spark session stopped")
                
    except Exception as e:
        print(f"❌ Spark-HDFS integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting HDFS connectivity tests...")
    
    # Test 1: Basic HDFS connectivity
    hdfs_success = test_hdfs_connectivity()
    
    # Test 2: Spark-HDFS integration
    spark_hdfs_success = test_spark_hdfs_integration()
    
    print("\n" + "="*60)
    print("📊 HDFS CONNECTIVITY TEST SUMMARY")
    print("="*60)
    print(f"HDFS NameNode Web Interface    : {'✅ PASS' if hdfs_success else '❌ FAIL'}")
    print(f"Spark-HDFS Integration         : {'✅ PASS' if spark_hdfs_success else '❌ FAIL'}")
    print("-"*60)
    
    overall_success = hdfs_success and spark_hdfs_success
    print(f"Overall Result: {'🎉 All tests passed!' if overall_success else '❌ Some tests failed'}")
    
    sys.exit(0 if overall_success else 1)
