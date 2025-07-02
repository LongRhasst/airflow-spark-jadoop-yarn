import os
from pyspark import SparkConf, SparkContext

def test_spark_yarn_connection():
    """Test SparkContext initialization with YARN cluster mode"""
    
    print("🧪 Testing Spark connection to YARN cluster...")
    
    try:
        # Configure Spark for YARN cluster
        conf = SparkConf()
        conf.setAppName("TestSparkContext-YARN")
        conf.setMaster("yarn")
        conf.set("spark.submit.deployMode", "client")
        conf.set("spark.yarn.resourcemanager.address", "resourcemanager:8032")
        conf.set("spark.yarn.resourcemanager.scheduler.address", "resourcemanager:8030")
        conf.set("spark.yarn.resourcemanager.resource-tracker.address", "resourcemanager:8031")
        conf.set("spark.yarn.resourcemanager.webapp.address", "resourcemanager:8088")
        conf.set("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        conf.set("spark.sql.warehouse.dir", "hdfs://hadoop-namenode:9000/user/hive/warehouse")
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.executor.cores", "1")
        conf.set("spark.executor.instances", "1")
        conf.set("spark.driver.memory", "512m")
        
        # Initialize SparkContext
        print("⚙️  Initializing SparkContext with YARN configuration...")
        sc = SparkContext(conf=conf)
        
        print("✅ SparkContext initialized successfully!")
        print(f"📊 Spark version: {sc.version}")
        print(f"🎯 Application ID: {sc.applicationId}")
        print(f"🔗 Master: {sc.master}")
        print(f"📁 Default parallelism: {sc.defaultParallelism}")
        
        # Test basic RDD operations
        print("🔄 Testing basic RDD operations...")
        test_rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = test_rdd.map(lambda x: x * 2).collect()
        print(f"📈 RDD test result: {result}")
        
        # Test HDFS connectivity (if available)
        try:
            print("💾 Testing HDFS connectivity...")
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            print("✅ HDFS connection successful!")
        except Exception as hdfs_error:
            print(f"⚠️  HDFS connection failed: {hdfs_error}")
        
        print("🎉 All tests completed successfully!")
        sc.stop()
        return True
        
    except Exception as e:
        print(f"❌ Failed to initialize SparkContext with YARN: {e}")
        print(f"🔍 Error type: {type(e).__name__}")
        
        # Provide troubleshooting suggestions
        print("\n🛠️  Troubleshooting suggestions:")
        print("   1. Ensure YARN ResourceManager is running on resourcemanager:8088")
        print("   2. Check Hadoop NameNode is accessible at hadoop-namenode:9000")
        print("   3. Verify network connectivity between containers")
        print("   4. Check YARN and Hadoop service logs")
        
        return False

def test_spark_local_fallback():
    """Fallback test with local Spark mode"""
    
    print("🔄 Testing fallback with local Spark mode...")
    
    try:
        conf = SparkConf()
        conf.setAppName("TestSparkContext-Local")
        conf.setMaster("local[*]")
        
        sc = SparkContext(conf=conf)
        print("✅ Local SparkContext initialized successfully!")
        print(f"📊 Spark version: {sc.version}")
        
        # Test basic operations
        test_rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = test_rdd.map(lambda x: x * 2).collect()
        print(f"📈 Local RDD test result: {result}")
        
        sc.stop()
        return True
        
    except Exception as e:
        print(f"❌ Even local mode failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting Spark connectivity tests...\n")
    
    # Try YARN first
    yarn_success = test_spark_yarn_connection()
    
    if not yarn_success:
        print("\n🔄 YARN connection failed, trying local mode...")
        local_success = test_spark_local_fallback()
        
        if not local_success:
            print("\n💥 All Spark connection attempts failed!")
            exit(1)
        else:
            print("\n⚠️  Local mode works, but YARN connection failed")
            exit(2)
    else:
        print("\n🎉 YARN connection successful!")
        exit(0)
