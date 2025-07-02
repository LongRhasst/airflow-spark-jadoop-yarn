import os
import sys
import socket
from pyspark import SparkConf, SparkContext

def check_service_connectivity(host, port, service_name):
    """Check connectivity to a service"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            print(f"✅ {service_name} ({host}:{port}) is reachable")
            return True
        else:
            print(f"❌ {service_name} ({host}:{port}) is not reachable")
            return False
    except Exception as e:
        print(f"❌ Error checking {service_name} ({host}:{port}): {e}")
        return False

def test_spark_yarn_connection():
    """Test SparkContext initialization with YARN cluster mode"""
    
    print("🧪 Testing Spark connection to YARN cluster...")
    
    try:
        # Set required environment variables for Spark
        # Use the paths from the Dockerfile
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
        
        # Try to find PySpark installation
        try:
            import pyspark
            spark_home = os.path.dirname(pyspark.__file__)
            os.environ['SPARK_HOME'] = spark_home
            print(f"🔍 Found PySpark at: {spark_home}")
        except ImportError:
            print("❌ PySpark not found. Installing...")
            return False
        
        # Set Hadoop environment variables for YARN connectivity
        os.environ['HADOOP_CONF_DIR'] = '/opt/hadoop/etc/hadoop'
        os.environ['YARN_CONF_DIR'] = '/opt/hadoop/etc/hadoop'
        
        print("🔧 Environment variables set for Spark connectivity")
        
        # Configure Spark for YARN cluster
        conf = SparkConf()
        conf.setAppName("TestSparkContext-YARN")
        conf.setMaster("yarn")
        conf.set("spark.submit.deployMode", "client")  # Use client mode for testing
        
        # YARN configuration - use container network names
        conf.set("spark.yarn.resourcemanager.address", "resourcemanager:8032")
        conf.set("spark.yarn.resourcemanager.scheduler.address", "resourcemanager:8030")
        conf.set("spark.yarn.resourcemanager.resource-tracker.address", "resourcemanager:8031")
        conf.set("spark.yarn.resourcemanager.webapp.address", "resourcemanager:8088")
        
        # Hadoop configuration
        conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        conf.set("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        
        # Resource allocation - reduced for container environment
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.executor.cores", "1")
        conf.set("spark.executor.instances", "2")
        conf.set("spark.driver.memory", "2g")
        
        # Additional configuration for containerized environment
        conf.set("spark.driver.host", "webserver")
        conf.set("spark.driver.bindAddress", "0.0.0.0")
        conf.set("spark.ui.enabled", "false")  # Disable UI for testing
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Add network configuration to handle container networking
        conf.set("spark.sql.adaptive.enabled", "false")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        
        # Initialize SparkContext
        print("⚙️  Initializing SparkContext with YARN configuration...")
        try:
            sc = SparkContext(conf=conf)
        except Exception as e:
            print("❌ SparkContext init failed:", str(e))
            raise
        
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
        print("   2. Check Hadoop NameNode is accessible at namenode:9000")
        print("   3. Verify network connectivity between containers")
        print("   4. Check YARN and Hadoop service logs")
        print("   5. Verify PySpark is properly installed")
        
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
    
    # First, check connectivity to required services
    print("🔍 Checking service connectivity...")
    services_ok = True
    services_ok &= check_service_connectivity("resourcemanager", 8088, "YARN ResourceManager")
    services_ok &= check_service_connectivity("namenode", 9000, "Hadoop NameNode")
    services_ok &= check_service_connectivity("namenode", 9870, "Hadoop NameNode WebUI")
    
    if not services_ok:
        print("⚠️  Some services are not reachable. Continuing with tests...")
    print()
    
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
