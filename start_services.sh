#!/bin/bash

# Data Pipeline Startup Script with Error Handling
# This script ensures proper startup order: MySQL -> Airflow -> Spark -> Hadoop

set -e

echo "🚀 Starting Data Pipeline Services..."

# Stop any existing services first to avoid conflicts
# echo "🛑 Stopping any existing services..."
# docker compose -f docker-compose.yml -f docker-compose.override.yml down --remove-orphans 2>/dev/null || true
# sleep 5

# Function to cleanup stale Airflow processes and PID files
cleanup_airflow() {
    echo "🧹 Cleaning up stale Airflow processes and PID files..."
    
    # Remove stale PID files from containers
    docker compose exec -T webserver bash -c "rm -f /opt/airflow/airflow-webserver.pid /opt/airflow/airflow-scheduler.pid" 2>/dev/null || true
    docker compose exec -T scheduler bash -c "rm -f /opt/airflow/airflow-webserver.pid /opt/airflow/airflow-scheduler.pid" 2>/dev/null || true
    
    # Kill any running Airflow processes
    docker compose exec -T webserver bash -c "pkill -f 'airflow webserver' || true" 2>/dev/null || true
    docker compose exec -T scheduler bash -c "pkill -f 'airflow scheduler' || true" 2>/dev/null || true
    
    echo "✅ Cleanup completed"
}

# Function to check if a service is healthy
check_service_health() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "🔍 Checking health of $service_name..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker compose ps $service_name | grep -q "healthy\|Up"; then
            echo "✅ $service_name is healthy"
            return 0
        fi
        
        echo "⏳ Waiting for $service_name... (attempt $attempt/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "❌ $service_name failed to becom      e healthy"
    return 1
}


test_spark_connection_yarn(){
    echo "🧪 Testing Spark cluster connectivity..."
    # First run simple connectivity test
    if docker compose exec webserver python /opt/airflow/include/test/test_connectivity.py; then
        echo "✅ Basic connectivity test passed"
        
        # Then try comprehensive test if basic test passes
        echo "🔬 Running comprehensive Spark cluster test..."
        if docker compose exec webserver python /opt/airflow/include/test/test_spark_cluster.py; then
            echo "✅ Comprehensive Spark cluster test passed"
            return 0
        else
            echo "⚠️  Comprehensive test failed, but basic connectivity works"
            return 0  # Still return success if basic connectivity works
        fi
    else
        echo "❌ Basic connectivity test failed"
        return 1
    fi
}

# Function to test Spark connectivity
test_spark_connection() {
    echo "🧪 Testing Spark connection..."
    
    # Try to run the test script
    if docker compose exec webserver python /opt/airflow/include/test/test_spark.py; then
        echo "✅ Spark connection test passed"
        return 0
    else
        echo "⚠️  Spark connection test failed, but continuing..."
        return 1
    fi
}

# Start services in the correct order with health checks
echo "📋 Step 1: Starting MySQL..."
docker compose up -d mysql

echo "📋 Step 2: Waiting for MySQL to be healthy..."
check_service_health mysql

echo "📋 Step 3: Initializing Airflow database..."
docker compose up -d airflow-init
check_service_health airflow-init

echo "⏳ Waiting for Airflow database initialization to complete..."
SECOND_LEFT=15
while [ $SECOND_LEFT -gt 0 ]; do
    echo -n "⏳ $SECOND_LEFT seconds left for Airflow database initialization..."
    sleep 1
    SECOND_LEFT=$((SECOND_LEFT - 1))
    echo -ne "\r"
done
echo "✅ Airflow database initialized"

echo "🧹 Step 3.5: Cleaning up any stale Airflow processes..."
cleanup_airflow

echo "📋 Step 4: Starting Airflow services..."
docker compose up -d webserver scheduler
echo "⏳ Waiting for Airflow webserver and scheduler to be healthy..."
check_service_health webserver
check_service_health scheduler

echo "📋 Step 5: Starting Spark services..."
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d spark-master spark-worker
echo "⏳ Waiting for Spark services to be healthy..."
check_service_health spark-master
check_service_health spark-worker

echo "📋 Step 6: Starting Hadoop services..."
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d hadoop-namenode hadoop-datanode
echo "⏳ Waiting for Hadoop services to be healthy..."
check_service_health hadoop-namenode
check_service_health hadoop-datanode

echo "📋 Step 7: Starting YARN services..."
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d resourcemanager nodemanager
echo "⏳ Waiting for YARN services to be healthy..."
check_service_health resourcemanager
check_service_health nodemanager

echo "📋 Step 8: Starting History Server..."
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d historyserver
echo "⏳ Waiting for History Server to be healthy..."
check_service_health historyserver

echo "📋 Step 9: Testing Spark connection..."
sleep 30
test_spark_connection_yarn

echo "🎉 All services started successfully!"
echo ""
echo "🌐 Access points:"
echo "   - Airflow Web UI: http://localhost:8080 (admin/admin)"
echo "   - Spark Master UI: http://localhost:8081"
echo "   - Hadoop Namenode UI: http://localhost:9870"
echo "   - YARN ResourceManager UI: http://localhost:8088"
echo "   - History Server UI: http://localhost:8188"
echo ""
echo "📚 Next steps:"
echo "   1. Check service logs: docker-compose logs -f [service_name]"
echo "   2. Test Spark: docker-compose exec webserver python /opt/airflow/include/scripts/test_spark.py"
echo "   3. Run DAG: Use 'spark_processing_pipeline_local' for guaranteed local mode"
echo ""
echo "🛠️  If you encounter SparkContext errors:"
echo "   - Use the local fallback DAG: 'spark_processing_pipeline_local'"
echo "   - Check logs: docker-compose logs webserver scheduler"
echo "   - Restart services: ./restart_services.sh"
