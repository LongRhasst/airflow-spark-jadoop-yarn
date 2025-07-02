#!/bin/bash

# Spark-on-YARN Pipeline Startup Script
# Author: Long Phạm Huy - July 2025

set -e

echo "🚀 Starting Spark-on-YARN Data Pipeline Services..."

# Cleanup Airflow PIDs if stale
cleanup_airflow() {
    echo "🧹 Cleaning up Airflow PID files..."
    docker compose exec -T webserver bash -c "rm -f /opt/airflow/airflow-webserver.pid" || true
    docker compose exec -T scheduler bash -c "rm -f /opt/airflow/airflow-scheduler.pid" || true
    docker compose exec -T webserver bash -c "pkill -f 'airflow webserver' || true" || true
    docker compose exec -T scheduler bash -c "pkill -f 'airflow scheduler' || true" || true
}

check_service_health() {
    local service=$1
    local max_attempts=30
    local attempt=1

    echo "🔍 Checking health of $service..."

    while [ $attempt -le $max_attempts ]; do
        if docker compose ps "$service" | grep -q "healthy\|Up"; then
            echo "✅ $service is healthy"
            return 0
        fi
        echo "⏳ Waiting for $service... ($attempt/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "❌ $service failed to become healthy"
    return 1
}

test_spark_connection_yarn() {
    echo "🧪 Testing Spark YARN connectivity..."
    if docker compose exec webserver python /opt/airflow/include/test/test_connectivity.py; then
        echo "✅ Basic Spark connectivity test passed"
        if docker compose exec webserver python /opt/airflow/include/test/test_spark_cluster.py; then
            echo "✅ Spark-on-YARN cluster test passed"
        else
            echo "⚠️  Cluster test failed, but Spark is reachable"
        fi
    else
        echo "❌ Spark connectivity test failed"
        return 1
    fi
}

### START PIPELINE ###
echo "📋 Step 1: Start MySQL..."
docker compose up -d mysql
check_service_health mysql

echo "📋 Step 2: Init Airflow database..."
docker compose up -d airflow-init
check_service_health airflow-init
sleep 10

echo "📋 Step 3: Start Airflow Webserver & Scheduler..."
cleanup_airflow
docker compose up -d webserver scheduler
check_service_health webserver
check_service_health scheduler

echo "📋 Step 4: Start Hadoop HDFS (Namenode + Datanode)..."
docker compose up -d namenode datanode
check_service_health namenode
check_service_health datanode

echo "📋 Step 5: Start YARN ResourceManager + NodeManager..."
docker compose up -d resourcemanager nodemanager
check_service_health resourcemanager
check_service_health nodemanager

echo "📋 Step 6: Start Spark client container..."
docker compose up -d spark
check_service_health spark

echo "📋 Step 7: Start HistoryServer..."
docker compose up -d historyserver
check_service_health historyserver

echo "📋 Step 8: Test Spark-on-YARN connectivity..."
sleep 15
test_spark_connection_yarn || echo "⚠️ Spark-on-YARN test failed (check logs)"

echo "🎉 All services started successfully!"
echo ""
echo "🌐 Access points:"
echo "   - Airflow Web UI:           http://localhost:8080 (admin/admin)"
echo "   - Spark on YARN via spark-submit"
echo "   - HDFS UI (NameNode):       http://localhost:9870"
echo "   - YARN ResourceManager UI:  http://localhost:8088"
echo "   - Spark History Server UI:  http://localhost:8188"
echo ""
echo "🧪 To test Spark manually:"
echo "   docker compose exec spark bash"
echo "   spark-submit --master yarn --deploy-mode client /opt/spark-apps/your_script.py"
echo ""
echo "📚 DAGs using Spark-on-YARN should use SparkSubmitOperator with master=yarn"
